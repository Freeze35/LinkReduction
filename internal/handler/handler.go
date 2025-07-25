package handler

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	initprometheus "linkreduction/internal/prometheus"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// InitRoutes настраивает маршруты
func (h *Handler) InitRoutes(app *fiber.App) {
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
	app.Post("/createShortLink", h.createShortLink)
	app.Get("/:key", h.redirect)
	go h.consumeShortenURLs()
	go h.cleanupOldLinks() // Запуск горутины для очистки старых записей
}

// BaseURL - базовый домен для коротких ссылок
var BaseURL = os.Getenv("BASE_URL")

// Handler - структура для хранения подключения к базе данных, Redis, Kafka и метрик Prometheus
type Handler struct {
	Db       *sql.DB
	redis    *redis.Client
	producer sarama.SyncProducer
	metrics  *initprometheus.PrometheusMetrics
}

// ShortenRequest - структура для парсинга JSON-запроса
type ShortenRequest struct {
	URL string `json:"url"`
}

// ShortenMessage - структура для сообщений Kafka
type ShortenMessage struct {
	OriginalURL string `json:"original_url"`
	ShortLink   string `json:"short_link"`
}

// NewHandler создаёт новый экземпляр Handler
func NewHandler(db *sql.DB, redisClient *redis.Client, producer sarama.SyncProducer,
	prometheus *initprometheus.PrometheusMetrics) (*Handler, error) {

	return &Handler{
		Db:       db,
		redis:    redisClient,
		producer: producer,
		metrics:  prometheus,
	}, nil
}

// generateShortLink генерирует короткий ключ (6 символов)
func generateShortLink(originalURL string) string {
	hash := md5.Sum([]byte(originalURL))
	return fmt.Sprintf("%x", hash)[:6]
}

// shortenURL проверяет URL, ищет в кэше/БД или генерирует новый ключ
func (h *Handler) shortenURL(originalURL string) (string, error) {
	if !strings.HasPrefix(originalURL, "http://") && !strings.HasPrefix(originalURL, "https://") {
		return "", fmt.Errorf("некорректный URL: должен начинаться с http:// или https://")
	}
	parsedURL, err := url.Parse(originalURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return "", fmt.Errorf("некорректный формат URL")
	}

	ctx := context.Background()
	cacheKey := "shorten:" + originalURL
	if cachedShortLink, err := h.redis.Get(ctx, cacheKey).Result(); err == nil {
		log.Printf("Найдено в кэше (shorten): %s -> %s", originalURL, cachedShortLink)
		return cachedShortLink, nil
	} else if err != redis.Nil {
		log.Printf("Ошибка чтения из Redis (shorten): %v", err)
	}

	var shortLink string
	err = h.Db.QueryRow("SELECT short_link FROM links WHERE link = $1", originalURL).Scan(&shortLink)
	if err == nil {
		if err := h.redis.Set(ctx, cacheKey, shortLink, 10*time.Minute).Err(); err != nil {
			log.Printf("Ошибка записи в Redis (shorten): %v", err)
		} else {
			log.Printf("Закэшировано (shorten): %s -> %s с TTL 10 минут", originalURL, shortLink)
		}
		return shortLink, nil
	}
	if err != sql.ErrNoRows {
		return "", fmt.Errorf("ошибка проверки URL в базе данных: %v", err)
	}

	for i := 0; i < 3; i++ {
		inputURL := originalURL
		if i > 0 {
			inputURL = fmt.Sprintf("%s_%d", originalURL, i)
		}
		shortLink = generateShortLink(inputURL)
		var exists string
		err = h.Db.QueryRow("SELECT short_link FROM links WHERE short_link = $1", shortLink).Scan(&exists)
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return "", fmt.Errorf("ошибка проверки ключа: %v", err)
		}
		if i == 2 {
			return "", fmt.Errorf("не удалось сгенерировать уникальный ключ после %d попыток", i+1)
		}
	}

	return shortLink, nil
}

// createShortLink обрабатывает POST-запрос для создания короткой ссылки
func (h *Handler) createShortLink(c *fiber.Ctx) error {
	start := time.Now()
	log.Printf("Получен POST-запрос на /createShortLink")
	log.Printf("Тело запроса: %s", c.Body())
	log.Printf("Заголовки запроса: %v", c.GetReqHeaders())

	var originalURL string
	if c.Get("Content-Type") == "application/json" {
		var req ShortenRequest
		if err := c.BodyParser(&req); err != nil {
			log.Printf("Ошибка парсинга JSON: %v", err)
			h.metrics.CreateShortLinkTotal.WithLabelValues("error").Inc()
			h.metrics.CreateShortLinkLatency.WithLabelValues("error").Observe(time.Since(start).Seconds())
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("некорректное тело JSON: %v", err),
			})
		}
		originalURL = req.URL
	} else {
		originalURL = strings.TrimSpace(string(c.Body()))
	}

	if originalURL == "" {
		h.metrics.CreateShortLinkTotal.WithLabelValues("error").Inc()
		h.metrics.CreateShortLinkLatency.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "URL обязателен",
		})
	}

	shortLink, err := h.shortenURL(originalURL)
	if err != nil {
		h.metrics.CreateShortLinkTotal.WithLabelValues("error").Inc()
		h.metrics.CreateShortLinkLatency.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	shortURL := fmt.Sprintf("%s/%s", BaseURL, shortLink)
	message := &ShortenMessage{OriginalURL: originalURL, ShortLink: shortLink}
	messageBytes, err := json.Marshal(message)
	if err != nil {
		log.Printf("Ошибка сериализации сообщения Kafka: %v", err)
		h.metrics.CreateShortLinkTotal.WithLabelValues("error").Inc()
		h.metrics.CreateShortLinkLatency.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("ошибка сериализации: %v", err),
		})
	}

	_, _, err = h.producer.SendMessage(&sarama.ProducerMessage{
		Topic: "shorten-urls",
		Value: sarama.ByteEncoder(messageBytes),
	})
	if err != nil {
		log.Printf("Ошибка отправки в Kafka: %v", err)
		h.metrics.CreateShortLinkTotal.WithLabelValues("error").Inc()
		h.metrics.CreateShortLinkLatency.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("ошибка отправки в Kafka: %v", err),
		})
	}
	log.Printf("Отправлено в Kafka: %s -> %s", originalURL, shortLink)

	h.metrics.CreateShortLinkTotal.WithLabelValues("success").Inc()
	h.metrics.CreateShortLinkLatency.WithLabelValues("success").Observe(time.Since(start).Seconds())
	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"message":  "Короткая ссылка создана",
		"shortURL": shortURL,
	})
}

// consumeShortenURLs обрабатывает сообщения из Kafka
func (h *Handler) consumeShortenURLs() {
	kafkaBrokers := strings.Split(os.Getenv("KAFKA_BROKER"), ",")
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	var consumerGroup sarama.ConsumerGroup
	var err error
	for i := 0; i < 10; i++ {
		consumerGroup, err = sarama.NewConsumerGroup(kafkaBrokers, "shorten-urls-group", config)
		if err == nil {
			break
		}
		log.Printf("Попытка %d: ошибка создания consumer group: %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Ошибка создания consumer group после 10 попыток: %v", err)
	}
	defer consumerGroup.Close()

	ctx := context.Background()
	for {
		err := consumerGroup.Consume(ctx, []string{"shorten-urls"}, h)
		if err != nil {
			log.Printf("Ошибка consumer group: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}

// Setup - реализация sarama.ConsumerGroupHandler
func (h *Handler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup - реализация sarama.ConsumerGroupHandler
func (h *Handler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim обрабатывает сообщения из Kafka с батч-вставкой
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := context.Background()
	batchSize := 100                 // Максимальный размер батча
	batchTimeout := 10 * time.Second // Максимальное время ожидания для батча
	batch := make([]ShortenMessage, 0, batchSize)
	batchChan := make(chan ShortenMessage, batchSize)
	var wg sync.WaitGroup

	// Горутина для пакетной вставки
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(batchTimeout)
		defer ticker.Stop()

		for {
			select {
			case msg, ok := <-batchChan:
				if !ok {
					if len(batch) > 0 {
						h.insertBatch(ctx, batch)
					}
					return
				}
				batch = append(batch, msg)
				if len(batch) >= batchSize {
					h.insertBatch(ctx, batch)
					batch = batch[:0]
					ticker.Reset(batchTimeout)
				}
			case <-ticker.C:
				if len(batch) > 0 {
					h.insertBatch(ctx, batch)
					batch = batch[:0]
				}
				ticker.Reset(batchTimeout)
			}
		}
	}()

	// Обработка сообщений из Kafka
	for message := range claim.Messages() {
		var shortenMsg ShortenMessage
		if err := json.Unmarshal(message.Value, &shortenMsg); err != nil {
			log.Printf("Ошибка десериализации сообщения Kafka: %v", err)
			h.metrics.DbInsertTotal.WithLabelValues("error").Inc()
			continue
		}

		log.Printf("Обработка сообщения Kafka: original_url=%s, short_link=%s", shortenMsg.OriginalURL, shortenMsg.ShortLink)
		batchChan <- shortenMsg
		session.MarkMessage(message, "")
	}

	// Закрываем канал и ждём завершения батч-вставки
	close(batchChan)
	wg.Wait()
	return nil
}

// insertBatch выполняет пакетную вставку в PostgreSQL
func (h *Handler) insertBatch(ctx context.Context, batch []ShortenMessage) {
	if len(batch) == 0 {
		return
	}

	tx, err := h.Db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Ошибка начала транзакции: %v", err)
		for range batch {
			h.metrics.DbInsertTotal.WithLabelValues("error").Inc()
		}
		return
	}

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO links (link, short_link, created_at) VALUES ($1, $2, NOW()) ON CONFLICT (link) DO NOTHING")
	if err != nil {
		log.Printf("Ошибка подготовки запроса: %v", err)
		tx.Rollback()
		for range batch {
			h.metrics.DbInsertTotal.WithLabelValues("error").Inc()
		}
		return
	}
	defer stmt.Close()

	rowsAffected := int64(0)
	for _, msg := range batch {
		result, err := stmt.ExecContext(ctx, msg.OriginalURL, msg.ShortLink)
		if err != nil {
			log.Printf("Ошибка вставки в БД: %v", err)
			h.metrics.DbInsertTotal.WithLabelValues("error").Inc()
			continue
		}
		if ra, _ := result.RowsAffected(); ra > 0 {
			rowsAffected++
			// Кэширование в Redis
			cacheKey := "shorten:" + msg.OriginalURL
			if err := h.redis.Set(ctx, cacheKey, msg.ShortLink, 10*time.Minute).Err(); err != nil {
				log.Printf("Ошибка записи в Redis (shorten): %v", err)
			} else {
				log.Printf("Закэшировано (shorten): %s -> %s с TTL 10 минут", msg.OriginalURL, msg.ShortLink)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Ошибка коммита транзакции: %v", err)
		tx.Rollback()
		for range batch {
			h.metrics.DbInsertTotal.WithLabelValues("error").Inc()
		}
		return
	}

	log.Printf("Вставлено в БД: %d строк из %d сообщений", rowsAffected, len(batch))
	for i := 0; i < int(rowsAffected); i++ {
		h.metrics.DbInsertTotal.WithLabelValues("success").Inc()
	}
	for i := int(rowsAffected); i < len(batch); i++ {
		h.metrics.DbInsertTotal.WithLabelValues("error").Inc()
	}
}

// cleanupOldLinks периодически удаляет записи старше 2 недель
func (h *Handler) cleanupOldLinks() {
	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			result, err := h.Db.ExecContext(ctx, "DELETE FROM links WHERE created_at < NOW() - INTERVAL '2 weeks'")
			if err != nil {
				log.Printf("Ошибка удаления старых записей: %v", err)
				h.metrics.CleanupTotal.WithLabelValues("error").Inc()
				continue
			}
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				log.Printf("Удалено %d старых записей из БД", rowsAffected)
				for i := 0; i < int(rowsAffected); i++ {
					h.metrics.CleanupTotal.WithLabelValues("success").Inc()
				}
			}
		}
	}
}

// redirect обрабатывает GET-запрос для редиректа
func (h *Handler) redirect(c *fiber.Ctx) error {
	shortLink := c.Params("key")
	ctx := context.Background()
	cacheKey := "redirect:" + shortLink
	if cachedURL, err := h.redis.Get(ctx, cacheKey).Result(); err == nil {
		log.Printf("Найдено в кэше (redirect): %s -> %s", shortLink, cachedURL)
		h.metrics.RedirectTotal.WithLabelValues("success").Inc()
		return c.Redirect(cachedURL, http.StatusMovedPermanently)
	} else if err != redis.Nil {
		log.Printf("Ошибка чтения из Redis (redirect): %v", err)
	}

	var originalURL string
	err := h.Db.QueryRow("SELECT link FROM links WHERE short_link = $1", shortLink).Scan(&originalURL)
	if err == sql.ErrNoRows {
		h.metrics.RedirectTotal.WithLabelValues("not_found").Inc()
		return c.Status(http.StatusNotFound).JSON(fiber.Map{
			"error": "Короткая ссылка не найдена",
		})
	}
	if err != nil {
		h.metrics.RedirectTotal.WithLabelValues("error").Inc()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Ошибка базы данных: %v", err),
		})
	}

	if err := h.redis.Set(ctx, cacheKey, originalURL, 10*time.Minute).Err(); err != nil {
		log.Printf("Ошибка записи в Redis (redirect): %v", err)
	} else {
		log.Printf("Закэшировано (redirect): %s -> %s с TTL 10 минут", shortLink, originalURL)
	}

	h.metrics.RedirectTotal.WithLabelValues("success").Inc()
	return c.Redirect(originalURL, http.StatusMovedPermanently)
}
