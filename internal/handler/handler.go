package handler

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// InitRoutes настраивает маршруты
func (h *Handler) InitRoutes(app *fiber.App) {
	app.Post("/createShortLink", h.createShortLink)
	app.Get("/:key", h.redirect)
	go h.consumeShortenURLs()
}

// BaseURL - базовый домен для коротких ссылок
var BaseURL = os.Getenv("BASE_URL")

// Handler - структура для хранения подключения к базе данных, Redis и Kafka
type Handler struct {
	Db       *sql.DB
	redis    *redis.Client
	producer sarama.SyncProducer
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
func NewHandler(dbURL string) (*Handler, error) {
	// Подключение к PostgreSQL
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия базы данных: %v", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("база данных недоступна: %v", err)
	}

	// Проверка BaseURL
	if BaseURL == "" {
		return nil, fmt.Errorf("BaseURL пуст")
	}

	// Подключение к Redis
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return nil, fmt.Errorf("REDIS_URL пуст")
	}
	log.Printf("Подключение к Redis: %s", redisURL)
	redisClient := redis.NewClient(&redis.Options{Addr: redisURL})
	ctx := context.Background()
	if _, err = redisClient.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("Redis недоступен: %v", err)
	}
	log.Printf("Redis успешно подключён")

	// Подключение к Kafka
	kafkaBrokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	log.Printf("KAFKA_BROKERS value: %v", os.Getenv("KAFKA_BROKERS"))
	if len(kafkaBrokers) == 0 || kafkaBrokers[0] == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS пуст")
	}
	log.Printf("Подключение к Kafka: %v", kafkaBrokers)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	var producer sarama.SyncProducer
	for i := 0; i < 10; i++ {
		producer, err = sarama.NewSyncProducer(kafkaBrokers, config)
		if err == nil {
			break
		}
		log.Printf("Попытка %d: ошибка подключения к Kafka: %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к Kafka после 10 попыток: %v", err)
	}
	log.Printf("Kafka успешно подключён")

	return &Handler{
		Db:       db,
		redis:    redisClient,
		producer: producer,
	}, nil
}

// Close закрывает соединения
func (h *Handler) Close() {
	if h.Db != nil {
		h.Db.Close()
	}
	if h.redis != nil {
		h.redis.Close()
	}
	if h.producer != nil {
		h.producer.Close()
	}
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
	log.Printf("Получен POST-запрос на /createShortLink")
	log.Printf("Тело запроса: %s", c.Body())
	log.Printf("Заголовки запроса: %v", c.GetReqHeaders())

	var originalURL string
	if c.Get("Content-Type") == "application/json" {
		var req ShortenRequest
		if err := c.BodyParser(&req); err != nil {
			log.Printf("Ошибка парсинга JSON: %v", err)
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("некорректное тело JSON: %v", err),
			})
		}
		originalURL = req.URL
	} else {
		originalURL = strings.TrimSpace(string(c.Body()))
	}

	if originalURL == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "URL обязателен",
		})
	}

	shortLink, err := h.shortenURL(originalURL)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	shortURL := fmt.Sprintf("%s/%s", BaseURL, shortLink)
	message := &ShortenMessage{OriginalURL: originalURL, ShortLink: shortLink}
	messageBytes, err := json.Marshal(message)
	if err != nil {
		log.Printf("Ошибка сериализации сообщения Kafka: %v", err)
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
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("ошибка отправки в Kafka: %v", err),
		})
	}
	log.Printf("Отправлено в Kafka: %s -> %s", originalURL, shortLink)

	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"message":  "Короткая ссылка создана",
		"shortURL": shortURL,
	})
}

// consumeShortenURLs обрабатывает сообщения из Kafka
func (h *Handler) consumeShortenURLs() {
	kafkaBrokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
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

// ConsumeClaim - обработка сообщений из Kafka
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := context.Background()
	for message := range claim.Messages() {
		var shortenMsg ShortenMessage
		if err := json.Unmarshal(message.Value, &shortenMsg); err != nil {
			log.Printf("Ошибка десериализации сообщения Kafka: %v", err)
			continue
		}

		log.Printf("Обработка сообщения Kafka: original_url=%s, short_link=%s", shortenMsg.OriginalURL, shortenMsg.ShortLink)
		result, err := h.Db.Exec("INSERT INTO links (link, short_link) VALUES ($1, $2) ON CONFLICT (link) DO NOTHING", shortenMsg.OriginalURL, shortenMsg.ShortLink)
		if err != nil {
			log.Printf("Ошибка сохранения в базу данных: %v", err)
			continue
		}
		rowsAffected, _ := result.RowsAffected()
		log.Printf("Вставлено в БД: %d строк, link=%s, short_link=%s", rowsAffected, shortenMsg.OriginalURL, shortenMsg.ShortLink)

		cacheKey := "shorten:" + shortenMsg.OriginalURL
		if err := h.redis.Set(ctx, cacheKey, shortenMsg.ShortLink, 10*time.Minute).Err(); err != nil {
			log.Printf("Ошибка записи в Redis (shorten): %v", err)
		} else {
			log.Printf("Закэшировано (shorten): %s -> %s с TTL 10 минут", shortenMsg.OriginalURL, shortenMsg.ShortLink)
		}

		session.MarkMessage(message, "")
	}
	return nil
}

// redirect обрабатывает GET-запрос для редиректа
func (h *Handler) redirect(c *fiber.Ctx) error {
	shortLink := c.Params("key")
	ctx := context.Background()
	cacheKey := "redirect:" + shortLink
	if cachedURL, err := h.redis.Get(ctx, cacheKey).Result(); err == nil {
		log.Printf("Найдено в кэше (redirect): %s -> %s", shortLink, cachedURL)
		return c.Redirect(cachedURL, http.StatusMovedPermanently)
	} else if err != redis.Nil {
		log.Printf("Ошибка чтения из Redis (redirect): %v", err)
	}

	var originalURL string
	err := h.Db.QueryRow("SELECT link FROM links WHERE short_link = $1", shortLink).Scan(&originalURL)
	if err == sql.ErrNoRows {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{
			"error": "Короткая ссылка не найдена",
		})
	}
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Ошибка базы данных: %v", err),
		})
	}

	if err := h.redis.Set(ctx, cacheKey, originalURL, 10*time.Minute).Err(); err != nil {
		log.Printf("Ошибка записи в Redis (redirect): %v", err)
	} else {
		log.Printf("Закэшировано (redirect): %s -> %s с TTL 10 минут", shortLink, originalURL)
	}

	return c.Redirect(originalURL, http.StatusMovedPermanently)
}
