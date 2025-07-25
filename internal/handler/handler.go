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
	"github.com/sirupsen/logrus"
	_ "linkreduction/internal/prometheus"
	initprometheus "linkreduction/internal/prometheus"
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
	if h.producer != nil {
		go h.consumeShortenURLs()
	}
	go h.cleanupOldLinks() // Запуск горутины для очистки старых записей
}

// BaseURL - базовый домен для коротких ссылок
var BaseURL = os.Getenv("BASE_URL")

// Handler - структура для хранения подключения к базе данных, Redis, Kafka, метрик Prometheus и логгера
type Handler struct {
	Db       *sql.DB
	redis    *redis.Client
	producer sarama.SyncProducer
	metrics  *initprometheus.PrometheusMetrics
	logger   *logrus.Logger
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
	prometheus *initprometheus.PrometheusMetrics, logger *logrus.Logger) (*Handler, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection must not be nil")
	}
	if redisClient == nil {
		return nil, fmt.Errorf("redis client must not be nil")
	}
	if prometheus == nil {
		return nil, fmt.Errorf("prometheus metrics must not be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger must not be nil")
	}
	if BaseURL == "" {
		return nil, fmt.Errorf("BaseURL пуст")
	}

	return &Handler{
		Db:       db,
		redis:    redisClient,
		producer: producer,
		metrics:  prometheus,
		logger:   logger,
	}, nil
}

// generateShortLink генерирует короткий ключ (6 символов)
func generateShortLink(originalURL string) string {
	hash := md5.Sum([]byte(originalURL))
	return fmt.Sprintf("%x", hash)[:6]
}

// shortenURL проверяет URL, ищет в кэше/БД или генерирует новый ключ
func (h *Handler) shortenURL(originalURL string) (string, error) {
	logger := h.logger.WithField("component", "handler")
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
		logger.WithFields(logrus.Fields{
			"original_url": originalURL,
			"short_link":   cachedShortLink,
		}).Info("Найдено в кэше (shorten)")
		return cachedShortLink, nil
	} else if err != redis.Nil {
		logger.WithFields(logrus.Fields{
			"cache_key": cacheKey,
			"error":     err,
		}).Error("Ошибка чтения из Redis (shorten)")
	}

	var shortLink string
	err = h.Db.QueryRow("SELECT short_link FROM links WHERE link = $1", originalURL).Scan(&shortLink)
	if err == nil {
		if err := h.redis.Set(ctx, cacheKey, shortLink, 10*time.Minute).Err(); err != nil {
			logger.WithFields(logrus.Fields{
				"cache_key": cacheKey,
				"error":     err,
			}).Error("Ошибка записи в Redis (shorten)")
		} else {
			logger.WithFields(logrus.Fields{
				"original_url": originalURL,
				"short_link":   shortLink,
			}).Info("Закэшировано (shorten) с TTL 10 минут")
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
	logger := h.logger.WithField("component", "handler")
	start := time.Now()
	logger.Info("Получен POST-запрос на /createShortLink")
	logger.WithField("body", string(c.Body())).Debug("Тело запроса")
	logger.WithField("headers", c.GetReqHeaders()).Debug("Заголовки запроса")

	var originalURL string
	if c.Get("Content-Type") == "application/json" {
		var req ShortenRequest
		if err := c.BodyParser(&req); err != nil {
			logger.WithField("error", err).Error("Ошибка парсинга JSON")
			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "json_parse").Inc()
			h.metrics.CreateShortLinkLatency.WithLabelValues("error", "json_parse").Observe(time.Since(start).Seconds())
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("некорректное тело JSON: %v", err),
			})
		}
		originalURL = req.URL
	} else {
		originalURL = strings.TrimSpace(string(c.Body()))
	}

	if originalURL == "" {
		logger.Error("URL обязателен")
		h.metrics.CreateShortLinkTotal.WithLabelValues("error", "empty_url").Inc()
		h.metrics.CreateShortLinkLatency.WithLabelValues("error", "empty_url").Observe(time.Since(start).Seconds())
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "URL обязателен",
		})
	}

	shortLink, err := h.shortenURL(originalURL)
	if err != nil {
		errorType := "url_validation"
		if strings.Contains(err.Error(), "база данных") {
			errorType = "db_query"
		} else if strings.Contains(err.Error(), "уникальный ключ") {
			errorType = "key_generation"
		}
		logger.WithFields(logrus.Fields{
			"original_url": originalURL,
			"error":        err,
		}).Error("Ошибка создания короткой ссылки")
		h.metrics.CreateShortLinkTotal.WithLabelValues("error", errorType).Inc()
		h.metrics.CreateShortLinkLatency.WithLabelValues("error", errorType).Observe(time.Since(start).Seconds())
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	shortURL := fmt.Sprintf("%s/%s", BaseURL, shortLink)

	// Если Kafka доступна, отправляем сообщение
	if h.producer != nil {
		message := &ShortenMessage{OriginalURL: originalURL, ShortLink: shortLink}
		messageBytes, err := json.Marshal(message)
		if err != nil {
			logger.WithField("error", err).Error("Ошибка сериализации сообщения Kafka")
			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_serialization").Inc()
			h.metrics.CreateShortLinkLatency.WithLabelValues("error", "kafka_serialization").Observe(time.Since(start).Seconds())
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка сериализации: %v", err),
			})
		}

		_, _, err = h.producer.SendMessage(&sarama.ProducerMessage{
			Topic: "shorten-urls",
			Value: sarama.ByteEncoder(messageBytes),
		})
		if err != nil {
			logger.WithField("error", err).Error("Ошибка отправки в Kafka")
			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_send").Inc()
			h.metrics.CreateShortLinkLatency.WithLabelValues("error", "kafka_send").Observe(time.Since(start).Seconds())
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка отправки в Kafka: %v", err),
			})
		}
		logger.WithFields(logrus.Fields{
			"original_url": originalURL,
			"short_link":   shortLink,
		}).Info("Отправлено в Kafka")
	} else {
		// Если Kafka недоступна, вставляем напрямую в БД
		ctx := context.Background()
		tx, err := h.Db.BeginTx(ctx, nil)
		if err != nil {
			logger.WithField("error", err).Error("Ошибка начала транзакции")
			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "db_transaction").Inc()
			h.metrics.CreateShortLinkLatency.WithLabelValues("error", "db_transaction").Observe(time.Since(start).Seconds())
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка начала транзакции: %v", err),
			})
		}

		result, err := tx.ExecContext(ctx, "INSERT INTO links (link, short_link, created_at) VALUES ($1, $2, NOW()) ON CONFLICT (link) DO NOTHING", originalURL, shortLink)
		if err != nil {
			tx.Rollback()
			logger.WithField("error", err).Error("Ошибка вставки в БД")
			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "db_insert").Inc()
			h.metrics.CreateShortLinkLatency.WithLabelValues("error", "db_insert").Observe(time.Since(start).Seconds())
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка вставки в БД: %v", err),
			})
		}

		if err := tx.Commit(); err != nil {
			logger.WithField("error", err).Error("Ошибка коммита транзакции")
			tx.Rollback()
			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "db_commit").Inc()
			h.metrics.CreateShortLinkLatency.WithLabelValues("error", "db_commit").Observe(time.Since(start).Seconds())
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка коммита транзакции: %v", err),
			})
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			cacheKey := "shorten:" + originalURL
			if err := h.redis.Set(ctx, cacheKey, shortLink, 10*time.Minute).Err(); err != nil {
				logger.WithFields(logrus.Fields{
					"cache_key": cacheKey,
					"error":     err,
				}).Error("Ошибка записи в Redis (shorten)")
			} else {
				logger.WithFields(logrus.Fields{
					"original_url": originalURL,
					"short_link":   shortLink,
				}).Info("Закэшировано (shorten) с TTL 10 минут")
			}
			logger.WithFields(logrus.Fields{
				"original_url": originalURL,
				"short_link":   shortLink,
			}).Info("Вставлено в БД")
			h.metrics.DbInsertTotal.WithLabelValues("success", "none").Inc()
		} else {
			h.metrics.DbInsertTotal.WithLabelValues("error", "no_rows_affected").Inc()
		}
	}

	h.metrics.CreateShortLinkTotal.WithLabelValues("success", "none").Inc()
	h.metrics.CreateShortLinkLatency.WithLabelValues("success", "none").Observe(time.Since(start).Seconds())
	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"message":  "Короткая ссылка создана",
		"shortURL": shortURL,
	})
}

// consumeShortenURLs обрабатывает сообщения из Kafka
func (h *Handler) consumeShortenURLs() {
	logger := h.logger.WithField("component", "handler")
	kafkaEnv := os.Getenv("KAFKA_BROKERS")
	kafkaBrokers := strings.Split(kafkaEnv, ",")

	logger.WithField("kafka_brokers", kafkaEnv).Info("Проверка переменной окружения KAFKA_BROKERS")
	if len(kafkaBrokers) == 0 || kafkaBrokers[0] == "" {
		logger.Info("Переменная окружения KAFKA_BROKERS пуста или не задана, пропуск создания consumer group")
		return
	}

	for _, broker := range kafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			logger.Info("Обнаружен пустой адрес брокера Kafka, пропуск создания consumer group")
			return
		}
	}

	logger.WithField("kafka_brokers", kafkaBrokers).Info("Создание consumer group для Kafka")
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
		logger.WithFields(logrus.Fields{
			"attempt": i + 1,
			"error":   err,
		}).Error("Ошибка создания consumer group")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		logger.WithField("error", err).Warn("Ошибка создания consumer group после 10 попыток")
		return
	}
	defer consumerGroup.Close()

	logger.Info("Consumer group успешно создан")
	ctx := context.Background()
	for {
		err := consumerGroup.Consume(ctx, []string{"shorten-urls"}, h)
		if err != nil {
			logger.WithField("error", err).Error("Ошибка consumer group")
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
	logger := h.logger.WithField("component", "handler")
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
			logger.WithField("error", err).Error("Ошибка десериализации сообщения Kafka")
			h.metrics.DbInsertTotal.WithLabelValues("error", "kafka_deserialization").Inc()
			continue
		}

		logger.WithFields(logrus.Fields{
			"original_url": shortenMsg.OriginalURL,
			"short_link":   shortenMsg.ShortLink,
		}).Info("Обработка сообщения Kafka")
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
	logger := h.logger.WithField("component", "handler")
	if len(batch) == 0 {
		return
	}

	tx, err := h.Db.BeginTx(ctx, nil)
	if err != nil {
		logger.WithField("error", err).Error("Ошибка начала транзакции")
		for range batch {
			h.metrics.DbInsertTotal.WithLabelValues("error", "db_transaction").Inc()
		}
		return
	}

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO links (link, short_link, created_at) VALUES ($1, $2, NOW()) ON CONFLICT (link) DO NOTHING")
	if err != nil {
		logger.WithField("error", err).Error("Ошибка подготовки запроса")
		tx.Rollback()
		for range batch {
			h.metrics.DbInsertTotal.WithLabelValues("error", "db_prepare").Inc()
		}
		return
	}
	defer stmt.Close()

	rowsAffected := int64(0)
	for _, msg := range batch {
		result, err := stmt.ExecContext(ctx, msg.OriginalURL, msg.ShortLink)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"original_url": msg.OriginalURL,
				"error":        err,
			}).Error("Ошибка вставки в БД")
			h.metrics.DbInsertTotal.WithLabelValues("error", "db_insert").Inc()
			continue
		}
		if ra, _ := result.RowsAffected(); ra > 0 {
			rowsAffected++
			cacheKey := "shorten:" + msg.OriginalURL
			if err := h.redis.Set(ctx, cacheKey, msg.ShortLink, 10*time.Minute).Err(); err != nil {
				logger.WithFields(logrus.Fields{
					"cache_key": cacheKey,
					"error":     err,
				}).Error("Ошибка записи в Redis (shorten)")
			} else {
				logger.WithFields(logrus.Fields{
					"original_url": msg.OriginalURL,
					"short_link":   msg.ShortLink,
				}).Info("Закэшировано (shorten) с TTL 10 минут")
			}
		}
	}

	if err := tx.Commit(); err != nil {
		logger.WithField("error", err).Error("Ошибка коммита транзакции")
		tx.Rollback()
		for range batch {
			h.metrics.DbInsertTotal.WithLabelValues("error", "db_commit").Inc()
		}
		return
	}

	logger.WithFields(logrus.Fields{
		"rows_affected": rowsAffected,
		"batch_size":    len(batch),
	}).Info("Вставлено в БД")
	for i := 0; i < int(rowsAffected); i++ {
		h.metrics.DbInsertTotal.WithLabelValues("success", "none").Inc()
	}
	for i := int(rowsAffected); i < len(batch); i++ {
		h.metrics.DbInsertTotal.WithLabelValues("error", "no_rows_affected").Inc()
	}
}

// cleanupOldLinks периодически удаляет записи старше 2 недель
func (h *Handler) cleanupOldLinks() {
	logger := h.logger.WithField("component", "handler")
	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			result, err := h.Db.ExecContext(ctx, "DELETE FROM links WHERE created_at < NOW() - INTERVAL '2 weeks'")
			if err != nil {
				logger.WithField("error", err).Error("Ошибка удаления старых записей")
				h.metrics.CleanupTotal.WithLabelValues("error", "db_delete").Inc()
				continue
			}
			rowsAffected, _ := result.RowsAffected()
			if rowsAffected > 0 {
				logger.WithField("rows_affected", rowsAffected).Info("Удалено старых записей из БД")
				for i := 0; i < int(rowsAffected); i++ {
					h.metrics.CleanupTotal.WithLabelValues("success", "none").Inc()
				}
			}
		}
	}
}

// redirect обрабатывает GET-запрос для редиректа
func (h *Handler) redirect(c *fiber.Ctx) error {
	logger := h.logger.WithField("component", "handler")
	shortLink := c.Params("key")
	ctx := context.Background()
	cacheKey := "redirect:" + shortLink
	if cachedURL, err := h.redis.Get(ctx, cacheKey).Result(); err == nil {
		logger.WithFields(logrus.Fields{
			"short_link": shortLink,
			"cached_url": cachedURL,
		}).Info("Найдено в кэше (redirect)")
		h.metrics.RedirectTotal.WithLabelValues("success", "none").Inc()
		return c.Redirect(cachedURL, http.StatusMovedPermanently)
	} else if err != redis.Nil {
		logger.WithFields(logrus.Fields{
			"cache_key": cacheKey,
			"error":     err,
		}).Error("Ошибка чтения из Redis (redirect)")
	}

	var originalURL string
	err := h.Db.QueryRow("SELECT link FROM links WHERE short_link = $1", shortLink).Scan(&originalURL)
	if err == sql.ErrNoRows {
		logger.WithField("short_link", shortLink).Warn("Короткая ссылка не найдена")
		h.metrics.RedirectTotal.WithLabelValues("not_found", "none").Inc()
		return c.Status(http.StatusNotFound).JSON(fiber.Map{
			"error": "Короткая ссылка не найдена",
		})
	}
	if err != nil {
		logger.WithFields(logrus.Fields{
			"short_link": shortLink,
			"error":      err,
		}).Error("Ошибка базы данных")
		h.metrics.RedirectTotal.WithLabelValues("error", "db_query").Inc()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Ошибка базы данных: %v", err),
		})
	}

	if err := h.redis.Set(ctx, cacheKey, originalURL, 10*time.Minute).Err(); err != nil {
		logger.WithFields(logrus.Fields{
			"cache_key": cacheKey,
			"error":     err,
		}).Error("Ошибка записи в Redis (redirect)")
	} else {
		logger.WithFields(logrus.Fields{
			"short_link":   shortLink,
			"original_url": originalURL,
		}).Info("Закэшировано (redirect) с TTL 10 минут")
	}

	h.metrics.RedirectTotal.WithLabelValues("success", "none").Inc()
	return c.Redirect(originalURL, http.StatusMovedPermanently)
}
