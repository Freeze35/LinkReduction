package handler

import (
	_ "context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/prometheus"
	"linkreduction/internal/service"
	"net/http"
	"os"
	"strings"
	"time"
)

// BaseURL - базовый домен для коротких ссылок
var BaseURL = os.Getenv("BASE_URL")

// Handler - структура для обработки HTTP-запросов
type Handler struct {
	service  *service.Service
	metrics  *initprometheus.PrometheusMetrics
	logger   *logrus.Logger
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
func NewHandler(service *service.Service, metrics *initprometheus.PrometheusMetrics, logger *logrus.Logger) (*Handler, error) {

	return &Handler{
		service: service,
		metrics: metrics,
		logger:  logger,
	}, nil
}

// InitRoutes настраивает маршруты
func (h *Handler) InitRoutes(app *fiber.App) {
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))
	app.Post("/createShortLink", h.createShortLink)
	app.Get("/:key", h.redirect)
}

// createShortLink обрабатывает POST-запрос для создания короткой ссылки
func (h *Handler) createShortLink(c *fiber.Ctx) error {

	start := time.Now()

	var originalURL string
	if c.Get("Content-Type") == "application/json" {
		var req ShortenRequest
		if err := c.BodyParser(&req); err != nil {

			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "json_parse").Inc()

			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("некорректное тело JSON: %v", err),
			})
		}
		originalURL = req.URL
	} else {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "Неверный Content-Type != application/json",
		})
	}

	if originalURL == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "URL обязателен",
		})
	}

	ctx := c.Context()
	shortLink, err := h.service.ShortenURL(ctx, originalURL)
	if err != nil {
		errorType := "url_validation"
		if strings.Contains(err.Error(), "база данных") {
			errorType = "db_query"
		} else if strings.Contains(err.Error(), "уникальный ключ") {
			errorType = "key_generation"
		}

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

			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_serialization").Inc()

			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка сериализации: %v", err),
			})
		}

		_, _, err = h.producer.SendMessage(&sarama.ProducerMessage{
			Topic: "shorten-urls",
			Value: sarama.ByteEncoder(messageBytes),
		})
		if err != nil {

			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "kafka_send").Inc()

			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка отправки в Kafka: %v", err),
			})
		}

	} else {
		// Если Kafka недоступна, вставляем напрямую
		if ok, err := h.service.InsertLink(ctx, originalURL, shortLink); err != nil {

			h.metrics.CreateShortLinkTotal.WithLabelValues("error", "db_insert").Inc()

			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ошибка вставки в БД: %v", err),
			})
		} else if ok {
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

// redirect обрабатывает GET-запрос для редиректа
func (h *Handler) redirect(c *fiber.Ctx) error {

	shortLink := c.Params("key")
	ctx := c.Context()

	originalURL, err := h.service.GetOriginalURL(ctx, shortLink)
	if err != nil {

		h.metrics.RedirectTotal.WithLabelValues("error", "db_query").Inc()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("ошибка базы данных: %v", err),
		})
	}
	if originalURL == "" {

		h.metrics.RedirectTotal.WithLabelValues("not_found", "none").Inc()
		return c.Status(http.StatusNotFound).JSON(fiber.Map{
			"error": "Короткая ссылка не найдена",
		})
	}

	h.metrics.RedirectTotal.WithLabelValues("success", "none").Inc()
	return c.Redirect(originalURL, http.StatusMovedPermanently)
}
