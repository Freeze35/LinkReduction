package handler

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"github.com/gofiber/fiber/v2"
	_ "github.com/lib/pq"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// InitRoutes настраивает маршруты для приложения Fiber
func (h *Handler) InitRoutes(app *fiber.App) {
	app.Post("/createShortLink", h.createShortLink)
	app.Get("/:key", h.redirect)
}

// BaseURL - базовый домен для коротких ссылок (задаётся через переменную окружения)
var BaseURL = os.Getenv("BASE_URL")

// Handler - структура для хранения подключения к базе данных
type Handler struct {
	Db *sql.DB
}

// NewHandler создаёт новый экземпляр Handler и открывает соединение с PostgreSQL
func NewHandler(dbURL string) (*Handler, error) {
	// Подключение к PostgreSQL
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия базы данных: %v", err)
	}

	// Проверка доступности базы данных
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("база данных недоступна: %v", err)
	}

	// Проверка, что BaseURL задан
	if BaseURL == "" {
		return nil, fmt.Errorf("BaseURL пуст")
	}

	return &Handler{Db: db}, nil
}

// ShortenRequest - структура для парсинга JSON-запроса
type ShortenRequest struct {
	URL string `json:"url"` // Поле для входного URL
}

// generateShortLink генерирует короткий ключ фиксированной длины (6 символов) на основе URL
func generateShortLink(originalURL string) string {
	// Используем MD5 хэш от URL для генерации ключа
	hash := md5.Sum([]byte(originalURL))
	// Берём первые 6 символов хэша в hex-формате
	return fmt.Sprintf("%x", hash)[:6]
}

// shortenURL проверяет входной URL, ищет его в БД или создаёт новую короткую ссылку
func (h *Handler) shortenURL(originalURL string) (string, error) {
	// Проверка, начинается ли URL с http:// или https://
	if !strings.HasPrefix(originalURL, "http://") && !strings.HasPrefix(originalURL, "https://") {
		return "", fmt.Errorf("некорректный URL: должен начинаться с http:// или https://")
	}

	// Парсинг и валидация URL
	parsedURL, err := url.Parse(originalURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return "", fmt.Errorf("некорректный формат URL")
	}

	// Проверка, существует ли URL в базе данных
	var shortLink string
	err = h.Db.QueryRow("SELECT short_link FROM links WHERE link = $1", originalURL).Scan(&shortLink)
	if err == nil {
		// URL уже существует, возвращаем существующую короткую ссылку
		return fmt.Sprintf("%s/%s", BaseURL, shortLink), nil
	}
	if err != sql.ErrNoRows {
		return "", fmt.Errorf("ошибка проверки URL в базе данных: %v", err)
	}

	// Попытки генерации уникального ключа
	for i := 0; i < 3; i++ {
		// Генерация нового короткого ключа с суффиксом
		inputURL := originalURL
		if i > 0 {
			inputURL = fmt.Sprintf("%s_%d", originalURL, i)
		}
		shortLink = generateShortLink(inputURL)

		// Проверка, существует ли ключ в базе данных
		var exists string
		err = h.Db.QueryRow("SELECT short_link FROM links WHERE short_link = $1", shortLink).Scan(&exists)
		if err == sql.ErrNoRows {
			// Ключ уникален, можно использовать
			break
		}
		if err != nil {
			return "", fmt.Errorf("ошибка проверки ключа: %v", err)
		}
		if i == 2 {
			return "", fmt.Errorf("не удалось сгенерировать уникальный ключ после %d попыток", i+1)
		}
	}

	// Сохранение в базу данных
	_, err = h.Db.Exec("INSERT INTO links (link, short_link) VALUES ($1, $2)", originalURL, shortLink)
	if err != nil {
		return "", fmt.Errorf("ошибка сохранения в базу данных: %v", err)
	}

	// Формирование короткой ссылки с использованием BaseURL
	shortURL := fmt.Sprintf("%s/%s", BaseURL, shortLink)
	return shortURL, nil
}

// createShortLink обрабатывает POST-запрос для создания короткой ссылки
func (h *Handler) createShortLink(c *fiber.Ctx) error {
	var req ShortenRequest
	// Парсинг JSON-тела запроса
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "Некорректное тело запроса",
		})
	}

	// Проверка, что URL не пустой
	if req.URL == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "URL обязателен",
		})
	}

	// Создание или получение короткой ссылки
	shortURL, err := h.shortenURL(req.URL)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	// Возврат успешного ответа с короткой ссылкой
	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"message":  "Короткая ссылка создана",
		"shortURL": shortURL,
	})
}

// redirect обрабатывает GET-запрос для редиректа по короткой ссылке
func (h *Handler) redirect(c *fiber.Ctx) error {
	shortLink := c.Params("key")
	var originalURL string

	// Поиск оригинального URL в базе данных
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

	// Перенаправление на оригинальный URL
	return c.Redirect(originalURL, http.StatusMovedPermanently)
}
