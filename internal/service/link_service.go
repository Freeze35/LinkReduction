package service

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/repository"
	"net/url"
	"strings"
)

// LinkService - сервис для работы с сокращением ссылок
type LinkService struct {
	repo   repository.LinkRepository
	cache  repository.Cache
	logger *logrus.Logger
}

// NewLinkService создаёт новый экземпляр LinkService
func NewLinkService(repo repository.LinkRepository, cache repository.Cache, logger *logrus.Logger) *LinkService {
	return &LinkService{repo: repo, cache: cache, logger: logger}
}

// ShortenURL проверяет URL, ищет в кэше/БД или генерирует новый ключ
func (s *LinkService) ShortenURL(ctx context.Context, originalURL string) (string, error) {
	logger := s.logger.WithField("component", "service")
	if !strings.HasPrefix(originalURL, "http://") && !strings.HasPrefix(originalURL, "https://") {
		return "", fmt.Errorf("некорректный URL: должен начинаться с http:// или https://")
	}
	parsedURL, err := url.Parse(originalURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return "", fmt.Errorf("некорректный формат URL")
	}

	// Проверка в кэше
	if cachedShortLink, err := s.cache.GetShortLink(ctx, originalURL); err != nil {
		return "", fmt.Errorf("ошибка чтения из кэша: %v", err)
	} else if cachedShortLink != "" {
		logger.WithFields(logrus.Fields{
			"original_url": originalURL,
			"short_link":   cachedShortLink,
		}).Info("Найдено в кэше (shorten)")
		return cachedShortLink, nil
	}

	// Проверка в БД
	if shortLink, err := s.repo.FindByOriginalURL(ctx, originalURL); err != nil {
		return "", fmt.Errorf("ошибка проверки URL в базе данных: %v", err)
	} else if shortLink != "" {
		if err := s.cache.SetShortLink(ctx, originalURL, shortLink, 10*60); err != nil {
			logger.WithFields(logrus.Fields{
				"original_url": originalURL,
				"short_link":   shortLink,
				"error":        err,
			}).Error("Ошибка записи в кэш")
		}
		return shortLink, nil
	}

	// Генерация нового ключа
	for i := 0; i < 3; i++ {
		inputURL := originalURL
		if i > 0 {
			inputURL = fmt.Sprintf("%s_%d", originalURL, i)
		}
		shortLink := generateShortLink(inputURL)
		if exists, err := s.repo.FindByShortLink(ctx, shortLink); err != nil {
			return "", fmt.Errorf("ошибка проверки ключа: %v", err)
		} else if exists == "" {
			return shortLink, nil
		}
		if i == 2 {
			return "", fmt.Errorf("не удалось сгенерировать уникальный ключ после %d попыток", i+1)
		}
	}

	return "", fmt.Errorf("не удалось сгенерировать короткую ссылку")
}

// InsertLink вставляет новую ссылку в хранилище
func (s *LinkService) InsertLink(ctx context.Context, originalURL, shortLink string) (bool, error) {
	ok, err := s.repo.Insert(ctx, originalURL, shortLink)
	if err != nil {
		return false, err
	}
	if ok {
		if err := s.cache.SetShortLink(ctx, originalURL, shortLink, 10*60); err != nil {
			s.logger.WithFields(logrus.Fields{
				"component":    "service",
				"original_url": originalURL,
				"short_link":   shortLink,
				"error":        err,
			}).Error("Ошибка записи в кэш")
		}
	}
	return ok, nil
}

// GetOriginalURL получает оригинальный URL по короткой ссылке
func (s *LinkService) GetOriginalURL(ctx context.Context, shortLink string) (string, error) {
	// Проверка в кэше
	if cachedURL, err := s.cache.GetOriginalURL(ctx, shortLink); err != nil {
		return "", fmt.Errorf("ошибка чтения из кэша: %v", err)
	} else if cachedURL != "" {
		s.logger.WithFields(logrus.Fields{
			"component":  "service",
			"short_link": shortLink,
			"cached_url": cachedURL,
		}).Info("Найдено в кэше (redirect)")
		return cachedURL, nil
	}

	// Проверка в БД
	originalURL, err := s.repo.FindByShortLink(ctx, shortLink)
	if err != nil {
		return "", fmt.Errorf("ошибка базы данных: %v", err)
	}
	if originalURL == "" {
		return "", nil
	}

	// Кэширование результата
	if err := s.cache.SetOriginalURL(ctx, shortLink, originalURL, 10*60); err != nil {
		s.logger.WithFields(logrus.Fields{
			"component":    "service",
			"short_link":   shortLink,
			"original_url": originalURL,
			"error":        err,
		}).Error("Ошибка записи в кэш")
	}

	return originalURL, nil
}

// generateShortLink генерирует короткий ключ (6 символов)
func generateShortLink(originalURL string) string {
	hash := md5.Sum([]byte(originalURL))
	return fmt.Sprintf("%x", hash)[:6]
}
