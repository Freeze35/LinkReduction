package repository

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"time"
)

// Cache - интерфейс для работы с кэшем
type Cache interface {
	GetShortLink(ctx context.Context, originalURL string) (string, error)
	SetShortLink(ctx context.Context, originalURL, shortLink string, ttl time.Duration) error
	GetOriginalURL(ctx context.Context, shortLink string) (string, error)
	SetOriginalURL(ctx context.Context, shortLink, originalURL string, ttl time.Duration) error
}

// RedisCache - реализация Cache для Redis
type RedisCache struct {
	client *redis.Client
	logger *logrus.Logger
}

// NewRedisCache создаёт новый экземпляр RedisCache
func NewRedisCache(client *redis.Client, logger *logrus.Logger) *RedisCache {
	return &RedisCache{client: client, logger: logger}
}

// GetShortLink получает короткую ссылку из кэша
func (c *RedisCache) GetShortLink(ctx context.Context, originalURL string) (string, error) {
	cacheKey := "shorten:" + originalURL
	result, err := c.client.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"component": "repository",
			"cache_key": cacheKey,
			"error":     err,
		}).Error("Ошибка чтения из Redis (shorten)")
		return "", err
	}
	return result, nil
}

// SetShortLink сохраняет короткую ссылку в кэш
func (c *RedisCache) SetShortLink(ctx context.Context, originalURL, shortLink string, ttl time.Duration) error {
	cacheKey := "shorten:" + originalURL
	if err := c.client.Set(ctx, cacheKey, shortLink, ttl).Err(); err != nil {
		c.logger.WithFields(logrus.Fields{
			"component": "repository",
			"cache_key": cacheKey,
			"error":     err,
		}).Error("Ошибка записи в Redis (shorten)")
		return err
	}
	c.logger.WithFields(logrus.Fields{
		"component":    "repository",
		"original_url": originalURL,
		"short_link":   shortLink,
	}).Info("Закэшировано (shorten) с TTL 10 минут")
	return nil
}

// GetOriginalURL получает оригинальный URL из кэша
func (c *RedisCache) GetOriginalURL(ctx context.Context, shortLink string) (string, error) {
	cacheKey := "redirect:" + shortLink
	result, err := c.client.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		c.logger.WithFields(logrus.Fields{
			"component": "repository",
			"cache_key": cacheKey,
			"error":     err,
		}).Error("Ошибка чтения из Redis (redirect)")
		return "", err
	}
	return result, nil
}

// SetOriginalURL сохраняет оригинальный URL в кэш
func (c *RedisCache) SetOriginalURL(ctx context.Context, shortLink, originalURL string, ttl time.Duration) error {
	cacheKey := "redirect:" + shortLink
	if err := c.client.Set(ctx, cacheKey, originalURL, ttl).Err(); err != nil {
		c.logger.WithFields(logrus.Fields{
			"component": "repository",
			"cache_key": cacheKey,
			"error":     err,
		}).Error("Ошибка записи в Redis (redirect)")
		return err
	}
	c.logger.WithFields(logrus.Fields{
		"component":    "repository",
		"short_link":   shortLink,
		"original_url": originalURL,
	}).Info("Закэшировано (redirect) с TTL 10 минут")
	return nil
}
