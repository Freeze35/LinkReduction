package redis

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"time"
)

// LinkCache - интерфейс для работы с кэшем
type LinkCache interface {
	GetShortLink(ctx context.Context, originalURL string) (string, error)
	SetShortLink(ctx context.Context, originalURL, shortLink string, ttl time.Duration) error
	GetOriginalURL(ctx context.Context, shortLink string) (string, error)
	SetOriginalURL(ctx context.Context, shortLink, originalURL string, ttl time.Duration) error
}

// Link - реализация LinkCache для Redis
type Link struct {
	client *redis.Client
	logger *logrus.Logger
}

// NewLink создаёт новый экземпляр Link
func NewLink(client *redis.Client, logger *logrus.Logger) *Link {
	return &Link{client: client, logger: logger}
}

// GetShortLink получает короткую ссылку из кэша
func (c *Link) GetShortLink(ctx context.Context, originalURL string) (string, error) {
	cacheKey := "shorten:" + originalURL
	result, err := c.client.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return result, nil
}

// SetShortLink сохраняет короткую ссылку в кэш
func (c *Link) SetShortLink(ctx context.Context, originalURL, shortLink string, ttl time.Duration) error {
	cacheKey := "shorten:" + originalURL
	if err := c.client.Set(ctx, cacheKey, shortLink, ttl).Err(); err != nil {
		return err
	}
	return nil
}

// GetOriginalURL получает оригинальный URL из кэша
func (c *Link) GetOriginalURL(ctx context.Context, shortLink string) (string, error) {
	cacheKey := "redirect:" + shortLink
	result, err := c.client.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return result, nil
}

// SetOriginalURL сохраняет оригинальный URL в кэш
func (c *Link) SetOriginalURL(ctx context.Context, shortLink, originalURL string, ttl time.Duration) error {
	cacheKey := "redirect:" + shortLink
	if err := c.client.Set(ctx, cacheKey, originalURL, ttl).Err(); err != nil {
		return err
	}
	return nil
}
