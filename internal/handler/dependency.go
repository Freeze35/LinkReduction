package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

func InitPostgres(logger *logrus.Logger) (*sql.DB, error) {
	dbURL := os.Getenv("DB_DSN_LINKSDB")
	if dbURL == "" {
		logger.Fatal("Переменная окружения DB_DSN_LINKSDB не задана")
	}

	logger.WithField("db_url", dbURL).Info("Подключение к базе данных")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия базы данных: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("база данных недоступна: %v", err)
	}

	logger.WithField("db_url", dbURL).Info("База данных успешно подключена")
	return db, nil
}

func RedisConnect(ctx context.Context, logger *logrus.Logger) (*redis.Client, error) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		logger.Fatal("Переменная окружения REDIS_URL не задана")
	}

	logger.WithField("redis_url", redisURL).Info("Подключение к Redis")
	redisClient := redis.NewClient(&redis.Options{Addr: redisURL})

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("Redis недоступен: %v", err)
	}

	logger.Info("Redis успешно подключён")
	return redisClient, nil
}

// GetKafkaBrokers получает список Kafka брокеров из переменной окружения KAFKA_BROKERS.
// Возвращает срез строк с адресами брокеров или ошибку, если переменная не задана или содержит недопустимые значения.
func GetKafkaBrokers(logger *logrus.Logger) ([]string, error) {
	kafkaEnv := os.Getenv("KAFKA_BROKERS")
	if kafkaEnv == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS не задана")
	}
	brokers := strings.Split(kafkaEnv, ",")
	cleaned := make([]string, 0, len(brokers))
	for _, b := range brokers {
		trimmed := strings.TrimSpace(b)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS не содержит валидных брокеров")
	}
	logger.WithField("brokers", cleaned).Info("Получены адреса брокеров Kafka")
	return cleaned, nil
}

// NewKafkaProducerConfig создаёт и настраивает конфигурацию для Kafka продюсера.
// Устанавливаются параметры подтверждения, количество попыток и задержка между ними.
func NewKafkaProducerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 100 * time.Millisecond
	return cfg
}

// ConnectKafkaProducer пытается подключиться к Kafka брокерам с помощью переданной конфигурации.
// Делает до 10 попыток с задержкой в 2 секунды между ними. Возвращает SyncProducer или ошибку.
func ConnectKafkaProducer(brokers []string, cfg *sarama.Config, logger *logrus.Logger) (sarama.SyncProducer, error) {
	for i := 0; i < 10; i++ {
		producer, err := sarama.NewSyncProducer(brokers, cfg)
		if err == nil {
			logger.Info("Kafka продюсер успешно подключён")
			return producer, nil
		}
		logger.WithFields(logrus.Fields{
			"attempt": i + 1,
			"error":   err,
		}).Error("Ошибка подключения к Kafka")
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("не удалось подключиться к Kafka после 10 попыток")
}

// InitKafkaProducer объединяет шаги инициализации Kafka продюсера:
// получает брокеров, создаёт конфигурацию и устанавливает соединение.
// Возвращает подключённый SyncProducer или ошибку.
func InitKafkaProducer(logger *logrus.Logger) (sarama.SyncProducer, error) {
	// Получаем список брокеров из переменной окружения
	brokers, err := GetKafkaBrokers(logger)
	if err != nil {
		return nil, err
	}

	// Создаём конфигурацию для Kafka-продюсера
	config := NewKafkaProducerConfig()

	// Подключаемся к Kafka
	producer, err := ConnectKafkaProducer(brokers, config, logger)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
