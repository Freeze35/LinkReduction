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

type DependencyI interface {
	InitPostgres(logger *logrus.Logger) (*sql.DB, error)
	RedisConnect(ctx context.Context, logger *logrus.Logger) (*redis.Client, error)
	KafkaConnect(logger *logrus.Logger) (sarama.SyncProducer, error)
}

type Dependency struct{}

// NewDependency создаёт новый экземпляр Dependency
func NewDependency() *Dependency {
	return &Dependency{}
}

func (h *Dependency) InitPostgres(logger *logrus.Logger) (*sql.DB, error) {

	// Получаем строку подключения к базе данных
	dbURL := os.Getenv("DB_DSN_LINKSDB")
	if dbURL == "" {
		logger.Fatal("Переменная окружения DB_DSN_LINKSDB не задана")
	}

	logger.WithField("db_url", dbURL).Info("Подключение к базе данных")
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		logger.WithField("error", err).Error("Ошибка открытия базы данных")
		return nil, fmt.Errorf("ошибка открытия базы данных: %w", err)
	}

	if err := db.Ping(); err != nil {
		logger.WithField("error", err).Error("База данных недоступна")
		return nil, fmt.Errorf("база данных недоступна: %v", err)
	}

	logger.WithField("db_url", dbURL).Info("База данных успешно подключена")
	return db, nil
}

func (h *Dependency) RedisConnect(ctx context.Context, logger *logrus.Logger) (*redis.Client, error) {

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		logger.Fatal("Переменная окружения REDIS_URL не задана")
	}

	logger.WithField("redis_url", redisURL).Info("Подключение к Redis")
	redisClient := redis.NewClient(&redis.Options{Addr: redisURL})

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		logger.WithField("error", err).Error("Redis недоступен")
		return nil, fmt.Errorf("Redis недоступен: %v", err)
	}

	logger.Info("Redis успешно подключён")
	return redisClient, nil
}

func (h *Dependency) KafkaConnect(logger *logrus.Logger) (sarama.SyncProducer, error) {

	kafkaEnv := os.Getenv("KAFKA_BROKERS")
	kafkaBrokers := strings.Split(kafkaEnv, ",")

	logger.WithField("kafka_brokers", kafkaEnv).Info("Проверка переменной окружения KAFKA_BROKERS")
	if len(kafkaBrokers) == 0 || kafkaBrokers[0] == "" {
		logger.Info("Переменная окружения KAFKA_BROKERS пуста или не задана, Kafka будет отключена")
		return nil, nil // Возвращаем nil, чтобы обозначить отсутствие Kafka
	}

	// Проверяем, что все брокеры имеют валидные адреса
	for _, broker := range kafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			logger.Info("Обнаружен пустой адрес брокера Kafka, Kafka будет отключена")
			return nil, nil
		}
	}

	logger.WithField("kafka_brokers", kafkaBrokers).Info("Подключение к Kafka")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	var producer sarama.SyncProducer
	var err error
	for i := 0; i < 10; i++ {
		producer, err = sarama.NewSyncProducer(kafkaBrokers, config)
		if err == nil {
			break
		}
		logger.WithFields(logrus.Fields{
			"attempt": i + 1,
			"error":   err,
		}).Error("Ошибка подключения к Kafka")
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		logger.WithField("error", err).Warn("Ошибка подключения к Kafka после 10 попыток, Kafka будет отключена")
		return nil, nil
	}

	logger.Info("Kafka успешно подключён")
	return producer, nil
}
