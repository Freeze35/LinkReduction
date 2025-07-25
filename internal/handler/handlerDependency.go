package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"strings"
	"time"
)

type DependencyI interface {
	InitPostgres() (*sql.DB, error)
	RedisConnect(ctx context.Context) (*redis.Client, error)
	KafkaConnect() (sarama.SyncProducer, error)
}

type Dependency struct{}

func (h *Dependency) InitPostgres() (*sql.DB, error) {
	// Получаем строку подключения к базе данных
	dbURL := os.Getenv("DB_DSN_LINKSDB")
	if dbURL == "" {
		log.Fatal("Переменная окружения DB_DSN_LINKSDB не задана")
	}

	// Подключение к базе данных
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Printf("Ошибка открытия базы данных: %v", err)
		return nil, fmt.Errorf("ошибка открытия базы данных: %w", err)
	}

	if err := db.Ping(); err != nil {
		log.Printf("База данных недоступна: %v", err)
		return nil, fmt.Errorf("база данных недоступна: %v", err)
	}

	log.Printf("База данных успешно подключена: %s", dbURL)
	return db, nil
}

func (h *Dependency) RedisConnect(ctx context.Context) (*redis.Client, error) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("Переменная окружения REDIS_URL не задана")
	}

	log.Printf("Подключение к Redis: %s", redisURL)
	redisClient := redis.NewClient(&redis.Options{Addr: redisURL})

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Printf("Redis недоступен: %v", err)
	}

	log.Println("Redis успешно подключён")
	return redisClient, nil
}

func (h *Dependency) KafkaConnect() (sarama.SyncProducer, error) {
	kafkaEnv := os.Getenv("KAFKA_BROKER")
	kafkaBrokers := strings.Split(kafkaEnv, ",")

	log.Printf("Проверка переменной окружения KAFKA_BROKER: %s", kafkaEnv)
	if len(kafkaBrokers) == 0 || kafkaBrokers[0] == "" {
		log.Println("Переменная окружения KAFKA_BROKER пуста или не задана, Kafka будет отключена")
		return nil, nil // Возвращаем nil, чтобы обозначить отсутствие Kafka
	}

	// Проверяем, что все брокеры имеют валидные адреса
	for _, broker := range kafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			log.Println("Обнаружен пустой адрес брокера Kafka, Kafka будет отключена")
			return nil, nil
		}
	}

	log.Printf("Подключение к Kafka: %v", kafkaBrokers)

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
		log.Printf("Ошибка подключения к Kafka, попытка %d: %v", i+1, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Printf("Ошибка подключения к Kafka после 10 попыток, Kafka будет отключена: %v", err)
		return nil, nil
	}

	log.Println("Kafka успешно подключён")
	return producer, nil
}
