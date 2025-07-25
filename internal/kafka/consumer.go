package kafka

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/repository"
	"os"
	"strings"
	"sync"
	"time"
)

// ShortenMessage - структура для сообщений Kafka
type ShortenMessage struct {
	OriginalURL string `json:"original_url"`
	ShortLink   string `json:"short_link"`
}

// Consumer - структура для обработки сообщений Kafka
type Consumer struct {
	producer sarama.SyncProducer
	repo     repository.LinkRepository
	cache    repository.Cache
	logger   *logrus.Logger
}

// NewConsumer создаёт новый экземпляр Consumer
func NewConsumer(producer sarama.SyncProducer, repo repository.LinkRepository, cache repository.Cache, logger *logrus.Logger) *Consumer {
	return &Consumer{producer: producer, repo: repo, cache: cache, logger: logger}
}

// ConsumeShortenURLs обрабатывает сообщения из Kafka
func (c *Consumer) ConsumeShortenURLs() {
	logger := c.logger.WithField("component", "kafka")
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
		err := consumerGroup.Consume(ctx, []string{"shorten-urls"}, c)
		if err != nil {
			logger.WithField("error", err).Error("Ошибка consumer group")
			time.Sleep(5 * time.Second)
		}
	}
}

// Setup - реализация sarama.ConsumerGroupHandler
func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup - реализация sarama.ConsumerGroupHandler
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim обрабатывает сообщения из Kafka с батч-вставкой
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logger := c.logger.WithField("component", "kafka")
	ctx := context.Background()
	batchSize := 100                 // Максимальный размер батча
	batchTimeout := 10 * time.Second // Максимальное время ожидания для батча
	batch := make([]repository.Link, 0, batchSize)
	batchChan := make(chan repository.Link, batchSize)
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
						c.insertBatch(ctx, batch)
					}
					return
				}
				batch = append(batch, msg)
				if len(batch) >= batchSize {
					c.insertBatch(ctx, batch)
					batch = batch[:0]
					ticker.Reset(batchTimeout)
				}
			case <-ticker.C:
				if len(batch) > 0 {
					c.insertBatch(ctx, batch)
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
			continue
		}

		logger.WithFields(logrus.Fields{
			"original_url": shortenMsg.OriginalURL,
			"short_link":   shortenMsg.ShortLink,
		}).Info("Обработка сообщения Kafka")
		batchChan <- repository.Link{OriginalURL: shortenMsg.OriginalURL, ShortLink: shortenMsg.ShortLink}
		session.MarkMessage(message, "")
	}

	// Закрываем канал и ждём завершения батч-вставки
	close(batchChan)
	wg.Wait()
	return nil
}

// insertBatch выполняет пакетную вставку в PostgreSQL
func (c *Consumer) insertBatch(ctx context.Context, batch []repository.Link) {
	if len(batch) == 0 {
		return
	}

	rowsAffected, err := c.repo.InsertBatch(ctx, batch)
	if err != nil {
		for range batch {
			// Здесь можно добавить метрику для ошибок, если нужно
		}
		return
	}

	for _, link := range batch[:rowsAffected] {
		if err := c.cache.SetShortLink(ctx, link.OriginalURL, link.ShortLink, 10*60); err != nil {
			c.logger.WithFields(logrus.Fields{
				"component": "kafka",
				"cache_key": "shorten:" + link.OriginalURL,
				"error":     err,
			}).Error("Ошибка записи в Redis (shorten)")
		}
	}
}
