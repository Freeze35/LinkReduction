package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/repository/postgres"
	"linkreduction/internal/repository/redis"
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
	repo     postgres.LinkRepo
	cache    redis.LinkCache
	logger   *logrus.Logger
	ctx      context.Context
}

// NewConsumer создаёт новый экземпляр Consumer
func NewConsumer(producer sarama.SyncProducer, repo postgres.LinkRepo, cache redis.LinkCache, logger *logrus.Logger, ctx context.Context) *Consumer {
	return &Consumer{producer: producer, repo: repo, cache: cache, logger: logger, ctx: ctx}
}

// ConsumeShortenURLs обрабатывает сообщения из Kafka
func (c *Consumer) ConsumeShortenURLs() error {

	kafkaEnv := os.Getenv("KAFKA_BROKERS")
	kafkaBrokers := strings.Split(kafkaEnv, ",")

	if len(kafkaBrokers) == 0 || kafkaBrokers[0] == "" {
		return fmt.Errorf("переменная окружения KAFKA_BROKERS пуста или не задана, пропуск создания consumer group")
	}

	for _, broker := range kafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("обнаружен пустой адрес брокера Kafka, пропуск создания consumer group")
		}
	}

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
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("ошибка создания consumer group после 10 попыток: %v", err)
	}
	defer consumerGroup.Close()

	for {
		err := consumerGroup.Consume(c.ctx, []string{"shorten-urls"}, c)
		if err != nil {
			time.Sleep(5 * time.Second)
		}
	}
}

// ConsumeClaim обрабатывает сообщения из Kafka с батч-вставкой
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error { // Added error return type
	logger := c.logger.WithField("component", "kafka")

	batchSize := 100                 // Максимальный размер батча
	batchTimeout := 10 * time.Second // Максимальное время ожидания для батча
	batch := make([]postgres.LinkURL, 0, batchSize)
	batchChan := make(chan postgres.LinkURL, batchSize)

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
						if err := c.insertBatch(c.ctx, batch); err != nil {
							logger.Error("Ошибка при вставке последнего батча: ", err)
						}
					}
					return
				}
				batch = append(batch, msg)
				if len(batch) >= batchSize {
					if err := c.insertBatch(c.ctx, batch); err != nil {
						logger.Error("Ошибка при вставке батча: ", err)
					}
					batch = batch[:0]
					ticker.Reset(batchTimeout)
				}
			case <-ticker.C:
				if len(batch) > 0 {
					if err := c.insertBatch(c.ctx, batch); err != nil {
						logger.Error("Ошибка при вставке батча по таймеру: ", err)
					}
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
			logger.Error("Ошибка при разборе сообщения Kafka: ", err)
			continue
		}

		logger.WithFields(logrus.Fields{
			"original_url": shortenMsg.OriginalURL,
			"short_link":   shortenMsg.ShortLink,
		}).Info("Обработка сообщения Kafka")
		batchChan <- postgres.LinkURL{OriginalURL: shortenMsg.OriginalURL, ShortLink: shortenMsg.ShortLink}
		session.MarkMessage(message, "")
	}

	// Закрываем канал и ждём завершения батч-вставки
	close(batchChan)
	wg.Wait()
	return nil // Return nil if processing completes successfully
}

// Setup вызывается при инициализации consumer group (до начала потребления)
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup вызывается при завершении consumer group (после завершения потребления)
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// insertBatch выполняет пакетную вставку в PostgreSQL
func (c *Consumer) insertBatch(ctx context.Context, batch []postgres.LinkURL) error {
	if len(batch) == 0 {
		return fmt.Errorf("длина батча нулевая")
	}

	rowsAffected, err := c.repo.InsertBatch(ctx, batch)
	if err != nil {
		/*for range batch {// Здесь можно добавить метрику для ошибок, если нужно}*/
		return fmt.Errorf("ошибка при внедрение батча %v", err)
	}

	for _, link := range batch[:rowsAffected] {
		if err := c.cache.SetShortLink(ctx, link.OriginalURL, link.ShortLink, 10*60); err != nil {
			return fmt.Errorf("Ошибка записи в Redis (shorten): %v,%v", link.OriginalURL, err)
		}
	}
	return nil
}
