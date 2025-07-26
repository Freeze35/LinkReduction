package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	configKafka "linkreduction/internal/config"
	"linkreduction/internal/repository/postgres"
	"linkreduction/internal/repository/redis"
	"linkreduction/internal/service"
	"os"
	"strings"
	"time"
)

// ShortenMessage - структура для сообщений Kafka
type ShortenMessage struct {
	OriginalURL string `json:"original_url"`
	ShortLink   string `json:"short_link"`
}

// Consumer - структура для обработки сообщений Kafka
type Consumer struct {
	ctx         context.Context
	producer    sarama.SyncProducer
	repo        postgres.LinkRepo
	cache       redis.LinkCache
	logger      *logrus.Logger
	linkService *service.Service
}

// NewConsumer создаёт новый экземпляр Consumer
func NewConsumer(ctx context.Context, producer sarama.SyncProducer, repo postgres.LinkRepo, cache redis.LinkCache, logger *logrus.Logger, linkService *service.Service) *Consumer {
	return &Consumer{
		ctx:         ctx,
		producer:    producer,
		repo:        repo,
		cache:       cache,
		logger:      logger,
		linkService: linkService,
	}
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
		consumerGroup, err = sarama.NewConsumerGroup(kafkaBrokers, configKafka.KafkaShortenURLsGroup, config)
		if err == nil {
			break
		}
		c.logger.WithFields(logrus.Fields{
			"attempt": i + 1,
		}).Warn("Ошибка создания consumer group: ", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("ошибка создания consumer group после 10 попыток: %v", err)
	}
	defer consumerGroup.Close()

	for {
		err := consumerGroup.Consume(c.ctx, []string{configKafka.KafkaShortenURLsTopic}, c)
		if err != nil {
			c.logger.WithError(err).Error("Ошибка потребления сообщений Kafka")
			time.Sleep(5 * time.Second)
		}
	}
}

// deserializeMessage десериализует сообщение Kafka в ShortenMessage
func (c *Consumer) deserializeMessage(message *sarama.ConsumerMessage) (ShortenMessage, error) {
	var shortenMsg ShortenMessage
	if err := json.Unmarshal(message.Value, &shortenMsg); err != nil {
		c.logger.WithError(err).Error("Ошибка при разборе сообщения Kafka")
		return ShortenMessage{}, err
	}
	return shortenMsg, nil
}

// sendToBatchChan отправляет сообщение в batchChan с учётом контекста
func (c *Consumer) sendToBatchChan(ctx context.Context, batchChan chan<- postgres.LinkURL, msg ShortenMessage, session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	select {
	case batchChan <- postgres.LinkURL{OriginalURL: msg.OriginalURL, ShortLink: msg.ShortLink}:
		session.MarkMessage(message, "")
		c.logger.WithFields(logrus.Fields{
			"original_url": msg.OriginalURL,
			"short_link":   msg.ShortLink,
		}).Info("Отправлено сообщение в batchChan")
		return nil
	case <-ctx.Done():
		c.logger.Info("Контекст отменён, прекращение отправки в batchChan")
		return ctx.Err()
	}
}

// processBatchInsert выполняет пакетную вставку сообщений из batchChan
func (c *Consumer) processBatchInsert(ctx context.Context, batchChan <-chan postgres.LinkURL, batchSize int, batchTimeout time.Duration) {
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	batch := make([]postgres.LinkURL, 0, batchSize)
	for {
		select {
		case msg, ok := <-batchChan:
			if !ok { // Канал закрыт
				if len(batch) > 0 {
					if err := c.linkService.InsertBatch(ctx, batch); err != nil {
						c.logger.WithFields(logrus.Fields{
							"batch_size": len(batch),
						}).Error("Ошибка при вставке последнего батча: ", err)
					}
				}
				c.logger.Info("Канал batchChan закрыт, завершение пакетной вставки")
				return
			}
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				if err := c.linkService.InsertBatch(ctx, batch); err != nil {
					c.logger.WithFields(logrus.Fields{
						"batch_size": len(batch),
					}).Error("Ошибка при вставке батча: ", err)
				}
				batch = batch[:0]
				ticker.Reset(batchTimeout)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if err := c.linkService.InsertBatch(ctx, batch); err != nil {
					c.logger.WithFields(logrus.Fields{
						"batch_size": len(batch),
					}).Error("Ошибка при вставке батча по таймеру: ", err)
				}
				batch = batch[:0]
				ticker.Reset(batchTimeout)
			}
		case <-ctx.Done():
			if len(batch) > 0 {
				if err := c.linkService.InsertBatch(ctx, batch); err != nil && !errors.Is(err, context.Canceled) {
					c.logger.WithFields(logrus.Fields{
						"batch_size": len(batch),
					}).Error("Ошибка при вставке батча при завершении: ", err)
				}
			}
			c.logger.Info("Контекст отменён, завершение пакетной вставки")
			return
		}
	}
}

// processKafkaMessages обрабатывает сообщения из Kafka и отправляет их в batchChan
func (c *Consumer) processKafkaMessages(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim, batchChan chan<- postgres.LinkURL) error {
	for message := range claim.Messages() {
		shortenMsg, err := c.deserializeMessage(message)
		if err != nil {
			continue
		}
		if err := c.sendToBatchChan(c.ctx, batchChan, shortenMsg, session, message); err != nil {
			return err
		}
	}
	return nil
}

// ConsumeClaim реализует интерфейс sarama.ConsumerGroupHandler
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logger := c.logger.WithField("component", "kafka")
	batchSize := 50                  // Максимальный размер батча
	batchTimeout := 10 * time.Second // Максимальное время ожидания для батча

	batchChan := make(chan postgres.LinkURL, batchSize)

	// Запускаем горутину для пакетной вставки
	go c.processBatchInsert(c.ctx, batchChan, batchSize, batchTimeout)

	// Обрабатываем сообщения Kafka
	err := c.processKafkaMessages(session, claim, batchChan)
	if err != nil {
		logger.WithError(err).Info("Завершение обработки сообщений Kafka из-за ошибки")
		close(batchChan)
		return err
	}

	// Закрываем канал после обработки всех сообщений
	close(batchChan)
	return nil
}

// Setup вызывается при инициализации consumer group
func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	c.logger.Info("Инициализация consumer group")
	return nil
}

// Cleanup вызывается при завершении consumer group
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	c.logger.Info("Завершение consumer group")
	return nil
}
