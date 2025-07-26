package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"linkreduction/internal/cleanup"
	"linkreduction/internal/handler"
	"linkreduction/internal/kafka"
	"linkreduction/internal/prometheus"
	"linkreduction/internal/repository"
	"linkreduction/internal/service"
	"linkreduction/migrations"
)

// shortenCmd represents the shorten command
var shortenCmd = &cobra.Command{
	Use:   "shorten",
	Short: "Run the link shortening server",
	Long: `The shorten command starts the link shortening server, loading configuration from a .env file.
	Use --file to specify a custom file path (default: .env in current directory).`,
	Run: func(cmd *cobra.Command, args []string) {
		// Получение пути к файлу из флага
		filePath, _ := cmd.Flags().GetString("file")
		if filePath == "" {
			filePath = ".env"
		}

		// Разрешение абсолютного пути
		absPath, err := filepath.Abs(filePath)
		if err != nil {
			fmt.Printf("Ошибка разрешения пути к файлу: %v\n", err)
			os.Exit(1)
		}

		// Настройка logrus
		logger := logrus.New()
		logger.SetFormatter(&logrus.JSONFormatter{})
		logger.SetLevel(logrus.InfoLevel)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Загрузка .env файла
		if err := godotenv.Load(absPath); err != nil {
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Fatal("Ошибка загрузки .env файла")
		}

		// Выполнение миграций
		logger.WithField("component", "shorten").Info("Запуск миграций базы данных")
		migrations.RunMigrations()
		logger.WithField("component", "shorten").Info("Миграции успешно применены")

		// Инициализация зависимостей
		dep := handler.NewDependency()

		// Инициализация подключения к PostgreSQL
		db, err := dep.InitPostgres(logger)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Fatal("Ошибка инициализации базы данных")
		}
		defer func() {
			if err := db.Close(); err != nil {
				logger.WithFields(logrus.Fields{
					"component": "shorten",
					"error":     err,
				}).Error("Ошибка при закрытии базы данных")
			}
		}()

		// Инициализация подключения к Redis
		redisClient, err := dep.RedisConnect(ctx, logger)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Fatal("Ошибка инициализации Redis")
		}
		defer func() {
			if err := redisClient.Close(); err != nil {
				logger.WithFields(logrus.Fields{
					"component": "shorten",
					"error":     err,
				}).Error("Ошибка при закрытии Redis соединения")
			}
		}()

		// Инициализация подключения к Kafka
		kafkaProducer, err := dep.KafkaConnect(logger)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Fatal("Ошибка инициализации Kafka")
		}
		if kafkaProducer != nil {
			defer func() {
				if err := kafkaProducer.Close(); err != nil {
					logger.WithFields(logrus.Fields{
						"component": "shorten",
						"error":     err,
					}).Error("Ошибка при закрытии Kafka соединения")
				}
			}()
		}

		// Инициализация Prometheus метрик
		metrics := initprometheus.InitPrometheus()

		// Инициализация репозиториев
		linkRepo := repository.NewPostgresLinkRepository(db, logger)
		cache := repository.NewRedisCache(redisClient, logger)

		// Инициализация сервисов
		linkService := service.NewLinkService(linkRepo, cache, logger)
		cleanupService := cleanup.NewCleanupService(linkRepo, logger)
		kafkaConsumer := kafka.NewConsumer(kafkaProducer, linkRepo, cache, logger)

		// Инициализация обработчика HTTP
		h, err := handler.NewHandler(linkService, metrics, logger)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Fatal("Ошибка инициализации обработчика")
		}

		// Настройка Fiber
		app := fiber.New()

		// Инициализация маршрутов
		h.InitRoutes(app)

		// Запуск Kafka consumer, если Kafka доступна
		if kafkaConsumer != nil {
			go kafkaConsumer.ConsumeShortenURLs()
		}

		// Запуск очистки старых ссылок
		go cleanupService.CleanupOldLinks()

		// Канал для сигналов завершения
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

		// Канал ошибок сервера
		serverErr := make(chan error, 1)

		// Запуск сервера в горутине
		go func() {
			logger.WithField("component", "shorten").Info("Сервер запущен на http://localhost:8080")
			if err := app.Listen(":8080"); err != nil {
				serverErr <- err
			}
		}()

		select {
		case sig := <-quit:
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"signal":    sig,
			}).Info("Получен системный сигнал")
		case err := <-serverErr:
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Error("Ошибка сервера")
		}

		// Попытка корректного завершения
		logger.WithField("component", "shorten").Info("Остановка сервера...")
		if err := app.Shutdown(); err != nil {
			logger.WithFields(logrus.Fields{
				"component": "shorten",
				"error":     err,
			}).Error("Ошибка при завершении сервера")
		}

		logger.WithField("component", "shorten").Info("Сервер успешно остановлен")
	},
}

func init() {
	rootCmd.AddCommand(shortenCmd)
	// Определение флага --file
	shortenCmd.Flags().StringP("file", "f", "", "Путь к файлу конфигурации (по умолчанию: .env в текущей директории)")
}
