package main

import (
	"context"
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	_ "github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/handler"
	initprometheus "linkreduction/internal/prometheus"
	"linkreduction/migrations"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	// Настройка logrus
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Загружаем переменные из файла .env
	if err := godotenv.Load("/app/.env"); err != nil {
		logger.WithFields(logrus.Fields{
			"component": "main",
			"error":     err,
		}).Fatal("Ошибка загрузки .env файла")
	}

	// Выполняем миграции
	logger.WithField("component", "main").Info("Запуск миграций базы данных")
	migrations.RunMigrations()
	logger.WithField("component", "main").Info("Миграции успешно применены")

	// Инициализация handlerDependency
	dep := handler.NewDependency()

	// Инициализация подключения к PostgreSQL
	db, err := dep.InitPostgres(logger)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"component": "main",
			"error":     err,
		}).Fatal("Ошибка инициализации базы данных")
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.WithFields(logrus.Fields{
				"component": "main",
				"error":     err,
			}).Error("Ошибка при закрытии базы данных")
		}
	}()

	// Инициализация подключения к Redis
	redisClient, err := dep.RedisConnect(ctx, logger)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"component": "main",
			"error":     err,
		}).Fatal("Ошибка инициализации Redis")
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.WithFields(logrus.Fields{
				"component": "main",
				"error":     err,
			}).Error("Ошибка при закрытии Redis соединения")
		}
	}()

	// Инициализация подключения к Kafka
	kafka, err := dep.KafkaConnect(logger)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"component": "main",
			"error":     err,
		}).Fatal("Ошибка инициализации Kafka")
	}
	if kafka != nil {
		defer func() {
			if err := kafka.Close(); err != nil {
				logger.WithFields(logrus.Fields{
					"component": "main",
					"error":     err,
				}).Error("Ошибка при закрытии Kafka соединения")
			}
		}()
	}

	// Инициализация Prometheus метрик
	metrics := initprometheus.InitPrometheus()

	// Передаём подключение и логгер в handler через DI
	h, err := handler.NewHandler(db, redisClient, kafka, metrics, logger)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"component": "main",
			"error":     err,
		}).Fatal("Ошибка инициализации обработчика")
	}

	// Настройка Fiber
	app := fiber.New()

	// Инициализация маршрутов
	h.InitRoutes(app)

	// Канал для сигналов завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Канал ошибок сервера
	serverErr := make(chan error, 1)

	// Запускаем сервер в горутине
	go func() {
		logger.WithField("component", "main").Info("Сервер запущен на http://localhost:8080")
		if err := app.Listen(":8080"); err != nil {
			serverErr <- err
		}
	}()

	select {
	case sig := <-quit:
		logger.WithFields(logrus.Fields{
			"component": "main",
			"signal":    sig,
		}).Info("Получен системный сигнал")
	case err := <-serverErr:
		logger.WithFields(logrus.Fields{
			"component": "main",
			"error":     err,
		}).Error("Ошибка сервера")
	}

	// Попытка корректного завершения
	logger.WithField("component", "main").Info("Остановка сервера...")
	if err := app.Shutdown(); err != nil {
		logger.WithFields(logrus.Fields{
			"component": "main",
			"error":     err,
		}).Error("Ошибка при завершении сервера")
	}

	logger.WithField("component", "main").Info("Сервер успешно остановлен")
}
