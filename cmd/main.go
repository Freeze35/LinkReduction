package main

import (
	"context"
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	"linkreduction/internal/handler"
	initprometheus "linkreduction/internal/prometheus"
	"linkreduction/migrations"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Загружаем переменные из файла .env
	if err := godotenv.Load("/app/.env"); err != nil {
		log.Fatalf("Ошибка загрузки .env файла: %v", err)
	}

	// Выполняем миграции
	migrations.RunMigrations()

	//Инициализация handlerDependency
	dep := handler.Dependency{}

	//Инициализация Подключения DB
	db, err := dep.InitPostgres()
	// Обеспечим закрытие соединения при завершении
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatalf("Ошибка при закрытии базы данных: %v", err)
		}
	}()

	//Инициализация Подключения Redis
	redisClient, err := dep.RedisConnect(ctx)
	// Обеспечим закрытие redis соединения при завершении
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatalf("Ошибка при закрытии redis соединения: %v", err)
		}
	}()

	//Инициализация Подключения Kafka
	kafka, err := dep.KafkaConnect()
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatalf("Ошибка при закрытии kafka соединения: %v", err)
		}
	}()

	//
	metrics := initprometheus.InitPrometheus()

	// Передаём подключение в handler через DI
	h, err := handler.NewHandler(db, redisClient, kafka, metrics)
	if err != nil {
		log.Fatalf("Ошибка инициализации обработчика: %v", err)
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
		log.Println("Сервер запущен на http://localhost:8080")
		if err := app.Listen(":8080"); err != nil {
			serverErr <- err
		}
	}()

	select {
	case sig := <-quit:
		log.Printf("Получен системный сигнал: %s", sig)
	case err := <-serverErr:
		log.Printf("Ошибка сервера: %v", err)
	}

	// Попытка корректного завершения
	log.Println("Остановка сервера...")
	if err := app.Shutdown(); err != nil {
		log.Fatalf("Ошибка при завершении сервера: %v", err)
	}

	log.Println("Сервер успешно остановлен.")
}
