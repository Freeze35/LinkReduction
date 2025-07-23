package main

import (
	"database/sql"
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	"linkreduction/internal/handler"
	"linkreduction/migrations"
	"linkreduction/utils"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Загружаем переменные из файла .env
	if err := godotenv.Load("/app/.env"); err != nil {
		log.Fatalf("Ошибка загрузки .env файла: %v", err)
	}

	// Получаем строку подключения к базе данных
	dbURL := utils.DsnString(os.Getenv("DBNAME"))
	if dbURL == "" {
		log.Fatalf("Переменная окружения DATABASE_URL не задана")
	}

	// Выполняем миграции
	migrations.RunMigrations()

	// Инициализация обработчика
	h, err := handler.NewHandler(dbURL)
	if err != nil {
		log.Fatalf("Ошибка инициализации обработчика: %v", err)
	}
	defer func(Db *sql.DB) {
		err := Db.Close()
		if err != nil {
			log.Fatalf("Внутреняя ошибка закрытия базы данных:%v", err)
		}
	}(h.Db)

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
