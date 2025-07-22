package main

import (
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
	"linkreduction/internal/handler"
	"linkreduction/migrations"
	"log"
	"os"
	"os/signal"
	"syscall"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func main() {
	// Загружаем переменные из файла .env
	if err := godotenv.Load("/app/.env"); err != nil {
		log.Fatalf("Ошибка загрузки .env файла: %v", err)
	}

	// Выполняем миграции
	migrations.RunMigrations()

	app := fiber.New()

	// Инициализация маршрутов
	h := handler.NewHandler()
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
