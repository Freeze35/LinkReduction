package main

import (
	"LinkReduction/handler"
	"context"
	"github.com/gofiber/fiber/v2"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func main() {
	app := fiber.New()

	// Инициализация хендлеров и маршрутов
	h := handler.NewHandler()
	h.InitRoutes(app)

	// Канал для получения системных сигналов (Ctrl+C, Docker stop и т.п.)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Сервер запускается в отдельной горутине
	go func() {
		if err := app.Listen(":8080"); err != nil {
			log.Printf("Fiber server stopped: %v", err)
		}
	}()

	log.Println("🚀 Server started on http://localhost:8080")

	// Блокируем основной поток, ожидаем сигнал завершения
	<-quit
	log.Println("Gracefully shutting down...")

	// Таймаут для graceful shutdown
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Закрываем Fiber с учетом контекста
	if err := app.Shutdown(); err != nil {
		log.Fatalf("❌ Server shutdown failed: %v", err)
	}

	// Здесь можно закрыть другие ресурсы:

	log.Println("✅ Server exited properly")
}
