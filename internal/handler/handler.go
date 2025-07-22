package handler

import (
	"github.com/gofiber/fiber/v2"
	"net/http"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) InitRoutes(app *fiber.App) {
	api := app.Group("/api")

	chats := api.Group("/chats")
	chats.Post("/createShorLink", h.createShorLink)
}

func (h *Handler) createShorLink(c *fiber.Ctx) error {
	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"message": "Hi response",
	})
}
