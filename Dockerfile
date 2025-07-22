# Этап сборки
FROM golang:1.23 AS builder

WORKDIR /app

# Копируем go.mod и go.sum и скачиваем зависимости
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь проект
COPY . .

# Собираем бинарник из cmd/main.go с именем linkreduction
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o linkreduction ./cmd

# Этап рантайма — легковесный образ
FROM debian:bullseye-slim

WORKDIR /app

# Копируем бинарник
COPY --from=builder /app/linkreduction .

# Копируем .env если нужен
COPY .env .env

# Открываем порт (замени на нужный, например 8080)
EXPOSE 8080

# Запускаем приложение
CMD ["./linkreduction"]