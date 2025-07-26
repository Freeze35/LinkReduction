# Этап сборки
FROM golang:1.23 AS builder

WORKDIR /app

# Копируем go.mod и go.sum и скачиваем зависимости
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь проект
COPY . .

# Собираем бинарник из cmd/main.go с именем linkreduction
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o linkreduction ./main.go

# Этап рантайма — легковесный образ
FROM debian:bullseye-slim

WORKDIR /app

# Копируем бинарник
COPY --from=builder /app/linkreduction .

# Копируем .env
COPY .env .

# Копирование миграций
COPY ./migrations ./migrations

# Устанавливаем права на выполнение
RUN chmod +x ./linkreduction

# Открываем порт
EXPOSE 8080

# Запускаем приложение
CMD ["./linkreduction", "shorten", "--file", "/app/.env"]