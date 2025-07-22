# Сборка и запуск контейнеров
up:
	docker-compose up --build

# Остановить контейнеры и не сохранять их состояние
down:
	docker-compose down

# Остановка контейнеров
stop:
	docker-compose stop

# Сборка основного контейнера на основе Dockerfile
build:
	docker build -t linkreduction .

# Генерация go.mod
gomod:
	go mod init LinkReduction