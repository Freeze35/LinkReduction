package utils

import (
	"fmt"
	"os"
)

// DsnString создает строку подключения для базы данных PostgreSQL.
//
// Параметры:
// - dbName: Имя базы данных, к которой необходимо подключиться.
//
// Возвращает:
// - Строку подключения в формате, необходимом для подключения к PostgreSQL.
//
// Строка подключения содержит следующие параметры:
// - host: Адрес хоста базы данных (DB_HOST).
// - port: Порт для подключения к базе данных (DB_PORT).
// - user: Имя пользователя для подключения к базе данных (DB_USER).
// - password: Пароль пользователя для подключения к базе данных (DB_PASSWORD).
// - dbname: Имя базы данных, переданное в качестве параметра dbName.
// - sslmode: Режим SSL (установлено значение 'disable').
func DsnString(dbName string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		dbName)
}
