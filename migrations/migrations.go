package migrations

import (
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/sirupsen/logrus"
	"os"
)

func RunMigrations(logger *logrus.Logger) {

	targetDBName := os.Getenv("DBNAME") // Название создаваемой БД
	if targetDBName == "" {
		logger.Fatal("targetDBName не установлен")
	}

	// Сначала подключаемся к системной БД (postgres)
	sysDSN := os.Getenv("DB_DSN_POSTGRES")

	db, err := sql.Open("postgres", sysDSN)
	if err != nil {
		logger.Fatal("ошибка подключения к системной БД: %v", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logger.Fatal(err)
		}
	}(db)

	// Проверяем, существует ли база
	var exists bool
	checkQuery := `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1);`
	if err := db.QueryRow(checkQuery, targetDBName).Scan(&exists); err != nil {
		logger.Fatalf("ошибка при проверке существования БД: %v", err)
	}

	// Если не существует — создаём
	if !exists {
		_, err = db.Exec(fmt.Sprintf(`CREATE DATABASE "%s";`, targetDBName))
		if err != nil {
			logger.Fatalf("ошибка при создании базы данных %s: %v", targetDBName, err)
		}
		logger.Infof("База данных %s успешно создана.", targetDBName)

	} else {
		logger.Infof("База данных %s уже существует.\n", targetDBName)
	}

	// Сначала подключаемся к системной БД (postgres)
	newDBDSN := os.Getenv("DB_DSN_LINKSDB")

	newDB, err := sql.Open("postgres", newDBDSN)
	if err != nil {
		logger.Fatalf("ошибка подключения к системной БД: %v", err)
	}
	defer func(newDB *sql.DB) {
		err := newDB.Close()
		if err != nil {
			logger.Fatal(err)
		}
	}(newDB)

	// Получаем путь к схемам миграций в migrations -> linksDB без указания названия базы данных
	migrationPath := os.Getenv("MIGRATION_LINKS_PATH")

	if migrationPath == "" {
		logger.Fatal("migrationPath не установлен")
	}

	// Создаём инстанс драйвера для PostgreSQL
	driver, err := postgres.WithInstance(newDB, &postgres.Config{})
	if err != nil {
		logger.Fatalf("Ошибка создания инстанса миграции для PostgreSQL: %v", err)
	}

	// Создаём мигратор с указанием пути к миграциям
	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf(`%s%s`, migrationPath, targetDBName),
		targetDBName,
		driver,
	)
	if err != nil {
		logger.Fatalf("Ошибка создания миграции базы данных: %v", err)
	}

	/*// Путь к миграциям
	m, err := migrate.New(fmt.Sprintf(`%s%s`, migrationPath, targetDBName), targetDSN)
	if err != nil {
		logger.Fatalf("ошибка создания мигратора: %v", err)
	}*/

	// Применяем миграции
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		logger.Fatalf("ошибка при выполнении миграций: %v", err)
	}

	logger.Info("Миграции применены успешно.")
}
