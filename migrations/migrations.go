package migrations

import (
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"linkreduction/utils"
	"log"
	"os"
)

func RunMigrations() {
	mainDBName := "postgres"            // Подключаемся к системной БД
	targetDBName := os.Getenv("DBNAME") // Название создаваемой БД
	if targetDBName == "" {
		log.Fatal("targetDBName не установлен")
	}

	// Сначала подключаемся к системной БД (postgres)
	sysDSN := utils.DsnString(mainDBName)

	db, err := sql.Open("postgres", sysDSN)
	if err != nil {
		log.Fatalf("ошибка подключения к системной БД: %v", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	// Проверяем, существует ли база
	var exists bool
	checkQuery := `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1);`
	if err := db.QueryRow(checkQuery, targetDBName).Scan(&exists); err != nil {
		log.Fatalf("ошибка при проверке существования БД: %v", err)
	}

	// Если не существует — создаём
	if !exists {
		_, err = db.Exec(fmt.Sprintf(`CREATE DATABASE "%s";`, targetDBName))
		if err != nil {
			log.Fatalf("ошибка при создании базы данных %s: %v", targetDBName, err)
		}
		log.Printf("База данных %s успешно создана.\n", targetDBName)
	} else {
		log.Printf("База данных %s уже существует.\n", targetDBName)
	}

	// Сначала подключаемся к системной БД (postgres)
	newDBDSN := utils.DsnString(targetDBName)

	newDB, err := sql.Open("postgres", newDBDSN)
	if err != nil {
		log.Fatalf("ошибка подключения к системной БД: %v", err)
	}
	defer func(newDB *sql.DB) {
		err := newDB.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(newDB)

	// Получаем путь к схемам миграций в migrations -> linksDB без указания названия базы данных
	migrationPath := os.Getenv("MIGRATION_LINKS_PATH")

	if migrationPath == "" {
		log.Fatal("migrationPath не установлен")
	}

	// Создаём инстанс драйвера для PostgreSQL
	driver, err := postgres.WithInstance(newDB, &postgres.Config{})
	if err != nil {
		log.Fatalf("Ошибка создания инстанса миграции для PostgreSQL: %v", err)
	}

	// Создаём мигратор с указанием пути к миграциям
	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf(`%s%s`, migrationPath, targetDBName),
		targetDBName,
		driver,
	)
	if err != nil {
		log.Fatalf("Ошибка создания миграции базы данных: %v", err)
	}

	/*// Путь к миграциям
	m, err := migrate.New(fmt.Sprintf(`%s%s`, migrationPath, targetDBName), targetDSN)
	if err != nil {
		log.Fatalf("ошибка создания мигратора: %v", err)
	}*/

	// Применяем миграции
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("ошибка при выполнении миграций: %v", err)
	}

	log.Println("Миграции применены успешно.")
}
