package repository

import (
	"context"
	"database/sql"
	"github.com/sirupsen/logrus"
)

// LinkRepository - интерфейс для работы с хранилищем ссылок
type LinkRepository interface {
	FindByOriginalURL(ctx context.Context, originalURL string) (string, error)
	FindByShortLink(ctx context.Context, shortLink string) (string, error)
	Insert(ctx context.Context, originalURL, shortLink string) (bool, error)
	InsertBatch(ctx context.Context, links []Link) (int64, error)
	DeleteOldLinks(ctx context.Context, threshold string) (int64, error)
}

// Link - структура для представления ссылки
type Link struct {
	OriginalURL string
	ShortLink   string
}

// PostgresLinkRepository - реализация LinkRepository для PostgreSQL
type PostgresLinkRepository struct {
	db     *sql.DB
	logger *logrus.Logger
}

// NewPostgresLinkRepository создаёт новый экземпляр PostgresLinkRepository
func NewPostgresLinkRepository(db *sql.DB, logger *logrus.Logger) *PostgresLinkRepository {
	return &PostgresLinkRepository{db: db, logger: logger}
}

// FindByOriginalURL ищет короткую ссылку по оригинальному URL
func (r *PostgresLinkRepository) FindByOriginalURL(ctx context.Context, originalURL string) (string, error) {
	var shortLink string
	err := r.db.QueryRowContext(ctx, "SELECT short_link FROM links WHERE link = $1", originalURL).Scan(&shortLink)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		r.logger.WithFields(logrus.Fields{
			"component":    "repository",
			"original_url": originalURL,
			"error":        err,
		}).Error("Ошибка проверки URL в базе данных")
		return "", err
	}
	return shortLink, nil
}

// FindByShortLink ищет оригинальный URL по короткой ссылке
func (r *PostgresLinkRepository) FindByShortLink(ctx context.Context, shortLink string) (string, error) {
	var originalURL string
	err := r.db.QueryRowContext(ctx, "SELECT link FROM links WHERE short_link = $1", shortLink).Scan(&originalURL)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		r.logger.WithFields(logrus.Fields{
			"component":  "repository",
			"short_link": shortLink,
			"error":      err,
		}).Error("Ошибка базы данных")
		return "", err
	}
	return originalURL, nil
}

// Insert вставляет новую ссылку в базу данных
func (r *PostgresLinkRepository) Insert(ctx context.Context, originalURL, shortLink string) (bool, error) {
	result, err := r.db.ExecContext(ctx, "INSERT INTO links (link, short_link, created_at) VALUES ($1, $2, NOW()) ON CONFLICT (link) DO NOTHING", originalURL, shortLink)
	if err != nil {
		r.logger.WithFields(logrus.Fields{
			"component":    "repository",
			"original_url": originalURL,
			"error":        err,
		}).Error("Ошибка вставки в БД")
		return false, err
	}
	rowsAffected, _ := result.RowsAffected()
	return rowsAffected > 0, nil
}

// InsertBatch выполняет пакетную вставку ссылок
func (r *PostgresLinkRepository) InsertBatch(ctx context.Context, links []Link) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		r.logger.WithFields(logrus.Fields{
			"component": "repository",
			"error":     err,
		}).Error("Ошибка начала транзакции")
		return 0, err
	}

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO links (link, short_link, created_at) VALUES ($1, $2, NOW()) ON CONFLICT (link) DO NOTHING")
	if err != nil {
		r.logger.WithFields(logrus.Fields{
			"component": "repository",
			"error":     err,
		}).Error("Ошибка подготовки запроса")
		tx.Rollback()
		return 0, err
	}
	defer stmt.Close()

	rowsAffected := int64(0)
	for _, link := range links {
		result, err := stmt.ExecContext(ctx, link.OriginalURL, link.ShortLink)
		if err != nil {
			r.logger.WithFields(logrus.Fields{
				"component":    "repository",
				"original_url": link.OriginalURL,
				"error":        err,
			}).Error("Ошибка вставки в БД")
			continue
		}
		if ra, _ := result.RowsAffected(); ra > 0 {
			rowsAffected++
		}
	}

	if err := tx.Commit(); err != nil {
		r.logger.WithFields(logrus.Fields{
			"component": "repository",
			"error":     err,
		}).Error("Ошибка коммита транзакции")
		tx.Rollback()
		return 0, err
	}

	r.logger.WithFields(logrus.Fields{
		"component":     "repository",
		"rows_affected": rowsAffected,
		"batch_size":    len(links),
	}).Info("Вставлено в БД")
	return rowsAffected, nil
}

// DeleteOldLinks удаляет ссылки старше указанного порога
func (r *PostgresLinkRepository) DeleteOldLinks(ctx context.Context, threshold string) (int64, error) {
	result, err := r.db.ExecContext(ctx, "DELETE FROM links WHERE created_at < NOW() - INTERVAL $1", threshold)
	if err != nil {
		r.logger.WithFields(logrus.Fields{
			"component": "repository",
			"error":     err,
		}).Error("Ошибка удаления старых записей")
		return 0, err
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		r.logger.WithFields(logrus.Fields{
			"component":     "repository",
			"rows_affected": rowsAffected,
		}).Info("Удалено старых записей из БД")
	}
	return rowsAffected, nil
}
