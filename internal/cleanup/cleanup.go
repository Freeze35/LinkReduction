package cleanup

import (
	"context"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/repository/postgres"
	"time"
)

// CleanupService - сервис для очистки старых ссылок
type CleanupService struct {
	repo   postgres.LinkRepo
	logger *logrus.Logger
	ctx    context.Context
}

// NewCleanupService создаёт новый экземпляр CleanupService
func NewCleanupService(repo postgres.LinkRepo, logger *logrus.Logger, ctx context.Context) *CleanupService {
	return &CleanupService{repo: repo, logger: logger, ctx: ctx}
}

// CleanupOldLinks периодически удаляет записи старше 2 недель
func (s *CleanupService) CleanupOldLinks() {
	logger := s.logger.WithField("component", "cleanup")
	ticker := time.NewTicker(2 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rowsAffected, err := s.repo.DeleteOldLinks(s.ctx, "2 weeks")
			if err != nil {
				logger.WithField("error", err).Error("Ошибка удаления старых записей")
				continue
			}
			if rowsAffected > 0 {
				logger.WithField("rows_affected", rowsAffected).Info("Удалено старых записей из БД")
			}
		}
	}
}
