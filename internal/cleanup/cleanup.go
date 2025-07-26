package cleanup

import (
	"context"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/repository/postgres"
	"time"
)

// CleanupService - сервис для очистки старых ссылок
type CleanupService struct {
	ctx    context.Context
	repo   postgres.LinkRepo
	logger *logrus.Logger
}

// NewCleanupService создаёт новый экземпляр CleanupService
func NewCleanupService(ctx context.Context, repo postgres.LinkRepo, logger *logrus.Logger) *CleanupService {
	return &CleanupService{repo: repo, logger: logger, ctx: ctx}
}

// CleanupOldLinks периодически удаляет записи старше 2 недель
func (s *CleanupService) CleanupOldLinks() {

	ticker := time.NewTicker(2 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rowsAffected, err := s.repo.DeleteOldLinks(s.ctx, "2 weeks")
			if err != nil {
				s.logger.WithField("error", err).Error("Ошибка удаления старых записей")
				continue
			}
			if rowsAffected > 0 {
				s.logger.WithField("rows_affected", rowsAffected).Info("Удалено старых записей из БД")
			}
		}
	}
}
