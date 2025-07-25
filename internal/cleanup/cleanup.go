package cleanup

import (
	"context"
	"github.com/sirupsen/logrus"
	"linkreduction/internal/repository"
	"time"
)

// CleanupService - сервис для очистки старых ссылок
type CleanupService struct {
	repo   repository.LinkRepository
	logger *logrus.Logger
}

// NewCleanupService создаёт новый экземпляр CleanupService
func NewCleanupService(repo repository.LinkRepository, logger *logrus.Logger) *CleanupService {
	return &CleanupService{repo: repo, logger: logger}
}

// CleanupOldLinks периодически удаляет записи старше 2 недель
func (s *CleanupService) CleanupOldLinks() {
	logger := s.logger.WithField("component", "cleanup")
	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rowsAffected, err := s.repo.DeleteOldLinks(ctx, "2 weeks")
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
