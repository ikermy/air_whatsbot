package db

import (
	"air_whatsbot/internal/domain"
	"air_whatsbot/internal/repository"
	repoMysql "air_whatsbot/internal/repository/mysql"
	"context"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ikermy/air-common/pkg/comdb"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

type DB struct {
	*comdb.DB
	repo repository.Repository
}

// New создаёт подключение к БД и инициализирует репозитории
func New(parent context.Context) (*DB, error) {
	base, err := comdb.New(parent)
	if err != nil {
		return nil, err
	}
	repo, err := repoMysql.New(base)
	if err != nil {
		return nil, err
	}
	return &DB{
		DB:   base,
		repo: repo,
	}, nil
}

func (d *DB) HandlerClose() {
	go func() {
		// Получаю сигнал о завершении работы от главного контекста приложения
		<-d.MainCTX().Done()
		logger.Info("DB: контекст отменен, ожидаю завершения всех операций...")

		// Ожидаем сигнал о завершении от компонентов работающих с ДБ
		<-domain.UsersDB
		logger.Info("DB: все модули работающие с БД завершили работу, продолжаю остановку...")

		if err := d.Close(); err != nil {
			logger.Error("DB: ошибка при закрытии: %v", err)
		}

		close(domain.Exit)
	}()
}

// UpdateWhatsBotData Обновляет данные канала после фактической авторизации на сервере
func (d *DB) UpdateWhatsBotData(ctx context.Context, userId uint32, mk [32]byte, data string, enabled bool) error {
	return d.repo.Intermal.UpdateWhatsBotData(ctx, userId, mk, data, enabled)
}

// GetWaUserBotUsers получает данные всех включённый WA ботов
func (d *DB) GetWaUserBotUsers(ctx context.Context) ([]domain.WaUserBotData, error) {
	return d.repo.Intermal.GetWaUserBotUsers(ctx)
}

// GetWaUser получает данные WA бота по userID
func (d *DB) GetWaUser(ctx context.Context, userId uint32) (*domain.WaUserBotData, error) {
	return d.repo.Intermal.GetWaUser(ctx, userId)
}

// Repo возвращает набор репозиториев.
func (d *DB) Repo() repository.Repository {
	return d.repo
}
