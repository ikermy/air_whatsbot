package repository

import (
	"air_whatsbot/internal/domain"
	"context"

	"github.com/ikermy/air-common/pkg/comdb"
)

// InterDB внутренние методы для работы с БД WhatsApp блтами
type InterDB interface {
	UpdateWhatsBotData(ctx context.Context, userId uint32, mk [32]byte, data string, enabled bool) error
	GetWaUserBotUsers(ctx context.Context) ([]domain.WaUserBotData, error)
	GetWaUser(ctx context.Context, userId uint32) (*domain.WaUserBotData, error)
}

// ExtDB общие DB-методы
type ExtDB interface {
	comdb.Exterior
}

// Repository агрегирует внутренние и внешние контракты доступа к данным.
type Repository struct {
	Intermal InterDB
	External ExtDB
}
