package devicestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
)

// repository — минимальный порт хранилища, нужный Store для сохранения сессии.
type repository interface {
	UpdateWhatsBotData(ctx context.Context, userID uint32, mk [32]byte, data string, enabled bool) error
}

// Store использует in-memory SQLite с сохранением в JSON.
type Store struct {
	Container *sqlstore.Container
	device    *store.Device
	logger    waLog.Logger
	mu        sync.Mutex
	repo      repository
	Uids      string
	Call      bool // Разрешение отвечать на голосовые вызовы
	Text      bool // Разрешение отвечать на текстовые сообщения
}

// ParseUserBotContainer восстанавливает Store из сериализованного JSON пользователя.
func ParseUserBotContainer(ctx context.Context, userId uint32, containerString string, repo repository) (*Store, error) {
	l := waLog.Stdout(fmt.Sprintf("WhatsApp[%d]", userId), "INFO", true)

	container, err := initContainer(ctx, l)
	if err != nil {
		return nil, err
	}

	device := container.NewDevice()
	if device == nil {
		return nil, fmt.Errorf("ошибка создания устройства для пользователя %d", userId)
	}

	devData, err := parseDeviceData(containerString, userId)
	if err != nil {
		return nil, err
	}

	jid, err := parseJID(devData.ID, userId)
	if err != nil {
		return nil, err
	}

	fillBaseDeviceFields(device, devData, jid)

	if err := ensureAdvSecretKey(device, userId); err != nil {
		return nil, err
	}

	if err := fillAccount(device, devData); err != nil {
		return nil, err
	}

	finalizeAccount(device)

	return &Store{
		Container: container,
		device:    device,
		logger:    l,
		repo:      repo,
		Uids:      devData.Uids,
		Call:      devData.AllowCall,
		Text:      devData.AllowText,
	}, nil
}

// New создаёт пустое хранилище устройства для новой авторизации.
func New(ctx context.Context, logger waLog.Logger, repo repository) (*Store, error) {
	container, err := sqlstore.New(ctx, "sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)", logger)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания in-memory SQLite: %w", err)
	}

	if container == nil {
		return nil, errors.New("ошибка: контейнер не был создан")
	}

	device := container.NewDevice()
	if device == nil {
		return nil, errors.New("ошибка: устройство не было создано")
	}

	if device.NoiseKey == nil || device.IdentityKey == nil {
		return nil, errors.New("ошибка: устройство содержит некорректные данные шифрования")
	}

	if device.Platform == "" {
		device.Platform = "Debian 12"
	}
	if device.PushName == "" {
		device.PushName = "Marusia AI"
	}

	return &Store{
		Container: container,
		device:    device,
		logger:    logger,
		repo:      repo,
	}, nil
}

// GetDevice возвращает устройство для whatsmeow
func (s *Store) GetDevice() *store.Device {
	if s.device == nil {
		s.logger.Errorf("Ошибка: устройство не инициализировано")
		return nil
	}
	return s.device
}

// SaveJSON сохраняет данные устройства в БД в виде JSON
func (s *Store) SaveJSON(ctx context.Context, userID uint32, mk [32]byte, firstAuthorization bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if userID == 0 {
		return fmt.Errorf("получен некорректный userId: %d", userID)
	}
	if s.device == nil {
		return errors.New("ошибка: устройство не инициализировано")
	}

	if s.device.ID == nil || s.device.NoiseKey == nil || s.device.IdentityKey == nil {
		return errors.New("ошибка: устройство содержит некорректные данные")
	}

	// Для первоначального создания бота разрешаю по умолчанию текстовые и голосовые звонки
	allowText := s.Text
	allowCall := s.Call
	if firstAuthorization {
		allowText = true
		allowCall = true
	}

	data, err := json.MarshalIndent(map[string]any{
		"ID":             s.device.ID,
		"RegistrationID": s.device.RegistrationID,
		"NoiseKey":       s.device.NoiseKey,
		"IdentityKey":    s.device.IdentityKey,
		"SignedPreKey":   s.device.SignedPreKey,
		"Platform":       s.device.Platform,
		"PushName":       s.device.PushName,
		"AdvSecretKey":   s.device.AdvSecretKey,
		"Account":        s.device.Account,
		"Uids":           s.Uids,
		"AllowText":      allowText,
		"AllowCall":      allowCall,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации: %w", err)
	}

	if s.repo == nil {
		return errors.New("ошибка: репозиторий не инициализирован")
	}
	if err := s.repo.UpdateWhatsBotData(ctx, userID, mk, string(data), true); err != nil {
		logger.Error("ошибка сохранения данных в БД: %v\n", err, userID)
		return err
	}

	logger.Debug("Данные сессии успешно обновлены в БД", userID)

	return nil
}
