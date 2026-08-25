package whatsapp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// signalMACLogger переводит ожидаемые ошибки расшифровки старых Signal-сообщений
// в DEBUG. Остальные сообщения whatsmeow сохраняют исходный уровень.
type signalMACLogger struct {
	waLog.Logger
}

func (l signalMACLogger) Errorf(msg string, args ...any) {
	if isSignalMACError(msg) {
		l.Logger.Debugf(msg, args...)
		return
	}
	l.Logger.Errorf(msg, args...)
}

func (l signalMACLogger) Warnf(msg string, args ...any) {
	if isSignalMACError(msg) {
		l.Logger.Debugf(msg, args...)
		return
	}
	l.Logger.Warnf(msg, args...)
}

func (l signalMACLogger) Sub(module string) waLog.Logger {
	return signalMACLogger{Logger: l.Logger.Sub(module)}
}

func isSignalMACError(msg string) bool {
	msg = strings.ToLower(msg)
	return strings.Contains(msg, "mismatching mac") &&
		strings.Contains(msg, "signal message")
}

// DeviceData Структура для парсинга JSON с правильными типами полей
type DeviceData struct {
	ID             string                         `json:"ID"`
	RegistrationID uint32                         `json:"RegistrationID"`
	NoiseKey       *keys.KeyPair                  `json:"NoiseKey"`
	IdentityKey    *keys.KeyPair                  `json:"IdentityKey"`
	SignedPreKey   *keys.PreKey                   `json:"SignedPreKey"`
	Platform       string                         `json:"Platform"`
	PushName       string                         `json:"PushName"`
	AdvSecretKey   []byte                         `json:"AdvSecretKey"`
	Account        *waAdv.ADVSignedDeviceIdentity `json:"Account"`
	Uids           string                         `json:"Uids"`
	AllowText      bool                           `json:"AllowText"`
	AllowCall      bool                           `json:"AllowCall"`
}

// Получает MasterKey пользователя от Landing
func (u *User) userMasterKey(userID uint32) [32]byte {
	var (
		userMasterKey [32]byte
		err           error
	)

	if u.rpc != nil {
		if userMasterKey, err = u.rpc.GetUserMasterKey(u.ctx, userID); err == nil {
			return userMasterKey
		}
	}

	logger.Warn("Не удалось получить мастер-ключ пользователя: %v", err, userID)
	return [32]byte{}
}

// Проверяет подписку пользователя
func (u *User) checkUserSubscription(userId uint32) error {
	err := com.CheckUserSubscription(u.db, userId)
	if err != nil {
		var commonErr *com.SubscriptionError
		ok := errors.As(err, &commonErr)
		if ok {
			// Форматируем сообщение, включив код ошибки
			errorCode := fmt.Sprintf("%d", commonErr.Code)
			msg := com.CarpCh{
				Event:      "subscription",
				UserName:   "",
				AssistName: "",
				Target:     errorCode,
				UserID:     userId,
			}
			err := u.end.SendNotification(msg)
			if err != nil {
				return fmt.Errorf("ошибка отправки уведомления о подписке: %v", err)
			}

			// Выключаю все каналы пользователя
			if dbErr := u.db.DisableAllUserChannel(userId); dbErr != nil {
				return fmt.Errorf("ошибка при выключении каналов: %v", dbErr)
			}

			logger.Debug("Все каналы пользователя %d выключены из-за отсутствия подписки", userId)
		} else {
			return fmt.Errorf("неизвестная ошибка проверки подписки: %v", err)
		}
		return fmt.Errorf("ошибка проверки подписки: %v", err)
	}
	return nil
}

func initContainer(ctx context.Context, l waLog.Logger) (*sqlstore.Container, error) {
	l = signalMACLogger{Logger: l}
	container, err := sqlstore.New(ctx, "sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)", l)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания SQLite: %w", err)
	}
	if container == nil {
		return nil, errors.New("sqlstore.New вернул nil")
	}
	return container, nil
}

func parseDeviceData(containerString string, userId uint32) (DeviceData, error) {
	var devData DeviceData
	if err := json.Unmarshal([]byte(containerString), &devData); err != nil {
		return devData, fmt.Errorf("ошибка разбора JSON для %d: %w", userId, err)
	}
	if devData.ID == "" || devData.NoiseKey == nil || devData.IdentityKey == nil {
		return devData, fmt.Errorf("критические поля отсутствуют для %d", userId)
	}
	return devData, nil
}

func parseJID(id string, userId uint32) (types.JID, error) {
	jid, err := types.ParseJID(id)
	if err != nil {
		return jid, fmt.Errorf("ошибка разбора JID для %d: %w", userId, err)
	}
	if jid.IsEmpty() {
		return jid, fmt.Errorf("JID пуст для %d", userId)
	}
	return jid, nil
}

func fillBaseDeviceFields(device *store.Device, devData DeviceData, jid types.JID) {
	device.ID = &jid
	device.RegistrationID = devData.RegistrationID
	device.NoiseKey = devData.NoiseKey
	device.IdentityKey = devData.IdentityKey
	device.SignedPreKey = devData.SignedPreKey
	device.Platform = devData.Platform
	device.PushName = devData.PushName
	device.AdvSecretKey = devData.AdvSecretKey
}

func ensureAdvSecretKey(device *store.Device, userId uint32) error {
	if device.AdvSecretKey == nil || len(device.AdvSecretKey) != 32 {
		newKey := make([]byte, 32)
		if _, err := rand.Read(newKey); err != nil {
			return fmt.Errorf("ошибка генерации AdvSecretKey для %d: %w", userId, err)
		}
		device.AdvSecretKey = newKey
	}
	return nil
}

func fillAccount(device *store.Device, devData DeviceData) error {
	if device.Account == nil {
		device.Account = &waAdv.ADVSignedDeviceIdentity{
			Details:             []byte{},
			AccountSignatureKey: make([]byte, 32),
			AccountSignature:    make([]byte, 64),
			DeviceSignature:     make([]byte, 64),
		}
	}
	if devData.Account != nil {
		// обработка ключей и подписей (вынести в отдельные функции)
		device.Account.Details = devData.Account.Details
		device.Account.AccountSignatureKey = normalizeKey(devData.Account.AccountSignatureKey, 32)
		device.Account.AccountSignature = normalizeSignature(devData.Account.AccountSignature, 64)
		device.Account.DeviceSignature = normalizeSignature(devData.Account.DeviceSignature, 64)
	}
	return nil
}

// normalizeKey приводит входные данные к нужной длине (например 32 байта)
func normalizeKey(data []byte, targetLen int) []byte {
	if len(data) == 0 {
		return randomBytes(targetLen)
	}
	// пробуем base64
	raw, err := base64.StdEncoding.DecodeString(string(data))
	if err == nil {
		data = raw
	}
	if len(data) == targetLen {
		return data
	}
	if len(data) > targetLen {
		return data[:targetLen]
	}
	// если меньше — хэшируем
	hash := sha256.Sum256(data)
	return hash[:targetLen]
}

// normalizeSignature приводит подпись к нужной длине (например 64 байта)
func normalizeSignature(data []byte, targetLen int) []byte {
	if len(data) == 0 {
		return randomBytes(targetLen)
	}
	// пробуем base64
	raw, err := base64.StdEncoding.DecodeString(string(data))
	if err == nil {
		data = raw
	}
	if len(data) == targetLen {
		return data
	}
	if len(data) > targetLen {
		return data[:targetLen]
	}
	// если меньше — используем два хэша подряд
	hash1 := sha256.Sum256(data)
	hash2 := sha256.Sum256(append(data, hash1[:]...))
	return append(hash1[:], hash2[:targetLen-len(hash1)]...)
}

// randomBytes генерирует случайный массив нужной длины
func randomBytes(n int) []byte {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return buf
}

func finalizeAccount(device *store.Device) {
	if device.Account.Details == nil {
		device.Account.Details = []byte{}
	}
	if len(device.Account.AccountSignatureKey) != 32 {
		device.Account.AccountSignatureKey = randomBytes(32)
	}
	if len(device.Account.AccountSignature) != 64 {
		device.Account.AccountSignature = randomBytes(64)
	}
	if len(device.Account.DeviceSignature) != 64 {
		device.Account.DeviceSignature = randomBytes(64)
	}
}

func (b *Bot) sendFirstContactMessage(senderID uint64, senderName string) {
	if cachedResponder, ok := b.responders.Load(senderID); ok {
		if responderInfo, ok := cachedResponder.(*ResponderInfo); ok && responderInfo != nil && responderInfo.Known {
			if b.redisCache != nil {
				if err := b.redisCache.Set(b.ctx, b.userID, int64(senderID)); err != nil {
					logger.Warn("Redis: ошибка продления knownResponder для senderID=%d: %v", senderID, err, b.userID)
				}
			}
			return
		}
	}

	// Ищу пользователя в кеше редис
	isFirstInteraction := true
	if b.redisCache != nil {
		exists, err := b.redisCache.Has(b.ctx, b.userID, int64(senderID))
		if err != nil {
			logger.Warn("Redis: ошибка проверки knownResponder для senderID=%d: %v", senderID, err, b.userID)
		}
		logger.Debug("Redis: проверка knownResponder для senderID=%d, exists=%v", senderID, exists, b.userID)
		isFirstInteraction = !exists
	}

	// Отправляем уведомление о начале диалога, если требуется
	if isFirstInteraction && b.assist.Events.Start {
		notifyMsg := com.CarpCh{
			Event:      "start",
			UserName:   senderName,
			AssistName: b.assist.AssistName,
			Target:     "",
			UserID:     b.userID,
		}
		err := b.end.SendNotification(notifyMsg)
		if err != nil {
			logger.Error("Ошибка отправки уведомления о первом взаимодействии: %v", err, b.userID)
		}
	}

	// Сохраняю пользователя в редис после обработки
	if b.redisCache != nil {
		err := b.redisCache.Set(b.ctx, b.userID, int64(senderID))
		if err != nil {
			logger.Warn("Redis: ошибка сохранения knownResponder для senderID=%d: %v", senderID, err, b.userID)
		}
	}

	responderInfo, ok := b.responders.Load(senderID)
	if !ok {
		b.responders.Store(senderID, &ResponderInfo{Known: true})
		return
	}

	info, ok := responderInfo.(*ResponderInfo)
	if !ok || info == nil {
		b.responders.Store(senderID, &ResponderInfo{Known: true})
		return
	}

	info.Known = true
	b.responders.Store(senderID, info)
}

func (b *Bot) preloadFirstInteraction() {
	if b.redisCache == nil {
		return
	}

	senderIDs, err := b.redisCache.LoadUser(b.ctx, b.userID)
	if err != nil {
		logger.Warn("Redis: не удалось прогреть knownResponder: %v", err, b.userID)
		return
	}

	for _, senderID := range senderIDs {
		info := &ResponderInfo{
			Known: true,
		}
		b.responders.Store(senderID, info)
	}

	if len(senderIDs) > 0 {
		logger.Debug("Redis: прогрето knownResponder=%d", len(senderIDs), b.userID)
	}
}
