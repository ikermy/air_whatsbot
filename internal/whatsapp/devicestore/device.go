package devicestore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
	hash := sha256.Sum256(data)
	return hash[:targetLen]
}

// normalizeSignature приводит подпись к нужной длине (например 64 байта)
func normalizeSignature(data []byte, targetLen int) []byte {
	if len(data) == 0 {
		return randomBytes(targetLen)
	}
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
