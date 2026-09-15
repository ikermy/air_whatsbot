package whatsapp

import (
	"context"
	"encoding/json"
	"fmt"

	authpkg "air_whatsbot/internal/whatsapp/auth"
	"air_whatsbot/internal/whatsapp/devicestore"

	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// User реализует auth.Host.

func (u *User) Context() context.Context { return u.ctx }

func (u *User) MasterKey(userID uint32) [32]byte { return u.userMasterKey(userID) }

func (u *User) DeleteBot(userID uint32) { u.uBot.Delete(userID) }

// EnsureAuthBot возвращает существующего бота или создаёт временного для QR-авторизации.
func (u *User) EnsureAuthBot(ctx context.Context, userID uint32) (authpkg.Bot, error) {
	if value, exists := u.uBot.Load(userID); exists {
		if bot, ok := value.(*Bot); ok && bot != nil {
			return bot, nil
		}
	}

	logger.Debug("Инициализация нового бота", userID)

	l := waLog.Stdout(fmt.Sprintf("WhatsApp[%d]", userID), "DEBUG", true)
	deviceStore, err := devicestore.New(ctx, l, u.db)
	if err != nil {
		logger.Error("Ошибка создания хранилища устройства: %v", err, userID)
		return nil, fmt.Errorf("ошибка создания хранилища: %w", err)
	}

	// QR-авторизация сама по себе не содержит настройки бота.
	// Восстанавливаем AllowText/AllowCall из сохранённого token JSON,
	// чтобы пересоздание клиента не сбрасывало ограничения каналов.
	if userData, loadErr := u.db.GetWaUser(ctx, userID); loadErr == nil && userData != nil && userData.SessionData != "" {
		rawSession := userData.SessionData
		if decrypted, decryptErr := u.decryptSessionData(userID, rawSession); decryptErr == nil {
			rawSession = decrypted
		}
		var tokenSettings struct {
			AllowText bool   `json:"AllowText"`
			AllowCall bool   `json:"AllowCall"`
			Uids      string `json:"Uids"`
		}
		if parseErr := json.Unmarshal([]byte(rawSession), &tokenSettings); parseErr == nil {
			deviceStore.Text = tokenSettings.AllowText
			deviceStore.Call = tokenSettings.AllowCall
			deviceStore.Uids = tokenSettings.Uids
			logger.Debug("Настройки токена восстановлены при QR-авторизации: AllowText=%t AllowCall=%t", deviceStore.Text, deviceStore.Call, userID)
		}
	}

	// Создаем базовую структуру ассистента
	assist := model.Assistant{UserID: userID}

	// Инициализируем бота
	// Авторизационный бот подключается ниже через GetQRChannel/Connect.
	// Не запускаем автоматическое подключение из initializeBot, иначе
	// появляются два параллельных сценария авторизации.
	bot, err := u.initializeBot(userID, deviceStore, &assist, false)
	if err != nil {
		logger.Error("Ошибка инициализации бота: %v", err, userID)
		return nil, fmt.Errorf("ошибка инициализации бота: %w", err)
	}

	u.uBot.Store(userID, bot)
	return bot, nil
}
