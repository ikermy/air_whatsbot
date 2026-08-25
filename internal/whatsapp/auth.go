package whatsapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// AuthState представляет состояние процесса аутентификации
type AuthState struct {
	Type    string `json:"type"`    // Тип сообщения: "qr_code", "success", "error"
	Payload string `json:"payload"` // Содержимое сообщения: QR-код URL, текст ошибки и т.д.
}

// AuthSession Структура для хранения каналов сессии аутентификации
// Добавляем поле UserId для идентификации пользователя
type AuthSession struct {
	StateChan chan AuthState
	UserId    uint32 // Добавляем поле UserId для идентификации пользователя
}

// Глобальная карта активных сессий аутентификации
var authSessions = struct {
	sync.RWMutex
	sessions map[string]*AuthSession
}{
	sessions: make(map[string]*AuthSession),
}

// CleanupExistingAuthSessions очищает существующие сессии аутентификации для данного userId
func (u *User) CleanupExistingAuthSessions(userId uint32) {
	authSessions.Lock()
	defer authSessions.Unlock()
	for sessionID, session := range authSessions.sessions {
		if session.UserId == userId {
			// Оповещаем о завершении и удаляем сессию
			select {
			case session.StateChan <- AuthState{
				Type:    "cancelled",
				Payload: "Сессия сброшена из-за новой авторизации",
			}:
				// отправлено
			default:
				// канал переполнен или закрыт
			}
			close(session.StateChan)
			delete(authSessions.sessions, sessionID)
		}
	}
}

func (u *User) AuthenticateWithQRForWeb(userID uint32, stateChan chan<- AuthState) error {
	// Создаем отдельный контекст с таймаутом для авторизации
	authCtx, authCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer authCancel() // Гарантируем очистку после завершения авторизации

	// Функция для безопасной отправки в канал
	safeStateSend := func(state AuthState) {
		select {
		case stateChan <- state:
			// Успешно отправлено
		default:
			logger.Error("Не удалось отправить состояние: канал заполнен или закрыт")
		}
	}

	// Проверяем существование бота и создаем его при необходимости
	if _, exists := u.uBot.Load(userID); !exists {
		logger.Debug("Инициализация нового бота", userID)

		// Создаем JSONDeviceStore для нового бота
		l := waLog.Stdout(fmt.Sprintf("WhatsApp[%d]", userID), "DEBUG", true)
		deviceStore, err := NewJSONDeviceStore(authCtx, l, u.db)
		if err != nil {
			logger.Error("Ошибка создания хранилища устройства: %v", err, userID)
			safeStateSend(AuthState{
				Type:    "error",
				Payload: fmt.Sprintf("Ошибка создания хранилища: %v", err),
			})
			return fmt.Errorf("ошибка создания хранилища: %w", err)
		}

		// QR-авторизация сама по себе не содержит настройки бота.
		// Восстанавливаем AllowText/AllowCall из сохранённого token JSON,
		// чтобы пересоздание клиента не сбрасывало ограничения каналов.
		if userData, loadErr := u.db.GetWaUser(authCtx, userID); loadErr == nil && userData != nil && userData.SessionData != "" {
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
				deviceStore.text = tokenSettings.AllowText
				deviceStore.call = tokenSettings.AllowCall
				deviceStore.Uids = tokenSettings.Uids
				logger.Debug("Настройки токена восстановлены при QR-авторизации: AllowText=%t AllowCall=%t", deviceStore.text, deviceStore.call, userID)
			}
		}

		// Создаем базовую структуру ассистента
		assist := model.Assistant{UserID: userID}

		// Инициализируем бота
		bot, err := u.initializeBot(userID, deviceStore, &assist)
		if err != nil {
			logger.Error("Ошибка инициализации бота: %v", err, userID)
			safeStateSend(AuthState{
				Type:    "error",
				Payload: fmt.Sprintf("Ошибка инициализации бота: %v", err),
			})
			return fmt.Errorf("ошибка инициализации бота: %w", err)
		}

		// Сохраняем бота в sync.Map
		u.uBot.Store(userID, bot)
	}

	// Получаем бота из sync.Map
	value, exists := u.uBot.Load(userID)
	if !exists {
		return fmt.Errorf("бот не найден для пользователя %d", userID)
	}
	bot := value.(*Bot)

	// Проверяем статус авторизации перед подключением
	isAuthenticated := bot.b.Store.ID != nil
	if isAuthenticated {
		logger.Debug("Бот уже авторизован, выполняем сброс для повторной авторизации", userID)

		bot.stopCalls("reauthentication")
		// Отключаем текущий клиент
		bot.b.Disconnect()

		// Удаляем данные из Store (очищаем ID пользователя)
		bot.b.Store.ID = nil

		// Создаем новый клиент с очищенным Store
		bot.b = whatsmeow.NewClient(bot.container.GetDevice(), nil)
		meowLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().
			Str("component", "meowcaller").Logger()
		logger.Info("Инициализация meowcaller: ожидается MLow PCM 16 kHz mono; фактический negotiated codec будет записан библиотекой", bot.userID)
		bot.callClient = meowcaller.NewClient(bot.b, meowcaller.WithLogger(meowLog))

		// Регистрируем обработчик событий для нового клиента
		bot.b.AddEventHandler(bot.handleEvent)
		bot.registerCallHandlers()

		logger.Debug("Клиент пересоздан для повторной авторизации", userID)
	}

	// Каналы для обмена данными
	doneChan := make(chan struct{}, 1)
	errChan := make(chan error, 1)

	// Получение QR-канала
	qrChan, err := bot.b.GetQRChannel(authCtx)

	if err != nil {
		logger.Error("Ошибка получения канала QR-кода: %v", err, userID)
		safeStateSend(AuthState{
			Type:    "error",
			Payload: fmt.Sprintf("Ошибка получения канала QR-кода: %v", err),
		})
		return fmt.Errorf("ошибка получения канала QR-кода: %w", err)
	}

	// Подключение к серверу WhatsApp
	err = bot.b.Connect()

	if err != nil && !errors.Is(err, whatsmeow.ErrNotLoggedIn) {
		logger.Error("Ошибка подключения к WhatsApp: %v", err, userID)
		safeStateSend(AuthState{
			Type:    "error",
			Payload: fmt.Sprintf("Ошибка подключения к серверу WhatsApp: %v", err),
		})
		return fmt.Errorf("ошибка подключения к серверу WhatsApp: %w", err)
	}

	//// Информируем клиента, что ожидаем QR-код
	//safeStateSend(AuthState{
	//	Type:    "info",
	//	Payload: "Подготовка QR-кода для сканирования...",
	//})

	// Обработка событий QR-кода
	var qrCodeDisplayed bool
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			safeStateSend(AuthState{
				Type:    "qr_code",
				Payload: evt.Code,
			})

		case "timeout":
			if !qrCodeDisplayed {
				safeStateSend(AuthState{
					Type:    "error",
					Payload: "Не удалось получить QR-код от сервера",
				})
				bot.b.Disconnect()
				return fmt.Errorf("таймаут получения QR-кода от сервера")
			}
			safeStateSend(AuthState{
				Type:    "error",
				Payload: "Время ожидания сканирования QR-кода истекло",
			})
			bot.b.Disconnect()
			return fmt.Errorf("время ожидания сканирования QR-кода истекло")

		case "error":
			safeStateSend(AuthState{
				Type:    "error",
				Payload: fmt.Sprintf("Ошибка в процессе авторизации: %v", evt.Error),
			})
			bot.b.Disconnect()
			return fmt.Errorf("ошибка в канале QR: %v", evt.Error)

		case "success":
			logger.Info("WhatsApp: Завершение авторизации...", userID)
			safeStateSend(AuthState{
				Type:    "qr-success",
				Payload: "QR-код отсканирован, завершение авторизации...",
			})

			// Переходим к проверке авторизации через контролируемое ожидание
			go func() {
				// Даем время на завершение процесса авторизации
				time.Sleep(3 * time.Second)

				isLoggedIn := bot.b.IsLoggedIn()
				jid := ""
				if bot.b.Store.ID != nil {
					jid = bot.b.Store.ID.String()
				}

				if isLoggedIn {
					safeStateSend(AuthState{
						Type:    "success",
						Payload: fmt.Sprintf("Авторизация успешно завершена: %s", jid),
					})
					doneChan <- struct{}{}
				} else {
					errChan <- fmt.Errorf("авторизация не удалась, клиент не вошел в систему после обработки QR")
				}
			}()
			break

		default:
			logger.Debug("Login event: %s", evt.Event, userID)
		}
	}

	// Ожидаем завершение авторизации
	select {
	case <-doneChan:
		isLoggedIn := bot.b.IsLoggedIn()

		if !isLoggedIn {
			logger.Warn("WhatsApp: QR-код был отсканирован, но авторизация не завершилась", userID)
			safeStateSend(AuthState{
				Type:    "error",
				Payload: "Авторизация не удалась, клиент не вошел в систему после обработки QR",
			})
			bot.b.Disconnect()
			return fmt.Errorf("авторизация не удалась, клиент не вошел в систему после обработки QR")
		}

		if err := bot.container.SaveJSON(u.ctx, userID, u.userMasterKey(userID), true); err != nil {
			logger.Error("Предупреждение: не удалось сохранить данные устройства: %v", err, userID)
		}
		logger.Debug("Авторизация завершена с настройками токена: AllowText=%t AllowCall=%t", bot.textMessage, bot.voiceCall, userID)

		bot.b.Disconnect()

		// Удаляем временный бот из памяти
		u.uBot.Delete(userID)
		logger.Debug("Временный бот авторизации удален из памяти", userID)
		return nil

	case err := <-errChan:
		safeStateSend(AuthState{
			Type:    "error",
			Payload: err.Error(),
		})
		bot.b.Disconnect()
		return err

	case <-authCtx.Done():
		safeStateSend(AuthState{
			Type:    "error",
			Payload: "Превышено время ожидания авторизации",
		})
		bot.b.Disconnect()
		return fmt.Errorf("превышено время ожидания авторизации")
	}
}
