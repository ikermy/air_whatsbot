package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"air_whatsbot/internal/whatsapp/devicestore"

	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow"
)

// Bot — минимальный порт авторизуемого бота.
type Bot interface {
	UserID() uint32
	WhatsAppClient() *whatsmeow.Client
	Device() *devicestore.Store
	// PrepareReauth сбрасывает текущую сессию и пересоздаёт клиентов перед повторной авторизацией.
	PrepareReauth()
	// SetAuthSettings синхронизирует настройки каналов временного бота и хранилища.
	SetAuthSettings(allowText, allowCall bool)
}

// Host — порт владельца реестра ботов, предоставляющий авторизации доступ к
// хранилищу, мастер-ключу и запуску рабочего бота.
type Host interface {
	Context() context.Context
	EnsureAuthBot(ctx context.Context, userID uint32) (Bot, error)
	DeleteBot(userID uint32)
	StartUserBot(userID uint32) error
	MasterKey(userID uint32) [32]byte
}

// AuthenticateWithQR прогоняет полный цикл QR-авторизации пользователя и
// публикует состояния в stateChan.
func AuthenticateWithQR(host Host, userID uint32, stateChan chan<- State) error {
	authCtx, authCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer authCancel()

	safeStateSend := func(state State) {
		select {
		case stateChan <- state:
		default:
			logger.Error("Не удалось отправить состояние: канал заполнен или закрыт")
		}
	}

	bot, err := host.EnsureAuthBot(authCtx, userID)
	if err != nil {
		logger.Error("Ошибка инициализации бота: %v", err, userID)
		safeStateSend(State{
			Type:    "error",
			Payload: fmt.Sprintf("Ошибка инициализации бота: %v", err),
		})
		return fmt.Errorf("ошибка инициализации бота: %w", err)
	}
	if bot == nil {
		return fmt.Errorf("бот не найден для пользователя %d", userID)
	}

	client := bot.WhatsAppClient()
	if client == nil {
		return fmt.Errorf("WhatsApp-клиент не инициализирован для пользователя %d", userID)
	}

	// Проверяем статус авторизации перед подключением
	if client.Store.ID != nil {
		logger.Debug("Бот уже авторизован, выполняем сброс для повторной авторизации", userID)
		bot.PrepareReauth()
		client = bot.WhatsAppClient()
		logger.Debug("Клиент пересоздан для повторной авторизации", userID)
	}

	// Каналы для обмена данными
	doneChan := make(chan struct{}, 1)
	errChan := make(chan error, 1)

	// Получение QR-канала
	qrChan, err := client.GetQRChannel(authCtx)
	if err != nil {
		logger.Error("Ошибка получения канала QR-кода: %v", err, userID)
		safeStateSend(State{
			Type:    "error",
			Payload: fmt.Sprintf("Ошибка получения канала QR-кода: %v", err),
		})
		return fmt.Errorf("ошибка получения канала QR-кода: %w", err)
	}

	// Подключение к серверу WhatsApp
	err = client.Connect()
	if err != nil && !errors.Is(err, whatsmeow.ErrNotLoggedIn) {
		logger.Error("Ошибка подключения к WhatsApp: %v", err, userID)
		safeStateSend(State{
			Type:    "error",
			Payload: fmt.Sprintf("Ошибка подключения к серверу WhatsApp: %v", err),
		})
		return fmt.Errorf("ошибка подключения к серверу WhatsApp: %w", err)
	}

	// Обработка событий QR-кода
	var qrCodeDisplayed bool
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			qrCodeDisplayed = true
			safeStateSend(State{
				Type:    "qr_code",
				Payload: evt.Code,
			})

		case "timeout":
			if !qrCodeDisplayed {
				safeStateSend(State{
					Type:    "error",
					Payload: "Не удалось получить QR-код от сервера",
				})
				client.Disconnect()
				return fmt.Errorf("таймаут получения QR-кода от сервера")
			}
			safeStateSend(State{
				Type:    "error",
				Payload: "Время ожидания сканирования QR-кода истекло",
			})
			client.Disconnect()
			return fmt.Errorf("время ожидания сканирования QR-кода истекло")

		case "error":
			safeStateSend(State{
				Type:    "error",
				Payload: fmt.Sprintf("Ошибка в процессе авторизации: %v", evt.Error),
			})
			client.Disconnect()
			return fmt.Errorf("ошибка в канале QR: %v", evt.Error)

		case "success":
			logger.Info("WhatsApp: Завершение авторизации...", userID)
			safeStateSend(State{
				Type:    "qr-success",
				Payload: "QR-код отсканирован, завершение авторизации...",
			})

			// Переходим к проверке авторизации через контролируемое ожидание
			go func() {
				// Даем время на завершение процесса авторизации
				time.Sleep(3 * time.Second)

				isLoggedIn := client.IsLoggedIn()
				jid := ""
				if client.Store.ID != nil {
					jid = client.Store.ID.String()
				}

				if isLoggedIn {
					safeStateSend(State{
						Type:    "success",
						Payload: fmt.Sprintf("Авторизация успешно завершена: %s", jid),
					})
					doneChan <- struct{}{}
				} else {
					errChan <- fmt.Errorf("авторизация не удалась, клиент не вошел в систему после обработки QR")
				}
			}()

		default:
			logger.Debug("Login event: %s", evt.Event, userID)
		}
	}

	// Ожидаем завершение авторизации
	select {
	case <-doneChan:
		if !client.IsLoggedIn() {
			logger.Warn("WhatsApp: QR-код был отсканирован, но авторизация не завершилась", userID)
			safeStateSend(State{
				Type:    "error",
				Payload: "Авторизация не удалась, клиент не вошел в систему после обработки QR",
			})
			client.Disconnect()
			return fmt.Errorf("авторизация не удалась, клиент не вошел в систему после обработки QR")
		}

		if err := bot.Device().SaveJSON(host.Context(), userID, host.MasterKey(userID), true); err != nil {
			logger.Error("Предупреждение: не удалось сохранить данные устройства: %v", err, userID)
		}
		// SaveJSON при firstAuthorization записывает значения по умолчанию
		// только в БД. Синхронизируем также поля временного бота, чтобы они
		// не оставались false до создания рабочего бота.
		bot.SetAuthSettings(true, true)
		logger.Debug("Авторизация завершена с настройками токена: AllowText=%t AllowCall=%t", true, true, userID)

		client.Disconnect()

		// Удаляем временный бот из памяти
		host.DeleteBot(userID)
		logger.Debug("Временный бот авторизации удален из памяти", userID)

		// После QR-авторизации создаем и запускаем полноценного бота.
		if err := host.StartUserBot(userID); err != nil {
			logger.Error("Не удалось запустить бота после авторизации: %v", err, userID)
			return fmt.Errorf("не удалось запустить бота после авторизации: %w", err)
		}
		return nil

	case err := <-errChan:
		safeStateSend(State{
			Type:    "error",
			Payload: err.Error(),
		})
		client.Disconnect()
		return err

	case <-authCtx.Done():
		safeStateSend(State{
			Type:    "error",
			Payload: "Превышено время ожидания авторизации",
		})
		client.Disconnect()
		return fmt.Errorf("превышено время ожидания авторизации")
	}
}
