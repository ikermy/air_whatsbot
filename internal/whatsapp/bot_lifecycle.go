package whatsapp

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	callspkg "air_whatsbot/internal/whatsapp/calls"
	devicestore "air_whatsbot/internal/whatsapp/devicestore"
	responder "air_whatsbot/internal/whatsapp/responder"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"
)

func (b *Bot) resetSession() error {
	b.stopCalls("session_reset")
	b.b.Disconnect()

	// Устанавливаем флаг остановки атомарно
	b.stopped.Store(true)

	// Удаляем device из SQLite store перед повторной авторизацией
	if b.container != nil && b.container.Container != nil {
		// Получаем JID устройства
		if b.b.Store != nil && b.b.Store.ID != nil {
			jid := *b.b.Store.ID
			logger.Debug("Удаление device из SQLite store для JID: %s", jid.String(), b.userID)

			// Удаляем устройство из store
			err := b.container.Container.DeleteDevice(b.ctx, b.b.Store)
			if err != nil {
				logger.Warn("Ошибка удаления device из store: %v", err, b.userID)
			} else {
				logger.Debug("Device успешно удален из SQLite store", b.userID)
			}
		}
	}

	// Отменяем текущий контекст
	if b.cancel != nil {
		b.cancel()
	}

	return nil
}

// GetWaUserBotUsers получает данные пользователей из БД и обновляет состояние ботов

func (b *Bot) StopUserBot() {
	// Используем atomic.Bool.CompareAndSwap для атомарной проверки и установки
	if !b.stopped.CompareAndSwap(false, true) {
		// Бот уже был остановлен
		return
	}

	b.stopCalls("bot_stopped")

	if b.cancel != nil {
		b.cancel()
	}

	if b.b != nil {
		b.b.Disconnect()
	}

	logger.Info("Bot остановлен", b.userID)
}

// parseWaUserBotContainer — основной метод

func (u *User) createOrUpdateBot(userID uint32, containerData *devicestore.Store, assist *model.Assistant) (*Bot, error) {
	// Проверяем, существует ли уже такой бот
	if value, exists := u.uBot.Load(userID); exists {
		existingBot := value.(*Bot)
		// Если бот существует, всегда останавливаем его
		if existingBot.cancel != nil {
			//fmt.Printf("`createOrUpdateBot`: пересоздание бота для пользователя %d\n", userID)
			existingBot.cancel()
		}
		// Ждем немного, чтобы горутины завершились
		time.Sleep(100 * time.Millisecond)
	}

	// Создаем новый бот
	return u.initializeBot(userID, containerData, assist, true)
}

// Инициализирует нового бота с заданными параметрами
func (u *User) initializeBot(userID uint32, tokenData *devicestore.Store, assist *model.Assistant, autoConnect bool) (*Bot, error) {
	// Создаем логгер для этого пользователя
	//logger := waLog.Stdout(fmt.Sprintf("WhatsApp[%d]", userID), "INFO", true)

	client := whatsmeow.NewClient(tokenData.GetDevice(), nil)
	// meowcaller устанавливает низкоуровневый call hook, поэтому его нужно
	// создать до подключения whatsmeow. Обработчики звонков пока не включаем.
	callClient := meowcaller.NewClient(client)

	// Создаем клиент WhatsApp, используя устройство

	// Инициализируем CRM для этого пользователя
	crmUser, debug, err := u.crm.Init(userID)
	if err != nil {
		// Может быть не ошибка, просто не настроена или отключена CRM
		logger.Debug("Ошибка инициализации CRM: %v", err, userID)
	}

	if debug != "" {
		logger.Debug("User инициализирован с настройками: %s", debug, userID)
	}

	// Создаем контекст для бота
	botCtx, botCancel := context.WithCancel(u.ctx)

	// Создаем нового бота
	bot := &Bot{
		userID:     userID,
		b:          client,
		callClient: callClient,
		ctx:        botCtx,
		cancel:     botCancel,
		container:  tokenData,
		// stopped: atomic.Bool инициализируется в false по умолчанию
		assist:         assist,
		responders:     responder.NewStore(u.responderCache),
		end:            u.end,
		db:             u.db,
		mod:            u.mod,
		c:              crmUser,
		start:          u.start,
		parent:         u,
		offlineMsgSync: make(chan struct{}), // Канал для синхронизации оффлайн сообщений
		typingCancels:  sync.Map{},
		voiceCall:      tokenData.Call,
		textMessage:    tokenData.Text,
	}
	bot.calls = callspkg.NewManager(bot)

	// Гружу кеш первого взаимодействия из Redis
	go bot.preloadFirstInteraction()

	// Инициализируем поле uids на основе tokenData.Uids
	if tokenData.Uids != "" {
		uidsStr := strings.Fields(tokenData.Uids)
		uids := make([]int64, 0, len(uidsStr))

		for _, uidStr := range uidsStr {
			uid, err := strconv.ParseInt(uidStr, 10, 64)
			if err != nil {
				logger.Error("Не удалось преобразовать UID '%s': %v", uidStr, err, userID)
				continue
			}
			uids = append(uids, uid)
		}

		bot.uids = uids
		logger.Debug("Загружено %d разрешенных UIDs", len(uids), userID)
	}

	// Регистрируем обработчик событий
	client.AddEventHandler(bot.handleEvent)
	bot.registerCallHandlers()

	// Добавляем промежуточный обработчик для сохранения состояния
	client.AddEventHandler(func(evt any) {
		// После определенных событий сохраняем состояние в JSONDeviceStore
		switch evt.(type) {

		case *events.AppStateSyncComplete:
			//logger.Debug("WhatsApp: пользователь %d: завершена синхронизация состояния %s",
			//	userID, v.Name, userID)

		case *events.Connected:
			// Не блокируем обработку новых сообщений, если клиент не прислал
			// OfflineSyncCompleted (например, после повторной авторизации).
			bot.offlineSyncOnce.Do(func() { close(bot.offlineMsgSync) })

			// Перехватываем событие подключения и сохраняем состояние самостоятельно
			// Получаю mk для шифрования данных авторизации бота в ДБ
			if err := tokenData.SaveJSON(u.ctx, userID, u.userMasterKey(userID), false); err != nil {
				logger.Error("WhatsApp: ошибка сохранения состояния устройства: %v", err, userID)
			}
		}
	})

	if !autoConnect {
		return bot, nil
	}

	// Запускаем подключение в отдельной горутине
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Паника при подключении бота: %v", r, userID)
			}
		}()

		// Подключаемся к серверу WhatsApp
		if err := bot.Connect(); err != nil {
			// Не авторизован, получаем QR-канал ПЕРЕД подключением
			logger.Info("WhatsApp: требуется авторизация через QR-код", userID)
			// Отправляем событие о необходимости авторизации
			msg := com.CarpCh{
				Event:      "reauth",
				UserName:   "",
				AssistName: "",
				Target:     "WhatsApp User",
				UserID:     userID,
			}
			err := u.end.SendNotification(msg)
			if err != nil {
				logger.Error("Ошибка отправки уведомления о требовании авторизации: %v", err, userID)
			}
			//common.SendEvent(userID, "reauth", "", "", "WhatsApp User")
			// Отключаю канал
			err = u.db.SetChannelEnabled(userID, whatsAppChannelName, false)
			if err != nil {
				logger.Error("Ошибка при отключении канала: %v", err, userID)
			} else {
				logger.Debug("Канал WhatsApp отключен", userID)
			}
			// Помечаем бота как остановленный
			bot.stopped.Store(true)
			return
		}

		// ВАЖНО: Запускаем обработчик событий независимо от статуса подключения
		if client.IsLoggedIn() {
			logger.Info("WhatsApp клиент успешно подключен как %s", client.Store.ID.String(), userID)

			//// Принудительно запускаем синхронизацию и очистку проблемных сессий
			//go bot.handleCryptoErrors()
		}
	}()

	return bot, nil
}

func (b *Bot) Connect() error {
	// Проверяем статус авторизации перед подключением
	if b.b.Store.ID != nil {
		//logger.Debug("Попытка подключения с сохранённой сессией...", w.UserId)
		err := b.b.Connect()
		if err == nil {
			logger.Info("WhatsApp клиент успешно подключен как %s", b.b.Store.ID.String(), b.userID)
			return nil
		}

		// Если ошибка не связана с авторизацией, возвращаем её
		if !errors.Is(err, whatsmeow.ErrNotLoggedIn) {
			return fmt.Errorf("ошибка подключения: %w", err)
		}
	}

	return fmt.Errorf("требуется авторизация для пользователя")
}

// GetBotUsername возвращает информацию об имени запущенного бота
