package whatsapp

import (
	deliveryhttp "air_whatsbot/internal/delivery/http"
	"air_whatsbot/internal/domain"
	"air_whatsbot/internal/metrics"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/comdb"
	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-common/pkg/crm"
	"github.com/ikermy/air-common/pkg/crypto"
	"github.com/ikermy/air-common/pkg/endpoint"
	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-common/pkg/operator"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
	"github.com/redis/go-redis/v9"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func (u *User) webHook() {
	if err := deliveryhttp.NewServer(u).ListenAndServe(":8080"); err != nil {
		logger.Fatalf("ошибка запуска сервера аутентификации: %v", err)
	}
}

const messageIgnoreTime = 30 * time.Second // Время игнорирования сообщений после подключения

type IntDB interface {
	UpdateWhatsBotData(ctx context.Context, userId uint32, mk [32]byte, data string, enabled bool) error
	GetWaUserBotUsers(ctx context.Context) ([]domain.WaUserBotData, error)
	GetWaUser(ctx context.Context, userId uint32) (*domain.WaUserBotData, error)
}

type DB interface {
	ExtDB
	IntDB
}

type Model = model.Inter
type Endpoint = endpoint.Inter
type Operator = operator.Inter
type ExtDB = comdb.Exterior
type CRM = crm.Inter

type ORCClient interface {
	GetUserMasterKey(ctx context.Context, userId uint32) ([32]byte, error)
}

var StartCh = make(chan model.StartCh, 100) // Канал для запуска горутины слушателя

// ResponderInfo хранит информацию о респонденте (собеседнике)
type ResponderInfo struct {
	JID       types.JID // Оригинальный JID для отправки сообщений (может быть @lid или @s.whatsapp.net)
	RealPhone string    // Реальный телефон из SenderAlt для CRM
	Known     bool      // Флаг первого взаимодействия
}

type Bot struct {
	userID      uint32
	b           *whatsmeow.Client
	callClient  *meowcaller.Client
	activeCalls sync.Map // key: meowcaller call ID, value: *callSession
	ctx         context.Context
	cancel      context.CancelFunc
	container   *JSONDeviceStore
	stopped     atomic.Bool // Флаг остановки (потокобезопасный)
	// Модель ассистента
	assist *model.Assistant
	// Карта для хранения информации о респондентах (собеседниках)
	responders sync.Map // key: uint64 (senderID), value: *ResponderInfo
	// Список ид пользователей для которых бот будет работать
	uids            []int64
	messageReceived chan struct{}
	end             Endpoint
	db              DB
	mod             Model
	c               *crm.User
	// Канал для ожидания синхронизации
	syncComplete chan struct{}
	// время успешного подключения для игнорирования сообщений из истории
	connectedAt time.Time
	// Статус печатает до ответа ассистента
	typingCancels   sync.Map // key: uint64 (senderID), value: context.CancelFunc
	parent          *User
	offlineMsgSync  chan struct{} // Завершена синхронизация оффлайн сообщений
	offlineSyncOnce sync.Once
	// Редиско кеширование первого взаимодействия
	redisCache CacheMethods // при первоначальной загрузке тащит первые контакты из redis
	// Метка для хранения пользователей которые уже взаимодействовали с ботом находится в responders
	//responders ResponderInfo
	voiceCall   bool // Поддержка входящих вызовов (включается в настройках токена бота)
	textMessage bool // Поддержка входящих текстовых сообщений (включается в настройках токена бота)
}

// User представляет все пользовательские WhatsApp боты
type User struct {
	ctx    context.Context
	cancel context.CancelFunc
	db     DB
	mod    Model
	end    Endpoint
	crm    CRM
	uBot   sync.Map // key: uint32 (userID), value: *Bot
	// Включение операторского режима
	operatorModeByDialog sync.Map // key: dialogId (uint64), value: bool
	op                   Operator
	// Для управления кешированием первого взаимодействия
	redisCache CacheMethods
	// Получение ключа пользователя
	rpc         ORCClient
	masterKeyMu sync.Mutex
	masterKeys  map[uint32]cachedMasterKey
	// Канал для запуска горутины слушателя
	StartCh chan model.StartCh
}

// JSONDeviceStore использует in-memory SQLite с сохранением в JSON
type JSONDeviceStore struct {
	container *sqlstore.Container
	device    *store.Device
	logger    waLog.Logger
	mu        sync.Mutex
	db        DB
	Uids      string
	call      bool // Разрешение отвечать на голосовые вызовы
	text      bool // Разрешение отвечать на текстовые сообщения
}

func New(parent context.Context, d DB, m Model, e Endpoint, c CRM, o ORCClient, redisClient redis.UniversalClient) *User {
	ctx, cancel := context.WithCancel(parent)
	return &User{
		ctx:        ctx,
		cancel:     cancel,
		end:        e,
		db:         d,
		mod:        m,
		crm:        c,
		rpc:        o,
		masterKeys: make(map[uint32]cachedMasterKey),
		redisCache: newRedisKnownResponderCache(redisClient),
		StartCh:    make(chan model.StartCh, 100),
	}
}

func (u *User) SetOperator(op Operator) { u.op = op }

// DisableOperatorMode отключает режим оператора и уведомляет AI-модель
// вызывается из Startpoints при получении команды от оператора
func (u *User) DisableOperatorMode(userId uint32, dialogId uint64, silent ...bool) error {
	// Определяем значение silent (по умолчанию false)
	isSilent := false
	if len(silent) > 0 {
		isSilent = silent[0]
	}

	// 1. Выключаем режим оператора
	u.SetOperatorMode(dialogId, false)
	logger.Debug("Выключен режим оператора для диалога %d", dialogId, userId)

	// 2. Находим respId по dialogId
	respId, err := u.mod.GetRespIdByDialogID(dialogId)
	if err != nil {
		logger.Error("Не удалось найти respId для dialogId %d: %v", dialogId, err)
		return err // Если не нашли, то и сообщение отправить не сможем
	}

	// 3. Получаем экземпляр бота
	value, ok := u.uBot.Load(userId)
	if !ok {
		logger.Error("Бот для userId %d не найден, не могу отправить сообщение", userId)
		return fmt.Errorf("бот не найден")
	}
	waBot := value.(*Bot)
	if waBot == nil || waBot.b == nil {
		logger.Error("Бот для userId %d не инициализирован", userId)
		return fmt.Errorf("бот не инициализирован")
	}

	// 4. Отправляем сообщение напрямую через WhatsApp
	strId := strconv.FormatUint(respId, 10)
	jid, err := types.ParseJID(strId + "@s.whatsapp.net")
	if err != nil {
		logger.Error("Ошибка создания JID для пользователя %d: %v", respId, err, userId)
		return err
	}

	// 4. Отправляем сообщение пользователю только если не silent режим
	if !isSilent {
		_, err = waBot.b.SendMessage(waBot.ctx, jid, &waE2E.Message{
			//Conversation: proto.String("Оператор отключился. Маруся AI снова с вами!"),
			Conversation: proto.String(u.end.TranslateMessageWithUserID(userId, "operator.disconnected")),
		})
		if err != nil {
			logger.Error("Ошибка отправки сообщения о выключении оператора пользователю %d: %v", respId, err, userId)
			return err
		}
	}

	// 6. Уведомляем AI-модель о возобновлении работы
	//usrCh, err := u.mod.GetCh(respId)
	//if err != nil {
	//	logger.Error("Каналы для respId %d не найдены при отключении оператора: %v", respId, err)
	//	return err
	//}
	//
	//systemName := "assist"
	//operatorOffMsg := u.mod.NewMessage(
	//	model.Operator{SetOperator: false, Operator: false},
	//	"assist",
	//	//&model.AssistResponse{Message: "Режим оператора отключен, возобновляю работу AI"},
	//	&model.AssistResponse{Message: u.end.TranslateMessageWithUserID(userId, "operator.mode.is.disabled")},
	//	&systemName,
	//)
	//
	//if err := u.trySendToRxCh(usrCh, operatorOffMsg); err != nil {
	//	logger.Error("Не удалось отправить системное сообщение в модель о выключении оператора: %v", err)
	//}

	// 7. Закрываем SSE соединение для оператора, если есть хотя это уже должно быть сделано в air_oper
	if u.op != nil {
		if err := u.op.CloseOperatorSSE(u.ctx, userId, dialogId); err != nil {
			// Логируем ошибку, но не прерываем процесс, так как это не критично
			logger.Error("Не удалось закрыть SSE сессию оператора: %v", err)
		}
	}

	return nil
}

// trySendToRxCh пытается отправить сообщение в RxCh используя безопасный метод из v1.26.0
func (u *User) trySendToRxCh(usrCh *model.Ch, msg model.Message) error {
	return usrCh.SendToRx(msg)
}

// SetOperatorMode выставляет/снимает режим оператора для конкретного диалога
func (u *User) SetOperatorMode(dialogId uint64, operator bool) {
	if operator {
		u.operatorModeByDialog.Store(dialogId, true)
	} else {
		u.operatorModeByDialog.Delete(dialogId)
	}
	count := 0
	u.operatorModeByDialog.Range(func(_, _ any) bool {
		count++
		return true
	})
	metrics.TrackOperatorModeDialogs(0, count)
}

// IsOperatorMode возвращает текущий флаг оператора для диалога
func (u *User) IsOperatorMode(dialogId uint64) bool {
	_, ok := u.operatorModeByDialog.Load(dialogId)
	return ok
}

// StartBots запускает WhatsApp User
func (u *User) StartBots() error {
	logger.Info("WhatsApp: запуск ботов...")

	// Однократно получаем и запускаем ботов пользователей
	err := u.GetWaUserBotUsers()
	if err != nil {
		logger.Error("Ошибка получения пользователей WhatsApp: %w", err)
		return err
	}

	// Запускаю веб-сервер авторизации юзер ботов
	go u.webHook()

	logger.Info("WhatsApp: боты успешно запущены")
	return nil
}

func (u *User) StopBot() {
	logger.Info("WhatsApp: получен сигнал завершения, закрытие WhatsApp ботов...")

	done := make(chan struct{})
	var wg sync.WaitGroup

	go func() {
		u.uBot.Range(func(key, value any) bool {
			userId := key.(uint32)
			bot := value.(*Bot)
			wg.Add(1)
			go func(id uint32, b *Bot) {
				defer wg.Done()
				b.StopUserBot()
			}(userId, bot)
			return true // продолжаем итерацию
		})

		wg.Wait()
		close(done)

		// Сохраняем данные при завершении
		if u.mod != nil {
			u.mod.SaveAllContextDuringExit()
			logger.Info("WhatsApp: все данные сохранены")
		}
	}()

	select {
	case <-done:
		logger.Info("WhatsApp: Все пользовательские боты успешно остановлены")
	case <-time.After(5 * time.Second):
		logger.Warn("WhatsApp: Тайм-аут при остановке ботов, принудительное завершение")
	}

	logger.Info("WhatsApp: остановка завершена")
}

// handleEvent обрабатывает события WhatsApp
func (b *Bot) handleEvent(evt any) {
	switch v := evt.(type) {

	case *events.Message:
		metrics.MessagesReceived.WithLabelValues(metrics.BotLabel(b.userID), messageType(v)).Inc()
		if !b.textMessage && (v.Message.GetConversation() != "" || v.Message.GetExtendedTextMessage() != nil) {
			metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "text_disabled").Inc()
			logger.Debug("Текстовые сообщения отключены настройками токена", b.userID)
			return
		}
		//err := w.handleMessage(v)
		//if err != nil {
		//	logger.Error("Ошибка обработки сообщения: %v", err, w.UserId)
		//}

		// Проверяем на ошибки дешифрования ПЕРЕД обработкой
		if b.isDecryptionError(v) {
			metrics.DecryptErrors.WithLabelValues(metrics.BotLabel(b.userID), "decrypt_error").Inc()
			logger.Warn("Обнаружена ошибка дешифрования, пропускаем сообщение", b.userID)
			return
		}

		// требуется сброс сессии.
		msgContent := ""
		if v.Message.GetConversation() != "" {
			msgContent = v.Message.GetConversation()
		} else if v.Message.GetExtendedTextMessage() != nil {
			msgContent = v.Message.GetExtendedTextMessage().GetText()
		}

		if msgContent == "[Ошибка: mismatching MAC]" {
			metrics.DecryptErrors.WithLabelValues(metrics.BotLabel(b.userID), "mismatching_mac").Inc()
			msg := com.CarpCh{
				Event:      "reauth",
				UserName:   "",
				AssistName: "",
				Target:     "WhatsApp User",
				UserID:     b.userID,
			}
			err := b.end.SendNotification(msg)
			if err != nil {
				logger.Error("`Ошибка: mismatching MAC` Ошибка отправки уведомления о необходимости повторной авторизации: %v", err, b.userID)
			}
			//common.SendEvent(w.UserId, "reauth", "", "", "WhatsApp User")
			go func() {
				err := b.db.SetChannelEnabled(b.userID, "Whats", false)
				if err != nil {
					logger.Error("Ошибка отключения канала WhatsApp: %v", err, b.userID)
					return
				}

				logger.Debug("Канал WhatsApp отключен в БД", b.userID)
			}()
			logger.Warn("Ошибка криптопроверки, требуется повторная авторизация", b.userID)
			// Вызываем сброс сессии асинхронно, чтобы не блокировать обработчик
			go func() {
				if err := b.resetSession(); err != nil {
					logger.Warn("Ошибка сброса сессии: %w", err, b.userID)
				}
			}()
			return
		}

		// Если сообщение не содержит ошибки, обрабатываем его
		err := b.handleMessage(v)
		if err != nil {
			metrics.MessagesProcessed.WithLabelValues(metrics.BotLabel(b.userID), "error").Inc()
			logger.Error("Ошибка обработки сообщения: %v", err, b.userID)
		} else {
			metrics.MessagesProcessed.WithLabelValues(metrics.BotLabel(b.userID), "success").Inc()
		}

	case *events.Connected:
		// Время успешного подключения для игнорирования сообщений из истории
		b.connectedAt = time.Now()
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "connected").Set(1)
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "disconnected").Set(0)
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "logged_out").Set(0)
		metrics.Reconnects.WithLabelValues(metrics.BotLabel(b.userID), "success").Inc()
	case *events.Disconnected:
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "connected").Set(0)
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "disconnected").Set(1)
		metrics.Reconnects.WithLabelValues(metrics.BotLabel(b.userID), "disconnected").Inc()
		logger.Warn("WhatsApp: соединение разорвано", b.userID)
		// Если отключение произошло и клиент не авторизован, можно инициировать повторную авторизацию.
		if !b.b.IsLoggedIn() {
			msg := com.CarpCh{
				Event:      "reauth",
				UserName:   "",
				AssistName: "",
				Target:     "WhatsApp User",
				UserID:     b.userID,
			}
			err := b.end.SendNotification(msg)
			if err != nil {
				logger.Error("`*events.Disconnected` Ошибка отправки уведомления о необходимости повторной авторизации: %v", err, b.userID)
			}
			//common.SendEvent(w.UserId, "reauth", "", "", "WhatsApp User")
			go func() {
				err := b.db.SetChannelEnabled(b.userID, "Whats", false)
				if err != nil {
					logger.Error("Ошибка отключения канала WhatsApp: %v", err, b.userID)
					return
				}

				logger.Debug("Канал WhatsApp отключен в БД", b.userID)
			}()
			logger.Warn("WhatsApp: не авторизован, требуется повторная авторизация", b.userID)
			go func() {
				if err := b.resetSession(); err != nil {
					logger.Warn("Ошибка повторной авторизации: %w", err, b.userID)
				}
			}()
		}
	case *events.LoggedOut:
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "connected").Set(0)
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "logged_out").Set(1)
		metrics.Reconnects.WithLabelValues(metrics.BotLabel(b.userID), "logged_out").Inc()
		logger.Warn("WhatsApp: выполнен выход с другого устройства", b.userID)
		msg := com.CarpCh{
			Event:      "reauth",
			UserName:   "",
			AssistName: "",
			Target:     "WhatsApp User",
			UserID:     b.userID,
		}
		err := b.end.SendNotification(msg)
		if err != nil {
			logger.Error("`*events.LoggedOut` Ошибка отправки уведомления о необходимости повторной авторизации: %v", err, b.userID)
		}
		//common.SendEvent(w.UserId, "reauth", "", "", "WhatsApp User")

		go func() {
			err := b.db.SetChannelEnabled(b.userID, "Whats", false)
			if err != nil {
				logger.Error("Ошибка отключения канала WhatsApp: %v", err, b.userID)
				return
			}

			logger.Debug("Канал WhatsApp отключен в БД", b.userID)
		}()

		go func() {
			if err := b.resetSession(); err != nil {
				logger.Error("Ошибка сброса сессии: %w", err, b.userID)
			}
		}()

	case *events.OfflineSyncCompleted:
		b.offlineSyncOnce.Do(func() { close(b.offlineMsgSync) })
		logger.Debug("Завершена синхронизация оффлайн сообщений", b.userID)

	default:
		//logger.Debug("Неизвестное событие: %T", evt, w.UserId)
	}
}

// Добавьте проверку ошибок дешифрования
func (b *Bot) isDecryptionError(msg *events.Message) bool {
	// Проверяем различные признаки ошибок дешифрования
	if msg.Message == nil {
		return true
	}

	// Проверяем наличие ошибок в метаданных сообщения
	if msg.Info.Category == "error" || msg.Info.Category == "decrypt" {
		return true
	}

	return false
}

// handleMessage обрабатывает входящие сообщения
func (b *Bot) handleMessage(msg *events.Message) error {
	startedAt := time.Now()
	defer metrics.ObserveDuration(metrics.MessageProcessingDuration.WithLabelValues(metrics.BotLabel(b.userID), "handle_message"), startedAt)

	<-b.offlineMsgSync // Ждем завершения синхронизации оффлайн сообщений

	if time.Since(b.connectedAt) < messageIgnoreTime {
		metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "message_ignore_window").Inc()
		logger.Debug("Игнорируем сообщение в течение первых %s секунд после подключения от %s",
			messageIgnoreTime, msg.Info.Sender.String(), b.userID)
		return nil
	}

	// Игнорируем статусы (status@broadcast)
	if msg.Info.Sender.Server == "broadcast" || msg.Info.Chat.User == "status" {
		metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "status_broadcast").Inc()
		logger.Debug("Игнорируем статус от %s", msg.Info.Sender.String(), b.userID)
		return nil
	}

	// Игнорируем статусы более жёсткий вариант
	if msg.Info.Chat.String() == "status@broadcast" || msg.Info.Sender.String() == "status@broadcast" {
		metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "status_broadcast").Inc()
		logger.Debug("Игнорируем статус/историю от %s", msg.Info.Sender.String(), b.userID)
		return nil
	}

	// Проверяем дополнительные флаги для определения истории
	if msg.Info.MessageSource.IsFromMe || msg.Info.IsIncomingBroadcast() {
		metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "from_me_or_broadcast").Inc()
		//logger.Debug("Игнорируем сообщение от %s (IsFromMe/IsIncomingBroadcast)", msg.Info.Sender.String(), w.UserId)
		return nil
	}

	// Проверка на сообщения с ошибками дешифрования (часто бывают историческими)
	if msg.Info.Category == "peer" || msg.Info.Category == "retry" {
		metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "peer_or_retry").Inc()
		//logger.Debug("Игнорируем сообщение с категорией %s от %s", msg.Info.Category, msg.Info.Sender.String(), w.UserId)
		return nil
	}

	// Проверка на синхронизированные сообщения
	// ID сообщения может содержать информацию о том, что сообщение из истории
	if strings.Contains(msg.Info.ID, "BAE") || strings.Contains(msg.Info.ID, "_BAE") {
		metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "history_id").Inc()
		//logger.Debug("Игнорируем историческое сообщение (BAE) от %s", msg.Info.Sender.String(), w.UserId)
		return nil
	}

	// Если в messageContextInfo есть recipientTimestamp и его значение слишком большое
	if ext := msg.Message.GetExtendedTextMessage(); ext != nil && ext.ContextInfo != nil {
		// Преобразуем сообщение в строку и ищем признаки исторического сообщения
		msgStr := fmt.Sprintf("%+v", ext.ContextInfo)
		if strings.Contains(msgStr, "deviceListMetadata") ||
			strings.Contains(msgStr, "messageSecret") ||
			strings.Contains(msgStr, "recipientTimestamp") {
			metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "context_info").Inc()
			//logger.Debug("Игнорируем историческое сообщение от %s", msg.Info.Sender.String(), w.UserId)
			return nil
		}
	}

	// Игнорируем сообщения старше определенного времени
	if msg.Info.Timestamp.Before(time.Now().Add(-5 * time.Minute)) {
		metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "too_old").Inc()
		// Это старое сообщение из истории - игнорируем
		//logger.Debug("Игнорируем старое сообщение от %s: %s", msg.Info.Sender.String(), msg.Info.Timestamp, w.UserId)
		return nil
	}

	//// Преобразуем ID отправителя в число
	//senderID, err := strconv.ParseUint(msg.Info.Sender.User, 10, 64)
	//if err != nil {
	//	return fmt.Errorf("WhatsApp: ошибка преобразования ID пользователя %s: %v", msg.Info.Sender.User, err)
	//}

	// ИСПРАВЛЕНИЕ: Берем именно Sender (реальный собеседник), а не MessageSource.Chat
	senderJID := msg.Info.Sender
	senderName := msg.Info.PushName

	//logger.Infoln("senderJID.User", senderJID.User)

	senderID, err := strconv.ParseUint(senderJID.User, 10, 64)
	if err != nil {
		return fmt.Errorf("WhatsApp: ошибка преобразования ID пользователя %s: %v", senderJID.User, err)
	}

	// Проверяем список разрешенных пользователей
	if len(b.uids) > 0 {
		allowed := false
		for _, uid := range b.uids {
			if uid == int64(senderID) {
				allowed = true
				break
			}
		}

		if !allowed {
			metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "not_allowed_user").Inc()
			logger.Debug("Игнорирование сообщения от %d (нет в списке разрешенных)", senderID, b.userID)
			return nil
		}
	}

	var content string

	switch {
	case msg.Message.GetConversation() != "":
		content = msg.Message.GetConversation()
	case msg.Message.GetExtendedTextMessage() != nil:
		content = msg.Message.GetExtendedTextMessage().GetText()
	case msg.Message.GetAudioMessage() != nil && msg.Message.GetAudioMessage().GetPTT():
		content = "[Голосовое сообщение]"
	case msg.Message.GetDocumentMessage() != nil:
		content = "[Документ]"
	case msg.Message.GetImageMessage() != nil:
		content = "[Изображение]"
	default:
		content = "[Неподдерживаемый тип сообщения]"
	}

	if content == "[Неподдерживаемый тип сообщения]" {
		return nil
	}

	// Преобразую msg.Info.Sender.User в uint64
	senderID, err = strconv.ParseUint(msg.Info.Sender.User, 10, 64)
	if err != nil {
		return fmt.Errorf("WhatsApp: ошибка преобразования ID пользователя %s: %v", msg.Info.Sender.User, err)
	}

	// Получаем реальный номер телефона из разных источников
	var realPhone string

	// Сначала пробуем SenderAlt (для некоторых контактов работает)
	if msg.Info.MessageSource.SenderAlt.User != "" {
		realPhone = extractRealPhone(msg.Info.MessageSource.SenderAlt.String(), "")
	}

	// Если не получилось, используем основной Sender JID
	if realPhone == "" {
		realPhone = extractRealPhone(msg.Info.Sender.String(), strconv.FormatUint(senderID, 10))
	}

	// Если всё ещё пусто, используем senderID как fallback
	if realPhone == "" {
		realPhone = strconv.FormatUint(senderID, 10)
		logger.Warn("Не удалось извлечь реальный номер, используем senderID: %s", realPhone, b.userID)
	}

	logger.Debug("Создана информация о респонденте: JID=%s, RealPhone=%s, SenderID=%d",
		msg.Info.Sender.String(), realPhone, senderID, b.userID)

	// Получаем или создаем информацию о респонденте
	value, loaded := b.responders.LoadOrStore(senderID, &ResponderInfo{
		//JID:       msg.Info.Sender,
		JID:       types.NewJID(msg.Info.Sender.User, msg.Info.Sender.Server),
		RealPhone: realPhone,
	})

	respInfo := value.(*ResponderInfo)

	// Проверяем и корректируем realPhone: если он пустой или не совпадает с senderID, используем senderID
	//extractedPhone := extractRealPhone(msg.Info.MessageSource.SenderAlt.String())
	//if respInfo.RealPhone == "" || respInfo.RealPhone != strconv.FormatUint(senderID, 10) {
	//	respInfo.RealPhone = strconv.FormatUint(senderID, 10)
	//	logger.Warn("RealPhone скорректирован на senderID: %s (извлечённый: %s)", respInfo.RealPhone, extractedPhone, w.UserId)
	//}

	// Если RealPhone пустой, устанавливаем fallback
	if respInfo.RealPhone == "" {
		respInfo.RealPhone = strconv.FormatUint(senderID, 10)
		logger.Warn("Не удалось извлечь реальный телефон из SenderAlt, используем senderID: %s", respInfo.RealPhone, b.userID)
	}

	if !loaded {
		// Новый респондент - логируем создание
		logger.Debug("Создана информация о респонденте: JID=%s, RealPhone=%s, SenderID=%d",
			respInfo.JID.String(), respInfo.RealPhone, senderID, b.userID)
		metrics.TrackActiveDialogs(b.userID, countResponders(&b.responders))
	}

	// Проверяем, является ли это первым взаимодействием пользователя
	var first bool
	if !respInfo.Known {
		logger.Debug("Пользователь %s впервые взаимодействует с ботом", msg.Info.Sender.User, b.userID)

		// Отправляю уведомление о первом взаимодействии
		go b.sendFirstContactMessage(senderID, senderName)

		// Инициализируем каналы связи и модель
		if err = b.initializeUserChannels(senderID, senderName); err != nil {
			// Проверяем, является ли это сообщением о пустых данных
			if strings.Contains(err.Error(), "получены пустые данные") {
				// Это нормальная ситуация для нового диалога - не считаем ошибкой
				logger.Debug("Инициализация нового диалога для пользователя %s (ID: %d)", senderName, senderID, b.userID)
			} else {
				return fmt.Errorf("ошибка инициализации каналов пользователя %s: %w", senderName, err)
			}
		}

		// Помечаем пользователя как известного
		respInfo.Known = true
		b.responders.Store(senderID, respInfo)
		first = true // Отмечаем как первое взаимодействие для CRM
	}

	switch content {
	case "[Голосовое сообщение]":
		// Если разрешены голосовые сообщения
		if mode.IsAudioModeEnabled() {
			return b.processVoiceMessage(first, msg)
		}

		return fmt.Errorf("WhatsApp: для пользователя %d отключены голосовые сообщения", b.userID)
	default:
		message := MessageContent{
			Text:  content,
			Voice: false,
			First: first,
		}
		return b.processMessage(message, msg)
	}
}

// processVoiceMessage обрабатывает голосовое сообщение
func (b *Bot) processVoiceMessage(first bool, msg *events.Message) error {
	audioMsg := msg.Message.GetAudioMessage()
	if audioMsg == nil {
		logger.Warn("Нет AudioMessage в сообщении", b.userID)
		return nil
	}

	// Получаем медиа (сразу как []byte)
	mediaData, err := b.b.Download(b.ctx, audioMsg)
	if err != nil {
		logger.Error("Ошибка скачивания аудиофайла: %v", err, b.userID)
		return nil
	}

	text, err := b.mod.TranscribeAudio(b.userID, mediaData, "voice.ogg")
	if err != nil {
		logger.Error("Ошибка транскрибирования аудио: %v", err, b.userID)
		return nil
	}

	message := MessageContent{
		Text:  text,
		Voice: true,
		First: first,
	}

	return b.processMessage(message, msg)
}

// extractFilesFromMessage извлекает файлы из WhatsApp сообщения
func (b *Bot) extractFilesFromMessage(msg *events.Message) []model.FileUpload {
	var files []model.FileUpload

	if msg == nil || msg.Message == nil {
		return files
	}

	switch {
	case msg.Message.GetImageMessage() != nil:
		imageMsg := msg.Message.GetImageMessage()
		if mediaData, err := b.b.Download(b.ctx, imageMsg); err == nil {
			files = append(files, model.FileUpload{
				Name:     fmt.Sprintf("image_%s.jpg", msg.Info.ID),
				Content:  bytes.NewReader(mediaData),
				MimeType: imageMsg.GetMimetype(),
			})
		} else {
			logger.Error("Ошибка скачивания изображения: %v", err, b.userID)
		}

	case msg.Message.GetDocumentMessage() != nil:
		docMsg := msg.Message.GetDocumentMessage()
		if mediaData, err := b.b.Download(b.ctx, docMsg); err == nil {
			fileName := docMsg.GetFileName()
			if fileName == "" {
				fileName = fmt.Sprintf("document_%s", msg.Info.ID)
			}
			files = append(files, model.FileUpload{
				Name:     fileName,
				Content:  bytes.NewReader(mediaData),
				MimeType: docMsg.GetMimetype(),
			})
		} else {
			logger.Error("Ошибка скачивания документа: %v", err, b.userID)
		}

	case msg.Message.GetVideoMessage() != nil:
		videoMsg := msg.Message.GetVideoMessage()
		if mediaData, err := b.b.Download(b.ctx, videoMsg); err == nil {
			files = append(files, model.FileUpload{
				Name:     fmt.Sprintf("video_%s.mp4", msg.Info.ID),
				Content:  bytes.NewReader(mediaData),
				MimeType: videoMsg.GetMimetype(),
			})
		} else {
			logger.Error("Ошибка скачивания видео: %v", err, b.userID)
		}

	case msg.Message.GetAudioMessage() != nil && !msg.Message.GetAudioMessage().GetPTT():
		// Аудиофайлы (не голосовые сообщения)
		audioMsg := msg.Message.GetAudioMessage()
		if mediaData, err := b.b.Download(b.ctx, audioMsg); err == nil {
			files = append(files, model.FileUpload{
				Name:     fmt.Sprintf("audio_%s.ogg", msg.Info.ID),
				Content:  bytes.NewReader(mediaData),
				MimeType: audioMsg.GetMimetype(),
			})
		} else {
			logger.Error("Ошибка скачивания аудио: %v", err, b.userID)
		}
	}

	return files
}

// extractRealPhone извлекает реальный телефон из JID с учетом LID контактов
func extractRealPhone(jidString string, fallback string) string {
	// Если jidString пустой, сразу возвращаем fallback
	if jidString == "" {
		return fallback
	}

	jid, err := types.ParseJID(jidString)
	if err != nil {
		logger.Debug("Не удалось распарсить JID '%s', используем fallback: %s", jidString, fallback)
		return fallback
	}

	// Если это LID контакт (@lid сервер)
	if jid.Server == "lid" {
		// User часть содержит реальный номер телефона
		if jid.User != "" {
			logger.Debug("Извлечен номер из LID: %s (JID: %s)", jid.User, jidString)
			return jid.User
		}
	}

	// Для обычных контактов (@s.whatsapp.net)
	if jid.Server == types.DefaultUserServer && jid.User != "" {
		logger.Debug("Извлечен номер из стандартного JID: %s", jid.User)
		return jid.User
	}

	// Если User часть пустая или не совпадает с ожидаемым форматом
	if jid.User == "" {
		logger.Debug("User часть JID пустая, используем fallback: %s", fallback)
		return fallback
	}

	// Возвращаем User часть JID
	logger.Debug("Используем User часть JID: %s (сервер: %s)", jid.User, jid.Server)
	return jid.User
}

func (b *Bot) setTyping(senderID uint64) {
	// Создаем отменяемый контекст
	ctx, cancel := context.WithCancel(b.ctx)

	// Сохраняем функцию отмены в sync.Map
	b.typingCancels.Store(senderID, cancel)

	// пауза в пару секунд
	time.Sleep(2 * time.Second)

	// Получаем оригинальный JID пользователя из информации о респонденте
	var jid types.JID
	if value, exists := b.responders.Load(senderID); exists {
		respInfo := value.(*ResponderInfo)
		jid = respInfo.JID
	} else {
		// Если информация не найдена, создаем стандартный JID
		jid = types.NewJID(strconv.FormatUint(senderID, 10), types.DefaultUserServer)
		logger.Warn("Информация о респонденте не найдена для setTyping %d, используем стандартный JID", senderID, b.userID)
	}

	// Устанавливаем статус "печатает"
	if err := b.b.SendChatPresence(b.ctx, jid, types.ChatPresenceComposing, ""); err != nil {
		logger.Error("Ошибка установки статуса печатает: %v", err, b.userID)
		return
	}

	// Ждем завершения или таймаута
	select {
	case <-ctx.Done():
		// Контекст отменен - убираем статус печатает
		_ = b.b.SendChatPresence(b.ctx, jid, types.ChatPresencePaused, "")
		logger.Debug("Статус 'печатает' отменен для %d", senderID, b.userID)
	case <-time.After(30 * time.Second):
		// Таймаут - убираем статус печатает
		_ = b.b.SendChatPresence(b.ctx, jid, types.ChatPresencePaused, "")
		logger.Debug("Статус 'печатает' завершен по таймауту для %d", senderID, b.userID)
	}
}

func (b *Bot) getAndRemoveTypingCancel(senderID uint64) context.CancelFunc {
	// LoadAndDelete атомарно загружает и удаляет значение
	value, loaded := b.typingCancels.LoadAndDelete(senderID)
	if !loaded {
		return nil
	}
	return value.(context.CancelFunc)
}

type MessageContent struct {
	Text  string
	Voice bool
	First bool
}

// processMessage обрабатывает входящее сообщение от пользователя
func (b *Bot) processMessage(message MessageContent, msg *events.Message) error {
	startedAt := time.Now()
	defer metrics.ObserveDuration(metrics.MessageProcessingDuration.WithLabelValues(metrics.BotLabel(b.userID), "process_message"), startedAt)

	// Получаем senderID из сообщения
	senderID, err := strconv.ParseUint(msg.Info.Sender.User, 10, 64)
	if err != nil {
		return fmt.Errorf("ошибка преобразования ID пользователя: %w", err)
	}

	// Отмечаем сообщение как прочитанное в WhatsApp
	go func() {
		// Используем оригинальный JID из сообщения
		jid := msg.Info.Sender

		// Устанавливаем статус "в сети"
		err = b.b.SendPresence(b.ctx, types.PresenceAvailable)
		if err != nil {
			logger.Error("Ошибка установки статуса: %v", err, b.userID)
		}

		err = b.b.MarkRead(b.ctx, []types.MessageID{msg.Info.ID}, time.Now(), jid, types.EmptyJID)
		if err != nil {
			logger.Error("Ошибка отметки сообщений как прочитанных: %v", err, b.userID)
		}
	}()

	usrCh, err := b.mod.GetCh(senderID)
	if err != nil {
		// Если канал не найден или ошибка, пытаемся пересоздать каналы
		logger.Warn("Канал не найден для senderID %d, пересоздаём: %v", senderID, err, b.userID)
		senderName := msg.Info.PushName
		if initErr := b.initializeUserChannels(senderID, senderName); initErr != nil {
			return fmt.Errorf("ошибка пересоздания каналов пользователя: %w", initErr)
		}
		// Пытаемся получить канал снова
		usrCh, err = b.mod.GetCh(senderID)
		if err != nil {
			return fmt.Errorf("ошибка канала пользователя после пересоздания: %w", err)
		}
	}

	// Если не включён операторский режим
	if !b.parent.IsOperatorMode(usrCh.DialogID) {
		// Запускаем процесс печатания
		go b.setTyping(senderID)
	}

	content := model.AssistResponse{
		Message: message.Text,
	}

	// Извлекаем файлы из сообщения
	files := b.extractFilesFromMessage(msg)
	senderName := msg.Info.PushName

	// Получаем реальный телефон из сохраненной информации о респонденте
	value, exists := b.responders.Load(senderID)
	if !exists {
		logger.Error("Информация о респонденте не найдена для senderID: %d", senderID, b.userID)
		return fmt.Errorf("информация о респонденте не найдена")
	}

	respInfo := value.(*ResponderInfo)
	logger.Info("Реальный телефон: %s (senderID: %d)", respInfo.RealPhone, senderID, b.userID)

	// Отправляю сообщение в CRM - используем реальный телефон
	csg := b.c.MSG("user", senderName, message.Text).
		WithPhone(respInfo.RealPhone).
		NewDialog(message.First).
		WithVoice(message.Voice).
		WithFiles(func() []string {
			var f []string
			for _, file := range files {
				f = append(f, file.Name)
			}
			return f
		}()...)

	crmStartedAt := time.Now()
	if err := b.c.SendMessage(csg); err != nil {
		metrics.CRMRequests.WithLabelValues(metrics.BotLabel(b.userID), "incoming", "error").Inc()
		metrics.ObserveDuration(metrics.CRMRequestDuration.WithLabelValues(metrics.BotLabel(b.userID), "incoming"), crmStartedAt)
		logger.Error("Ошибка отправки сообщения в CRM: %v", err, b.userID)
	} else {
		metrics.CRMRequests.WithLabelValues(metrics.BotLabel(b.userID), "incoming", "success").Inc()
		metrics.ObserveDuration(metrics.CRMRequestDuration.WithLabelValues(metrics.BotLabel(b.userID), "incoming"), crmStartedAt)
	}
	/////////////////////////////////////

	// Проверяю включен ли режим оператора
	operatorMode := b.parent.IsOperatorMode(usrCh.DialogID)
	userMessage := b.mod.NewMessage(model.Operator{SetOperator: operatorMode, SenderName: senderName}, "user", &content, &senderName, files...)

	// Используем безопасный метод SendToRx из библиотеки v1.26.0
	if err := usrCh.SendToRx(userMessage); err != nil {
		// Канал закрыт или переполнен, пытаемся пересоздать
		logger.Warn("Ошибка отправки в RxCh для senderID %d: %v, пересоздаём каналы", senderID, err, b.userID)

		if initErr := b.initializeUserChannels(senderID, senderName); initErr != nil {
			return fmt.Errorf("канал RxCh недоступен, ошибка пересоздания: %w", initErr)
		}

		// Повторная попытка с новым каналом
		usrCh, err = b.mod.GetCh(senderID)
		if err != nil {
			return fmt.Errorf("не удалось получить канал после пересоздания: %w", err)
		}

		if err := usrCh.SendToRx(userMessage); err != nil {
			return fmt.Errorf("сообщение не доставлено даже после пересоздания каналов: %w", err)
		}

		logger.Info("Сообщение отправлено после пересоздания каналов для senderID %d", senderID, b.userID)
	}

	return nil
}

// initializeUserChannels создаёт каналы связи для пользователя
func (b *Bot) initializeUserChannels(senderID uint64, senderName string) error {
	startedAt := time.Now()
	status := "success"
	defer func() {
		metrics.ObserveDuration(metrics.UserChannelInitDuration.WithLabelValues(metrics.BotLabel(b.userID), status), startedAt)
	}()

	dialogId, err := b.db.GetOrSetTreadAndResponder(b.userID, senderID, senderName, comdom.WhatsApp)
	if err != nil {
		status = "error"
		return fmt.Errorf("ошибка ID диалога: %w", err)
	}

	// Проверяем, существует ли уже канал
	existingCh, existingErr := b.mod.GetCh(senderID)
	if existingErr == nil && existingCh.IsRxOpen() {
		// Канал существует и открыт, используем его
		logger.Debug("Используем существующий канал для senderID %d", senderID, b.userID)
		return nil
	}

	// Если канал закрыт или не существует, очищаем старые данные
	if existingErr == nil {
		logger.Warn("Обнаружен закрытый канал для senderID %d, очищаем старые данные", senderID, b.userID)
		b.mod.CleanDialogData(dialogId)
	}

	usrMod, err := b.mod.GetOrSetRespGPT(*b.assist, dialogId, senderID, senderName)
	if err != nil && !strings.Contains(err.Error(), "получены пустые данные") {
		status = "error"
		return fmt.Errorf("ошибка модели пользователя: %w", err)
	}

	if err != nil {
		logger.Debug("Создание новой модели для диалога %d с пользователем %s", dialogId, senderName, b.userID)
	}

	usrCh, err := b.mod.GetCh(senderID)
	if err != nil && !strings.Contains(err.Error(), "получены пустые данные") {
		status = "error"
		return fmt.Errorf("ошибка канала пользователя: %w", err)
	}

	if err != nil {
		logger.Debug("Инициализация каналов для нового пользователя %s", senderName, b.userID)
	}

	// Отправляем данные в канал запуска
	startCh := model.StartCh{
		Ctx:     b.ctx,
		Model:   usrMod,
		Chanel:  usrCh,
		TreadId: dialogId,
		RespId:  senderID,
	}

	// Сначала запущу слушателя каналов
	b.startResponseListener(senderID, usrCh)

	select {
	case StartCh <- startCh:
	default:
		return errors.New("ошибка отправки данных в StartCh")
	}

	return nil
}

// startResponseListener запускает горутину для прослушивания ответов ассистента
func (b *Bot) startResponseListener(respId uint64, usrCh *model.Ch) {
	// Получаем информацию о респонденте
	var jid types.JID
	var realPhone string

	if value, exists := b.responders.Load(respId); exists {
		respInfo := value.(*ResponderInfo)
		jid = respInfo.JID
		realPhone = respInfo.RealPhone
	} else {
		// Если информация не найдена, создаем стандартные значения
		realPhone = strconv.FormatUint(respId, 10)
		var err error
		jid, err = types.ParseJID(realPhone + "@s.whatsapp.net")
		if err != nil {
			logger.Error("Ошибка создания JID для пользователя %d: %v", respId, err, b.userID)
			return
		}
		logger.Warn("Информация о респонденте не найдена для %d, используем стандартные значения", respId, b.userID)
	}

	logger.Debug("Используем JID для отправки: %s, телефон для CRM: %s (respId: %d)",
		jid.String(), realPhone, respId, b.userID)

	// Запускаем горутину для прослушивания ответов ассистента
	go func() {

		for {
			select {
			case msg, ok := <-usrCh.TxCh:
				if !ok {
					metrics.MessagesProcessed.WithLabelValues(metrics.BotLabel(b.userID), "tx_channel_closed").Inc()
					logger.Error("Канал TxCh закрыт для пользователя %d", respId, b.userID)
					return
				}
				// Проверяю есть ли пометка операторского сообщения
				// Проверяю в сообщении команду выключения режима оператора
				if msg.Operator.Operator && msg.Operator.SetOperator {
					// Включаю для респондента режим оператора
					b.parent.SetOperatorMode(usrCh.DialogID, true)
					metrics.TrackOperatorModeDialogs(b.userID, countSyncMap(&b.parent.operatorModeByDialog))
					logger.Debug("Включен режим оператора для диалога %d", usrCh.DialogID, usrCh.UserID)
				}

				// Проверяем тип сообщения - нам нужны только ответы ассистента
				if msg.Type == "assist" {
					logger.Debug("Сообщение ассистента %v", msg)
					// Отправляем ответ пользователю через WhatsApp
					if err := b.sendMessage(jid, msg); err != nil {
						metrics.WhatsAppSend.WithLabelValues(metrics.BotLabel(b.userID), "error").Inc()
						logger.Error("Ошибка отправки сообщения пользователю %d: %v", respId, err, b.userID)
					} else {
						metrics.WhatsAppSend.WithLabelValues(metrics.BotLabel(b.userID), "success").Inc()
					}

					logger.Info("Отправка в CRM с телефоном: %s", realPhone, b.userID)
					// Отправляю ответ ассистента в CRM
					csg := b.c.MSG("assist", usrCh.RespName, msg.Content.Message).
						WithPhone(realPhone).
						WithFiles(func() []string {
							var f []string
							for _, file := range msg.Content.Action.SendFiles {
								f = append(f, file.FileName)
							}
							return f
						}()...).
						SetMeta(msg.Content.Meta)

					crmStartedAt := time.Now()
					if err := b.c.SendMessage(csg); err != nil {
						metrics.CRMRequests.WithLabelValues(metrics.BotLabel(b.userID), "outgoing", "error").Inc()
						metrics.ObserveDuration(metrics.CRMRequestDuration.WithLabelValues(metrics.BotLabel(b.userID), "outgoing"), crmStartedAt)
						logger.Error("Ошибка отправки ответа ассистента в CRM: %v", err, b.userID)
					} else {
						metrics.CRMRequests.WithLabelValues(metrics.BotLabel(b.userID), "outgoing", "success").Inc()
						metrics.ObserveDuration(metrics.CRMRequestDuration.WithLabelValues(metrics.BotLabel(b.userID), "outgoing"), crmStartedAt)
					}
					///////////////////////////////////

				}
				logger.Debug("Ответ отправлен пользователю %d", respId, b.userID)
			case <-b.ctx.Done():
				// Выход если контекст бота завершен
				return
			}
		}
	}()
}

func (b *Bot) sendMessage(jid types.JID, message model.Message) error {
	startedAt := time.Now()
	defer metrics.ObserveDuration(metrics.WhatsAppSendDuration.WithLabelValues(metrics.BotLabel(b.userID)), startedAt)

	// Получаем senderID для отмены печатания
	senderID, err := strconv.ParseUint(jid.User, 10, 64)
	if err != nil {
		return err
	}

	logger.Debug("Отправка сообщения на JID: %s (userID: %d)", jid.String(), senderID, b.userID)

	// Отменяем статус печатания в defer, чтобы гарантировать выполнение
	defer func() {
		// Получаем и отменяем контекст печатания
		cancelTyping := b.getAndRemoveTypingCancel(senderID)
		if cancelTyping != nil {
			cancelTyping()
			logger.Debug("Контекст печатания отменен для %d", senderID, b.userID)
		}
		// Устанавливаем статус "пауза"
		if err := b.b.SendChatPresence(b.ctx, jid, types.ChatPresencePaused, types.ChatPresenceMediaText); err != nil {
			logger.Debug("Ошибка установки статуса паузы: %v", err, b.userID)
		} else {
			logger.Debug("Статус 'пауза' установлен для %d", senderID, b.userID)
		}
	}()

	// Сначала отправляем текстовое сообщение, если оно есть
	if message.Content.Message != "" {
		logger.Debug("Отправка текста на JID: %s", jid.String(), b.userID)
		_, err := b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			Conversation: proto.String(message.Content.Message),
		})
		if err != nil {
			return fmt.Errorf("ошибка отправки текста: %w", err)
		}
		logger.Debug("Текст успешно отправлен на JID: %s", jid.String(), b.userID)
	}

	// Затем отправляем файлы из Action.SendFiles
	for _, file := range message.Content.Action.SendFiles {
		err := b.sendFile(jid, file)
		if err != nil {
			logger.Error("Ошибка отправки файла %s: %v", file.FileName, err, b.userID)
			continue
		}
	}

	return nil
}

func messageType(msg *events.Message) string {
	if msg == nil || msg.Message == nil {
		return "unknown"
	}
	switch {
	case msg.Message.GetConversation() != "":
		return "text"
	case msg.Message.GetExtendedTextMessage() != nil:
		return "extended_text"
	case msg.Message.GetAudioMessage() != nil:
		return "audio"
	case msg.Message.GetImageMessage() != nil:
		return "image"
	case msg.Message.GetVideoMessage() != nil:
		return "video"
	case msg.Message.GetDocumentMessage() != nil:
		return "document"
	default:
		return "other"
	}
}

func countResponders(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func countSyncMap(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func (b *Bot) sendFile(jid types.JID, file model.File) error {
	// Получаем файл с повторными попытками
	data, err := b.uploadFileWithRetry(file.URL, file.FileName)
	if err != nil {
		return fmt.Errorf("ошибка получения файла %s: %w", file.FileName, err)
	}

	// Определяем MIME-тип
	mimeType := b.getMimeTypeFromFilename(file.FileName)

	// Загружаем файл в WhatsApp с правильным типом медиа
	mediaType := b.getWhatsAppMediaType(file.Type)
	uploaded, err := b.b.Upload(b.ctx, data, mediaType)
	if err != nil {
		return fmt.Errorf("ошибка загрузки файла в WhatsApp: %w", err)
	}

	// Отправляем в зависимости от типа файла
	switch file.Type {
	case model.Photo:
		_, err = b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{
				Caption:       proto.String(file.Caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			},
		})
	case model.Video:
		_, err = b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			VideoMessage: &waE2E.VideoMessage{
				Caption:       proto.String(file.Caption),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			},
		})
	case model.Audio:
		_, err = b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			AudioMessage: &waE2E.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			},
		})
	case model.Doc:
		_, err = b.b.SendMessage(b.ctx, jid, &waE2E.Message{
			DocumentMessage: &waE2E.DocumentMessage{
				FileName:      proto.String(file.FileName),
				Mimetype:      proto.String(mimeType),
				URL:           proto.String(uploaded.URL),
				DirectPath:    proto.String(uploaded.DirectPath),
				MediaKey:      uploaded.MediaKey,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    proto.Uint64(uploaded.FileLength),
			},
		})
	default:
		return fmt.Errorf("неподдерживаемый тип файла: %s", file.Type)
	}

	if err != nil {
		return fmt.Errorf("ошибка отправки файла %s: %w", file.FileName, err)
	}

	return nil
}

func (b *Bot) uploadFileWithRetry(fileURL, fileName string) ([]byte, error) {
	maxRetries := 3
	baseDelay := time.Second

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(b.ctx, "GET", fileURL, nil)
		if err != nil {
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("ошибка создания запроса: %w", err)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		// Устанавливаем User-Agent для избежания блокировок
		req.Header.Set("User-Agent", "WhatsApp-Bot/1.0")

		resp, err := client.Do(req)
		if err != nil {
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("ошибка выполнения запроса: %w", err)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		if resp.StatusCode == 415 {
			_ = resp.Body.Close()
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("сервер не поддерживает тип файла после %d попыток", maxRetries)
			}
			logger.Warn("Попытка %d: получен статус 415 для файла %s, повторяем через %v", attempt+1, fileName, baseDelay*time.Duration(attempt+1))
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("HTTP ошибка: %d %s", resp.StatusCode, resp.Status)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("ошибка чтения данных: %w", err)
			}
			time.Sleep(baseDelay * time.Duration(attempt+1))
			continue
		}

		return data, nil
	}

	return nil, fmt.Errorf("все попытки загрузки файла исчерпаны")
}

func (b *Bot) getMimeTypeFromFilename(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".mp4":
		return "video/mp4"
	case ".mp3":
		return "audio/mpeg"
	case ".pdf":
		return "application/pdf"
	case ".txt":
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}

func (b *Bot) getWhatsAppMediaType(fileType model.FileType) whatsmeow.MediaType {
	switch fileType {
	case model.Photo:
		return whatsmeow.MediaImage
	case model.Video:
		return whatsmeow.MediaVideo
	case model.Audio:
		return whatsmeow.MediaAudio
	case model.Doc:
		return whatsmeow.MediaDocument
	default:
		return whatsmeow.MediaDocument
	}
}

func (b *Bot) resetSession() error {
	b.stopCalls("session_reset")
	b.b.Disconnect()

	// Устанавливаем флаг остановки атомарно
	b.stopped.Store(true)

	// Удаляем device из SQLite store перед повторной авторизацией
	if b.container != nil && b.container.container != nil {
		// Получаем JID устройства
		if b.b.Store != nil && b.b.Store.ID != nil {
			jid := *b.b.Store.ID
			logger.Debug("Удаление device из SQLite store для JID: %s", jid.String(), b.userID)

			// Удаляем устройство из store
			err := b.container.container.DeleteDevice(b.ctx, b.b.Store)
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
func (u *User) GetWaUserBotUsers() error {
	// Получаем данные пользователей
	userDetails, err := u.db.GetWaUserBotUsers(u.ctx)
	if err != nil {
		return fmt.Errorf("ошибка получения данных пользователей: %b", err)
	}
	// Если нет данных для обработки, выходим
	if len(userDetails) == 0 {
		return nil
	}

	// Обрабатываем состояние ботов
	u.processBotsState(userDetails)

	return nil
}

// processBotsState обрабатывает запуск ботов из списка пользователей
func (u *User) processBotsState(userDetails []domain.WaUserBotData) {
	// Обрабатываем пользователей и запускаем их ботов
	for _, user := range userDetails {
		// Пропускаем если нет данных сессии
		if user.SessionData == "" {
			logger.Warn("Пропуск пользователя - отсутствуют данные сессии", user.UserId)
			continue
		}

		// Проверяем подписку пользователя
		err := u.checkUserSubscription(user.UserId)
		{
			if err != nil {
				logger.Info("Пропуск бота из-за ошибки подписки: %v", err, user.UserId)
				continue
			}
		}

		// Если поле зашифровано MasterKey — расшифровываем
		if crypto.IsEncryptedWithMasterKey(user.SessionData) {
			sessionData, err := u.decryptSessionData(user.UserId, user.SessionData)
			if err != nil {
				logger.Error("Ошибка расшифровки данных сессии: %v", err, user.UserId)
				continue
			}
			// Обновляю данные сессии на расшифрованные
			user.SessionData = sessionData
		}

		// Парсим контейнер
		userBotContainer, err := parseWaUserBotContainer(u.ctx, user.UserId, user.SessionData, u.db)
		if err != nil {
			logger.Error("Ошибка разбора токена: %v", err, user.UserId)
			continue
		}

		// Создаем бота
		logger.Info("Создание бота для пользователя", user.UserId)
		assistModel := u.createAssistantModel(user)

		bot, err := u.createOrUpdateBot(user.UserId, userBotContainer, assistModel)
		if err != nil {
			logger.Error("Ошибка создания бота: %v", err, user.UserId)
			continue
		}

		u.uBot.Store(user.UserId, bot)
	}
}

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
func parseWaUserBotContainer(ctx context.Context, userId uint32, containerString string, database DB) (*JSONDeviceStore, error) {
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

	return &JSONDeviceStore{
		container: container,
		device:    device,
		logger:    l,
		db:        database,
		Uids:      devData.Uids,
		call:      devData.AllowCall,
		text:      devData.AllowText,
	}, nil
}

// Создает нового бота или обновляет существующего
func (u *User) createOrUpdateBot(userID uint32, containerData *JSONDeviceStore, assist *model.Assistant) (*Bot, error) {
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
func (u *User) initializeBot(userID uint32, tokenData *JSONDeviceStore, assist *model.Assistant, autoConnect bool) (*Bot, error) {
	// Создаем логгер для этого пользователя
	//logger := waLog.Stdout(fmt.Sprintf("WhatsApp[%d]", userID), "INFO", true)

	client := whatsmeow.NewClient(tokenData.GetDevice(), nil)
	// meowcaller устанавливает низкоуровневый call hook, поэтому его нужно
	// создать до подключения whatsmeow. Обработчики звонков пока не включаем.
	callClient := meowcaller.NewClient(client)

	// Канал для ожидания завершения синхронизации состояния
	syncComplete := make(chan struct{})

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
		assist:          assist,
		responders:      sync.Map{}, // sync.Map не требует make()
		messageReceived: make(chan struct{}),
		end:             u.end,
		db:              u.db,
		mod:             u.mod,
		c:               crmUser,
		syncComplete:    syncComplete,
		parent:          u,
		offlineMsgSync:  make(chan struct{}), // Канал для синхронизации оффлайн сообщений
		typingCancels:   sync.Map{},
		redisCache:      u.redisCache,
		voiceCall:       tokenData.call,
		textMessage:     tokenData.text,
	}

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
		switch v := evt.(type) {

		case *events.AppStateSyncComplete:
			//logger.Debug("WhatsApp: пользователь %d: завершена синхронизация состояния %s",
			//	userID, v.Name, userID)

			// Закрываем канал при синхронизации последнего состояния
			if v.Name == "critical_unblock_low" {
				select {
				case <-syncComplete:
					// Канал уже закрыт
				default:
					close(syncComplete)
					//logger.Debug("Все состояния синхронизированы", userID)
				}
			}

		case *events.Connected:
			// Не блокируем обработку новых сообщений, если клиент не прислал
			// OfflineSyncCompleted (например, после повторной авторизации).
			bot.offlineSyncOnce.Do(func() { close(bot.offlineMsgSync) })

			// Перехватываем событие подключения и сохраняем состояние самостоятельно
			// Получаю mk для шифрования данных авторизации бота в ДБ
			if err := tokenData.SaveJSON(u.ctx, userID, u.userMasterKey(userID), false); err != nil {
				logger.Error("WhatsApp: ошибка сохранения состояния устройства: %v", err, userID)
			}
			// Закрываем канал синхронизации при подключении
			close(syncComplete)
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
			err = u.db.SetChannelEnabled(userID, "whats", false)
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

		// Запускаем обработчик сообщений для этого пользователя
		if client.IsLoggedIn() {
			logger.Info("WhatsApp клиент успешно подключен как %s", client.Store.ID.String(), userID)
		}
	}()

	return bot, nil
}

func NewJSONDeviceStore(ctx context.Context, logger waLog.Logger, db DB) (*JSONDeviceStore, error) {
	// Создаем SQLite хранилище в памяти
	container, err := sqlstore.New(ctx, "sqlite", "file::memory:?cache=shared&_pragma=foreign_keys(1)", logger)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания in-memory SQLite: %w", err)
	}

	// Проверяем, что container не равен nil
	if container == nil {
		return nil, errors.New("ошибка: контейнер не был создан")
	}

	// Создаем новое устройство
	device := container.NewDevice()
	if device == nil {
		return nil, errors.New("ошибка: устройство не было создано")
	}

	// Проверяем только ключи шифрования, ID не требуется для нового устройства
	if device.NoiseKey == nil || device.IdentityKey == nil {
		return nil, errors.New("ошибка: устройство содержит некорректные данные шифрования")
	}

	// Устанавливаем базовые параметры для устройства
	if device.Platform == "" {
		device.Platform = "Debian 12"
	}
	if device.PushName == "" {
		device.PushName = "Marusia AI"
	}

	jds := &JSONDeviceStore{
		container: container,
		device:    device,
		logger:    logger,
		db:        db,
	}

	return jds, nil
}

// GetDevice возвращает устройство для whatsmeow
func (jds *JSONDeviceStore) GetDevice() *store.Device {
	if jds.device == nil {
		jds.logger.Errorf("Ошибка: устройство не инициализировано")
		return nil
	}
	return jds.device
}

// SaveJSON сохраняет данные в JSON
func (jds *JSONDeviceStore) SaveJSON(ctx context.Context, userID uint32, mk [32]byte, firstAuthorization bool) error {
	jds.mu.Lock()
	defer jds.mu.Unlock()

	if jds.device == nil {
		return errors.New("ошибка: устройство не инициализировано")
	}

	// Дополнительные проверки полей устройства
	if jds.device.ID == nil || jds.device.NoiseKey == nil || jds.device.IdentityKey == nil {
		return errors.New("ошибка: устройство содержит некорректные данные")
	}

	// Для первоначального создания бота разрешаю по умолчанию текстовые и голосовые звонки
	allowText := jds.text
	allowCall := jds.call
	if firstAuthorization {
		allowText = true
		allowCall = true
	}

	// Сохраняем состояние в JSON
	data, err := json.MarshalIndent(map[string]any{
		"ID":             jds.device.ID,
		"RegistrationID": jds.device.RegistrationID,
		"NoiseKey":       jds.device.NoiseKey,
		"IdentityKey":    jds.device.IdentityKey,
		"SignedPreKey":   jds.device.SignedPreKey,
		"Platform":       jds.device.Platform,
		"PushName":       jds.device.PushName,
		"AdvSecretKey":   jds.device.AdvSecretKey,
		"Account":        jds.device.Account,
		"Uids":           jds.Uids,
		"AllowText":      allowText,
		"AllowCall":      allowCall,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("ошибка сериализации: %w", err)
	}

	// Обновляю без уведомления users об изменении настроек бота
	err = jds.db.UpdateWhatsBotData(ctx, userID, mk, string(data), true)
	if err != nil {
		logger.Error("ошибка сохранения данных в БД: %v\n", err, userID)
		return err
	}

	logger.Debug("Данные сессии успешно обновлены в БД", userID)

	return nil
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
func (u *User) GetBotUsername(userID uint32) string {
	value, ok := u.uBot.Load(userID)
	if !ok {
		return ""
	}
	bot := value.(*Bot)
	if bot.b == nil || bot.b.Store.ID == nil {
		return ""
	}

	return bot.b.Store.ID.String()
}

// restartUserBot перезапускает бота для конкретного пользователя с перезагрузкой всех параметров из БД
func (u *User) restartUserBot(userId uint32) error {
	logger.Info("Получена команда на перезапуск бота...", userId)

	// Получаем данные конкретного пользователя из БД
	userData, err := u.db.GetWaUser(u.ctx, userId)
	if err != nil {
		logger.Error("Ошибка получения данных пользователя из БД: %v", err, userId)
		return fmt.Errorf("ошибка получения данных пользователя из БД: %w", err)
	}

	// Если поле зашифровано MasterKey — расшифровываем
	if crypto.IsEncryptedWithMasterKey(userData.SessionData) {
		sessionData, err := u.decryptSessionData(userData.UserId, userData.SessionData)
		if err != nil {
			return fmt.Errorf("ошибка расшифровки данных сессии: %v", err)
		}
		// Обновляю данные сессии на расшифрованные
		userData.SessionData = sessionData
	}

	// Проверяем, включен ли бот для этого пользователя
	if !userData.WaUserBotEnabled {
		logger.Warn("Бот отключен для пользователя, остановка...", userId)
		// Останавливаем бот, если он был запущен
		if _, exists := u.uBot.Load(userId); exists {
			if err := u.stopUserBot(userId); err != nil {
				logger.Error("Ошибка остановки отключенного бота: %v", err, userId)
				return fmt.Errorf("ошибка остановки отключенного бота: %w", err)
			}
		}
		return fmt.Errorf("бот отключен для пользователя %d", userId)
	}

	// Останавливаем текущий бот, если он запущен
	if _, exists := u.uBot.Load(userId); exists {
		logger.Debug("Остановка текущего бота перед перезапуском...", userId)
		if err := u.stopUserBot(userId); err != nil {
			logger.Error("Ошибка остановки бота: %v", err, userId)
			return fmt.Errorf("ошибка остановки бота: %w", err)
		}
		// Дополнительная пауза для полного завершения
		time.Sleep(500 * time.Millisecond)
	}

	// Проверяем подписку пользователя
	err = u.checkUserSubscription(userId)
	if err != nil {
		logger.Error("Ошибка проверки подписки: %v", err, userId)
		return fmt.Errorf("ошибка проверки подписки: %w", err)
	}

	// Парсим контейнер
	userBotContainer, err := parseWaUserBotContainer(u.ctx, userData.UserId, userData.SessionData, u.db)
	if err != nil {
		logger.Error("Ошибка разбора токена: %v", err, userId)
		return fmt.Errorf("ошибка разбора токена: %w", err)
	}

	// Создаем модель ассистента с обновленными параметрами из БД
	logger.Debug("Создание модели ассистента с обновленными параметрами...", userId)
	assistModel := u.createAssistantModel(*userData)

	// Создаем новый бот с обновленными параметрами
	logger.Debug("Создание нового бота с обновленными параметрами...", userId)
	bot, err := u.createOrUpdateBot(userData.UserId, userBotContainer, assistModel)
	if err != nil {
		logger.Error("Ошибка создания бота: %v", err, userId)
		return fmt.Errorf("ошибка создания бота: %w", err)
	}

	// Сохраняем бот в карту
	u.uBot.Store(userData.UserId, bot)

	logger.Info("Бот успешно перезапущен с обновленными параметрами из БД", userId)
	return nil
}

// stopUserBot останавливает бота для конкретного пользователя и удаляет его из карты
func (u *User) stopUserBot(userId uint32) error {
	value, exists := u.uBot.Load(userId)
	if !exists {
		logger.Warn("Бот не найден", userId)
		return fmt.Errorf("бот для пользователя %d не найден", userId)
	}

	bot := value.(*Bot)
	if bot == nil {
		logger.Warn("Бот не инициализирован", userId)
		return fmt.Errorf("бот для пользователя %d не инициализирован", userId)
	}

	// Проверяем, не остановлен ли бот уже
	if bot.stopped.Load() {
		logger.Warn("Бот уже остановлен", userId)
		return nil
	}

	logger.Debug("Остановка бота...", userId)

	// Устанавливаем флаг остановки атомарно
	bot.stopped.Store(true)
	bot.stopCalls("user_removed")

	// Отменяем контекст
	if bot.cancel != nil {
		bot.cancel()
		logger.Debug("Контекст отменен", userId)
	}

	// Отключаем WhatsApp клиент
	if bot.b != nil {
		bot.b.Disconnect()
		logger.Debug("WhatsApp клиент отключен", userId)
	}

	// Ждем некоторое время для корректного завершения
	time.Sleep(500 * time.Millisecond)

	// Удаляем бота из карты
	u.uBot.Delete(userId)

	logger.Debug("Бот остановлен", userId)
	return nil
}

// StartUserBot запускает бота для конкретного пользователя
func (u *User) StartUserBot(userId uint32) error {
	logger.Info("Получена команда на запуск бота...", userId)

	// Проверяем, запущен ли уже бот для этого пользователя
	if _, exists := u.uBot.Load(userId); exists {
		return fmt.Errorf("бот для пользователя %d уже запущен", userId)
	}

	// Получаем данные конкретного пользователя из БД
	userData, err := u.db.GetWaUser(u.ctx, userId)
	if err != nil {
		return fmt.Errorf("ошибка получения данных пользователя из БД: %w", err)
	}

	// Проверяем, включен ли бот для этого пользователя
	if !userData.WaUserBotEnabled {
		return fmt.Errorf("бот отключен для пользователя %d в настройках БД", userId)
	}

	// Проверяем подписку пользователя
	if err = u.checkUserSubscription(userId); err != nil {
		return fmt.Errorf("ошибка проверки подписки: %w", err)
	}

	// Если поле зашифровано MasterKey — расшифровываем
	if crypto.IsEncryptedWithMasterKey(userData.SessionData) {
		sessionData, err := u.decryptSessionData(userData.UserId, userData.SessionData)
		if err != nil {
			return fmt.Errorf("ошибка расшифровки данных сессии: %v", err)
		}
		// Обновляю данные сессии на расшифрованные
		userData.SessionData = sessionData
	}

	// Парсим контейнер
	userBotContainer, err := parseWaUserBotContainer(u.ctx, userData.UserId, userData.SessionData, u.db)
	if err != nil {
		return fmt.Errorf("ошибка разбора токена: %w", err)
	}

	// Создаем модель ассистента
	logger.Debug("Создание модели ассистента...", userId)
	assistModel := u.createAssistantModel(*userData)

	// Создаем новый бот
	logger.Debug("Создание нового бота...", userId)
	bot, err := u.initializeBot(userData.UserId, userBotContainer, assistModel, true)
	if err != nil {
		logger.Error("Ошибка создания бота: %v", err, userId)
		return fmt.Errorf("ошибка создания бота: %w", err)
	}

	// Сохраняем бот в карту
	u.uBot.Store(userData.UserId, bot)

	logger.Info("Бот успешно запущен для пользователя", userId)
	return nil
}

// Создает модель ассистента
func (u *User) createAssistantModel(user domain.WaUserBotData) *model.Assistant {
	return &model.Assistant{
		UserID:     user.UserId,
		AssistName: user.AssistName,
		AssistId:   user.AssistantId,
		Provider:   user.Provider,
		Espero:     user.Espero,
		Limit:      user.AskLimit,
		Ignore:     user.Ignore,
		Events: model.Notifications{
			Start:  user.Events.Start,
			End:    user.Events.End,
			Target: user.Events.Target,
		},
		Metas: model.Target{
			MetaAction: user.MetaAction,
			Triggers:   user.Triggers,
		},
	}
}
