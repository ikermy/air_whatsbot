package whatsapp

import (
	deliveryhttp "air_whatsbot/internal/delivery/http"
	"air_whatsbot/internal/domain"
	"air_whatsbot/internal/metrics"
	authpkg "air_whatsbot/internal/whatsapp/auth"
	callspkg "air_whatsbot/internal/whatsapp/calls"
	"air_whatsbot/internal/whatsapp/devicestore"
	handlerspkg "air_whatsbot/internal/whatsapp/handlers"
	responder "air_whatsbot/internal/whatsapp/responder"
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ikermy/air-common/pkg/comdb"
	"github.com/ikermy/air-common/pkg/crm"
	"github.com/ikermy/air-common/pkg/endpoint"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-common/pkg/operator"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
	"github.com/redis/go-redis/v9"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func (u *User) webHook() {
	if err := deliveryhttp.NewServer(u).ListenAndServe(":8080"); err != nil {
		logger.Fatalf("ошибка запуска сервера аутентификации: %v", err)
	}
}

type Model = model.Inter
type Endpoint = endpoint.Inter
type Operator = operator.Inter
type ExtDB = comdb.Exterior
type CRM = crm.Inter

// Repository — порт хранилища пользовательских WhatsApp-ботов.
type Repository interface {
	UpdateWhatsBotData(ctx context.Context, userID uint32, mk [32]byte, data string, enabled bool) error
	GetWaUserBotUsers(ctx context.Context) ([]domain.WaUserBotData, error)
	GetWaUser(ctx context.Context, userID uint32) (*domain.WaUserBotData, error)
}

// DB — единый порт хранилища WhatsApp: внешнее хранилище air-common плюс
// порт репозитория.
type DB interface {
	ExtDB
	Repository
}

type ORCClient interface {
	GetUserMasterKey(ctx context.Context, userID uint32) ([32]byte, error)
}

// Start — ядро является единственным владельцем lifecycle realtime-сессии:
// запускает провайдера, отдаёт каналы аудио/событий через StartCh.Realtime и
// закрывает сессию по respID.
type Start interface {
	StartSession(start *model.StartCh) <-chan error
	CloseSession(respID uint64)
}

// whatsAppChannelName — имя канала WhatsApp для включения/отключения в БД.
const whatsAppChannelName = "Whats"

// countSyncMap возвращает число ключей в sync.Map.
func countSyncMap(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

type Bot struct {
	userID     uint32
	b          *whatsmeow.Client
	callClient *meowcaller.Client
	calls      *callspkg.Manager
	ctx        context.Context
	cancel     context.CancelFunc
	container  *devicestore.Store
	stopped    atomic.Bool // Флаг остановки (потокобезопасный)
	// Модель ассистента
	assist *model.Assistant
	// Хранилище информации о респондентах (собеседниках) + Redis-кеш первого взаимодействия
	responders *responder.Store
	// Список ид пользователей для которых бот будет работать
	uids  []int64
	end   Endpoint
	db    DB
	mod   Model
	c     *crm.User
	start Start // Ядро Start — владелец lifecycle realtime-сессий
	// время успешного подключения для игнорирования сообщений из истории
	connectedAt time.Time
	// Статус печатает до ответа ассистента
	typingCancels   sync.Map // key: uint64 (senderID), value: context.CancelFunc
	parent          *User
	offlineMsgSync  chan struct{} // Завершена синхронизация оффлайн сообщений
	offlineSyncOnce sync.Once
	voiceCall       bool // Поддержка входящих вызовов (включается в настройках токена бота)
	textMessage     bool // Поддержка входящих текстовых сообщений (включается в настройках токена бота)
}

// User представляет все пользовательские WhatsApp боты
type User struct {
	ctx    context.Context
	cancel context.CancelFunc
	db     DB
	mod    Model
	end    Endpoint
	crm    CRM
	// HTTP/WS-адаптер (методы промотируются на *User)
	*handlerspkg.Handler
	authRegistry *authpkg.Registry
	uBot         sync.Map // key: uint32 (userID), value: *Bot
	start        Start    // Ядро Start — владелец lifecycle realtime-сессий
	// Канал запуска realtime-сессий для ядра Start (владелец — User, без глобалов)
	startCh chan model.StartCh
	// Включение операторского режима
	operatorModeByDialog sync.Map // key: dialogId (uint64), value: bool
	op                   Operator
	// Для управления кешированием первого взаимодействия
	responderCache responder.Cache
	// Получение ключа пользователя
	rpc         ORCClient
	masterKeyMu sync.Mutex
	masterKeys  map[uint32]cachedMasterKey
}

func New(parent context.Context, d DB, m Model, e Endpoint, c CRM, o ORCClient, redisClient redis.UniversalClient) *User {
	ctx, cancel := context.WithCancel(parent)
	u := &User{
		ctx:            ctx,
		cancel:         cancel,
		end:            e,
		db:             d,
		mod:            m,
		crm:            c,
		rpc:            o,
		masterKeys:     make(map[uint32]cachedMasterKey),
		responderCache: responder.NewRedisCache(redisClient),
		startCh:        make(chan model.StartCh, 100),
	}
	u.authRegistry = authpkg.NewRegistry()
	u.Handler = handlerspkg.New(u, u.authRegistry)
	return u
}

func (u *User) SetOperator(op Operator) { u.op = op }

// StartCh отдаёт канал запуска realtime-сессий для ядра Start.
func (u *User) StartCh() <-chan model.StartCh { return u.startCh }

// SetStart прокидывает ядро Start в User, чтобы боты могли запускать/закрывать
// realtime-сессии через единственного владельца lifecycle.
func (u *User) SetStart(s Start) { u.start = s }

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
	metrics.TrackOperatorModeDialogs(userId, countSyncMap(&u.operatorModeByDialog))
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

// SetOperatorMode выставляет/снимает режим оператора для конкретного диалога.
// Метрику трекают вызывающие, которые знают userID (User — мультипользовательский).
func (u *User) SetOperatorMode(dialogId uint64, operator bool) {
	if operator {
		u.operatorModeByDialog.Store(dialogId, true)
	} else {
		u.operatorModeByDialog.Delete(dialogId)
	}
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
