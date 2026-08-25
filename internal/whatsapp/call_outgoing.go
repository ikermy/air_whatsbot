package whatsapp

import (
	"air_whatsbot/internal/metrics"
	"context"
	"fmt"

	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
)

// InitiateOutgoingCall запускает исходящий WhatsApp-звонок.
// Realtime-аудиомост подключается после готовности media-канала звонка.
func (b *Bot) InitiateOutgoingCall(ctx context.Context, target string, handlers ...CallEventHandler) (*meowcaller.Call, error) {
	if b.callClient == nil {
		return nil, fmt.Errorf("WhatsApp call client не инициализирован")
	}
	if b.b == nil || !b.b.IsLoggedIn() {
		return nil, fmt.Errorf("WhatsApp-клиент ещё не подключён или не авторизован")
	}
	if b.stopped.Load() {
		return nil, fmt.Errorf("WhatsApp-бот остановлен")
	}
	if !b.voiceCall || !mode.IsVoiceCallModeEnabled() {
		return nil, fmt.Errorf("голосовые звонки WhatsApp отключены")
	}
	if target == "" {
		return nil, fmt.Errorf("контакт для WhatsApp-звонка не указан")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("контекст исходящего WhatsApp-звонка завершён: %w", err)
	}
	// One active call is allowed per WhatsApp bot/user. Replace any previous
	// call before sending a new offer, regardless of its direction.
	b.activeCalls.Range(func(key, value any) bool {
		callID := key.(string)
		session := value.(*callSession)
		logger.Info("Завершение предыдущего WhatsApp-звонка %s перед новым исходящим звонком", callID, b.userID)
		if err := session.call.Hangup(); err != nil {
			logger.Debug("Ошибка завершения предыдущего WhatsApp-звонка %s: %v", callID, err, b.userID)
		}
		b.cleanupCall(callID, "replaced_by_outgoing")
		return true
	})

	call, err := b.callClient.Call(ctx, target)
	if err != nil {
		metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(b.userID), "outgoing", "error").Inc()
		return nil, fmt.Errorf("ошибка исходящего WhatsApp-звонка: %w", err)
	}

	// The start RPC is intentionally fire-and-return. Keep the call session
	// independent from the short-lived RPC context; it must remain alive until
	// WhatsApp reports the call as ended or the bot explicitly cleans it up.
	callCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	var onEvent CallEventHandler
	if len(handlers) > 0 {
		onEvent = handlers[0]
	}
	session := &callSession{ctx: callCtx, cancel: cancel, call: call, direction: "outgoing", onEvent: onEvent}
	b.registerCallSession(call, session)
	session.emitEvent(CallEvent{Type: "call_started"})
	// По официальному примеру meowcaller источник подключается сразу после Call().
	// Player отправляет silence до ответа peer, поэтому media loop не блокируется,
	// а greeting air-common может быть поставлен в очередь заранее.
	respID, err := b.resolveCallPeer(call.Peer())
	if err != nil {
		b.failCallRealtime(call, err)
		return nil, err
	}
	session.ready.Store(true)
	if err := b.startRealtimeCall(session, respID); err != nil {
		b.failCallRealtime(call, err)
		return nil, err
	}
	metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(b.userID), "outgoing", "accepted").Inc()
	session.emitEvent(CallEvent{Type: "call_connected"})

	logger.Info("Исходящий WhatsApp-звонок запущен: %s", target, b.userID)
	metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(b.userID), "outgoing", "started").Inc()
	return call, nil
}

// HangupCall завершает активный звонок по его идентификатору.
func (b *Bot) HangupCall(callID string) error {
	if callID == "" {
		return fmt.Errorf("идентификатор WhatsApp-звонка не указан")
	}
	value, ok := b.activeCalls.Load(callID)
	if !ok {
		return fmt.Errorf("активный WhatsApp-звонок %s не найден", callID)
	}
	session := value.(*callSession)
	if err := session.call.Hangup(); err != nil {
		return fmt.Errorf("ошибка завершения WhatsApp-звонка %s: %w", callID, err)
	}
	b.cleanupCall(callID, "local_hangup")
	return nil
}

// ActiveCallIDs возвращает идентификаторы текущих звонков.
func (b *Bot) ActiveCallIDs() []string {
	ids := make([]string, 0)
	b.activeCalls.Range(func(key, _ any) bool {
		if id, ok := key.(string); ok {
			ids = append(ids, id)
		}
		return true
	})
	return ids
}

// InitiateOutgoingCall запускает звонок через бота пользователя.
func (u *User) InitiateOutgoingCall(ctx context.Context, userID uint32, target string, handlers ...CallEventHandler) (*meowcaller.Call, error) {
	value, ok := u.uBot.Load(userID)
	if !ok {
		return nil, fmt.Errorf("WhatsApp-бот пользователя %d не найден", userID)
	}
	bot, ok := value.(*Bot)
	if !ok || bot == nil {
		return nil, fmt.Errorf("некорректный WhatsApp-бот пользователя %d", userID)
	}
	return bot.InitiateOutgoingCall(ctx, target, handlers...)
}

// HangupCall завершает звонок пользователя через фасад User.
func (u *User) HangupCall(userID uint32, callID string) error {
	value, ok := u.uBot.Load(userID)
	if !ok {
		return fmt.Errorf("WhatsApp-бот пользователя %d не найден", userID)
	}
	bot, ok := value.(*Bot)
	if !ok || bot == nil {
		return fmt.Errorf("некорректный WhatsApp-бот пользователя %d", userID)
	}
	return bot.HangupCall(callID)
}

// ActiveCallIDs возвращает активные звонки указанного пользователя.
func (u *User) ActiveCallIDs(userID uint32) ([]string, error) {
	value, ok := u.uBot.Load(userID)
	if !ok {
		return nil, fmt.Errorf("WhatsApp-бот пользователя %d не найден", userID)
	}
	bot, ok := value.(*Bot)
	if !ok || bot == nil {
		return nil, fmt.Errorf("некорректный WhatsApp-бот пользователя %d", userID)
	}
	return bot.ActiveCallIDs(), nil
}

func (u *User) SubscribeCallEvents(ctx context.Context, userID uint32, callID string, afterSequence uint64) (<-chan CallEvent, error) {
	value, ok := u.uBot.Load(userID)
	if !ok {
		return nil, fmt.Errorf("WhatsApp bot %d not found", userID)
	}
	return value.(*Bot).SubscribeCallEvents(ctx, callID, afterSequence)
}
