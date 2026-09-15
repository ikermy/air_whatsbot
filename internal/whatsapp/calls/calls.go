package calls

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"air_whatsbot/internal/metrics"

	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

const (
	callSetupTimeout          = 45 * time.Second
	callLifetimeTimeout       = 2 * time.Hour
	whatsappRealtimeInputGain = 2.0 // Нужно боольше тестов!
)

// Host — порт, который реализует владелец бота и предоставляет звонкам доступ
// к клиентам, модели и lifecycle realtime-сессий.
type Host interface {
	Context() context.Context
	UserID() uint32
	VoiceCallsEnabled() bool
	IsCallAllowed(user string) bool
	Stopped() bool
	CallClient() *meowcaller.Client
	WhatsAppClient() *whatsmeow.Client
	StartSession(start *model.StartCh) <-chan error
	CloseSession(respID uint64)
	Model() model.Inter
	Assistant() *model.Assistant
	EnsureUserChannels(respID uint64, name string) error
	RealtimeProvider() (model.RealtimeProvider, bool)
}

// Manager управляет активными WhatsApp-звонками одного бота.
type Manager struct {
	host   Host
	active sync.Map // key: call ID (string), value: *callSession
}

func NewManager(host Host) *Manager {
	return &Manager{host: host}
}

// callSession содержит минимальное состояние звонка до подключения realtime-аудио.
type callSession struct {
	ctx          context.Context
	cancel       context.CancelFunc
	call         *meowcaller.Call
	once         sync.Once
	respID       uint64 // != 0 — realtime-сессия запущена (владелец lifecycle — Start)
	source       *realtimeAudioSource
	ready        atomic.Bool
	direction    string
	recorder     meowcaller.AudioSink
	onEvent      CallEventHandler
	eventHub     *eventHub
	lastActivity atomic.Int64
}

func (s *callSession) emitEvent(event CallEvent) {
	event.CallID = s.call.ID()
	if s.eventHub != nil {
		s.eventHub.publish(event)
	}
	if s.onEvent != nil {
		go s.onEvent(event)
	}
}

// Register подписывает менеджер на входящие звонки клиента.
func (m *Manager) Register() {
	client := m.host.CallClient()
	if client == nil {
		return
	}
	client.OnIncomingCall(func(call *meowcaller.Call) {
		go m.handleIncoming(call)
	})
}

func (m *Manager) handleIncoming(call *meowcaller.Call) {
	if call == nil {
		return
	}

	peer := call.Peer()
	userID := m.host.UserID()
	if !m.host.VoiceCallsEnabled() || !mode.IsVoiceCallModeEnabled() || !m.host.IsCallAllowed(peer.User) {
		metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(userID), "incoming", "rejected").Inc()
		if !m.host.VoiceCallsEnabled() || !mode.IsVoiceCallModeEnabled() {
			logger.Info("Входящий WhatsApp-звонок отклонён: голосовые звонки отключены", userID)
		} else {
			logger.Info("Входящий WhatsApp-звонок отклонён для %s: пользователь не разрешён", peer.String(), userID)
		}
		if err := call.Reject(); err != nil {
			logger.Warn("Ошибка отклонения WhatsApp-звонка: %v", err, userID)
		}
		return
	}

	ctx, cancel := context.WithCancel(m.host.Context())
	session := &callSession{ctx: ctx, cancel: cancel, call: call, direction: "incoming"}
	m.registerSession(call, session)

	call.OnReady(func() {
		session.ready.Store(true)
		metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(userID), "incoming", "accepted").Inc()
		respID, err := m.resolvePeer(peer)
		if err != nil {
			m.failRealtime(call, err)
			return
		}
		if err := m.startRealtime(session, respID); err != nil {
			m.failRealtime(call, err)
		}
	})
	if err := call.Answer(); err != nil {
		metrics.WhatsAppCallErrors.WithLabelValues(metrics.BotLabel(userID), "answer").Inc()
		logger.Error("Ошибка ответа на входящий WhatsApp-звонок от %s: %v", peer.String(), err, userID)
		m.cleanup(call.ID(), "answer_error")
		return
	}

	logger.Info("Входящий WhatsApp-звонок принят от %s", peer.String(), userID)
}

func (m *Manager) registerSession(call *meowcaller.Call, session *callSession) {
	session.eventHub = newEventHub()
	session.lastActivity.Store(time.Now().UnixNano())
	m.active.Store(call.ID(), session)
	metrics.ActiveWhatsAppCalls.WithLabelValues(metrics.BotLabel(m.host.UserID())).Set(float64(len(m.ActiveCallIDs())))
	m.watchTimeout(session)
	call.OnEnd(func(reason string) {
		m.cleanup(call.ID(), reason)
	})
}

// Subscribe возвращает поток событий звонка, начиная после указанной последовательности.
func (m *Manager) Subscribe(ctx context.Context, callID string, afterSequence uint64) (<-chan CallEvent, error) {
	value, ok := m.active.Load(callID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCallNotFound, callID)
	}
	session := value.(*callSession)
	return session.eventHub.subscribe(ctx, afterSequence)
}

func (m *Manager) watchTimeout(session *callSession) {
	userID := m.host.UserID()
	go func() {
		setupTimer := time.NewTimer(callSetupTimeout)
		defer setupTimer.Stop()
		select {
		case <-session.ctx.Done():
			return
		case <-setupTimer.C:
			if !session.ready.Load() {
				metrics.WhatsAppCallErrors.WithLabelValues(metrics.BotLabel(userID), "media_setup_timeout").Inc()
				m.endTimedOut(session, "media_setup_timeout")
				return
			}
		}
		lifeTimer := time.NewTimer(callLifetimeTimeout)
		idleTicker := time.NewTicker(15 * time.Second)
		defer lifeTimer.Stop()
		defer idleTicker.Stop()
		select {
		case <-session.ctx.Done():
		case <-lifeTimer.C:
			m.endTimedOut(session, "lifetime_timeout")
		case <-idleTicker.C:
			if time.Since(time.Unix(0, session.lastActivity.Load())) > 2*time.Minute {
				metrics.WhatsAppCallErrors.WithLabelValues(metrics.BotLabel(userID), "idle_timeout").Inc()
				m.endTimedOut(session, "idle_timeout")
			}
		}
	}()
}

func (m *Manager) endTimedOut(session *callSession, reason string) {
	userID := m.host.UserID()
	if _, ok := m.active.Load(session.call.ID()); !ok {
		return
	}
	logger.Warn("WhatsApp-звонок %s завершён по таймауту: %s", session.call.ID(), reason, userID)
	if err := session.call.Hangup(); err != nil {
		logger.Debug("Ошибка завершения WhatsApp-звонка по таймауту: %v", err, userID)
	}
	m.cleanup(session.call.ID(), reason)
}

func (m *Manager) failRealtime(call *meowcaller.Call, err error) {
	userID := m.host.UserID()
	metrics.WhatsAppCallErrors.WithLabelValues(metrics.BotLabel(userID), "realtime").Inc()
	logger.Error("Realtime для WhatsApp-звонка не запущен: %v", err, userID)
	m.cleanup(call.ID(), "realtime_start_error")
	if hangupErr := call.Hangup(); hangupErr != nil {
		logger.Debug("Ошибка завершения звонка после ошибки realtime: %v", hangupErr, userID)
	}
}

func (m *Manager) cleanup(callID, reason string) {
	userID := m.host.UserID()
	value, ok := m.active.Load(callID)
	if !ok {
		return
	}
	session := value.(*callSession)
	session.once.Do(func() {
		session.cancel()
		session.emitEvent(CallEvent{Type: "call_ended"})
		if session.eventHub != nil {
			session.eventHub.close()
		}
		metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(userID), session.direction, "ended").Inc()
		// Ядро Start — единственный владелец lifecycle realtime-сессии.
		if session.respID != 0 {
			m.host.CloseSession(session.respID)
		}
		if session.source != nil {
			_ = session.source.Close()
		}
		if session.recorder != nil {
			_ = session.recorder.Close()
		}
		m.active.Delete(callID)
		metrics.ActiveWhatsAppCalls.WithLabelValues(metrics.BotLabel(userID)).Set(float64(len(m.ActiveCallIDs())))
		logger.Debug("WhatsApp-звонок %s завершён: %s", callID, reason, userID)
	})
}

// Stop завершает все активные звонки.
func (m *Manager) Stop(reason string) {
	m.active.Range(func(key, _ any) bool {
		m.cleanup(key.(string), reason)
		return true
	})
}

// InitiateOutgoing запускает исходящий WhatsApp-звонок.
func (m *Manager) InitiateOutgoing(ctx context.Context, target string, handlers ...CallEventHandler) (*meowcaller.Call, error) {
	userID := m.host.UserID()
	callClient := m.host.CallClient()
	if callClient == nil {
		return nil, fmt.Errorf("WhatsApp call client не инициализирован")
	}
	waClient := m.host.WhatsAppClient()
	if waClient == nil || !waClient.IsLoggedIn() {
		return nil, fmt.Errorf("WhatsApp-клиент ещё не подключён или не авторизован")
	}
	if m.host.Stopped() {
		return nil, fmt.Errorf("WhatsApp-бот остановлен")
	}
	if !m.host.VoiceCallsEnabled() || !mode.IsVoiceCallModeEnabled() {
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
	m.active.Range(func(key, value any) bool {
		callID := key.(string)
		session := value.(*callSession)
		logger.Info("Завершение предыдущего WhatsApp-звонка %s перед новым исходящим звонком", callID, userID)
		if err := session.call.Hangup(); err != nil {
			logger.Debug("Ошибка завершения предыдущего WhatsApp-звонка %s: %v", callID, err, userID)
		}
		m.cleanup(callID, "replaced_by_outgoing")
		return true
	})

	call, err := callClient.Call(ctx, target)
	if err != nil {
		metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(userID), "outgoing", "error").Inc()
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
	m.registerSession(call, session)
	session.emitEvent(CallEvent{Type: "call_started"})
	// По официальному примеру meowcaller источник подключается сразу после Call().
	// Player отправляет silence до ответа peer, поэтому media loop не блокируется,
	// а greeting air-common может быть поставлен в очередь заранее.
	respID, err := m.resolvePeer(call.Peer())
	if err != nil {
		m.failRealtime(call, err)
		return nil, err
	}
	session.ready.Store(true)
	if err := m.startRealtime(session, respID); err != nil {
		m.failRealtime(call, err)
		return nil, err
	}
	metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(userID), "outgoing", "accepted").Inc()
	session.emitEvent(CallEvent{Type: "call_connected"})

	logger.Info("Исходящий WhatsApp-звонок запущен: %s", target, userID)
	metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(userID), "outgoing", "started").Inc()
	return call, nil
}

// Hangup завершает активный звонок по его идентификатору.
func (m *Manager) Hangup(callID string) error {
	if callID == "" {
		return fmt.Errorf("идентификатор WhatsApp-звонка не указан")
	}
	value, ok := m.active.Load(callID)
	if !ok {
		return fmt.Errorf("активный WhatsApp-звонок %s не найден", callID)
	}
	session := value.(*callSession)
	if err := session.call.Hangup(); err != nil {
		return fmt.Errorf("ошибка завершения WhatsApp-звонка %s: %w", callID, err)
	}
	m.cleanup(callID, "local_hangup")
	return nil
}

// ActiveCallIDs возвращает идентификаторы текущих звонков.
func (m *Manager) ActiveCallIDs() []string {
	ids := make([]string, 0)
	m.active.Range(func(key, _ any) bool {
		if id, ok := key.(string); ok {
			ids = append(ids, id)
		}
		return true
	})
	return ids
}

// startRealtime запускает realtime-сессию через ядро Start — единственного
// владельца её lifecycle. StartSession сам достаёт провайдера из Router и
// заполняет startCh.Realtime каналами; после старта провайдер тянется вручную
// только для SendRealtimeAudio (входящее аудио не канализуется ядром).
func (m *Manager) startRealtime(session *callSession, respID uint64) error {
	userID := m.host.UserID()
	provider, ok := m.host.RealtimeProvider()
	if !ok {
		return fmt.Errorf("модель пользователя не поддерживает RealtimeProvider")
	}
	mod := m.host.Model()
	ch, err := mod.GetCh(respID)
	if err != nil {
		if initErr := m.host.EnsureUserChannels(respID, strconv.FormatUint(respID, 10)); initErr != nil {
			return err
		}
		ch, err = mod.GetCh(respID)
		if err != nil {
			return err
		}
	}
	// A channel may survive a bot restart while its in-memory RespModel is
	// gone. GetOrSetRespGPT восстанавливает модель для диалога.
	usrMod, err := mod.GetOrSetRespGPT(*m.host.Assistant(), ch.DialogID, respID, strconv.FormatUint(respID, 10))
	if err != nil && !strings.Contains(err.Error(), "получены пустые данные") {
		return fmt.Errorf("ошибка модели пользователя: %w", err)
	}
	if usrMod == nil {
		return fmt.Errorf("модель пользователя не инициализирована для respId=%d", respID)
	}

	startCh := &model.StartCh{
		Ctx:      m.host.Context(),
		ChName:   comdom.WhatsApp,
		Model:    usrMod,
		Channel:  ch,
		ThreadId: ch.DialogID,
		RespId:   respID,
		// Realtime != nil — запрос realtime-режима; StartSession заполнит
		// AudioTx/Drain/Events до возврата.
		Realtime: &model.RealtimeChannels{},
	}
	errCh := m.host.StartSession(startCh)
	if startCh.Realtime == nil || startCh.Realtime.AudioTx == nil {
		select {
		case e := <-errCh:
			if e != nil {
				return fmt.Errorf("realtime-сессия не запущена: %w", e)
			}
		default:
		}
		return fmt.Errorf("realtime-сессия не запущена для respId=%d", respID)
	}
	source := newRealtimeAudioSource(session.ctx)
	session.respID, session.source = respID, source
	// errCh закрывает ядро; вычитываем, иначе писатель заблокируется и ошибки потеряются.
	go func() {
		for err := range errCh {
			if err != nil {
				logger.Warn("Realtime-сессия respId=%d: %v", respID, err, userID)
			}
		}
	}()

	audioOut := startCh.Realtime.AudioTx
	drain := startCh.Realtime.Drain
	events := startCh.Realtime.Events

	var realtimeInputPending []byte
	session.call.Receive(meowcaller.SinkFunc(func(frame []float32) {
		session.lastActivity.Store(time.Now().UnixNano())
		if session.recorder != nil {
			if err := session.recorder.WriteFrame(frame); err != nil {
				logger.Debug("Ошибка записи входящего WhatsApp-аудио: %v", err, userID)
			}
		}
		realtimeInputPending = append(realtimeInputPending, amplifyPCM16(pcm16At24k(frame), whatsappRealtimeInputGain)...)
		const realtimeInputChunkBytes = 960 // 20 ms @ 24 kHz, mono PCM16
		for len(realtimeInputPending) >= realtimeInputChunkBytes {
			pcm24 := append([]byte(nil), realtimeInputPending[:realtimeInputChunkBytes]...)
			realtimeInputPending = realtimeInputPending[realtimeInputChunkBytes:]
			if err := provider.SendRealtimeAudio(respID, pcm24); err != nil {
				logger.Debug("Ошибка передачи аудио в realtime: %v", err, userID)
			}
		}
	}))
	session.call.Play(source)
	go func() {
		pending := make([]float32, 0, meowcaller.FrameSamples*2)
		pushFrame := func(frame []float32) bool {
			if err := source.Push(frame); err != nil {
				if session.ctx.Err() == nil {
					metrics.WhatsAppCallDroppedFrames.WithLabelValues(metrics.BotLabel(userID), session.direction).Inc()
				}
				return false
			}
			session.lastActivity.Store(time.Now().UnixNano())
			return true
		}
		for {
			select {
			case <-session.ctx.Done():
				return
			case data, ok := <-audioOut:
				if !ok {
					if len(pending) > 0 {
						frame := make([]float32, meowcaller.FrameSamples)
						copy(frame, pending)
						pushFrame(frame)
					}
					return
				}
				pending = append(pending, pcm16At16k(data)...)
				for len(pending) >= meowcaller.FrameSamples {
					frame := pending[:meowcaller.FrameSamples]
					pending = pending[meowcaller.FrameSamples:]
					if !pushFrame(frame) && session.ctx.Err() != nil {
						return
					}
				}
			}
		}
	}()
	go func() {
		select {
		case <-session.ctx.Done():
		case <-drain:
			metrics.WhatsAppCallResponses.WithLabelValues(metrics.BotLabel(userID), "drain").Inc()
			logger.Debug("Realtime audio drain завершён для WhatsApp-звонка %s", session.call.ID(), userID)
		}
	}()
	go func() {
		for {
			select {
			case <-session.ctx.Done():
				return
			case event, ok := <-events:
				if !ok {
					return
				}
				if event.Type == "error" {
					session.emitEvent(CallEvent{Type: event.Type, Delta: event.Delta, Text: event.Text, ResponseID: event.ResponseID, Err: event.Err, Data: event.Data, Files: event.Files})
					m.failRealtime(session.call, event.Err)
					return
				}
				session.emitEvent(CallEvent{Type: event.Type, Delta: event.Delta, Text: event.Text, ResponseID: event.ResponseID, Err: event.Err, Data: event.Data, Files: event.Files})
				if event.Type == "response_done" {
					metrics.WhatsAppCallResponses.WithLabelValues(metrics.BotLabel(userID), "response_done").Inc()
				}
			}
		}
	}()
	return nil
}

func (m *Manager) resolvePeer(peer types.JID) (uint64, error) {
	waClient := m.host.WhatsAppClient()
	if peer.Server != types.DefaultUserServer && waClient != nil && waClient.Store != nil {
		alt, err := waClient.Store.GetAltJID(m.host.Context(), peer)
		if err == nil && alt.Server == types.DefaultUserServer && alt.User != "" {
			peer = alt
		}
	}
	respID, err := strconv.ParseUint(peer.User, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("не удалось определить телефон собеседника %s: %w", peer.String(), err)
	}
	return respID, nil
}
