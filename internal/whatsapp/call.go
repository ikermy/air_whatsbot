package whatsapp

import (
	"air_whatsbot/internal/metrics"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/types"
)

var ErrCallNotFound = errors.New("call not found")

const (
	callSetupTimeout          = 45 * time.Second
	callLifetimeTimeout       = 2 * time.Hour
	whatsappRealtimeInputGain = 2.0 // Нужно боольше тестов!
)

// callSession содержит минимальное состояние звонка до подключения realtime-аудио.
// Аудиомост будет добавлен на следующем этапе.
type callSession struct {
	ctx          context.Context
	cancel       context.CancelFunc
	call         *meowcaller.Call
	once         sync.Once
	respID       uint64
	realtime     model.RealtimeProvider
	source       *realtimeAudioSource
	ready        atomic.Bool
	events       <-chan model.RealtimeEvent
	direction    string
	drain        <-chan struct{}
	recorder     meowcaller.AudioSink
	onEvent      CallEventHandler
	eventHub     *callEventHub
	lastActivity atomic.Int64
}

type CallEvent struct {
	CallID     string
	Type       string
	Delta      string
	Text       string
	ResponseID string
	Err        error
	Sequence   uint64
	Timestamp  time.Time
	Data       []byte
	Files      []model.File
}

type CallEventHandler func(CallEvent)

func (s *callSession) emitEvent(event CallEvent) {
	event.CallID = s.call.ID()
	if s.eventHub != nil {
		s.eventHub.publish(event)
	}
	if s.onEvent != nil {
		go s.onEvent(event)
	}
}

func (b *Bot) registerCallHandlers() {
	if b.callClient == nil {
		return
	}
	b.callClient.OnIncomingCall(func(call *meowcaller.Call) {
		go b.handleIncomingCall(call)
	})
}

func (b *Bot) handleIncomingCall(call *meowcaller.Call) {
	if call == nil {
		return
	}

	peer := call.Peer()
	if !b.voiceCall || !mode.IsVoiceCallModeEnabled() || !b.isCallAllowed(peer.User) {
		metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(b.userID), "incoming", "rejected").Inc()
		if !b.voiceCall || !mode.IsVoiceCallModeEnabled() {
			logger.Info("Входящий WhatsApp-звонок отклонён: голосовые звонки отключены", b.userID)
		} else {
			logger.Info("Входящий WhatsApp-звонок отклонён для %s: пользователь не разрешён", peer.String(), b.userID)
		}
		if err := call.Reject(); err != nil {
			logger.Warn("Ошибка отклонения WhatsApp-звонка: %v", err, b.userID)
		}
		return
	}

	ctx, cancel := context.WithCancel(b.ctx)
	session := &callSession{ctx: ctx, cancel: cancel, call: call, direction: "incoming"}
	b.registerCallSession(call, session)

	call.OnReady(func() {
		session.ready.Store(true)
		metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(b.userID), "incoming", "accepted").Inc()
		respID, err := b.resolveCallPeer(peer)
		if err != nil {
			b.failCallRealtime(call, err)
			return
		}
		if err := b.startRealtimeCall(session, respID); err != nil {
			b.failCallRealtime(call, err)
		}
	})
	if err := call.Answer(); err != nil {
		metrics.WhatsAppCallErrors.WithLabelValues(metrics.BotLabel(b.userID), "answer").Inc()
		logger.Error("Ошибка ответа на входящий WhatsApp-звонок от %s: %v", peer.String(), err, b.userID)
		b.cleanupCall(call.ID(), "answer_error")
		return
	}

	logger.Info("Входящий WhatsApp-звонок принят от %s", peer.String(), b.userID)
}

func (b *Bot) registerCallSession(call *meowcaller.Call, session *callSession) {
	session.eventHub = newCallEventHub()
	session.lastActivity.Store(time.Now().UnixNano())
	b.activeCalls.Store(call.ID(), session)
	metrics.ActiveWhatsAppCalls.WithLabelValues(metrics.BotLabel(b.userID)).Set(float64(len(b.ActiveCallIDs())))
	b.watchCallTimeout(session)
	call.OnEnd(func(reason string) {
		b.cleanupCall(call.ID(), reason)
	})
}

func (b *Bot) SubscribeCallEvents(ctx context.Context, callID string, afterSequence uint64) (<-chan CallEvent, error) {
	value, ok := b.activeCalls.Load(callID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCallNotFound, callID)
	}
	session := value.(*callSession)
	return session.eventHub.subscribe(ctx, afterSequence)
}

func (b *Bot) watchCallTimeout(session *callSession) {
	go func() {
		setupTimer := time.NewTimer(callSetupTimeout)
		defer setupTimer.Stop()
		select {
		case <-session.ctx.Done():
			return
		case <-setupTimer.C:
			if !session.ready.Load() {
				metrics.WhatsAppCallErrors.WithLabelValues(metrics.BotLabel(b.userID), "media_setup_timeout").Inc()
				b.endTimedOutCall(session, "media_setup_timeout")
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
			b.endTimedOutCall(session, "lifetime_timeout")
		case <-idleTicker.C:
			if time.Since(time.Unix(0, session.lastActivity.Load())) > 2*time.Minute {
				metrics.WhatsAppCallErrors.WithLabelValues(metrics.BotLabel(b.userID), "idle_timeout").Inc()
				b.endTimedOutCall(session, "idle_timeout")
			}
		}
	}()
}

func (b *Bot) endTimedOutCall(session *callSession, reason string) {
	if _, ok := b.activeCalls.Load(session.call.ID()); !ok {
		return
	}
	logger.Warn("WhatsApp-звонок %s завершён по таймауту: %s", session.call.ID(), reason, b.userID)
	if err := session.call.Hangup(); err != nil {
		logger.Debug("Ошибка завершения WhatsApp-звонка по таймауту: %v", err, b.userID)
	}
	b.cleanupCall(session.call.ID(), reason)
}

func (b *Bot) failCallRealtime(call *meowcaller.Call, err error) {
	metrics.WhatsAppCallErrors.WithLabelValues(metrics.BotLabel(b.userID), "realtime").Inc()
	logger.Error("Realtime для WhatsApp-звонка не запущен: %v", err, b.userID)
	b.cleanupCall(call.ID(), "realtime_start_error")
	if hangupErr := call.Hangup(); hangupErr != nil {
		logger.Debug("Ошибка завершения звонка после ошибки realtime: %v", hangupErr, b.userID)
	}
}

func (b *Bot) cleanupCall(callID, reason string) {
	value, ok := b.activeCalls.Load(callID)
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
		metrics.WhatsAppCalls.WithLabelValues(metrics.BotLabel(b.userID), session.direction, "ended").Inc()
		if session.realtime != nil {
			if session.events != nil {
				session.realtime.UnsubscribeEvents(session.respID, session.events)
			}
			session.realtime.CloseRealtimeSession(session.respID)
		}
		if session.source != nil {
			_ = session.source.Close()
		}
		if session.recorder != nil {
			_ = session.recorder.Close()
		}
		b.activeCalls.Delete(callID)
		metrics.ActiveWhatsAppCalls.WithLabelValues(metrics.BotLabel(b.userID)).Set(float64(len(b.ActiveCallIDs())))
		logger.Debug("WhatsApp-звонок %s завершён: %s", callID, reason, b.userID)
	})
}

func (b *Bot) stopCalls(reason string) {
	b.activeCalls.Range(func(key, _ any) bool {
		b.cleanupCall(key.(string), reason)
		return true
	})
}

func (b *Bot) startRealtimeCall(session *callSession, respID uint64) error {
	provider, ok := b.getRealtimeProvider()
	if !ok {
		return fmt.Errorf("модель пользователя не поддерживает RealtimeProvider")
	}
	ch, err := b.mod.GetCh(respID)
	if err != nil {
		if initErr := b.initializeUserChannels(respID, strconv.FormatUint(respID, 10)); initErr != nil {
			return err
		}
		ch, err = b.mod.GetCh(respID)
		if err != nil {
			return err
		}
	}
	if err := provider.StartRealtimeSession(b.userID, ch.DialogID, respID); err != nil {
		// A channel may survive a bot restart while its in-memory RespModel is
		// gone. Rebuild the channel once and retry the realtime session.
		logger.Warn("Realtime-модель для respID=%d не запущена, переинициализируем канал: %v", respID, err, b.userID)
		if initErr := b.initializeUserChannels(respID, strconv.FormatUint(respID, 10)); initErr != nil {
			return fmt.Errorf("%w; переинициализация канала не удалась: %v", err, initErr)
		}
		ch, getErr := b.mod.GetCh(respID)
		if getErr != nil {
			return fmt.Errorf("%w; канал после переинициализации не найден: %v", err, getErr)
		}
		if retryErr := provider.StartRealtimeSession(b.userID, ch.DialogID, respID); retryErr != nil {
			return retryErr
		}
	}
	audioOut, err := provider.GetRealtimeAudio(respID)
	if err != nil {
		provider.CloseRealtimeSession(respID)
		return err
	}
	events, err := provider.SubscribeEvents(respID)
	if err != nil {
		provider.CloseRealtimeSession(respID)
		return err
	}
	drain, err := provider.GetRealtimeDrain(respID)
	if err != nil {
		provider.UnsubscribeEvents(respID, events)
		provider.CloseRealtimeSession(respID)
		return err
	}
	source := newRealtimeAudioSource(session.ctx)
	session.respID, session.realtime, session.source, session.events, session.drain = respID, provider, source, events, drain
	var realtimeInputPending []byte
	session.call.Receive(meowcaller.SinkFunc(func(frame []float32) {
		session.lastActivity.Store(time.Now().UnixNano())
		if session.recorder != nil {
			if err := session.recorder.WriteFrame(frame); err != nil {
				logger.Debug("Ошибка записи входящего WhatsApp-аудио: %v", err, b.userID)
			}
		}
		realtimeInputPending = append(realtimeInputPending, amplifyPCM16(pcm16At24k(frame), whatsappRealtimeInputGain)...)
		const realtimeInputChunkBytes = 960 // 20 ms @ 24 kHz, mono PCM16
		for len(realtimeInputPending) >= realtimeInputChunkBytes {
			pcm24 := append([]byte(nil), realtimeInputPending[:realtimeInputChunkBytes]...)
			realtimeInputPending = realtimeInputPending[realtimeInputChunkBytes:]
			if err := provider.SendRealtimeAudio(respID, pcm24); err != nil {
				logger.Debug("Ошибка передачи аудио в realtime: %v", err, b.userID)
			}
		}
	}))
	session.call.Play(source)
	go func() {
		pending := make([]float32, 0, meowcaller.FrameSamples*2)
		pushFrame := func(frame []float32) bool {
			if err := source.Push(frame); err != nil {
				if session.ctx.Err() == nil {
					metrics.WhatsAppCallDroppedFrames.WithLabelValues(metrics.BotLabel(b.userID), session.direction).Inc()
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
			metrics.WhatsAppCallResponses.WithLabelValues(metrics.BotLabel(b.userID), "drain").Inc()
			logger.Debug("Realtime audio drain завершён для WhatsApp-звонка %s", session.call.ID(), b.userID)
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
					b.failCallRealtime(session.call, event.Err)
					return
				}
				session.emitEvent(CallEvent{Type: event.Type, Delta: event.Delta, Text: event.Text, ResponseID: event.ResponseID, Err: event.Err, Data: event.Data, Files: event.Files})
				if event.Type == "response_done" {
					metrics.WhatsAppCallResponses.WithLabelValues(metrics.BotLabel(b.userID), "response_done").Inc()
				}
			}
		}
	}()
	return nil
}

func amplifyPCM16(data []byte, gain float64) []byte {
	if len(data) < 2 || gain == 1 {
		return data
	}
	out := make([]byte, len(data))
	for i := 0; i+1 < len(data); i += 2 {
		v := float64(int16(binary.LittleEndian.Uint16(data[i:]))) * gain
		if v > 32767 {
			v = 32767
		} else if v < -32768 {
			v = -32768
		}
		binary.LittleEndian.PutUint16(out[i:], uint16(int16(v)))
	}
	return out
}

type realtimeProviderRouter interface {
	GetRealtimeProvider(userID uint32) (model.RealtimeProvider, bool)
}

func (b *Bot) getRealtimeProvider() (model.RealtimeProvider, bool) {
	if provider, ok := any(b.mod).(model.RealtimeProvider); ok {
		return provider, true
	}
	if router, ok := any(b.mod).(realtimeProviderRouter); ok {
		return router.GetRealtimeProvider(b.userID)
	}
	return nil, false
}

func (b *Bot) resolveCallPeer(peer types.JID) (uint64, error) {
	if peer.Server != types.DefaultUserServer && b.b != nil && b.b.Store != nil {
		alt, err := b.b.Store.GetAltJID(b.ctx, peer)
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

// pcm16At24k converts meowcaller PCM float samples (16 kHz) to PCM16 at 24 kHz.
func pcm16At24k(frame []float32) []byte {
	if len(frame) == 0 {
		return nil
	}
	out := make([]byte, len(frame)*3)
	for i := 0; i < len(frame)*3/2; i++ {
		pos := float64(i) * 2 / 3
		left := int(pos)
		frac := float32(pos - float64(left))
		v := frame[left]
		if left+1 < len(frame) {
			v += (frame[left+1] - v) * frac
		}
		if v > 1 {
			v = 1
		}
		if v < -1 {
			v = -1
		}
		binary.LittleEndian.PutUint16(out[i*2:], uint16(int16(v*32767)))
	}
	return out
}

// pcm16At16k converts Realtime PCM16 (24 kHz) to meowcaller float samples (16 kHz).
func pcm16At16k(data []byte) []float32 {
	n := len(data) / 2
	if n == 0 {
		return nil
	}
	out := make([]float32, n*2/3)
	for i := range out {
		pos := float64(i) * 3 / 2
		left := int(pos)
		frac := float32(pos - float64(left))
		v := float32(int16(binary.LittleEndian.Uint16(data[left*2:]))) / 32768
		if left+1 < n {
			next := float32(int16(binary.LittleEndian.Uint16(data[(left+1)*2:]))) / 32768
			v += (next - v) * frac
		}
		out[i] = v
	}
	return out
}

type realtimeAudioSource struct {
	ctx    context.Context
	frames chan []float32
	done   chan struct{}
	once   sync.Once
	closed atomic.Bool
}

func newRealtimeAudioSource(ctx context.Context) *realtimeAudioSource {
	// Keep enough jitter tolerance for the 60 ms meowcaller media tick. The
	// producer uses a blocking Push, so shrinking this queue can stall the
	// media path and indirectly stop inbound audio delivery.
	return &realtimeAudioSource{ctx: ctx, frames: make(chan []float32, 8), done: make(chan struct{})}
}
func (s *realtimeAudioSource) ReadFrame() ([]float32, error) {
	select {
	case <-s.ctx.Done():
		return nil, io.EOF
	case <-s.done:
		return nil, io.EOF
	case f := <-s.frames:
		return f, nil
	default:
		// meowcaller должен получать кадр на каждом media tick.
		// Не блокируем его send loop в ожидании ответа realtime.
		return make([]float32, meowcaller.FrameSamples), nil
	}
}
func (s *realtimeAudioSource) Close() error {
	s.once.Do(func() { close(s.done) })
	s.closed.Store(true)
	return nil
}
func (s *realtimeAudioSource) Push(frame []float32) error {
	if s.closed.Load() {
		return io.EOF
	}
	select {
	case <-s.ctx.Done():
		return io.EOF
	case <-s.done:
		return io.EOF
	case s.frames <- frame:
		if s.closed.Load() {
			return io.EOF
		}
		return nil
	}
}

func (s *realtimeAudioSource) TryPush(frame []float32) bool {
	if s.closed.Load() {
		return false
	}
	select {
	case <-s.ctx.Done():
		return false
	case <-s.done:
		return false
	case s.frames <- frame:
		return true
	default:
		return false
	}
}

func (b *Bot) isCallAllowed(user string) bool {
	if len(b.uids) == 0 {
		return true
	}
	uid, err := strconv.ParseInt(user, 10, 64)
	if err != nil {
		return false
	}
	for _, allowedUID := range b.uids {
		if allowedUID == uid {
			return true
		}
	}
	return false
}
