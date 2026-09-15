package whatsapp

import (
	"context"
	"fmt"
	"strconv"

	"air_whatsbot/internal/whatsapp/calls"

	"github.com/ikermy/air-common/pkg/model"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
)

// Публичные типы звонков реэкспортируются для delivery/rpc.
type CallEvent = calls.CallEvent
type CallEventHandler = calls.CallEventHandler

type realtimeProviderRouter interface {
	GetRealtimeProvider(userID uint32) (model.RealtimeProvider, bool)
}

var ErrCallNotFound = calls.ErrCallNotFound

// Bot реализует calls.Host.

func (b *Bot) Context() context.Context          { return b.ctx }
func (b *Bot) UserID() uint32                    { return b.userID }
func (b *Bot) VoiceCallsEnabled() bool           { return b.voiceCall }
func (b *Bot) Stopped() bool                     { return b.stopped.Load() }
func (b *Bot) CallClient() *meowcaller.Client    { return b.callClient }
func (b *Bot) WhatsAppClient() *whatsmeow.Client { return b.b }
func (b *Bot) StartSession(start *model.StartCh) <-chan error {
	if b.start == nil {
		errCh := make(chan error, 1)
		errCh <- fmt.Errorf("ядро Start не сконфигурировано")
		close(errCh)
		return errCh
	}
	return b.start.StartSession(start)
}

func (b *Bot) CloseSession(respID uint64) {
	if b.start != nil {
		b.start.CloseSession(respID)
	}
}

func (b *Bot) Model() model.Inter          { return b.mod }
func (b *Bot) Assistant() *model.Assistant { return b.assist }
func (b *Bot) EnsureUserChannels(respID uint64, name string) error {
	return b.initializeUserChannels(respID, name)
}
func (b *Bot) RealtimeProvider() (model.RealtimeProvider, bool) {
	return b.getRealtimeProvider()
}

// IsCallAllowed проверяет, что собеседник входит в список разрешённых.
func (b *Bot) IsCallAllowed(user string) bool {
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

func (b *Bot) registerCallHandlers() {
	if b.calls != nil {
		b.calls.Register()
	}
}

func (b *Bot) stopCalls(reason string) {
	if b.calls != nil {
		b.calls.Stop(reason)
	}
}

func (b *Bot) InitiateOutgoingCall(ctx context.Context, target string, handlers ...CallEventHandler) (*meowcaller.Call, error) {
	if b.calls == nil {
		return nil, ErrCallNotFound
	}
	return b.calls.InitiateOutgoing(ctx, target, handlers...)
}

func (b *Bot) HangupCall(callID string) error {
	if b.calls == nil {
		return ErrCallNotFound
	}
	return b.calls.Hangup(callID)
}

func (b *Bot) ActiveCallIDs() []string {
	if b.calls == nil {
		return nil
	}
	return b.calls.ActiveCallIDs()
}

func (b *Bot) SubscribeCallEvents(ctx context.Context, callID string, afterSequence uint64) (<-chan CallEvent, error) {
	if b.calls == nil {
		return nil, ErrCallNotFound
	}
	return b.calls.Subscribe(ctx, callID, afterSequence)
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
