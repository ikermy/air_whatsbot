package calls

import (
	"context"
	"errors"
	"testing"

	"github.com/ikermy/air-common/pkg/model"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
)

type stubHost struct{}

func (stubHost) Context() context.Context                         { return context.Background() }
func (stubHost) UserID() uint32                                   { return 1 }
func (stubHost) VoiceCallsEnabled() bool                          { return false }
func (stubHost) IsCallAllowed(string) bool                        { return true }
func (stubHost) Stopped() bool                                    { return false }
func (stubHost) CallClient() *meowcaller.Client                   { return nil }
func (stubHost) WhatsAppClient() *whatsmeow.Client                { return nil }
func (stubHost) StartSession(*model.StartCh) <-chan error         { return nil }
func (stubHost) CloseSession(uint64)                              {}
func (stubHost) Model() model.Inter                               { return nil }
func (stubHost) Assistant() *model.Assistant                      { return nil }
func (stubHost) EnsureUserChannels(uint64, string) error          { return nil }
func (stubHost) RealtimeProvider() (model.RealtimeProvider, bool) { return nil, false }

func TestActiveCallIDsEmpty(t *testing.T) {
	m := NewManager(stubHost{})
	if ids := m.ActiveCallIDs(); len(ids) != 0 {
		t.Fatalf("got %v active calls, want none", ids)
	}
}

func TestSubscribeUnknownCall(t *testing.T) {
	m := NewManager(stubHost{})
	_, err := m.Subscribe(context.Background(), "missing", 0)
	if !errors.Is(err, ErrCallNotFound) {
		t.Fatalf("Subscribe error = %v, want ErrCallNotFound", err)
	}
}

func TestInitiateOutgoingWithoutCallClient(t *testing.T) {
	m := NewManager(stubHost{})
	if _, err := m.InitiateOutgoing(context.Background(), "123"); err == nil {
		t.Fatal("expected error when call client is not configured")
	}
}

func TestStopWithoutCalls(t *testing.T) {
	m := NewManager(stubHost{})
	m.Stop("test") // must not panic
}
