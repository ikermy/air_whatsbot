package messages

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func newFilterTextMessage() *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			ID:        "1",
			Timestamp: time.Now(),
			MessageSource: types.MessageSource{
				Sender: types.NewJID("1234567890", types.DefaultUserServer),
				Chat:   types.NewJID("1234567890", types.DefaultUserServer),
			},
		},
		Message: &waE2E.Message{Conversation: proto.String("hi")},
	}
}

func TestShouldIgnoreMessage(t *testing.T) {
	connectedLongAgo := time.Now().Add(-time.Minute)

	assertIgnored := func(t *testing.T, msg *events.Message, connectedAt time.Time, wantReason string) {
		t.Helper()
		ignore, reason := ShouldIgnoreMessage(msg, connectedAt, "bot")
		if !ignore {
			t.Fatalf("message must be ignored, got reason %q", reason)
		}
		if reason != wantReason {
			t.Fatalf("reason = %q, want %q", reason, wantReason)
		}
	}

	assertAllowed := func(t *testing.T, msg *events.Message, connectedAt time.Time) {
		t.Helper()
		if ignore, reason := ShouldIgnoreMessage(msg, connectedAt, "bot"); ignore {
			t.Fatalf("message must not be ignored, reason %q", reason)
		}
	}

	assertIgnored(t, nil, connectedLongAgo, "nil_message")
	assertIgnored(t, newFilterTextMessage(), time.Now(), "message_ignore_window")

	status := newFilterTextMessage()
	status.Info.Sender = types.NewJID("status", "broadcast")
	assertIgnored(t, status, connectedLongAgo, "status_broadcast")

	fromMe := newFilterTextMessage()
	fromMe.Info.MessageSource.IsFromMe = true
	assertIgnored(t, fromMe, connectedLongAgo, "from_me_or_broadcast")

	history := newFilterTextMessage()
	history.Info.ID = "BAE5history"
	assertIgnored(t, history, connectedLongAgo, "history_id")

	tooOld := newFilterTextMessage()
	tooOld.Info.Timestamp = time.Now().Add(-10 * time.Minute)
	assertIgnored(t, tooOld, connectedLongAgo, "too_old")

	assertAllowed(t, newFilterTextMessage(), connectedLongAgo)
}
