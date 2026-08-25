package whatsapp

import (
	"context"
	"testing"
)

func TestInitiateOutgoingCallRejectsStoppedBot(t *testing.T) {
	var bot Bot
	bot.stopped.Store(true)

	_, err := bot.InitiateOutgoingCall(context.Background(), "1234567891000")
	if err == nil {
		t.Fatal("expected stopped bot error")
	}
}

func TestActiveCallIDsEmpty(t *testing.T) {
	var bot Bot
	if ids := bot.ActiveCallIDs(); len(ids) != 0 {
		t.Fatalf("got %v active calls, want none", ids)
	}
}
