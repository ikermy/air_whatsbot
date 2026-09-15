package whatsapp

import (
	"errors"
	"testing"

	"github.com/ikermy/air-common/pkg/com"
)

type fakeNotifier struct {
	called bool
	err    error
}

func (f *fakeNotifier) SendNotification(com.CarpCh) error {
	f.called = true
	return f.err
}

type fakeChannelDisabler struct {
	called bool
	err    error
}

func (f *fakeChannelDisabler) SetChannelEnabled(uint32, string, bool) error {
	f.called = true
	return f.err
}

func TestRunReauthRunsAllStepsOnNotificationError(t *testing.T) {
	notifier := &fakeNotifier{err: errors.New("notification failed")}
	disabler := &fakeChannelDisabler{}
	resetCalled := false

	err := runReauth(42, notifier, disabler, func() error {
		resetCalled = true
		return nil
	})

	if err == nil {
		t.Fatal("expected aggregated error when notification fails")
	}
	if !disabler.called {
		t.Error("channel must be disabled even when notification fails")
	}
	if !resetCalled {
		t.Error("reset must run even when notification fails")
	}
}

func TestRunReauthSuccess(t *testing.T) {
	notifier := &fakeNotifier{}
	disabler := &fakeChannelDisabler{}
	resetCalled := false

	if err := runReauth(42, notifier, disabler, func() error {
		resetCalled = true
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !notifier.called || !disabler.called || !resetCalled {
		t.Error("all reauth steps must run on success")
	}
}

func TestRunReauthRejectsZeroUser(t *testing.T) {
	if err := runReauth(0, nil, nil, nil); err == nil {
		t.Fatal("expected validation error for userId=0")
	}
}
