package auth

import "testing"

func TestRegistryAdd(t *testing.T) {
	r := NewRegistry()

	if !r.Add("s1", &Session{UserId: 1}) {
		t.Fatal("first Add must succeed")
	}
	if r.Add("s2", &Session{UserId: 1}) {
		t.Fatal("second Add for the same user must fail while the first is active")
	}
	if !r.Add("s3", &Session{UserId: 2}) {
		t.Fatal("Add for another user must succeed")
	}

	r.Remove("s1")
	if !r.Add("s4", &Session{UserId: 1}) {
		t.Fatal("Add must succeed after the previous session was removed")
	}
}

func TestRegistryCancelUser(t *testing.T) {
	r := NewRegistry()
	old := &Session{UserId: 1, StateChan: make(chan State, 1)}
	if !r.Add("s1", old) {
		t.Fatal("Add must succeed")
	}
	other := &Session{UserId: 2}
	if !r.Add("s2", other) {
		t.Fatal("Add for another user must succeed")
	}

	r.CancelUser(1)

	select {
	case state := <-old.StateChan:
		if state.Type != "cancelled" {
			t.Fatalf("cancelled state type = %q", state.Type)
		}
	default:
		t.Fatal("cancelled state was not delivered")
	}

	// После CancelUser можно зарегистрировать новую сессию того же пользователя.
	if !r.Add("s3", &Session{UserId: 1}) {
		t.Fatal("Add after CancelUser must succeed")
	}
	// Сессия другого пользователя не тронута и всё ещё активна.
	if r.Add("s4", &Session{UserId: 2}) {
		t.Fatal("session of another user must remain active")
	}
}

func TestRegistryNilSafe(t *testing.T) {
	var r *Registry
	if !r.Add("s", &Session{UserId: 1}) {
		t.Fatal("nil registry Add should be a no-op returning true")
	}
	r.Remove("s")
}
