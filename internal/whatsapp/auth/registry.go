package auth

import "sync"

// Registry — потокобезопасный реестр активных сессий QR-авторизации.
// Инстанс принадлежит владельцу (например, User), а не является глобальной переменной.
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewRegistry() *Registry {
	return &Registry{sessions: make(map[string]*Session)}
}

// Add регистрирует сессию. Возвращает false, если для пользователя уже идёт авторизация.
func (r *Registry) Add(sessionID string, session *Session) bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.sessions {
		if existing.UserId == session.UserId {
			return false
		}
	}
	r.sessions[sessionID] = session
	return true
}

func (r *Registry) Remove(sessionID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, sessionID)
}

// CancelUser сбрасывает активные сессии пользователя, уведомляя их об отмене.
// Нужен, чтобы новое WebSocket-подключение для того же пользователя не
// отклонялось, а заменяло предыдущее: иначе событие success уходит в старое,
// уже не слушаемое соединение.
func (r *Registry) CancelUser(userID uint32) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for sessionID, session := range r.sessions {
		if session.UserId != userID {
			continue
		}
		select {
		case session.StateChan <- State{
			Type:    "cancelled",
			Payload: "Сессия сброшена из-за новой авторизации",
		}:
		default:
		}
		delete(r.sessions, sessionID)
	}
}
