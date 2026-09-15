package responder

import (
	"context"
	"sync"

	"go.mau.fi/whatsmeow/types"
)

// Info хранит информацию о респонденте (собеседнике).
type Info struct {
	JID       types.JID // Оригинальный JID для отправки сообщений (может быть @lid или @s.whatsapp.net)
	RealPhone string    // Реальный телефон из SenderAlt для CRM
	Known     bool      // Флаг первого взаимодействия
}

// Store — потокобезопасное хранилище респондентов с опциональным Redis-кешем.
type Store struct {
	items sync.Map // key: uint64 (senderID), value: *Info
	cache Cache
}

func NewStore(cache Cache) *Store {
	return &Store{cache: cache}
}

func (s *Store) Load(id uint64) (*Info, bool) {
	value, ok := s.items.Load(id)
	if !ok {
		return nil, false
	}
	info, ok := value.(*Info)
	return info, ok
}

func (s *Store) LoadOrStore(id uint64, info *Info) (*Info, bool) {
	value, loaded := s.items.LoadOrStore(id, info)
	got, _ := value.(*Info)
	return got, loaded
}

func (s *Store) Store(id uint64, info *Info) {
	s.items.Store(id, info)
}

// Count возвращает количество известных респондентов в памяти.
func (s *Store) Count() int {
	count := 0
	s.items.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// Known проверяет метку «уже взаимодействовал» в кеше. Без кеша всегда false.
func (s *Store) Known(ctx context.Context, userID uint32, senderID int64) (bool, error) {
	if s.cache == nil {
		return false, nil
	}
	return s.cache.Has(ctx, userID, senderID)
}

// SetKnown сохраняет метку «уже взаимодействовал» в кеше. Без кеша — no-op.
func (s *Store) SetKnown(ctx context.Context, userID uint32, senderID int64) error {
	if s.cache == nil {
		return nil
	}
	return s.cache.Set(ctx, userID, senderID)
}

// LoadUser возвращает известных респондентов пользователя из кеша.
func (s *Store) LoadUser(ctx context.Context, userID uint32) ([]int64, error) {
	if s.cache == nil {
		return nil, nil
	}
	return s.cache.LoadUser(ctx, userID)
}
