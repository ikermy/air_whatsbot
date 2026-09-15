package responder

import (
	"context"
	"errors"
	"testing"
)

type fakeCache struct {
	has    bool
	setIDs []int64
	ids    []int64
	err    error
}

func (f *fakeCache) Has(context.Context, uint32, int64) (bool, error) { return f.has, f.err }
func (f *fakeCache) Set(_ context.Context, _ uint32, senderID int64) error {
	f.setIDs = append(f.setIDs, senderID)
	return f.err
}
func (f *fakeCache) LoadUser(context.Context, uint32) ([]int64, error) { return f.ids, f.err }

func TestStoreLoadOrStoreAndCount(t *testing.T) {
	s := NewStore(nil)

	info, loaded := s.LoadOrStore(7, &Info{Known: true})
	if loaded {
		t.Fatal("first LoadOrStore must report not loaded")
	}
	if info == nil || !info.Known {
		t.Fatal("unexpected stored info")
	}

	again, loaded := s.LoadOrStore(7, &Info{Known: false})
	if !loaded {
		t.Fatal("second LoadOrStore must report loaded")
	}
	if again != info {
		t.Fatal("LoadOrStore must return the existing instance")
	}
	if s.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", s.Count())
	}
}

func TestStoreKnownWithoutCache(t *testing.T) {
	s := NewStore(nil)

	if known, err := s.Known(context.Background(), 1, 2); err != nil || known {
		t.Fatalf("Known() = (%v, %v), want (false, nil)", known, err)
	}
	if err := s.SetKnown(context.Background(), 1, 2); err != nil {
		t.Fatalf("SetKnown() without cache must be a no-op, got %v", err)
	}
	if ids, err := s.LoadUser(context.Background(), 1); err != nil || ids != nil {
		t.Fatalf("LoadUser() without cache = (%v, %v), want (nil, nil)", ids, err)
	}
}

func TestStoreDelegatesToCache(t *testing.T) {
	cache := &fakeCache{has: true, ids: []int64{11, 22}}
	s := NewStore(cache)

	if known, err := s.Known(context.Background(), 1, 2); err != nil || !known {
		t.Fatalf("Known() = (%v, %v), want (true, nil)", known, err)
	}
	if err := s.SetKnown(context.Background(), 1, 42); err != nil {
		t.Fatalf("SetKnown() error: %v", err)
	}
	if len(cache.setIDs) != 1 || cache.setIDs[0] != 42 {
		t.Fatalf("SetKnown did not reach cache: %v", cache.setIDs)
	}
	ids, err := s.LoadUser(context.Background(), 1)
	if err != nil || len(ids) != 2 {
		t.Fatalf("LoadUser() = (%v, %v), want two ids", ids, err)
	}
}

func TestStorePropagatesCacheError(t *testing.T) {
	s := NewStore(&fakeCache{err: errors.New("redis down")})
	if _, err := s.Known(context.Background(), 1, 2); err == nil {
		t.Fatal("expected cache error to propagate")
	}
}
