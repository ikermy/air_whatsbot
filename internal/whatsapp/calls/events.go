package calls

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ikermy/air-common/pkg/model"
)

// ErrCallNotFound возвращается при подписке на неизвестный звонок.
var ErrCallNotFound = errors.New("call not found")

const callEventReplayLimit = 128

// CallEvent описывает событие жизненного цикла/медиа WhatsApp-звонка.
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

type eventHub struct {
	mu          sync.Mutex
	sequence    uint64
	replay      []CallEvent
	subscribers map[chan CallEvent]struct{}
	closed      bool
}

func newEventHub() *eventHub {
	return &eventHub{subscribers: make(map[chan CallEvent]struct{})}
}

func (h *eventHub) publish(event CallEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.sequence++
	event.Sequence = h.sequence
	event.Timestamp = time.Now()
	h.replay = append(h.replay, event)
	if len(h.replay) > callEventReplayLimit {
		h.replay = h.replay[len(h.replay)-callEventReplayLimit:]
	}
	for subscriber := range h.subscribers {
		select {
		case subscriber <- event:
		default:
		}
	}
}

func (h *eventHub) subscribe(ctx context.Context, after uint64) (<-chan CallEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, fmt.Errorf("call event stream is closed")
	}
	ch := make(chan CallEvent, callEventReplayLimit)
	for _, event := range h.replay {
		if event.Sequence > after {
			ch <- event
		}
	}
	h.subscribers[ch] = struct{}{}
	go func() {
		<-ctx.Done()
		h.unsubscribe(ch)
	}()
	return ch, nil
}

func (h *eventHub) unsubscribe(ch chan CallEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subscribers[ch]; ok {
		delete(h.subscribers, ch)
		close(ch)
	}
}

func (h *eventHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for subscriber := range h.subscribers {
		close(subscriber)
	}
	h.subscribers = make(map[chan CallEvent]struct{})
}
