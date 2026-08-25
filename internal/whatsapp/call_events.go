package whatsapp

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const callEventReplayLimit = 128

type callEventHub struct {
	mu          sync.Mutex
	sequence    uint64
	replay      []CallEvent
	subscribers map[chan CallEvent]struct{}
	closed      bool
}

func newCallEventHub() *callEventHub {
	return &callEventHub{subscribers: make(map[chan CallEvent]struct{})}
}

func (h *callEventHub) publish(event CallEvent) {
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

func (h *callEventHub) subscribe(ctx context.Context, after uint64) (<-chan CallEvent, error) {
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

func (h *callEventHub) unsubscribe(ch chan CallEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subscribers[ch]; ok {
		delete(h.subscribers, ch)
		close(ch)
	}
}

func (h *callEventHub) close() {
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
