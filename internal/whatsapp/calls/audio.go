package calls

import (
	"context"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"

	"github.com/purpshell/meowcaller"
)

func amplifyPCM16(data []byte, gain float64) []byte {
	if len(data) < 2 || gain == 1 {
		return data
	}
	out := make([]byte, len(data))
	for i := 0; i+1 < len(data); i += 2 {
		v := float64(int16(binary.LittleEndian.Uint16(data[i:]))) * gain
		if v > 32767 {
			v = 32767
		} else if v < -32768 {
			v = -32768
		}
		binary.LittleEndian.PutUint16(out[i:], uint16(int16(v)))
	}
	return out
}

// pcm16At24k converts meowcaller PCM float samples (16 kHz) to PCM16 at 24 kHz.
func pcm16At24k(frame []float32) []byte {
	if len(frame) == 0 {
		return nil
	}
	out := make([]byte, len(frame)*3)
	for i := 0; i < len(frame)*3/2; i++ {
		pos := float64(i) * 2 / 3
		left := int(pos)
		frac := float32(pos - float64(left))
		v := frame[left]
		if left+1 < len(frame) {
			v += (frame[left+1] - v) * frac
		}
		if v > 1 {
			v = 1
		}
		if v < -1 {
			v = -1
		}
		binary.LittleEndian.PutUint16(out[i*2:], uint16(int16(v*32767)))
	}
	return out
}

// pcm16At16k converts Realtime PCM16 (24 kHz) to meowcaller float samples (16 kHz).
func pcm16At16k(data []byte) []float32 {
	n := len(data) / 2
	if n == 0 {
		return nil
	}
	out := make([]float32, n*2/3)
	for i := range out {
		pos := float64(i) * 3 / 2
		left := int(pos)
		frac := float32(pos - float64(left))
		v := float32(int16(binary.LittleEndian.Uint16(data[left*2:]))) / 32768
		if left+1 < n {
			next := float32(int16(binary.LittleEndian.Uint16(data[(left+1)*2:]))) / 32768
			v += (next - v) * frac
		}
		out[i] = v
	}
	return out
}

type realtimeAudioSource struct {
	ctx    context.Context
	frames chan []float32
	done   chan struct{}
	once   sync.Once
	closed atomic.Bool
}

func newRealtimeAudioSource(ctx context.Context) *realtimeAudioSource {
	// Keep enough jitter tolerance for the 60 ms meowcaller media tick. The
	// producer uses a blocking Push, so shrinking this queue can stall the
	// media path and indirectly stop inbound audio delivery.
	return &realtimeAudioSource{ctx: ctx, frames: make(chan []float32, 8), done: make(chan struct{})}
}

func (s *realtimeAudioSource) ReadFrame() ([]float32, error) {
	select {
	case <-s.ctx.Done():
		return nil, io.EOF
	case <-s.done:
		return nil, io.EOF
	case f := <-s.frames:
		return f, nil
	default:
		// meowcaller должен получать кадр на каждом media tick.
		// Не блокируем его send loop в ожидании ответа realtime.
		return make([]float32, meowcaller.FrameSamples), nil
	}
}

func (s *realtimeAudioSource) Close() error {
	s.once.Do(func() { close(s.done) })
	s.closed.Store(true)
	return nil
}

func (s *realtimeAudioSource) Push(frame []float32) error {
	if s.closed.Load() {
		return io.EOF
	}
	select {
	case <-s.ctx.Done():
		return io.EOF
	case <-s.done:
		return io.EOF
	case s.frames <- frame:
		if s.closed.Load() {
			return io.EOF
		}
		return nil
	}
}

func (s *realtimeAudioSource) TryPush(frame []float32) bool {
	if s.closed.Load() {
		return false
	}
	select {
	case <-s.ctx.Done():
		return false
	case <-s.done:
		return false
	case s.frames <- frame:
		return true
	default:
		return false
	}
}
