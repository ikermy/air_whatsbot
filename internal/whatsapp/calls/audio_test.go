package calls

import (
	"context"
	"encoding/binary"
	"io"
	"testing"
)

func TestPCM16At24kResamplesFrame(t *testing.T) {
	frame := make([]float32, 960)
	data := pcm16At24k(frame)
	if got, want := len(data), 1440*2; got != want {
		t.Fatalf("got %d bytes, want %d", got, want)
	}
	for i := 0; i < len(data); i += 2 {
		if value := int16(binary.LittleEndian.Uint16(data[i:])); value != 0 {
			t.Fatalf("sample %d is %d, want silence", i/2, value)
		}
	}
}

func TestPCM16At16kResamplesChunk(t *testing.T) {
	data := make([]byte, 2400*2)
	if got, want := len(pcm16At16k(data)), 1600; got != want {
		t.Fatalf("got %d samples, want %d", got, want)
	}
}

func TestRealtimeAudioSourceClose(t *testing.T) {
	source := newRealtimeAudioSource(context.Background())
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ReadFrame(); err != io.EOF {
		t.Fatalf("ReadFrame error = %v, want io.EOF", err)
	}
	if err := source.Push(make([]float32, 1)); err != io.EOF {
		t.Fatalf("Push error = %v, want io.EOF", err)
	}
}
