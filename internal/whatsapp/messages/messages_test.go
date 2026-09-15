package messages

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestMessageText(t *testing.T) {
	cases := []struct {
		name string
		msg  *events.Message
		want string
	}{
		{"nil", nil, ""},
		{"text", &events.Message{Message: &waE2E.Message{Conversation: proto.String("hi")}}, "hi"},
		{"extended", &events.Message{Message: &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("ext")}}}, "ext"},
		{"voice", &events.Message{Message: &waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true)}}}, "[Голосовое сообщение]"},
		{"document", &events.Message{Message: &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{}}}, "[Документ]"},
		{"image", &events.Message{Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}}, "[Изображение]"},
		{"unsupported", &events.Message{Message: &waE2E.Message{VideoMessage: &waE2E.VideoMessage{}}}, "[Неподдерживаемый тип сообщения]"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MessageText(tc.msg); got != tc.want {
				t.Fatalf("MessageText() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExtractFilesFromMessage(t *testing.T) {
	msg := &events.Message{
		Info:    types.MessageInfo{ID: "abc"},
		Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg")}},
	}
	download := func(context.Context, whatsmeow.DownloadableMessage) ([]byte, error) {
		return []byte("data"), nil
	}

	files := ExtractFilesFromMessage(msg, context.Background(), download)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Name != "image_abc.jpg" {
		t.Errorf("unexpected file name %q", files[0].Name)
	}
	if files[0].MimeType != "image/jpeg" {
		t.Errorf("unexpected mime type %q", files[0].MimeType)
	}
}

func TestExtractFilesFromMessageDownloadError(t *testing.T) {
	msg := &events.Message{
		Info:    types.MessageInfo{ID: "abc"},
		Message: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{}},
	}
	download := func(context.Context, whatsmeow.DownloadableMessage) ([]byte, error) {
		return nil, errors.New("download failed")
	}

	if files := ExtractFilesFromMessage(msg, context.Background(), download); len(files) != 0 {
		t.Fatalf("expected no files on download error, got %d", len(files))
	}
}

func TestExtractFilesFromMessageNil(t *testing.T) {
	if files := ExtractFilesFromMessage(nil, context.Background(), nil); files != nil {
		t.Fatalf("expected nil for empty input, got %v", files)
	}
}
