package messages

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

// MessageContent stores the incoming content to be processed by the bot.
type MessageContent struct {
	Text  string
	Voice bool
	First bool
}

// DownloadFunc describes the adapter used to fetch media for a message.
type DownloadFunc func(context.Context, whatsmeow.DownloadableMessage) ([]byte, error)

// ExtractFilesFromMessage extracts uploadable files from a WhatsApp message.
// This keeps media extraction logic out of the monolithic whatsapp.go.
func ExtractFilesFromMessage(msg *events.Message, ctx context.Context, download DownloadFunc) []model.FileUpload {
	if msg == nil || msg.Message == nil || download == nil {
		return nil
	}

	var files []model.FileUpload

	switch {
	case msg.Message.GetImageMessage() != nil:
		imageMsg := msg.Message.GetImageMessage()
		if mediaData, err := download(ctx, imageMsg); err == nil {
			files = append(files, model.FileUpload{
				Name:     fmt.Sprintf("image_%s.jpg", msg.Info.ID),
				Content:  bytes.NewReader(mediaData),
				MimeType: imageMsg.GetMimetype(),
			})
		} else {
			logger.Error("Ошибка скачивания изображения: %v", err)
		}
	case msg.Message.GetDocumentMessage() != nil:
		docMsg := msg.Message.GetDocumentMessage()
		if mediaData, err := download(ctx, docMsg); err == nil {
			fileName := docMsg.GetFileName()
			if fileName == "" {
				fileName = fmt.Sprintf("document_%s", msg.Info.ID)
			}
			files = append(files, model.FileUpload{
				Name:     fileName,
				Content:  bytes.NewReader(mediaData),
				MimeType: docMsg.GetMimetype(),
			})
		} else {
			logger.Error("Ошибка скачивания документа: %v", err)
		}
	case msg.Message.GetVideoMessage() != nil:
		videoMsg := msg.Message.GetVideoMessage()
		if mediaData, err := download(ctx, videoMsg); err == nil {
			files = append(files, model.FileUpload{
				Name:     fmt.Sprintf("video_%s.mp4", msg.Info.ID),
				Content:  bytes.NewReader(mediaData),
				MimeType: videoMsg.GetMimetype(),
			})
		} else {
			logger.Error("Ошибка скачивания видео: %v", err)
		}
	case msg.Message.GetAudioMessage() != nil && !msg.Message.GetAudioMessage().GetPTT():
		audioMsg := msg.Message.GetAudioMessage()
		if mediaData, err := download(ctx, audioMsg); err == nil {
			files = append(files, model.FileUpload{
				Name:     fmt.Sprintf("audio_%s.ogg", msg.Info.ID),
				Content:  bytes.NewReader(mediaData),
				MimeType: audioMsg.GetMimetype(),
			})
		} else {
			logger.Error("Ошибка скачивания аудио: %v", err)
		}
	}

	return files
}

// MessageText returns the text payload from a WhatsApp message, or an empty string when unsupported.
func MessageText(msg *events.Message) string {
	if msg == nil || msg.Message == nil {
		return ""
	}
	switch {
	case msg.Message.GetConversation() != "":
		return msg.Message.GetConversation()
	case msg.Message.GetExtendedTextMessage() != nil:
		return msg.Message.GetExtendedTextMessage().GetText()
	case msg.Message.GetAudioMessage() != nil && msg.Message.GetAudioMessage().GetPTT():
		return "[Голосовое сообщение]"
	case msg.Message.GetDocumentMessage() != nil:
		return "[Документ]"
	case msg.Message.GetImageMessage() != nil:
		return "[Изображение]"
	default:
		return "[Неподдерживаемый тип сообщения]"
	}
}

// SenderID returns the numeric sender ID from the upstream event.
func SenderID(msg *events.Message) (uint64, error) {
	if msg == nil {
		return 0, fmt.Errorf("message is nil")
	}
	return strconv.ParseUint(msg.Info.Sender.User, 10, 64)
}
