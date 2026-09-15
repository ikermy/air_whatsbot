package messages

import (
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// ExtractRealPhone extracts the real phone from a JID while preserving a fallback.
func ExtractRealPhone(jidString string, fallback string) string {
	if jidString == "" {
		return fallback
	}

	jid, err := types.ParseJID(jidString)
	if err != nil {
		logger.Debug("Не удалось распарсить JID '%s', используем fallback: %s", jidString, fallback)
		return fallback
	}

	if jid.Server == "lid" && jid.User != "" {
		logger.Debug("Извлечен номер из LID: %s (JID: %s)", jid.User, jidString)
		return jid.User
	}

	if jid.Server == types.DefaultUserServer && jid.User != "" {
		logger.Debug("Извлечен номер из стандартного JID: %s", jid.User)
		return jid.User
	}

	if jid.User == "" {
		logger.Debug("User часть JID пустая, используем fallback: %s", fallback)
		return fallback
	}

	logger.Debug("Используем User часть JID: %s (сервер: %s)", jid.User, jid.Server)
	return jid.User
}

// MessageType determines the message type for metrics and logs.
func MessageType(msg *events.Message) string {
	if msg == nil || msg.Message == nil {
		return "unknown"
	}
	switch {
	case msg.Message.GetConversation() != "":
		return "text"
	case msg.Message.GetExtendedTextMessage() != nil:
		return "extended_text"
	case msg.Message.GetAudioMessage() != nil:
		return "audio"
	case msg.Message.GetImageMessage() != nil:
		return "image"
	case msg.Message.GetVideoMessage() != nil:
		return "video"
	case msg.Message.GetDocumentMessage() != nil:
		return "document"
	default:
		return "other"
	}
}
