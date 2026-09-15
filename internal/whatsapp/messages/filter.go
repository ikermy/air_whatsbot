package messages

import (
	"fmt"
	"strings"
	"time"

	"air_whatsbot/internal/metrics"

	"go.mau.fi/whatsmeow/types/events"
)

// ShouldIgnoreMessage centralizes the pre-checks that classify incoming WhatsApp messages as noise/history.
// It returns the ignore decision together with the matched rule so callers can log the cause.
func ShouldIgnoreMessage(msg *events.Message, connectedAt time.Time, botLabel string) (bool, string) {
	if msg == nil || msg.Message == nil {
		return true, "nil_message"
	}
	if time.Since(connectedAt) < 30*time.Second {
		metrics.MessagesIgnored.WithLabelValues(botLabel, "message_ignore_window").Inc()
		return true, "message_ignore_window"
	}
	if msg.Info.Sender.Server == "broadcast" || msg.Info.Chat.User == "status" {
		metrics.MessagesIgnored.WithLabelValues(botLabel, "status_broadcast").Inc()
		return true, "status_broadcast"
	}
	if msg.Info.Chat.String() == "status@broadcast" || msg.Info.Sender.String() == "status@broadcast" {
		metrics.MessagesIgnored.WithLabelValues(botLabel, "status_broadcast").Inc()
		return true, "status_broadcast"
	}
	if msg.Info.MessageSource.IsFromMe || msg.Info.IsIncomingBroadcast() {
		metrics.MessagesIgnored.WithLabelValues(botLabel, "from_me_or_broadcast").Inc()
		return true, "from_me_or_broadcast"
	}
	if msg.Info.Category == "peer" || msg.Info.Category == "retry" {
		metrics.MessagesIgnored.WithLabelValues(botLabel, "peer_or_retry").Inc()
		return true, "peer_or_retry"
	}
	if strings.Contains(msg.Info.ID, "BAE") || strings.Contains(msg.Info.ID, "_BAE") {
		metrics.MessagesIgnored.WithLabelValues(botLabel, "history_id").Inc()
		return true, "history_id"
	}
	if ext := msg.Message.GetExtendedTextMessage(); ext != nil && ext.ContextInfo != nil {
		msgStr := fmt.Sprintf("%+v", ext.ContextInfo)
		if strings.Contains(msgStr, "deviceListMetadata") || strings.Contains(msgStr, "messageSecret") || strings.Contains(msgStr, "recipientTimestamp") {
			metrics.MessagesIgnored.WithLabelValues(botLabel, "context_info").Inc()
			return true, "context_info"
		}
	}
	if msg.Info.Timestamp.Before(time.Now().Add(-5 * time.Minute)) {
		metrics.MessagesIgnored.WithLabelValues(botLabel, "too_old").Inc()
		return true, "too_old"
	}
	return false, ""
}
