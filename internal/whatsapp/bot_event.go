package whatsapp

import (
	"air_whatsbot/internal/metrics"
	messagespkg "air_whatsbot/internal/whatsapp/messages"
	"time"

	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"
)

// triggerReauth инициирует повторную авторизацию асинхронно: resetSession
// выполняет Disconnect/cancel, поэтому его нельзя вызывать синхронно из
// обработчика событий whatsmeow.
func (b *Bot) triggerReauth(reason string) {
	logger.Warn("Требуется повторная авторизация (%s)", reason, b.userID)
	go func() {
		if err := runReauth(b.userID, b.end, b.db, b.resetSession); err != nil {
			logger.Error("Ошибка процесса повторной авторизации (%s): %v", reason, err, b.userID)
		}
	}()
}

// handleEvent обрабатывает события WhatsApp
func (b *Bot) handleEvent(evt any) {
	switch v := evt.(type) {

	case *events.Message:
		metrics.MessagesReceived.WithLabelValues(metrics.BotLabel(b.userID), messagespkg.MessageType(v)).Inc()
		if !b.textMessage && (v.Message.GetConversation() != "" || v.Message.GetExtendedTextMessage() != nil) {
			metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "text_disabled").Inc()
			logger.Debug("Текстовые сообщения отключены настройками токена", b.userID)
			return
		}
		//err := w.handleMessage(v)
		//if err != nil {
		//	logger.Error("Ошибка обработки сообщения: %v", err, w.UserId)
		//}

		// Проверяем на ошибки дешифрования ПЕРЕД обработкой
		if b.isDecryptionError(v) {
			metrics.DecryptErrors.WithLabelValues(metrics.BotLabel(b.userID), "decrypt_error").Inc()
			logger.Warn("Обнаружена ошибка дешифрования, пропускаем сообщение", b.userID)
			return
		}

		// требуется сброс сессии.
		msgContent := messagespkg.MessageText(v)

		if msgContent == "[Ошибка: mismatching MAC]" {
			metrics.DecryptErrors.WithLabelValues(metrics.BotLabel(b.userID), "mismatching_mac").Inc()
			b.triggerReauth("mismatching_mac")
			return
		}

		// Если сообщение не содержит ошибки, обрабатываем его
		err := b.handleMessage(v)
		if err != nil {
			metrics.MessagesProcessed.WithLabelValues(metrics.BotLabel(b.userID), "error").Inc()
			logger.Error("Ошибка обработки сообщения: %v", err, b.userID)
		} else {
			metrics.MessagesProcessed.WithLabelValues(metrics.BotLabel(b.userID), "success").Inc()
		}

	case *events.Connected:
		// Время успешного подключения для игнорирования сообщений из истории
		b.connectedAt = time.Now()
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "connected").Set(1)
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "disconnected").Set(0)
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "logged_out").Set(0)
		metrics.Reconnects.WithLabelValues(metrics.BotLabel(b.userID), "success").Inc()
	case *events.Disconnected:
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "connected").Set(0)
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "disconnected").Set(1)
		metrics.Reconnects.WithLabelValues(metrics.BotLabel(b.userID), "disconnected").Inc()
		logger.Warn("WhatsApp: соединение разорвано", b.userID)
		// Если отключение произошло и клиент не авторизован, можно инициировать повторную авторизацию.
		if !b.b.IsLoggedIn() {
			b.triggerReauth("disconnected_not_logged_in")
		}
	case *events.LoggedOut:
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "connected").Set(0)
		metrics.ActiveSessions.WithLabelValues(metrics.BotLabel(b.userID), "logged_out").Set(1)
		metrics.Reconnects.WithLabelValues(metrics.BotLabel(b.userID), "logged_out").Inc()
		logger.Warn("WhatsApp: выполнен выход с другого устройства", b.userID)
		b.triggerReauth("logged_out")

	case *events.OfflineSyncCompleted:
		b.offlineSyncOnce.Do(func() { close(b.offlineMsgSync) })
		logger.Debug("Завершена синхронизация оффлайн сообщений", b.userID)

	default:
		//logger.Debug("Неизвестное событие: %T", evt, w.UserId)
	}
}

// Добавьте проверку ошибок дешифрования
func (b *Bot) isDecryptionError(msg *events.Message) bool {
	// Проверяем различные признаки ошибок дешифрования
	if msg.Message == nil {
		return true
	}

	// Проверяем наличие ошибок в метаданных сообщения
	if msg.Info.Category == "error" || msg.Info.Category == "decrypt" {
		return true
	}

	return false
}

// handleMessage обрабатывает входящие сообщения
