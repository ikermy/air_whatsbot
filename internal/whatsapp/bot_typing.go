package whatsapp

import (
	"context"
	"strconv"
	"time"

	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow/types"
	_ "modernc.org/sqlite"
)

func (b *Bot) setTyping(senderID uint64) {
	// Создаем отменяемый контекст
	ctx, cancel := context.WithCancel(b.ctx)

	// Сохраняем функцию отмены
	b.typingCancels.Store(senderID, cancel)

	// пауза в пару секунд
	time.Sleep(2 * time.Second)

	// Получаем оригинальный JID пользователя из информации о респонденте
	var jid types.JID
	if respInfo, exists := b.responders.Load(senderID); exists {
		jid = respInfo.JID
	} else {
		// Если информация не найдена, создаем стандартный JID
		jid = types.NewJID(strconv.FormatUint(senderID, 10), types.DefaultUserServer)
		logger.Warn("Информация о респонденте не найдена для setTyping %d, используем стандартный JID", senderID, b.userID)
	}

	// Устанавливаем статус "печатает"
	if err := b.b.SendChatPresence(b.ctx, jid, types.ChatPresenceComposing, ""); err != nil {
		logger.Error("Ошибка установки статуса печатает: %v", err, b.userID)
		return
	}

	// Ждем завершения или таймаута
	select {
	case <-ctx.Done():
		// Контекст отменен - убираем статус печатает
		_ = b.b.SendChatPresence(b.ctx, jid, types.ChatPresencePaused, "")
		logger.Debug("Статус 'печатает' отменен для %d", senderID, b.userID)
	case <-time.After(30 * time.Second):
		// Таймаут - убираем статус печатает
		_ = b.b.SendChatPresence(b.ctx, jid, types.ChatPresencePaused, "")
		logger.Debug("Статус 'печатает' завершен по таймауту для %d", senderID, b.userID)
	}
}

func (b *Bot) getAndRemoveTypingCancel(senderID uint64) context.CancelFunc {
	value, loaded := b.typingCancels.LoadAndDelete(senderID)
	if !loaded {
		return nil
	}
	cancel, _ := value.(context.CancelFunc)
	return cancel
}

// processMessage обрабатывает входящее сообщение от пользователя
