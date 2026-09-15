package whatsapp

import (
	responder "air_whatsbot/internal/whatsapp/responder"
	"strconv"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow/types"
)

// restoreResponderFields восстанавливает JID и RealPhone у записи респондента,
// прогруженной из Redis с одним лишь флагом Known (preloadFirstInteraction).
// Без этого ответ уходит на пустой JID, а в CRM попадает LID вместо телефона.
// Возвращает true, если пустой RealPhone удалось заполнить.
func restoreResponderFields(info *responder.Info, senderJID types.JID, realPhone string, senderID uint64) bool {
	if info == nil {
		return false
	}
	if info.JID.User == "" {
		info.JID = senderJID
	}
	if info.RealPhone != "" {
		return false
	}
	if realPhone != "" {
		info.RealPhone = realPhone
	} else {
		info.RealPhone = strconv.FormatUint(senderID, 10)
	}
	return true
}

func (b *Bot) sendFirstContactMessage(senderID uint64, senderName string) {
	if info, ok := b.responders.Load(senderID); ok && info != nil && info.Known {
		if err := b.responders.SetKnown(b.ctx, b.userID, int64(senderID)); err != nil {
			logger.Warn("Redis: ошибка продления knownResponder для senderID=%d: %v", senderID, err, b.userID)
		}
		return
	}

	// Ищу пользователя в кеше редис
	exists, err := b.responders.Known(b.ctx, b.userID, int64(senderID))
	if err != nil {
		logger.Warn("Redis: ошибка проверки knownResponder для senderID=%d: %v", senderID, err, b.userID)
	}
	logger.Debug("Redis: проверка knownResponder для senderID=%d, exists=%v", senderID, exists, b.userID)
	isFirstInteraction := !exists

	// Отправляем уведомление о начале диалога, если требуется
	if isFirstInteraction && b.assist.Events.Start {
		notifyMsg := com.CarpCh{
			Event:      "start",
			UserName:   senderName,
			AssistName: b.assist.AssistName,
			Target:     "",
			UserID:     b.userID,
		}
		if err := b.end.SendNotification(notifyMsg); err != nil {
			logger.Error("Ошибка отправки уведомления о первом взаимодействии: %v", err, b.userID)
		}
	}

	// Сохраняю пользователя в редис после обработки
	if err := b.responders.SetKnown(b.ctx, b.userID, int64(senderID)); err != nil {
		logger.Warn("Redis: ошибка сохранения knownResponder для senderID=%d: %v", senderID, err, b.userID)
	}

	info, ok := b.responders.Load(senderID)
	if !ok || info == nil {
		b.responders.Store(senderID, &responder.Info{Known: true})
		return
	}

	info.Known = true
	b.responders.Store(senderID, info)
}

func (b *Bot) preloadFirstInteraction() {
	senderIDs, err := b.responders.LoadUser(b.ctx, b.userID)
	if err != nil {
		logger.Warn("Redis: не удалось прогреть knownResponder: %v", err, b.userID)
		return
	}

	for _, senderID := range senderIDs {
		b.responders.Store(uint64(senderID), &responder.Info{Known: true})
	}

	if len(senderIDs) > 0 {
		logger.Debug("Redis: прогрето knownResponder=%d", len(senderIDs), b.userID)
	}
}
