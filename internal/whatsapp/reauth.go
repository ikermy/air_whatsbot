package whatsapp

import (
	"fmt"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

type reauthNotifier interface {
	SendNotification(msg com.CarpCh) error
}

type reauthChannelDisabler interface {
	SetChannelEnabled(userID uint32, channel string, enabled bool) error
}

// runReauth выполняет шаги повторной авторизации независимо: сбой одного шага
// (например, транспорта уведомлений) не должен мешать отключить канал и сбросить
// сессию, иначе бот останется в нерабочем состоянии.
func runReauth(userID uint32, notifier reauthNotifier, disabler reauthChannelDisabler, reset func() error) error {
	if userID == 0 {
		return fmt.Errorf("получен некорректный userId: %d", userID)
	}

	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if notifier != nil {
		msg := com.CarpCh{
			Event:  "reauth",
			Target: "WhatsApp User",
			UserID: userID,
		}
		if err := notifier.SendNotification(msg); err != nil {
			logger.Error("reauth: ошибка отправки уведомления для userId %d: %v", userID, err)
			record(fmt.Errorf("send reauth notification: %w", err))
		}
	}

	if disabler != nil {
		if err := disabler.SetChannelEnabled(userID, whatsAppChannelName, false); err != nil {
			logger.Error("reauth: ошибка отключения канала для userId %d: %v", userID, err)
			record(fmt.Errorf("disable WhatsApp channel: %w", err))
		} else {
			logger.Debug("reauth: канал WhatsApp отключён для userId %d", userID)
		}
	}

	if reset != nil {
		if err := reset(); err != nil {
			logger.Error("reauth: ошибка сброса сессии для userId %d: %v", userID, err)
			record(fmt.Errorf("reset session: %w", err))
		}
	}

	return firstErr
}
