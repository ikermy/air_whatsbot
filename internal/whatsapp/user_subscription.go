package whatsapp

import (
	"errors"
	"fmt"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// Получает MasterKey пользователя от Landing
func (u *User) userMasterKey(userID uint32) [32]byte {
	var (
		userMasterKey [32]byte
		err           error
	)

	if userMasterKey, err = u.getMasterKey(userID); err == nil {
		return userMasterKey
	}

	logger.Warn("Не удалось получить мастер-ключ пользователя: %v", err, userID)
	return [32]byte{}
}

// Проверяет подписку пользователя
func (u *User) checkUserSubscription(userId uint32) error {
	err := com.CheckUserSubscription(u.db, userId)
	if err != nil {
		var commonErr *com.SubscriptionError
		ok := errors.As(err, &commonErr)
		if ok {
			// Форматируем сообщение, включив код ошибки
			errorCode := fmt.Sprintf("%d", commonErr.Code)
			msg := com.CarpCh{
				Event:      "subscription",
				UserName:   "",
				AssistName: "",
				Target:     errorCode,
				UserID:     userId,
			}
			err := u.end.SendNotification(msg)
			if err != nil {
				return fmt.Errorf("ошибка отправки уведомления о подписке: %v", err)
			}

			// Выключаю все каналы пользователя
			if dbErr := u.db.DisableAllUserChannel(userId); dbErr != nil {
				return fmt.Errorf("ошибка при выключении каналов: %v", dbErr)
			}

			logger.Debug("Все каналы пользователя %d выключены из-за отсутствия подписки", userId)
		} else {
			return fmt.Errorf("неизвестная ошибка проверки подписки: %v", err)
		}
		return fmt.Errorf("ошибка проверки подписки: %v", err)
	}
	return nil
}
