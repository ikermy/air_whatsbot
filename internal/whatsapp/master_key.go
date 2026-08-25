package whatsapp

import (
	"fmt"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/crypto"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

func (u *User) decryptSessionData(userId uint32, raw string) (string, error) {
	if u.rpc == nil {
		return "", fmt.Errorf("пользователь %d: ORC клиент не инициализирован", userId)
	}

	mk, err := u.rpc.GetUserMasterKey(u.ctx, userId)
	if err != nil {
		logger.Error("Ошибка получения MasterKey: %v (требуется вход на Landing)", err, userId)

		notifyMsg := com.CarpCh{
			Event:  "reauth-userkey",
			UserID: userId,
		}
		if err := u.end.SendNotification(notifyMsg); err != nil {
			logger.Error("Ошибка отправки уведомления о повторной аутентификации %v", err, userId)
		}

		return "", fmt.Errorf("ошибка получения MasterKey для пользователя %d: %w", userId, err)
	}

	decrypted, err := crypto.DecryptFieldWithMasterKey(mk, raw)
	if err != nil {
		logger.Error("Ошибка расшифрования токена: %v", userId, err)
		return "", fmt.Errorf("ошибка расшифрования токена: %w", err)
	}

	return decrypted, nil
}
