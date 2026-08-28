package whatsapp

import (
	"fmt"
	"time"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/crypto"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

const masterKeyCacheTTL = 5 * time.Minute

type cachedMasterKey struct {
	key       [32]byte
	expiresAt time.Time
}

func (u *User) getMasterKey(userID uint32) ([32]byte, error) {
	u.masterKeyMu.Lock()
	if cached, ok := u.masterKeys[userID]; ok && time.Now().Before(cached.expiresAt) {
		u.masterKeyMu.Unlock()
		return cached.key, nil
	}
	u.masterKeyMu.Unlock()

	if u.rpc == nil {
		return [32]byte{}, fmt.Errorf("пользователь %d: ORC клиент не инициализирован", userID)
	}
	key, err := u.rpc.GetUserMasterKey(u.ctx, userID)
	if err != nil {
		return [32]byte{}, err
	}
	u.masterKeyMu.Lock()
	u.masterKeys[userID] = cachedMasterKey{key: key, expiresAt: time.Now().Add(masterKeyCacheTTL)}
	u.masterKeyMu.Unlock()
	return key, nil
}

func (u *User) invalidateMasterKey(userID uint32) {
	u.masterKeyMu.Lock()
	delete(u.masterKeys, userID)
	u.masterKeyMu.Unlock()
}

func (u *User) decryptSessionData(userId uint32, raw string) (string, error) {
	if u.rpc == nil {
		return "", fmt.Errorf("пользователь %d: ORC клиент не инициализирован", userId)
	}

	mk, err := u.getMasterKey(userId)
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
		u.invalidateMasterKey(userId)
		logger.Error("Ошибка расшифрования токена: %v", userId, err)
		return "", fmt.Errorf("ошибка расшифрования токена: %w", err)
	}

	return decrypted, nil
}
