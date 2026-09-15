package whatsapp

import (
	"context"
	"fmt"

	"github.com/purpshell/meowcaller"
)

// InitiateOutgoingCall запускает звонок через бота пользователя.
func (u *User) InitiateOutgoingCall(ctx context.Context, userID uint32, target string, handlers ...CallEventHandler) (*meowcaller.Call, error) {
	bot, err := u.botForUser(userID)
	if err != nil {
		return nil, err
	}
	return bot.InitiateOutgoingCall(ctx, target, handlers...)
}

// HangupCall завершает звонок пользователя через фасад User.
func (u *User) HangupCall(userID uint32, callID string) error {
	bot, err := u.botForUser(userID)
	if err != nil {
		return err
	}
	return bot.HangupCall(callID)
}

// ActiveCallIDs возвращает активные звонки указанного пользователя.
func (u *User) ActiveCallIDs(userID uint32) ([]string, error) {
	bot, err := u.botForUser(userID)
	if err != nil {
		return nil, err
	}
	return bot.ActiveCallIDs(), nil
}

// SubscribeCallEvents подписывает клиента на события звонка пользователя.
func (u *User) SubscribeCallEvents(ctx context.Context, userID uint32, callID string, afterSequence uint64) (<-chan CallEvent, error) {
	bot, err := u.botForUser(userID)
	if err != nil {
		return nil, err
	}
	return bot.SubscribeCallEvents(ctx, callID, afterSequence)
}

func (u *User) botForUser(userID uint32) (*Bot, error) {
	value, ok := u.uBot.Load(userID)
	if !ok {
		return nil, fmt.Errorf("WhatsApp-бот пользователя %d не найден", userID)
	}
	bot, ok := value.(*Bot)
	if !ok || bot == nil {
		return nil, fmt.Errorf("некорректный WhatsApp-бот пользователя %d", userID)
	}
	return bot, nil
}
