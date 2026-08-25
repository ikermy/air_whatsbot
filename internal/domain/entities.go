package domain

import "github.com/ikermy/air-common/pkg/comdom"

// Notifications события уведомлений.
type Notifications struct {
	Start  bool
	End    bool
	Target bool
}

// WaUserBotData представляет данные пользователя с WaUserBot.
type WaUserBotData struct {
	Triggers         []string            // Список триггеров из модели ассистента
	SessionData      string              // Данные сессии
	AssistName       string              // Имя ассистента
	AssistantId      string              // Идентификатор ассистента
	MetaAction       string              // Поле MetaAction из модели ассистента
	UserId           uint32              // Идентификатор пользователя
	AskLimit         uint32              // Лимит запросов
	Events           Notifications       // При каких событиях присылать уведомления
	Provider         comdom.ProviderType // Провайдер модели
	Espero           uint8               // Значение Espero
	WaUserBotEnabled bool                // Флаг включения бота
	Ignore           bool                // Игнорировать сообщения до ответа ассистента
}
