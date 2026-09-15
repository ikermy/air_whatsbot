package whatsapp

import (
	"os"

	"air_whatsbot/internal/whatsapp/devicestore"

	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
)

// Device возвращает хранилище устройства бота.
func (b *Bot) Device() *devicestore.Store { return b.container }

// PrepareReauth сбрасывает текущую авторизацию и пересоздаёт клиентов.
func (b *Bot) PrepareReauth() {
	b.stopCalls("reauthentication")

	// Отключаем текущий клиент и очищаем ID пользователя
	b.b.Disconnect()
	b.b.Store.ID = nil

	// Создаем новый клиент с очищенным Store
	b.b = whatsmeow.NewClient(b.container.GetDevice(), nil)
	meowLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().
		Str("component", "meowcaller").Logger()
	logger.Info("Инициализация meowcaller: ожидается MLow PCM 16 kHz mono; фактический negotiated codec будет записан библиотекой", b.userID)
	b.callClient = meowcaller.NewClient(b.b, meowcaller.WithLogger(meowLog))

	// Регистрируем обработчик событий для нового клиента
	b.b.AddEventHandler(b.handleEvent)
	b.registerCallHandlers()
}

// SetAuthSettings синхронизирует настройки каналов временного бота и хранилища.
func (b *Bot) SetAuthSettings(allowText, allowCall bool) {
	b.container.Text = allowText
	b.container.Call = allowCall
	b.textMessage = allowText
	b.voiceCall = allowCall
}
