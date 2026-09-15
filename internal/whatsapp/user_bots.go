package whatsapp

import (
	"air_whatsbot/internal/domain"
	"air_whatsbot/internal/whatsapp/contacts"
	devicestore "air_whatsbot/internal/whatsapp/devicestore"
	"fmt"
	"time"

	"github.com/ikermy/air-common/pkg/crypto"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	_ "modernc.org/sqlite"
)

func (u *User) GetWaUserBotUsers() error {
	// Получаем данные пользователей
	userDetails, err := u.db.GetWaUserBotUsers(u.ctx)
	if err != nil {
		return fmt.Errorf("ошибка получения данных пользователей: %w", err)
	}
	// Если нет данных для обработки, выходим
	if len(userDetails) == 0 {
		return nil
	}

	// Обрабатываем состояние ботов
	u.processBotsState(userDetails)

	return nil
}

// processBotsState обрабатывает запуск ботов из списка пользователей
func (u *User) processBotsState(userDetails []domain.WaUserBotData) {
	// Обрабатываем пользователей и запускаем их ботов
	for _, user := range userDetails {
		// Пропускаем если нет данных сессии
		if user.SessionData == "" {
			logger.Warn("Пропуск пользователя - отсутствуют данные сессии", user.UserId)
			continue
		}

		// Проверяем подписку пользователя
		if err := u.checkUserSubscription(user.UserId); err != nil {
			logger.Info("Пропуск бота из-за ошибки подписки: %v", err, user.UserId)
			continue
		}

		// Если поле зашифровано MasterKey — расшифровываем
		if crypto.IsEncryptedWithMasterKey(user.SessionData) {
			sessionData, err := u.decryptSessionData(user.UserId, user.SessionData)
			if err != nil {
				logger.Error("Ошибка расшифровки данных сессии: %v", err, user.UserId)
				continue
			}
			// Обновляю данные сессии на расшифрованные
			user.SessionData = sessionData
		}

		// Парсим контейнер
		userBotContainer, err := devicestore.ParseUserBotContainer(u.ctx, user.UserId, user.SessionData, u.db)
		if err != nil {
			logger.Error("Ошибка разбора токена: %v", err, user.UserId)
			continue
		}

		// Создаем бота
		logger.Info("Создание бота для пользователя", user.UserId)
		assistModel := u.createAssistantModel(user)

		bot, err := u.createOrUpdateBot(user.UserId, userBotContainer, assistModel)
		if err != nil {
			logger.Error("Ошибка создания бота: %v", err, user.UserId)
			continue
		}

		u.uBot.Store(user.UserId, bot)
	}
}

func (u *User) GetBotUsername(userID uint32) string {
	value, ok := u.uBot.Load(userID)
	if !ok {
		return ""
	}
	bot := value.(*Bot)
	if bot.b == nil || bot.b.Store.ID == nil {
		return ""
	}

	return bot.b.Store.ID.String()
}

// BotState сообщает, существует ли бот для пользователя и остановлен ли он.
func (u *User) BotState(userID uint32) (found bool, stopped bool) {
	value, ok := u.uBot.Load(userID)
	if !ok {
		return false, false
	}
	bot, ok := value.(*Bot)
	if !ok || bot == nil {
		return false, false
	}
	return true, bot.stopped.Load()
}

// restartUserBot перезапускает бота для конкретного пользователя с перезагрузкой всех параметров из БД
func (u *User) restartUserBot(userId uint32) error {
	logger.Info("Получена команда на перезапуск бота...", userId)

	// Получаем данные конкретного пользователя из БД
	userData, err := u.db.GetWaUser(u.ctx, userId)
	if err != nil {
		logger.Error("Ошибка получения данных пользователя из БД: %v", err, userId)
		return fmt.Errorf("ошибка получения данных пользователя из БД: %w", err)
	}

	// Если поле зашифровано MasterKey — расшифровываем
	if crypto.IsEncryptedWithMasterKey(userData.SessionData) {
		sessionData, err := u.decryptSessionData(userData.UserId, userData.SessionData)
		if err != nil {
			return fmt.Errorf("ошибка расшифровки данных сессии: %v", err)
		}
		// Обновляю данные сессии на расшифрованные
		userData.SessionData = sessionData
	}

	// Проверяем, включен ли бот для этого пользователя
	if !userData.WaUserBotEnabled {
		logger.Warn("Бот отключен для пользователя, остановка...", userId)
		// Останавливаем бот, если он был запущен
		if _, exists := u.uBot.Load(userId); exists {
			if err := u.stopUserBot(userId); err != nil {
				logger.Error("Ошибка остановки отключенного бота: %v", err, userId)
				return fmt.Errorf("ошибка остановки отключенного бота: %w", err)
			}
		}
		return fmt.Errorf("бот отключен для пользователя %d", userId)
	}

	// Останавливаем текущий бот, если он запущен
	if _, exists := u.uBot.Load(userId); exists {
		logger.Debug("Остановка текущего бота перед перезапуском...", userId)
		if err := u.stopUserBot(userId); err != nil {
			logger.Error("Ошибка остановки бота: %v", err, userId)
			return fmt.Errorf("ошибка остановки бота: %w", err)
		}
		// Дополнительная пауза для полного завершения
		time.Sleep(500 * time.Millisecond)
	}

	// Проверяем подписку пользователя
	err = u.checkUserSubscription(userId)
	if err != nil {
		logger.Error("Ошибка проверки подписки: %v", err, userId)
		return fmt.Errorf("ошибка проверки подписки: %w", err)
	}

	// Парсим контейнер
	userBotContainer, err := devicestore.ParseUserBotContainer(u.ctx, userData.UserId, userData.SessionData, u.db)
	if err != nil {
		logger.Error("Ошибка разбора токена: %v", err, userId)
		return fmt.Errorf("ошибка разбора токена: %w", err)
	}

	// Создаем модель ассистента с обновленными параметрами из БД
	logger.Debug("Создание модели ассистента с обновленными параметрами...", userId)
	assistModel := u.createAssistantModel(*userData)

	// Создаем новый бот с обновленными параметрами
	logger.Debug("Создание нового бота с обновленными параметрами...", userId)
	bot, err := u.createOrUpdateBot(userData.UserId, userBotContainer, assistModel)
	if err != nil {
		logger.Error("Ошибка создания бота: %v", err, userId)
		return fmt.Errorf("ошибка создания бота: %w", err)
	}

	// Сохраняем бот в карту
	u.uBot.Store(userData.UserId, bot)

	logger.Info("Бот успешно перезапущен с обновленными параметрами из БД", userId)
	return nil
}

func (u *User) StopUserBot(userId uint32) error {
	return u.stopUserBot(userId)
}

// stopUserBot останавливает бота для конкретного пользователя и удаляет его из карты
func (u *User) stopUserBot(userId uint32) error {
	value, exists := u.uBot.Load(userId)
	if !exists {
		logger.Warn("Бот не найден", userId)
		return fmt.Errorf("бот для пользователя %d не найден", userId)
	}

	bot := value.(*Bot)
	if bot == nil {
		logger.Warn("Бот не инициализирован", userId)
		return fmt.Errorf("бот для пользователя %d не инициализирован", userId)
	}

	// Проверяем, не остановлен ли бот уже
	if bot.stopped.Load() {
		logger.Warn("Бот уже остановлен", userId)
		return nil
	}

	logger.Debug("Остановка бота...", userId)

	// Устанавливаем флаг остановки атомарно
	bot.stopped.Store(true)
	bot.stopCalls("user_removed")

	// Отменяем контекст
	if bot.cancel != nil {
		bot.cancel()
		logger.Debug("Контекст отменен", userId)
	}

	// Отключаем WhatsApp клиент
	if bot.b != nil {
		bot.b.Disconnect()
		logger.Debug("WhatsApp клиент отключен", userId)
	}

	// Ждем некоторое время для корректного завершения
	time.Sleep(500 * time.Millisecond)

	// Удаляем бота из карты
	u.uBot.Delete(userId)

	logger.Debug("Бот остановлен", userId)
	return nil
}

func (u *User) RestartUserBot(userId uint32) error {
	return u.restartUserBot(userId)
}

// StartUserBot запускает бота для конкретного пользователя
func (u *User) StartUserBot(userId uint32) error {
	logger.Info("Получена команда на запуск бота...", userId)

	// Проверяем, запущен ли уже бот для этого пользователя
	if _, exists := u.uBot.Load(userId); exists {
		return fmt.Errorf("бот для пользователя %d уже запущен", userId)
	}

	// Получаем данные конкретного пользователя из БД
	userData, err := u.db.GetWaUser(u.ctx, userId)
	if err != nil {
		return fmt.Errorf("ошибка получения данных пользователя из БД: %w", err)
	}

	// Проверяем, включен ли бот для этого пользователя
	if !userData.WaUserBotEnabled {
		return fmt.Errorf("бот отключен для пользователя %d в настройках БД", userId)
	}

	// Проверяем подписку пользователя
	if err = u.checkUserSubscription(userId); err != nil {
		return fmt.Errorf("ошибка проверки подписки: %w", err)
	}

	// Если поле зашифровано MasterKey — расшифровываем
	if crypto.IsEncryptedWithMasterKey(userData.SessionData) {
		sessionData, err := u.decryptSessionData(userData.UserId, userData.SessionData)
		if err != nil {
			return fmt.Errorf("ошибка расшифровки данных сессии: %v", err)
		}
		// Обновляю данные сессии на расшифрованные
		userData.SessionData = sessionData
	}

	// Парсим контейнер
	userBotContainer, err := devicestore.ParseUserBotContainer(u.ctx, userData.UserId, userData.SessionData, u.db)
	if err != nil {
		return fmt.Errorf("ошибка разбора токена: %w", err)
	}

	// Создаем модель ассистента
	logger.Debug("Создание модели ассистента...", userId)
	assistModel := u.createAssistantModel(*userData)

	// Создаем новый бот
	logger.Debug("Создание нового бота...", userId)
	bot, err := u.initializeBot(userData.UserId, userBotContainer, assistModel, true)
	if err != nil {
		logger.Error("Ошибка создания бота: %v", err, userId)
		return fmt.Errorf("ошибка создания бота: %w", err)
	}

	// Сохраняем бот в карту
	u.uBot.Store(userData.UserId, bot)

	logger.Info("Бот успешно запущен для пользователя", userId)
	return nil
}

// Создает модель ассистента
func (u *User) createAssistantModel(user domain.WaUserBotData) *model.Assistant {
	return &model.Assistant{
		UserID:     user.UserId,
		AssistName: user.AssistName,
		AssistId:   user.AssistantId,
		Provider:   user.Provider,
		Espero:     user.Espero,
		Limit:      user.AskLimit,
		Ignore:     user.Ignore,
		Events: model.Notifications{
			Start:  user.Events.Start,
			End:    user.Events.End,
			Target: user.Events.Target,
		},
		Metas: model.Target{
			MetaAction: user.MetaAction,
			Triggers:   user.Triggers,
		},
	}
}

// GetUserContactsStreaming отправляет контакты и группы пользователя через канал в потоковом режиме.
func (u *User) GetUserContactsStreaming(userID uint32, dataChan chan<- any) error {
	value, exists := u.uBot.Load(userID)
	if !exists {
		return fmt.Errorf("WhatsApp: пользователь %d: бот не найден", userID)
	}
	bot, ok := value.(*Bot)
	if !ok || bot == nil {
		return fmt.Errorf("WhatsApp: пользователь %d: бот не инициализирован", userID)
	}
	return contacts.Stream(u.ctx, bot.b, userID, dataChan)
}
