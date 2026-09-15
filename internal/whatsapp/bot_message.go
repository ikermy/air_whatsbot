package whatsapp

import (
	responder "air_whatsbot/internal/whatsapp/responder"

	"air_whatsbot/internal/metrics"
	messagespkg "air_whatsbot/internal/whatsapp/messages"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	_ "modernc.org/sqlite"
)

func (b *Bot) handleMessage(msg *events.Message) error {
	startedAt := time.Now()
	defer metrics.ObserveDuration(metrics.MessageProcessingDuration.WithLabelValues(metrics.BotLabel(b.userID), "handle_message"), startedAt)

	<-b.offlineMsgSync // Ждем завершения синхронизации оффлайн сообщений

	if ignore, reason := messagespkg.ShouldIgnoreMessage(msg, b.connectedAt, metrics.BotLabel(b.userID)); ignore {
		logger.Debug("Игнорируем сообщение по фильтру сессии от %s: %s", msg.Info.Sender.String(), reason, b.userID)
		return nil
	}

	//// Преобразуем ID отправителя в число
	//senderID, err := strconv.ParseUint(msg.Info.Sender.User, 10, 64)
	//if err != nil {
	//	return fmt.Errorf("WhatsApp: ошибка преобразования ID пользователя %s: %v", msg.Info.Sender.User, err)
	//}

	// ИСПРАВЛЕНИЕ: Берем именно Sender (реальный собеседник), а не MessageSource.Chat
	senderJID := msg.Info.Sender
	senderName := msg.Info.PushName

	//logger.Infoln("senderJID.User", senderJID.User)

	senderID, err := strconv.ParseUint(senderJID.User, 10, 64)
	if err != nil {
		return fmt.Errorf("WhatsApp: ошибка преобразования ID пользователя %s: %v", senderJID.User, err)
	}

	// Проверяем список разрешенных пользователей
	if len(b.uids) > 0 {
		allowed := false
		for _, uid := range b.uids {
			if uid == int64(senderID) {
				allowed = true
				break
			}
		}

		if !allowed {
			metrics.MessagesIgnored.WithLabelValues(metrics.BotLabel(b.userID), "not_allowed_user").Inc()
			logger.Debug("Игнорирование сообщения от %d (нет в списке разрешенных)", senderID, b.userID)
			return nil
		}
	}

	content := messagespkg.MessageText(msg)

	if content == "[Неподдерживаемый тип сообщения]" {
		return nil
	}

	// Получаем реальный номер телефона из разных источников
	var realPhone string

	// Сначала пробуем SenderAlt (для некоторых контактов работает)
	if msg.Info.MessageSource.SenderAlt.User != "" {
		realPhone = messagespkg.ExtractRealPhone(msg.Info.MessageSource.SenderAlt.String(), "")
	}

	// Если не получилось, используем основной Sender JID
	if realPhone == "" {
		realPhone = messagespkg.ExtractRealPhone(msg.Info.Sender.String(), strconv.FormatUint(senderID, 10))
	}

	// Если всё ещё пусто, используем senderID как fallback
	if realPhone == "" {
		realPhone = strconv.FormatUint(senderID, 10)
		logger.Warn("Не удалось извлечь реальный номер, используем senderID: %s", realPhone, b.userID)
	}

	logger.Debug("Создана информация о респонденте: JID=%s, RealPhone=%s, SenderID=%d",
		msg.Info.Sender.String(), realPhone, senderID, b.userID)

	// Получаем или создаем информацию о респонденте
	respInfo, loaded := b.responders.LoadOrStore(senderID, &responder.Info{
		JID:       types.NewJID(msg.Info.Sender.User, msg.Info.Sender.Server),
		RealPhone: realPhone,
	})

	// Прогретые из Redis респонденты (preloadFirstInteraction) хранят только
	// флаг Known. Восстанавливаем JID для отправки и реальный телефон из
	// текущего сообщения, иначе ответ уйдёт на пустой JID, а в CRM — LID.
	if restoredPhone := restoreResponderFields(
		respInfo,
		types.NewJID(msg.Info.Sender.User, msg.Info.Sender.Server),
		realPhone,
		senderID,
	); restoredPhone {
		logger.Debug("Восстановлен реальный телефон респондента: %s (senderID: %d)", respInfo.RealPhone, senderID, b.userID)
	}

	if !loaded {
		// Новый респондент - логируем создание
		logger.Debug("Создана информация о респонденте: JID=%s, RealPhone=%s, SenderID=%d",
			respInfo.JID.String(), respInfo.RealPhone, senderID, b.userID)
		metrics.TrackActiveDialogs(b.userID, b.responders.Count())
	}

	// Проверяем, является ли это первым взаимодействием пользователя
	var first bool
	if !respInfo.Known {
		logger.Debug("Пользователь %s впервые взаимодействует с ботом", msg.Info.Sender.User, b.userID)

		// Отправляю уведомление о первом взаимодействии
		go b.sendFirstContactMessage(senderID, senderName)

		// Инициализируем каналы связи и модель
		if err = b.initializeUserChannels(senderID, senderName); err != nil {
			// Проверяем, является ли это сообщением о пустых данных
			if strings.Contains(err.Error(), "получены пустые данные") {
				// Это нормальная ситуация для нового диалога - не считаем ошибкой
				logger.Debug("Инициализация нового диалога для пользователя %s (ID: %d)", senderName, senderID, b.userID)
			} else {
				return fmt.Errorf("ошибка инициализации каналов пользователя %s: %w", senderName, err)
			}
		}

		// Помечаем пользователя как известного
		respInfo.Known = true
		b.responders.Store(senderID, respInfo)
		first = true // Отмечаем как первое взаимодействие для CRM
	}

	switch content {
	case "[Голосовое сообщение]":
		// Если разрешены голосовые сообщения
		if mode.IsAudioModeEnabled() {
			return b.processVoiceMessage(first, msg)
		}

		return fmt.Errorf("WhatsApp: для пользователя %d отключены голосовые сообщения", b.userID)
	default:
		message := messagespkg.MessageContent{
			Text:  content,
			Voice: false,
			First: first,
		}
		return b.processMessage(message, msg)
	}
}

// processVoiceMessage обрабатывает голосовое сообщение
func (b *Bot) processVoiceMessage(first bool, msg *events.Message) error {
	audioMsg := msg.Message.GetAudioMessage()
	if audioMsg == nil {
		logger.Warn("Нет AudioMessage в сообщении", b.userID)
		return nil
	}

	// Получаем медиа (сразу как []byte)
	mediaData, err := b.b.Download(b.ctx, audioMsg)
	if err != nil {
		logger.Error("Ошибка скачивания аудиофайла: %v", err, b.userID)
		return nil
	}

	text, err := b.mod.TranscribeAudio(b.userID, mediaData, "voice.ogg")
	if err != nil {
		logger.Error("Ошибка транскрибирования аудио: %v", err, b.userID)
		return nil
	}

	message := messagespkg.MessageContent{
		Text:  text,
		Voice: true,
		First: first,
	}

	return b.processMessage(message, msg)
}

// extractFilesFromMessage извлекает файлы из WhatsApp сообщения

func (b *Bot) processMessage(message messagespkg.MessageContent, msg *events.Message) error {
	startedAt := time.Now()
	defer metrics.ObserveDuration(metrics.MessageProcessingDuration.WithLabelValues(metrics.BotLabel(b.userID), "process_message"), startedAt)

	// Получаем senderID из сообщения
	senderID, err := messagespkg.SenderID(msg)
	if err != nil {
		return fmt.Errorf("ошибка преобразования ID пользователя: %w", err)
	}

	// Отмечаем сообщение как прочитанное в WhatsApp
	go func() {
		// Используем оригинальный JID из сообщения
		jid := msg.Info.Sender

		// Устанавливаем статус "в сети"
		if perr := b.b.SendPresence(b.ctx, types.PresenceAvailable); perr != nil {
			logger.Error("Ошибка установки статуса: %v", perr, b.userID)
		}

		if perr := b.b.MarkRead(b.ctx, []types.MessageID{msg.Info.ID}, time.Now(), jid, types.EmptyJID); perr != nil {
			logger.Error("Ошибка отметки сообщений как прочитанных: %v", perr, b.userID)
		}
	}()

	usrCh, err := b.mod.GetCh(senderID)
	if err != nil {
		// Если канал не найден или ошибка, пытаемся пересоздать каналы
		logger.Warn("Канал не найден для senderID %d, пересоздаём: %v", senderID, err, b.userID)
		senderName := msg.Info.PushName
		if initErr := b.initializeUserChannels(senderID, senderName); initErr != nil {
			return fmt.Errorf("ошибка пересоздания каналов пользователя: %w", initErr)
		}
		// Пытаемся получить канал снова
		usrCh, err = b.mod.GetCh(senderID)
		if err != nil {
			return fmt.Errorf("ошибка канала пользователя после пересоздания: %w", err)
		}
	}

	// Если не включён операторский режим
	if !b.parent.IsOperatorMode(usrCh.DialogID) {
		// Запускаем процесс печатания
		go b.setTyping(senderID)
	}

	content := model.AssistResponse{
		Message: message.Text,
	}

	// Извлекаем файлы из сообщения
	files := b.extractFilesFromMessage(msg)
	senderName := msg.Info.PushName

	// Получаем реальный телефон из сохраненной информации о респонденте
	respInfo, exists := b.responders.Load(senderID)
	if !exists {
		logger.Error("Информация о респонденте не найдена для senderID: %d", senderID, b.userID)
		return fmt.Errorf("информация о респонденте не найдена")
	}

	logger.Info("Реальный телефон: %s (senderID: %d)", respInfo.RealPhone, senderID, b.userID)

	// Отправляю сообщение в CRM - используем реальный телефон
	csg := b.c.MSG("user", senderName, message.Text).
		WithPhone(respInfo.RealPhone).
		NewDialog(message.First).
		WithVoice(message.Voice).
		WithFiles(func() []string {
			var f []string
			for _, file := range files {
				f = append(f, file.Name)
			}
			return f
		}()...)

	crmStartedAt := time.Now()
	if err := b.c.SendMessage(csg); err != nil {
		metrics.CRMRequests.WithLabelValues(metrics.BotLabel(b.userID), "incoming", "error").Inc()
		metrics.ObserveDuration(metrics.CRMRequestDuration.WithLabelValues(metrics.BotLabel(b.userID), "incoming"), crmStartedAt)
		logger.Error("Ошибка отправки сообщения в CRM: %v", err, b.userID)
	} else {
		metrics.CRMRequests.WithLabelValues(metrics.BotLabel(b.userID), "incoming", "success").Inc()
		metrics.ObserveDuration(metrics.CRMRequestDuration.WithLabelValues(metrics.BotLabel(b.userID), "incoming"), crmStartedAt)
	}
	/////////////////////////////////////

	// Проверяю включен ли режим оператора
	operatorMode := b.parent.IsOperatorMode(usrCh.DialogID)
	userMessage := b.mod.NewMessage(model.Operator{SetOperator: operatorMode, SenderName: senderName}, "user", &content, &senderName, files...)

	// Используем безопасный метод SendToRx из библиотеки v1.26.0
	if err := usrCh.SendToRx(userMessage); err != nil {
		// Канал закрыт или переполнен, пытаемся пересоздать
		logger.Warn("Ошибка отправки в RxCh для senderID %d: %v, пересоздаём каналы", senderID, err, b.userID)

		if initErr := b.initializeUserChannels(senderID, senderName); initErr != nil {
			return fmt.Errorf("канал RxCh недоступен, ошибка пересоздания: %w", initErr)
		}

		// Повторная попытка с новым каналом
		usrCh, err = b.mod.GetCh(senderID)
		if err != nil {
			return fmt.Errorf("не удалось получить канал после пересоздания: %w", err)
		}

		if err := usrCh.SendToRx(userMessage); err != nil {
			return fmt.Errorf("сообщение не доставлено даже после пересоздания каналов: %w", err)
		}

		logger.Info("Сообщение отправлено после пересоздания каналов для senderID %d", senderID, b.userID)
	}

	return nil
}
