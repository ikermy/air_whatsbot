package contacts

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ikermy/air-logger/v2/pkg/logger"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// Stream отправляет контакты и группы пользователя через канал в потоковом режиме.
func Stream(ctx context.Context, client *whatsmeow.Client, userID uint32, dataChan chan<- any) error {
	// Структуры для разных типов контактов, соответствующие формату AiR_TgUserBot
	// ContactInfo содержит базовую информацию о контакте (пользователе или боте).
	type ContactInfo struct {
		ID        int64  `json:"id"`
		FirstName string `json:"first_name,omitempty"`
		LastName  string `json:"last_name,omitempty"`
		Username  string `json:"username,omitempty"`
		Phone     string `json:"phone,omitempty"`
	}

	// ChannelInfo содержит базовую информацию о канале.
	type ChannelInfo struct {
		ID       int64  `json:"id"`
		Title    string `json:"title"`
		Username string `json:"username,omitempty"`
	}

	// GroupInfo содержит базовую информацию об обычной группе.
	type GroupInfo struct {
		ID    int64  `json:"id"`
		Title string `json:"title"`
	}

	// SupergroupInfo содержит базовую информацию о супергруппе.
	type SupergroupInfo struct {
		ID       int64  `json:"id"`
		Title    string `json:"title"`
		Username string `json:"username,omitempty"`
	}

	// Проверяем, инициализирован ли клиент
	if client == nil {
		return fmt.Errorf("WhatsApp: пользователь %d: клиент не инициализирован", userID)
	}

	// Проверка авторизации
	if !client.IsLoggedIn() {
		return fmt.Errorf("бот для пользователя %d не авторизован", userID)
	}

	// Отправляем статус начала процесса
	dataChan <- map[string]any{
		"type":   "status",
		"status": "started",
		"stage":  "contacts",
	}

	// Инициализируем переменные для хранения результатов
	var humans []ContactInfo
	var whatsappGroups []GroupInfo
	var wg sync.WaitGroup
	var contactsErr, groupsErr error

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// Получаем контакты из store
	wg.Add(1)
	go func() {
		defer wg.Done()

		store := client.Store
		if store == nil {
			logger.Error("'GetUserContactsStreaming' Хранилище контактов недоступно", userID)
			contactsErr = errors.New("хранилище контактов недоступно")
			return
		}

		// Получаем контакты через GetAllContacts
		allContacts, errGetContacts := store.Contacts.GetAllContacts(ctx)
		if errGetContacts != nil {
			logger.Error("'GetUserContactsStreaming' Ошибка получения контактов: %v", errGetContacts, userID)
			contactsErr = fmt.Errorf("ошибка получения контактов: %w", errGetContacts)
			return
		}

		// Отправляем статус обработки контактов
		dataChan <- map[string]any{
			"type":  "status",
			"stage": "processing_contacts",
			"total": len(allContacts),
		}

		processedCount := 0
		validContactsCount := 0

		for jid, contact := range allContacts {

			// Проверяем, что JID относится к пользователю, а не к группе
			if jid.Server != types.DefaultUserServer {
				continue // Пропускаем не пользователей
			}

			validContactsCount++

			// Обрабатываем полное имя: разбиваем на имя и фамилию, если возможно
			firstName := ""
			lastName := ""

			if contact.FullName != "" {
				nameParts := strings.SplitN(contact.FullName, " ", 2)
				firstName = nameParts[0]
				if len(nameParts) > 1 {
					lastName = nameParts[1]
				}
			} else if contact.PushName != "" {
				firstName = contact.PushName
			}

			// Если имя всё еще пустое, используем номер телефона
			if firstName == "" {
				phone := jid.User
				if phone != "" {
					firstName = phone
				} else {
					firstName = fmt.Sprintf("Контакт %s", jid.String())
				}
			}

			// Преобразуем JID строку в числовой идентификатор для совместимости с форматом Telegram
			idString := jid.String()
			hash := int64(0)
			for _, char := range idString {
				hash = 31*hash + int64(char)
			}
			if hash < 0 {
				hash = -hash // Убедимся, что ID положительный
			}

			info := ContactInfo{
				ID:        hash,
				FirstName: firstName,
				LastName:  lastName,
				Phone:     jid.User,
			}

			humans = append(humans, info)
			processedCount++

			// Отправляем прогресс каждые 10 контактов или если это последний
			if processedCount%10 == 0 || processedCount == validContactsCount {
				dataChan <- map[string]any{
					"type":      "progress",
					"stage":     "contacts",
					"processed": processedCount,
					"total":     validContactsCount,
				}
			}
		}
	}()

	// Получаем группы
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Отправляем статус начала обработки групп
		dataChan <- map[string]any{
			"type":   "status",
			"status": "started",
			"stage":  "groups",
		}

		joinedGroups, errGetGroups := client.GetJoinedGroups(ctx)
		if errGetGroups != nil {
			groupsErr = fmt.Errorf("ошибка получения групп: %w", errGetGroups)
			logger.Warn("Ошибка получения групп: %v", errGetGroups, userID)

			if client.Store != nil {
				// Создаем список JID групп, с которыми общался пользователь
				var groupJIDs []types.JID

				// Смотрим, есть ли контакты с серверами g.us
				storeContacts, _ := client.Store.Contacts.GetAllContacts(ctx)
				for jid := range storeContacts {
					if strings.HasSuffix(jid.Server, "g.us") {
						groupJIDs = append(groupJIDs, jid)
					}
				}

				// Отправляем информацию о количестве найденных групп
				dataChan <- map[string]any{
					"type":  "status",
					"stage": "processing_groups",
					"total": len(groupJIDs),
				}

				// Обрабатываем найденные JID как группы и преобразуем их в формат GroupInfo
				for i, jid := range groupJIDs {
					// Используем ID группы для формирования имени
					groupName := fmt.Sprintf("Группа %s", strings.Split(jid.User, "@")[0])

					// Преобразуем JID строку в числовой идентификатор для совместимости с форматом Telegram
					idString := jid.String()
					hash := int64(0)
					for _, char := range idString {
						hash = 31*hash + int64(char)
					}
					if hash < 0 {
						hash = -hash // Убедимся, что ID положительный
					}

					info := GroupInfo{
						ID:    hash,
						Title: groupName,
					}
					whatsappGroups = append(whatsappGroups, info)

					// Отправляем прогресс
					if (i+1)%5 == 0 || i == len(groupJIDs)-1 {
						dataChan <- map[string]any{
							"type":      "progress",
							"stage":     "groups",
							"processed": i + 1,
							"total":     len(groupJIDs),
						}
					}
				}

				if len(whatsappGroups) > 0 {
					groupsErr = nil // Сбрасываем ошибку
				}
			}

			return
		}

		// Отправляем информацию о количестве групп
		dataChan <- map[string]any{
			"type":  "status",
			"stage": "processing_groups",
			"total": len(joinedGroups),
		}

		for i, group := range joinedGroups {
			groupName := group.Name
			if groupName == "" {
				// Если имя группы пустое, пробуем использовать тему
				if group.Topic != "" {
					groupName = group.Topic
				} else {
					// Если и тема пуста, формируем название на основе ID
					groupName = fmt.Sprintf("%s", group.JID.User)
				}
			}

			// Преобразуем JID строку в числовой идентификатор для совместимости с форматом Telegram
			idString := group.JID.String()
			hash := int64(0)
			for _, char := range idString {
				hash = 31*hash + int64(char)
			}
			if hash < 0 {
				hash = -hash // Убедимся, что ID положительный
			}

			info := GroupInfo{
				ID:    hash,
				Title: groupName,
			}
			whatsappGroups = append(whatsappGroups, info)

			// Отправляем прогресс каждые 5 групп или если это последняя
			if (i+1)%5 == 0 || i == len(joinedGroups)-1 {
				dataChan <- map[string]any{
					"type":      "progress",
					"stage":     "groups",
					"processed": i + 1,
					"total":     len(joinedGroups),
				}
			}
		}
	}()

	wg.Wait()

	// Проверяем наличие критических ошибок
	hasCriticalErrors := false

	if contactsErr != nil && len(humans) == 0 {
		logger.Error("Ошибка получения контактов: %v", contactsErr, userID)
		// Если это критическая ошибка подключения и нет групп, считаем это критической ошибкой
		if len(whatsappGroups) == 0 {
			// Проверяем, является ли ошибка критической (websocket not connected, клиент не инициализирован и т.д.)
			errorStr := contactsErr.Error()
			if strings.Contains(errorStr, "websocket not connected") ||
				strings.Contains(errorStr, "клиент не инициализирован") ||
				strings.Contains(errorStr, "бот не найден") {
				hasCriticalErrors = true
			}
		}

		// Если нет групп и это не критическая ошибка, возвращаем ошибку
		if len(whatsappGroups) == 0 && !hasCriticalErrors {
			return contactsErr
		}
	}

	if groupsErr != nil && len(whatsappGroups) == 0 {
		logger.Error("Ошибка получения групп: %v", groupsErr, userID)
		// Проверяем, является ли ошибка критической
		errorStr := groupsErr.Error()
		if strings.Contains(errorStr, "websocket not connected") ||
			strings.Contains(errorStr, "клиент не инициализирован") ||
			strings.Contains(errorStr, "бот не найден") {
			hasCriticalErrors = true
		}

		// Если нет контактов и это не критическая ошибка, возвращаем ошибку
		if len(humans) == 0 && !hasCriticalErrors {
			return groupsErr
		}
	}

	// Если есть критические ошибки, отправляем сообщение об ошибке вместо результата
	if hasCriticalErrors {
		errorResult := map[string]any{
			"type":  "error",
			"error": "Сервис WhatsApp недоступен. Убедитесь, что бот подключен.",
		}
		dataChan <- errorResult
		return fmt.Errorf("критические ошибки подключения к WhatsApp")
	}

	// Формируем финальный результат в формате, совместимом с AiR_TgUserBot
	// Преобразуем структуры ContactInfo и GroupInfo в map[string]any
	humansMap := make([]any, 0, len(humans))
	for _, h := range humans {
		humansMap = append(humansMap, map[string]any{
			"id":         h.ID,
			"first_name": h.FirstName,
			"last_name":  h.LastName,
			"username":   h.Username,
			"phone":      h.Phone,
		})
	}

	groupsMap := make([]any, 0, len(whatsappGroups))
	for _, g := range whatsappGroups {
		groupsMap = append(groupsMap, map[string]any{
			"id":    g.ID,
			"title": g.Title,
		})
	}

	dataMap := map[string]any{
		"humans":      humansMap,
		"bots":        []any{}, // В WhatsApp нет ботов, поэтому оставляем пустой массив
		"channels":    []any{}, // В WhatsApp нет каналов, оставляем пустой массив
		"groups":      groupsMap,
		"supergroups": []any{}, // В WhatsApp нет супергрупп, оставляем пустой массив
	}

	result := map[string]any{
		"type": "final_result",
		"data": dataMap,
	}

	// Отправляем финальный результат
	dataChan <- result

	// Отправляем статус завершения
	dataChan <- map[string]any{
		"type":           "status",
		"status":         "completed",
		"total_contacts": len(humans),
		"total_groups":   len(whatsappGroups),
	}

	return nil
}
