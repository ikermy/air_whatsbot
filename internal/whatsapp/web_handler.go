package whatsapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// AuthWebSocketHandler Обработчик WebSocket для QR-авторизации и отслеживания состояния
func (u *User) AuthWebSocketHandler(h http.ResponseWriter, r *http.Request) {
	userId, ok := readUID(h, r)
	if !ok {
		return
	}

	// Очищаем предыдущие сессии для этого пользователя
	u.CleanupExistingAuthSessions(userId)

	// Создаем уникальный ID сессии
	sessionID := fmt.Sprintf("auth_%d_%d", userId, time.Now().UnixNano())

	// Создаем канал для управления процессом авторизации
	stateChan := make(chan AuthState, 10)

	// Создаем контекст для управления горутиной авторизации
	authCtx, cancelAuth := context.WithCancel(context.Background())

	// Сохраняем каналы в карте активных сессий
	authSessions.Lock()
	authSessions.sessions[sessionID] = &AuthSession{
		StateChan: stateChan,
		UserId:    userId,
	}
	authSessions.Unlock()

	// Настраиваем WebSocket
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // В продакшене нужна более строгая проверка
		},
	}

	conn, err := upgrader.Upgrade(h, r, nil)
	if err != nil {
		logger.Error("Ошибка установки WebSocket: %v", err, userId)
		cancelAuth()
		return
	}

	// Канал для обработки закрытия соединения
	done := make(chan struct{})
	doneIsClosed := false
	var doneMutex sync.Mutex

	// Безопасное закрытие канала
	safeCloseDone := func() {
		doneMutex.Lock()
		defer doneMutex.Unlock()
		if !doneIsClosed {
			close(done)
			doneIsClosed = true
		}
	}

	// Флаг закрытия stateChan
	stateChannelClosed := false
	var stateChannelMutex sync.Mutex

	// Безопасная отправка в stateChan
	safeSendState := func(state AuthState) bool {
		stateChannelMutex.Lock()
		defer stateChannelMutex.Unlock()
		if stateChannelClosed {
			return false
		}
		select {
		case stateChan <- state:
			return true
		case <-authCtx.Done():
			return false
		default:
			return false
		}
	}

	// После завершения обработчика закрываем каналы и удаляем сессию
	defer func() {
		cancelAuth() // Отменяем контекст авторизации
		safeCloseDone()
		authSessions.Lock()
		delete(authSessions.sessions, sessionID)
		authSessions.Unlock()

		// Безопасно закрываем stateChan
		stateChannelMutex.Lock()
		if !stateChannelClosed {
			close(stateChan)
			stateChannelClosed = true
		}
		stateChannelMutex.Unlock()

		_ = conn.Close()
	}()

	// Запускаем процесс авторизации в отдельной горутине
	go func() {
		err := u.AuthenticateWithQRForWeb(userId, stateChan)
		if err != nil {
			logger.Error("Ошибка авторизации: %v", err, userId)
			// Безопасно отправляем ошибку в канал состояний
			safeSendState(AuthState{
				Type:    "error",
				Payload: fmt.Sprintf("Ошибка авторизации: %v", err),
			})
			return
		}
	}()

	// Отправляем состояния процесса авторизации клиенту
	for {
		select {
		case state, ok := <-stateChan:
			if !ok {
				return // Канал закрыт, завершаем работу
			}

			// Отправляем состояние клиенту
			if err = conn.WriteJSON(state); err != nil {
				logger.Error("Ошибка отправки состояния: %v", err, userId)
				return
			}

			// Завершаем соединение после успешной авторизации или ошибки
			if state.Type == "success" || state.Type == "error" {
				return
			}

		case <-done:
			return // Соединение закрыто
		}
	}
}

// AvailableHandler Доступность канала
func (u *User) AvailableHandler(h http.ResponseWriter, _ *http.Request) {
	h.WriteHeader(http.StatusOK)
}

// HandlerGetContactsWS обрабатывает WebSocket соединение для получения контактов в потоковом режиме
func (u *User) HandlerGetContactsWS(h http.ResponseWriter, r *http.Request) {
	userId, ok := readUID(h, r)
	if !ok {
		return
	}

	// Настраиваем WebSocket сразу после проверки токена
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // В продакшене нужна более строгая проверка
		},
	}

	conn, err := upgrader.Upgrade(h, r, nil)
	if err != nil {
		logger.Error("Ошибка установки WebSocket: %v", err, userId)
		return
	}
	defer func() { _ = conn.Close() }()

	// Проверяем, существует ли бот для данного пользователя
	value, exists := u.uBot.Load(userId)
	if !exists {
		logger.Error("'HandlerGetContactsWS' Бот не найден для пользователя", userId)
		// Отправляем ошибку через WebSocket и закрываем с кодом 1011
		errorMsg := map[string]any{
			"type":  "error",
			"error": "WhatsApp бот не найден. Необходимо создать и настроить WhatsApp бот",
		}
		_ = conn.WriteJSON(errorMsg)
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1011, "bot not found"))
		return
	}
	bot := value.(*Bot)

	// Проверяем что бот отключён
	if bot.stopped.Load() {
		logger.Warn("'HandlerGetContactsWS' Бот остановлен для пользователя", userId)
		// Отправляем ошибку через WebSocket и закрываем с кодом 1006
		errorMsg := map[string]any{
			"type":  "error",
			"error": "WhatsApp бот остановлен. Проверьте настройки канала",
		}
		_ = conn.WriteJSON(errorMsg)
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1006, "bot stopped"))
		return
	}

	// Создаем канал для передачи данных о контактах
	contactsChan := make(chan any, 100)

	// Переменная для отслеживания успешности операции
	var hasError bool

	// Запускаем получение контактов в отдельной горутине
	go func() {
		defer close(contactsChan)
		err := u.GetUserContactsStreaming(userId, contactsChan)
		if err != nil {
			hasError = true
			logger.Error("'HandlerGetContactsWS' Ошибка получения контактов: %v", err, userId)
			// Отправляем ошибку через WebSocket
			errorMsg := map[string]any{
				"type":  "error",
				"error": err.Error(),
			}
			contactsChan <- errorMsg
		}
	}()

	// Отправляем данные клиенту через WebSocket
	for data := range contactsChan {
		// Проверяем, является ли сообщение ошибкой
		if dataMap, ok := data.(map[string]any); ok {
			if dataMap["type"] == "error" {
				hasError = true
			}
		}

		if err := conn.WriteJSON(data); err != nil {
			logger.Error("Ошибка отправки данных через WebSocket: %v", err, userId)
			return
		}
	}

	// Логируем результат только если не было ошибок
	if !hasError {
		logger.Info("'HandlerGetContactsWS' Контакты успешно отправлены", userId)
	} else {
		logger.Warn("'HandlerGetContactsWS' Обработка контактов завершена с ошибкой", userId)
	}
}

func (u *User) GetBotName(h http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		h.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(h, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := readUID(h, r)
	if !ok {
		return
	}

	botName := u.GetBotUsername(userId)
	response := map[string]string{"bot_name": botName}
	h.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(h).Encode(response); err != nil {
		logger.Error("'GetBotName' Ошибка при кодировании JSON: %v", err)
		h.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (u *User) startBot(h http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		h.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(h, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := readUID(h, r)
	if !ok {
		return
	}

	err := u.StartUserBot(userId)
	if err != nil {
		logger.Error("Ошибка запуска бота для uid %d: %v", userId, err)
		h.Header().Set("Content-Type", "application/json")
		h.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(h).Encode(map[string]string{"error": "failed to stop bot"})
		return
	}

	// Успешный ответ после запуска бота
	h.Header().Set("Content-Type", "application/json")
	h.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(h).Encode(map[string]string{"status": "bot started successfully"})
}

func (u *User) stopBot(h http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		h.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(h, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := readUID(h, r)
	if !ok {
		return
	}

	err := u.stopUserBot(userId)
	if err != nil {
		logger.Error("'stopBot' Ошибка остановки бота для uid %d: %v", userId, err)
		h.Header().Set("Content-Type", "application/json")
		h.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(h).Encode(map[string]string{"error": "failed to stop bot"})
		return
	}

	// Успешный ответ после остановки бота
	h.Header().Set("Content-Type", "application/json")
	h.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(h).Encode(map[string]string{"status": "bot stopped successfully"})
}

func (u *User) RestartBot(h http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		h.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(h, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := readUID(h, r)
	if !ok {
		return
	}

	// канал для результата
	resultCh := make(chan error, 1)

	// запускаем в горутине
	go func() {
		resultCh <- u.restartUserBot(userId)
	}()

	select {
	case err := <-resultCh:
		if err != nil {
			logger.Error("Ошибка перезагрузки бота: %v", err, userId)
			http.Error(h, "Failed to reload bot", http.StatusInternalServerError)
			return
		}
		// успех
		h.Header().Set("Content-Type", "application/json")
		h.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(h).Encode(map[string]string{"status": "bot reloaded successfully"})
	case <-time.After(5 * time.Second):
		// если за 5 секунд не пришёл результат — считаем успехом
		h.Header().Set("Content-Type", "application/json")
		h.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(h).Encode(map[string]string{"status": "bot reload in progress"})
	}
}

func (u *User) StartBotHTTP(h http.ResponseWriter, r *http.Request) { u.startBot(h, r) }
func (u *User) StopBotHTTP(h http.ResponseWriter, r *http.Request)  { u.stopBot(h, r) }

func readUID(h http.ResponseWriter, r *http.Request) (uint32, bool) {
	// Читаем uid из query-параметров перед upgrade
	uidStr := r.URL.Query().Get("uid")
	if uidStr == "" {
		h.Header().Set("Content-Type", "application/json")
		h.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(h).Encode(map[string]string{"error": "uid is required"})
		return 0, false
	}

	uid, err := strconv.ParseUint(uidStr, 10, 32)
	if err != nil {
		logger.Error("Некорректный uid: %v", err)
		h.Header().Set("Content-Type", "application/json")
		h.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(h).Encode(map[string]string{"error": "invalid uid"})
		return 0, false
	}

	return uint32(uid), true
}
