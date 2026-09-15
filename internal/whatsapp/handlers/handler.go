package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	authpkg "air_whatsbot/internal/whatsapp/auth"

	"github.com/gorilla/websocket"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// Controller описывает минимальный набор операций WhatsApp-ботов, нужный HTTP/WS-адаптеру.
type Controller interface {
	GetBotUsername(userID uint32) string
	StartUserBot(userID uint32) error
	StopUserBot(userID uint32) error
	RestartUserBot(userID uint32) error
	// AuthenticateWithQRForWeb запускает процесс QR-авторизации и пишет состояния в канал.
	AuthenticateWithQRForWeb(userID uint32, stateChan chan<- authpkg.State) error
	// GetUserContactsStreaming стримит контакты пользователя в канал.
	GetUserContactsStreaming(userID uint32, dataChan chan<- any) error
	// BotState сообщает, существует ли бот и остановлен ли он.
	BotState(userID uint32) (found bool, stopped bool)
}

// Handler адаптирует операции ботов и QR-авторизации к HTTP/WebSocket.
type Handler struct {
	controller Controller
	auth       *authpkg.Registry
}

func New(controller Controller, registry *authpkg.Registry) *Handler {
	return &Handler{controller: controller, auth: registry}
}

// AvailableHandler Доступность канала
func (h *Handler) AvailableHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// AuthWebSocketHandler Обработчик WebSocket для QR-авторизации и отслеживания состояния
func (h *Handler) AuthWebSocketHandler(w http.ResponseWriter, r *http.Request) {
	userID, ok := ReadUID(w, r)
	if !ok || userID == 0 {
		return
	}

	// Создаем уникальный ID сессии
	sessionID := fmt.Sprintf("auth_%d_%d", userID, time.Now().UnixNano())

	// Создаем канал для управления процессом авторизации
	stateChan := make(chan authpkg.State, 10)

	// Создаем контекст для управления горутиной авторизации
	authCtx, cancelAuth := context.WithCancel(context.Background())

	// Настраиваем WebSocket
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // В продакшене нужна более строгая проверка
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("Ошибка установки WebSocket: %v", err, userID)
		cancelAuth()
		h.auth.Remove(sessionID)
		return
	}

	// Сбрасываем предыдущие сессии пользователя, чтобы новая авторизация
	// заменяла прежнее соединение, а не отклонялась. Регистрируем сессию уже
	// после Upgrade: ошибки авторизации должны возвращаться в формате
	// authpkg.State, а не обычного HTTP-ответа (клиент ждёт поле type).
	h.auth.CancelUser(userID)
	h.auth.Add(sessionID, &authpkg.Session{StateChan: stateChan, UserId: userID})

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

	// Безопасная отправка в stateChan
	safeSendState := func(state authpkg.State) bool {
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
		h.auth.Remove(sessionID)
		_ = conn.Close()
	}()

	// Запускаем процесс авторизации в отдельной горутине
	go func() {
		if err := h.controller.AuthenticateWithQRForWeb(userID, stateChan); err != nil {
			logger.Error("Ошибка авторизации: %v", err, userID)
			safeSendState(authpkg.State{
				Type:    "error",
				Payload: fmt.Sprintf("Ошибка авторизации: %v", err),
			})
		}
	}()

	// Отправляем состояния процесса авторизации клиенту
	for {
		select {
		case state, ok := <-stateChan:
			if !ok {
				return // Канал закрыт, завершаем работу
			}

			if state.Type == "success" || state.Type == "error" {
				logger.Debug("WS auth: отправляю терминальное состояние %q", state.Type, userID)
			}
			if err = conn.WriteJSON(state); err != nil {
				logger.Error("Ошибка отправки состояния: %v", err, userID)
				return
			}

			// Завершаем соединение после успешной авторизации или ошибки.
			// Делаем корректный close-handshake с коротким ожиданием чтения:
			// немедленный conn.Close() может привести к RST и потерять только
			// что отправленный терминальный кадр (особенно через прокси).
			if state.Type == "success" || state.Type == "error" {
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
					time.Now().Add(time.Second))
				_ = conn.SetReadDeadline(time.Now().Add(time.Second))
				for {
					if _, _, readErr := conn.ReadMessage(); readErr != nil {
						break
					}
				}
				return
			}

		case <-done:
			return // Соединение закрыто
		}
	}
}

// HandlerGetContactsWS обрабатывает WebSocket соединение для получения контактов в потоковом режиме
func (h *Handler) HandlerGetContactsWS(w http.ResponseWriter, r *http.Request) {
	userID, ok := ReadUID(w, r)
	if !ok || userID == 0 {
		// ReadUID уже записал 400 + JSON, повторная запись заголовка не нужна.
		return
	}

	// Настраиваем WebSocket сразу после проверки токена
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // В продакшене нужна более строгая проверка
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("Ошибка установки WebSocket: %v", err, userID)
		return
	}
	defer func() { _ = conn.Close() }()

	// Проверяем, существует ли бот для данного пользователя
	found, stopped := h.controller.BotState(userID)
	if !found {
		logger.Error("'HandlerGetContactsWS' Бот не найден для пользователя", userID)
		_ = conn.WriteJSON(map[string]any{
			"type":  "error",
			"error": "WhatsApp бот не найден. Необходимо создать и настроить WhatsApp бот",
		})
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1011, "bot not found"))
		return
	}
	if stopped {
		logger.Warn("'HandlerGetContactsWS' Бот остановлен для пользователя", userID)
		_ = conn.WriteJSON(map[string]any{
			"type":  "error",
			"error": "WhatsApp бот остановлен. Проверьте настройки канала",
		})
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1006, "bot stopped"))
		return
	}

	// Создаем канал для передачи данных о контактах
	contactsChan := make(chan any, 100)
	var hasError bool

	// Запускаем получение контактов в отдельной горутине
	go func() {
		defer close(contactsChan)
		if err := h.controller.GetUserContactsStreaming(userID, contactsChan); err != nil {
			hasError = true
			logger.Error("'HandlerGetContactsWS' Ошибка получения контактов: %v", err, userID)
			contactsChan <- map[string]any{
				"type":  "error",
				"error": err.Error(),
			}
		}
	}()

	// Отправляем данные клиенту через WebSocket
	for data := range contactsChan {
		if dataMap, ok := data.(map[string]any); ok && dataMap["type"] == "error" {
			hasError = true
		}

		if err := conn.WriteJSON(data); err != nil {
			logger.Error("Ошибка отправки данных через WebSocket: %v", err, userID)
			return
		}
	}

	if !hasError {
		logger.Info("'HandlerGetContactsWS' Контакты успешно отправлены", userID)
	} else {
		logger.Warn("'HandlerGetContactsWS' Обработка контактов завершена с ошибкой", userID)
	}
}

func (h *Handler) GetBotName(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := ReadUID(w, r)
	if !ok || userID == 0 {
		return
	}

	response := map[string]string{"bot_name": h.controller.GetBotUsername(userID)}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error("'GetBotName' Ошибка при кодировании JSON: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *Handler) StartBotHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := ReadUID(w, r)
	if !ok || userID == 0 {
		return
	}

	if err := h.controller.StartUserBot(userID); err != nil {
		logger.Error("Ошибка запуска бота для uid %d: %v", userID, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		if err := json.NewEncoder(w).Encode(map[string]string{"error": "failed to start bot"}); err != nil {
			logger.Error("Ошибка ответа при старте бота: %v", err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "bot started successfully"}); err != nil {
		logger.Error("Ошибка ответа при старте бота: %v", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

func (h *Handler) StopBotHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := ReadUID(w, r)
	if !ok || userID == 0 {
		return
	}

	if err := h.controller.StopUserBot(userID); err != nil {
		logger.Error("'StopBotHTTP' Ошибка остановки бота для uid %d: %v", userID, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		if err := json.NewEncoder(w).Encode(map[string]string{"error": "failed to stop bot"}); err != nil {
			logger.Error("Ошибка ответа при остановке бота: %v", err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "bot stopped successfully"}); err != nil {
		logger.Error("Ошибка ответа при остановке бота: %v", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

func (h *Handler) RestartBot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := ReadUID(w, r)
	if !ok || userID == 0 {
		return
	}

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- h.controller.RestartUserBot(userID)
	}()

	select {
	case err := <-resultCh:
		if err != nil {
			logger.Error("Ошибка перезагрузки бота: %v", err, userID)
			http.Error(w, "Failed to reload bot", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{"status": "bot reloaded successfully"}); err != nil {
			logger.Error("Ошибка ответа при перезагрузке бота: %v", err)
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
		}
	case <-time.After(5 * time.Second):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{"status": "bot reload in progress"}); err != nil {
			logger.Error("Ошибка ответа 'reload in progress': %v", err)
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
		}
	}
}
