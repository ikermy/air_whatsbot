package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// ReadUID извлекает и валидирует query-параметр uid (обязателен и больше нуля),
// записывая ошибку в том же формате, что и остальной HTTP-слой.
func ReadUID(w http.ResponseWriter, r *http.Request) (uint32, bool) {
	uidStr := r.URL.Query().Get("uid")
	if uidStr == "" {
		writeUIDError(w, "uid is required")
		return 0, false
	}

	uid, err := strconv.ParseUint(uidStr, 10, 32)
	if err != nil {
		logger.Error("Некорректный uid: %v", err)
		writeUIDError(w, "invalid uid")
		return 0, false
	}
	if uid == 0 {
		logger.Error("Некорректный uid: значение не может быть нулевым")
		writeUIDError(w, "invalid uid")
		return 0, false
	}

	return uint32(uid), true
}

func writeUIDError(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
