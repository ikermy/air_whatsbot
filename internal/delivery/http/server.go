package http

import (
	"air_whatsbot/internal/metrics"
	stdhttp "net/http"

	"github.com/ikermy/air-logger/v2/pkg/logger"
)

type WhatsAppHandlers interface {
	AvailableHandler(stdhttp.ResponseWriter, *stdhttp.Request)
	AuthWebSocketHandler(stdhttp.ResponseWriter, *stdhttp.Request)
	HandlerGetContactsWS(stdhttp.ResponseWriter, *stdhttp.Request)
	GetBotName(stdhttp.ResponseWriter, *stdhttp.Request)
	StartBotHTTP(stdhttp.ResponseWriter, *stdhttp.Request)
	StopBotHTTP(stdhttp.ResponseWriter, *stdhttp.Request)
	RestartBot(stdhttp.ResponseWriter, *stdhttp.Request)
}

type Server struct {
	handlers WhatsAppHandlers
}

func NewServer(handlers WhatsAppHandlers) *Server {
	return &Server{handlers: handlers}
}

func (s *Server) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/whats/available", metrics.HTTPMiddleware("/available", enableCORS(s.handlers.AvailableHandler)))
	mux.Handle("/whats/ws", metrics.HTTPMiddleware("/ws", stdhttp.HandlerFunc(s.handlers.AuthWebSocketHandler)))
	mux.Handle("/whats/contacts/ws", metrics.HTTPMiddleware("/contacts/ws", stdhttp.HandlerFunc(s.handlers.HandlerGetContactsWS)))
	mux.Handle("/whats/getname", metrics.HTTPMiddleware("/getname", enableCORS(s.handlers.GetBotName)))
	mux.Handle("/whats/enable", metrics.HTTPMiddleware("/enable", enableCORS(s.handlers.StartBotHTTP)))
	mux.Handle("/whats/disable", metrics.HTTPMiddleware("/disable", enableCORS(s.handlers.StopBotHTTP)))
	mux.Handle("/whats/restart", metrics.HTTPMiddleware("/restart", enableCORS(s.handlers.RestartBot)))
	return mux
}

func (s *Server) ListenAndServe(addr string) error {
	logger.Info("Сервер аутентификации запущен")
	return stdhttp.ListenAndServe(addr, s.Handler())
}

func enableCORS(next stdhttp.HandlerFunc) stdhttp.HandlerFunc {
	return func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		next(w, r)
	}
}
