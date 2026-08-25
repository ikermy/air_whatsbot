package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	registerOnce sync.Once

	MessagesReceived          = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "messages_received_total", Help: "Total number of incoming WhatsApp messages received by the service."}, []string{"bot_id", "message_type"})
	MessagesIgnored           = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "messages_ignored_total", Help: "Total number of incoming WhatsApp messages ignored by reason."}, []string{"bot_id", "reason"})
	MessagesProcessed         = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "messages_processed_total", Help: "Total number of WhatsApp messages processed by status."}, []string{"bot_id", "status"})
	CRMRequests               = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "crm_requests_total", Help: "Total number of CRM requests by direction and status."}, []string{"bot_id", "direction", "status"})
	CRMRequestDuration        = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "whatsbot", Name: "crm_request_duration_seconds", Help: "Duration of CRM requests in seconds.", Buckets: prometheus.DefBuckets}, []string{"bot_id", "direction"})
	WhatsAppSend              = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "whatsapp_send_total", Help: "Total number of outgoing WhatsApp sends by status."}, []string{"bot_id", "status"})
	WhatsAppSendDuration      = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "whatsbot", Name: "whatsapp_send_duration_seconds", Help: "Duration of outgoing WhatsApp sends in seconds.", Buckets: prometheus.DefBuckets}, []string{"bot_id"})
	MessageProcessingDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "whatsbot", Name: "message_processing_duration_seconds", Help: "Duration of WhatsApp message processing in seconds.", Buckets: prometheus.DefBuckets}, []string{"bot_id", "stage"})
	UserChannelInitDuration   = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "whatsbot", Name: "user_channel_init_duration_seconds", Help: "Duration of user channel initialization in seconds.", Buckets: prometheus.DefBuckets}, []string{"bot_id", "status"})
	DecryptErrors             = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "decrypt_errors_total", Help: "Total number of decrypt or session-related errors."}, []string{"bot_id", "category"})
	Reconnects                = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "reconnects_total", Help: "Total number of connect and reconnect attempts by result."}, []string{"bot_id", "result"})
	HTTPSRequests             = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "http_requests_total", Help: "Total number of HTTP requests handled by the service."}, []string{"method", "route", "status"})
	HTTPRequestDuration       = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "air", Subsystem: "whatsbot", Name: "http_request_duration_seconds", Help: "Duration of HTTP requests handled by the service.", Buckets: prometheus.DefBuckets}, []string{"method", "route"})
	ActiveSessions            = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "air", Subsystem: "whatsbot", Name: "active_sessions", Help: "Current number of active WhatsApp sessions by state."}, []string{"bot_id", "state"})
	ActiveDialogs             = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "air", Subsystem: "whatsbot", Name: "active_dialogs", Help: "Current number of active dialogs tracked in memory."}, []string{"bot_id"})
	OperatorModeDialogs       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "air", Subsystem: "whatsbot", Name: "operator_mode_dialogs", Help: "Current number of dialogs in operator mode."}, []string{"bot_id"})
	WhatsAppCalls             = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "whatsapp_calls_total", Help: "Total WhatsApp calls by direction and result."}, []string{"bot_id", "direction", "result"})
	WhatsAppCallErrors        = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "whatsapp_call_errors_total", Help: "Total WhatsApp call errors by stage."}, []string{"bot_id", "stage"})
	ActiveWhatsAppCalls       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "air", Subsystem: "whatsbot", Name: "active_whatsapp_calls", Help: "Current number of active WhatsApp calls."}, []string{"bot_id"})
	WhatsAppCallDroppedFrames = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "whatsapp_call_dropped_audio_frames_total", Help: "Dropped WhatsApp call audio frames."}, []string{"bot_id", "direction"})
	WhatsAppCallResponses     = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "air", Subsystem: "whatsbot", Name: "whatsapp_call_realtime_responses_total", Help: "Realtime response events during WhatsApp calls."}, []string{"bot_id", "event"})
)

func Register() {
	registerOnce.Do(func() {
		prometheus.DefaultRegisterer = prometheus.WrapRegistererWithPrefix("", prometheus.DefaultRegisterer)
		registerCollector(collectors.NewGoCollector())
		registerCollector(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		registerCollector(MessagesReceived)
		registerCollector(MessagesIgnored)
		registerCollector(MessagesProcessed)
		registerCollector(CRMRequests)
		registerCollector(CRMRequestDuration)
		registerCollector(WhatsAppSend)
		registerCollector(WhatsAppSendDuration)
		registerCollector(MessageProcessingDuration)
		registerCollector(UserChannelInitDuration)
		registerCollector(DecryptErrors)
		registerCollector(Reconnects)
		registerCollector(HTTPSRequests)
		registerCollector(HTTPRequestDuration)
		registerCollector(ActiveSessions)
		registerCollector(ActiveDialogs)
		registerCollector(OperatorModeDialogs)
		registerCollector(WhatsAppCalls)
		registerCollector(WhatsAppCallErrors)
		registerCollector(ActiveWhatsAppCalls)
		registerCollector(WhatsAppCallDroppedFrames)
		registerCollector(WhatsAppCallResponses)
	})
}

func registerCollector(collector prometheus.Collector) {
	if err := prometheus.Register(collector); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return
		}
		panic(err)
	}
}

func Handler() http.Handler {
	Register()
	return promhttp.Handler()
}

func ObserveDuration(observer prometheus.Observer, startedAt time.Time) {
	observer.Observe(time.Since(startedAt).Seconds())
}

func NormalizeRoute(path string) string {
	if path == "" {
		return "unknown"
	}
	if path == "/metrics" {
		return path
	}
	trimmed := strings.TrimSuffix(path, "/")
	if trimmed == "" {
		return "/"
	}
	return trimmed
}

func BotLabel(userID uint32) string {
	return strconv.FormatUint(uint64(userID), 10)
}

func TrackActiveDialogs(userID uint32, count int) {
	ActiveDialogs.WithLabelValues(BotLabel(userID)).Set(float64(count))
}

func TrackOperatorModeDialogs(userID uint32, count int) {
	OperatorModeDialogs.WithLabelValues(BotLabel(userID)).Set(float64(count))
}
