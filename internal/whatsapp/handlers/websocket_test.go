package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	authpkg "air_whatsbot/internal/whatsapp/auth"

	"github.com/gorilla/websocket"
)

type fakeController struct{}

func (fakeController) GetBotUsername(uint32) string                      { return "" }
func (fakeController) StartUserBot(uint32) error                         { return nil }
func (fakeController) StopUserBot(uint32) error                          { return nil }
func (fakeController) RestartUserBot(uint32) error                       { return nil }
func (fakeController) BotState(uint32) (bool, bool)                      { return true, false }
func (fakeController) GetUserContactsStreaming(uint32, chan<- any) error { return nil }

func (fakeController) AuthenticateWithQRForWeb(_ uint32, stateChan chan<- authpkg.State) error {
	stateChan <- authpkg.State{Type: "qr_code", Payload: "qr-1"}
	stateChan <- authpkg.State{Type: "qr-success", Payload: "scanned"}
	stateChan <- authpkg.State{Type: "success", Payload: "Авторизация успешно завершена: 123"}
	return nil
}

func TestAuthWebSocketDeliversSuccess(t *testing.T) {
	h := New(fakeController{}, authpkg.NewRegistry())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.AuthWebSocketHandler(w, r)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?uid=23"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var got []string
	for {
		var state authpkg.State
		if err := conn.ReadJSON(&state); err != nil {
			break
		}
		got = append(got, state.Type)
		if state.Type == "success" || state.Type == "error" {
			break
		}
	}

	last := ""
	if len(got) > 0 {
		last = got[len(got)-1]
	}
	if last != "success" {
		t.Fatalf("terminal state = %q, sequence = %v; want success", last, got)
	}
}
