package whatsapp

import (
	"testing"

	responder "air_whatsbot/internal/whatsapp/responder"
	"go.mau.fi/whatsmeow/types"
)

func TestRestoreResponderFields(t *testing.T) {
	lid := types.NewJID("33947538968617", "lid")

	t.Run("preloaded known-only entry is filled", func(t *testing.T) {
		info := &responder.Info{Known: true}
		restored := restoreResponderFields(info, lid, "79222913731", 33947538968617)
		if !restored {
			t.Fatal("expected restored = true")
		}
		if info.JID.User != "33947538968617" || info.JID.Server != "lid" {
			t.Fatalf("JID not restored: %s", info.JID.String())
		}
		if info.RealPhone != "79222913731" {
			t.Fatalf("RealPhone = %q, want %q", info.RealPhone, "79222913731")
		}
	})

	t.Run("existing info is preserved", func(t *testing.T) {
		info := &responder.Info{JID: lid, RealPhone: "79222913731"}
		restored := restoreResponderFields(info, types.NewJID("1", "s.whatsapp.net"), "999", 1)
		if restored {
			t.Fatal("expected restored = false")
		}
		if info.JID.User != "33947538968617" || info.RealPhone != "79222913731" {
			t.Fatalf("info mutated: %+v", info)
		}
	})

	t.Run("empty real phone falls back to senderID", func(t *testing.T) {
		info := &responder.Info{}
		restoreResponderFields(info, lid, "", 42)
		if info.RealPhone != "42" {
			t.Fatalf("RealPhone = %q, want %q", info.RealPhone, "42")
		}
	})

	t.Run("nil info is ignored", func(t *testing.T) {
		if restoreResponderFields(nil, lid, "1", 1) {
			t.Fatal("expected restored = false for nil info")
		}
	})
}
