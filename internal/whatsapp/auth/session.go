package auth

// State represents the current status of the QR-authentication flow.
type State struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

// Session stores channels for a single authentication session.
type Session struct {
	StateChan chan State
	UserId    uint32
}
