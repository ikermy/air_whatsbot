package whatsapp

import (
	authpkg "air_whatsbot/internal/whatsapp/auth"
)

// AuthenticateWithQRForWeb запускает QR-авторизацию через пакет auth.
func (u *User) AuthenticateWithQRForWeb(userID uint32, stateChan chan<- authpkg.State) error {
	return authpkg.AuthenticateWithQR(u, userID, stateChan)
}
