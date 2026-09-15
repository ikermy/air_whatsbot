package whatsapp

import "testing"

func TestValidateResponder(t *testing.T) {
	tests := []struct {
		name     string
		userID   uint32
		senderID uint64
		wantErr  bool
	}{
		{name: "zero user", userID: 0, senderID: 5491100000, wantErr: true},
		{name: "zero sender", userID: 23, senderID: 0, wantErr: true},
		{name: "real phone exceeds uint32", userID: 23, senderID: 5491100000, wantErr: false},
		{name: "lid exceeds uint32", userID: 23, senderID: 33333344441231, wantErr: false},
		{name: "small sender", userID: 23, senderID: 42, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateResponder(tt.userID, tt.senderID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateResponder(%d, %d) error = %v, wantErr = %v", tt.userID, tt.senderID, err, tt.wantErr)
			}
		})
	}
}
