package devicestore

import (
	"bytes"
	"testing"
)

func TestIsSignalMACError(t *testing.T) {
	if !isSignalMACError("failed to decrypt: mismatching MAC in signal message") {
		t.Error("expected signal MAC error to be detected")
	}
	if isSignalMACError("some other error") {
		t.Error("did not expect unrelated message to be detected")
	}
	if isSignalMACError("mismatching MAC") {
		t.Error("signal message wording is required")
	}
}

func TestNormalizeKeyLength(t *testing.T) {
	cases := [][]byte{nil, []byte("abc"), make([]byte, 64)}
	for _, in := range cases {
		if got := normalizeKey(in, 32); len(got) != 32 {
			t.Errorf("normalizeKey(len=%d) returned %d bytes, want 32", len(in), len(got))
		}
	}
}

func TestNormalizeSignatureLength(t *testing.T) {
	cases := [][]byte{nil, []byte("abc"), make([]byte, 128)}
	for _, in := range cases {
		if got := normalizeSignature(in, 64); len(got) != 64 {
			t.Errorf("normalizeSignature(len=%d) returned %d bytes, want 64", len(in), len(got))
		}
	}
}

func TestNormalizeKeyKeepsExactLength(t *testing.T) {
	in := bytes.Repeat([]byte{7}, 32)
	got := normalizeKey(in, 32)
	if !bytes.Equal(got, in) {
		t.Error("exact-length key must be returned unchanged")
	}
}
