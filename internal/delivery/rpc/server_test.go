package rpc

import (
	"context"
	"testing"

	"air_whatsbot/internal/whatsapp"
	"github.com/purpshell/meowcaller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type validationCallAPI struct{}

func (validationCallAPI) InitiateOutgoingCall(context.Context, uint32, string, ...whatsapp.CallEventHandler) (*meowcaller.Call, error) {
	panic("must not be called")
}
func (validationCallAPI) SubscribeCallEvents(context.Context, uint32, string, uint64) (<-chan whatsapp.CallEvent, error) {
	panic("must not be called")
}
func (validationCallAPI) HangupCall(uint32, string) error { panic("must not be called") }

func TestStartOutgoingCallValidatesRequest(t *testing.T) {
	s := NewServer(validationCallAPI{}, nil)
	_, err := s.StartOutgoingCall(context.Background(), &StartOutgoingCallRequest{UserId: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func TestSubscribeCallEventsValidatesRequest(t *testing.T) {
	s := NewServer(validationCallAPI{}, nil)
	stream := &testEventStream{ctx: context.Background()}
	err := s.SubscribeCallEvents(&SubscribeCallEventsRequest{UserId: 1}, stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func TestHangupCallValidatesRequest(t *testing.T) {
	s := NewServer(validationCallAPI{}, nil)
	_, err := s.HangupCall(context.Background(), &HangupCallRequest{UserId: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

type testEventStream struct{ ctx context.Context }

func (s *testEventStream) SetHeader(metadata.MD) error  { return nil }
func (s *testEventStream) SendHeader(metadata.MD) error { return nil }
func (s *testEventStream) SetTrailer(metadata.MD)       {}
func (s *testEventStream) Context() context.Context     { return s.ctx }
func (s *testEventStream) Send(*CallEvent) error        { return nil }
func (s *testEventStream) SendMsg(any) error            { return nil }
func (s *testEventStream) RecvMsg(any) error            { return nil }
