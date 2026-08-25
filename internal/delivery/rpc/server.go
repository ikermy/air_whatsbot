package rpc

import (
	"context"
	"net"
	"strings"

	"air_whatsbot/internal/whatsapp"

	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/purpshell/meowcaller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CallAPI interface {
	InitiateOutgoingCall(context.Context, uint32, string, ...whatsapp.CallEventHandler) (*meowcaller.Call, error)
	SubscribeCallEvents(context.Context, uint32, string, uint64) (<-chan whatsapp.CallEvent, error)
	HangupCall(uint32, string) error
}

type DB interface {
	GetActiveProvider(userID uint32) (comdom.ProviderType, error)
}

type Server struct {
	UnimplementedCallsServer
	api CallAPI
	db  DB
}

func NewServer(api CallAPI, db DB) *Server {
	return &Server{
		api: api,
		db:  db,
	}
}

func (s *Server) StartOutgoingCall(ctx context.Context, req *StartOutgoingCallRequest) (*StartOutgoingCallResponse, error) {
	logger.Debug("RPC StartOutgoingCall received")
	if req == nil || req.GetUserId() == 0 || strings.TrimSpace(req.GetTarget()) == "" {
		logger.Error("RPC StartOutgoingCall rejected: invalid user_id or target")
		return nil, status.Error(codes.InvalidArgument, "user_id and target are required")
	}
	logger.Error("RPC StartOutgoingCall: user_id=%d target=%s", req.GetUserId(), strings.TrimSpace(req.GetTarget()))
	// The start RPC is fire-and-return. Do not bind the call lifetime to the
	// short-lived request context; HangupCall or the bot lifecycle owns it.
	call, err := s.api.InitiateOutgoingCall(context.WithoutCancel(ctx), req.GetUserId(), strings.TrimSpace(req.GetTarget()))
	if err != nil {
		logger.Error("RPC StartOutgoingCall failed: %v", err)
		return nil, err
	}
	provider, err := s.db.GetActiveProvider(req.GetUserId())
	if err != nil {
		logger.Error("RPC StartOutgoingCall failed: %v", err)
	}

	logger.Debug("RPC StartOutgoingCall started: user_id=%d call_id=%s", req.GetUserId(), call.ID())
	return &StartOutgoingCallResponse{CallId: call.ID(), Status: "starting", AiProvider: provider.String()}, nil
}

func (s *Server) SubscribeCallEvents(req *SubscribeCallEventsRequest, stream grpc.ServerStreamingServer[CallEvent]) error {
	logger.Debug("RPC SubscribeCallEvents received")
	if req == nil || req.GetUserId() == 0 || strings.TrimSpace(req.GetCallId()) == "" {
		logger.Error("RPC SubscribeCallEvents rejected: invalid user_id or call_id")
		return status.Error(codes.InvalidArgument, "user_id and call_id are required")
	}
	logger.Debug("RPC SubscribeCallEvents: user_id=%d call_id=%s after_sequence=%d", req.GetUserId(), req.GetCallId(), req.GetAfterSequence())
	events, err := s.api.SubscribeCallEvents(stream.Context(), req.GetUserId(), strings.TrimSpace(req.GetCallId()), req.GetAfterSequence())
	if err != nil {
		logger.Error("RPC SubscribeCallEvents failed: %v", err)
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			logger.Debug("RPC SubscribeCallEvents context done: user_id=%d call_id=%s", req.GetUserId(), req.GetCallId())
			return stream.Context().Err()
		case event, ok := <-events:
			if !ok {
				logger.Debug("RPC SubscribeCallEvents completed: user_id=%d call_id=%s", req.GetUserId(), req.GetCallId())
				return nil
			}
			if event.Type == "token_usage" {
				logger.Debug("RPC SubscribeCallEvents ignored non-user event: call_id=%s sequence=%d raw_type=%s", event.CallID, event.Sequence, event.Type)
				continue
			}
			protoEvent := toProtoEvent(req.GetUserId(), event)
			logger.Debug("RPC SubscribeCallEvents event: call_id=%s sequence=%d raw_type=%s proto_type=%s delta=%q text=%q response_id=%s error=%q", event.CallID, event.Sequence, event.Type, protoEvent.GetType().String(), event.Delta, event.Text, event.ResponseID, protoEvent.GetError())
			if err := stream.Send(protoEvent); err != nil {
				logger.Debug("RPC SubscribeCallEvents send failed: call_id=%s: %v", req.GetCallId(), err)
				return err
			}
		}
	}
}

func (s *Server) HangupCall(_ context.Context, req *HangupCallRequest) (*HangupCallResponse, error) {
	logger.Debug("RPC HangupCall received")
	if req == nil || req.GetUserId() == 0 || strings.TrimSpace(req.GetCallId()) == "" {
		logger.Error("RPC HangupCall rejected: invalid user_id or call_id")
		return nil, status.Error(codes.InvalidArgument, "user_id and call_id are required")
	}
	logger.Debug("RPC HangupCall: user_id=%d call_id=%s", req.GetUserId(), req.GetCallId())
	if err := s.api.HangupCall(req.GetUserId(), strings.TrimSpace(req.GetCallId())); err != nil {
		logger.Error("RPC HangupCall failed: %v", err)
		return nil, err
	}
	logger.Debug("RPC HangupCall accepted: user_id=%d call_id=%s", req.GetUserId(), req.GetCallId())
	return &HangupCallResponse{CallId: req.GetCallId(), Status: "hangup_requested"}, nil
}

func toProtoEvent(_ uint32, event whatsapp.CallEvent) *CallEvent {
	result := &CallEvent{
		CallId:          event.CallID,
		Sequence:        event.Sequence,
		TimestampUnixMs: event.Timestamp.UnixMilli(),
		Provider:        CallProvider_CALL_PROVIDER_WHATSAPP,
		Type:            callEventType(event.Type, event.Text, event.Delta),
		Delta:           event.Delta,
		Text:            event.Text,
		ResponseId:      event.ResponseID,
	}
	if event.Err != nil {
		result.Error = event.Err.Error()
	}
	return result
}

func callEventType(eventType, text, delta string) CallEventType {
	if value, ok := map[string]CallEventType{
		"call_started":           CallEventType_CALL_STARTED,
		"realtime_starting":      CallEventType_REALTIME_STARTING,
		"realtime_started":       CallEventType_REALTIME_STARTED,
		"realtime_subscribed":    CallEventType_REALTIME_SUBSCRIBED,
		"audio_bridge_started":   CallEventType_AUDIO_BRIDGE_STARTED,
		"call_connected":         CallEventType_CALL_CONNECTED,
		"input_transcript_delta": CallEventType_INPUT_TRANSCRIPT_DELTA,
		"transcript_delta":       CallEventType_INPUT_TRANSCRIPT_DELTA,
		"input_transcript_done":  CallEventType_INPUT_TRANSCRIPT_DONE,
		"transcript":             CallEventType_INPUT_TRANSCRIPT_DONE,
		"response_started":       CallEventType_RESPONSE_STARTED,
		"response_text_delta":    CallEventType_RESPONSE_TEXT_DELTA,
		"response_done":          CallEventType_RESPONSE_DONE,
		"error":                  CallEventType_ERROR,
		"call_ended":             CallEventType_CALL_ENDED,
	}[eventType]; ok {
		return value
	}
	if eventType == "token_usage" {
		return CallEventType_CALL_EVENT_TYPE_UNSPECIFIED
	}
	// Compatibility fallback for air-common versions that emitted transcript
	// events without a stable event type. Never expose these as enum 0 when
	// the payload clearly identifies a text delta or a completed response.
	if text != "" || delta != "" {
		logger.Debug("Unknown realtime event type %q with payload, mapping to RESPONSE_TEXT_DELTA", eventType)
		return CallEventType_RESPONSE_TEXT_DELTA
	}
	logger.Debug("Unknown realtime event type %q without payload, mapping to RESPONSE_DONE", eventType)
	return CallEventType_RESPONSE_DONE
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		return err
	}
	logger.Debug("WhatsApp call server listening on :9090")
	logger.Info("WhatsApp call server started")
	grpcServer := grpc.NewServer()
	RegisterCallsServer(grpcServer, s)
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
	return grpcServer.Serve(listener)
}
