package network

import (
	"errors"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/consumer"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestErrorCodeFromError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want uint16
	}{
		{name: "nil error", err: nil, want: protocol.ErrorNone},
		{name: "unknown API", err: broker.ErrUnknownAPI, want: protocol.ErrorUnknownAPI},
		{name: "unknown topic", err: topic.ErrTopicNotFound, want: protocol.ErrorUnknownTopic},
		{name: "partition not found", err: topic.ErrPartitionNotFound, want: protocol.ErrorPartitionNotFound},
		{name: "offset before start", err: consumer.ErrOffsetBeforeStart, want: protocol.ErrorOffsetBeforeStart},
		{name: "offset out of range", err: consumer.ErrOffsetOutOfRange, want: protocol.ErrorOffsetOutOfRange},
		{name: "invalid fetch limit", err: consumer.ErrInvalidFetchLimit, want: protocol.ErrorInvalidFetchLimit},
		{name: "unsupported version", err: protocol.ErrUnsupportedVersion, want: protocol.ErrorUnsupportedVersion},
		{name: "not partition leader", err: protocol.ErrNotPartitionLeader, want: protocol.ErrorNotPartitionLeader},
		{name: "insufficient ISR", err: protocol.ErrInsufficientISR, want: protocol.ErrorInsufficientISR},
		{name: "internal error", err: errors.New("something went wrong"), want: protocol.ErrorInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorCodeFromError(tt.err); got != tt.want {
				t.Fatalf("error code = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestErrorCodeFromWrappedUnknownAPI(t *testing.T) {
	err := errors.Join(errors.New("dispatcher failed"), broker.ErrUnknownAPI)
	if got := errorCodeFromError(err); got != protocol.ErrorUnknownAPI {
		t.Fatalf("error code = %d, want %d", got, protocol.ErrorUnknownAPI)
	}
}

func TestProducerErrorCodeMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want uint16
	}{
		{name: "invalid acknowledgement mode", err: protocol.ErrInvalidAckMode, want: protocol.ErrorInvalidAckMode},
		{name: "message too large", err: protocol.ErrMessageTooLarge, want: protocol.ErrorMessageTooLarge},
		{name: "invalid request", err: protocol.ErrInvalidRequest, want: protocol.ErrorInvalidRequest},
		{name: "unexpected error", err: errors.New("unexpected failure"), want: protocol.ErrorInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorCodeFromError(tt.err); got != tt.want {
				t.Fatalf("got error code %d, want %d", got, tt.want)
			}
		})
	}
}

func TestConsumeErrorCodeMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want uint16
	}{
		{name: "protocol invalid fetch limit", err: protocol.ErrInvalidFetchLimit, want: protocol.ErrorInvalidFetchLimit},
		{name: "consumer offset before start", err: consumer.ErrOffsetBeforeStart, want: protocol.ErrorOffsetBeforeStart},
		{name: "consumer offset out of range", err: consumer.ErrOffsetOutOfRange, want: protocol.ErrorOffsetOutOfRange},
		{name: "unsupported consume version", err: protocol.ErrUnsupportedVersion, want: protocol.ErrorUnsupportedVersion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorCodeFromError(tt.err); got != tt.want {
				t.Fatalf("got error code %d, want %d", got, tt.want)
			}
		})
	}
}
