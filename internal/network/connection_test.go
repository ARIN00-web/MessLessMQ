package network

import (
	"errors"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func TestErrorCodeFromError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want uint16
	}{
		{
			name: "nil error",
			err:  nil,
			want: protocol.ErrorNone,
		},
		{
			name: "unknown API",
			err:  broker.ErrUnknownAPI,
			want: protocol.ErrorUnknownAPI,
		},
		{
			name: "internal error",
			err:  errors.New("something went wrong"),
			want: protocol.ErrorInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := errorCodeFromError(tt.err)

			if got != tt.want {
				t.Fatalf(
					"error code mismatch: got %d, want %d",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestErrorCodeFromWrappedUnknownAPI(t *testing.T) {
	err := errors.Join(
		errors.New("dispatcher failed"),
		broker.ErrUnknownAPI,
	)

	got := errorCodeFromError(err)

	if got != protocol.ErrorUnknownAPI {
		t.Fatalf(
			"error code mismatch: got %d, want %d",
			got,
			protocol.ErrorUnknownAPI,
		)
	}
}
