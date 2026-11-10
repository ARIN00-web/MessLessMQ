package broker

import (
	"errors"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

type metadataTestHandler struct{}

func (metadataTestHandler) Handle(request protocol.Request) ([]byte, error) {
	if request.APIKey != protocol.APIMETADATA {
		return nil, protocol.ErrInvalidRequest
	}
	return []byte("metadata-response"), nil
}

func TestDispatcherMetadataHandler(t *testing.T) {
	dispatcher := NewDispatcher()
	request := protocol.Request{APIKey: protocol.APIMETADATA, APIVersion: protocol.MetadataAPIVersion}
	if _, err := dispatcher.Dispatch(request); !errors.Is(err, ErrHandlerUnavailable) {
		t.Fatalf("error = %v, want ErrHandlerUnavailable", err)
	}
	dispatcher.SetMetadataHandler(metadataTestHandler{})
	payload, err := dispatcher.Dispatch(request)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "metadata-response" {
		t.Fatalf("payload = %q", payload)
	}
}
