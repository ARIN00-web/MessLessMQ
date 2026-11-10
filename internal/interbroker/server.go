package interbroker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type Handler interface {
	Handle(context.Context, Envelope) (Envelope, error)
}

type HandlerFunc func(context.Context, Envelope) (Envelope, error)

func (f HandlerFunc) Handle(ctx context.Context, request Envelope) (Envelope, error) {
	return f(ctx, request)
}

type Mux struct {
	mu       sync.RWMutex
	handlers map[MessageType]Handler
}

func NewMux() *Mux { return &Mux{handlers: make(map[MessageType]Handler)} }
func (m *Mux) Register(messageType MessageType, handler Handler) error {
	if m == nil || messageType == 0 || handler == nil {
		return ErrInvalidEnvelope
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.handlers == nil {
		m.handlers = make(map[MessageType]Handler)
	}
	if _, exists := m.handlers[messageType]; exists {
		return fmt.Errorf("message type %d already registered", messageType)
	}
	m.handlers[messageType] = handler
	return nil
}
func (m *Mux) Handle(ctx context.Context, request Envelope) (Envelope, error) {
	return m.Dispatch(ctx, request)
}

func (m *Mux) Dispatch(ctx context.Context, request Envelope) (Envelope, error) {
	if m == nil {
		return Envelope{}, ErrInvalidEnvelope
	}
	m.mu.RLock()
	handler := m.handlers[request.Type]
	m.mu.RUnlock()
	if handler == nil {
		return Envelope{}, fmt.Errorf("unsupported peer message type %d", request.Type)
	}
	return handler.Handle(ctx, request)
}

type Server struct {
	Handler      Handler
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	workers      sync.WaitGroup
}

func NewServer(handler Handler) (*Server, error) {
	if handler == nil {
		return nil, ErrInvalidEnvelope
	}
	return &Server{Handler: handler, ReadTimeout: DefaultTimeout, WriteTimeout: DefaultTimeout}, nil
}

// Serve accepts one framed request per connection. It returns nil when the
// listener is closed, and waits for accepted connections to finish.
func (s *Server) Serve(listener net.Listener) error {
	if s == nil || s.Handler == nil || listener == nil {
		return ErrInvalidEnvelope
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.workers.Wait()
			if isClosedNetworkError(err) {
				return nil
			}
			return err
		}
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			s.handleConnection(conn)
		}()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	readTimeout := s.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = DefaultTimeout
	}
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	request, err := readFrame(conn)
	if err != nil {
		return
	}
	if request.IsResponse {
		return
	}

	response, handlerErr := s.Handler.Handle(context.Background(), request)
	response.Version = ProtocolVersion
	response.Type = request.Type
	response.RequestID = request.RequestID
	response.SourceID = request.TargetID
	response.TargetID = request.SourceID
	response.IsResponse = true
	if handlerErr != nil {
		response.Error = handlerErr.Error()
		if len(response.Error) > maxPeerErrorLength {
			response.Error = response.Error[:maxPeerErrorLength]
		}
		response.Payload = nil
	}
	writeTimeout := s.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = DefaultTimeout
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_ = writeFrame(conn, response)
}

// ReplicationApplyFunc decodes and applies a received Phase 14 replication
// batch. The local broker ID is known by the application wiring and can be
// captured by the callback; sourceID is the sending broker identity.
type ReplicationApplyFunc func(ctx context.Context, sourceID string, payload []byte) ([]byte, error)

// ReplicationBatchHandler adapts the generic peer transport to a Phase 14
// replication manager without coupling the transport package to storage.
type ReplicationBatchHandler struct{ Apply ReplicationApplyFunc }

func NewReplicationBatchHandler(apply ReplicationApplyFunc) (*ReplicationBatchHandler, error) {
	if apply == nil {
		return nil, ErrInvalidEnvelope
	}
	return &ReplicationBatchHandler{Apply: apply}, nil
}

func (h *ReplicationBatchHandler) Handle(ctx context.Context, request Envelope) (Envelope, error) {
	if h == nil || h.Apply == nil || request.Type != MessageReplicationBatch || request.IsResponse {
		return Envelope{}, ErrInvalidEnvelope
	}
	payload, err := h.Apply(ctx, request.SourceID, append([]byte(nil), request.Payload...))
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Payload: payload}, nil
}
