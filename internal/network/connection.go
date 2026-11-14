package network

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/consumer"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/security"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

type RequestDispatcher interface {
	Dispatch(protocol.Request) ([]byte, error)
}

var errForbidden = errors.New("request forbidden")

// ConnectionAuthenticator keeps credentials out of every normal request and
// lets the transport maintain a principal for one TCP connection.
type ConnectionAuthenticator interface {
	AuthenticationRequired() bool
	AuthenticateConnection(string) (security.Principal, error)
	AuthorizeConnection(security.Principal, protocol.Request) error
}

func HandleConnection(conn net.Conn, dispatcher RequestDispatcher, shutdown <-chan struct{}) {
	HandleConnectionWithTimeouts(conn, dispatcher, shutdown, ReadTimeout, WriteTimeout)
}

func HandleConnectionWithTimeouts(conn net.Conn, dispatcher RequestDispatcher, shutdown <-chan struct{}, readTimeout, writeTimeout time.Duration) {
	defer conn.Close()
	if readTimeout <= 0 {
		readTimeout = ReadTimeout
	}
	if writeTimeout <= 0 {
		writeTimeout = WriteTimeout
	}
	for {
		select {
		case <-shutdown:
			return
		default:
		}
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return
		}
		var principal security.Principal
		if err := handleRequest(conn, dispatcher, writeTimeout, &principal); err != nil {
			if errors.Is(err, io.EOF) || isTimeoutError(err) {
				return
			}
			return
		}
	}
}

func handleRequest(conn net.Conn, dispatcher RequestDispatcher, writeTimeout time.Duration, principal *security.Principal) error {
	frame, err := ReadFrame(conn)
	if err != nil {
		return err
	}
	request, err := protocol.DecodeRequest(frame)
	if err != nil {
		return err
	}
	var responsePayload []byte
	var dispatchErr error
	if authenticator, ok := dispatcher.(ConnectionAuthenticator); ok {
		if request.APIKey == protocol.APIAUTH {
			if request.APIVersion != protocol.AuthAPIVersion {
				dispatchErr = protocol.ErrUnsupportedVersion
			} else {
				var auth protocol.AuthRequest
				auth, dispatchErr = protocol.DecodeAuthRequest(request.Payload)
				if dispatchErr == nil {
					*principal, dispatchErr = authenticator.AuthenticateConnection(auth.Token)
				}
				if dispatchErr == nil {
					responsePayload, dispatchErr = protocol.EncodeAuthResponse(protocol.AuthResponse{PrincipalID: principal.ID})
				}
			}
		} else if authenticator.AuthenticationRequired() && principal.ID == "" {
			dispatchErr = security.ErrInvalidCredential
		} else if principal.ID != "" {
			dispatchErr = authenticator.AuthorizeConnection(*principal, request)
			if dispatchErr == nil {
				responsePayload, dispatchErr = dispatcher.Dispatch(request)
			}
		} else {
			responsePayload, dispatchErr = dispatcher.Dispatch(request)
		}
	} else {
		responsePayload, dispatchErr = dispatcher.Dispatch(request)
	}
	response := protocol.Response{
		APIKey: request.APIKey, APIVersion: request.APIVersion,
		CorrelationID: request.CorrelationID,
		ErrorCode:     errorCodeFromError(dispatchErr), Payload: responsePayload,
	}
	if dispatchErr != nil {
		response.Payload = []byte("request failed")
	}
	responseFrame, err := protocol.EncodeResponse(response)
	if err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return WriteFrame(conn, responseFrame)
}

func errorCodeFromError(err error) uint16 {
	if err == nil {
		return protocol.ErrorNone
	}
	if errors.Is(err, broker.ErrUnknownAPI) {
		return protocol.ErrorUnknownAPI
	}
	if errors.Is(err, protocol.ErrInvalidAckMode) {
		return protocol.ErrorInvalidAckMode
	}
	if errors.Is(err, protocol.ErrMessageTooLarge) {
		return protocol.ErrorMessageTooLarge
	}
	if errors.Is(err, protocol.ErrInvalidRequest) {
		return protocol.ErrorInvalidRequest
	}
	if errors.Is(err, protocol.ErrInvalidFetchLimit) || errors.Is(err, consumer.ErrInvalidFetchLimit) {
		return protocol.ErrorInvalidFetchLimit
	}
	if errors.Is(err, protocol.ErrUnsupportedVersion) {
		return protocol.ErrorUnsupportedVersion
	}
	if errors.Is(err, protocol.ErrRateLimited) {
		return protocol.ErrorRateLimited
	}
	if errors.Is(err, protocol.ErrNotPartitionLeader) {
		return protocol.ErrorNotPartitionLeader
	}
	if errors.Is(err, protocol.ErrInsufficientISR) {
		return protocol.ErrorInsufficientISR
	}
	if errors.Is(err, protocol.ErrCommitUncertain) {
		return protocol.ErrorCommitUncertain
	}
	if errors.Is(err, protocol.ErrClusterMetadataUnavailable) {
		return protocol.ErrorClusterMetadataUnavailable
	}
	if errors.Is(err, protocol.ErrControllerAuthorityUnavailable) {
		return protocol.ErrorControllerAuthorityUnavailable
	}
	if errors.Is(err, security.ErrInvalidCredential) || errors.Is(err, security.ErrExpiredCredential) {
		return protocol.ErrorUnauthorized
	}
	if errors.Is(err, errForbidden) || errors.Is(err, security.ErrForbidden) {
		return protocol.ErrorForbidden
	}
	if errors.Is(err, topic.ErrTopicNotFound) {
		return protocol.ErrorUnknownTopic
	}
	if errors.Is(err, topic.ErrPartitionNotFound) {
		return protocol.ErrorPartitionNotFound
	}
	if errors.Is(err, consumer.ErrOffsetBeforeStart) {
		return protocol.ErrorOffsetBeforeStart
	}
	if errors.Is(err, consumer.ErrOffsetOutOfRange) {
		return protocol.ErrorOffsetOutOfRange
	}
	return protocol.ErrorInternal
}

func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
