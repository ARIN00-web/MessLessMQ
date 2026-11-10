package interbroker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

func startPeerServer(t *testing.T, handler Handler) string {
	t.Helper()
	server, err := NewServer(handler)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server Serve() error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("peer server did not stop")
		}
	})
	return listener.Addr().String()
}

func TestTCPTransportRoundTrip(t *testing.T) {
	address := startPeerServer(t, HandlerFunc(func(_ context.Context, request Envelope) (Envelope, error) {
		return Envelope{Payload: append([]byte("ack:"), request.Payload...)}, nil
	}))
	transport := NewTransport(time.Second)
	response, err := transport.RoundTrip(context.Background(), address, Envelope{
		Type: MessageHeartbeat, RequestID: 12, SourceID: "broker-a", TargetID: "broker-b", Payload: []byte("ping"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != "ack:ping" || !response.IsResponse {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestTCPTransportReturnsRemoteHandlerError(t *testing.T) {
	address := startPeerServer(t, HandlerFunc(func(context.Context, Envelope) (Envelope, error) {
		return Envelope{}, errors.New("replica rejected stale epoch")
	}))
	_, err := NewTransport(time.Second).RoundTrip(context.Background(), address, Envelope{
		Type: MessageReplicationBatch, RequestID: 13, SourceID: "leader", TargetID: "follower",
	})
	var remote *RemoteError
	if !errors.As(err, &remote) {
		t.Fatalf("error = %v, want RemoteError", err)
	}
	if remote.Message != "replica rejected stale epoch" {
		t.Fatalf("remote message = %q", remote.Message)
	}
}

func TestControllerVoteRequestOverTCP(t *testing.T) {
	cluster, err := controller.NewCluster("controller-a", "controller-b", "controller-c")
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := cluster.Node("controller-a")
	if err != nil {
		t.Fatal(err)
	}
	voter, err := cluster.Node("controller-b")
	if err != nil {
		t.Fatal(err)
	}
	voteRequest, err := candidate.BeginElection()
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewControllerHandler(voter)
	if err != nil {
		t.Fatal(err)
	}
	address := startPeerServer(t, handler)
	payload, err := json.Marshal(voteRequest)
	if err != nil {
		t.Fatal(err)
	}
	response, err := NewTransport(time.Second).RoundTrip(context.Background(), address, Envelope{
		Type: MessageVoteRequest, RequestID: 99, SourceID: "controller-a", TargetID: "controller-b", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var voteResponse controller.VoteResponse
	if err := json.Unmarshal(response.Payload, &voteResponse); err != nil {
		t.Fatal(err)
	}
	if !voteResponse.VoteGranted || voteResponse.Term != voteRequest.Term {
		t.Fatalf("vote response = %+v", voteResponse)
	}
}

func TestControllerHandlerChecksPeerIdentity(t *testing.T) {
	cluster, err := controller.NewCluster("controller-a", "controller-b")
	if err != nil {
		t.Fatal(err)
	}
	voter, _ := cluster.Node("controller-b")
	handler, _ := NewControllerHandler(voter)
	payload, _ := json.Marshal(controller.VoteRequest{Term: 1, CandidateID: "controller-a"})
	_, err = handler.Handle(context.Background(), Envelope{Type: MessageVoteRequest, RequestID: 1, SourceID: "spoofed", TargetID: "controller-b", Payload: payload})
	if err == nil {
		t.Fatal("expected identity mismatch error")
	}
}

func TestMuxRoutesAndRejectsDuplicateHandlers(t *testing.T) {
	mux := NewMux()
	handler := HandlerFunc(func(context.Context, Envelope) (Envelope, error) { return Envelope{Payload: []byte("pong")}, nil })
	if err := mux.Register(MessageHeartbeat, handler); err != nil {
		t.Fatal(err)
	}
	if err := mux.Register(MessageHeartbeat, handler); err == nil {
		t.Fatal("expected duplicate registration error")
	}
	response, err := mux.Dispatch(context.Background(), Envelope{Type: MessageHeartbeat})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != "pong" {
		t.Fatalf("payload = %q", response.Payload)
	}
	if _, err := mux.Dispatch(context.Background(), Envelope{Type: MessageReplicationBatch}); err == nil {
		t.Fatal("expected unregistered message type error")
	}
}

func TestReplicationBatchHandlerPassesPayloadToApplier(t *testing.T) {
	var gotSource string
	var gotPayload []byte
	handler, err := NewReplicationBatchHandler(func(_ context.Context, source string, payload []byte) ([]byte, error) {
		gotSource = source
		gotPayload = append([]byte(nil), payload...)
		return []byte("applied"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := handler.Handle(context.Background(), Envelope{
		Type: MessageReplicationBatch, RequestID: 1, SourceID: "leader-broker", TargetID: "follower-broker", Payload: []byte("encoded-batch"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotSource != "leader-broker" || string(gotPayload) != "encoded-batch" || string(response.Payload) != "applied" {
		t.Fatalf("source=%q payload=%q response=%q", gotSource, gotPayload, response.Payload)
	}
}

func TestControllerAppendEntriesOverTCP(t *testing.T) {
	cluster, err := controller.NewCluster("controller-a", "controller-b", "controller-c")
	if err != nil {
		t.Fatal(err)
	}
	leader, _ := cluster.Node("controller-a")
	follower, _ := cluster.Node("controller-b")
	request := controller.AppendEntriesRequest{
		Term: 1, LeaderID: "controller-a",
		Entries:           []controller.LogEntry{{Index: 1, Term: 1, Command: controller.Command{Type: controller.CommandRegisterBroker, Broker: controller.Broker{ID: "broker-1", Address: "127.0.0.1:9001"}}}},
		LeaderCommitIndex: 1,
	}
	_ = leader
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewControllerHandler(follower)
	if err != nil {
		t.Fatal(err)
	}
	address := startPeerServer(t, handler)
	response, err := NewTransport(time.Second).RoundTrip(context.Background(), address, Envelope{
		Type: MessageAppendEntries, RequestID: 100, SourceID: "controller-a", TargetID: "controller-b", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var appendResponse controller.AppendEntriesResponse
	if err := json.Unmarshal(response.Payload, &appendResponse); err != nil {
		t.Fatal(err)
	}
	if !appendResponse.Success || appendResponse.Term != 1 {
		t.Fatalf("append response = %+v", appendResponse)
	}
	metadata := follower.Metadata()
	if _, exists := metadata.Brokers["broker-1"]; !exists {
		t.Fatal("committed broker metadata was not applied")
	}
}

func TestMuxCanServeMultiplePeerMessageTypes(t *testing.T) {
	mux := NewMux()
	if err := mux.Register(MessageHeartbeat, HandlerFunc(func(context.Context, Envelope) (Envelope, error) {
		return Envelope{Payload: []byte("heartbeat-ack")}, nil
	})); err != nil {
		t.Fatal(err)
	}
	address := startPeerServer(t, mux)
	response, err := NewTransport(time.Second).RoundTrip(context.Background(), address, Envelope{
		Type: MessageHeartbeat, RequestID: 777, SourceID: "broker-a", TargetID: "broker-b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(response.Payload) != "heartbeat-ack" {
		t.Fatalf("payload = %q", response.Payload)
	}
}
