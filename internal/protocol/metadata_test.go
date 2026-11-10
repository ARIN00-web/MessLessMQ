package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestMetadataRequestRoundTrip(t *testing.T) {
	for _, topic := range []string{"", "orders", "topic-with-dash"} {
		encoded, err := EncodeMetadataRequest(MetadataRequest{Topic: topic})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeMetadataRequest(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Topic != topic {
			t.Fatalf("topic = %q, want %q", decoded.Topic, topic)
		}
	}
}

func TestMetadataResponseRoundTrip(t *testing.T) {
	want := MetadataResponse{
		Version:    7,
		Brokers:    []BrokerMetadata{{ID: "broker-a", Address: "127.0.0.1:9001"}, {ID: "broker-b", Address: "127.0.0.1:9002"}},
		Partitions: []PartitionMetadata{{Topic: "orders", PartitionID: 2, Replicas: []string{"broker-a", "broker-b"}, Leader: "broker-a", LeaderEpoch: 3}},
	}
	encoded, err := EncodeMetadataResponse(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMetadataResponse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != want.Version || len(got.Brokers) != 2 || len(got.Partitions) != 1 {
		t.Fatalf("unexpected decoded metadata: %+v", got)
	}
	if got.Partitions[0].Leader != "broker-a" || got.Partitions[0].PartitionID != 2 || !bytes.Equal([]byte(got.Partitions[0].Replicas[1]), []byte("broker-b")) {
		t.Fatalf("partition mismatch: %+v", got.Partitions[0])
	}
}

func TestMetadataDecodersRejectMalformedPayloads(t *testing.T) {
	if _, err := DecodeMetadataRequest([]byte{0}); !errors.Is(err, ErrInvalidMetadataPayload) {
		t.Fatalf("request error = %v", err)
	}
	valid, err := EncodeMetadataRequest(MetadataRequest{Topic: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeMetadataRequest(append(valid, 1)); !errors.Is(err, ErrInvalidMetadataPayload) {
		t.Fatalf("trailing request byte error = %v", err)
	}
	if _, err := DecodeMetadataResponse(make([]byte, 13)); !errors.Is(err, ErrInvalidMetadataPayload) {
		t.Fatalf("response error = %v", err)
	}
}

func TestMetadataResponseRejectsEmptyReplicaSet(t *testing.T) {
	_, err := EncodeMetadataResponse(MetadataResponse{Partitions: []PartitionMetadata{{Topic: "orders", Leader: "b1", LeaderEpoch: 1}}})
	if !errors.Is(err, ErrInvalidMetadataPayload) {
		t.Fatalf("error = %v, want ErrInvalidMetadataPayload", err)
	}
}

func TestMetadataResponseRejectsUnknownOrDuplicateReplicas(t *testing.T) {
	brokers := []BrokerMetadata{{ID: "b1", Address: "127.0.0.1:1"}}
	unknown := MetadataResponse{Brokers: brokers, Partitions: []PartitionMetadata{{Topic: "orders", Replicas: []string{"b1", "b2"}, Leader: "b1", LeaderEpoch: 1}}}
	if _, err := EncodeMetadataResponse(unknown); !errors.Is(err, ErrInvalidMetadataPayload) {
		t.Fatalf("unknown replica error = %v", err)
	}
	duplicate := MetadataResponse{Brokers: brokers, Partitions: []PartitionMetadata{{Topic: "orders", Replicas: []string{"b1", "b1"}, Leader: "b1", LeaderEpoch: 1}}}
	if _, err := EncodeMetadataResponse(duplicate); !errors.Is(err, ErrInvalidMetadataPayload) {
		t.Fatalf("duplicate replica error = %v", err)
	}
}
