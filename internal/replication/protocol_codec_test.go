package replication

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestReplicationBatchCodecRoundTrip(t *testing.T) {
	original := ReplicationBatch{
		Version: ProtocolVersion, Topic: "orders", PartitionID: 3,
		LeaderID: "broker-1", LeaderEpoch: 7,
		Records: []Record{
			{Offset: 10, Timestamp: 1000, Value: []byte("created")},
			{Offset: 11, Timestamp: 2000, Value: []byte("paid")},
		},
	}
	encoded, err := EncodeReplicationBatch(original)
	if err != nil {
		t.Fatalf("EncodeReplicationBatch() error: %v", err)
	}
	decoded, err := DecodeReplicationBatch(encoded)
	if err != nil {
		t.Fatalf("DecodeReplicationBatch() error: %v", err)
	}
	if decoded.Version != original.Version || decoded.Topic != original.Topic ||
		decoded.PartitionID != original.PartitionID || decoded.LeaderID != original.LeaderID ||
		decoded.LeaderEpoch != original.LeaderEpoch || len(decoded.Records) != len(original.Records) {
		t.Fatalf("decoded header differs: %+v", decoded)
	}
	for i, want := range original.Records {
		got := decoded.Records[i]
		if got.Offset != want.Offset || got.Timestamp != want.Timestamp || string(got.Value) != string(want.Value) {
			t.Errorf("record %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestReplicationBatchCodecSupportsEmptyBatch(t *testing.T) {
	batch := ReplicationBatch{
		Version: ProtocolVersion, Topic: "orders", LeaderID: "broker-1", LeaderEpoch: 1,
	}
	encoded, err := EncodeReplicationBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeReplicationBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 0 {
		t.Fatalf("record count = %d, want 0", len(decoded.Records))
	}
}

func TestReplicationBatchCodecRejectsInvalidData(t *testing.T) {
	tests := []struct {
		name  string
		batch ReplicationBatch
	}{
		{
			name:  "invalid topic",
			batch: ReplicationBatch{Version: ProtocolVersion, Topic: "orders/payments", LeaderID: "broker-1"},
		},
		{
			name:  "invalid leader ID",
			batch: ReplicationBatch{Version: ProtocolVersion, Topic: "orders", LeaderID: "bad id"},
		},
		{
			name: "non-contiguous offsets",
			batch: ReplicationBatch{
				Version: ProtocolVersion, Topic: "orders", LeaderID: "broker-1",
				Records: []Record{{Offset: 1}, {Offset: 3}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := EncodeReplicationBatch(tt.batch); !errors.Is(err, ErrInvalidReplicationBatch) {
				t.Fatalf("EncodeReplicationBatch() error = %v, want ErrInvalidReplicationBatch", err)
			}
		})
	}
}

func TestReplicationBatchCodecRejectsTruncationAndTrailingBytes(t *testing.T) {
	encoded, err := EncodeReplicationBatch(ReplicationBatch{
		Version: ProtocolVersion, Topic: "orders", LeaderID: "broker-1", LeaderEpoch: 1,
		Records: []Record{{Offset: 0, Timestamp: 5, Value: []byte("value")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeReplicationBatch(encoded[:len(encoded)-1]); !errors.Is(err, ErrInvalidReplicationBatch) {
		t.Fatalf("truncated decode error = %v", err)
	}
	withTrailing := append(append([]byte(nil), encoded...), 0xff)
	if _, err := DecodeReplicationBatch(withTrailing); !errors.Is(err, ErrInvalidReplicationBatch) {
		t.Fatalf("trailing byte decode error = %v", err)
	}
}

func TestReplicationBatchCodecRejectsUnsupportedVersion(t *testing.T) {
	encoded, err := EncodeReplicationBatch(ReplicationBatch{
		Version: ProtocolVersion, Topic: "orders", LeaderID: "broker-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint16(encoded[:2], ProtocolVersion+1)
	if _, err := DecodeReplicationBatch(encoded); !errors.Is(err, ErrInvalidReplicationBatch) {
		t.Fatalf("error = %v, want ErrInvalidReplicationBatch", err)
	}
}

func TestValidateReplicationBatchRejectsTooManyRecords(t *testing.T) {
	batch := ReplicationBatch{
		Version: ProtocolVersion, Topic: "orders", LeaderID: "broker-1",
		Records: make([]Record, MaxReplicationRecords+1),
	}
	if _, err := EncodeReplicationBatch(batch); !errors.Is(err, ErrInvalidReplicationBatch) {
		t.Fatalf("error = %v, want ErrInvalidReplicationBatch", err)
	}
}
