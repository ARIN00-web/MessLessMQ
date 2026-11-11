package protocol

import (
	"errors"
	"testing"
)

func TestConsumeRequestRoundTrip(t *testing.T) {
	want := ConsumeRequest{
		Topic:       "orders",
		PartitionID: 3,
		Offset:      42,
		MaxMessages: 25,
	}

	encoded, err := EncodeConsumeRequest(want)
	if err != nil {
		t.Fatalf("EncodeConsumeRequest() error: %v", err)
	}

	got, err := DecodeConsumeRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeConsumeRequest() error: %v", err)
	}
	if got != want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestConsumeRequestRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		req  ConsumeRequest
		want error
	}{
		{name: "empty topic", req: ConsumeRequest{MaxMessages: 1}, want: ErrInvalidRequest},
		{name: "invalid topic character", req: ConsumeRequest{Topic: "orders/one", MaxMessages: 1}, want: ErrInvalidRequest},
		{name: "zero max", req: ConsumeRequest{Topic: "orders", MaxMessages: 0}, want: ErrInvalidFetchLimit},
		{name: "max too large", req: ConsumeRequest{Topic: "orders", MaxMessages: MaxConsumeMessages + 1}, want: ErrInvalidFetchLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EncodeConsumeRequest(tt.req)
			if !errors.Is(err, tt.want) {
				t.Fatalf("EncodeConsumeRequest() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDecodeConsumeRequestRejectsMalformedPayloads(t *testing.T) {
	valid, err := EncodeConsumeRequest(ConsumeRequest{
		Topic: "orders", PartitionID: 1, Offset: 2, MaxMessages: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "short length", data: []byte{0}},
		{name: "zero topic length", data: append([]byte{0, 0}, valid[2:]...)},
		{name: "truncated", data: valid[:len(valid)-1]},
		{name: "trailing bytes", data: append(append([]byte(nil), valid...), 0)},
		{name: "invalid topic", data: func() []byte { b := append([]byte(nil), valid...); b[2] = '/'; return b }()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DecodeConsumeRequest(tt.data); err == nil {
				t.Fatal("DecodeConsumeRequest() succeeded; want error")
			}
		})
	}
}

func TestDecodeConsumeRequestRejectsInvalidMaxMessages(t *testing.T) {
	data, err := EncodeConsumeRequest(ConsumeRequest{
		Topic: "orders", PartitionID: 0, Offset: 0, MaxMessages: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// MaxMessages is the final 4-byte field.
	data[len(data)-4] = 0
	data[len(data)-3] = 0
	data[len(data)-2] = 0
	data[len(data)-1] = 0

	_, err = DecodeConsumeRequest(data)
	if !errors.Is(err, ErrInvalidFetchLimit) {
		t.Fatalf("DecodeConsumeRequest() error = %v, want ErrInvalidFetchLimit", err)
	}
}

func TestConsumeResponseRoundTrip(t *testing.T) {
	want := ConsumeResponse{
		StartOffset:  4,
		LogEndOffset: 8,
		NextOffset:   6,
		Records: []ConsumeRecord{
			{Offset: 4, Timestamp: 100, Value: []byte("first")},
			{Offset: 5, Timestamp: 200, Value: []byte("second")},
		},
	}

	encoded, err := EncodeConsumeResponse(want)
	if err != nil {
		t.Fatalf("EncodeConsumeResponse() error: %v", err)
	}
	got, err := DecodeConsumeResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeConsumeResponse() error: %v", err)
	}
	if got.StartOffset != want.StartOffset || got.LogEndOffset != want.LogEndOffset || got.NextOffset != want.NextOffset {
		t.Fatalf("response offsets = %+v, want %+v", got, want)
	}
	if len(got.Records) != len(want.Records) {
		t.Fatalf("record count = %d, want %d", len(got.Records), len(want.Records))
	}
	for i := range want.Records {
		if got.Records[i].Offset != want.Records[i].Offset || got.Records[i].Timestamp != want.Records[i].Timestamp || string(got.Records[i].Value) != string(want.Records[i].Value) {
			t.Errorf("record %d = %+v, want %+v", i, got.Records[i], want.Records[i])
		}
	}
}

func TestConsumeResponseAllowsEmptyFetch(t *testing.T) {
	want := ConsumeResponse{StartOffset: 2, LogEndOffset: 5, NextOffset: 5}
	encoded, err := EncodeConsumeResponse(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeConsumeResponse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 0 || got.NextOffset != 5 {
		t.Fatalf("decoded empty response = %+v", got)
	}
}

func TestConsumeResponseRejectsInvalidOffsets(t *testing.T) {
	tests := []struct {
		name string
		resp ConsumeResponse
	}{
		{name: "start beyond end", resp: ConsumeResponse{StartOffset: 3, LogEndOffset: 2, NextOffset: 2}},
		{name: "next before start", resp: ConsumeResponse{StartOffset: 3, LogEndOffset: 5, NextOffset: 2}},
		{name: "next after end", resp: ConsumeResponse{StartOffset: 0, LogEndOffset: 5, NextOffset: 6}},
		{name: "record before start", resp: ConsumeResponse{StartOffset: 2, LogEndOffset: 4, NextOffset: 3, Records: []ConsumeRecord{{Offset: 1}}}},
		{name: "record at end", resp: ConsumeResponse{StartOffset: 0, LogEndOffset: 4, NextOffset: 5, Records: []ConsumeRecord{{Offset: 4}}}},
		{name: "unordered records", resp: ConsumeResponse{StartOffset: 0, LogEndOffset: 5, NextOffset: 3, Records: []ConsumeRecord{{Offset: 2}, {Offset: 1}}}},
		{name: "next does not follow records", resp: ConsumeResponse{StartOffset: 0, LogEndOffset: 5, NextOffset: 2, Records: []ConsumeRecord{{Offset: 2}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := EncodeConsumeResponse(tt.resp); err == nil {
				t.Fatal("EncodeConsumeResponse() succeeded; want error")
			}
		})
	}
}

func TestDecodeConsumeResponseRejectsTruncatedRecord(t *testing.T) {
	encoded, err := EncodeConsumeResponse(ConsumeResponse{
		StartOffset: 0, LogEndOffset: 1, NextOffset: 1,
		Records: []ConsumeRecord{{Offset: 0, Timestamp: 10, Value: []byte("payload")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = DecodeConsumeResponse(encoded[:len(encoded)-1])
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("DecodeConsumeResponse() error = %v, want ErrInvalidRequest", err)
	}
}

func TestDecodeConsumeResponseRejectsTrailingBytes(t *testing.T) {
	encoded, err := EncodeConsumeResponse(ConsumeResponse{
		StartOffset: 0, LogEndOffset: 0, NextOffset: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, 1)

	_, err = DecodeConsumeResponse(encoded)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("DecodeConsumeResponse() error = %v, want ErrInvalidRequest", err)
	}
}

func TestConsumeResponseV2RoundTripPreservesKeys(t *testing.T) {
	original := ConsumeResponse{StartOffset: 4, LogEndOffset: 6, NextOffset: 6, Records: []ConsumeRecord{
		{Offset: 4, Timestamp: 100, Key: nil, Value: []byte("nil-key")},
		{Offset: 5, Timestamp: 101, Key: []byte("customer-7"), Value: []byte("keyed")},
	}}
	encoded, err := EncodeConsumeResponseV2(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeConsumeResponseV2(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 2 || decoded.Records[0].Key != nil || string(decoded.Records[1].Key) != "customer-7" || string(decoded.Records[1].Value) != "keyed" {
		t.Fatalf("unexpected V2 response: %+v", decoded)
	}
}
