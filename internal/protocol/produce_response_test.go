package protocol

import (
	"reflect"
	"testing"
)

func TestProduceResponseRoundTrip(t *testing.T) {
	want := ProduceResponse{Records: []ProduceRecordResult{
		{Topic: "orders", PartitionID: 2, Offset: 17},
		{Topic: "payments", PartitionID: 0, Offset: 4},
	}}
	encoded, err := EncodeProduceResponse(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeProduceResponse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, want)
	}
}

func TestDecodeProduceResponseRejectsTrailingBytes(t *testing.T) {
	encoded, err := EncodeProduceResponse(ProduceResponse{Records: []ProduceRecordResult{{Topic: "orders"}}})
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, 0)
	if _, err := DecodeProduceResponse(encoded); err == nil {
		t.Fatal("expected trailing bytes to be rejected")
	}
}
