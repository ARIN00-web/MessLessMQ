package broker

import "testing"

func TestPartitionerRejectsZeroPartitions(t *testing.T) {
	_, err := NewPartitioner(0)
	if err != ErrInvalidPartitionCount {
		t.Fatalf("got error %v, want %v", err, ErrInvalidPartitionCount)
	}
}

func TestPartitionerSameKeyIsStable(t *testing.T) {
	p, err := NewPartitioner(4)
	if err != nil {
		t.Fatal(err)
	}

	first, err := p.Partition([]byte("order-1042"))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		got, err := p.Partition([]byte("order-1042"))
		if err != nil {
			t.Fatal(err)
		}

		if got != first {
			t.Fatalf("same key routed to different partitions: %d and %d",
				first, got)
		}
	}
}

func TestPartitionerRoundRobin(t *testing.T) {
	p, err := NewPartitioner(3)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint32{0, 1, 2, 0, 1, 2}

	for i, expected := range want {
		got, err := p.Partition(nil)
		if err != nil {
			t.Fatal(err)
		}

		if got != expected {
			t.Fatalf("call %d: got partition %d, want %d",
				i, got, expected)
		}
	}
}

func TestPartitionerEmptyKeyIsStable(t *testing.T) {
	p, err := NewPartitioner(3)
	if err != nil {
		t.Fatal(err)
	}

	first, err := p.Partition([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	second, err := p.Partition([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatalf("empty key routing changed: %d and %d", first, second)
	}
}
