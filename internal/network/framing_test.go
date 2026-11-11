package network

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

type shortWriter struct {
	buffer bytes.Buffer
	limit  int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		p = p[:w.limit]
	}
	return w.buffer.Write(p)
}

func TestFrameRoundTrip(t *testing.T) {
	want := []byte("messlessmq-frame")
	var buffer bytes.Buffer
	if err := WriteFrame(&buffer, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame mismatch: got %q want %q", got, want)
	}
}

func TestWriteFrameHandlesShortWrites(t *testing.T) {
	writer := &shortWriter{limit: 2}
	want := []byte("partial-writes-must-complete")
	if err := WriteFrame(writer, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&writer.buffer)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame mismatch: got %q want %q", got, want)
	}
}

func TestReadFrameRejectsInvalidLengths(t *testing.T) {
	for _, test := range []struct {
		name   string
		length uint32
		want   error
	}{
		{name: "empty", length: 0},
		{name: "too large", length: MaxFrameSize + 1, want: ErrFrameTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			var header [4]byte
			binary.BigEndian.PutUint32(header[:], test.length)
			_, err := ReadFrame(bytes.NewReader(header[:]))
			if err == nil {
				t.Fatal("expected invalid frame length to fail")
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
		})
	}
}

func TestReadFrameRejectsTruncatedPayload(t *testing.T) {
	var frame bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 5)
	frame.Write(header[:])
	frame.WriteString("abc")
	_, err := ReadFrame(&frame)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected unexpected EOF, got %v", err)
	}
}

func TestWriteFrameRejectsEmptyPayload(t *testing.T) {
	if err := WriteFrame(io.Discard, nil); err == nil {
		t.Fatal("expected empty frame to fail")
	}
}
