package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const MaxFrameSize uint32 = 12 * 1024 * 1024

var ErrFrameTooLarge = errors.New("network frame exceeds maximum size")

func ReadFrame(r io.Reader) ([]byte, error) {
	var lengthBytes [4]byte
	if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBytes[:])
	if length == 0 {
		return nil, errors.New("network frame is empty")
	}
	if length > MaxFrameSize {
		return nil, fmt.Errorf("%w: declared=%d maximum=%d", ErrFrameTooLarge, length, MaxFrameSize)
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) == 0 {
		return errors.New("network frame is empty")
	}
	if uint64(len(payload)) > uint64(MaxFrameSize) {
		return ErrFrameTooLarge
	}
	var lengthBytes [4]byte
	binary.BigEndian.PutUint32(lengthBytes[:], uint32(len(payload)))
	if err := writeAll(w, lengthBytes[:]); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
