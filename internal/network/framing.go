package network

import (
	"encoding/binary"
	"io"
)

func ReadFrame(r io.Reader) ([]byte, error) {
	var lengthBytes [4]byte

	_, err := io.ReadFull(r, lengthBytes[:])
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBytes[:])

	payload := make([]byte, length)

	_, err = io.ReadFull(r, payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func WriteFrame(w io.Writer, payload []byte) error {
	var lengthBytes [4]byte

	binary.BigEndian.PutUint32(lengthBytes[:], uint32(len(payload)))

	if _, err := w.Write(lengthBytes[:]); err != nil {
		return err
	}

	_, err := w.Write(payload)
	return err
}