// Package recordmeta defines an internal, versioned envelope stored inside
// partition-log payloads. Consumers unwrap it before exposing application data.
package recordmeta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	version             byte = 1
	fixedHeaderSize          = 8 + 1 + 2 + 8 + 8 + 4
	checksumSize             = 4
	MaxProducerIDLength      = 128
	MaxPayloadSize           = 10 * 1024 * 1024
)

var (
	magic                = [8]byte{'M', 'L', 'M', 'Q', 'I', 'D', 'M', '1'}
	ErrInvalidProducerID = errors.New("invalid producer ID")
	ErrInvalidEnvelope   = errors.New("invalid producer record envelope")
	ErrPayloadTooLarge   = errors.New("producer payload too large")
)

type ProducerMetadata struct {
	ProducerID string
	Epoch      uint64
	Sequence   uint64
}

// Encode wraps application payload and producer identity/sequence metadata in
// one storage record. Keeping metadata and payload in the same append-only log
// record allows recovery to rebuild deduplication state without a dual-write
// race between a message log and a separate producer-state log.
func Encode(meta ProducerMetadata, payload []byte) ([]byte, error) {
	if !validProducerID(meta.ProducerID) {
		return nil, ErrInvalidProducerID
	}
	if len(payload) > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}
	if len(payload) > int(^uint32(0)) {
		return nil, ErrPayloadTooLarge
	}

	total := fixedHeaderSize + len(meta.ProducerID) + len(payload) + checksumSize
	out := make([]byte, total)
	copy(out[:8], magic[:])
	out[8] = version
	binary.BigEndian.PutUint16(out[9:11], uint16(len(meta.ProducerID)))
	binary.BigEndian.PutUint64(out[11:19], meta.Epoch)
	binary.BigEndian.PutUint64(out[19:27], meta.Sequence)
	binary.BigEndian.PutUint32(out[27:31], uint32(len(payload)))
	cursor := fixedHeaderSize
	copy(out[cursor:], meta.ProducerID)
	cursor += len(meta.ProducerID)
	copy(out[cursor:], payload)
	cursor += len(payload)
	binary.BigEndian.PutUint32(out[cursor:], crc32.ChecksumIEEE(out[:cursor]))
	return out, nil
}

// Decode returns the producer metadata and original application payload.
// If data is not an internal producer envelope, isEnvelope is false and the
// original bytes are returned unchanged. A payload beginning with the exact
// magic but containing a malformed envelope is rejected as corruption.
func Decode(data []byte) (meta ProducerMetadata, payload []byte, isEnvelope bool, err error) {
	if len(data) < len(magic) || string(data[:len(magic)]) != string(magic[:]) {
		return ProducerMetadata{}, append([]byte(nil), data...), false, nil
	}
	if len(data) < fixedHeaderSize+checksumSize {
		return ProducerMetadata{}, nil, true, ErrInvalidEnvelope
	}
	if data[8] != version {
		return ProducerMetadata{}, nil, true, fmt.Errorf("%w: unsupported version %d", ErrInvalidEnvelope, data[8])
	}

	producerIDLength := int(binary.BigEndian.Uint16(data[9:11]))
	payloadLength := uint64(binary.BigEndian.Uint32(data[27:31]))
	if producerIDLength == 0 || producerIDLength > MaxProducerIDLength || payloadLength > MaxPayloadSize {
		return ProducerMetadata{}, nil, true, ErrInvalidEnvelope
	}

	expected := uint64(fixedHeaderSize) + uint64(producerIDLength) + payloadLength + checksumSize
	if expected != uint64(len(data)) {
		return ProducerMetadata{}, nil, true, ErrInvalidEnvelope
	}

	checksumOffset := len(data) - checksumSize
	wantCRC := binary.BigEndian.Uint32(data[checksumOffset:])
	gotCRC := crc32.ChecksumIEEE(data[:checksumOffset])
	if wantCRC != gotCRC {
		return ProducerMetadata{}, nil, true, fmt.Errorf("%w: checksum mismatch", ErrInvalidEnvelope)
	}

	cursor := fixedHeaderSize
	producerID := string(data[cursor : cursor+producerIDLength])
	if !validProducerID(producerID) {
		return ProducerMetadata{}, nil, true, ErrInvalidEnvelope
	}
	cursor += producerIDLength
	value := append([]byte(nil), data[cursor:checksumOffset]...)

	return ProducerMetadata{
		ProducerID: producerID,
		Epoch:      binary.BigEndian.Uint64(data[11:19]),
		Sequence:   binary.BigEndian.Uint64(data[19:27]),
	}, value, true, nil
}

func validProducerID(id string) bool {
	if len(id) == 0 || len(id) > MaxProducerIDLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !((c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

var messageMagic = [8]byte{'M', 'L', 'M', 'Q', 'M', 'S', 'G', '1'}

const messageHeaderSize = 8 + 1 + 4 + 4

// EncodeMessage stores a message key beside its value in one checksummed log
// payload. A nil key is distinct from an empty-but-present key.
func EncodeMessage(key, value []byte) ([]byte, error) {
	if len(key) > MaxPayloadSize || len(value) > MaxPayloadSize || len(key)+len(value) > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}
	keyLength := uint32(len(key))
	if key == nil {
		keyLength = ^uint32(0)
	}
	total := messageHeaderSize + len(key) + len(value) + checksumSize
	out := make([]byte, total)
	copy(out[:8], messageMagic[:])
	out[8] = version
	binary.BigEndian.PutUint32(out[9:13], keyLength)
	binary.BigEndian.PutUint32(out[13:17], uint32(len(value)))
	cursor := messageHeaderSize
	copy(out[cursor:], key)
	cursor += len(key)
	copy(out[cursor:], value)
	cursor += len(value)
	binary.BigEndian.PutUint32(out[cursor:], crc32.ChecksumIEEE(out[:cursor]))
	return out, nil
}

// DecodeMessage unwraps the stored key/value envelope. isEnvelope is false for
// records written before key persistence was introduced; those bytes are
// returned as the original value with a nil key.
func DecodeMessage(data []byte) (key, value []byte, isEnvelope bool, err error) {
	if len(data) < len(messageMagic) || string(data[:len(messageMagic)]) != string(messageMagic[:]) {
		return nil, append([]byte(nil), data...), false, nil
	}
	if len(data) < messageHeaderSize+checksumSize || data[8] != version {
		return nil, nil, true, ErrInvalidEnvelope
	}
	keyLength := binary.BigEndian.Uint32(data[9:13])
	valueLength := binary.BigEndian.Uint32(data[13:17])
	actualKeyLength := uint64(keyLength)
	if keyLength == ^uint32(0) {
		actualKeyLength = 0
	}
	if actualKeyLength > MaxPayloadSize || uint64(valueLength) > MaxPayloadSize || actualKeyLength+uint64(valueLength) > MaxPayloadSize {
		return nil, nil, true, ErrInvalidEnvelope
	}
	expected := uint64(messageHeaderSize) + actualKeyLength + uint64(valueLength) + checksumSize
	if expected != uint64(len(data)) {
		return nil, nil, true, ErrInvalidEnvelope
	}
	checksumOffset := len(data) - checksumSize
	if binary.BigEndian.Uint32(data[checksumOffset:]) != crc32.ChecksumIEEE(data[:checksumOffset]) {
		return nil, nil, true, fmt.Errorf("%w: message checksum mismatch", ErrInvalidEnvelope)
	}
	cursor := messageHeaderSize
	if keyLength != ^uint32(0) {
		key = append([]byte{}, data[cursor:cursor+int(actualKeyLength)]...)
		cursor += int(actualKeyLength)
	}
	value = append([]byte(nil), data[cursor:checksumOffset]...)
	return key, value, true, nil
}
