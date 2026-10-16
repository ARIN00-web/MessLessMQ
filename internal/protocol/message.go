package protocol

// Message represents the logical content published by a producer.
// Timestamp is Unix time in milliseconds.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Timestamp int64
}
