package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	APIMETADATA        uint16 = 3
	MetadataAPIVersion uint16 = 1
	MaxMetadataPayload        = 4 * 1024 * 1024
	maxMetadataEntries        = 16 * 1024
)

var (
	ErrInvalidMetadataPayload = errors.New("invalid metadata payload")
	ErrMetadataTooLarge       = errors.New("metadata payload too large")
)

type MetadataRequest struct {
	// Empty Topic requests metadata for every topic.
	Topic string
}

type BrokerMetadata struct {
	ID      string
	Address string
}

type PartitionMetadata struct {
	Topic       string
	PartitionID uint32
	Replicas    []string
	Leader      string
	LeaderEpoch uint64
}

type MetadataResponse struct {
	Version    uint64
	Brokers    []BrokerMetadata
	Partitions []PartitionMetadata
}

func EncodeMetadataRequest(request MetadataRequest) ([]byte, error) {
	if request.Topic != "" && !validMetadataTopic(request.Topic) {
		return nil, ErrInvalidMetadataPayload
	}
	data := make([]byte, 2+len(request.Topic))
	binary.BigEndian.PutUint16(data[:2], uint16(len(request.Topic)))
	copy(data[2:], request.Topic)
	return data, nil
}

func DecodeMetadataRequest(data []byte) (MetadataRequest, error) {
	if len(data) < 2 {
		return MetadataRequest{}, ErrInvalidMetadataPayload
	}
	length := int(binary.BigEndian.Uint16(data[:2]))
	if length > 249 || len(data) != 2+length {
		return MetadataRequest{}, ErrInvalidMetadataPayload
	}
	topic := string(data[2:])
	if topic != "" && !validMetadataTopic(topic) {
		return MetadataRequest{}, ErrInvalidMetadataPayload
	}
	return MetadataRequest{Topic: topic}, nil
}

func EncodeMetadataResponse(response MetadataResponse) ([]byte, error) {
	if len(response.Brokers) > maxMetadataEntries || len(response.Partitions) > maxMetadataEntries {
		return nil, ErrMetadataTooLarge
	}
	data := make([]byte, 10)
	binary.BigEndian.PutUint64(data[:8], response.Version)
	binary.BigEndian.PutUint16(data[8:10], uint16(len(response.Brokers)))
	seenBrokers := make(map[string]struct{}, len(response.Brokers))
	for _, broker := range response.Brokers {
		if broker.ID == "" || broker.Address == "" {
			return nil, ErrInvalidMetadataPayload
		}
		if _, exists := seenBrokers[broker.ID]; exists {
			return nil, ErrInvalidMetadataPayload
		}
		seenBrokers[broker.ID] = struct{}{}
		var err error
		data, err = appendMetadataString(data, broker.ID)
		if err != nil {
			return nil, err
		}
		data, err = appendMetadataString(data, broker.Address)
		if err != nil {
			return nil, err
		}
	}
	countOffset := len(data)
	data = append(data, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(data[countOffset:countOffset+4], uint32(len(response.Partitions)))
	seenPartitions := make(map[string]struct{}, len(response.Partitions))
	for _, partition := range response.Partitions {
		if !validMetadataTopic(partition.Topic) || partition.Leader == "" || len(partition.Replicas) == 0 || partition.LeaderEpoch == 0 || len(partition.Replicas) > 0xffff {
			return nil, ErrInvalidMetadataPayload
		}
		partitionKey := fmt.Sprintf("%s\x00%d", partition.Topic, partition.PartitionID)
		if _, exists := seenPartitions[partitionKey]; exists {
			return nil, ErrInvalidMetadataPayload
		}
		seenPartitions[partitionKey] = struct{}{}
		seenReplicas := make(map[string]struct{}, len(partition.Replicas))
		leaderFound := false
		for _, replica := range partition.Replicas {
			if replica == "" {
				return nil, ErrInvalidMetadataPayload
			}
			if _, exists := seenReplicas[replica]; exists {
				return nil, ErrInvalidMetadataPayload
			}
			if _, brokerExists := seenBrokers[replica]; !brokerExists {
				return nil, ErrInvalidMetadataPayload
			}
			seenReplicas[replica] = struct{}{}
			if replica == partition.Leader {
				leaderFound = true
			}
		}
		if !leaderFound {
			return nil, ErrInvalidMetadataPayload
		}
		var err error
		data, err = appendMetadataString(data, partition.Topic)
		if err != nil {
			return nil, err
		}
		var number [4]byte
		binary.BigEndian.PutUint32(number[:], partition.PartitionID)
		data = append(data, number[:]...)
		var replicaCount [2]byte
		binary.BigEndian.PutUint16(replicaCount[:], uint16(len(partition.Replicas)))
		data = append(data, replicaCount[:]...)
		for _, replica := range partition.Replicas {
			data, err = appendMetadataString(data, replica)
			if err != nil {
				return nil, err
			}
		}
		data, err = appendMetadataString(data, partition.Leader)
		if err != nil {
			return nil, err
		}
		var epoch [8]byte
		binary.BigEndian.PutUint64(epoch[:], partition.LeaderEpoch)
		data = append(data, epoch[:]...)
		if len(data) > MaxMetadataPayload {
			return nil, ErrMetadataTooLarge
		}
	}
	if len(data) > MaxMetadataPayload {
		return nil, ErrMetadataTooLarge
	}
	return data, nil
}

func DecodeMetadataResponse(data []byte) (MetadataResponse, error) {
	if len(data) < 14 || len(data) > MaxMetadataPayload {
		return MetadataResponse{}, ErrInvalidMetadataPayload
	}
	cursor := metadataCursor{data: data}
	version, ok := cursor.readUint64()
	if !ok {
		return MetadataResponse{}, ErrInvalidMetadataPayload
	}
	brokerCount, ok := cursor.readUint16()
	if !ok || brokerCount > maxMetadataEntries {
		return MetadataResponse{}, ErrInvalidMetadataPayload
	}
	response := MetadataResponse{Version: version, Brokers: make([]BrokerMetadata, 0, brokerCount)}
	seenBrokers := make(map[string]struct{}, brokerCount)
	for i := 0; i < int(brokerCount); i++ {
		id, ok := cursor.readString()
		if !ok {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		address, ok := cursor.readString()
		if !ok || id == "" || address == "" {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		if _, exists := seenBrokers[id]; exists {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		seenBrokers[id] = struct{}{}
		response.Brokers = append(response.Brokers, BrokerMetadata{ID: id, Address: address})
	}
	partitionCount, ok := cursor.readUint32()
	if !ok || partitionCount > maxMetadataEntries {
		return MetadataResponse{}, ErrInvalidMetadataPayload
	}
	response.Partitions = make([]PartitionMetadata, 0, partitionCount)
	seenPartitions := make(map[string]struct{}, partitionCount)
	for i := uint32(0); i < partitionCount; i++ {
		topic, ok := cursor.readString()
		if !ok || !validMetadataTopic(topic) {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		partitionID, ok := cursor.readUint32()
		if !ok {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		replicaCount, ok := cursor.readUint16()
		if !ok || replicaCount == 0 {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		partition := PartitionMetadata{Topic: topic, PartitionID: partitionID, Replicas: make([]string, 0, replicaCount)}
		for j := 0; j < int(replicaCount); j++ {
			replica, ok := cursor.readString()
			if !ok || replica == "" {
				return MetadataResponse{}, ErrInvalidMetadataPayload
			}
			partition.Replicas = append(partition.Replicas, replica)
		}
		leader, ok := cursor.readString()
		if !ok || leader == "" {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		partition.Leader = leader
		partition.LeaderEpoch, ok = cursor.readUint64()
		if !ok || partition.LeaderEpoch == 0 {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		partitionKey := fmt.Sprintf("%s\x00%d", partition.Topic, partition.PartitionID)
		if _, exists := seenPartitions[partitionKey]; exists {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		seenPartitions[partitionKey] = struct{}{}
		seenReplicas := make(map[string]struct{}, len(partition.Replicas))
		leaderFound := false
		for _, replica := range partition.Replicas {
			if _, exists := seenReplicas[replica]; exists {
				return MetadataResponse{}, ErrInvalidMetadataPayload
			}
			if _, brokerExists := seenBrokers[replica]; !brokerExists {
				return MetadataResponse{}, ErrInvalidMetadataPayload
			}
			seenReplicas[replica] = struct{}{}
			if replica == partition.Leader {
				leaderFound = true
			}
		}
		if !leaderFound {
			return MetadataResponse{}, ErrInvalidMetadataPayload
		}
		response.Partitions = append(response.Partitions, partition)
	}
	if cursor.offset != len(data) {
		return MetadataResponse{}, fmt.Errorf("trailing metadata bytes: %w", ErrInvalidMetadataPayload)
	}
	return response, nil
}

func appendMetadataString(data []byte, value string) ([]byte, error) {
	if len(value) == 0 || len(value) > 0xffff {
		return nil, ErrInvalidMetadataPayload
	}
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(value)))
	data = append(data, length[:]...)
	data = append(data, value...)
	if len(data) > MaxMetadataPayload {
		return nil, ErrMetadataTooLarge
	}
	return data, nil
}

type metadataCursor struct {
	data   []byte
	offset int
}

func (c *metadataCursor) take(size int) ([]byte, bool) {
	if size < 0 || size > len(c.data)-c.offset {
		return nil, false
	}
	value := c.data[c.offset : c.offset+size]
	c.offset += size
	return value, true
}
func (c *metadataCursor) readUint16() (uint16, bool) {
	b, ok := c.take(2)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint16(b), true
}
func (c *metadataCursor) readUint32() (uint32, bool) {
	b, ok := c.take(4)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint32(b), true
}
func (c *metadataCursor) readUint64() (uint64, bool) {
	b, ok := c.take(8)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}
func (c *metadataCursor) readString() (string, bool) {
	n, ok := c.readUint16()
	if !ok || n == 0 {
		return "", false
	}
	b, ok := c.take(int(n))
	if !ok {
		return "", false
	}
	return string(b), true
}

func validMetadataTopic(name string) bool {
	if len(name) == 0 || len(name) > 249 || name == "." || name == ".." {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		valid := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
		if !valid {
			return false
		}
	}
	return true
}
