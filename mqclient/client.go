// Package client provides a small cluster-aware MessLessMQ client.
package client

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/network"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

var (
	ErrNoBootstrap = errors.New("messlessmq client: at least one bootstrap address is required")
	ErrNoRoute     = errors.New("messlessmq client: no route for topic partition")
)

// ResponseError preserves a broker protocol error code for safe retry policy.
type ResponseError struct{ Code uint16 }

func (e *ResponseError) Error() string {
	return fmt.Sprintf("messlessmq broker returned error code %d", e.Code)
}

type Config struct {
	Bootstrap   []string
	Timeout     time.Duration
	MetadataTTL time.Duration
	MaxRetries  int
}

type Client struct {
	bootstrap []string
	timeout   time.Duration
	ttl       time.Duration
	retries   int

	mu       sync.Mutex
	metadata protocol.MetadataResponse
	loadedAt time.Time
	round    map[string]uint64
	sequence atomic.Uint32
}

func New(cfg Config) (*Client, error) {
	addresses := make([]string, 0, len(cfg.Bootstrap))
	for _, address := range cfg.Bootstrap {
		if address != "" {
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return nil, ErrNoBootstrap
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MetadataTTL <= 0 {
		cfg.MetadataTTL = 30 * time.Second
	}
	if cfg.MaxRetries < 0 {
		return nil, fmt.Errorf("messlessmq client: negative retry count")
	}
	return &Client{bootstrap: addresses, timeout: cfg.Timeout, ttl: cfg.MetadataTTL, retries: cfg.MaxRetries, round: make(map[string]uint64)}, nil
}

func (c *Client) Metadata(ctx context.Context, topic string) (protocol.MetadataResponse, error) {
	if c == nil {
		return protocol.MetadataResponse{}, ErrNoBootstrap
	}
	c.mu.Lock()
	if !c.loadedAt.IsZero() && time.Since(c.loadedAt) < c.ttl {
		cached := cloneMetadata(c.metadata)
		c.mu.Unlock()
		return cached, nil
	}
	c.mu.Unlock()
	return c.refresh(ctx, topic)
}

func (c *Client) refresh(ctx context.Context, topic string) (protocol.MetadataResponse, error) {
	payload, err := protocol.EncodeMetadataRequest(protocol.MetadataRequest{Topic: topic})
	if err != nil {
		return protocol.MetadataResponse{}, err
	}
	var last error
	for _, address := range c.bootstrap {
		response, requestErr := c.request(ctx, address, protocol.APIMETADATA, protocol.MetadataAPIVersion, payload)
		if requestErr != nil {
			last = requestErr
			continue
		}
		metadata, decodeErr := protocol.DecodeMetadataResponse(response.Payload)
		if decodeErr != nil {
			last = decodeErr
			continue
		}
		c.mu.Lock()
		if c.loadedAt.IsZero() || metadata.Version >= c.metadata.Version {
			c.metadata, c.loadedAt = cloneMetadata(metadata), time.Now()
		}
		out := cloneMetadata(c.metadata)
		c.mu.Unlock()
		return out, nil
	}
	if last == nil {
		last = ErrNoBootstrap
	}
	return protocol.MetadataResponse{}, last
}

type ProduceRequest struct {
	Topic     string
	Key       []byte
	Value     []byte
	Timestamp int64
	AckMode   protocol.AckMode
}

// Produce sends exactly one record using PRODUCE v2. A key chooses its stable
// hash partition; an unkeyed record uses client-side round robin. It retries
// only stale-routing failures, never uncertain append outcomes.
func (c *Client) Produce(ctx context.Context, input ProduceRequest) (protocol.ProduceRecordResult, error) {
	if input.Timestamp == 0 {
		input.Timestamp = time.Now().UnixMilli()
	}
	for attempt := 0; ; attempt++ {
		metadata, err := c.Metadata(ctx, input.Topic)
		if err != nil {
			return protocol.ProduceRecordResult{}, err
		}
		partition, address, err := c.selectRoute(metadata, input.Topic, input.Key)
		if err != nil {
			return protocol.ProduceRecordResult{}, err
		}
		payload, err := protocol.EncodeProduceRequestV2(protocol.ProduceRequestV2{PartitionID: partition, Request: protocol.ProduceRequest{AckMode: input.AckMode, Batch: protocol.MessageBatch{Messages: []protocol.Message{{Topic: input.Topic, Key: input.Key, Value: input.Value, Timestamp: input.Timestamp}}}}})
		if err != nil {
			return protocol.ProduceRecordResult{}, err
		}
		response, err := c.request(ctx, address, protocol.APIPRODUCE, protocol.ProduceAPIVersion2, payload)
		if err == nil {
			decoded, decodeErr := protocol.DecodeProduceResponse(response.Payload)
			if decodeErr != nil || len(decoded.Records) != 1 {
				if decodeErr == nil {
					decodeErr = protocol.ErrInvalidRequest
				}
				return protocol.ProduceRecordResult{}, decodeErr
			}
			return decoded.Records[0], nil
		}
		if attempt >= c.retries || !isStaleRoute(err) {
			return protocol.ProduceRecordResult{}, err
		}
		if _, refreshErr := c.refresh(ctx, input.Topic); refreshErr != nil {
			return protocol.ProduceRecordResult{}, errors.Join(err, refreshErr)
		}
	}
}

func (c *Client) Consume(ctx context.Context, topic string, partition uint32, offset uint64, maxMessages uint32) (protocol.ConsumeResponse, error) {
	payload, err := protocol.EncodeConsumeRequest(protocol.ConsumeRequest{Topic: topic, PartitionID: partition, Offset: offset, MaxMessages: maxMessages})
	if err != nil {
		return protocol.ConsumeResponse{}, err
	}
	for attempt := 0; ; attempt++ {
		metadata, err := c.Metadata(ctx, topic)
		if err != nil {
			return protocol.ConsumeResponse{}, err
		}
		_, address, err := c.routeForPartition(metadata, topic, partition)
		if err != nil {
			return protocol.ConsumeResponse{}, err
		}
		response, err := c.request(ctx, address, protocol.APICONSUME, 2, payload)
		if err == nil {
			return protocol.DecodeConsumeResponseV2(response.Payload)
		}
		if attempt >= c.retries || !isStaleRoute(err) {
			return protocol.ConsumeResponse{}, err
		}
		if _, refreshErr := c.refresh(ctx, topic); refreshErr != nil {
			return protocol.ConsumeResponse{}, errors.Join(err, refreshErr)
		}
	}
}

func (c *Client) selectRoute(metadata protocol.MetadataResponse, topic string, key []byte) (uint32, string, error) {
	partitions := partitionsFor(metadata, topic)
	if len(partitions) == 0 {
		return 0, "", ErrNoRoute
	}
	var index int
	if len(key) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(key)
		index = int(h.Sum32() % uint32(len(partitions)))
	} else {
		c.mu.Lock()
		index = int(c.round[topic] % uint64(len(partitions)))
		c.round[topic]++
		c.mu.Unlock()
	}
	partition := partitions[index]
	_, address, err := c.routeForPartition(metadata, topic, partition.PartitionID)
	return partition.PartitionID, address, err
}

func (c *Client) routeForPartition(metadata protocol.MetadataResponse, topic string, partitionID uint32) (protocol.PartitionMetadata, string, error) {
	for _, partition := range metadata.Partitions {
		if partition.Topic == topic && partition.PartitionID == partitionID {
			for _, broker := range metadata.Brokers {
				if broker.ID == partition.Leader {
					return partition, broker.Address, nil
				}
			}
		}
	}
	return protocol.PartitionMetadata{}, "", ErrNoRoute
}

func (c *Client) request(ctx context.Context, address string, api, version uint16, payload []byte) (protocol.Response, error) {
	dialer := net.Dialer{Timeout: c.timeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return protocol.Response{}, err
	}
	defer conn.Close()
	deadline := time.Now().Add(c.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	frame, err := protocol.EncodeRequest(protocol.Request{APIKey: api, APIVersion: version, CorrelationID: c.sequence.Add(1), Payload: payload})
	if err != nil {
		return protocol.Response{}, err
	}
	if err = network.WriteFrame(conn, frame); err != nil {
		return protocol.Response{}, err
	}
	responseFrame, err := network.ReadFrame(conn)
	if err != nil {
		return protocol.Response{}, err
	}
	response, err := protocol.DecodeResponse(responseFrame)
	if err != nil {
		return protocol.Response{}, err
	}
	if response.ErrorCode != protocol.ErrorNone {
		return response, &ResponseError{Code: response.ErrorCode}
	}
	return response, nil
}

func isStaleRoute(err error) bool {
	var response *ResponseError
	return errors.As(err, &response) && (response.Code == protocol.ErrorNotPartitionLeader || response.Code == protocol.ErrorControllerAuthorityUnavailable)
}
func partitionsFor(metadata protocol.MetadataResponse, topic string) []protocol.PartitionMetadata {
	out := make([]protocol.PartitionMetadata, 0)
	for _, p := range metadata.Partitions {
		if p.Topic == topic {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartitionID < out[j].PartitionID })
	return out
}
func cloneMetadata(metadata protocol.MetadataResponse) protocol.MetadataResponse {
	out := protocol.MetadataResponse{Version: metadata.Version, Brokers: append([]protocol.BrokerMetadata(nil), metadata.Brokers...), Partitions: make([]protocol.PartitionMetadata, len(metadata.Partitions))}
	for i, p := range metadata.Partitions {
		p.Replicas = append([]string(nil), p.Replicas...)
		out.Partitions[i] = p
	}
	return out
}
