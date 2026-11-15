// Package client provides a small cluster-aware MessLessMQ client.
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"sort"
	"strings"
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
	switch e.Code {
	case protocol.ErrorUnknownAPI:
		return "messlessmq broker: unknown API request (error 1)"
	case protocol.ErrorInvalidRequest:
		return "messlessmq broker: invalid request payload or parameters (error 2)"
	case protocol.ErrorInternal:
		return "messlessmq broker: internal broker failure (error 3)"
	case protocol.ErrorInvalidAckMode:
		return "messlessmq broker: unsupported or invalid acknowledgement mode (error 4)"
	case protocol.ErrorMessageTooLarge:
		return "messlessmq broker: message exceeds maximum size (error 5)"
	case protocol.ErrorUnknownTopic:
		return "messlessmq broker: topic not found (error 6)"
	case protocol.ErrorPartitionNotFound:
		return "messlessmq broker: partition not found (error 7)"
	case protocol.ErrorOffsetOutOfRange:
		return "messlessmq broker: requested offset out of range (error 8)"
	case protocol.ErrorOffsetBeforeStart:
		return "messlessmq broker: requested offset before start offset (deleted by retention) (error 9)"
	case protocol.ErrorInvalidFetchLimit:
		return "messlessmq broker: invalid fetch limit (error 10)"
	case protocol.ErrorUnsupportedVersion:
		return "messlessmq broker: unsupported protocol version (error 11)"
	case protocol.ErrorRateLimited:
		return "messlessmq broker: request rate limit exceeded (error 12)"
	case protocol.ErrorNotPartitionLeader:
		return "messlessmq broker: not partition leader (error 13)"
	case protocol.ErrorInsufficientISR:
		return "messlessmq broker: insufficient in-sync replicas (ISR) to satisfy MinISR durability (error 14)"
	case protocol.ErrorCommitUncertain:
		return "messlessmq broker: commit outcome uncertain (error 15)"
	case protocol.ErrorControllerAuthorityUnavailable:
		return "messlessmq broker: controller authority lease expired or unavailable (error 16)"
	case protocol.ErrorUnauthorized:
		return "messlessmq broker: authentication failure (error 17)"
	case protocol.ErrorForbidden:
		return "messlessmq broker: authorization forbidden (error 18)"
	default:
		return fmt.Sprintf("messlessmq broker returned error code %d", e.Code)
	}
}

type Config struct {
	Bootstrap   []string
	Timeout     time.Duration
	MetadataTTL time.Duration
	MaxRetries  int
	// TLSConfig enables TLS/mTLS. When ServerName is empty, the broker
	// hostname from each advertised address is used for certificate checks.
	TLSConfig *tls.Config
	// APIKey is sent in an AUTH request on every newly opened connection.
	APIKey string
	// EndpointResolver optionally rewrites a broker's advertised address for routing.
	EndpointResolver func(brokerID string, advertisedAddr string) string
}

type metadataCacheEntry struct {
	metadata protocol.MetadataResponse
	loadedAt time.Time
}

type Client struct {
	bootstrap []string
	timeout   time.Duration
	ttl       time.Duration
	retries   int

	mu               sync.Mutex
	metadata         map[string]metadataCacheEntry
	round            map[string]uint64
	sequence         atomic.Uint32
	tlsConfig        *tls.Config
	apiKey           string
	endpointResolver func(brokerID string, advertisedAddr string) string
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
	client := &Client{
		bootstrap:        addresses,
		timeout:          cfg.Timeout,
		ttl:              cfg.MetadataTTL,
		round:            make(map[string]uint64),
		metadata:         make(map[string]metadataCacheEntry),
		apiKey:           cfg.APIKey,
		endpointResolver: cfg.EndpointResolver,
	}
	if cfg.TLSConfig != nil {
		client.tlsConfig = cfg.TLSConfig.Clone()
	}
	return client, nil
}

func (c *Client) Metadata(ctx context.Context, topic string) (protocol.MetadataResponse, error) {
	if c == nil {
		return protocol.MetadataResponse{}, ErrNoBootstrap
	}
	c.mu.Lock()
	entry, exists := c.metadata[topic]
	if exists && time.Since(entry.loadedAt) < c.ttl {
		cached := cloneMetadata(entry.metadata)
		c.mu.Unlock()
		return cached, nil
	}
	c.mu.Unlock()
	return c.refresh(ctx, topic)
}

func (c *Client) refresh(ctx context.Context, topic string) (protocol.MetadataResponse, error) {
	// Cache each topic filter independently. This preserves topic-scoped ACLs
	// and prevents a response for one topic from being reused for another.
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
		current, exists := c.metadata[topic]
		if !exists || metadata.Version >= current.metadata.Version {
			c.metadata[topic] = metadataCacheEntry{metadata: cloneMetadata(metadata), loadedAt: time.Now()}
		}
		out := cloneMetadata(c.metadata[topic].metadata)
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
					addr := c.resolveBrokerAddress(broker.ID, broker.Address)
					return partition, addr, nil
				}
			}
		}
	}
	return protocol.PartitionMetadata{}, "", ErrNoRoute
}

func (c *Client) resolveBrokerAddress(brokerID, advertisedAddr string) string {
	if c.endpointResolver != nil {
		if resolved := c.endpointResolver(brokerID, advertisedAddr); resolved != "" {
			return resolved
		}
	}
	return ResolveAddressWithBootstrap(advertisedAddr, c.bootstrap, brokerID)
}

// ResolveAddressWithBootstrap adapts advertised broker addresses to client network context.
// When connecting to an external machine or Docker container, advertised loopback addresses
// are remapped using the reachable bootstrap host.
func ResolveAddressWithBootstrap(advertisedAddr string, bootstrapAddresses []string, brokerID string) string {
	if len(bootstrapAddresses) == 0 {
		return advertisedAddr
	}
	advHost, advPort, err := net.SplitHostPort(advertisedAddr)
	if err != nil {
		return advertisedAddr
	}
	isAdvLoopback := advHost == "127.0.0.1" || strings.ToLower(advHost) == "localhost" || advHost == "::1"

	for _, boot := range bootstrapAddresses {
		bootHost, bootPort, bootErr := net.SplitHostPort(boot)
		if bootErr != nil {
			continue
		}
		isBootLoopback := bootHost == "127.0.0.1" || strings.ToLower(bootHost) == "localhost" || bootHost == "::1"

		// Container-internal client: dialed via internal port 9092, map directly to broker ID on container network
		if isAdvLoopback && bootPort == "9092" && brokerID != "" {
			return net.JoinHostPort(brokerID, "9092")
		}

		// External network client: dialed a non-loopback host (remote IP/DNS), but broker returned loopback
		if isAdvLoopback && !isBootLoopback {
			return net.JoinHostPort(bootHost, advPort)
		}
	}
	return advertisedAddr
}

func (c *Client) request(ctx context.Context, address string, api, version uint16, payload []byte) (protocol.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dialer := net.Dialer{Timeout: c.timeout}
	var conn net.Conn
	var err error
	if c.tlsConfig != nil {
		config := c.tlsConfig.Clone()
		if config.ServerName == "" {
			host, _, splitErr := net.SplitHostPort(address)
			if splitErr != nil {
				return protocol.Response{}, fmt.Errorf("parse TLS broker address %q: %w", address, splitErr)
			}
			config.ServerName = strings.Trim(host, "[]")
		}
		tlsDialer := tls.Dialer{NetDialer: &dialer, Config: config}
		conn, err = tlsDialer.DialContext(ctx, "tcp", address)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return protocol.Response{}, err
	}
	defer conn.Close()
	deadline := time.Now().Add(c.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return protocol.Response{}, err
	}
	if c.apiKey != "" {
		authPayload, encodeErr := protocol.EncodeAuthRequest(protocol.AuthRequest{Token: c.apiKey})
		if encodeErr != nil {
			return protocol.Response{}, encodeErr
		}
		authResponse, authErr := c.exchange(conn, protocol.APIAUTH, protocol.AuthAPIVersion, authPayload)
		if authErr != nil {
			return protocol.Response{}, fmt.Errorf("authenticate broker connection: %w", authErr)
		}
		if _, decodeErr := protocol.DecodeAuthResponse(authResponse.Payload); decodeErr != nil {
			return protocol.Response{}, fmt.Errorf("decode broker authentication response: %w", decodeErr)
		}
	}
	return c.exchange(conn, api, version, payload)
}

// Request sends one versioned protocol request to a specific broker. It is
// useful for administrative/client APIs that need a known broker address;
// normal producers and consumers should use Produce and Consume for routing.
func (c *Client) Request(ctx context.Context, address string, api, version uint16, payload []byte) (protocol.Response, error) {
	if c == nil || address == "" {
		return protocol.Response{}, ErrNoBootstrap
	}
	return c.request(ctx, address, api, version, payload)
}

func (c *Client) exchange(conn net.Conn, api, version uint16, payload []byte) (protocol.Response, error) {
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
