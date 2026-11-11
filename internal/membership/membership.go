package membership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ARIN00-web/messlessmq/internal/interbroker"
)

var (
	ErrInvalidConfig   = errors.New("membership: invalid configuration")
	ErrClusterMismatch = errors.New("membership: cluster ID mismatch")
	ErrBrokerConflict  = errors.New("membership: broker ID is already registered with different addresses")
	ErrUnknownPeer     = errors.New("membership: unknown peer")
	ErrInvalidJoin     = errors.New("membership: invalid join request")
	ErrNoSeedReachable = errors.New("membership: no seed peer could be reached")
)

type Broker struct {
	ID            string `json:"id"`
	ClientAddress string `json:"client_address"`
	PeerAddress   string `json:"peer_address"`
}

type Seed struct {
	ID      string
	Address string
}

type Snapshot struct {
	ClusterID string   `json:"cluster_id"`
	Version   uint64   `json:"version"`
	Brokers   []Broker `json:"brokers"`
}

type Registry struct {
	mu        sync.RWMutex
	clusterID string
	self      Broker
	brokers   map[string]Broker
	version   uint64
}

func NewRegistry(clusterID string, self Broker) (*Registry, error) {
	if strings.TrimSpace(clusterID) == "" || len(clusterID) > 128 || !validBroker(self) {
		return nil, ErrInvalidConfig
	}
	return &Registry{
		clusterID: clusterID,
		self:      self,
		brokers:   map[string]Broker{self.ID: self},
		version:   1,
	}, nil
}

func validBroker(broker Broker) bool {
	if strings.TrimSpace(broker.ID) == "" || len(broker.ID) > 128 ||
		strings.TrimSpace(broker.ClientAddress) == "" || strings.TrimSpace(broker.PeerAddress) == "" {
		return false
	}
	return validAdvertisedAddress(broker.ClientAddress) && validAdvertisedAddress(broker.PeerAddress)
}

func validAdvertisedAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" {
		return false
	}
	number, err := strconv.Atoi(port)
	return err == nil && number >= 1 && number <= 65535
}

func (r *Registry) ClusterID() string {
	if r == nil {
		return ""
	}
	return r.clusterID
}

func (r *Registry) Self() Broker {
	if r == nil {
		return Broker{}
	}
	return r.self
}

func (r *Registry) Register(clusterID string, broker Broker) error {
	if r == nil || !validBroker(broker) {
		return ErrInvalidConfig
	}
	if clusterID != r.clusterID {
		return ErrClusterMismatch
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, exists := r.brokers[broker.ID]; exists {
		if current != broker {
			return fmt.Errorf("broker %q: %w", broker.ID, ErrBrokerConflict)
		}
		return nil
	}
	r.brokers[broker.ID] = broker
	r.version++
	return nil
}

func (r *Registry) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	brokers := make([]Broker, 0, len(r.brokers))
	for _, broker := range r.brokers {
		brokers = append(brokers, broker)
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
	return Snapshot{ClusterID: r.clusterID, Version: r.version, Brokers: brokers}
}

func (r *Registry) Broker(id string) (Broker, bool) {
	if r == nil {
		return Broker{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	broker, ok := r.brokers[id]
	return broker, ok
}

func ParseSeeds(values []string) ([]Seed, error) {
	seeds := make([]Seed, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		id, address, ok := strings.Cut(value, "@")
		id, address = strings.TrimSpace(id), strings.TrimSpace(address)
		if !ok || id == "" || len(id) > 128 || address == "" {
			return nil, fmt.Errorf("invalid seed %q: expected broker-id@host:port: %w", value, ErrInvalidConfig)
		}
		if !validAdvertisedAddress(address) {
			return nil, fmt.Errorf("invalid seed address %q: %w", address, ErrInvalidConfig)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate seed broker %q: %w", id, ErrInvalidConfig)
		}
		seen[id] = struct{}{}
		seeds = append(seeds, Seed{ID: id, Address: address})
	}
	return seeds, nil
}

type joinRequest struct {
	ClusterID string `json:"cluster_id"`
	Broker    Broker `json:"broker"`
}

type joinResponse struct {
	Snapshot Snapshot `json:"snapshot"`
}

type Handler struct {
	registry *Registry
}

func NewHandler(registry *Registry) (*Handler, error) {
	if registry == nil || registry.ClusterID() == "" {
		return nil, ErrInvalidConfig
	}
	return &Handler{registry: registry}, nil
}

func (h *Handler) Handle(_ context.Context, request interbroker.Envelope) (interbroker.Envelope, error) {
	if h == nil || h.registry == nil || request.Type != interbroker.MessageBrokerJoin || request.IsResponse {
		return interbroker.Envelope{}, ErrInvalidJoin
	}
	var payload joinRequest
	if err := json.Unmarshal(request.Payload, &payload); err != nil {
		return interbroker.Envelope{}, fmt.Errorf("decode join request: %w", ErrInvalidJoin)
	}
	if payload.Broker.ID != request.SourceID || request.TargetID != h.registry.Self().ID {
		return interbroker.Envelope{}, ErrInvalidJoin
	}
	if err := h.registry.Register(payload.ClusterID, payload.Broker); err != nil {
		return interbroker.Envelope{}, err
	}
	encoded, err := json.Marshal(joinResponse{Snapshot: h.registry.Snapshot()})
	if err != nil {
		return interbroker.Envelope{}, err
	}
	return interbroker.Envelope{Payload: encoded}, nil
}

var requestSequence atomic.Uint64

// Join contacts configured seeds, learns their known membership, and announces
// this broker to every discovered peer. It returns successfully when at least
// one seed was reachable; unavailable discovered peers are left for a later
// restart/rejoin rather than preventing a partially available cluster from
// starting.
func (r *Registry) Join(ctx context.Context, transport *interbroker.Transport, seeds []Seed) error {
	if r == nil || transport == nil {
		return ErrInvalidConfig
	}
	queue := make([]Seed, 0, len(seeds))
	for _, seed := range seeds {
		if seed.ID == "" || seed.Address == "" {
			return ErrInvalidConfig
		}
		if seed.ID != r.self.ID {
			queue = append(queue, seed)
		}
	}
	if len(queue) == 0 {
		return nil
	}

	visited := make(map[string]struct{})
	contacted := 0
	var failures []error
	for cursor := 0; cursor < len(queue); cursor++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		seed := queue[cursor]
		if seed.ID == r.self.ID {
			continue
		}
		if _, exists := visited[seed.ID]; exists {
			continue
		}
		visited[seed.ID] = struct{}{}

		payload, err := json.Marshal(joinRequest{ClusterID: r.clusterID, Broker: r.self})
		if err != nil {
			return err
		}
		response, err := transport.RoundTrip(ctx, seed.Address, interbroker.Envelope{
			Type: interbroker.MessageBrokerJoin, RequestID: requestSequence.Add(1),
			SourceID: r.self.ID, TargetID: seed.ID, Payload: payload,
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("join seed %s at %s: %w", seed.ID, seed.Address, err))
			continue
		}
		var decoded joinResponse
		if err := json.Unmarshal(response.Payload, &decoded); err != nil {
			failures = append(failures, fmt.Errorf("decode membership from %s: %w", seed.ID, err))
			continue
		}
		if decoded.Snapshot.ClusterID != r.clusterID {
			return ErrClusterMismatch
		}
		seedFound := false
		for _, broker := range decoded.Snapshot.Brokers {
			if broker.ID == seed.ID {
				seedFound = broker.PeerAddress == seed.Address
			}
			if err := r.Register(decoded.Snapshot.ClusterID, broker); err != nil {
				return err
			}
			if broker.ID != r.self.ID {
				queue = append(queue, Seed{ID: broker.ID, Address: broker.PeerAddress})
			}
		}
		if !seedFound {
			return fmt.Errorf("seed response did not confirm %s at %s: %w", seed.ID, seed.Address, ErrUnknownPeer)
		}
		contacted++
	}
	if contacted == 0 {
		if len(failures) == 0 {
			return ErrNoSeedReachable
		}
		return errors.Join(append([]error{ErrNoSeedReachable}, failures...)...)
	}
	return nil
}
