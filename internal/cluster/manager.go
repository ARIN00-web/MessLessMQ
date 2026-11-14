package cluster

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/ARIN00-web/messlessmq/internal/controller"
)

const MaxTopicPartitions = 1 << 16

var (
	ErrInvalidBroker       = errors.New("cluster: invalid broker")
	ErrNoBrokers           = errors.New("cluster: no brokers registered")
	ErrInvalidPlacement    = errors.New("cluster: invalid partition placement")
	ErrPartitionUnassigned = errors.New("cluster: partition is not assigned")
	ErrBrokerUnavailable   = errors.New("cluster: partition leader broker is not registered")
	ErrPartitionAssigned   = errors.New("cluster: partition is already assigned")
)

// Config defines the fixed controller membership for this learning cluster.
type Config struct {
	ControllerIDs   []string
	BootstrapLeader string
}

// Route is the metadata a client needs to locate a partition leader.
type Route struct {
	MetadataVersion uint64
	Assignment      controller.PartitionAssignment
	Leader          controller.Broker
}

// Manager is the cluster control-plane facade. It commits metadata through
// the Phase 15 controller quorum and exposes placement and routing operations.
// Data-plane storage remains owned by individual broker processes.
type Manager struct {
	mu          sync.Mutex
	controllers *controller.Cluster
}

func NewManager(cfg Config) (*Manager, error) {
	if len(cfg.ControllerIDs) == 0 || cfg.BootstrapLeader == "" {
		return nil, controller.ErrInvalidConfig
	}
	controllers, err := controller.NewCluster(cfg.ControllerIDs...)
	if err != nil {
		return nil, err
	}
	if err := controllers.Campaign(cfg.BootstrapLeader); err != nil {
		return nil, fmt.Errorf("bootstrap controller election: %w", err)
	}
	return &Manager{controllers: controllers}, nil
}

// ControllerCluster exposes the underlying deterministic controller harness
// for failure tests and explicit controller campaigns.
func (m *Manager) ControllerCluster() *controller.Cluster {
	if m == nil {
		return nil
	}
	return m.controllers
}

func (m *Manager) ControllerLeader() string {
	if m == nil {
		return ""
	}
	return m.controllers.CurrentLeader()
}

// RegisterBroker commits a broker's stable ID and advertised client/peer
// address through the active controller leader.
func (m *Manager) RegisterBroker(broker controller.Broker) error {
	if m == nil || m.controllers == nil {
		return controller.ErrInvalidConfig
	}
	if broker.ID == "" || broker.Address == "" {
		return ErrInvalidBroker
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.controllers.Commit(controller.Command{
		Type:   controller.CommandRegisterBroker,
		Broker: broker,
	})
}

// Brokers returns the committed broker registry in stable ID order.
func (m *Manager) Brokers() ([]controller.Broker, uint64, error) {
	if m == nil || m.controllers == nil {
		return nil, 0, controller.ErrInvalidConfig
	}
	leaderID := m.controllers.CurrentLeader()
	if leaderID == "" {
		return nil, 0, controller.ErrNotLeader
	}
	snapshot, err := m.controllers.Metadata(leaderID)
	if err != nil {
		return nil, 0, err
	}
	brokers := make([]controller.Broker, 0, len(snapshot.Brokers))
	for _, broker := range snapshot.Brokers {
		brokers = append(brokers, broker)
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
	return brokers, snapshot.Version, nil
}

// PlacePartition chooses a deterministic replica set from the registered
// brokers. The first selected replica is the initial partition leader.
// Selection rotates by partition ID to spread leaders and replicas.
func (m *Manager) PlacePartition(topic string, partition int, replicationFactor int) (controller.PartitionAssignment, error) {
	if m == nil || m.controllers == nil {
		return controller.PartitionAssignment{}, controller.ErrInvalidConfig
	}
	if !validTopicName(topic) || partition < 0 || uint64(partition) > uint64(^uint32(0)) || replicationFactor < 1 {
		return controller.PartitionAssignment{}, ErrInvalidPlacement
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	leaderID := m.controllers.CurrentLeader()
	if leaderID == "" {
		return controller.PartitionAssignment{}, controller.ErrNotLeader
	}
	snapshot, err := m.controllers.Metadata(leaderID)
	if err != nil {
		return controller.PartitionAssignment{}, err
	}
	key := controller.PartitionKey{Topic: topic, Partition: partition}
	if _, ok := snapshot.Partitions[key]; ok {
		return controller.PartitionAssignment{}, fmt.Errorf("%s[%d]: %w", topic, partition, ErrPartitionAssigned)
	}

	brokers := make([]controller.Broker, 0, len(snapshot.Brokers))
	for _, broker := range snapshot.Brokers {
		brokers = append(brokers, broker)
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
	if len(brokers) == 0 {
		return controller.PartitionAssignment{}, ErrNoBrokers
	}
	if replicationFactor > len(brokers) {
		return controller.PartitionAssignment{}, fmt.Errorf("replication factor %d exceeds %d registered brokers: %w", replicationFactor, len(brokers), ErrInvalidPlacement)
	}

	start := partition % len(brokers)
	replicas := make([]string, 0, replicationFactor)
	for i := 0; i < replicationFactor; i++ {
		replicas = append(replicas, brokers[(start+i)%len(brokers)].ID)
	}
	assignment := controller.PartitionAssignment{
		Key:         key,
		Replicas:    replicas,
		ISR:         append([]string(nil), replicas...),
		Leader:      replicas[0],
		LeaderEpoch: 1,
	}
	if err := m.controllers.Commit(controller.Command{
		Type:       controller.CommandAssignPartition,
		Assignment: assignment,
	}); err != nil {
		return controller.PartitionAssignment{}, err
	}
	return cloneAssignment(assignment), nil
}

// CreateTopic assigns a consecutive range of partition IDs. Each partition
// assignment is its own metadata commit, so callers should treat an error as
// potentially partial progress and inspect Metadata before retrying.
func (m *Manager) CreateTopic(topic string, partitionCount int, replicationFactor int) ([]controller.PartitionAssignment, error) {
	if !validTopicName(topic) || partitionCount < 1 || partitionCount > MaxTopicPartitions || replicationFactor < 1 {
		return nil, ErrInvalidPlacement
	}
	assignments := make([]controller.PartitionAssignment, 0, partitionCount)
	for partition := 0; partition < partitionCount; partition++ {
		assignment, err := m.PlacePartition(topic, partition, replicationFactor)
		if err != nil {
			return assignments, err
		}
		assignments = append(assignments, assignment)
	}
	return assignments, nil
}

// ResolveRoute returns the committed leader broker for one topic partition.
func (m *Manager) ResolveRoute(topic string, partition int) (Route, error) {
	if m == nil || m.controllers == nil {
		return Route{}, controller.ErrInvalidConfig
	}
	if !validTopicName(topic) || partition < 0 || uint64(partition) > uint64(^uint32(0)) {
		return Route{}, ErrInvalidPlacement
	}
	leaderID := m.controllers.CurrentLeader()
	if leaderID == "" {
		return Route{}, controller.ErrNotLeader
	}
	snapshot, err := m.controllers.Metadata(leaderID)
	if err != nil {
		return Route{}, err
	}
	assignment, exists := snapshot.Partitions[controller.PartitionKey{Topic: topic, Partition: partition}]
	if !exists {
		return Route{}, ErrPartitionUnassigned
	}
	broker, exists := snapshot.Brokers[assignment.Leader]
	if !exists {
		return Route{}, ErrBrokerUnavailable
	}
	return Route{MetadataVersion: snapshot.Version, Assignment: cloneAssignment(assignment), Leader: broker}, nil
}

// Metadata returns the snapshot maintained by the requested controller.
func (m *Manager) Metadata(controllerID string) (controller.MetadataSnapshot, error) {
	if m == nil || m.controllers == nil {
		return controller.MetadataSnapshot{}, controller.ErrInvalidConfig
	}
	return m.controllers.Metadata(controllerID)
}

func cloneAssignment(assignment controller.PartitionAssignment) controller.PartitionAssignment {
	assignment.Replicas = append([]string(nil), assignment.Replicas...)
	assignment.ISR = append([]string(nil), assignment.ISR...)
	return assignment
}

func validTopicName(name string) bool {
	if len(name) == 0 || len(name) > 249 || name == "." || name == ".." {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		valid := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'
		if !valid {
			return false
		}
	}
	return true
}
