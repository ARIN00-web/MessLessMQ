package topic

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrTopicAlreadyExists = errors.New("topic already exists")
	ErrTopicNotFound      = errors.New("topic not found")
)

// Registry stores the broker's currently known topics.
//
// The registry is in-memory in this phase. Durable topic metadata is a later
// cluster-metadata concern. The zero value is ready for use.
type Registry struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

// NewRegistry creates and initializes a topic registry.
func NewRegistry() *Registry {
	return &Registry{
		topics: make(map[string]*Topic),
	}
}

// Create registers a new topic with one default partition.
func (r *Registry) Create(name string) (*Topic, error) {
	return r.CreateWithPartitionCount(name, 1)
}

// CreateWithPartitionCount registers a topic with the specified initial
// partition count.
func (r *Registry) CreateWithPartitionCount(
	name string,
	partitionCount uint32,
) (*Topic, error) {
	if err := ValidateTopicName(name); err != nil {
		return nil, err
	}

	if partitionCount == 0 {
		return nil, ErrInvalidPartitionCount
	}

	t := newTopic(name, partitionCount)

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.topics[name]; exists {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrTopicAlreadyExists,
			name,
		)
	}

	if r.topics == nil {
		r.topics = make(map[string]*Topic)
	}

	r.topics[name] = t

	return t, nil
}

// Get returns a registered topic by its exact, case-sensitive name.
func (r *Registry) Get(name string) (*Topic, error) {
	if err := ValidateTopicName(name); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	t, exists := r.topics[name]
	if !exists {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrTopicNotFound,
			name,
		)
	}

	return t, nil
}

// List returns all registered topic names in deterministic lexical order.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.topics))

	for name := range r.topics {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}
