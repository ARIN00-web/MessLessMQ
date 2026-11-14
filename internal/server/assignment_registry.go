package server

import "sync"

// assignmentRegistry publishes immutable snapshots of controller-approved
// partition assignments to the data plane. Readers never observe a map while
// it is being replaced.
type assignmentRegistry struct {
	mu    sync.RWMutex
	items map[partitionKey]partitionReplicaAssignment
}

func newAssignmentRegistry(initial map[partitionKey]partitionReplicaAssignment) *assignmentRegistry {
	registry := &assignmentRegistry{items: make(map[partitionKey]partitionReplicaAssignment, len(initial))}
	registry.replace(initial)
	return registry
}

func (r *assignmentRegistry) replace(items map[partitionKey]partitionReplicaAssignment) {
	if r == nil {
		return
	}
	copyItems := make(map[partitionKey]partitionReplicaAssignment, len(items))
	for key, assignment := range items {
		assignment.Replicas = append([]string(nil), assignment.Replicas...)
		assignment.ISR = append([]string(nil), assignment.ISR...)
		copyItems[key] = assignment
	}
	r.mu.Lock()
	r.items = copyItems
	r.mu.Unlock()
}

func (r *assignmentRegistry) get(key partitionKey) (partitionReplicaAssignment, bool) {
	if r == nil {
		return partitionReplicaAssignment{}, false
	}
	r.mu.RLock()
	assignment, ok := r.items[key]
	r.mu.RUnlock()
	assignment.Replicas = append([]string(nil), assignment.Replicas...)
	assignment.ISR = append([]string(nil), assignment.ISR...)
	return assignment, ok
}

func (r *assignmentRegistry) snapshot() map[partitionKey]partitionReplicaAssignment {
	if r == nil {
		return map[partitionKey]partitionReplicaAssignment{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[partitionKey]partitionReplicaAssignment, len(r.items))
	for key, assignment := range r.items {
		assignment.Replicas = append([]string(nil), assignment.Replicas...)
		assignment.ISR = append([]string(nil), assignment.ISR...)
		out[key] = assignment
	}
	return out
}
