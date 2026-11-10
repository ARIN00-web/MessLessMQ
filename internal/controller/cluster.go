package controller

import "sync"

type link struct{ a, b string }

// Cluster is a deterministic in-memory transport and failure harness for a
// fixed Raft membership. It deliberately has no timers or sockets: callers
// trigger elections and metadata proposals explicitly.
type Cluster struct {
	mu sync.Mutex

	nodes     map[string]*Node
	available map[string]bool
	links     map[link]bool
	leaderID  string
}

func NewCluster(nodeIDs ...string) (*Cluster, error) {
	if len(nodeIDs) == 0 {
		return nil, ErrInvalidConfig
	}
	unique := make(map[string]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		if id == "" {
			return nil, ErrInvalidConfig
		}
		if _, exists := unique[id]; exists {
			return nil, ErrInvalidConfig
		}
		unique[id] = struct{}{}
	}

	cluster := &Cluster{
		nodes:     make(map[string]*Node, len(nodeIDs)),
		available: make(map[string]bool, len(nodeIDs)),
		links:     make(map[link]bool),
	}
	for _, id := range nodeIDs {
		peers := make([]string, 0, len(nodeIDs)-1)
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}
		node, err := newNode(id, peers)
		if err != nil {
			return nil, err
		}
		cluster.nodes[id] = node
		cluster.available[id] = true
	}
	for _, a := range nodeIDs {
		for _, b := range nodeIDs {
			if a != b {
				cluster.links[link{a: a, b: b}] = true
			}
		}
	}
	return cluster, nil
}

func (c *Cluster) Node(id string) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, exists := c.nodes[id]
	if !exists {
		return nil, ErrUnknownNode
	}
	return node, nil
}

func (c *Cluster) CurrentLeader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaderID
}

func (c *Cluster) SetAvailable(id string, available bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.nodes[id]; !exists {
		return ErrUnknownNode
	}
	c.available[id] = available
	if !available && c.leaderID == id {
		c.leaderID = ""
	}
	return nil
}

// SetLink controls a bidirectional link between two controllers. A request and
// its response both need to cross the link, so the model is symmetric.
func (c *Cluster) SetLink(a, b string, available bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if a == b {
		return ErrInvalidConfig
	}
	if _, exists := c.nodes[a]; !exists {
		return ErrUnknownNode
	}
	if _, exists := c.nodes[b]; !exists {
		return ErrUnknownNode
	}
	c.links[link{a: a, b: b}] = available
	c.links[link{a: b, b: a}] = available
	return nil
}

func (c *Cluster) canCommunicateLocked(a, b string) bool {
	return c.available[a] && c.available[b] && c.links[link{a: a, b: b}] && c.links[link{a: b, b: a}]
}

// Campaign runs one deterministic election round. No node becomes leader
// without votes from a majority of the configured membership.
func (c *Cluster) Campaign(candidateID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	candidate, exists := c.nodes[candidateID]
	if !exists {
		return ErrUnknownNode
	}
	if !c.available[candidateID] {
		return ErrNodeUnavailable
	}
	request, err := candidate.BeginElection()
	if err != nil {
		return err
	}
	for _, peerID := range candidate.peers {
		if !c.canCommunicateLocked(candidateID, peerID) {
			continue
		}
		response := c.nodes[peerID].HandleVoteRequest(request)
		if err := candidate.recordVote(peerID, response); err != nil {
			return err
		}
	}
	if err := candidate.winElection(); err != nil {
		return err
	}
	c.leaderID = candidateID
	c.broadcastLocked(candidate)
	return nil
}

// Commit appends a metadata command and commits it only after the leader and a
// majority of controllers have the entry. A failed proposal remains uncommitted
// and may be reconciled by a later leader or a later successful proposal.
func (c *Cluster) Commit(command Command) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leaderID == "" {
		return ErrNotLeader
	}
	leaderID := c.leaderID
	if !c.available[leaderID] {
		c.leaderID = ""
		return ErrNodeUnavailable
	}
	leader := c.nodes[leaderID]
	if leader.Status().Role != RoleLeader {
		c.leaderID = ""
		return ErrNotLeader
	}
	if _, err := leader.appendLocal(command); err != nil {
		return err
	}

	replicated := 1 // the leader has its own entry
	for peerID := range c.nodes {
		if peerID == leaderID || !c.canCommunicateLocked(leaderID, peerID) {
			continue
		}
		response := c.nodes[peerID].HandleAppendEntries(leader.appendRequest())
		if response.Term > leader.Status().Term {
			c.leaderID = ""
			return ErrNotLeader
		}
		if response.Success {
			replicated++
		}
	}
	if err := leader.commitCurrentTermEntry(replicated); err != nil {
		return ErrNoQuorum
	}
	c.broadcastLocked(leader)
	return nil
}

func (c *Cluster) broadcastLocked(leader *Node) {
	request := leader.appendRequest()
	for peerID := range c.nodes {
		if peerID == leader.id || !c.canCommunicateLocked(leader.id, peerID) {
			continue
		}
		c.nodes[peerID].HandleAppendEntries(request)
	}
}

func (c *Cluster) Metadata(nodeID string) (MetadataSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, exists := c.nodes[nodeID]
	if !exists {
		return MetadataSnapshot{}, ErrUnknownNode
	}
	return node.Metadata(), nil
}

func (c *Cluster) Statuses() map[string]NodeStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]NodeStatus, len(c.nodes))
	for id, node := range c.nodes {
		out[id] = node.Status()
	}
	return out
}
