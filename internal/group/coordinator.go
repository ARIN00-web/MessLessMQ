package group

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/consumer"
	"github.com/ARIN00-web/messlessmq/internal/offsets"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

var (
	ErrNilRegistry               = errors.New("group coordinator requires a topic registry")
	ErrInvalidGroupID            = errors.New("invalid consumer group ID")
	ErrInvalidMemberID           = errors.New("invalid consumer member ID")
	ErrInvalidSubscription       = errors.New("consumer group requires at least one topic subscription")
	ErrSubscribedTopicMissing    = errors.New("subscribed topic does not exist")
	ErrGroupNotFound             = errors.New("consumer group not found")
	ErrMemberNotFound            = errors.New("consumer group member not found")
	ErrMemberAlreadyExists       = errors.New("consumer group member already exists")
	ErrStaleGeneration           = errors.New("consumer group generation is stale")
	ErrPartitionNotOwned         = errors.New("partition is not owned by this group member")
	ErrInvalidSessionTimeout     = errors.New("session timeout must be positive")
	ErrOffsetStoreNotConfigured  = errors.New("consumer offset store is not configured")
	ErrCommittedOffsetOutOfRange = errors.New("committed offset is outside the retained partition range")
	ErrCommitAheadOfPosition     = errors.New("cannot commit beyond the consumer read position")
	ErrOffsetCommitNotOwned      = errors.New("cannot commit an offset for a partition not owned by the member")
)

const maxIdentifierLength = 128

type memberState struct {
	id            string
	subscriptions []string
	topics        map[string]struct{}
	assignments   []TopicPartition
	partitions    []*topic.Partition
	reader        *consumer.Consumer
	lastHeartbeat time.Time
}

type groupState struct {
	id         string
	generation uint64
	state      State
	members    map[string]*memberState
}

// Coordinator manages consumer-group membership, partition ownership,
// generations, heartbeats, and eager rebalancing.
//
// Group membership and assignments remain in memory. Committed offsets are
// persisted separately in the __consumer_offsets internal log.
type Coordinator struct {
	mu          sync.Mutex
	registry    *topic.Registry
	groups      map[string]*groupState
	offsetStore *offsets.Store
}

// NewCoordinator creates a coordinator backed by the broker's shared topic
// registry. An optional offset store enables durable offset commits and
// restoration. Existing callers without a store remain supported.
func NewCoordinator(registry *topic.Registry, stores ...*offsets.Store) (*Coordinator, error) {
	if registry == nil {
		return nil, ErrNilRegistry
	}
	if len(stores) > 1 {
		return nil, errors.New("NewCoordinator accepts at most one offset store")
	}
	var offsetStore *offsets.Store
	if len(stores) == 1 {
		if stores[0] == nil {
			return nil, ErrOffsetStoreNotConfigured
		}
		offsetStore = stores[0]
	}

	return &Coordinator{
		registry:    registry,
		groups:      make(map[string]*groupState),
		offsetStore: offsetStore,
	}, nil
}

// JoinGroup registers a member, records its topic subscriptions, and performs
// an eager rebalance. Member IDs must be unique within a group.
func (c *Coordinator) JoinGroup(
	groupID string,
	memberID string,
	topicNames []string,
) (JoinResult, error) {
	if c == nil || c.registry == nil {
		return JoinResult{}, ErrNilRegistry
	}
	if err := validateIdentifier(groupID); err != nil {
		return JoinResult{}, fmt.Errorf("%w: %q", ErrInvalidGroupID, groupID)
	}
	if err := validateIdentifier(memberID); err != nil {
		return JoinResult{}, fmt.Errorf("%w: %q", ErrInvalidMemberID, memberID)
	}

	subscriptions, topicSet, err := c.validateSubscriptions(topicNames)
	if err != nil {
		return JoinResult{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	g, exists := c.groups[groupID]
	createdGroup := false
	if !exists {
		g = &groupState{
			id:      groupID,
			state:   StateEmpty,
			members: make(map[string]*memberState),
		}
		c.groups[groupID] = g
		createdGroup = true
	}

	if _, exists := g.members[memberID]; exists {
		return JoinResult{}, fmt.Errorf("%w: %q", ErrMemberAlreadyExists, memberID)
	}

	member := &memberState{
		id:            memberID,
		subscriptions: subscriptions,
		topics:        topicSet,
		lastHeartbeat: time.Now(),
	}
	g.members[memberID] = member

	if err := c.rebalanceLocked(g); err != nil {
		delete(g.members, memberID)
		if createdGroup {
			delete(c.groups, groupID)
		}
		return JoinResult{}, fmt.Errorf("rebalance after member join: %w", err)
	}

	return JoinResult{
		GroupID:     groupID,
		MemberID:    memberID,
		Generation:  g.generation,
		Assignments: cloneAssignments(member.assignments),
	}, nil
}

// LeaveGroup removes a member and reassigns its partitions to the remaining
// subscribed members. The group generation advances after a successful leave.
func (c *Coordinator) LeaveGroup(groupID, memberID string) (uint64, error) {
	if c == nil {
		return 0, ErrGroupNotFound
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	g, exists := c.groups[groupID]
	if !exists {
		return 0, fmt.Errorf("%w: %q", ErrGroupNotFound, groupID)
	}

	member, exists := g.members[memberID]
	if !exists {
		return 0, fmt.Errorf("%w: %q", ErrMemberNotFound, memberID)
	}

	delete(g.members, memberID)
	if err := c.rebalanceLocked(g); err != nil {
		g.members[memberID] = member
		return g.generation, fmt.Errorf("rebalance after member leave: %w", err)
	}

	return g.generation, nil
}

// Heartbeat records liveness for a member. The supplied generation fences
// members that still hold an assignment from an earlier rebalance.
func (c *Coordinator) Heartbeat(groupID, memberID string, generation uint64) error {
	if c == nil {
		return ErrGroupNotFound
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	g, exists := c.groups[groupID]
	if !exists {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupID)
	}
	if generation != g.generation {
		return fmt.Errorf("%w: supplied=%d current=%d", ErrStaleGeneration, generation, g.generation)
	}

	member, exists := g.members[memberID]
	if !exists {
		return fmt.Errorf("%w: %q", ErrMemberNotFound, memberID)
	}
	member.lastHeartbeat = time.Now()
	return nil
}

// Fetch allows a group member to fetch only partitions currently assigned to
// it. Holding the coordinator lock through the fetch serializes a fetch with
// membership changes, preventing a rebalance from racing the ownership check.
func (c *Coordinator) Fetch(
	groupID string,
	memberID string,
	generation uint64,
	topicName string,
	partitionID uint32,
	maxMessages uint32,
) (consumer.FetchResult, error) {
	if c == nil {
		return consumer.FetchResult{}, ErrGroupNotFound
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	g, exists := c.groups[groupID]
	if !exists {
		return consumer.FetchResult{}, fmt.Errorf("%w: %q", ErrGroupNotFound, groupID)
	}
	if generation != g.generation {
		return consumer.FetchResult{}, fmt.Errorf("%w: supplied=%d current=%d", ErrStaleGeneration, generation, g.generation)
	}
	if g.state != StateStable {
		return consumer.FetchResult{}, fmt.Errorf("group %q is not stable", groupID)
	}

	member, exists := g.members[memberID]
	if !exists {
		return consumer.FetchResult{}, fmt.Errorf("%w: %q", ErrMemberNotFound, memberID)
	}
	if !containsAssignment(member.assignments, topicName, partitionID) {
		return consumer.FetchResult{}, fmt.Errorf(
			"%w: member=%q topic=%q partition=%d",
			ErrPartitionNotOwned,
			memberID,
			topicName,
			partitionID,
		)
	}
	if member.reader == nil {
		return consumer.FetchResult{}, fmt.Errorf("%w: member %q has no assigned partitions", ErrPartitionNotOwned, memberID)
	}

	return member.reader.Fetch(topicName, partitionID, maxMessages)
}

// Rebalance explicitly recalculates assignments. This is useful after topic
// partition expansion as well as for administrative recovery operations.
func (c *Coordinator) Rebalance(groupID string) (uint64, error) {
	if c == nil {
		return 0, ErrGroupNotFound
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	g, exists := c.groups[groupID]
	if !exists {
		return 0, fmt.Errorf("%w: %q", ErrGroupNotFound, groupID)
	}
	if err := c.rebalanceLocked(g); err != nil {
		return g.generation, err
	}
	return g.generation, nil
}

// ExpireMembers removes members whose last heartbeat is older than the
// supplied session timeout, then rebalances each affected group.
//
// The caller supplies now so expiration can be tested deterministically. A
// production scheduler can invoke this method periodically in a later phase.
func (c *Coordinator) ExpireMembers(now time.Time, sessionTimeout time.Duration) ([]ExpiredMember, error) {
	if sessionTimeout <= 0 {
		return nil, ErrInvalidSessionTimeout
	}
	if c == nil {
		return nil, ErrNilRegistry
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	groupIDs := make([]string, 0, len(c.groups))
	for id := range c.groups {
		groupIDs = append(groupIDs, id)
	}
	sort.Strings(groupIDs)

	expired := make([]ExpiredMember, 0)
	for _, groupID := range groupIDs {
		g := c.groups[groupID]
		removed := make(map[string]*memberState)
		memberIDs := make([]string, 0, len(g.members))
		for memberID := range g.members {
			memberIDs = append(memberIDs, memberID)
		}
		sort.Strings(memberIDs)

		for _, memberID := range memberIDs {
			member := g.members[memberID]
			if now.Sub(member.lastHeartbeat) > sessionTimeout {
				removed[memberID] = member
				delete(g.members, memberID)
			}
		}
		if len(removed) == 0 {
			continue
		}

		if err := c.rebalanceLocked(g); err != nil {
			for memberID, member := range removed {
				g.members[memberID] = member
			}
			return expired, fmt.Errorf("rebalance after member expiration in group %q: %w", groupID, err)
		}

		for _, memberID := range memberIDs {
			if _, wasRemoved := removed[memberID]; wasRemoved {
				expired = append(expired, ExpiredMember{GroupID: groupID, MemberID: memberID})
			}
		}
	}

	return expired, nil
}

// DescribeGroup returns a stable snapshot with sorted members, subscriptions,
// and assignments. The returned slices do not expose coordinator internals.
func (c *Coordinator) DescribeGroup(groupID string) (Description, error) {
	if c == nil {
		return Description{}, ErrGroupNotFound
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	g, exists := c.groups[groupID]
	if !exists {
		return Description{}, fmt.Errorf("%w: %q", ErrGroupNotFound, groupID)
	}

	description := Description{
		GroupID:    g.id,
		Generation: g.generation,
		State:      g.state,
		Members:    make([]MemberInfo, 0, len(g.members)),
	}

	memberIDs := make([]string, 0, len(g.members))
	for memberID := range g.members {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)

	for _, memberID := range memberIDs {
		member := g.members[memberID]
		description.Members = append(description.Members, MemberInfo{
			MemberID:      member.id,
			Subscriptions: append([]string(nil), member.subscriptions...),
			Assignments:   cloneAssignments(member.assignments),
			LastHeartbeat: member.lastHeartbeat,
		})
	}

	return description, nil
}

func (c *Coordinator) validateSubscriptions(names []string) ([]string, map[string]struct{}, error) {
	if len(names) == 0 {
		return nil, nil, ErrInvalidSubscription
	}

	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) != name || name == "" {
			return nil, nil, fmt.Errorf("%w: invalid topic name %q", ErrInvalidSubscription, name)
		}
		if _, exists := set[name]; exists {
			continue
		}
		if _, err := c.registry.Get(name); err != nil {
			return nil, nil, fmt.Errorf("%w: %q: %v", ErrSubscribedTopicMissing, name, err)
		}
		set[name] = struct{}{}
	}

	sorted := make([]string, 0, len(set))
	for name := range set {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	return sorted, set, nil
}

// rebalanceLocked computes every assignment first and publishes the new
// assignment set only after all consumer readers have been constructed.
// The caller must hold c.mu.
func (c *Coordinator) rebalanceLocked(g *groupState) error {
	previousState := g.state
	g.state = StateRebalancing
	succeeded := false
	defer func() {
		if !succeeded {
			g.state = previousState
		}
	}()

	assignments := make(map[string][]*topic.Partition, len(g.members))
	for memberID := range g.members {
		assignments[memberID] = nil
	}

	topicMembers := make(map[string][]string)
	for memberID, member := range g.members {
		for _, topicName := range member.subscriptions {
			topicMembers[topicName] = append(topicMembers[topicName], memberID)
		}
	}

	topicNames := make([]string, 0, len(topicMembers))
	for topicName := range topicMembers {
		topicNames = append(topicNames, topicName)
	}
	sort.Strings(topicNames)

	for _, topicName := range topicNames {
		members := topicMembers[topicName]
		sort.Strings(members)

		registeredTopic, err := c.registry.Get(topicName)
		if err != nil {
			return fmt.Errorf("resolve subscribed topic %q: %w", topicName, err)
		}

		partitions := registeredTopic.Partitions()
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].ID() < partitions[j].ID()
		})

		for i, partition := range partitions {
			owner := members[i%len(members)]
			assignments[owner] = append(assignments[owner], partition)
		}
	}

	memberIDs := make([]string, 0, len(g.members))
	for memberID := range g.members {
		memberIDs = append(memberIDs, memberID)
	}
	sort.Strings(memberIDs)

	type rebuiltMember struct {
		reader      *consumer.Consumer
		assignments []TopicPartition
	}
	rebuilt := make(map[string]rebuiltMember, len(g.members))

	for _, memberID := range memberIDs {
		member := g.members[memberID]
		partitions := assignments[memberID]
		if len(partitions) == 0 {
			rebuilt[memberID] = rebuiltMember{}
			continue
		}

		reader, err := consumer.New(partitions...)
		if err != nil {
			return fmt.Errorf("create consumer for member %q: %w", memberID, err)
		}

		// Preserve an in-memory position when the same member retains ownership.
		// When a partition is newly assigned, restore its durable committed
		// next offset if one exists. Without a commit, consumer.New starts at the
		// earliest retained offset.
		for _, partition := range partitions {
			topicName := partition.TopicName()
			partitionID := partition.ID()
			retainedBySameMember := member.reader != nil && member.reader.HasPartition(topicName, partitionID)
			if retainedBySameMember {
				position, positionErr := member.reader.Position(topicName, partitionID)
				if positionErr == nil {
					if setErr := reader.SetPosition(topicName, partitionID, position); setErr == nil {
						continue
					}
					// If retention invalidated the old in-memory cursor, keep the
					// new reader's earliest-retained initial position.
					continue
				}
			}

			if c.offsetStore != nil {
				key := offsets.Key{GroupID: g.id, Topic: topicName, PartitionID: partitionID}
				commit, exists, commitErr := c.offsetStore.Get(key)
				if commitErr != nil {
					return fmt.Errorf("load committed offset for group %q topic %q partition %d: %w", g.id, topicName, partitionID, commitErr)
				}
				if exists {
					if setErr := reader.SetPosition(topicName, partitionID, commit.NextOffset); setErr != nil {
						return errors.Join(fmt.Errorf("%w: group=%q topic=%q partition=%d offset=%d", ErrCommittedOffsetOutOfRange, g.id, topicName, partitionID, commit.NextOffset), setErr)
					}
				}
			}
		}

		memberAssignments := make([]TopicPartition, 0, len(partitions))
		for _, partition := range partitions {
			memberAssignments = append(memberAssignments, TopicPartition{
				Topic:       partition.TopicName(),
				PartitionID: partition.ID(),
			})
		}
		sort.Slice(memberAssignments, func(i, j int) bool {
			if memberAssignments[i].Topic != memberAssignments[j].Topic {
				return memberAssignments[i].Topic < memberAssignments[j].Topic
			}
			return memberAssignments[i].PartitionID < memberAssignments[j].PartitionID
		})

		rebuilt[memberID] = rebuiltMember{
			reader:      reader,
			assignments: memberAssignments,
		}
	}

	for _, memberID := range memberIDs {
		member := g.members[memberID]
		member.partitions = append([]*topic.Partition(nil), assignments[memberID]...)
		member.reader = rebuilt[memberID].reader
		member.assignments = rebuilt[memberID].assignments
	}

	g.generation++
	if len(g.members) == 0 {
		g.state = StateEmpty
	} else {
		g.state = StateStable
	}
	succeeded = true
	return nil
}

func validateIdentifier(value string) error {
	if len(value) == 0 || len(value) > maxIdentifierLength {
		return errors.New("identifier length must be between 1 and 128")
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		valid := (ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '.' || ch == '_' || ch == '-'
		if !valid {
			return fmt.Errorf("invalid identifier character %q", ch)
		}
	}
	return nil
}

func containsAssignment(assignments []TopicPartition, topicName string, partitionID uint32) bool {
	for _, assignment := range assignments {
		if assignment.Topic == topicName && assignment.PartitionID == partitionID {
			return true
		}
	}
	return false
}

func cloneAssignments(assignments []TopicPartition) []TopicPartition {
	return append([]TopicPartition(nil), assignments...)
}
