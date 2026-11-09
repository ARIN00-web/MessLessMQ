package broker

import (
	"fmt"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/topic"
)

// Creates a topic, opens its partition logs, and initializes its assignment.
func newPartitionIntegrationTopic(
	t *testing.T,
	rootDir string,
	name string,
	partitionCount uint32,
) (*topic.Topic, *PartitionAssignment) {
	t.Helper()

	registry := topic.NewRegistry()

	testTopic, err := registry.CreateWithPartitionCount(
		name,
		partitionCount,
	)
	if err != nil {
		t.Fatalf("CreateWithPartitionCount(%q) error: %v", name, err)
	}

	if err := testTopic.OpenLogs(rootDir, 1024*1024); err != nil {
		t.Fatalf("OpenLogs(%q) error: %v", name, err)
	}

	t.Cleanup(func() {
		if err := testTopic.CloseLogs(); err != nil {
			t.Errorf("CloseLogs(%q) error: %v", name, err)
		}
	})

	assignment, err := NewPartitionAssignment(testTopic)
	if err != nil {
		t.Fatalf("NewPartitionAssignment(%q) error: %v", name, err)
	}

	return testTopic, assignment
}

// Different topics must have isolated storage, even when their partition
// IDs and offsets are identical.
func TestPartitionIntegrationTopicIsolationAndRecovery(t *testing.T) {
	rootDir := t.TempDir()

	orders, ordersAssignment := newPartitionIntegrationTopic(
		t,
		rootDir,
		"orders",
		3,
	)

	payments, paymentsAssignment := newPartitionIntegrationTopic(
		t,
		rootDir,
		"payments",
		3,
	)

	orderMessages := [][]byte{
		[]byte("order-created"),
		[]byte("order-confirmed"),
		[]byte("order-shipped"),
	}

	paymentMessages := [][]byte{
		[]byte("payment-initiated"),
		[]byte("payment-authorized"),
		[]byte("payment-completed"),
	}

	const key = "customer-42"

	var orderPartitionID uint32
	var paymentPartitionID uint32

	for i := range orderMessages {
		orderResult, err := ordersAssignment.Append(
			[]byte(key),
			orderMessages[i],
		)
		if err != nil {
			t.Fatalf("orders Append() error: %v", err)
		}

		paymentResult, err := paymentsAssignment.Append(
			[]byte(key),
			paymentMessages[i],
		)
		if err != nil {
			t.Fatalf("payments Append() error: %v", err)
		}

		if i == 0 {
			orderPartitionID = orderResult.PartitionID
			paymentPartitionID = paymentResult.PartitionID
		}

		if orderResult.PartitionID != orderPartitionID {
			t.Fatalf("orders key moved between partitions")
		}

		if paymentResult.PartitionID != paymentPartitionID {
			t.Fatalf("payments key moved between partitions")
		}

		if orderResult.Offset != uint64(i) {
			t.Fatalf(
				"orders offset = %d, want %d",
				orderResult.Offset,
				i,
			)
		}

		if paymentResult.Offset != uint64(i) {
			t.Fatalf(
				"payments offset = %d, want %d",
				paymentResult.Offset,
				i,
			)
		}
	}

	// Both topics may use the same partition ID, but they must have
	// separate partition objects and storage logs.
	orderPartition, err := orders.Partition(orderPartitionID)
	if err != nil {
		t.Fatal(err)
	}

	paymentPartition, err := payments.Partition(paymentPartitionID)
	if err != nil {
		t.Fatal(err)
	}

	if orderPartition == paymentPartition {
		t.Fatal("different topics unexpectedly share a partition object")
	}

	if orderPartition.TopicName() == paymentPartition.TopicName() {
		t.Fatal("different topics have the same topic identity")
	}

	// Verify data isolation before closing.
	for i := range orderMessages {
		orderGot, err := orderPartition.Read(uint64(i))
		if err != nil {
			t.Fatalf("orders Read(%d) error: %v", i, err)
		}

		paymentGot, err := paymentPartition.Read(uint64(i))
		if err != nil {
			t.Fatalf("payments Read(%d) error: %v", i, err)
		}

		if string(orderGot) != string(orderMessages[i]) {
			t.Errorf(
				"orders Read(%d) = %q, want %q",
				i,
				orderGot,
				orderMessages[i],
			)
		}

		if string(paymentGot) != string(paymentMessages[i]) {
			t.Errorf(
				"payments Read(%d) = %q, want %q",
				i,
				paymentGot,
				paymentMessages[i],
			)
		}
	}

	// Simulate a broker restart.
	if err := orders.CloseLogs(); err != nil {
		t.Fatalf("orders CloseLogs() error: %v", err)
	}

	if err := payments.CloseLogs(); err != nil {
		t.Fatalf("payments CloseLogs() error: %v", err)
	}

	// Reconstruct topics using the same persisted partition layout.
	restartedOrders, _ := newPartitionIntegrationTopic(
		t,
		rootDir,
		"orders",
		3,
	)

	restartedPayments, _ := newPartitionIntegrationTopic(
		t,
		rootDir,
		"payments",
		3,
	)

	restartedOrderPartition, err := restartedOrders.Partition(orderPartitionID)
	if err != nil {
		t.Fatal(err)
	}

	restartedPaymentPartition, err := restartedPayments.Partition(paymentPartitionID)
	if err != nil {
		t.Fatal(err)
	}

	// All messages must survive independently.
	for i := range orderMessages {
		orderGot, err := restartedOrderPartition.Read(uint64(i))
		if err != nil {
			t.Fatalf("restarted orders Read(%d) error: %v", i, err)
		}

		paymentGot, err := restartedPaymentPartition.Read(uint64(i))
		if err != nil {
			t.Fatalf("restarted payments Read(%d) error: %v", i, err)
		}

		if string(orderGot) != string(orderMessages[i]) {
			t.Errorf("restarted orders message %d corrupted", i)
		}

		if string(paymentGot) != string(paymentMessages[i]) {
			t.Errorf("restarted payments message %d corrupted", i)
		}
	}

	orderNext, err := restartedOrderPartition.NextOffset()
	if err != nil {
		t.Fatal(err)
	}

	paymentNext, err := restartedPaymentPartition.NextOffset()
	if err != nil {
		t.Fatal(err)
	}

	if orderNext != uint64(len(orderMessages)) {
		t.Fatalf("orders NextOffset() = %d", orderNext)
	}

	if paymentNext != uint64(len(paymentMessages)) {
		t.Fatalf("payments NextOffset() = %d", paymentNext)
	}
}

// Expanding a topic must preserve all old partition data and create
// independent persistent logs for newly added partitions.
func TestPartitionIntegrationExpansionSurvivesRestart(t *testing.T) {
	rootDir := t.TempDir()

	testTopic, initialAssignment := newPartitionIntegrationTopic(
		t,
		rootDir,
		"orders",
		2,
	)

	oldResult, err := initialAssignment.Append(
		[]byte("customer-42"),
		[]byte("before-expansion"),
	)
	if err != nil {
		t.Fatalf("initial Append() error: %v", err)
	}

	// Expand from two partitions to four while logs are open.
	if err := testTopic.AddPartitions(2); err != nil {
		t.Fatalf("AddPartitions(2) error: %v", err)
	}

	if testTopic.PartitionCount() != 4 {
		t.Fatalf(
			"PartitionCount() = %d, want 4",
			testTopic.PartitionCount(),
		)
	}

	// Build a fresh assignment using the new partition count.
	newAssignment, err := NewPartitionAssignment(testTopic)
	if err != nil {
		t.Fatalf("NewPartitionAssignment() after expansion: %v", err)
	}

	var newResults []PartitionAppendResult

	for i := 0; i < 4; i++ {
		message := []byte(fmt.Sprintf("after-expansion-%d", i))

		result, err := newAssignment.Append(nil, message)
		if err != nil {
			t.Fatalf("post-expansion Append() error: %v", err)
		}

		newResults = append(newResults, result)
	}

	// Close all partition logs before reconstructing the topic.
	if err := testTopic.CloseLogs(); err != nil {
		t.Fatalf("CloseLogs() error: %v", err)
	}

	// Recreate topic metadata with the expanded partition count.
	restartedTopic, _ := newPartitionIntegrationTopic(
		t,
		rootDir,
		"orders",
		4,
	)

	if restartedTopic.PartitionCount() != 4 {
		t.Fatalf("restarted topic partition count is incorrect")
	}

	// Verify the old message.
	oldPartition, err := restartedTopic.Partition(oldResult.PartitionID)
	if err != nil {
		t.Fatalf("old partition lookup error: %v", err)
	}

	oldMessage, err := oldPartition.Read(oldResult.Offset)
	if err != nil {
		t.Fatalf("old message Read() error: %v", err)
	}

	if string(oldMessage) != "before-expansion" {
		t.Fatalf(
			"old message = %q, want before-expansion",
			oldMessage,
		)
	}

	// Verify every message written after expansion.
	for i, result := range newResults {
		partition, err := restartedTopic.Partition(result.PartitionID)
		if err != nil {
			t.Fatalf("Partition(%d) error: %v", result.PartitionID, err)
		}

		got, err := partition.Read(result.Offset)
		if err != nil {
			t.Fatalf("Read() after restart error: %v", err)
		}

		want := fmt.Sprintf("after-expansion-%d", i)

		if string(got) != want {
			t.Errorf(
				"message = %q, want %q",
				got,
				want,
			)
		}
	}
}
