package broker

import (
	"path/filepath"
	"testing"

	"github.com/ARIN00-web/messlessmq/internal/group"
	"github.com/ARIN00-web/messlessmq/internal/offsets"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

func TestGroupHandlerJoinFetchAndCommit(t *testing.T) {
	registry := topic.NewRegistry()
	registered, err := registry.CreateWithPartitionCount("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := registered.OpenLogs(filepath.Join(t.TempDir(), "logs"), 1<<20); err != nil {
		t.Fatal(err)
	}
	defer registered.CloseLogs()
	partition, err := registered.Partition(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partition.AppendAtSync([]byte("created"), 10); err != nil {
		t.Fatal(err)
	}
	store, err := offsets.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	coordinator, err := group.NewCoordinator(registry, store)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewGroupHandler(coordinator)

	call := func(input protocol.GroupRequest) protocol.GroupResponse {
		payload, err := protocol.EncodeGroupRequest(input)
		if err != nil {
			t.Fatal(err)
		}
		response, err := handler.Handle(protocol.Request{APIKey: protocol.APIGROUP, APIVersion: protocol.GroupAPIVersion, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := protocol.DecodeGroupResponse(response)
		if err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	joined := call(protocol.GroupRequest{Operation: protocol.GroupJoin, GroupID: "workers", MemberID: "member-1", Topics: []string{"orders"}})
	if joined.Generation == 0 || len(joined.Assignments) != 1 {
		t.Fatalf("join response = %+v", joined)
	}
	fetched := call(protocol.GroupRequest{Operation: protocol.GroupFetch, GroupID: "workers", MemberID: "member-1", Generation: joined.Generation, Topic: "orders", PartitionID: 0, MaxMessages: 10})
	if fetched.Fetch == nil || len(fetched.Fetch.Records) != 1 || string(fetched.Fetch.Records[0].Value) != "created" {
		t.Fatalf("fetch response = %+v", fetched)
	}
	committed := call(protocol.GroupRequest{Operation: protocol.GroupCommit, GroupID: "workers", MemberID: "member-1", Generation: joined.Generation, Topic: "orders", PartitionID: 0, Offset: fetched.Fetch.NextOffset})
	if !committed.HasOffset || committed.Offset != 1 {
		t.Fatalf("commit response = %+v", committed)
	}
	offset := call(protocol.GroupRequest{Operation: protocol.GroupFetchOffset, GroupID: "workers", Topic: "orders", PartitionID: 0})
	if !offset.HasOffset || offset.Offset != 1 {
		t.Fatalf("offset response = %+v", offset)
	}
}
