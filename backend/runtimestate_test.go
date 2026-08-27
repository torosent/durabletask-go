package backend

import (
	"testing"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestOrchestrationRuntimeStateSnapshotIncludesRelationships(t *testing.T) {
	parent := helpers.NewParentInfo(3, "parent", "parent-instance")
	state := NewOrchestrationRuntimeState("child-instance", []*HistoryEvent{
		helpers.NewExecutionStartedEvent(
			"child",
			"child-instance",
			nil,
			parent,
			nil,
			nil,
			wrapperspb.String("v2"),
		),
		helpers.NewSubOrchestrationCreatedEvent(0, "z-child", nil, nil, "z", nil),
		helpers.NewSubOrchestrationCreatedEvent(1, "a-child", nil, nil, "a", nil),
	})
	if err := state.AddEvent(helpers.NewTaskScheduledEvent(2, "activity", nil, nil, nil)); err != nil {
		t.Fatal(err)
	}

	snapshot := state.Snapshot()
	if snapshot.InstanceID != "child-instance" ||
		snapshot.Name != "child" ||
		snapshot.Version != "v2" ||
		snapshot.ParentInstanceID != "parent-instance" {
		t.Fatalf("unexpected snapshot identity: %+v", snapshot)
	}
	if snapshot.HistoryLength != 4 || snapshot.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING {
		t.Fatalf("unexpected snapshot state: %+v", snapshot)
	}
	if len(snapshot.ChildInstanceIDs) != 2 ||
		snapshot.ChildInstanceIDs[0] != "a" ||
		snapshot.ChildInstanceIDs[1] != "z" {
		t.Fatalf("unexpected child IDs: %v", snapshot.ChildInstanceIDs)
	}
}
