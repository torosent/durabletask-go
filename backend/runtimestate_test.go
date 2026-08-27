package backend

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
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

func TestContinueAsNewDropsCorrelatedPendingWork(t *testing.T) {
	state := NewOrchestrationRuntimeState("parent", []*HistoryEvent{
		helpers.NewExecutionStartedEvent("parent", "parent", nil, nil, nil, nil),
	})
	signal := &protos.OrchestratorAction{
		Id: 4,
		OrchestratorActionType: &protos.OrchestratorAction_SendEntityMessage{
			SendEntityMessage: &protos.SendEntityMessageAction{
				EntityMessageType: &protos.SendEntityMessageAction_EntityOperationSignaled{
					EntityOperationSignaled: &protos.EntityOperationSignaledEvent{
						RequestId:        uuid.NewString(),
						Operation:        "signal",
						TargetInstanceId: wrapperspb.String("@counter@one"),
					},
				},
			},
		},
	}
	continued, err := state.ApplyActions([]*protos.OrchestratorAction{
		helpers.NewScheduleTaskAction(0, "activity", nil),
		helpers.NewCreateTimerAction(1, time.Now().Add(time.Minute)),
		helpers.NewCreateSubOrchestrationAction(2, "child", "child", nil),
		func() *protos.OrchestratorAction {
			action := helpers.NewSendEventAction("target", "event", wrapperspb.String("1"))
			action.Id = 3
			return action
		}(),
		signal,
		helpers.NewTerminateOrchestrationAction(5, "victim", false, nil),
		helpers.NewCompleteOrchestrationAction(
			6,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
			wrapperspb.String("1"),
			nil,
			nil,
		),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !continued {
		t.Fatal("expected ContinueAsNew")
	}
	if len(state.pendingTasks) != 0 || len(state.pendingTimers) != 0 {
		t.Fatalf("correlated pending work carried over: tasks=%d timers=%d", len(state.pendingTasks), len(state.pendingTimers))
	}
	if len(state.pendingMessages) != 2 ||
		state.pendingMessages[0].HistoryEvent.GetEventRaised() == nil ||
		state.pendingMessages[1].HistoryEvent.GetExecutionTerminated() == nil {
		t.Fatalf("unexpected orchestration messages: %v", state.pendingMessages)
	}
	if len(state.pendingEntityMessages) != 1 ||
		state.pendingEntityMessages[0].HistoryEvent.GetEntityOperationSignaled() == nil {
		t.Fatalf("unexpected entity messages: %v", state.pendingEntityMessages)
	}
	if state.instanceID != api.InstanceID("parent") {
		t.Fatalf("instance ID changed: %s", state.instanceID)
	}
}

func TestContinueAsNewAppliesNewVersion(t *testing.T) {
	state := NewOrchestrationRuntimeState("instance", []*HistoryEvent{
		helpers.NewExecutionStartedEvent(
			"orchestration",
			"instance",
			nil,
			nil,
			nil,
			nil,
			wrapperspb.String("v1"),
		),
	})
	action := helpers.NewCompleteOrchestrationAction(
		0,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
		nil,
		nil,
		nil,
	)
	action.GetCompleteOrchestration().NewVersion = wrapperspb.String("v2")

	continued, err := state.ApplyActions([]*protos.OrchestratorAction{action}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !continued {
		t.Fatal("expected ContinueAsNew")
	}
	if got := state.startEvent.GetVersion().GetValue(); got != "v2" {
		t.Fatalf("continued version = %q, want v2", got)
	}
	if !state.ContinueAsNewVersionChanged() {
		t.Fatal("expected version-changing ContinueAsNew boundary")
	}
}

func TestContinueAsNewCanMigrateToUnversioned(t *testing.T) {
	state := NewOrchestrationRuntimeState("instance", []*HistoryEvent{
		helpers.NewExecutionStartedEvent(
			"orchestration",
			"instance",
			nil,
			nil,
			nil,
			nil,
			wrapperspb.String("v1"),
		),
	})
	action := helpers.NewCompleteOrchestrationAction(
		0,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
		nil,
		nil,
		nil,
	)
	action.GetCompleteOrchestration().NewVersion = wrapperspb.String("")

	continued, err := state.ApplyActions([]*protos.OrchestratorAction{action}, nil)
	require.NoError(t, err)
	require.True(t, continued)
	require.True(t, state.ContinueAsNewVersionChanged())
	require.NotNil(t, state.startEvent.Version)
	require.Empty(t, state.startEvent.Version.GetValue())
}
