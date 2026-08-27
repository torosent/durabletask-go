package backend

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestWorkItemMatchesFilters(t *testing.T) {
	orchestration := &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{OrchestratorRequest: &protos.OrchestratorRequest{
			NewEvents: []*protos.HistoryEvent{{
				EventType: &protos.HistoryEvent_ExecutionStarted{
					ExecutionStarted: &protos.ExecutionStartedEvent{
						Name:    "orchestration",
						Version: wrapperspb.String("v1"),
					},
				},
			}},
		}},
	}

	activity := &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
			Name:    "activity",
			Version: wrapperspb.String("v2"),
		}},
	}
	require.True(t, workItemMatchesFilters(&protos.WorkItemFilters{}, orchestration))
	require.True(t, workItemMatchesFilters(&protos.WorkItemFilters{
		Orchestrations: []*protos.OrchestrationFilter{{Name: "orchestration", Versions: []string{"v1"}}},
	}, orchestration))
	require.False(t, workItemMatchesFilters(&protos.WorkItemFilters{
		Orchestrations: []*protos.OrchestrationFilter{{Name: "other"}},
	}, orchestration))
	require.True(t, workItemMatchesFilters(&protos.WorkItemFilters{
		Activities: []*protos.ActivityFilter{{Name: "activity", Versions: []string{"v2"}}},
	}, activity))
	require.False(t, workItemMatchesFilters(&protos.WorkItemFilters{
		Activities: []*protos.ActivityFilter{{Name: "activity", Versions: []string{"v1"}}},
	}, activity))
}

func TestDispatchWorkItemRoutesToMatchingSubscriber(t *testing.T) {
	executor := &grpcExecutor{
		workItemSubscribers: &sync.Map{},
		shutdownChan:        make(chan struct{}),
	}

	other := &workItemSubscriber{
		filters: &protos.WorkItemFilters{
			Orchestrations: []*protos.OrchestrationFilter{{Name: "other"}},
		},
		queue: make(chan *protos.WorkItem),
	}
	matching := &workItemSubscriber{
		filters: &protos.WorkItemFilters{
			Orchestrations: []*protos.OrchestrationFilter{{Name: "orchestration"}},
		},
		queue: make(chan *protos.WorkItem),
	}
	executor.workItemSubscribers.Store(uint64(1), other)
	executor.workItemSubscribers.Store(uint64(2), matching)
	workItem := &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{OrchestratorRequest: &protos.OrchestratorRequest{
			NewEvents: []*protos.HistoryEvent{{
				EventType: &protos.HistoryEvent_ExecutionStarted{
					ExecutionStarted: &protos.ExecutionStartedEvent{Name: "orchestration"},
				},
			}},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	received := make(chan *protos.WorkItem, 1)
	go func() {
		received <- <-matching.queue
	}()
	require.NoError(t, executor.dispatchWorkItem(ctx, workItem))
	require.Same(t, workItem, <-received)
	select {
	case <-other.queue:
		t.Fatal("work item routed to non-matching subscriber")
	default:
	}
}

func TestGrpcExecutorShutdownClaimsPendingCompletions(t *testing.T) {
	executor := &grpcExecutor{
		pendingActivities:    &sync.Map{},
		pendingOrchestrators: &sync.Map{},
		shutdownChan:         make(chan struct{}),
	}
	orchestration := &ExecutionResults{complete: make(chan struct{})}
	activity := &activityExecutionResult{complete: make(chan struct{})}
	executor.pendingOrchestrators.Store(api.InstanceID("instance"), orchestration)
	executor.pendingActivities.Store("instance/1", activity)

	require.NoError(t, executor.Shutdown(context.Background()))
	require.NoError(t, executor.Shutdown(context.Background()))
	_, orchestrationPending := executor.pendingOrchestrators.Load("instance")
	_, activityPending := executor.pendingActivities.Load("instance/1")
	require.False(t, orchestrationPending)
	require.False(t, activityPending)
	select {
	case <-orchestration.complete:
	default:
		t.Fatal("orchestration completion was not closed")
	}
	select {
	case <-activity.complete:
	default:
		t.Fatal("activity completion was not closed")
	}
}
