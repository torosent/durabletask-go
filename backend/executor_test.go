package backend

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type capturingBackend struct {
	created []*HistoryEvent
	added   []*HistoryEvent
	addedTo []api.InstanceID
	signals []*protos.SignalEntityRequest
}

func (*capturingBackend) CreateTaskHub(context.Context) error { return nil }
func (*capturingBackend) DeleteTaskHub(context.Context) error { return nil }
func (*capturingBackend) Start(context.Context) error         { return nil }
func (*capturingBackend) Stop(context.Context) error          { return nil }
func (b *capturingBackend) CreateOrchestrationInstance(_ context.Context, e *HistoryEvent, _ ...OrchestrationIdReusePolicyOptions) error {
	b.created = append(b.created, e)
	return nil
}
func (b *capturingBackend) AddNewOrchestrationEvent(_ context.Context, iid api.InstanceID, e *HistoryEvent) error {
	b.addedTo = append(b.addedTo, iid)
	b.added = append(b.added, e)
	return nil
}
func (*capturingBackend) GetOrchestrationWorkItem(context.Context) (*OrchestrationWorkItem, error) {
	return nil, nil
}
func (*capturingBackend) GetOrchestrationRuntimeState(context.Context, *OrchestrationWorkItem) (*OrchestrationRuntimeState, error) {
	return nil, nil
}
func (*capturingBackend) GetOrchestrationMetadata(context.Context, api.InstanceID) (*api.OrchestrationMetadata, error) {
	return nil, nil
}
func (*capturingBackend) CompleteOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error {
	return nil
}
func (*capturingBackend) AbandonOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error {
	return nil
}
func (*capturingBackend) GetActivityWorkItem(context.Context) (*ActivityWorkItem, error) {
	return nil, nil
}
func (*capturingBackend) CompleteActivityWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}
func (*capturingBackend) AbandonActivityWorkItem(context.Context, *ActivityWorkItem) error {
	return nil
}
func (*capturingBackend) PurgeOrchestrationState(context.Context, api.InstanceID) error { return nil }
func (b *capturingBackend) SignalEntity(_ context.Context, request *protos.SignalEntityRequest) error {
	b.signals = append(b.signals, request)
	return nil
}

func Test_GrpcExecutor_ExecuteEntity_RejectsConcurrentInstance(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)

	req := &protos.EntityBatchRequest{InstanceId: "@counter@key"}
	g.pendingEntityInstances.Store(req.InstanceId, "token")

	_, err := g.ExecuteEntity(context.Background(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already pending")
}

func Test_GrpcExecutor_StartInstance_RejectsEntityInstanceID(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)

	_, err := g.StartInstance(context.Background(), &protos.CreateInstanceRequest{
		Name:       "orchestrator",
		InstanceId: "@counter@key",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved entity format")
}

func Test_GrpcExecutor_SignalEntity_PreservesScheduledTimeAndRequestID(t *testing.T) {
	be := &capturingBackend{}
	executor, _ := NewGrpcExecutor(be, DefaultLogger())
	g := executor.(*grpcExecutor)

	scheduledTime := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Millisecond)
	requestID := uuid.NewString()
	_, err := g.SignalEntity(context.Background(), &protos.SignalEntityRequest{
		InstanceId:    "@counter@key",
		Name:          "increment",
		RequestId:     requestID,
		ScheduledTime: timestamppb.New(scheduledTime),
	})
	require.NoError(t, err)

	require.Len(t, be.signals, 1)
	require.Equal(t, requestID, be.signals[0].RequestId)
	require.Equal(t, "increment", be.signals[0].Name)
	require.WithinDuration(t, scheduledTime, be.signals[0].ScheduledTime.AsTime(), time.Millisecond)
}

func Test_GrpcExecutor_SignalEntity_RejectsInvalidRequestID(t *testing.T) {
	be := &capturingBackend{}
	executor, _ := NewGrpcExecutor(be, DefaultLogger())
	g := executor.(*grpcExecutor)

	_, err := g.SignalEntity(context.Background(), &protos.SignalEntityRequest{
		InstanceId: "@counter@key",
		Name:       "increment",
		RequestId:  "not-a-guid",
	})
	require.Error(t, err)
	require.Empty(t, be.signals)
}

func Test_GrpcExecutor_CompleteEntityTask_UsesCompletionToken(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)

	pending := &entityExecutionResult{instanceID: "@counter@one", complete: make(chan struct{})}
	g.pendingEntities.Store("token-one", pending)
	g.pendingEntityInstances.Store("@counter@one", "token-one")

	_, err := g.CompleteEntityTask(context.Background(), &protos.EntityBatchResult{
		CompletionToken: "token-one",
	})
	require.NoError(t, err)

	_, ok := g.pendingEntities.Load("token-one")
	assert.False(t, ok)
	_, ok = g.pendingEntityInstances.Load("@counter@one")
	assert.False(t, ok)
	select {
	case <-pending.complete:
	default:
		t.Fatal("entity completion did not unblock pending execution")
	}
}

func Test_GrpcExecutor_StaleEntityCompletionPreservesNewInstanceGuard(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)
	const instanceID = "@counter@one"
	pending := &entityExecutionResult{instanceID: instanceID, complete: make(chan struct{})}
	g.pendingEntities.Store("old-token", pending)
	g.pendingEntityInstances.Store(instanceID, "new-token")

	_, err := g.CompleteEntityTask(context.Background(), &protos.EntityBatchResult{
		CompletionToken: "old-token",
	})
	require.NoError(t, err)
	value, ok := g.pendingEntityInstances.Load(instanceID)
	require.True(t, ok)
	require.Equal(t, "new-token", value)
}

func Test_GrpcExecutor_AbandonOrchestratorTaskUnblocksPendingExecution(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)
	pending := &ExecutionResults{completionToken: "orchestration-token", complete: make(chan struct{})}
	g.pendingOrchestrators.Store(api.InstanceID("instance"), pending)
	g.pendingOrchestratorTokens.Store("orchestration-token", api.InstanceID("instance"))

	_, err := g.AbandonTaskOrchestratorWorkItem(
		context.Background(),
		&protos.AbandonOrchestrationTaskRequest{CompletionToken: "orchestration-token"},
	)
	require.NoError(t, err)
	select {
	case <-pending.complete:
	default:
		t.Fatal("orchestration abandon did not unblock pending execution")
	}
	require.Nil(t, pending.Response)
}

func Test_GrpcExecutor_AbandonActivityTaskUnblocksPendingExecution(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)
	pending := &activityExecutionResult{completionToken: "activity-token", complete: make(chan struct{})}
	g.pendingActivities.Store("instance/1", pending)
	g.pendingActivityTokens.Store("activity-token", "instance/1")

	_, err := g.AbandonTaskActivityWorkItem(
		context.Background(),
		&protos.AbandonActivityTaskRequest{CompletionToken: "activity-token"},
	)
	require.NoError(t, err)
	select {
	case <-pending.complete:
	default:
		t.Fatal("activity abandon did not unblock pending execution")
	}
	require.Nil(t, pending.response)
}

func Test_GrpcExecutor_StaleOrchestrationTokenDoesNotAbortReplacement(t *testing.T) {
	executor, _ := NewGrpcExecutor(nil, DefaultLogger())
	g := executor.(*grpcExecutor)
	instanceID := api.InstanceID("instance")
	replacement := &ExecutionResults{completionToken: "new-token", complete: make(chan struct{})}
	g.pendingOrchestrators.Store(instanceID, replacement)
	g.pendingOrchestratorTokens.Store("old-token", instanceID)
	g.pendingOrchestratorTokens.Store("new-token", instanceID)

	_, err := g.AbandonTaskOrchestratorWorkItem(
		context.Background(),
		&protos.AbandonOrchestrationTaskRequest{CompletionToken: "old-token"},
	)
	require.Error(t, err)
	value, ok := g.pendingOrchestrators.Load(instanceID)
	require.True(t, ok)
	require.Same(t, replacement, value)
	select {
	case <-replacement.complete:
		t.Fatal("stale token aborted the replacement execution")
	default:
	}
}
