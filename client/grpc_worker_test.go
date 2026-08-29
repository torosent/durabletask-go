package client

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type fakeWorkItemResult struct {
	item *protos.WorkItem
	err  error
}

type fakeWorkItemsStream struct {
	protos.TaskHubSidecarService_GetWorkItemsClient
	ctx     context.Context
	results chan fakeWorkItemResult
}

func (s *fakeWorkItemsStream) Recv() (*protos.WorkItem, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case result := <-s.results:
		return result.item, result.err
	}
}

type fakeHistoryStream struct {
	protos.TaskHubSidecarService_StreamInstanceHistoryClient
	ctx    context.Context
	chunks []*protos.HistoryChunk
	block  bool
	index  int
}

func (s *fakeHistoryStream) Recv() (*protos.HistoryChunk, error) {
	if s.index < len(s.chunks) {
		chunk := s.chunks[s.index]
		s.index++
		return chunk, nil
	}
	if s.block {
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
	return nil, io.EOF
}

func TestMaximumTimerIntervalWorkerOption(t *testing.T) {
	options := defaultTaskHubGrpcWorkerOptions()
	require.Nil(t, options.maximumTimerInterval)
	require.Empty(t, options.executorOptions())
	require.NoError(t, WithTaskExecutorOptions(task.WithMaximumTimerInterval(time.Hour))(&options))
	require.Len(t, options.executorOptions(), 1)
	require.Error(t, WithMaximumTimerInterval(-time.Second)(&options))
	require.NoError(t, WithMaximumTimerInterval(0)(&options))
	require.Equal(t, task.DefaultMaximumTimerInterval, *options.maximumTimerInterval)
	require.Len(t, options.executorOptions(), 2)
	require.NoError(t, WithMaximumTimerInterval(2*time.Hour)(&options))
	require.Equal(t, 2*time.Hour, *options.maximumTimerInterval)
}

type fakeSchedulerClient struct {
	protos.TaskHubSidecarServiceClient

	mu sync.Mutex

	helloErr error
	stream   *fakeWorkItemsStream
	request  *protos.GetWorkItemsRequest

	history       []*protos.HistoryChunk
	historyBlocks bool

	orchestrationCompletions []*protos.OrchestratorResponse
	activityCompletions      []*protos.ActivityResponse
	entityCompletions        []*protos.EntityBatchResult
	activityCompletionErr    error
	orchestrationAbandons    int
	activityAbandons         int
	entityAbandonAttempts    int
	entityAbandonFailures    int
}

func (c *fakeSchedulerClient) Hello(context.Context, *emptypb.Empty, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, c.helloErr
}

func (c *fakeSchedulerClient) GetWorkItems(
	ctx context.Context,
	request *protos.GetWorkItemsRequest,
	_ ...grpc.CallOption,
) (protos.TaskHubSidecarService_GetWorkItemsClient, error) {
	c.mu.Lock()
	c.request = request
	c.stream.ctx = ctx
	c.mu.Unlock()
	return c.stream, nil
}

func (c *fakeSchedulerClient) CompleteOrchestratorTask(
	_ context.Context,
	response *protos.OrchestratorResponse,
	_ ...grpc.CallOption,
) (*protos.CompleteTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orchestrationCompletions = append(c.orchestrationCompletions, response)
	return &protos.CompleteTaskResponse{}, nil
}

func (c *fakeSchedulerClient) CompleteActivityTask(
	_ context.Context,
	response *protos.ActivityResponse,
	_ ...grpc.CallOption,
) (*protos.CompleteTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activityCompletions = append(c.activityCompletions, response)
	return &protos.CompleteTaskResponse{}, c.activityCompletionErr
}

func (c *fakeSchedulerClient) CompleteEntityTask(
	_ context.Context,
	response *protos.EntityBatchResult,
	_ ...grpc.CallOption,
) (*protos.CompleteTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entityCompletions = append(c.entityCompletions, response)
	return &protos.CompleteTaskResponse{}, nil
}

func (c *fakeSchedulerClient) StreamInstanceHistory(
	ctx context.Context,
	_ *protos.StreamInstanceHistoryRequest,
	_ ...grpc.CallOption,
) (protos.TaskHubSidecarService_StreamInstanceHistoryClient, error) {
	return &fakeHistoryStream{ctx: ctx, chunks: c.history, block: c.historyBlocks}, nil
}

func (c *fakeSchedulerClient) AbandonTaskOrchestratorWorkItem(
	_ context.Context,
	_ *protos.AbandonOrchestrationTaskRequest,
	_ ...grpc.CallOption,
) (*protos.AbandonOrchestrationTaskResponse, error) {
	c.mu.Lock()
	c.orchestrationAbandons++
	c.mu.Unlock()
	return &protos.AbandonOrchestrationTaskResponse{}, nil
}

func (c *fakeSchedulerClient) AbandonTaskActivityWorkItem(
	_ context.Context,
	_ *protos.AbandonActivityTaskRequest,
	_ ...grpc.CallOption,
) (*protos.AbandonActivityTaskResponse, error) {
	c.mu.Lock()
	c.activityAbandons++
	c.mu.Unlock()
	return &protos.AbandonActivityTaskResponse{}, nil
}

func (c *fakeSchedulerClient) AbandonTaskEntityWorkItem(
	context.Context,
	*protos.AbandonEntityTaskRequest,
	...grpc.CallOption,
) (*protos.AbandonEntityTaskResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entityAbandonAttempts++
	if c.entityAbandonAttempts <= c.entityAbandonFailures {
		return nil, status.Error(codes.Unavailable, "transient")
	}
	return &protos.AbandonEntityTaskResponse{}, nil
}

type recordingExecutor struct {
	executeOrchestrator func(context.Context, api.InstanceID, []*protos.HistoryEvent, []*protos.HistoryEvent) (*backend.ExecutionResults, error)
	executeActivity     func(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
	executeEntity       func(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error)
}

func (e *recordingExecutor) ExecuteOrchestrator(
	ctx context.Context,
	id api.InstanceID,
	pastEvents []*protos.HistoryEvent,
	newEvents []*protos.HistoryEvent,
) (*backend.ExecutionResults, error) {
	return e.executeOrchestrator(ctx, id, pastEvents, newEvents)
}

func (e *recordingExecutor) ExecuteActivity(
	ctx context.Context,
	id api.InstanceID,
	event *protos.HistoryEvent,
) (*protos.HistoryEvent, error) {
	return e.executeActivity(ctx, id, event)
}

func (*recordingExecutor) Shutdown(context.Context) error {
	return nil
}

func (e *recordingExecutor) ExecuteEntity(
	ctx context.Context,
	request *protos.EntityBatchRequest,
) (*protos.EntityBatchResult, error) {
	return e.executeEntity(ctx, request)
}

func newFakeWorker(t *testing.T, client *fakeSchedulerClient, opts ...TaskHubGrpcWorkerOption) *TaskHubGrpcWorker {
	t.Helper()
	registry := task.NewTaskRegistry()
	options := []TaskHubGrpcWorkerOption{
		WithWorkerHelloTimeout(time.Second),
		WithWorkerSilentDisconnectTimeout(time.Second),
		WithWorkerRPCTimeout(time.Second),
		WithWorkerReconnectBackoff(time.Millisecond, 5*time.Millisecond),
		WithWorkerTransientRetryPolicy(3, time.Millisecond, 5*time.Millisecond),
	}
	options = append(options, opts...)
	worker, err := newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			return client, nil, nil
		},
		registry,
		backend.DefaultLogger(),
		options...,
	)
	require.NoError(t, err)
	return worker
}

func newFakeWorkItemStream(buffer int) *fakeWorkItemsStream {
	return &fakeWorkItemsStream{results: make(chan fakeWorkItemResult, buffer)}
}

func TestTaskHubGrpcWorkerAdvertisesCapabilitiesAndCompletesActivity(t *testing.T) {
	stream := newFakeWorkItemStream(2)
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(
		t,
		client,
		WithMaxConcurrentOrchestrationWorkItems(2),
		WithMaxConcurrentActivityWorkItems(3),
		WithMaxConcurrentEntityWorkItems(4),
		WithWorkItemFilters(&WorkItemFilters{
			Entities: []string{"Counter"},
		}),
	)
	worker.executor = &recordingExecutor{
		executeActivity: func(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error) {
			return &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_TaskCompleted{
					TaskCompleted: &protos.TaskCompletedEvent{Result: wrapperspb.String(`"done"`)},
				},
			}, nil
		},
	}

	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_HealthPing{HealthPing: &protos.HealthPing{}},
	}}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
			Name:                  "activity",
			TaskId:                7,
			OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
		}},
		CompletionToken: "activity-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.activityCompletions) == 1
	}, time.Second, time.Millisecond)

	client.mu.Lock()
	require.EqualValues(t, 2, client.request.MaxConcurrentOrchestrationWorkItems)
	require.EqualValues(t, 3, client.request.MaxConcurrentActivityWorkItems)
	require.EqualValues(t, 4, client.request.MaxConcurrentEntityWorkItems)
	require.Equal(t, "counter", client.request.WorkItemFilters.Entities[0].Name)
	require.Equal(t, []protos.WorkerCapability{
		protos.WorkerCapability_WORKER_CAPABILITY_HISTORY_STREAMING,
	}, client.request.Capabilities)
	require.Equal(t, "activity-token", client.activityCompletions[0].CompletionToken)
	client.mu.Unlock()

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerDoesNotAbandonExpiredActivityLease(t *testing.T) {
	stream := newFakeWorkItemStream(1)
	client := &fakeSchedulerClient{
		stream:                stream,
		activityCompletionErr: status.Error(codes.NotFound, "work item not found"),
	}
	worker := newFakeWorker(t, client)
	worker.executor = &recordingExecutor{
		executeActivity: func(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error) {
			return &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_TaskCompleted{
					TaskCompleted: &protos.TaskCompletedEvent{Result: wrapperspb.String(`"done"`)},
				},
			}, nil
		},
	}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
			Name:                  "activity",
			TaskId:                7,
			OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
		}},
		CompletionToken: "expired-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.activityCompletions) == 1
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))

	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.activityCompletions, 1)
	require.Zero(t, client.activityAbandons)
}

func TestWorkItemFiltersApplyIndependentlyByKind(t *testing.T) {
	filters, err := cloneWorkItemFilters(&WorkItemFilters{
		Entities: []string{"Counter"},
	})
	require.NoError(t, err)
	require.True(t, matchesWorkItemFilters(filters, true, "orchestration", "v1"))
	require.True(t, matchesWorkItemFilters(filters, false, "activity", "v1"))
	require.True(t, matchesEntityWorkItemFilters(filters, "counter"))
	require.False(t, matchesEntityWorkItemFilters(filters, "other"))

	filters, err = cloneWorkItemFilters(&WorkItemFilters{RejectAllActivities: true})
	require.NoError(t, err)
	require.False(t, matchesWorkItemFilters(filters, false, "activity", "v1"))
	require.True(t, matchesWorkItemFilters(filters, true, "orchestration", "v1"))
	require.True(t, matchesEntityWorkItemFilters(filters, "counter"))
	wire := workItemFiltersToProto(&WorkItemFilters{
		RejectAllOrchestrations: true,
		RejectAllActivities:     true,
		RejectAllEntities:       true,
	})
	require.Equal(t, helpers.RejectAllWorkItemFilterName, wire.Orchestrations[0].Name)
	require.Equal(t, helpers.RejectAllWorkItemFilterName, wire.Activities[0].Name)
	require.Equal(t, helpers.RejectAllWorkItemFilterName, wire.Entities[0].Name)
}

func TestStrictAutoFiltersPreserveAllowedUnversionedOrchestrator(t *testing.T) {
	filters := workItemFiltersFromRegistry(
		task.TaskRegistrySnapshot{
			Orchestrators: []task.TaskRegistration{
				{Name: "system"},
				{Name: "application", Version: "1.0"},
			},
		},
		&task.VersioningOptions{Version: "1.0", MatchStrategy: task.VersionMatchStrict},
		map[string]struct{}{"system": {}},
		nil,
	)
	require.Contains(t, filters.Orchestrations, WorkItemFilter{Name: "system", Versions: []string{""}})
	require.Contains(t, filters.Orchestrations, WorkItemFilter{Name: "application", Versions: []string{"1.0"}})
}

// TestStrictAutoFiltersPreserveAllowedUnversionedActivity keeps a system
// component's unversioned activities routable under strict worker versioning.
// An activity inherits its caller's version, so an unversioned system
// orchestration schedules unversioned activities.
func TestStrictAutoFiltersPreserveAllowedUnversionedActivity(t *testing.T) {
	snapshot := task.TaskRegistrySnapshot{
		Activities: []task.TaskRegistration{
			{Name: "SystemActivity"},
			{Name: "application", Version: "1.0"},
		},
	}
	versioning := &task.VersioningOptions{Version: "1.0", MatchStrategy: task.VersionMatchStrict}

	// Without the allow-list the worker demands its own version for the system
	// activity, so the service never dispatches the unversioned work item.
	blocked := workItemFiltersFromRegistry(snapshot, versioning, nil, nil)
	require.Contains(t, blocked.Activities, WorkItemFilter{Name: "SystemActivity", Versions: []string{"1.0"}})

	allowed := workItemFiltersFromRegistry(
		snapshot, versioning, nil, map[string]struct{}{"systemactivity": {}})
	require.Contains(t, allowed.Activities, WorkItemFilter{Name: "SystemActivity", Versions: []string{""}})
	require.Contains(t, allowed.Activities, WorkItemFilter{Name: "application", Versions: []string{"1.0"}})
}

func TestWithUnversionedActivityNamesRejectsBlankNames(t *testing.T) {
	options := defaultTaskHubGrpcWorkerOptions()
	require.Error(t, WithUnversionedActivityNames("  ")(&options))
	require.NoError(t, WithUnversionedActivityNames("Alpha", "beta")(&options))
	require.Contains(t, options.unversionedActivities, "alpha")
	require.Contains(t, options.unversionedActivities, "beta")
}

func TestWorkItemFiltersFromRegistryMatchVersionedFallbackRules(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("legacy", func(*task.OrchestrationContext) (any, error) {
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("mixed", func(*task.OrchestrationContext) (any, error) {
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("mixed", "v2", func(*task.OrchestrationContext) (any, error) {
		return nil, nil
	}))
	require.NoError(t, registry.AddActivityNVersion("activity", "V1", func(task.ActivityContext) (any, error) {
		return nil, nil
	}))

	filters := workItemFiltersFromRegistry(registry.Snapshot(), nil, nil, nil)
	require.Equal(t, []WorkItemFilter{
		{Name: "legacy", Versions: []string{}},
		{Name: "mixed", Versions: []string{"", "v2"}},
	}, filters.Orchestrations)
	require.Equal(t, []WorkItemFilter{{Name: "activity", Versions: []string{"V1"}}}, filters.Activities)

	strict := workItemFiltersFromRegistry(registry.Snapshot(), &task.VersioningOptions{
		Version:       "v3",
		MatchStrategy: task.VersionMatchStrict,
	}, nil, nil)
	require.Equal(t, []string{"v3"}, strict.Orchestrations[0].Versions)
	require.Equal(t, []string{"v3"}, strict.Activities[0].Versions)
	require.Error(t, validateStrictAutoFilters(registry.Snapshot(), &task.VersioningOptions{
		Version:       "v3",
		MatchStrategy: task.VersionMatchStrict,
	}))
	require.True(t, filters.RejectAllEntities)
}

func TestWorkerVersioningOptionOverridesGenericExecutorVersioning(t *testing.T) {
	worker := newFakeWorker(
		t,
		&fakeSchedulerClient{},
		WithTaskVersioning(task.VersioningOptions{
			Version:         "1.0",
			MatchStrategy:   task.VersionMatchStrict,
			FailureStrategy: task.VersionFailureReject,
		}),
		WithTaskExecutorOptions(task.WithVersioning(task.VersioningOptions{
			Version:         "2.0",
			MatchStrategy:   task.VersionMatchStrict,
			FailureStrategy: task.VersionFailureReject,
		})),
	)

	accepted, err := worker.executor.ExecuteActivity(
		context.Background(),
		"instance",
		helpers.NewTaskScheduledEvent(1, "activity", wrapperspb.String("1.0"), nil, nil),
	)
	require.NoError(t, err)
	require.NotNil(t, accepted.GetTaskFailed(), "v1 should pass version acceptance and reach registry dispatch")

	_, err = worker.executor.ExecuteActivity(
		context.Background(),
		"instance",
		helpers.NewTaskScheduledEvent(1, "activity", wrapperspb.String("2.0"), nil, nil),
	)
	var mismatch *task.VersionMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "1.0", mismatch.WorkerVersion)
}

func TestExplicitWorkItemFiltersValidateRegisteredNames(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("known", func(*task.OrchestrationContext) (any, error) {
		return nil, nil
	}))
	err := validateWorkItemFilters(
		&WorkItemFilters{Orchestrations: []WorkItemFilter{{Name: "unknown"}}},
		registry.Snapshot(),
	)
	require.ErrorContains(t, err, "not registered")
}

func TestStrictAutoFiltersValidateNamedRegistrationsWithWildcard(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorNVersion("known", "v1", func(*task.OrchestrationContext) (any, error) {
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("*", func(*task.OrchestrationContext) (any, error) {
		return nil, nil
	}))
	require.Error(t, validateStrictAutoFilters(registry.Snapshot(), &task.VersioningOptions{
		Version:       "v2",
		MatchStrategy: task.VersionMatchStrict,
	}))
}

func TestTaskHubGrpcWorkerAdvertisesExplicitCapabilitiesAndFilters(t *testing.T) {
	stream := newFakeWorkItemStream(1)
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(
		t,
		client,
		WithScheduledTaskCapability(true),
		WithWorkItemFilters(&WorkItemFilters{
			Orchestrations: []WorkItemFilter{{Name: "orchestration", Versions: []string{"v1", "v2"}}},
			Activities:     []WorkItemFilter{{Name: "activity", Versions: []string{"v3"}}},
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.request != nil
	}, time.Second, time.Millisecond)

	client.mu.Lock()
	require.Equal(t, []protos.WorkerCapability{
		protos.WorkerCapability_WORKER_CAPABILITY_HISTORY_STREAMING,
		protos.WorkerCapability_WORKER_CAPABILITY_SCHEDULED_TASKS,
	}, client.request.Capabilities)
	require.Equal(t, []*protos.OrchestrationFilter{{
		Name:     "orchestration",
		Versions: []string{"v1", "v2"},
	}}, client.request.WorkItemFilters.Orchestrations)
	require.Equal(t, []*protos.ActivityFilter{{
		Name:     "activity",
		Versions: []string{"v3"},
	}}, client.request.WorkItemFilters.Activities)
	client.mu.Unlock()

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerLocallyRejectsFilteredWorkItems(t *testing.T) {
	stream := newFakeWorkItemStream(2)
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(
		t,
		client,
		WithWorkItemFilters(&WorkItemFilters{
			Orchestrations: []WorkItemFilter{{Name: "allowed-orchestration", Versions: []string{"v1"}}},
			Activities:     []WorkItemFilter{{Name: "allowed-activity", Versions: []string{"v1"}}},
		}),
	)
	var executionCount atomic.Int32
	worker.executor = &recordingExecutor{
		executeOrchestrator: func(
			context.Context,
			api.InstanceID,
			[]*protos.HistoryEvent,
			[]*protos.HistoryEvent,
		) (*backend.ExecutionResults, error) {
			executionCount.Add(1)
			return &backend.ExecutionResults{Response: &protos.OrchestratorResponse{}}, nil
		},
		executeActivity: func(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error) {
			executionCount.Add(1)
			return &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_TaskCompleted{
					TaskCompleted: &protos.TaskCompletedEvent{},
				},
			}, nil
		},
	}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{OrchestratorRequest: &protos.OrchestratorRequest{
			InstanceId: "instance",
			NewEvents: []*protos.HistoryEvent{{
				EventType: &protos.HistoryEvent_ExecutionStarted{
					ExecutionStarted: &protos.ExecutionStartedEvent{
						Name:    "other-orchestration",
						Version: wrapperspb.String("v1"),
					},
				},
			}},
		}},
		CompletionToken: "orchestration-token",
	}}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
			Name:                  "allowed-activity",
			Version:               wrapperspb.String("v2"),
			TaskId:                7,
			OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
		}},
		CompletionToken: "activity-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.orchestrationAbandons == 1 && client.activityAbandons == 1
	}, time.Second, time.Millisecond)
	require.Zero(t, executionCount.Load())

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerHydratesAndExternalizesLargePayloads(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	input, err := largepayload.Externalize(context.Background(), options, wrapperspb.String(`"large-input"`))
	require.NoError(t, err)

	stream := newFakeWorkItemStream(1)
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(t, client, WithWorkerLargePayloads(options))
	worker.executor = &recordingExecutor{
		executeActivity: func(_ context.Context, _ api.InstanceID, event *protos.HistoryEvent) (*protos.HistoryEvent, error) {
			require.Equal(t, `"large-input"`, event.GetTaskScheduled().GetInput().GetValue())
			return &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_TaskCompleted{
					TaskCompleted: &protos.TaskCompletedEvent{Result: wrapperspb.String(`"large-output"`)},
				},
			}, nil
		},
	}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
			Name:                  "activity",
			Input:                 input,
			TaskId:                7,
			OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
		}},
		CompletionToken: "activity-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.activityCompletions) == 1
	}, time.Second, time.Millisecond)

	client.mu.Lock()
	require.Contains(t, client.request.Capabilities, protos.WorkerCapability_WORKER_CAPABILITY_LARGE_PAYLOADS)
	result := client.activityCompletions[0].Result
	client.mu.Unlock()
	require.NotEqual(t, `"large-output"`, result.GetValue())
	hydrated, err := largepayload.Hydrate(context.Background(), options, result)
	require.NoError(t, err)
	require.Equal(t, `"large-output"`, hydrated.GetValue())

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerStreamsRequiredHistory(t *testing.T) {
	stream := newFakeWorkItemStream(1)
	pastEvent := &protos.HistoryEvent{EventId: 1}
	newEvent := &protos.HistoryEvent{EventId: 2}
	client := &fakeSchedulerClient{
		stream:  stream,
		history: []*protos.HistoryChunk{{Events: []*protos.HistoryEvent{pastEvent}}},
	}
	worker := newFakeWorker(t, client)

	executed := make(chan struct{})
	worker.executor = &recordingExecutor{
		executeOrchestrator: func(
			_ context.Context,
			_ api.InstanceID,
			pastEvents []*protos.HistoryEvent,
			newEvents []*protos.HistoryEvent,
		) (*backend.ExecutionResults, error) {
			require.Equal(t, []*protos.HistoryEvent{pastEvent}, pastEvents)
			require.Equal(t, []*protos.HistoryEvent{newEvent}, newEvents)
			close(executed)
			return &backend.ExecutionResults{Response: &protos.OrchestratorResponse{}}, nil
		},
	}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{OrchestratorRequest: &protos.OrchestratorRequest{
			InstanceId:               "instance",
			NewEvents:                []*protos.HistoryEvent{newEvent},
			RequiresHistoryStreaming: true,
		}},
		CompletionToken: "orchestration-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		select {
		case <-executed:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.orchestrationCompletions) == 1
	}, time.Second, time.Millisecond)

	client.mu.Lock()
	require.Equal(t, "orchestration-token", client.orchestrationCompletions[0].CompletionToken)
	client.mu.Unlock()
	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerAbandonsSilentHistoryStream(t *testing.T) {
	stream := newFakeWorkItemStream(1)
	client := &fakeSchedulerClient{
		stream:        stream,
		historyBlocks: true,
	}
	worker := newFakeWorker(t, client, WithWorkerSilentDisconnectTimeout(20*time.Millisecond))

	var executions atomic.Int32
	worker.executor = &recordingExecutor{
		executeOrchestrator: func(
			context.Context,
			api.InstanceID,
			[]*protos.HistoryEvent,
			[]*protos.HistoryEvent,
		) (*backend.ExecutionResults, error) {
			executions.Add(1)
			return &backend.ExecutionResults{Response: &protos.OrchestratorResponse{}}, nil
		},
	}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{OrchestratorRequest: &protos.OrchestratorRequest{
			InstanceId:               "instance",
			RequiresHistoryStreaming: true,
		}},
		CompletionToken: "orchestration-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.orchestrationAbandons == 1
	}, time.Second, time.Millisecond)
	require.Zero(t, executions.Load())

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerAppliesActivityBackpressure(t *testing.T) {
	stream := newFakeWorkItemStream(2)
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(t, client, WithMaxConcurrentActivityWorkItems(1))

	started := make(chan int32, 2)
	release := make(chan struct{})
	worker.executor = &recordingExecutor{
		executeActivity: func(_ context.Context, _ api.InstanceID, event *protos.HistoryEvent) (*protos.HistoryEvent, error) {
			started <- event.EventId
			<-release
			return &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{}},
			}, nil
		},
	}
	for taskID := int32(1); taskID <= 2; taskID++ {
		stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
			Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
				Name:                  "activity",
				TaskId:                taskID,
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
			}},
			CompletionToken: "token",
		}}
	}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.EqualValues(t, 1, <-started)
	select {
	case taskID := <-started:
		t.Fatalf("second activity %d started before the first completed", taskID)
	case <-time.After(25 * time.Millisecond):
	}
	release <- struct{}{}
	require.EqualValues(t, 2, <-started)
	release <- struct{}{}
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.activityCompletions) == 2
	}, time.Second, time.Millisecond)

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerCompletesLegacyAndV2EntityBatches(t *testing.T) {
	stream := newFakeWorkItemStream(2)
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(t, client, WithMaxConcurrentEntityWorkItems(1))
	worker.executor = &recordingExecutor{
		executeEntity: func(_ context.Context, request *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
			require.Equal(t, "@counter@key", request.InstanceId)
			require.Len(t, request.Operations, 1)
			return &protos.EntityBatchResult{
				Results: []*protos.OperationResult{{
					ResultType: &protos.OperationResult_Success{
						Success: &protos.OperationResultSuccess{Result: wrapperspb.String("1")},
					},
				}},
				EntityState: wrapperspb.String("1"),
			}, nil
		},
	}
	legacyRequestID := uuid.NewString()
	v2RequestID := uuid.NewString()
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_EntityRequest{EntityRequest: &protos.EntityBatchRequest{
			InstanceId: "@counter@key",
			Operations: []*protos.OperationRequest{{Operation: "add", RequestId: legacyRequestID}},
		}},
		CompletionToken: "legacy-token",
	}}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_EntityRequestV2{EntityRequestV2: &protos.EntityRequest{
			InstanceId: "@counter@key",
			OperationRequests: []*protos.HistoryEvent{{
				EventType: &protos.HistoryEvent_EntityOperationCalled{
					EntityOperationCalled: &protos.EntityOperationCalledEvent{
						RequestId:        v2RequestID,
						Operation:        "add",
						ParentInstanceId: wrapperspb.String("caller"),
					},
				},
			}},
		}},
		CompletionToken: "v2-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.entityCompletions) == 2
	}, time.Second, time.Millisecond)
	client.mu.Lock()
	require.Equal(t, "legacy-token", client.entityCompletions[0].CompletionToken)
	require.Equal(t, "v2-token", client.entityCompletions[1].CompletionToken)
	require.Len(t, client.entityCompletions[1].OperationInfos, 1)
	require.Equal(t, v2RequestID, client.entityCompletions[1].OperationInfos[0].RequestId)
	require.Equal(t, "caller", client.entityCompletions[1].OperationInfos[0].ResponseDestination.InstanceId)
	client.mu.Unlock()
	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerAbandonsInvalidV2EntityWithBoundedRetry(t *testing.T) {
	stream := newFakeWorkItemStream(1)
	client := &fakeSchedulerClient{stream: stream, entityAbandonFailures: 2}
	worker := newFakeWorker(t, client)
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_EntityRequestV2{EntityRequestV2: &protos.EntityRequest{
			InstanceId: "@counter@key",
			OperationRequests: []*protos.HistoryEvent{{
				EventType: &protos.HistoryEvent_EntityOperationSignaled{
					EntityOperationSignaled: &protos.EntityOperationSignaledEvent{
						RequestId: "not-a-guid",
						Operation: "add",
					},
				},
			}},
		}},
		CompletionToken: "entity-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.entityAbandonAttempts == 3
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerRejectsMismatchedTaskVersion(t *testing.T) {
	stream := newFakeWorkItemStream(1)
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(t, client, WithTaskExecutorOptions(task.WithVersioning(task.VersioningOptions{
		Version:         "1.0",
		MatchStrategy:   task.VersionMatchStrict,
		FailureStrategy: task.VersionFailureReject,
	})))
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
			Name:                  "activity",
			Version:               wrapperspb.String("2.0"),
			TaskId:                1,
			OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
		}},
		CompletionToken: "version-token",
	}}

	ctx, cancel := context.WithCancel(context.Background())
	startedAt := time.Now()
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.activityAbandons == 1
	}, 2*time.Second, time.Millisecond)
	require.GreaterOrEqual(t, time.Since(startedAt), 900*time.Millisecond)
	client.mu.Lock()
	require.Empty(t, client.activityCompletions)
	client.mu.Unlock()

	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerRecreatesConnectionAfterTransientDisconnect(t *testing.T) {
	firstStream := newFakeWorkItemStream(1)
	firstStream.results <- fakeWorkItemResult{err: status.Error(codes.Unavailable, "disconnect")}
	secondStream := newFakeWorkItemStream(1)
	secondStream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_HealthPing{HealthPing: &protos.HealthPing{}},
	}}
	clients := []*fakeSchedulerClient{
		{stream: firstStream},
		{stream: secondStream},
	}
	var factoryCalls atomic.Int32

	registry := task.NewTaskRegistry()
	worker, err := newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			index := int(factoryCalls.Add(1)) - 1
			if index >= len(clients) {
				return clients[len(clients)-1], nil, nil
			}
			return clients[index], nil, nil
		},
		registry,
		backend.DefaultLogger(),
		WithWorkerHelloTimeout(time.Second),
		WithWorkerSilentDisconnectTimeout(time.Second),
		WithWorkerReconnectBackoff(time.Millisecond, 5*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		return factoryCalls.Load() >= 2
	}, time.Second, time.Millisecond)
	cancel()
	require.NoError(t, worker.Shutdown(context.Background()))
}

func TestTaskHubGrpcWorkerStopsOnAuthenticationError(t *testing.T) {
	stream := newFakeWorkItemStream(1)
	stream.results <- fakeWorkItemResult{err: status.Error(codes.Unauthenticated, "bad token")}
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(t, client)

	require.NoError(t, worker.Start(context.Background()))
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := worker.Wait(waitCtx)
	require.ErrorContains(t, err, "non-retryable")
	require.ErrorContains(t, err, "Unauthenticated")
}

func TestTaskHubGrpcWorkerGracefulDrainKeepsCompletionContextAlive(t *testing.T) {
	stream := newFakeWorkItemStream(1)
	client := &fakeSchedulerClient{stream: stream}
	worker := newFakeWorker(t, client, WithMaxConcurrentActivityWorkItems(1))

	started := make(chan struct{})
	release := make(chan struct{})
	worker.executor = &recordingExecutor{
		executeActivity: func(ctx context.Context, _ api.InstanceID, _ *protos.HistoryEvent) (*protos.HistoryEvent, error) {
			close(started)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return &protos.HistoryEvent{
					EventType: &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{}},
				}, nil
			}
		},
	}
	stream.results <- fakeWorkItemResult{item: &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
			Name:                  "activity",
			TaskId:                1,
			OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
		}},
		CompletionToken: "token",
	}}

	runCtx, cancelRun := context.WithCancel(context.Background())
	require.NoError(t, worker.Start(runCtx))
	<-started
	cancelRun()
	close(release)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	require.NoError(t, worker.Shutdown(shutdownCtx))
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.activityCompletions, 1)
}

func TestTaskHubGrpcWorkerHelloFailsFast(t *testing.T) {
	client := &fakeSchedulerClient{
		stream:   newFakeWorkItemStream(0),
		helloErr: status.Error(codes.PermissionDenied, "forbidden"),
	}
	worker := newFakeWorker(t, client)
	err := worker.Start(context.Background())
	require.ErrorContains(t, err, "Hello")
	require.False(t, worker.Running())
}

func TestTaskHubGrpcWorkerOptionValidation(t *testing.T) {
	registry := task.NewTaskRegistry()
	_, err := newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			return nil, nil, errors.New("unused")
		},
		registry,
		backend.DefaultLogger(),
		WithMaxConcurrentActivityWorkItems(0),
	)
	require.Error(t, err)
}

func TestTransientWorkerGRPCCodes(t *testing.T) {
	for _, code := range []codes.Code{
		codes.Canceled,
		codes.DeadlineExceeded,
		codes.NotFound,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal,
		codes.Unavailable,
		codes.Unknown,
	} {
		require.True(t, isTransientWorkerGRPCCode(code), code.String())
	}
	for _, code := range []codes.Code{
		codes.Unauthenticated,
		codes.PermissionDenied,
		codes.InvalidArgument,
	} {
		require.False(t, isTransientWorkerGRPCCode(code), code.String())
	}
	require.False(t, isTransientWorkerError(
		status.Error(codes.Canceled, "grpc: the client connection is closing"),
	))
}
