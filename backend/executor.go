package backend

import (
	context "context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

var emptyCompleteTaskResponse = &protos.CompleteTaskResponse{}

var errShuttingDown error = status.Error(codes.Canceled, "shutting down")

type ExecutionResults struct {
	Response *protos.OrchestratorResponse
	complete chan struct{}
	pending  chan string
}

type activityExecutionResult struct {
	response *protos.ActivityResponse
	complete chan struct{}
	pending  chan string
}

type entityExecutionResult struct {
	instanceID string
	response   *protos.EntityBatchResult
	complete   chan struct{}
	pending    chan string
}

type Executor interface {
	ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*ExecutionResults, error)
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
	Shutdown(ctx context.Context) error
}

type grpcExecutor struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	workItemQueue              chan *protos.WorkItem
	pendingOrchestrators       *sync.Map // map[api.InstanceID]*ExecutionResults
	pendingActivities          *sync.Map // map[string]*activityExecutionResult
	pendingEntities            *sync.Map // map[completionToken]*entityExecutionResult
	pendingEntityInstances     *sync.Map // map[instanceID]completionToken
	backend                    Backend
	logger                     Logger
	onWorkItemConnection       func(context.Context) error
	streamShutdownChan         <-chan any
	allowReplaceableStatusWire bool
}

type grpcExecutorOptions func(g *grpcExecutor)

// IsDurableTaskGrpcRequest returns true if the specified gRPC method name represents an operation
// that is compatible with the gRPC executor.
func IsDurableTaskGrpcRequest(fullMethodName string) bool {
	return strings.HasPrefix(fullMethodName, "/TaskHubSidecarService/")
}

// WithOnGetWorkItemsConnectionCallback allows the caller to get a notification when an external process
// connects over gRPC and invokes the GetWorkItems operation. This can be useful for doing things like
// lazily auto-starting the task hub worker only when necessary.
func WithOnGetWorkItemsConnectionCallback(callback func(context.Context) error) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.onWorkItemConnection = callback
	}
}

func WithStreamShutdownChannel(c <-chan any) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.streamShutdownChan = c
	}
}

// WithCurrentOrchestrationIDReusePolicyWire enables current-proto
// replaceableStatus semantics for external gRPC clients. Without this option,
// ambiguous field-1-only policies fail closed to protect legacy ERROR callers.
func WithCurrentOrchestrationIDReusePolicyWire() grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.allowReplaceableStatusWire = true
	}
}

// NewGrpcExecutor returns the Executor object and a method to invoke to register the gRPC server in the executor.
func NewGrpcExecutor(be Backend, logger Logger, opts ...grpcExecutorOptions) (executor Executor, registerServerFn func(grpcServer grpc.ServiceRegistrar)) {
	grpcExecutor := &grpcExecutor{
		workItemQueue:          make(chan *protos.WorkItem),
		backend:                be,
		logger:                 logger,
		pendingOrchestrators:   &sync.Map{},
		pendingActivities:      &sync.Map{},
		pendingEntities:        &sync.Map{},
		pendingEntityInstances: &sync.Map{},
	}

	for _, opt := range opts {
		opt(grpcExecutor)
	}

	return grpcExecutor, func(grpcServer grpc.ServiceRegistrar) {
		protos.RegisterTaskHubSidecarServiceServer(grpcServer, grpcExecutor)
	}
}

// ExecuteOrchestrator implements Executor
func (executor *grpcExecutor) ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*ExecutionResults, error) {
	result := &ExecutionResults{complete: make(chan struct{})}
	executor.pendingOrchestrators.Store(iid, result)

	workItem := &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{
			OrchestratorRequest: &protos.OrchestratorRequest{
				InstanceId:  string(iid),
				ExecutionId: nil,
				PastEvents:  oldEvents,
				NewEvents:   newEvents,
				EntityParameters: &protos.OrchestratorEntityParameters{
					EntityMessageReorderWindow: durationpb.New(0),
				},
			},
		},
	}

	// Send the orchestration execution work-item to the connected worker.
	// This will block if the worker isn't listening for work items.
	select {
	case <-ctx.Done():
		executor.logger.Warnf("%s: context canceled before dispatching orchestrator work item", iid)
		return nil, ctx.Err()
	case executor.workItemQueue <- workItem:
	}

	// Wait for the connected worker to signal that it's done executing the work-item
	select {
	case <-ctx.Done():
		executor.logger.Warnf("%s: context canceled before receiving orchestrator result", iid)
		return nil, ctx.Err()
	case <-result.complete:
		executor.logger.Debugf("%s: orchestrator got result", iid)
		if result.Response == nil {
			return nil, ErrOperationAborted
		}
	}

	return result, nil
}

// ExecuteActivity implements Executor
func (executor *grpcExecutor) ExecuteActivity(ctx context.Context, iid api.InstanceID, e *protos.HistoryEvent) (*protos.HistoryEvent, error) {
	key := getActivityExecutionKey(string(iid), e.EventId)
	result := &activityExecutionResult{complete: make(chan struct{})}
	executor.pendingActivities.Store(key, result)

	task := e.GetTaskScheduled()
	workItem := &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{
			ActivityRequest: &protos.ActivityRequest{
				Name:                  task.Name,
				Version:               task.Version,
				Input:                 task.Input,
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: string(iid)},
				TaskId:                e.EventId,
				Tags:                  contextprop.Clone(task.Tags),
			},
		},
	}

	// Send the activity execution work-item to the connected worker.
	// This will block if the worker isn't listening for work items.
	select {
	case <-ctx.Done():
		executor.logger.Warnf("%s/%s#%d: context canceled before dispatching activity work item", iid, task.Name, e.EventId)
		return nil, ctx.Err()
	case executor.workItemQueue <- workItem:
	}

	// Wait for the connected worker to signal that it's done executing the work-item
	select {
	case <-ctx.Done():
		executor.logger.Warnf("%s/%s#%d: context canceled before receiving activity result", iid, task.Name, e.EventId)
		return nil, ctx.Err()
	case <-result.complete:
		executor.logger.Debugf("%s: activity got result", key)
		if result.response == nil {
			return nil, ErrOperationAborted
		}
	}

	var responseEvent *protos.HistoryEvent
	if failureDetails := result.response.GetFailureDetails(); failureDetails != nil {
		responseEvent = helpers.NewTaskFailedEvent(result.response.TaskId, result.response.FailureDetails)
	} else {
		responseEvent = helpers.NewTaskCompletedEvent(result.response.TaskId, result.response.Result)
	}

	return responseEvent, nil
}

// ExecuteEntity dispatches a legacy entity batch to a connected gRPC worker.
func (executor *grpcExecutor) ExecuteEntity(ctx context.Context, req *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("entity batch request must not be nil")
	}
	completionToken := uuid.NewString()
	if _, loaded := executor.pendingEntityInstances.LoadOrStore(req.InstanceId, completionToken); loaded {
		return nil, fmt.Errorf("entity batch for instance %q is already pending", req.InstanceId)
	}
	result := &entityExecutionResult{
		instanceID: req.InstanceId,
		complete:   make(chan struct{}),
	}
	executor.pendingEntities.Store(completionToken, result)
	cleanup := func() {
		executor.pendingEntities.Delete(completionToken)
		executor.pendingEntityInstances.Delete(req.InstanceId)
	}

	workItem := &protos.WorkItem{
		Request:         &protos.WorkItem_EntityRequest{EntityRequest: req},
		CompletionToken: completionToken,
	}
	select {
	case <-ctx.Done():
		cleanup()
		return nil, ctx.Err()
	case executor.workItemQueue <- workItem:
	}

	select {
	case <-ctx.Done():
		cleanup()
		return nil, ctx.Err()
	case <-result.complete:
		if result.response == nil {
			return nil, ErrOperationAborted
		}
		return result.response, nil
	}
}

// Shutdown implements Executor
func (g *grpcExecutor) Shutdown(ctx context.Context) error {
	// closing the work item queue is a signal for shutdown
	close(g.workItemQueue)

	// Iterate through all pending items and close them to unblock the goroutines waiting on this
	g.pendingActivities.Range(func(_, value any) bool {
		p, ok := value.(*activityExecutionResult)
		if ok {
			close(p.complete)
		}
		return true
	})
	g.pendingOrchestrators.Range(func(_, value any) bool {
		p, ok := value.(*ExecutionResults)
		if ok {
			close(p.complete)
		}
		return true
	})
	g.pendingEntities.Range(func(_, value any) bool {
		if pending, ok := value.(*entityExecutionResult); ok {
			close(pending.complete)
		}
		return true
	})

	return nil
}

// Hello implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) Hello(ctx context.Context, empty *emptypb.Empty) (*emptypb.Empty, error) {
	return empty, nil
}

// GetWorkItems implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) GetWorkItems(req *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		g.logger.Infof("work item stream established by user-agent: %v", md.Get("user-agent"))
	}

	// There are some cases where the app may need to be notified when a client connects to fetch work items, like
	// for auto-starting the worker. The app also has an opportunity to set itself as unavailable by returning an error.
	callback := g.onWorkItemConnection
	if callback != nil {
		if err := callback(stream.Context()); err != nil {
			message := "unable to establish work item stream at this time: " + err.Error()
			g.logger.Warn(message)
			return status.Errorf(codes.Unavailable, message)
		}
	}

	// Collect all pending activities on this stream
	// Note: we don't need sync.Map's here because access is only on this thread
	pendingActivities := make(map[string]struct{})
	pendingActivityCh := make(chan string, 1)
	pendingOrchestrators := make(map[string]struct{})
	pendingOrchestratorCh := make(chan string, 1)
	pendingEntities := make(map[string]struct{})
	pendingEntityCh := make(chan string, 1)
	defer func() {
		// If there's any pending activity left, remove them
		for key := range pendingActivities {
			g.logger.Debugf("cleaning up pending activity: %s", key)
			p, ok := g.pendingActivities.LoadAndDelete(key)
			if ok {
				pending := p.(*activityExecutionResult)
				close(pending.complete)
			}
		}
		for key := range pendingOrchestrators {
			g.logger.Debugf("cleaning up pending orchestrator: %s", key)
			p, ok := g.pendingOrchestrators.LoadAndDelete(api.InstanceID(key))
			if ok {
				pending := p.(*ExecutionResults)
				close(pending.complete)
			}
		}
		for token := range pendingEntities {
			if value, ok := g.pendingEntities.LoadAndDelete(token); ok {
				pending := value.(*entityExecutionResult)
				g.pendingEntityInstances.Delete(pending.instanceID)
				close(pending.complete)
			}
		}
	}()

	// The worker client invokes this method, which streams back work-items as they arrive.
	for {
		select {
		case <-stream.Context().Done():
			g.logger.Info("work item stream closed")
			return nil
		case wi, ok := <-g.workItemQueue:
			if !ok {
				continue
			}
			switch x := wi.Request.(type) {
			case *protos.WorkItem_OrchestratorRequest:
				key := x.OrchestratorRequest.GetInstanceId()
				pendingOrchestrators[key] = struct{}{}
				p, ok := g.pendingOrchestrators.Load(api.InstanceID(key))
				if ok {
					p.(*ExecutionResults).pending = pendingOrchestratorCh
				}
			case *protos.WorkItem_ActivityRequest:
				key := getActivityExecutionKey(x.ActivityRequest.GetOrchestrationInstance().GetInstanceId(), x.ActivityRequest.GetTaskId())
				pendingActivities[key] = struct{}{}
				p, ok := g.pendingActivities.Load(key)
				if ok {
					p.(*activityExecutionResult).pending = pendingActivityCh
				}
			case *protos.WorkItem_EntityRequest, *protos.WorkItem_EntityRequestV2:
				token := wi.GetCompletionToken()
				pendingEntities[token] = struct{}{}
				if pending, ok := g.pendingEntities.Load(token); ok {
					pending.(*entityExecutionResult).pending = pendingEntityCh
				}
			}

			if err := stream.Send(wi); err != nil {
				g.logger.Errorf("encountered an error while sending work item: %v", err)
				return err
			}
		case key := <-pendingActivityCh:
			delete(pendingActivities, key)
		case key := <-pendingOrchestratorCh:
			delete(pendingOrchestrators, key)
		case token := <-pendingEntityCh:
			delete(pendingEntities, token)
		case <-g.streamShutdownChan:
			return errShuttingDown
		}
	}
}

// CompleteOrchestratorTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteOrchestratorTask(ctx context.Context, res *protos.OrchestratorResponse) (*protos.CompleteTaskResponse, error) {
	iid := api.InstanceID(res.InstanceId)
	if g.deletePendingOrchestrator(iid, res) {
		return emptyCompleteTaskResponse, nil
	}

	return emptyCompleteTaskResponse, fmt.Errorf("unknown instance ID: %s", res.InstanceId)
}

func (g *grpcExecutor) deletePendingOrchestrator(iid api.InstanceID, res *protos.OrchestratorResponse) bool {
	p, ok := g.pendingOrchestrators.LoadAndDelete(iid)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*ExecutionResults)
	pending.Response = res
	if pending.pending != nil {
		pending.pending <- string(iid)
	}
	close(pending.complete)
	return true
}

// CompleteActivityTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteActivityTask(ctx context.Context, res *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	key := getActivityExecutionKey(res.InstanceId, res.TaskId)
	if g.deletePendingActivityTask(key, res) {
		return emptyCompleteTaskResponse, nil
	}

	return emptyCompleteTaskResponse, fmt.Errorf("unknown instance ID/task ID combo: %s", key)
}

func (g *grpcExecutor) deletePendingActivityTask(key string, res *protos.ActivityResponse) bool {
	p, ok := g.pendingActivities.LoadAndDelete(key)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*activityExecutionResult)
	pending.response = res
	if pending.pending != nil {
		pending.pending <- key
	}
	close(pending.complete)
	return true
}

func getActivityExecutionKey(iid string, taskID int32) string {
	return iid + "/" + strconv.FormatInt(int64(taskID), 10)
}

// CompleteEntityTask implements protos.TaskHubSidecarServiceServer.
func (g *grpcExecutor) CompleteEntityTask(ctx context.Context, response *protos.EntityBatchResult) (*protos.CompleteTaskResponse, error) {
	if response == nil {
		return nil, status.Error(codes.InvalidArgument, "entity response must not be nil")
	}
	token := response.CompletionToken
	if token == "" {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			values := md.Get("entity-completion-token")
			if len(values) > 0 {
				token = values[0]
			}
		}
	}
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "entity completion token is required")
	}
	value, ok := g.pendingEntities.LoadAndDelete(token)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown entity completion token %q", token)
	}
	pending := value.(*entityExecutionResult)
	g.pendingEntityInstances.Delete(pending.instanceID)
	pending.response = response
	if pending.pending != nil {
		pending.pending <- token
	}
	close(pending.complete)
	return emptyCompleteTaskResponse, nil
}

// AbandonTaskEntityWorkItem implements protos.TaskHubSidecarServiceServer.
func (g *grpcExecutor) AbandonTaskEntityWorkItem(_ context.Context, request *protos.AbandonEntityTaskRequest) (*protos.AbandonEntityTaskResponse, error) {
	if request.GetCompletionToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "entity completion token is required")
	}
	value, ok := g.pendingEntities.LoadAndDelete(request.CompletionToken)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown entity completion token %q", request.CompletionToken)
	}
	pending := value.(*entityExecutionResult)
	g.pendingEntityInstances.Delete(pending.instanceID)
	if pending.pending != nil {
		pending.pending <- request.CompletionToken
	}
	close(pending.complete)
	return &protos.AbandonEntityTaskResponse{}, nil
}

// CreateTaskHub implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) CreateTaskHub(context.Context, *protos.CreateTaskHubRequest) (*protos.CreateTaskHubResponse, error) {
	return nil, status.Error(codes.Unimplemented, "CreateTaskHub is not implemented")
}

// DeleteTaskHub implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) DeleteTaskHub(context.Context, *protos.DeleteTaskHubRequest) (*protos.DeleteTaskHubResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DeleteTaskHub is not implemented")
}

// GetInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) GetInstance(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	metadata, err := g.backend.GetOrchestrationMetadata(ctx, api.InstanceID(req.InstanceId))
	if err != nil {
		if errors.Is(err, api.ErrInstanceNotFound) {
			return &protos.GetInstanceResponse{Exists: false}, nil
		}
		return nil, err
	}

	if metadata == nil {
		return &protos.GetInstanceResponse{Exists: false}, nil
	}

	return createGetInstanceResponse(req, metadata), nil
}

// PurgeInstances implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) PurgeInstances(ctx context.Context, req *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
	if req.GetPurgeInstanceFilter() != nil {
		return nil, status.Error(codes.Unimplemented, "multi-instance purge is not yet implemented")
	}
	count, err := purgeOrchestrationState(ctx, g.backend, api.InstanceID(req.GetInstanceId()), req.Recursive)
	resp := &protos.PurgeInstancesResponse{DeletedInstanceCount: int32(count)}
	if err != nil {
		return resp, fmt.Errorf("failed to purge orchestration state: %w", err)
	}
	return resp, nil
}

// QueryInstances implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) QueryInstances(context.Context, *protos.QueryInstancesRequest) (*protos.QueryInstancesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "QueryInstances is not implemented")
}

// RaiseEvent implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) RaiseEvent(ctx context.Context, req *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	if entityBackend, ok := g.backend.(EntitySignalBackend); ok && helpers.IsEntityInstanceID(req.InstanceId) &&
		strings.EqualFold(req.Name, helpers.EntityRequestEventName) {
		var message helpers.EntityRequestMessage
		if err := json.Unmarshal([]byte(req.Input.GetValue()), &message); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid entity request payload: %v", err)
		}
		if !message.IsSignal {
			return nil, status.Error(codes.InvalidArgument, "RaiseEvent supports entity signals only")
		}
		signal := &protos.SignalEntityRequest{
			InstanceId:  req.InstanceId,
			Name:        message.Operation,
			RequestId:   message.ID,
			RequestTime: timestamppb.Now(),
		}
		if message.Input != "" {
			signal.Input = wrapperspb.String(message.Input)
		}
		if err := entityBackend.SignalEntity(ctx, signal); err != nil {
			return nil, err
		}
		return &protos.RaiseEventResponse{}, nil
	}
	e := helpers.NewEventRaisedEvent(req.Name, req.Input)
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	return &protos.RaiseEventResponse{}, nil
}

// StartInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) StartInstance(ctx context.Context, req *protos.CreateInstanceRequest) (*protos.CreateInstanceResponse, error) {
	instanceID := req.InstanceId
	if err := helpers.ValidateOrchestrationInstanceID(instanceID); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, span := helpers.StartNewCreateOrchestrationSpan(ctx, req.Name, req.Version.GetValue(), instanceID)
	defer span.End()

	e := helpers.NewExecutionStartedEvent(req.Name, instanceID, req.Input, nil, helpers.TraceContextFromSpan(span), req.ScheduledStartTimestamp, req.Version)
	e.GetExecutionStarted().Tags = contextprop.Clone(req.Tags)
	policy, err := orchestrationIDReusePolicyFromProto(
		req.OrchestrationIdReusePolicy,
		g.allowReplaceableStatusWire,
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid orchestration ID reuse policy: %v", err)
	}
	if err := g.backend.CreateOrchestrationInstance(ctx, e, WithOrchestrationIdReusePolicy(policy)); err != nil {
		return nil, err
	}

	return &protos.CreateInstanceResponse{InstanceId: instanceID}, nil
}

// SignalEntity implements protos.TaskHubSidecarServiceServer.
func (g *grpcExecutor) SignalEntity(ctx context.Context, req *protos.SignalEntityRequest) (*protos.SignalEntityResponse, error) {
	entityBackend, ok := g.backend.(EntitySignalBackend)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "backend does not support durable entities")
	}
	if _, err := api.EntityIDFromString(req.InstanceId); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid entity instance ID: %v", err)
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "entity operation name must not be empty")
	}
	if req.RequestId == "" {
		req.RequestId = uuid.NewString()
	}
	if req.RequestTime == nil {
		req.RequestTime = timestamppb.Now()
	}
	if err := entityBackend.SignalEntity(ctx, req); err != nil {
		return nil, err
	}
	return &protos.SignalEntityResponse{}, nil
}

// GetEntity implements protos.TaskHubSidecarServiceServer.
func (g *grpcExecutor) GetEntity(ctx context.Context, req *protos.GetEntityRequest) (*protos.GetEntityResponse, error) {
	entityBackend, ok := g.backend.(EntityQueryBackend)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "backend does not support durable entities")
	}
	entityID, err := api.EntityIDFromString(req.InstanceId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid entity instance ID: %v", err)
	}
	entity, err := entityBackend.GetEntityMetadata(ctx, entityID, req.IncludeState)
	if err != nil {
		return nil, err
	}
	if entity == nil {
		return &protos.GetEntityResponse{Exists: false}, nil
	}
	return &protos.GetEntityResponse{Exists: true, Entity: entityMetadataToProto(entity)}, nil
}

// QueryEntities implements protos.TaskHubSidecarServiceServer.
func (g *grpcExecutor) QueryEntities(ctx context.Context, req *protos.QueryEntitiesRequest) (*protos.QueryEntitiesResponse, error) {
	entityBackend, ok := g.backend.(EntityQueryBackend)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "backend does not support durable entities")
	}
	query := api.EntityQuery{}
	if req.Query != nil {
		query.InstanceIDStartsWith = req.Query.InstanceIdStartsWith.GetValue()
		query.IncludeState = req.Query.IncludeState
		query.IncludeTransient = req.Query.IncludeTransient
		query.PageSize = req.Query.PageSize.GetValue()
		query.ContinuationToken = req.Query.ContinuationToken.GetValue()
		if req.Query.LastModifiedFrom != nil {
			query.LastModifiedFrom = req.Query.LastModifiedFrom.AsTime()
		}
		if req.Query.LastModifiedTo != nil {
			query.LastModifiedTo = req.Query.LastModifiedTo.AsTime()
		}
	}
	result, err := entityBackend.QueryEntities(ctx, query)
	if err != nil {
		return nil, err
	}
	response := &protos.QueryEntitiesResponse{}
	if result != nil {
		response.ContinuationToken = wrapperspb.String(result.ContinuationToken)
		response.Entities = make([]*protos.EntityMetadata, 0, len(result.Entities))
		for _, entity := range result.Entities {
			if entity != nil {
				response.Entities = append(response.Entities, entityMetadataToProto(entity))
			}
		}
	}
	return response, nil
}

// CleanEntityStorage implements protos.TaskHubSidecarServiceServer.
func (g *grpcExecutor) CleanEntityStorage(ctx context.Context, req *protos.CleanEntityStorageRequest) (*protos.CleanEntityStorageResponse, error) {
	entityBackend, ok := g.backend.(EntityQueryBackend)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "backend does not support durable entities")
	}
	result, err := entityBackend.CleanEntityStorage(ctx, api.CleanEntityStorageRequest{
		ContinuationToken:    req.ContinuationToken.GetValue(),
		RemoveEmptyEntities:  req.RemoveEmptyEntities,
		ReleaseOrphanedLocks: req.ReleaseOrphanedLocks,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return &protos.CleanEntityStorageResponse{}, nil
	}
	return &protos.CleanEntityStorageResponse{
		ContinuationToken:     wrapperspb.String(result.ContinuationToken),
		EmptyEntitiesRemoved:  result.EmptyEntitiesRemoved,
		OrphanedLocksReleased: result.OrphanedLocksReleased,
	}, nil
}

func entityMetadataToProto(entity *api.EntityMetadata) *protos.EntityMetadata {
	metadata := &protos.EntityMetadata{
		InstanceId:       entity.InstanceID.String(),
		LastModifiedTime: timestamppb.New(entity.LastModifiedTime),
		BacklogQueueSize: entity.BacklogQueueSize,
	}
	if entity.LockedBy != "" {
		metadata.LockedBy = wrapperspb.String(entity.LockedBy)
	}
	if entity.SerializedState != "" {
		metadata.SerializedState = wrapperspb.String(entity.SerializedState)
	}
	return metadata
}

// TerminateInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) TerminateInstance(ctx context.Context, req *protos.TerminateRequest) (*protos.TerminateResponse, error) {
	e := helpers.NewExecutionTerminatedEvent(req.Output, req.Recursive)
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, fmt.Errorf("failed to submit termination request: %w", err)
	}
	return &protos.TerminateResponse{}, nil
}

// SuspendInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) SuspendInstance(ctx context.Context, req *protos.SuspendRequest) (*protos.SuspendResponse, error) {
	e := helpers.NewSuspendOrchestrationEvent(req.Reason.GetValue())
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	return &protos.SuspendResponse{}, nil
}

// ResumeInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) ResumeInstance(ctx context.Context, req *protos.ResumeRequest) (*protos.ResumeResponse, error) {
	e := helpers.NewResumeOrchestrationEvent(req.Reason.GetValue())
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	return &protos.ResumeResponse{}, nil
}

// WaitForInstanceCompletion implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) WaitForInstanceCompletion(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return g.waitForInstance(ctx, req, func(m *api.OrchestrationMetadata) bool {
		return m.IsComplete()
	})
}

// WaitForInstanceStart implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) WaitForInstanceStart(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return g.waitForInstance(ctx, req, func(m *api.OrchestrationMetadata) bool {
		return m.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

func (g *grpcExecutor) waitForInstance(ctx context.Context, req *protos.GetInstanceRequest, condition func(*api.OrchestrationMetadata) bool) (*protos.GetInstanceResponse, error) {
	iid := api.InstanceID(req.InstanceId)

	var b backoff.BackOff = &backoff.ExponentialBackOff{
		InitialInterval:     1 * time.Millisecond,
		MaxInterval:         3 * time.Second,
		Multiplier:          1.5,
		RandomizationFactor: 0.5,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b = backoff.WithContext(b, ctx)
	b.Reset()

loop:
	for {
		t := time.NewTimer(b.NextBackOff())
		select {
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			break loop

		case <-t.C:
			metadata, err := g.backend.GetOrchestrationMetadata(ctx, iid)
			if err != nil {
				return nil, err
			}
			if metadata == nil {
				return &protos.GetInstanceResponse{Exists: false}, nil
			}
			if condition(metadata) {
				return createGetInstanceResponse(req, metadata), nil
			}
		}
	}

	return nil, status.Errorf(codes.Canceled, "instance hasn't completed")
}

// mustEmbedUnimplementedTaskHubSidecarServiceServer implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) mustEmbedUnimplementedTaskHubSidecarServiceServer() { //nolint:unused
}

func createGetInstanceResponse(req *protos.GetInstanceRequest, metadata *api.OrchestrationMetadata) *protos.GetInstanceResponse {
	state := &protos.OrchestrationState{
		InstanceId:           req.InstanceId,
		Name:                 metadata.Name,
		Version:              wrapperspb.String(metadata.Version),
		OrchestrationStatus:  metadata.RuntimeStatus,
		CreatedTimestamp:     timestamppb.New(metadata.CreatedAt),
		LastUpdatedTimestamp: timestamppb.New(metadata.LastUpdatedAt),
	}

	if req.GetInputsAndOutputs {
		state.Input = wrapperspb.String(metadata.SerializedInput)
		state.CustomStatus = wrapperspb.String(metadata.SerializedCustomStatus)
		state.Output = wrapperspb.String(metadata.SerializedOutput)
		state.FailureDetails = metadata.FailureDetails
	}

	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: state}
}
