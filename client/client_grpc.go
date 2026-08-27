package client

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// REVIEW: Can this be merged with backend/client.go somehow?

type TaskHubGrpcClient struct {
	client                       protos.TaskHubSidecarServiceClient
	connection                   grpc.ClientConnInterface
	logger                       backend.Logger
	allowLegacyIDReusePolicyWire bool
	largePayloads                *api.LargePayloadOptions

	listenerMu sync.Mutex
	listener   *TaskHubGrpcWorker
}

type TaskHubGrpcClientOption func(*TaskHubGrpcClient)

var ErrUnsupportedOrchestrationIDReusePolicy = errors.New("orchestration ID reuse policy is not supported by the current gRPC wire contract")

// WithLegacyOrchestrationIDReusePolicyWire allows the client to send the legacy
// IGNORE action to a known-compatible sidecar. Do not use this option with DTS:
// current DTS servers interpret the shared status field as a replacement policy.
func WithLegacyOrchestrationIDReusePolicyWire() TaskHubGrpcClientOption {
	return func(c *TaskHubGrpcClient) {
		c.allowLegacyIDReusePolicyWire = true
	}
}

// WithLargePayloads configures externalization and hydration for management payloads.
func WithLargePayloads(options *api.LargePayloadOptions) TaskHubGrpcClientOption {
	return func(c *TaskHubGrpcClient) {
		if options == nil {
			c.largePayloads = nil
			return
		}
		clone := *options
		c.largePayloads = &clone
	}
}

// NewTaskHubGrpcClient creates a client that can be used to manage orchestrations over a gRPC connection.
// The gRPC connection must be to a task hub worker that understands the Durable Task gRPC protocol.
func NewTaskHubGrpcClient(cc grpc.ClientConnInterface, logger backend.Logger, opts ...TaskHubGrpcClientOption) *TaskHubGrpcClient {
	c := &TaskHubGrpcClient{
		client:     protos.NewTaskHubSidecarServiceClient(cc),
		connection: cc,
		logger:     logger,
	}
	for _, configure := range opts {
		configure(c)
	}
	return c
}

// ScheduleNewOrchestration schedules a new orchestration instance with a specified set of options for execution.
func (c *TaskHubGrpcClient) ScheduleNewOrchestration(ctx context.Context, orchestrator string, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	req := &protos.CreateInstanceRequest{Name: orchestrator}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure orchestration request: %w", err)
		}
	}
	if err := c.prepareOrchestrationIDReusePolicy(req); err != nil {
		return api.EmptyInstanceID, err
	}
	if req.InstanceId == "" {
		u, err := uuid.NewV7()
		if err == nil {
			req.InstanceId = u.String()
		} else {
			req.InstanceId = uuid.NewString()
		}
	}
	var err error
	req.Input, err = largepayload.Externalize(ctx, c.largePayloads, req.Input)
	if err != nil {
		return api.EmptyInstanceID, fmt.Errorf("failed to externalize orchestration input: %w", err)
	}

	resp, err := c.client.StartInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return api.EmptyInstanceID, ctx.Err()
		}
		return api.EmptyInstanceID, fmt.Errorf("failed to start orchestrator: %w", err)
	}
	return api.InstanceID(resp.InstanceId), nil
}

func (c *TaskHubGrpcClient) prepareOrchestrationIDReusePolicy(req *protos.CreateInstanceRequest) error {
	policy := req.OrchestrationIdReusePolicy
	action, hasLegacyAction, err := protos.GetLegacyOrchestrationIDReuseAction(policy)
	if err != nil {
		return fmt.Errorf("invalid orchestration ID reuse policy: %w", err)
	}
	if !hasLegacyAction {
		return nil
	}

	switch api.CreateOrchestrationAction(action) {
	case api.REUSE_ID_ACTION_TERMINATE:
		return nil
	case api.REUSE_ID_ACTION_ERROR:
		req.OrchestrationIdReusePolicy = nil
		return nil
	case api.REUSE_ID_ACTION_IGNORE:
		if c.allowLegacyIDReusePolicyWire {
			return nil
		}
		return fmt.Errorf("%w: IGNORE cannot be distinguished from TERMINATE by current DTS servers", ErrUnsupportedOrchestrationIDReusePolicy)
	default:
		return fmt.Errorf("invalid orchestration ID reuse action: %d", action)
	}
}

// FetchOrchestrationMetadata fetches metadata for the specified orchestration from the configured task hub.
//
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	req := makeGetInstanceRequest(id, opts)
	resp, err := c.client.GetInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to fetch orchestration metadata: %w", err)
	}
	return c.makeOrchestrationMetadata(ctx, resp)
}

// WaitForOrchestrationStart waits for an orchestration to start running and returns an [api.OrchestrationMetadata] object that contains
// metadata about the started instance.
//
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	var resp *protos.GetInstanceResponse
	var err error
	err = backoff.Retry(func() error {
		req := makeGetInstanceRequest(id, opts)
		resp, err = c.client.WaitForInstanceStart(ctx, req)
		if err != nil {
			// if its context cancelled stop retrying
			if ctx.Err() != nil {
				return backoff.Permanent(ctx.Err())
			}
			return fmt.Errorf("failed to wait for orchestration start: %w", err)
		}
		return nil
	}, backoff.WithContext(newInfiniteRetries(), ctx))
	if err != nil {
		return nil, err
	}
	return c.makeOrchestrationMetadata(ctx, resp)
}

// WaitForOrchestrationCompletion waits for an orchestration to complete and returns an [api.OrchestrationMetadata] object that contains
// metadata about the completed instance.
//
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID, opts ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	var resp *protos.GetInstanceResponse
	var err error
	err = backoff.Retry(func() error {
		req := makeGetInstanceRequest(id, opts)
		resp, err = c.client.WaitForInstanceCompletion(ctx, req)
		if err != nil {
			// if its context cancelled stop retrying
			if ctx.Err() != nil {
				return backoff.Permanent(ctx.Err())
			}
			return fmt.Errorf("failed to wait for orchestration completion: %w", err)
		}
		return nil
	}, backoff.WithContext(newInfiniteRetries(), ctx))
	if err != nil {
		return nil, err
	}
	return c.makeOrchestrationMetadata(ctx, resp)
}

// TerminateOrchestration terminates a running orchestration by causing it to stop receiving new events and
// putting it directly into the TERMINATED state.
func (c *TaskHubGrpcClient) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	req := &protos.TerminateRequest{InstanceId: string(id), Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure termination request: %w", err)
		}
	}
	var err error
	req.Output, err = largepayload.Externalize(ctx, c.largePayloads, req.Output)
	if err != nil {
		return fmt.Errorf("failed to externalize termination output: %w", err)
	}

	_, err = c.client.TerminateInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to terminate instance: %w", err)
	}
	return nil
}

// RaiseEvent sends an asynchronous event notification to a waiting orchestration.
func (c *TaskHubGrpcClient) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	req := &protos.RaiseEventRequest{InstanceId: string(id), Name: eventName}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure raise event request: %w", err)
		}
	}
	var err error
	req.Input, err = largepayload.Externalize(ctx, c.largePayloads, req.Input)
	if err != nil {
		return fmt.Errorf("failed to externalize event payload: %w", err)
	}

	if _, err := c.client.RaiseEvent(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to raise event: %w", err)
	}
	return nil
}

// SuspendOrchestration suspends an orchestration instance, halting processing of its events until a "resume" operation resumes it.
//
// Note that suspended orchestrations are still considered to be "running" even though they will not process events.
func (c *TaskHubGrpcClient) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	req := &protos.SuspendRequest{
		InstanceId: string(id),
		Reason:     wrapperspb.String(reason),
	}
	if _, err := c.client.SuspendInstance(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to suspend orchestration: %w", err)
	}
	return nil
}

// ResumeOrchestration resumes an orchestration instance that was previously suspended.
func (c *TaskHubGrpcClient) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	req := &protos.ResumeRequest{
		InstanceId: string(id),
		Reason:     wrapperspb.String(reason),
	}
	if _, err := c.client.ResumeInstance(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to resume orchestration: %w", err)
	}
	return nil
}

// PurgeOrchestrationState deletes the state of the specified orchestration instance.
//
// [api.api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
func (c *TaskHubGrpcClient) PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	req := &protos.PurgeInstancesRequest{
		Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)},
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure purge request: %w", err)
		}
	}

	res, err := c.client.PurgeInstances(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to purge orchestration state: %w", err)
	} else if res.GetDeletedInstanceCount() == 0 {
		return api.ErrInstanceNotFound
	}
	return nil
}

// SignalEntity sends a fire-and-forget operation to an entity.
func (c *TaskHubGrpcClient) SignalEntity(ctx context.Context, entityID api.EntityID, operationName string, opts ...api.SignalEntityOptions) error {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return err
	}
	if operationName == "" {
		return fmt.Errorf("entity operation name must not be empty")
	}
	req := &protos.SignalEntityRequest{
		InstanceId:         entityID.String(),
		Name:               operationName,
		RequestId:          uuid.NewString(),
		RequestTime:        timestamppb.Now(),
		ParentTraceContext: helpers.TraceContextFromSpan(trace.SpanFromContext(ctx)),
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure signal entity request: %w", err)
		}
	}
	var err error
	req.Input, err = largepayload.Externalize(ctx, c.largePayloads, req.Input)
	if err != nil {
		return fmt.Errorf("failed to externalize entity signal input: %w", err)
	}
	if _, err := c.client.SignalEntity(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to signal entity: %w", err)
	}
	return nil
}

// FetchEntityMetadata retrieves metadata for an entity instance.
func (c *TaskHubGrpcClient) FetchEntityMetadata(ctx context.Context, entityID api.EntityID, includeState bool) (*api.EntityMetadata, error) {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return nil, err
	}
	response, err := c.client.GetEntity(ctx, &protos.GetEntityRequest{
		InstanceId:   entityID.String(),
		IncludeState: includeState,
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to get entity metadata: %w", err)
	}
	if !response.Exists || response.Entity == nil {
		return nil, nil
	}
	return entityMetadataFromProto(ctx, c.largePayloads, response.Entity)
}

// QueryEntities queries entities matching the supplied filters.
func (c *TaskHubGrpcClient) QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error) {
	protoQuery := &protos.EntityQuery{
		IncludeState:     query.IncludeState,
		IncludeTransient: query.IncludeTransient,
	}
	if query.InstanceIDStartsWith != "" {
		protoQuery.InstanceIdStartsWith = wrapperspb.String(query.InstanceIDStartsWith)
	}
	if !query.LastModifiedFrom.IsZero() {
		protoQuery.LastModifiedFrom = timestamppb.New(query.LastModifiedFrom)
	}
	if !query.LastModifiedTo.IsZero() {
		protoQuery.LastModifiedTo = timestamppb.New(query.LastModifiedTo)
	}
	if query.PageSize > 0 {
		protoQuery.PageSize = wrapperspb.Int32(query.PageSize)
	}
	if query.ContinuationToken != "" {
		protoQuery.ContinuationToken = wrapperspb.String(query.ContinuationToken)
	}
	response, err := c.client.QueryEntities(ctx, &protos.QueryEntitiesRequest{Query: protoQuery})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to query entities: %w", err)
	}
	result := &api.EntityQueryResults{
		Entities:          make([]*api.EntityMetadata, 0, len(response.Entities)),
		ContinuationToken: response.ContinuationToken.GetValue(),
	}
	for _, entity := range response.Entities {
		metadata, err := entityMetadataFromProto(ctx, c.largePayloads, entity)
		if err != nil {
			return nil, err
		}
		result.Entities = append(result.Entities, metadata)
	}
	return result, nil
}

// CleanEntityStorage removes empty entities and releases orphaned locks.
func (c *TaskHubGrpcClient) CleanEntityStorage(ctx context.Context, request api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error) {
	protoRequest := &protos.CleanEntityStorageRequest{
		RemoveEmptyEntities:  request.RemoveEmptyEntities,
		ReleaseOrphanedLocks: request.ReleaseOrphanedLocks,
	}
	if request.ContinuationToken != "" {
		protoRequest.ContinuationToken = wrapperspb.String(request.ContinuationToken)
	}
	response, err := c.client.CleanEntityStorage(ctx, protoRequest)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to clean entity storage: %w", err)
	}
	return &api.CleanEntityStorageResult{
		ContinuationToken:     response.ContinuationToken.GetValue(),
		EmptyEntitiesRemoved:  response.EmptyEntitiesRemoved,
		OrphanedLocksReleased: response.OrphanedLocksReleased,
	}, nil
}

func entityMetadataFromProto(
	ctx context.Context,
	options *api.LargePayloadOptions,
	entity *protos.EntityMetadata,
) (*api.EntityMetadata, error) {
	if entity == nil {
		return nil, fmt.Errorf("entity metadata must not be nil")
	}
	entityID, err := api.EntityIDFromString(entity.InstanceId)
	if err != nil {
		return nil, fmt.Errorf("invalid entity metadata instance ID %q: %w", entity.InstanceId, err)
	}
	metadata := &api.EntityMetadata{
		InstanceID:       entityID,
		BacklogQueueSize: entity.BacklogQueueSize,
		LockedBy:         entity.LockedBy.GetValue(),
		SerializedState:  entity.SerializedState.GetValue(),
	}
	if entity.LastModifiedTime != nil {
		metadata.LastModifiedTime = entity.LastModifiedTime.AsTime()
	}
	state, err := largepayload.Hydrate(ctx, options, entity.SerializedState)
	if err != nil {
		return nil, fmt.Errorf("failed to hydrate entity state: %w", err)
	}
	metadata.SerializedState = state.GetValue()
	return metadata, nil
}

func makeGetInstanceRequest(id api.InstanceID, opts []api.FetchOrchestrationMetadataOptions) *protos.GetInstanceRequest {
	req := &protos.GetInstanceRequest{
		InstanceId:          string(id),
		GetInputsAndOutputs: true,
	}
	for _, configure := range opts {
		configure(req)
	}
	return req
}

// makeOrchestrationMetadata validates and converts protos.GetInstanceResponse to api.OrchestrationMetadata
// api.ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *TaskHubGrpcClient) makeOrchestrationMetadata(
	ctx context.Context,
	resp *protos.GetInstanceResponse,
) (*api.OrchestrationMetadata, error) {
	if !resp.Exists {
		return nil, api.ErrInstanceNotFound
	}
	if resp.OrchestrationState == nil {
		return nil, fmt.Errorf("orchestration state is nil")
	}
	if err := largepayload.TransformOrchestrationState(ctx, c.largePayloads, resp.OrchestrationState); err != nil {
		return nil, fmt.Errorf("failed to hydrate orchestration metadata: %w", err)
	}
	return orchestrationMetadataFromState(resp.OrchestrationState)
}

func orchestrationMetadataFromState(state *protos.OrchestrationState) (*api.OrchestrationMetadata, error) {
	if state == nil {
		return nil, errors.New("orchestration state is nil")
	}
	metadata := &api.OrchestrationMetadata{
		InstanceID:             api.InstanceID(state.InstanceId),
		Name:                   state.Name,
		Version:                state.Version.GetValue(),
		ExecutionID:            state.ExecutionId.GetValue(),
		ParentInstanceID:       api.InstanceID(state.ParentInstanceId.GetValue()),
		RuntimeStatus:          state.OrchestrationStatus,
		SerializedInput:        state.Input.GetValue(),
		SerializedCustomStatus: state.CustomStatus.GetValue(),
		SerializedOutput:       state.Output.GetValue(),
		FailureDetails:         state.FailureDetails,
		Tags:                   maps.Clone(state.Tags),
	}
	if state.ScheduledStartTimestamp != nil {
		metadata.ScheduledStartAt = state.ScheduledStartTimestamp.AsTime()
	}
	if state.CreatedTimestamp != nil {
		metadata.CreatedAt = state.CreatedTimestamp.AsTime()
	}
	if state.LastUpdatedTimestamp != nil {
		metadata.LastUpdatedAt = state.LastUpdatedTimestamp.AsTime()
	}
	if state.CompletedTimestamp != nil {
		metadata.CompletedAt = state.CompletedTimestamp.AsTime()
	}
	return metadata, nil
}
