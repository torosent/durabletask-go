package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type TaskHubClient interface {
	ScheduleNewOrchestration(ctx context.Context, orchestrator any, opts ...api.NewOrchestrationOptions) (api.InstanceID, error)
	FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	WaitForOrchestrationStart(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error)
	TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error
	RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error
	SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error
	ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error
	PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error
}

// EntityTaskHubClient extends TaskHubClient with durable entity operations.
type EntityTaskHubClient interface {
	TaskHubClient
	SignalEntity(ctx context.Context, entityID api.EntityID, operationName string, opts ...api.SignalEntityOptions) error
	FetchEntityMetadata(ctx context.Context, entityID api.EntityID, includeState bool) (*api.EntityMetadata, error)
	QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error)
	CleanEntityStorage(ctx context.Context, req api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error)
}

type TaskHubManagementClient interface {
	TaskHubClient
	QueryInstances(context.Context, api.OrchestrationQuery) (*api.OrchestrationQueryResult, error)
	ListInstanceIDs(context.Context, api.InstanceIDQuery) (*api.InstanceIDQueryResult, error)
	RestartInstance(context.Context, api.InstanceID, ...api.RestartOptions) (api.InstanceID, error)
	RewindInstance(context.Context, api.InstanceID, ...api.RewindOptions) error
	PurgeInstances(context.Context, api.PurgeInstancesRequest) (*api.PurgeInstancesResult, error)
	SkipGracefulOrchestrationTerminations(context.Context, []api.InstanceID, string) ([]api.InstanceID, error)
	CreateTaskHub(context.Context, ...api.CreateTaskHubOptions) error
	DeleteTaskHub(context.Context) error
}

type backendClient struct {
	be Backend
}

func NewTaskHubClient(be Backend) TaskHubClient {
	return &backendClient{
		be: be,
	}
}

func NewTaskHubManagementClient(be Backend) TaskHubManagementClient {
	return &backendClient{
		be: be,
	}
}

func (c *backendClient) ScheduleNewOrchestration(ctx context.Context, orchestrator any, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	name := helpers.GetTaskFunctionName(orchestrator)
	req := &protos.CreateInstanceRequest{Name: name}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure create instance request: %w", err)
		}
	}
	if req.InstanceId == "" {
		u, err := uuid.NewV7()
		if err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to generate instance ID: %w", err)
		}
		req.InstanceId = u.String()
	}

	var span trace.Span
	ctx, span = helpers.StartNewCreateOrchestrationSpan(ctx, req.Name, req.Version.GetValue(), req.InstanceId)
	defer span.End()

	tc := helpers.TraceContextFromSpan(span)
	e := helpers.NewExecutionStartedEvent(req.Name, req.InstanceId, req.Input, nil, tc, req.ScheduledStartTimestamp, req.Version)
	e.GetExecutionStarted().Tags = maps.Clone(req.Tags)
	policy, err := orchestrationIDReusePolicyFromProto(req.OrchestrationIdReusePolicy, false)
	if err != nil {
		return api.EmptyInstanceID, fmt.Errorf("failed to decode orchestration ID reuse policy: %w", err)
	}
	if err := c.be.CreateOrchestrationInstance(ctx, e, WithOrchestrationIdReusePolicy(policy)); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return api.EmptyInstanceID, fmt.Errorf("failed to start orchestration: %w", err)
	}
	return api.InstanceID(req.InstanceId), nil
}

// FetchOrchestrationMetadata fetches metadata for the specified orchestration from the configured task hub.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	metadata, err := c.be.GetOrchestrationMetadata(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch orchestration metadata: %w", err)
	}
	return metadata, nil
}

// WaitForOrchestrationStart waits for an orchestration to start running and returns an [OrchestrationMetadata] object that contains
// metadata about the started instance.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return c.waitForOrchestrationCondition(ctx, id, func(metadata *api.OrchestrationMetadata) bool {
		return metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

// WaitForOrchestrationCompletion waits for an orchestration to complete and returns an [OrchestrationMetadata] object that contains
// metadata about the completed instance.
//
// ErrInstanceNotFound is returned when the specified orchestration doesn't exist.
func (c *backendClient) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return c.waitForOrchestrationCondition(ctx, id, func(metadata *api.OrchestrationMetadata) bool {
		return metadata.IsComplete()
	})
}

func (c *backendClient) waitForOrchestrationCondition(ctx context.Context, id api.InstanceID, condition func(metadata *api.OrchestrationMetadata) bool) (*api.OrchestrationMetadata, error) {
	b := backoff.ExponentialBackOff{
		InitialInterval:     100 * time.Millisecond,
		MaxInterval:         10 * time.Second,
		Multiplier:          1.5,
		RandomizationFactor: 0.05,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b.Reset()

	for {
		t := time.NewTimer(b.NextBackOff())
		select {
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return nil, ctx.Err()
		case <-t.C:
			metadata, err := c.FetchOrchestrationMetadata(ctx, id)
			if err != nil {
				return nil, err
			}
			if metadata != nil && condition(metadata) {
				return metadata, nil
			}
		}
	}
}

// TerminateOrchestration enqueues a message to terminate a running orchestration, causing it to stop receiving new events and
// go directly into the TERMINATED state. This operation is asynchronous. An orchestration worker must
// dequeue the termination event before the orchestration will be terminated.
func (c *backendClient) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	req := &protos.TerminateRequest{InstanceId: string(id), Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure termination request: %w", err)
		}
	}
	e := helpers.NewExecutionTerminatedEvent(req.Output, req.Recursive)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to submit termination request:: %w", err)
	}
	return nil
}

// RaiseEvent implements TaskHubClient and sends an asynchronous event notification to a waiting orchestration.
//
// In order to handle the event, the target orchestration instance must be waiting for an event named [eventName]
// using the [WaitForSingleEvent] method of the orchestration context parameter. If the target orchestration instance
// is not yet waiting for an event named [eventName], then the event will be bufferred in memory until a task
// subscribing to that event name is created.
//
// Raised events for a completed or non-existent orchestration instance will be silently discarded.
func (c *backendClient) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	req := &protos.RaiseEventRequest{InstanceId: string(id), Name: eventName}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure raise event request: %w", err)
		}
	}

	e := helpers.NewEventRaisedEvent(req.Name, req.Input)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to raise event: %w", err)
	}
	return nil
}

// SuspendOrchestration suspends an orchestration instance, halting processing of its events until a "resume" operation resumes it.
//
// Note that suspended orchestrations are still considered to be "running" even though they will not process events.
func (c *backendClient) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	e := helpers.NewSuspendOrchestrationEvent(reason)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to suspend orchestration: %w", err)
	}
	return nil
}

// ResumeOrchestration resumes an orchestration instance that was previously suspended.
func (c *backendClient) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	e := helpers.NewResumeOrchestrationEvent(reason)
	if err := c.be.AddNewOrchestrationEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to resume orchestration: %w", err)
	}
	return nil
}

// PurgeOrchestrationState deletes the state of the specified orchestration instance.
//
// [api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
// [api.ErrNotCompleted] is returned if the specified orchestration instance is still running.
func (c *backendClient) PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	req := &protos.PurgeInstancesRequest{Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)}, Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure purge request: %w", err)
		}
	}
	if _, err := purgeOrchestrationState(ctx, c.be, id, req.Recursive); err != nil {
		return fmt.Errorf("failed to purge orchestration state: %w", err)
	}
	return nil
}

// SignalEntity sends a fire-and-forget operation to an entity.
func (c *backendClient) SignalEntity(ctx context.Context, entityID api.EntityID, operationName string, opts ...api.SignalEntityOptions) error {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return err
	}
	if operationName == "" {
		return fmt.Errorf("entity operation name must not be empty")
	}
	req := &protos.SignalEntityRequest{
		InstanceId:  entityID.String(),
		Name:        operationName,
		RequestId:   uuid.NewString(),
		RequestTime: timestamppb.Now(),
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure signal entity request: %w", err)
		}
	}
	if entityBackend, ok := c.be.(EntitySignalBackend); ok {
		if err := entityBackend.SignalEntity(ctx, req); err != nil {
			return fmt.Errorf("failed to signal entity: %w", err)
		}
		return nil
	}

	request := helpers.EntityRequestMessage{
		ID:        req.RequestId,
		IsSignal:  true,
		Operation: req.Name,
	}
	if req.Input != nil {
		request.Input = req.Input.Value
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal entity signal: %w", err)
	}
	startEvent := helpers.NewExecutionStartedEvent(entityID.Name, req.InstanceId, nil, nil, nil, nil, nil)
	createErr := c.be.CreateOrchestrationInstance(ctx, startEvent, WithOrchestrationIdReusePolicy(&api.OrchestrationIdReusePolicy{
		Action:          api.REUSE_ID_ACTION_IGNORE,
		OperationStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_RUNNING, api.RUNTIME_STATUS_PENDING},
	}))
	if createErr != nil && !errors.Is(createErr, api.ErrDuplicateInstance) && !errors.Is(createErr, api.ErrIgnoreInstance) {
		return fmt.Errorf("failed to create compatibility entity instance: %w", createErr)
	}
	event := helpers.NewEventRaisedEvent(helpers.EntityRequestEventName, wrapperspb.String(string(payload)))
	if req.ScheduledTime != nil {
		event.Timestamp = req.ScheduledTime
	}
	if err := c.be.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), event); err != nil {
		return fmt.Errorf("failed to enqueue compatibility entity signal: %w", err)
	}
	return nil
}

func (c *backendClient) QueryInstances(ctx context.Context, query api.OrchestrationQuery) (*api.OrchestrationQueryResult, error) {
	capability, ok := c.be.(OrchestrationQueryBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	return capability.QueryOrchestrations(ctx, query)
}

func (c *backendClient) ListInstanceIDs(ctx context.Context, query api.InstanceIDQuery) (*api.InstanceIDQueryResult, error) {
	capability, ok := c.be.(InstanceIDQueryBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	return capability.ListInstanceIDs(ctx, query)
}

func (c *backendClient) RestartInstance(ctx context.Context, id api.InstanceID, opts ...api.RestartOptions) (api.InstanceID, error) {
	req := &protos.RestartInstanceRequest{InstanceId: string(id)}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure restart request: %w", err)
		}
	}
	capability, ok := c.be.(RestartInstanceBackend)
	if !ok {
		return api.EmptyInstanceID, api.ErrFeatureNotSupported
	}
	return capability.RestartInstance(ctx, id, req.RestartWithNewInstanceId)
}

func (c *backendClient) RewindInstance(ctx context.Context, id api.InstanceID, opts ...api.RewindOptions) error {
	req := &protos.RewindInstanceRequest{InstanceId: string(id)}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure rewind request: %w", err)
		}
	}
	capability, ok := c.be.(RewindInstanceBackend)
	if !ok {
		return api.ErrFeatureNotSupported
	}
	return capability.RewindInstance(ctx, id, req.Reason.GetValue())
}

func (c *backendClient) PurgeInstances(ctx context.Context, req api.PurgeInstancesRequest) (*api.PurgeInstancesResult, error) {
	capability, ok := c.be.(PurgeInstancesBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	if len(req.InstanceIDs) > api.MaxInstanceBatchSize {
		result := &api.PurgeInstancesResult{IsComplete: true}
		for start := 0; start < len(req.InstanceIDs); start += api.MaxInstanceBatchSize {
			end := min(start+api.MaxInstanceBatchSize, len(req.InstanceIDs))
			batchRequest := req
			batchRequest.InstanceIDs = append([]api.InstanceID(nil), req.InstanceIDs[start:end]...)
			partial, err := pollBackendPurge(ctx, capability, batchRequest)
			if err != nil {
				return nil, err
			}
			result.DeletedInstanceCount += partial.DeletedInstanceCount
			result.IsComplete = result.IsComplete && partial.IsComplete
		}
		return result, nil
	}
	return pollBackendPurge(ctx, capability, req)
}

func pollBackendPurge(
	ctx context.Context,
	capability PurgeInstancesBackend,
	req api.PurgeInstancesRequest,
) (*api.PurgeInstancesResult, error) {
	pollInterval := req.PollInterval
	if pollInterval <= 0 {
		pollInterval = api.DefaultPurgePollInterval
	}
	result := &api.PurgeInstancesResult{}
	for {
		partial, err := capability.PurgeInstances(ctx, req)
		if err != nil {
			return nil, err
		}
		result.DeletedInstanceCount += partial.DeletedInstanceCount
		if partial.IsComplete {
			result.IsComplete = true
			return result, nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *backendClient) SkipGracefulOrchestrationTerminations(ctx context.Context, ids []api.InstanceID, reason string) ([]api.InstanceID, error) {
	capability, ok := c.be.(SkipGracefulTerminationsBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	return capability.SkipGracefulOrchestrationTerminations(ctx, ids, reason)
}

func (c *backendClient) CreateTaskHub(ctx context.Context, opts ...api.CreateTaskHubOptions) error {
	req := &protos.CreateTaskHubRequest{}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure task hub creation request: %w", err)
		}
	}
	if req.RecreateIfExists {
		if err := c.be.DeleteTaskHub(ctx); err != nil && !errors.Is(err, ErrTaskHubNotFound) {
			return fmt.Errorf("failed to recreate task hub: %w", err)
		}
	}
	if err := c.be.CreateTaskHub(ctx); err != nil {
		return fmt.Errorf("failed to create task hub: %w", err)
	}
	return nil
}

// FetchEntityMetadata retrieves metadata for an entity instance.
func (c *backendClient) FetchEntityMetadata(ctx context.Context, entityID api.EntityID, includeState bool) (*api.EntityMetadata, error) {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return nil, err
	}
	if entityBackend, ok := c.be.(EntityQueryBackend); ok {
		return entityBackend.GetEntityMetadata(ctx, entityID, includeState)
	}
	metadata, err := c.be.GetOrchestrationMetadata(ctx, api.InstanceID(entityID.String()))
	if errors.Is(err, api.ErrInstanceNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get compatibility entity metadata: %w", err)
	}
	result := &api.EntityMetadata{
		InstanceID:       entityID,
		LastModifiedTime: metadata.LastUpdatedAt,
	}
	if includeState {
		result.SerializedState = metadata.SerializedCustomStatus
	}
	return result, nil
}

// QueryEntities queries native entity storage.
func (c *backendClient) QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error) {
	entityBackend, ok := c.be.(EntityQueryBackend)
	if !ok {
		return nil, fmt.Errorf("QueryEntities requires an EntityBackend with native entity support")
	}
	return entityBackend.QueryEntities(ctx, query)
}

// CleanEntityStorage removes empty entities and releases orphaned locks.
func (c *backendClient) CleanEntityStorage(ctx context.Context, req api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error) {
	entityBackend, ok := c.be.(EntityQueryBackend)
	if !ok {
		return nil, fmt.Errorf("CleanEntityStorage requires an EntityBackend with native entity support")
	}
	return entityBackend.CleanEntityStorage(ctx, req)
}

func (c *backendClient) DeleteTaskHub(ctx context.Context) error {
	if err := c.be.DeleteTaskHub(ctx); err != nil {
		return fmt.Errorf("failed to delete task hub: %w", err)
	}
	return nil
}
