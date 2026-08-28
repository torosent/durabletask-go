package backend

import (
	"context"
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
	"github.com/microsoft/durabletask-go/internal/historyconv"
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
	GetOrchestrationHistory(context.Context, api.InstanceID, api.HistoryQuery) (*api.OrchestrationHistory, error)
	StreamOrchestrationHistory(context.Context, api.InstanceID, api.HistoryQuery, api.HistoryEventHandler) error
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
	be             Backend
	defaultVersion string
	converter      api.DataConverter
}

// TaskHubClientOption configures an embedded task hub client.
type TaskHubClientOption func(*backendClient)

// WithDefaultVersion configures the version used when a top-level orchestration
// is scheduled without an explicit [api.WithVersion] option.
func WithDefaultVersion(version string) TaskHubClientOption {
	return func(client *backendClient) {
		client.defaultVersion = version
	}
}

// WithDataConverter configures application payload serialization.
func WithDataConverter(converter api.DataConverter) TaskHubClientOption {
	return func(client *backendClient) {
		client.converter = api.NormalizeDataConverter(converter)
	}
}

func NewTaskHubClient(be Backend, opts ...TaskHubClientOption) TaskHubClient {
	return newBackendClient(be, opts)
}

func NewTaskHubManagementClient(be Backend, opts ...TaskHubClientOption) TaskHubManagementClient {
	return newBackendClient(be, opts)
}

func newBackendClient(be Backend, opts []TaskHubClientOption) *backendClient {
	client := &backendClient{be: be, converter: api.DefaultDataConverter()}
	for _, configure := range opts {
		configure(client)
	}
	return client
}

func (c *backendClient) ScheduleNewOrchestration(ctx context.Context, orchestrator any, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	name := helpers.GetTaskFunctionName(orchestrator)
	req := &protos.CreateInstanceRequest{Name: name}
	for _, configure := range opts {
		if err := configure(req, c.converter); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure create instance request: %w", err)
		}
	}
	if req.Version == nil && c.defaultVersion != "" {
		req.Version = wrapperspb.String(c.defaultVersion)
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
	if metadata != nil {
		metadata.Converter = c.converter
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
		if err := configure(req, c.converter); err != nil {
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
		if err := configure(req, c.converter); err != nil {
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
		if err := configure(req, c.converter); err != nil {
			return fmt.Errorf("failed to configure signal entity request: %w", err)
		}
	}
	if entityBackend, ok := GetBackendCapability[EntitySignalBackend](c.be); ok {
		if err := entityBackend.SignalEntity(ctx, req); err != nil {
			return fmt.Errorf("failed to signal entity: %w", err)
		}
		return nil
	}
	return api.ErrFeatureNotSupported
}

func (c *backendClient) QueryInstances(ctx context.Context, query api.OrchestrationQuery) (*api.OrchestrationQueryResult, error) {
	result, err := queryOrchestrations(ctx, c.be, query)
	if err != nil {
		return nil, err
	}
	for _, metadata := range result.Orchestrations {
		metadata.Converter = c.converter
	}
	return result, nil
}

func (c *backendClient) GetOrchestrationHistory(
	ctx context.Context,
	id api.InstanceID,
	query api.HistoryQuery,
) (*api.OrchestrationHistory, error) {
	return historyconv.Collect(id, query, func(handler api.HistoryEventHandler) error {
		return c.StreamOrchestrationHistory(ctx, id, query, handler)
	})
}

func (c *backendClient) StreamOrchestrationHistory(
	ctx context.Context,
	id api.InstanceID,
	query api.HistoryQuery,
	handler api.HistoryEventHandler,
) error {
	normalized, err := historyconv.NormalizeStreamRequest(id, query, handler)
	if err != nil {
		return err
	}
	metadata, err := c.be.GetOrchestrationMetadata(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to fetch orchestration history metadata: %w", err)
	}
	if metadata == nil || normalized.ExecutionID != "" && metadata.ExecutionID != normalized.ExecutionID {
		return api.ErrInstanceNotFound
	}
	state, err := c.be.GetOrchestrationRuntimeState(ctx, &OrchestrationWorkItem{InstanceID: id})
	if err != nil {
		return fmt.Errorf("failed to fetch orchestration history: %w", err)
	}
	if state == nil {
		return api.ErrInstanceNotFound
	}
	converter := historyconv.New(c.converter)
	eventCount := 0
	for _, events := range [][]*HistoryEvent{state.OldEvents(), state.NewEvents()} {
		for _, event := range events {
			if err := ctx.Err(); err != nil {
				return err
			}
			converted, err := converter.Convert(event)
			if err != nil {
				return fmt.Errorf("failed to convert orchestration history event %d: %w", eventCount, err)
			}
			if err := handler(converted); err != nil {
				return err
			}
			eventCount++
		}
	}
	return nil
}

func (c *backendClient) ListInstanceIDs(ctx context.Context, query api.InstanceIDQuery) (*api.InstanceIDQueryResult, error) {
	capability, ok := GetBackendCapability[InstanceIDQueryBackend](c.be)
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
	capability, ok := GetBackendCapability[RestartInstanceBackend](c.be)
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
	capability, ok := GetBackendCapability[RewindInstanceBackend](c.be)
	if !ok {
		return api.ErrFeatureNotSupported
	}
	return capability.RewindInstance(ctx, id, req.Reason.GetValue())
}

func (c *backendClient) PurgeInstances(ctx context.Context, req api.PurgeInstancesRequest) (*api.PurgeInstancesResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	capability, ok := GetBackendCapability[PurgeInstancesBackend](c.be)
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
			if partial != nil {
				result.DeletedInstanceCount += partial.DeletedInstanceCount
				result.IsComplete = result.IsComplete && partial.IsComplete
			}
			if err != nil {
				return result, err
			}
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
	if req.Filter != nil && req.Filter.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Filter.Timeout)
		defer cancel()
	}
	pollInterval := req.PollInterval
	if pollInterval <= 0 {
		pollInterval = api.DefaultPurgePollInterval
	}
	result := &api.PurgeInstancesResult{}
	for {
		partial, err := capability.PurgeInstances(ctx, req)
		if err != nil {
			if partial != nil {
				result.DeletedInstanceCount += partial.DeletedInstanceCount
				result.IsComplete = partial.IsComplete
				return result, err
			}
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
			return result, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *backendClient) SkipGracefulOrchestrationTerminations(ctx context.Context, ids []api.InstanceID, reason string) ([]api.InstanceID, error) {
	capability, ok := GetBackendCapability[SkipGracefulTerminationsBackend](c.be)
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
	if entityBackend, ok := GetBackendCapability[EntityQueryBackend](c.be); ok {
		metadata, err := entityBackend.GetEntityMetadata(ctx, entityID, includeState)
		if metadata != nil {
			metadata.Converter = c.converter
		}
		return metadata, err
	}
	return nil, api.ErrFeatureNotSupported
}

// QueryEntities queries native entity storage.
func (c *backendClient) QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error) {
	entityBackend, ok := GetBackendCapability[EntityQueryBackend](c.be)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	result, err := entityBackend.QueryEntities(ctx, query)
	if err != nil {
		return nil, err
	}
	for _, metadata := range result.Entities {
		metadata.Converter = c.converter
	}
	return result, nil
}

// CleanEntityStorage removes empty entities and releases orphaned locks.
func (c *backendClient) CleanEntityStorage(ctx context.Context, req api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error) {
	entityBackend, ok := GetBackendCapability[EntityQueryBackend](c.be)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	return entityBackend.CleanEntityStorage(ctx, req)
}

func (c *backendClient) DeleteTaskHub(ctx context.Context) error {
	if err := c.be.DeleteTaskHub(ctx); err != nil {
		return fmt.Errorf("failed to delete task hub: %w", err)
	}
	return nil
}
