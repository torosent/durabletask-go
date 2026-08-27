package backend

import (
	"context"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type largePayloadBackend struct {
	Backend
	options *api.LargePayloadOptions
}

// NewLargePayloadBackend decorates a backend with payload externalization and hydration.
func NewLargePayloadBackend(be Backend, options *api.LargePayloadOptions) (Backend, error) {
	if be == nil {
		return nil, fmt.Errorf("backend is required")
	}
	normalized, err := api.NormalizeLargePayloadOptions(options)
	if err != nil {
		return nil, err
	}
	if normalized == nil {
		return be, nil
	}
	return &largePayloadBackend{Backend: be, options: normalized}, nil
}

func (be *largePayloadBackend) CreateOrchestrationInstance(
	ctx context.Context,
	event *HistoryEvent,
	opts ...OrchestrationIdReusePolicyOptions,
) error {
	cloned, err := cloneAndTransformHistoryEvent(ctx, be.options, event, true)
	if err != nil {
		return err
	}
	return be.Backend.CreateOrchestrationInstance(ctx, cloned, opts...)
}

func (be *largePayloadBackend) AddNewOrchestrationEvent(ctx context.Context, id api.InstanceID, event *HistoryEvent) error {
	cloned, err := cloneAndTransformHistoryEvent(ctx, be.options, event, true)
	if err != nil {
		return err
	}
	return be.Backend.AddNewOrchestrationEvent(ctx, id, cloned)
}

func (be *largePayloadBackend) GetOrchestrationWorkItem(ctx context.Context) (*OrchestrationWorkItem, error) {
	workItem, err := be.Backend.GetOrchestrationWorkItem(ctx)
	if err != nil || workItem == nil {
		return workItem, err
	}
	for _, event := range workItem.NewEvents {
		if err := largepayload.TransformHistoryEvent(ctx, be.options, event, false); err != nil {
			return nil, fmt.Errorf("failed to hydrate orchestration work item: %w", err)
		}
	}
	return workItem, nil
}

func (be *largePayloadBackend) GetOrchestrationRuntimeState(
	ctx context.Context,
	workItem *OrchestrationWorkItem,
) (*OrchestrationRuntimeState, error) {
	state, err := be.Backend.GetOrchestrationRuntimeState(ctx, workItem)
	if err != nil || state == nil {
		return state, err
	}
	for _, events := range [][]*HistoryEvent{state.OldEvents(), state.NewEvents()} {
		for _, event := range events {
			if err := largepayload.TransformHistoryEvent(ctx, be.options, event, false); err != nil {
				return nil, fmt.Errorf("failed to hydrate orchestration history: %w", err)
			}
		}
	}
	state.CustomStatus, err = largepayload.Hydrate(ctx, be.options, state.CustomStatus)
	if err != nil {
		return nil, fmt.Errorf("failed to hydrate orchestration custom status: %w", err)
	}
	return state, nil
}

func (be *largePayloadBackend) CompleteOrchestrationWorkItem(ctx context.Context, workItem *OrchestrationWorkItem) error {
	if workItem == nil || workItem.State == nil {
		return be.Backend.CompleteOrchestrationWorkItem(ctx, workItem)
	}
	for _, events := range [][]*HistoryEvent{
		workItem.State.NewEvents(),
		workItem.State.PendingTasks(),
		workItem.State.PendingTimers(),
	} {
		for _, event := range events {
			if err := largepayload.TransformHistoryEvent(ctx, be.options, event, true); err != nil {
				return fmt.Errorf("failed to externalize orchestration history: %w", err)
			}
		}
	}
	for _, message := range workItem.State.PendingMessages() {
		if err := largepayload.TransformHistoryEvent(ctx, be.options, message.HistoryEvent, true); err != nil {
			return fmt.Errorf("failed to externalize orchestration message: %w", err)
		}
	}
	for _, message := range workItem.State.PendingEntityMessages() {
		if err := largepayload.TransformHistoryEvent(ctx, be.options, message.HistoryEvent, true); err != nil {
			return fmt.Errorf("failed to externalize entity message: %w", err)
		}
	}
	var err error
	workItem.State.CustomStatus, err = largepayload.Externalize(ctx, be.options, workItem.State.CustomStatus)
	if err != nil {
		return fmt.Errorf("failed to externalize orchestration custom status: %w", err)
	}
	return be.Backend.CompleteOrchestrationWorkItem(ctx, workItem)
}

func (be *largePayloadBackend) GetActivityWorkItem(ctx context.Context) (*ActivityWorkItem, error) {
	workItem, err := be.Backend.GetActivityWorkItem(ctx)
	if err != nil || workItem == nil {
		return workItem, err
	}
	if err := largepayload.TransformHistoryEvent(ctx, be.options, workItem.NewEvent, false); err != nil {
		return nil, fmt.Errorf("failed to hydrate activity work item: %w", err)
	}
	return workItem, nil
}

func (be *largePayloadBackend) CompleteActivityWorkItem(ctx context.Context, workItem *ActivityWorkItem) error {
	if workItem != nil && workItem.Result != nil {
		if err := largepayload.TransformHistoryEvent(ctx, be.options, workItem.Result, true); err != nil {
			return fmt.Errorf("failed to externalize activity result: %w", err)
		}
	}
	return be.Backend.CompleteActivityWorkItem(ctx, workItem)
}

func (be *largePayloadBackend) SignalEntity(ctx context.Context, request *protos.SignalEntityRequest) error {
	capability, ok := be.Backend.(EntitySignalBackend)
	if !ok {
		return api.ErrFeatureNotSupported
	}
	if request == nil {
		return fmt.Errorf("signal entity request is required")
	}
	cloned := proto.Clone(request).(*protos.SignalEntityRequest)
	var err error
	cloned.Input, err = largepayload.Externalize(ctx, be.options, cloned.Input)
	if err != nil {
		return fmt.Errorf("failed to externalize entity signal input: %w", err)
	}
	return capability.SignalEntity(ctx, cloned)
}

func (be *largePayloadBackend) GetEntityWorkItem(ctx context.Context) (*EntityWorkItem, error) {
	capability, ok := be.Backend.(EntityBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	workItem, err := capability.GetEntityWorkItem(ctx)
	if err != nil || workItem == nil {
		return workItem, err
	}
	state, err := largepayload.Hydrate(ctx, be.options, StringValue(workItem.State))
	if err != nil {
		return nil, fmt.Errorf("failed to hydrate entity state: %w", err)
	}
	if workItem.State != nil {
		value := state.GetValue()
		workItem.State = &value
	}
	for _, event := range workItem.Operations {
		if err := largepayload.TransformHistoryEvent(ctx, be.options, event, false); err != nil {
			return nil, fmt.Errorf("failed to hydrate entity operation: %w", err)
		}
	}
	return workItem, nil
}

func (be *largePayloadBackend) CompleteEntityWorkItem(ctx context.Context, workItem *EntityWorkItem) error {
	capability, ok := be.Backend.(EntityBackend)
	if !ok {
		return api.ErrFeatureNotSupported
	}
	if workItem != nil && workItem.Result != nil {
		if err := largepayload.TransformEntityBatchResult(ctx, be.options, workItem.Result); err != nil {
			return fmt.Errorf("failed to externalize entity result: %w", err)
		}
	}
	return capability.CompleteEntityWorkItem(ctx, workItem)
}

func (be *largePayloadBackend) AbandonEntityWorkItem(ctx context.Context, workItem *EntityWorkItem) error {
	capability, ok := be.Backend.(EntityBackend)
	if !ok {
		return api.ErrFeatureNotSupported
	}
	return capability.AbandonEntityWorkItem(ctx, workItem)
}

func (be *largePayloadBackend) GetEntityMetadata(
	ctx context.Context,
	entityID api.EntityID,
	includeState bool,
) (*api.EntityMetadata, error) {
	capability, ok := be.Backend.(EntityQueryBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	metadata, err := capability.GetEntityMetadata(ctx, entityID, includeState)
	if err != nil || metadata == nil || !includeState {
		return metadata, err
	}
	state, err := largepayload.Hydrate(ctx, be.options, wrapperspb.String(metadata.SerializedState))
	if err != nil {
		return nil, fmt.Errorf("failed to hydrate entity metadata: %w", err)
	}
	metadata.SerializedState = state.GetValue()
	return metadata, nil
}

func (be *largePayloadBackend) QueryEntities(
	ctx context.Context,
	query api.EntityQuery,
) (*api.EntityQueryResults, error) {
	capability, ok := be.Backend.(EntityQueryBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	result, err := capability.QueryEntities(ctx, query)
	if err != nil || result == nil || !query.IncludeState {
		return result, err
	}
	for _, metadata := range result.Entities {
		if metadata == nil {
			continue
		}
		state, err := largepayload.Hydrate(ctx, be.options, wrapperspb.String(metadata.SerializedState))
		if err != nil {
			return nil, fmt.Errorf("failed to hydrate queried entity state: %w", err)
		}
		metadata.SerializedState = state.GetValue()
	}
	return result, nil
}

func (be *largePayloadBackend) CleanEntityStorage(
	ctx context.Context,
	request api.CleanEntityStorageRequest,
) (*api.CleanEntityStorageResult, error) {
	capability, ok := be.Backend.(EntityQueryBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	return capability.CleanEntityStorage(ctx, request)
}

func (be *largePayloadBackend) GetOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	metadata, err := be.Backend.GetOrchestrationMetadata(ctx, id)
	if err != nil || metadata == nil {
		return metadata, err
	}
	if err := hydrateMetadata(ctx, be.options, metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (be *largePayloadBackend) QueryOrchestrations(ctx context.Context, query api.OrchestrationQuery) (*api.OrchestrationQueryResult, error) {
	capability, ok := be.Backend.(OrchestrationQueryBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	result, err := capability.QueryOrchestrations(ctx, query)
	if err != nil || result == nil || !query.FetchInputsAndOutputs {
		return result, err
	}
	for _, metadata := range result.Orchestrations {
		if err := hydrateMetadata(ctx, be.options, metadata); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (be *largePayloadBackend) ListInstanceIDs(ctx context.Context, query api.InstanceIDQuery) (*api.InstanceIDQueryResult, error) {
	capability, ok := be.Backend.(InstanceIDQueryBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	return capability.ListInstanceIDs(ctx, query)
}

func (be *largePayloadBackend) RestartInstance(ctx context.Context, id api.InstanceID, newID bool) (api.InstanceID, error) {
	capability, ok := be.Backend.(RestartInstanceBackend)
	if !ok {
		return api.EmptyInstanceID, api.ErrFeatureNotSupported
	}
	return capability.RestartInstance(ctx, id, newID)
}

func (be *largePayloadBackend) RewindInstance(ctx context.Context, id api.InstanceID, reason string) error {
	capability, ok := be.Backend.(RewindInstanceBackend)
	if !ok {
		return api.ErrFeatureNotSupported
	}
	return capability.RewindInstance(ctx, id, reason)
}

func (be *largePayloadBackend) PurgeInstances(ctx context.Context, request api.PurgeInstancesRequest) (*api.PurgeInstancesResult, error) {
	capability, ok := be.Backend.(PurgeInstancesBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	return capability.PurgeInstances(ctx, request)
}

func (be *largePayloadBackend) SkipGracefulOrchestrationTerminations(
	ctx context.Context,
	ids []api.InstanceID,
	reason string,
) ([]api.InstanceID, error) {
	capability, ok := be.Backend.(SkipGracefulTerminationsBackend)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	return capability.SkipGracefulOrchestrationTerminations(ctx, ids, reason)
}

func cloneAndTransformHistoryEvent(
	ctx context.Context,
	options *api.LargePayloadOptions,
	event *HistoryEvent,
	externalize bool,
) (*HistoryEvent, error) {
	if event == nil {
		return nil, ErrNilHistoryEvent
	}
	cloned := proto.Clone(event).(*HistoryEvent)
	if err := largepayload.TransformHistoryEvent(ctx, options, cloned, externalize); err != nil {
		return nil, err
	}
	return cloned, nil
}

func hydrateMetadata(ctx context.Context, options *api.LargePayloadOptions, metadata *api.OrchestrationMetadata) error {
	targets := []*string{&metadata.SerializedInput, &metadata.SerializedOutput, &metadata.SerializedCustomStatus}
	for _, target := range targets {
		hydrated, err := largepayload.Hydrate(ctx, options, wrapperspb.String(*target))
		if err != nil {
			return fmt.Errorf("failed to hydrate orchestration metadata: %w", err)
		}
		*target = hydrated.GetValue()
	}
	return nil
}
