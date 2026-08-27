package task

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type taskExecutor struct {
	Registry             *TaskRegistry
	versioning           *VersioningOptions
	orchestrationOptions OrchestrationOptions
	logger               *slog.Logger
	metrics              backend.MetricsHooks
	contextFields        api.ContextFields
}

// TaskExecutorOption configures the in-memory task executor.
type TaskExecutorOption func(*taskExecutor)

// WithVersioning configures version-aware orchestration and activity dispatch.
func WithVersioning(options VersioningOptions) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.versioning = &options
	}
}

// WithOrchestrationOptions configures deterministic orchestration engine policies.
func WithOrchestrationOptions(options OrchestrationOptions) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.orchestrationOptions = options
	}
}

// WithLogger configures the slog logger exposed to orchestration and activity code.
func WithLogger(logger *slog.Logger) TaskExecutorOption {
	return func(executor *taskExecutor) {
		if logger != nil {
			executor.logger = logger
		}
	}
}

// WithMetricsHooks configures optional backend-neutral metric callbacks.
func WithMetricsHooks(hooks backend.MetricsHooks) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.metrics = hooks
	}
}

// WithContextFields configures immutable fields propagated into task contexts.
func WithContextFields(fields api.ContextFields) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.contextFields = make(api.ContextFields, len(fields))
		maps.Copy(executor.contextFields, fields)
	}
}

// NewTaskExecutor returns a [backend.Executor] implementation that executes orchestrator and activity functions in-memory.
func NewTaskExecutor(registry *TaskRegistry, opts ...TaskExecutorOption) backend.Executor {
	executor := &taskExecutor{
		Registry: registry,
		logger:   slog.Default(),
	}
	for _, configure := range opts {
		configure(executor)
	}
	return executor
}

// ExecuteActivity implements backend.Executor and executes an activity function in the current goroutine.
func (te *taskExecutor) ExecuteActivity(ctx context.Context, id api.InstanceID, e *protos.HistoryEvent) (response *protos.HistoryEvent, err error) {
	ts := e.GetTaskScheduled()
	if ts == nil {
		// No clean way to deal with this other than to abandon it
		return nil, fmt.Errorf("unexpected event type for ExecuteActivity: %v", e.EventType)
	}
	if versionErr := te.versioning.check(ts.GetVersion().GetValue()); versionErr != nil {
		if te.versioning.FailureStrategy == VersionFailureReject {
			return nil, versionErr
		}
		return helpers.NewTaskFailedEvent(e.EventId, versionFailureDetails(versionErr)), nil
	}
	ctx = api.ContextWithFields(ctx, te.contextFields)
	tagInfo, tagFields := contextprop.Decode(ts.GetTags())
	ctx = api.ContextWithFields(ctx, tagFields)
	orchestrationInfo, _ := api.OrchestrationContextInfoFromContext(ctx)
	if orchestrationInfo.Name == "" {
		orchestrationInfo.Name = tagInfo.Name
	}
	if orchestrationInfo.Version == "" {
		orchestrationInfo.Version = tagInfo.Version
	}
	if orchestrationInfo.ParentInstanceID == "" {
		orchestrationInfo.ParentInstanceID = tagInfo.ParentInstanceID
	}
	if orchestrationInfo.InstanceID == "" {
		orchestrationInfo.InstanceID = tagInfo.InstanceID
		if orchestrationInfo.InstanceID == "" {
			orchestrationInfo.InstanceID = id
		}
	}
	ctx = api.WithOrchestrationContextInfo(ctx, orchestrationInfo)
	ctx = api.WithActivityContextInfo(ctx, api.ActivityContextInfo{
		InstanceID: id,
		Name:       ts.Name,
		Version:    ts.GetVersion().GetValue(),
		TaskID:     e.EventId,
	})
	ctx = withActivityLogger(ctx, te.logger)
	invoker, ok := te.Registry.activities[ts.Name]
	if !ok {
		// try the wildcard match
		invoker, ok = te.Registry.activities["*"]
		if !ok {
			return helpers.NewTaskFailedEvent(e.EventId, &protos.TaskFailureDetails{
				ErrorType:    "TaskActivityNotRegistered",
				ErrorMessage: fmt.Sprintf("no task activity named '%s' was registered", ts.Name),
			}), nil
		}
	}
	activityCtx := newTaskActivityContext(ctx, e.EventId, ts)

	// convert panics into activity failures
	defer func() {
		panicVal := recover()
		if panicVal != nil {
			response = helpers.NewTaskFailedEvent(e.EventId, &protos.TaskFailureDetails{
				ErrorType:    "TaskActivityPanic",
				ErrorMessage: fmt.Sprintf("panic: %v", panicVal),
			})
		}
	}()

	result, err := invoker(activityCtx)
	if err != nil {
		return helpers.NewTaskFailedEvent(e.EventId, &protos.TaskFailureDetails{
			ErrorType:    fmt.Sprintf("%T", err),
			ErrorMessage: fmt.Sprintf("%+v", err),
		}), nil
	}

	bytes, err := marshalData(result)
	if err != nil {
		return helpers.NewTaskFailedEvent(e.EventId, &protos.TaskFailureDetails{
			ErrorType:    fmt.Sprintf("%T", err),
			ErrorMessage: fmt.Sprintf("%+v", err),
		}), nil
	}
	var rawResult *wrapperspb.StringValue
	if len(bytes) > 0 {
		rawResult = wrapperspb.String(string(bytes))
	}
	return helpers.NewTaskCompletedEvent(e.EventId, rawResult), nil
}

// ExecuteOrchestrator implements backend.Executor and executes an orchestrator function in the current goroutine.
func (te *taskExecutor) ExecuteOrchestrator(ctx context.Context, id api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*backend.ExecutionResults, error) {
	version := orchestrationVersion(oldEvents, newEvents)
	if versionErr := te.versioning.check(version); versionErr != nil {
		if te.versioning.FailureStrategy == VersionFailureReject {
			return nil, versionErr
		}
		return &backend.ExecutionResults{
			Response: &protos.OrchestratorResponse{
				InstanceId: string(id),
				Actions: []*protos.OrchestratorAction{
					helpers.NewCompleteOrchestrationAction(
						0,
						protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
						nil,
						nil,
						versionFailureDetails(versionErr),
					),
				},
			},
		}, nil
	}
	orchestrationCtx := newOrchestrationContext(
		ctx,
		te.Registry,
		id,
		oldEvents,
		newEvents,
		te.orchestrationOptions,
		te.logger,
		te.metrics,
		te.contextFields,
	)
	actions := orchestrationCtx.start()
	te.reportHistoryMetric(orchestrationCtx)

	results := &backend.ExecutionResults{
		Response: &protos.OrchestratorResponse{
			InstanceId:   string(id),
			Actions:      actions,
			CustomStatus: wrapperspb.String(orchestrationCtx.customStatus),
		},
	}
	return results, nil
}

// ExecuteEntity executes an entity batch in the current goroutine.
func (te *taskExecutor) ExecuteEntity(ctx context.Context, req *protos.EntityBatchRequest) (*protos.EntityBatchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("entity batch request must not be nil")
	}
	entityID, err := api.EntityIDFromString(req.InstanceId)
	if err != nil {
		return nil, fmt.Errorf("invalid entity instance ID: %w", err)
	}
	invoker, ok := te.Registry.entities[entityID.Name]
	if !ok {
		invoker, ok = te.Registry.entities["*"]
	}
	if !ok {
		result := &protos.EntityBatchResult{
			EntityState: req.EntityState,
			Results:     make([]*protos.OperationResult, 0, len(req.Operations)),
		}
		for range req.Operations {
			now := time.Now().UTC()
			failure := entityOperationFailure(
				"EntityTaskNotFound",
				fmt.Sprintf("no entity named '%s' was registered", entityID.Name),
				now,
				now,
			)
			failure.GetFailure().FailureDetails.IsNonRetriable = true
			result.Results = append(result.Results, failure)
		}
		return result, nil
	}
	if req.EntityState == nil {
		if property, exists := req.Properties["entityStateIncluded"]; exists && !property.GetBoolValue() {
			return &protos.EntityBatchResult{RequiresState: true}, nil
		}
	}

	var state entityState
	if req.EntityState != nil {
		state = entityState{value: []byte(req.EntityState.Value), hasValue: true}
	}
	result := &protos.EntityBatchResult{
		Results: make([]*protos.OperationResult, 0, len(req.Operations)),
		Actions: make([]*protos.OperationAction, 0),
	}
	nextActionID := int32(0)
	for _, operation := range req.Operations {
		if operation == nil {
			result.Results = append(result.Results, entityOperationFailure(
				"InvalidEntityOperation",
				"entity operation request must not be nil",
				time.Now().UTC(),
				time.Now().UTC(),
			))
			continue
		}
		startedAt := time.Now().UTC()
		isSignal := req.Properties[helpers.EntitySignalProperty(operation.RequestId)].GetBoolValue()
		operationCtx := te.newEntityContext(ctx, entityID, operation, state, nextActionID, startedAt, isSignal)
		output, operationErr := invokeEntity(invoker, operationCtx)
		endedAt := time.Now().UTC()

		var rawResult *wrapperspb.StringValue
		if operationErr == nil && output != nil {
			bytes, marshalErr := marshalData(output)
			if marshalErr != nil {
				operationErr = fmt.Errorf("failed to marshal entity result: %w", marshalErr)
			} else if len(bytes) > 0 {
				rawResult = wrapperspb.String(string(bytes))
			}
		}

		if operationErr != nil {
			result.Results = append(result.Results, entityOperationFailure(
				fmt.Sprintf("%T", operationErr),
				fmt.Sprintf("%+v", operationErr),
				startedAt,
				endedAt,
			))
			continue
		}

		state = operationCtx.state
		nextActionID = operationCtx.actionIDSeq
		result.Actions = append(result.Actions, operationCtx.actions...)
		result.Results = append(result.Results, &protos.OperationResult{
			ResultType: &protos.OperationResult_Success{
				Success: &protos.OperationResultSuccess{
					Result:       rawResult,
					StartTimeUtc: timestamppb.New(startedAt),
					EndTimeUtc:   timestamppb.New(endedAt),
				},
			},
		})
	}

	if state.hasValue {
		result.EntityState = wrapperspb.String(string(state.value))
	}
	return result, nil
}

func (te *taskExecutor) newEntityContext(
	ctx context.Context,
	entityID api.EntityID,
	operation *protos.OperationRequest,
	state entityState,
	nextActionID int32,
	currentTime time.Time,
	isSignal bool,
) *EntityContext {
	ctx = api.ContextWithFields(ctx, te.contextFields)
	ctx = api.WithEntityContextInfo(ctx, api.EntityContextInfo{
		EntityID:  entityID,
		Operation: operation.Operation,
		RequestID: operation.RequestId,
		IsSignal:  isSignal,
	})
	logger := te.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(
		slog.String("durabletask.entity.id", entityID.String()),
		slog.String("durabletask.entity.operation", operation.Operation),
		slog.String("durabletask.entity.request_id", operation.RequestId),
	)
	ctx = context.WithValue(ctx, taskLoggerKey{}, logger)
	return &EntityContext{
		ID:          entityID,
		Operation:   operation.Operation,
		RequestID:   operation.RequestId,
		IsSignal:    isSignal,
		rawInput:    []byte(operation.Input.GetValue()),
		state:       state,
		actionIDSeq: nextActionID,
		currentTime: currentTime,
		ctx:         ctx,
		logger:      logger,
	}
}

func invokeEntity(invoker Entity, entityCtx *EntityContext) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("entity operation panic: %v", recovered)
		}
	}()
	return invoker(entityCtx)
}

func entityOperationFailure(errorType, message string, startedAt, endedAt time.Time) *protos.OperationResult {
	return &protos.OperationResult{
		ResultType: &protos.OperationResult_Failure{
			Failure: &protos.OperationResultFailure{
				FailureDetails: &protos.TaskFailureDetails{
					ErrorType:    errorType,
					ErrorMessage: message,
				},
				StartTimeUtc: timestamppb.New(startedAt),
				EndTimeUtc:   timestamppb.New(endedAt),
			},
		},
	}
}

func (te *taskExecutor) reportHistoryMetric(ctx *OrchestrationContext) {
	if te.metrics.History == nil {
		return
	}
	metric := backend.HistoryMetric{
		InstanceID:           ctx.ID,
		OrchestrationName:    ctx.Name,
		OrchestrationVersion: ctx.Version,
		HistoryLength:        ctx.HistoryLength(),
		ProcessedEvents:      ctx.processedEventsThisTurn,
		HistoryLimitExceeded: ctx.HistoryLimitExceeded(),
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			ctx.Logger().Error("history metrics callback panicked", "error", recovered)
		}
	}()
	te.metrics.History(metric)
}

func orchestrationVersion(eventLists ...[]*protos.HistoryEvent) string {
	for _, events := range eventLists {
		for _, event := range events {
			if started := event.GetExecutionStarted(); started != nil {
				return started.GetVersion().GetValue()
			}
		}
	}
	return ""
}

func versionFailureDetails(err error) *protos.TaskFailureDetails {
	return &protos.TaskFailureDetails{
		ErrorType:      "VersionMismatch",
		ErrorMessage:   err.Error(),
		IsNonRetriable: true,
	}
}

func (te taskExecutor) Shutdown(ctx context.Context) error {
	// Nothing to do
	return nil
}

func unmarshalData(data []byte, v any) error {
	switch {
	case v == nil:
		return nil
	case len(data) == 0:
		return nil
	default:
		return json.Unmarshal(data, v)
	}
}

func marshalData(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
