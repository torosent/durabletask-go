package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// EntityExecutor is an optional extension implemented by executors that can
// process durable entity operation batches.
type EntityExecutor interface {
	ExecuteEntity(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error)
}

type EntitySignalBackend interface {
	SignalEntity(context.Context, *protos.SignalEntityRequest) error
}

type EntityQueryBackend interface {
	GetEntityMetadata(context.Context, api.EntityID, bool) (*api.EntityMetadata, error)
	QueryEntities(context.Context, api.EntityQuery) (*api.EntityQueryResults, error)
	CleanEntityStorage(context.Context, api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error)
}

// EntityBackend is an optional backend extension for native durable entity
// persistence and work-item scheduling.
type EntityBackend interface {
	EntitySignalBackend
	EntityQueryBackend
	GetEntityWorkItem(context.Context) (*EntityWorkItem, error)
	CompleteEntityWorkItem(context.Context, *EntityWorkItem) error
	AbandonEntityWorkItem(context.Context, *EntityWorkItem) error
}

// EntityWorkItem contains one serialized entity batch locked by a local backend.
type EntityWorkItem struct {
	InstanceID  api.EntityID
	ExecutionID string
	State       *string
	Operations  []*protos.HistoryEvent
	MessageIDs  []int64
	LockedBy    string
	RetryCount  int32
	EnqueuedAt  time.Time
	Result      *protos.EntityBatchResult
}

func (wi EntityWorkItem) String() string {
	return wi.InstanceID.String()
}

func (wi EntityWorkItem) IsWorkItem() bool {
	return true
}

// EntityBatchFromRequestV2 converts a backend-scheduled V2 entity request into
// the worker-facing batch model and response routing metadata.
func EntityBatchFromRequestV2(request *protos.EntityRequest) (*protos.EntityBatchRequest, []*protos.OperationInfo, error) {
	if request == nil {
		return nil, nil, fmt.Errorf("entity request must not be nil")
	}
	if _, err := api.EntityIDFromString(request.InstanceId); err != nil {
		return nil, nil, fmt.Errorf("invalid entity instance ID: %w", err)
	}
	batch := &protos.EntityBatchRequest{
		InstanceId:  request.InstanceId,
		EntityState: request.EntityState,
		Operations:  make([]*protos.OperationRequest, 0, len(request.OperationRequests)),
		Properties:  make(map[string]*structpb.Value),
	}
	operationInfos := make([]*protos.OperationInfo, 0, len(request.OperationRequests))
	for _, historyEvent := range request.OperationRequests {
		if historyEvent == nil {
			return nil, nil, fmt.Errorf("entity operation history event must not be nil")
		}
		switch {
		case historyEvent.GetEntityOperationSignaled() != nil:
			event := historyEvent.GetEntityOperationSignaled()
			if _, err := uuid.Parse(event.RequestId); err != nil {
				return nil, nil, fmt.Errorf("invalid entity signal request ID %q: %w", event.RequestId, err)
			}
			batch.Operations = append(batch.Operations, &protos.OperationRequest{
				Operation: event.Operation,
				RequestId: event.RequestId,
				Input:     event.Input,
			})
			batch.Properties[helpers.EntitySignalProperty(event.RequestId)] = structpb.NewBoolValue(true)
			operationInfos = append(operationInfos, &protos.OperationInfo{RequestId: event.RequestId})
		case historyEvent.GetEntityOperationCalled() != nil:
			event := historyEvent.GetEntityOperationCalled()
			if _, err := uuid.Parse(event.RequestId); err != nil {
				return nil, nil, fmt.Errorf("invalid entity call request ID %q: %w", event.RequestId, err)
			}
			batch.Operations = append(batch.Operations, &protos.OperationRequest{
				Operation: event.Operation,
				RequestId: event.RequestId,
				Input:     event.Input,
			})
			operationInfos = append(operationInfos, &protos.OperationInfo{
				RequestId: event.RequestId,
				ResponseDestination: &protos.OrchestrationInstance{
					InstanceId:  event.ParentInstanceId.GetValue(),
					ExecutionId: event.ParentExecutionId,
				},
			})
		}
	}
	if len(batch.Properties) == 0 {
		batch.Properties = nil
	}
	return batch, operationInfos, nil
}

type EntityMessageDescriptor struct {
	RequestID        string
	Kind             string
	ParentInstanceID string
	VisibleTime      *time.Time
}

func DescribeEntityMessage(event *protos.HistoryEvent) (EntityMessageDescriptor, error) {
	if event == nil {
		return EntityMessageDescriptor{}, ErrNilHistoryEvent
	}
	descriptor := EntityMessageDescriptor{}
	switch {
	case event.GetEntityOperationSignaled() != nil:
		value := event.GetEntityOperationSignaled()
		descriptor.RequestID = value.RequestId
		descriptor.Kind = "signal"
		descriptor.VisibleTime = timestampTime(value.ScheduledTime)
	case event.GetEntityOperationCalled() != nil:
		value := event.GetEntityOperationCalled()
		descriptor.RequestID = value.RequestId
		descriptor.Kind = "call"
		descriptor.ParentInstanceID = value.ParentInstanceId.GetValue()
		descriptor.VisibleTime = timestampTime(value.ScheduledTime)
	case event.GetEntityLockRequested() != nil:
		value := event.GetEntityLockRequested()
		descriptor.RequestID = value.CriticalSectionId
		descriptor.Kind = "lock"
		descriptor.ParentInstanceID = value.ParentInstanceId.GetValue()
	case event.GetEntityUnlockSent() != nil:
		value := event.GetEntityUnlockSent()
		descriptor.RequestID = value.CriticalSectionId
		descriptor.Kind = "unlock"
		descriptor.ParentInstanceID = value.ParentInstanceId.GetValue()
	default:
		return EntityMessageDescriptor{}, fmt.Errorf("unsupported entity message event")
	}
	if descriptor.RequestID == "" {
		return EntityMessageDescriptor{}, fmt.Errorf("entity message request ID must not be empty")
	}
	return descriptor, nil
}

func NewEntitySignalEvent(request *protos.SignalEntityRequest) (*protos.HistoryEvent, error) {
	if request == nil {
		return nil, fmt.Errorf("signal entity request must not be nil")
	}
	if _, err := api.EntityIDFromString(request.InstanceId); err != nil {
		return nil, err
	}
	if request.RequestId == "" {
		request.RequestId = uuid.NewString()
	}
	timestamp := request.RequestTime
	if timestamp == nil {
		timestamp = timestamppb.Now()
	}
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamp,
		EventType: &protos.HistoryEvent_EntityOperationSignaled{
			EntityOperationSignaled: &protos.EntityOperationSignaledEvent{
				RequestId:     request.RequestId,
				Operation:     request.Name,
				ScheduledTime: request.ScheduledTime,
				Input:         request.Input,
			},
		},
	}, nil
}

var entityRequestNamespace = uuid.MustParse("ea25d996-980f-59f4-a15d-24eaa7d445f0")

func DeterministicEntityRequestID(instanceID, executionID string, actionIndex int) string {
	return uuid.NewSHA1(
		entityRequestNamespace,
		[]byte(fmt.Sprintf("%s|%s|%d", instanceID, executionID, actionIndex)),
	).String()
}

func timestampTime(timestamp *timestamppb.Timestamp) *time.Time {
	if timestamp == nil {
		return nil
	}
	value := timestamp.AsTime()
	return &value
}

func NewEntityOperationResponseEvent(info *protos.OperationInfo, result *protos.OperationResult) (*protos.HistoryEvent, string, error) {
	if info == nil || info.ResponseDestination == nil {
		return nil, "", nil
	}
	if result == nil {
		return nil, "", fmt.Errorf("entity operation result must not be nil")
	}
	event := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now()}
	switch {
	case result.GetSuccess() != nil:
		event.EventType = &protos.HistoryEvent_EntityOperationCompleted{
			EntityOperationCompleted: &protos.EntityOperationCompletedEvent{
				RequestId: info.RequestId,
				Output:    result.GetSuccess().Result,
			},
		}
	case result.GetFailure() != nil:
		event.EventType = &protos.HistoryEvent_EntityOperationFailed{
			EntityOperationFailed: &protos.EntityOperationFailedEvent{
				RequestId:      info.RequestId,
				FailureDetails: result.GetFailure().FailureDetails,
			},
		}
	default:
		return nil, "", fmt.Errorf("entity operation result has no success or failure payload")
	}
	return event, info.ResponseDestination.InstanceId, nil
}

func NewEntityLockGrantedEvent(criticalSectionID string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityLockGranted{
			EntityLockGranted: &protos.EntityLockGrantedEvent{CriticalSectionId: criticalSectionID},
		},
	}
}

func NewEntitySignalMessage(
	sourceInstanceID string,
	sourceExecutionID string,
	actionIndex int,
	action *protos.SendSignalAction,
) *protos.HistoryEvent {
	requestID := DeterministicEntityRequestID(sourceInstanceID, sourceExecutionID, actionIndex)
	timestamp := action.RequestTime
	if timestamp == nil {
		timestamp = timestamppb.Now()
	}
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamp,
		EventType: &protos.HistoryEvent_EntityOperationSignaled{
			EntityOperationSignaled: &protos.EntityOperationSignaledEvent{
				RequestId:     requestID,
				Operation:     action.Name,
				ScheduledTime: action.ScheduledTime,
				Input:         action.Input,
			},
		},
	}
}

func StringValue(value *string) *wrapperspb.StringValue {
	if value == nil {
		return nil
	}
	return wrapperspb.String(*value)
}
