package backend

import (
	"fmt"
	"maps"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func RebuildRewindHistory(
	id api.InstanceID,
	history []*protos.HistoryEvent,
) ([]*protos.HistoryEvent, *protos.ExecutionStartedEvent, map[int32]string, error) {
	failedTasks := make(map[int32]struct{})
	failedSubOrchestrations := make(map[int32]struct{})
	scheduledTasks := make(map[int32]struct{})
	createdSubOrchestrations := make(map[int32]string)
	var originalStart *protos.ExecutionStartedEvent
	for _, event := range history {
		switch {
		case event == nil:
			return nil, nil, nil, fmt.Errorf("%w: nil history event", api.ErrInvalidState)
		case event.GetTaskFailed() != nil:
			failedTasks[event.GetTaskFailed().GetTaskScheduledId()] = struct{}{}
		case event.GetSubOrchestrationInstanceFailed() != nil:
			failedSubOrchestrations[event.GetSubOrchestrationInstanceFailed().GetTaskScheduledId()] = struct{}{}
		case event.GetTaskScheduled() != nil:
			scheduledTasks[event.GetEventId()] = struct{}{}
		case event.GetSubOrchestrationInstanceCreated() != nil:
			createdSubOrchestrations[event.GetEventId()] = event.GetSubOrchestrationInstanceCreated().GetInstanceId()
		case event.GetExecutionStarted() != nil:
			if originalStart != nil {
				return nil, nil, nil, fmt.Errorf("%w: multiple execution started events", api.ErrInvalidState)
			}
			originalStart = event.GetExecutionStarted()
		}
	}
	if originalStart == nil {
		return nil, nil, nil, fmt.Errorf("%w: missing execution started event", api.ErrInvalidState)
	}
	for taskID := range failedTasks {
		if _, ok := scheduledTasks[taskID]; !ok {
			return nil, nil, nil, fmt.Errorf("%w: failed task %d has no scheduling event", api.ErrInvalidState, taskID)
		}
	}
	failedChildren := make(map[int32]string, len(failedSubOrchestrations))
	for taskID := range failedSubOrchestrations {
		childID, ok := createdSubOrchestrations[taskID]
		if !ok || childID == "" {
			return nil, nil, nil, fmt.Errorf("%w: failed sub-orchestration %d has no creation event", api.ErrInvalidState, taskID)
		}
		failedChildren[taskID] = childID
	}

	newStartEvent := helpers.NewExecutionStartedEvent(
		originalStart.GetName(),
		string(id),
		originalStart.GetInput(),
		originalStart.GetParentInstance(),
		originalStart.GetParentTraceContext(),
		nil,
		originalStart.GetVersion(),
	)
	newStart := newStartEvent.GetExecutionStarted()
	newStart.Tags = maps.Clone(originalStart.GetTags())

	rebuilt := make([]*protos.HistoryEvent, 0, len(history))
	for _, event := range history {
		switch {
		case event.GetTaskFailed() != nil:
			continue
		case event.GetTaskScheduled() != nil:
			if _, failed := failedTasks[event.GetEventId()]; failed {
				continue
			}
		case event.GetSubOrchestrationInstanceFailed() != nil:
			continue
		case event.GetExecutionCompleted() != nil &&
			event.GetExecutionCompleted().GetOrchestrationStatus() == api.RUNTIME_STATUS_FAILED:
			continue
		case event.GetExecutionStarted() != nil:
			replacement := proto.Clone(newStartEvent).(*protos.HistoryEvent)
			replacement.EventId = event.GetEventId()
			rebuilt = append(rebuilt, replacement)
			continue
		}
		rebuilt = append(rebuilt, proto.Clone(event).(*protos.HistoryEvent))
	}
	return rebuilt, newStart, failedChildren, nil
}

func NewExecutionRewoundEvent(id api.InstanceID, reason string, start *protos.ExecutionStartedEvent) *protos.HistoryEvent {
	rewound := &protos.ExecutionRewoundEvent{
		Reason:             wrapperspb.String(reason),
		ParentTraceContext: start.GetParentTraceContext(),
		Name:               wrapperspb.String(start.GetName()),
		Version:            start.GetVersion(),
		Input:              start.GetInput(),
		ParentInstance:     start.GetParentInstance(),
		Tags:               maps.Clone(start.GetTags()),
	}
	if parent := start.GetParentInstance(); parent != nil {
		rewound.InstanceId = wrapperspb.String(string(id))
		rewound.ParentExecutionId = parent.GetOrchestrationInstance().GetExecutionId()
	}
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionRewound{ExecutionRewound: rewound},
	}
}
