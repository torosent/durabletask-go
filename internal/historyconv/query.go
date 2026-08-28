package historyconv

import (
	"errors"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
)

// NormalizeStreamRequest validates the arguments shared by every
// StreamOrchestrationHistory implementation and applies query defaults.
func NormalizeStreamRequest(
	id api.InstanceID,
	query api.HistoryQuery,
	handler api.HistoryEventHandler,
) (api.HistoryQuery, error) {
	if id == api.EmptyInstanceID {
		return api.HistoryQuery{}, api.WrapInvalidArgument(errors.New("instance ID cannot be empty"))
	}
	if err := helpers.ValidateOrchestrationInstanceID(string(id)); err != nil {
		return api.HistoryQuery{}, api.WrapInvalidArgument(err)
	}
	if handler == nil {
		return api.HistoryQuery{}, api.WrapInvalidArgument(errors.New("history event handler is required"))
	}
	return api.NormalizeHistoryQuery(query)
}

// Collect buffers a bounded history snapshot from a streaming history read.
func Collect(
	id api.InstanceID,
	query api.HistoryQuery,
	stream func(api.HistoryEventHandler) error,
) (*api.OrchestrationHistory, error) {
	normalized, err := api.NormalizeHistoryQuery(query)
	if err != nil {
		return nil, err
	}
	result := &api.OrchestrationHistory{
		InstanceID:  id,
		ExecutionID: query.ExecutionID,
	}
	totalBytes := 0
	err = stream(func(event *api.HistoryEvent) error {
		if len(result.Events) >= normalized.MaxEvents {
			return fmt.Errorf("%w: limit %d", api.ErrHistoryLimitExceeded, normalized.MaxEvents)
		}
		if result.ExecutionID == "" && event.ExecutionStarted != nil {
			result.ExecutionID = event.ExecutionStarted.ExecutionID
		}
		totalBytes += ApproximateEventSize(event)
		if totalBytes > normalized.MaxBytes {
			return fmt.Errorf("%w: byte limit %d", api.ErrHistoryLimitExceeded, normalized.MaxBytes)
		}
		result.Events = append(result.Events, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ApproximateEventSize estimates retained history memory from payload strings,
// maps, and a fixed allowance for the event/detail structs.
func ApproximateEventSize(event *api.HistoryEvent) int {
	if event == nil {
		return 0
	}
	size := 512 + len(event.Type) + len(event.UnknownType)
	addMap := func(values map[string]string) {
		for key, value := range values {
			size += len(key) + len(value) + 32
		}
	}
	switch {
	case event.ExecutionStarted != nil:
		value := event.ExecutionStarted
		size += len(value.Name) + len(value.Version) + len(value.InstanceID) +
			len(value.ExecutionID) + len(value.SerializedInput) + len(value.OrchestrationSpanID)
		addMap(value.Tags)
		addMap(map[string]string(value.ContextFields))
	case event.ExecutionCompleted != nil:
		size += len(event.ExecutionCompleted.SerializedResult)
	case event.ExecutionTerminated != nil:
		size += len(event.ExecutionTerminated.SerializedInput)
	case event.TaskScheduled != nil:
		value := event.TaskScheduled
		size += len(value.Name) + len(value.Version) + len(value.SerializedInput)
		addMap(value.Tags)
		addMap(map[string]string(value.ContextFields))
	case event.TaskCompleted != nil:
		size += len(event.TaskCompleted.SerializedResult)
	case event.SubOrchestrationInstanceCreated != nil:
		value := event.SubOrchestrationInstanceCreated
		size += len(value.InstanceID) + len(value.Name) + len(value.Version) + len(value.SerializedInput)
		addMap(value.Tags)
		addMap(map[string]string(value.ContextFields))
	case event.SubOrchestrationInstanceCompleted != nil:
		size += len(event.SubOrchestrationInstanceCompleted.SerializedResult)
	case event.EventSent != nil:
		size += len(event.EventSent.InstanceID) + len(event.EventSent.Name) + len(event.EventSent.SerializedInput)
	case event.EventRaised != nil:
		size += len(event.EventRaised.Name) + len(event.EventRaised.SerializedInput)
	case event.Generic != nil:
		size += len(event.Generic.SerializedInput)
	case event.ContinueAsNew != nil:
		size += len(event.ContinueAsNew.SerializedInput)
	case event.ExecutionSuspended != nil:
		size += len(event.ExecutionSuspended.SerializedInput)
	case event.ExecutionResumed != nil:
		size += len(event.ExecutionResumed.SerializedInput)
	case event.Entity != nil:
		value := event.Entity
		size += len(value.RequestID) + len(value.Operation) + len(value.TargetInstanceID) +
			len(value.ParentInstanceID) + len(value.ParentExecutionID) + len(value.CriticalSectionID) +
			len(value.SerializedInput) + len(value.SerializedOutput)
		for _, lock := range value.LockSet {
			size += len(lock) + 16
		}
	case event.ExecutionRewound != nil:
		value := event.ExecutionRewound
		size += len(value.Reason) + len(value.Name) + len(value.Version) + len(value.InstanceID) +
			len(value.ParentExecutionID) + len(value.SerializedInput)
		addMap(value.Tags)
		addMap(map[string]string(value.ContextFields))
	}
	return size
}
