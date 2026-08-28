package backend

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
)

var ErrDuplicateEvent = errors.New("duplicate event")

type OrchestrationRuntimeState struct {
	instanceID            api.InstanceID
	newEvents             []*protos.HistoryEvent
	oldEvents             []*protos.HistoryEvent
	pendingTasks          []*protos.HistoryEvent
	pendingTimers         []*protos.HistoryEvent
	pendingMessages       []OrchestratorMessage
	pendingEntityMessages []EntityMessage

	startEvent      *protos.ExecutionStartedEvent
	completedEvent  *protos.ExecutionCompletedEvent
	createdTime     time.Time
	lastUpdatedTime time.Time
	completedTime   time.Time
	continuedAsNew  bool
	versionBoundary bool
	isSuspended     bool

	CustomStatus *wrapperspb.StringValue
}

type OrchestratorMessage struct {
	HistoryEvent     *HistoryEvent
	TargetInstanceID string
}

type EntityMessage struct {
	HistoryEvent     *HistoryEvent
	TargetInstanceID string
}

// OrchestrationRuntimeSnapshot is an immutable diagnostic view of runtime state.
type OrchestrationRuntimeSnapshot struct {
	InstanceID       api.InstanceID
	Name             string
	Version          string
	ParentInstanceID api.InstanceID
	ChildInstanceIDs []api.InstanceID
	RuntimeStatus    protos.OrchestrationStatus
	CreatedTime      time.Time
	LastUpdatedTime  time.Time
	CompletedTime    time.Time
	HistoryLength    int
	PendingTasks     int
	PendingTimers    int
	PendingMessages  int
	IsSuspended      bool
	ContinuedAsNew   bool
}

func NewOrchestrationRuntimeState(instanceID api.InstanceID, existingHistory []*HistoryEvent) *OrchestrationRuntimeState {
	s := &OrchestrationRuntimeState{
		instanceID: instanceID,
		oldEvents:  make([]*HistoryEvent, 0, len(existingHistory)),
		newEvents:  make([]*HistoryEvent, 0, 10),
	}

	for _, e := range existingHistory {
		_ = s.addEvent(e, false)
	}

	return s
}

// AddEvent appends a new history event to the orchestration history
func (s *OrchestrationRuntimeState) AddEvent(e *HistoryEvent) error {
	return s.addEvent(e, true)
}

func (s *OrchestrationRuntimeState) addEvent(e *HistoryEvent, isNew bool) error {
	if startEvent := e.GetExecutionStarted(); startEvent != nil {
		if s.startEvent != nil {
			return ErrDuplicateEvent
		}
		s.startEvent = startEvent
		s.createdTime = e.Timestamp.AsTime()
	} else if completedEvent := e.GetExecutionCompleted(); completedEvent != nil {
		if s.completedEvent != nil {
			return ErrDuplicateEvent
		}
		s.completedEvent = completedEvent
		s.completedTime = e.Timestamp.AsTime()
	} else if e.GetExecutionSuspended() != nil {
		s.isSuspended = true
	} else if e.GetExecutionResumed() != nil {
		s.isSuspended = false
	}

	if isNew {
		s.newEvents = append(s.newEvents, e)
	} else {
		s.oldEvents = append(s.oldEvents, e)
	}

	s.lastUpdatedTime = e.Timestamp.AsTime()
	return nil
}

func (s *OrchestrationRuntimeState) IsValid() bool {
	if len(s.oldEvents) == 0 && len(s.newEvents) == 0 {
		// empty orchestration state
		return true
	} else if s.startEvent != nil {
		// orchestration history has a start event
		return true
	}
	return false
}

// ApplyActions takes a set of actions and updates its internal state, including populating the outbox.
func (s *OrchestrationRuntimeState) ApplyActions(actions []*protos.OrchestratorAction, currentTraceContext *protos.TraceContext) (bool, error) {
	for _, action := range actions {
		if completedAction := action.GetCompleteOrchestration(); completedAction != nil {
			if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
				newState := NewOrchestrationRuntimeState(s.instanceID, []*protos.HistoryEvent{})
				newState.continuedAsNew = true
				if err := newState.AddEvent(helpers.NewOrchestratorStartedEvent()); err != nil {
					return false, fmt.Errorf("failed to add orchestrator started event: %w", err)
				}

				// Duplicate the start event info, updating just the input
				version := s.startEvent.Version
				if completedAction.NewVersion != nil {
					newVersion := completedAction.NewVersion.GetValue()
					version = completedAction.NewVersion
					newState.versionBoundary = !strings.EqualFold(
						s.startEvent.Version.GetValue(),
						newVersion,
					)
				}
				startEvent := helpers.NewExecutionStartedEvent(
					s.startEvent.Name,
					string(s.instanceID),
					completedAction.Result,
					s.startEvent.ParentInstance,
					s.startEvent.ParentTraceContext,
					nil,
					version,
				)
				startEvent.GetExecutionStarted().Tags = contextprop.Clone(s.startEvent.Tags)
				if err := newState.AddEvent(startEvent); err != nil {
					return false, fmt.Errorf("failed to add execution started event: %w", err)
				}

				// Unprocessed "carryover" events
				for _, e := range completedAction.CarryoverEvents {
					if err := newState.AddEvent(e); err != nil {
						return false, fmt.Errorf("failed to add carryover event: %w", err)
					}
				}
				// ContinueAsNew discards correlated work from the old execution.
				// Only fire-and-forget orchestration events and entity signals can
				// safely cross the execution boundary.
				for _, message := range s.pendingMessages {
					if message.HistoryEvent.GetEventRaised() != nil ||
						message.HistoryEvent.GetExecutionTerminated() != nil {
						newState.pendingMessages = append(newState.pendingMessages, message)
					}
				}
				for _, message := range s.pendingEntityMessages {
					if message.HistoryEvent.GetEntityOperationSignaled() != nil ||
						message.HistoryEvent.GetEntityUnlockSent() != nil {
						newState.pendingEntityMessages = append(newState.pendingEntityMessages, message)
					}
				}

				// Overwrite the current state object with a new one
				*s = *newState

				// ignore all remaining actions
				return true, nil
			} else {
				if err := s.AddEvent(helpers.NewExecutionCompletedEvent(action.Id, completedAction.OrchestrationStatus, completedAction.Result, completedAction.FailureDetails)); err != nil {
					return false, fmt.Errorf("failed to add execution completed event: %w", err)
				}
				if s.startEvent.GetParentInstance() != nil {
					msg := OrchestratorMessage{
						HistoryEvent:     &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now()},
						TargetInstanceID: s.startEvent.GetParentInstance().OrchestrationInstance.GetInstanceId(),
					}
					if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
						msg.HistoryEvent.EventType = &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
							SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
								TaskScheduledId: s.startEvent.ParentInstance.TaskScheduledId,
								Result:          completedAction.Result,
							},
						}
					} else {
						// TODO: What is the expected result for termination?
						msg.HistoryEvent.EventType = &protos.HistoryEvent_SubOrchestrationInstanceFailed{
							SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
								TaskScheduledId: s.startEvent.ParentInstance.TaskScheduledId,
								FailureDetails:  completedAction.FailureDetails,
							},
						}
					}
					s.pendingMessages = append(s.pendingMessages, msg)
				}
			}
		} else if createtimer := action.GetCreateTimer(); createtimer != nil {
			if err := s.AddEvent(helpers.NewTimerCreatedEvent(action.Id, createtimer.FireAt)); err != nil {
				return false, fmt.Errorf("failed to add timer created event: %w", err)
			}
			s.pendingTimers = append(s.pendingTimers, helpers.NewTimerFiredEvent(action.Id, createtimer.FireAt, currentTraceContext))
		} else if scheduleTask := action.GetScheduleTask(); scheduleTask != nil {
			scheduledEvent := helpers.NewTaskScheduledEvent(
				action.Id,
				scheduleTask.Name,
				scheduleTask.Version,
				scheduleTask.Input,
				currentTraceContext,
			)
			scheduledEvent.GetTaskScheduled().Tags = contextprop.Clone(scheduleTask.Tags)
			if err := s.AddEvent(scheduledEvent); err != nil {
				return false, fmt.Errorf("failed to add task scheduled event: %w", err)
			}
			s.pendingTasks = append(s.pendingTasks, scheduledEvent)
		} else if createSO := action.GetCreateSubOrchestration(); createSO != nil {
			// Autogenerate an instance ID for the sub-orchestration if none is provided, using a
			// deterministic algorithm based on the parent instance ID to help enable de-duplication.
			if createSO.InstanceId == "" {
				createSO.InstanceId = fmt.Sprintf("%s:%04x", s.instanceID, action.Id)
			}
			if err := s.AddEvent(helpers.NewSubOrchestrationCreatedEvent(
				action.Id,
				createSO.Name,
				createSO.Version,
				createSO.Input,
				createSO.InstanceId,
				currentTraceContext)); err != nil {
				return false, fmt.Errorf("failed to add sub-orchestration created event: %w", err)
			}
			startEvent := helpers.NewExecutionStartedEvent(
				createSO.Name,
				createSO.InstanceId,
				createSO.Input,
				helpers.NewParentInfo(action.Id, s.startEvent.Name, string(s.instanceID)),
				currentTraceContext,
				nil,
				createSO.Version,
			)
			_, fields := contextprop.Decode(createSO.Tags)
			startEvent.GetExecutionStarted().Tags = contextprop.Encode(
				api.OrchestrationContextInfo{
					InstanceID:       api.InstanceID(createSO.InstanceId),
					Name:             createSO.Name,
					Version:          createSO.Version.GetValue(),
					ParentInstanceID: s.instanceID,
				},
				fields,
				tagcodec.DecodeUserTagsOrPlain(createSO.Tags),
			)
			s.pendingMessages = append(s.pendingMessages, OrchestratorMessage{HistoryEvent: startEvent, TargetInstanceID: createSO.InstanceId})
		} else if sendEvent := action.GetSendEvent(); sendEvent != nil {
			sentEvent := helpers.NewSendEventEvent(action.Id, sendEvent.Instance.InstanceId, sendEvent.Name, sendEvent.Data)
			if err := s.AddEvent(sentEvent); err != nil {
				return false, fmt.Errorf("failed to add send event: %w", err)
			}
			s.pendingMessages = append(s.pendingMessages, OrchestratorMessage{
				HistoryEvent:     helpers.NewEventRaisedEvent(sendEvent.Name, sendEvent.Data),
				TargetInstanceID: sendEvent.Instance.InstanceId,
			})
		} else if sendEntityMessage := action.GetSendEntityMessage(); sendEntityMessage != nil {
			historyEvent, message, err := s.buildEntityMessage(action.Id, sendEntityMessage)
			if err != nil {
				return false, err
			}
			if err := s.AddEvent(historyEvent); err != nil {
				return false, fmt.Errorf("failed to add entity message history: %w", err)
			}
			s.pendingEntityMessages = append(s.pendingEntityMessages, message)
		} else if terminate := action.GetTerminateOrchestration(); terminate != nil {
			// Send a message to terminate the target orchestration
			msg := OrchestratorMessage{
				TargetInstanceID: terminate.InstanceId,
				HistoryEvent:     helpers.NewExecutionTerminatedEvent(terminate.Reason, terminate.Recurse),
			}
			s.pendingMessages = append(s.pendingMessages, msg)
		} else {
			return false, fmt.Errorf("unknown action type: %v", action)
		}
	}

	return false, nil
}

func (s *OrchestrationRuntimeState) InstanceID() api.InstanceID {
	return s.instanceID
}

// Snapshot returns an immutable diagnostic view of the current runtime state.
func (s *OrchestrationRuntimeState) Snapshot() OrchestrationRuntimeSnapshot {
	snapshot := OrchestrationRuntimeSnapshot{
		InstanceID:       s.instanceID,
		ChildInstanceIDs: s.ChildInstanceIDs(),
		RuntimeStatus:    s.RuntimeStatus(),
		CreatedTime:      s.createdTime,
		LastUpdatedTime:  s.lastUpdatedTime,
		CompletedTime:    s.completedTime,
		HistoryLength:    s.HistoryLength(),
		PendingTasks:     len(s.pendingTasks),
		PendingTimers:    len(s.pendingTimers),
		PendingMessages:  len(s.pendingMessages),
		IsSuspended:      s.isSuspended,
		ContinuedAsNew:   s.continuedAsNew,
	}
	if s.startEvent != nil {
		snapshot.Name = s.startEvent.GetName()
		snapshot.Version = s.startEvent.GetVersion().GetValue()
		if parentInstanceID, ok := s.ParentInstanceID(); ok {
			snapshot.ParentInstanceID = parentInstanceID
		}
	}
	return snapshot
}

// ParentInstanceID returns the parent orchestration instance ID, when present.
func (s *OrchestrationRuntimeState) ParentInstanceID() (api.InstanceID, bool) {
	if s.startEvent == nil {
		return "", false
	}
	parent := s.startEvent.GetParentInstance()
	if parent == nil {
		return "", false
	}
	instanceID := api.InstanceID(parent.GetOrchestrationInstance().GetInstanceId())
	return instanceID, instanceID != ""
}

// ChildInstanceIDs returns a sorted copy of child orchestration instance IDs.
func (s *OrchestrationRuntimeState) ChildInstanceIDs() []api.InstanceID {
	return getSubOrchestrationInstances(s.oldEvents, s.newEvents)
}

// HistoryLength returns the total persisted and pending history event count.
func (s *OrchestrationRuntimeState) HistoryLength() int {
	return len(s.oldEvents) + len(s.newEvents)
}

func (s *OrchestrationRuntimeState) Name() (string, error) {
	if s.startEvent == nil {
		return "", api.ErrNotStarted
	}

	return s.startEvent.Name, nil
}

func (s *OrchestrationRuntimeState) Input() (string, error) {
	if s.startEvent == nil {
		return "", api.ErrNotStarted
	}

	// REVIEW: Should we distinguish between no input and the empty string?
	return s.startEvent.Input.GetValue(), nil
}

func (s *OrchestrationRuntimeState) Output() (string, error) {
	if s.completedEvent == nil {
		return "", api.ErrNotCompleted
	}

	// REVIEW: Should we distinguish between no output and the empty string?
	return s.completedEvent.Result.GetValue(), nil
}

func (s *OrchestrationRuntimeState) RuntimeStatus() protos.OrchestrationStatus {
	switch {
	case s.startEvent == nil:
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	// Completion wins over suspension: terminating a suspended orchestration
	// must report the terminal status instead of leaving it stuck as SUSPENDED.
	case s.completedEvent != nil:
		return s.completedEvent.GetOrchestrationStatus()
	case s.isSuspended:
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED
	}

	return protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
}

func (s *OrchestrationRuntimeState) CreatedTime() (time.Time, error) {
	if s.startEvent == nil {
		return time.Time{}, api.ErrNotStarted
	}

	return s.createdTime, nil
}

func (s *OrchestrationRuntimeState) LastUpdatedTime() (time.Time, error) {
	if s.startEvent == nil {
		return time.Time{}, api.ErrNotStarted
	}

	return s.lastUpdatedTime, nil
}

func (s *OrchestrationRuntimeState) CompletedTime() (time.Time, error) {
	if s.completedEvent == nil {
		return time.Time{}, api.ErrNotCompleted
	}

	return s.completedTime, nil
}

func (s *OrchestrationRuntimeState) IsCompleted() bool {
	return s.completedEvent != nil
}

func (s *OrchestrationRuntimeState) OldEvents() []*HistoryEvent {
	return s.oldEvents
}

func (s *OrchestrationRuntimeState) NewEvents() []*HistoryEvent {
	return s.newEvents
}

func (s *OrchestrationRuntimeState) FailureDetails() (*protos.TaskFailureDetails, error) {
	if s.completedEvent == nil {
		return nil, api.ErrNotCompleted
	} else if s.completedEvent.FailureDetails == nil {
		return nil, api.ErrNoFailures
	}

	return s.completedEvent.FailureDetails, nil
}

func (s *OrchestrationRuntimeState) PendingTimers() []*HistoryEvent {
	return s.pendingTimers
}

func (s *OrchestrationRuntimeState) PendingTasks() []*HistoryEvent {
	return s.pendingTasks
}

func (s *OrchestrationRuntimeState) PendingMessages() []OrchestratorMessage {
	return s.pendingMessages
}

func (s *OrchestrationRuntimeState) PendingEntityMessages() []EntityMessage {
	return s.pendingEntityMessages
}

func (s *OrchestrationRuntimeState) ContinuedAsNew() bool {
	return s.continuedAsNew
}

// ContinueAsNewVersionChanged reports whether ContinueAsNew selected a new
// version that must be dispatched to a compatible worker.
func (s *OrchestrationRuntimeState) ContinueAsNewVersionChanged() bool {
	return s.versionBoundary
}

func (s *OrchestrationRuntimeState) String() string {
	return fmt.Sprintf("%v:%v", s.instanceID, helpers.ToRuntimeStatusString(s.RuntimeStatus()))
}

func (s *OrchestrationRuntimeState) buildEntityMessage(
	eventID int32,
	action *protos.SendEntityMessageAction,
) (*HistoryEvent, EntityMessage, error) {
	timestamp := timestamppb.Now()
	message := EntityMessage{HistoryEvent: &protos.HistoryEvent{EventId: -1, Timestamp: timestamp}}
	history := &protos.HistoryEvent{EventId: eventID, Timestamp: timestamp}
	parentInstanceID := wrapperspb.String(string(s.instanceID))
	var parentExecutionID *wrapperspb.StringValue
	if s.startEvent != nil && s.startEvent.OrchestrationInstance != nil {
		parentExecutionID = s.startEvent.OrchestrationInstance.ExecutionId
	}

	switch {
	case action.GetEntityOperationSignaled() != nil:
		historyValue := proto.Clone(action.GetEntityOperationSignaled()).(*protos.EntityOperationSignaledEvent)
		message.TargetInstanceID = historyValue.TargetInstanceId.GetValue()
		messageValue := proto.Clone(historyValue).(*protos.EntityOperationSignaledEvent)
		messageValue.TargetInstanceId = nil
		history.EventType = &protos.HistoryEvent_EntityOperationSignaled{EntityOperationSignaled: historyValue}
		message.HistoryEvent.EventType = &protos.HistoryEvent_EntityOperationSignaled{EntityOperationSignaled: messageValue}
	case action.GetEntityOperationCalled() != nil:
		historyValue := proto.Clone(action.GetEntityOperationCalled()).(*protos.EntityOperationCalledEvent)
		message.TargetInstanceID = historyValue.TargetInstanceId.GetValue()
		historyValue.ParentInstanceId = nil
		historyValue.ParentExecutionId = nil
		messageValue := proto.Clone(historyValue).(*protos.EntityOperationCalledEvent)
		messageValue.TargetInstanceId = nil
		messageValue.ParentInstanceId = parentInstanceID
		messageValue.ParentExecutionId = parentExecutionID
		history.EventType = &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: historyValue}
		message.HistoryEvent.EventType = &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: messageValue}
	case action.GetEntityLockRequested() != nil:
		historyValue := proto.Clone(action.GetEntityLockRequested()).(*protos.EntityLockRequestedEvent)
		if historyValue.Position < 0 || int(historyValue.Position) >= len(historyValue.LockSet) {
			return nil, EntityMessage{}, fmt.Errorf("invalid entity lock position %d", historyValue.Position)
		}
		message.TargetInstanceID = historyValue.LockSet[historyValue.Position]
		historyValue.ParentInstanceId = nil
		messageValue := proto.Clone(historyValue).(*protos.EntityLockRequestedEvent)
		messageValue.ParentInstanceId = parentInstanceID
		history.EventType = &protos.HistoryEvent_EntityLockRequested{EntityLockRequested: historyValue}
		message.HistoryEvent.EventType = &protos.HistoryEvent_EntityLockRequested{EntityLockRequested: messageValue}
	case action.GetEntityUnlockSent() != nil:
		historyValue := proto.Clone(action.GetEntityUnlockSent()).(*protos.EntityUnlockSentEvent)
		message.TargetInstanceID = historyValue.TargetInstanceId.GetValue()
		historyValue.ParentInstanceId = nil
		messageValue := proto.Clone(historyValue).(*protos.EntityUnlockSentEvent)
		messageValue.TargetInstanceId = nil
		messageValue.ParentInstanceId = parentInstanceID
		history.EventType = &protos.HistoryEvent_EntityUnlockSent{EntityUnlockSent: historyValue}
		message.HistoryEvent.EventType = &protos.HistoryEvent_EntityUnlockSent{EntityUnlockSent: messageValue}
	default:
		return nil, EntityMessage{}, fmt.Errorf("unknown entity message action: %v", action)
	}
	if message.TargetInstanceID == "" {
		return nil, EntityMessage{}, fmt.Errorf("entity message action is missing a target instance ID")
	}
	return history, message, nil
}

func (s *OrchestrationRuntimeState) getStartedTime() time.Time {
	var startTime time.Time
	if len(s.oldEvents) > 0 {
		startTime = s.oldEvents[0].Timestamp.AsTime()
	} else if len(s.newEvents) > 0 {
		startTime = s.newEvents[0].Timestamp.AsTime()
	}
	return startTime
}
