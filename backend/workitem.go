package backend

import (
	"errors"
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/api"
)

var ErrNoWorkItems = errors.New("no work items were found")

type WorkItem interface {
	fmt.Stringer
	IsWorkItem() bool
}

// WorkItemAbandonDelayError optionally asks local workers to defer redelivery
// after a processing error.
type WorkItemAbandonDelayError interface {
	error
	WorkItemAbandonDelay() time.Duration
}

type OrchestrationWorkItem struct {
	InstanceID api.InstanceID
	NewEvents  []*HistoryEvent
	LockedBy   string
	RetryCount int32
	EnqueuedAt time.Time
	State      *OrchestrationRuntimeState
	Properties map[string]any
}

// String implements core.WorkItem and fmt.Stringer
func (wi OrchestrationWorkItem) String() string {
	return fmt.Sprintf("%s (%d event(s))", wi.InstanceID, len(wi.NewEvents))
}

// IsWorkItem implements core.WorkItem
func (wi OrchestrationWorkItem) IsWorkItem() bool {
	return true
}

func (wi *OrchestrationWorkItem) GetAbandonDelay() time.Duration {
	switch {
	case wi.RetryCount == 0:
		return time.Duration(0) // no delay
	case wi.RetryCount > 100:
		return 5 * time.Minute // max delay
	default:
		return time.Duration(wi.RetryCount) * time.Second // linear backoff
	}
}

type ActivityWorkItem struct {
	SequenceNumber int64
	InstanceID     api.InstanceID
	NewEvent       *HistoryEvent
	Result         *HistoryEvent
	LockedBy       string
	RetryCount     int32
	EnqueuedAt     time.Time
	AbandonDelay   time.Duration
	Properties     map[string]any
}

// String implements core.WorkItem and fmt.Stringer
func (wi ActivityWorkItem) String() string {
	name := wi.NewEvent.GetTaskScheduled().GetName()
	taskID := wi.NewEvent.EventId
	return fmt.Sprintf("%s/%s#%d", wi.InstanceID, name, taskID)
}

// IsWorkItem implements core.WorkItem
func (wi ActivityWorkItem) IsWorkItem() bool {
	return true
}

func (wi *ActivityWorkItem) GetAbandonDelay() time.Duration {
	if wi == nil {
		return 0
	}
	return wi.AbandonDelay
}
