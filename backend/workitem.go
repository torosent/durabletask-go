package backend

import (
	"time"
)

// WorkItemAbandonDelayError is an optional marker a work-item processing error
// can implement to ask the worker to defer redelivery of that work item.
type WorkItemAbandonDelayError interface {
	error
	WorkItemAbandonDelay() time.Duration
}
