// Package backend holds the transport-neutral runtime plumbing shared by the
// Durable Task Scheduler (DTS) client, the DTS worker, and the in-process task
// executor.
//
// It is not an extension point. This SDK does not support hosting a task hub or
// plugging in a custom storage backend: DTS owns durable state, dispatch, and
// recovery. What remains here is only the small set of contracts those three
// pieces need to talk to each other — a logger, metric hooks, the executor
// interface, the history event alias, entity batch conversion, and the error
// sentinels used for gRPC status mapping.
package backend

import (
	"errors"

	"github.com/microsoft/durabletask-go/internal/protos"
)

// Task hub lifecycle error sentinels.
//
// These remain exported from this package for source compatibility with callers
// that already match on them. [github.com/microsoft/durabletask-go/client] maps
// the gRPC statuses returned by CreateTaskHub and DeleteTaskHub onto them so DTS
// lifecycle failures stay matchable with [errors.Is].
var (
	ErrTaskHubExists   = errors.New("task hub already exists")
	ErrTaskHubNotFound = errors.New("task hub not found")
)

// HistoryEvent is the wire history event exchanged with the scheduler.
type HistoryEvent = protos.HistoryEvent
