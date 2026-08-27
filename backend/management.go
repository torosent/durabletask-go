package backend

import (
	"context"

	"github.com/microsoft/durabletask-go/api"
)

// OrchestrationQueryBackend is an optional backend capability for bounded instance queries.
type OrchestrationQueryBackend interface {
	QueryOrchestrations(context.Context, api.OrchestrationQuery) (*api.OrchestrationQueryResult, error)
}

// InstanceIDQueryBackend is an optional backend capability for bounded instance ID queries.
type InstanceIDQueryBackend interface {
	ListInstanceIDs(context.Context, api.InstanceIDQuery) (*api.InstanceIDQueryResult, error)
}

// RestartInstanceBackend is an optional backend capability for restarting completed instances.
type RestartInstanceBackend interface {
	RestartInstance(context.Context, api.InstanceID, bool) (api.InstanceID, error)
}

// RewindInstanceBackend is an optional backend capability for rewinding failed instances.
type RewindInstanceBackend interface {
	RewindInstance(context.Context, api.InstanceID, string) error
}

// PurgeInstancesBackend is an optional backend capability for bounded multi-instance purge operations.
type PurgeInstancesBackend interface {
	PurgeInstances(context.Context, api.PurgeInstancesRequest) (*api.PurgeInstancesResult, error)
}

// SkipGracefulTerminationsBackend is an optional backend capability for immediate storage-level termination.
type SkipGracefulTerminationsBackend interface {
	SkipGracefulOrchestrationTerminations(context.Context, []api.InstanceID, string) ([]api.InstanceID, error)
}
