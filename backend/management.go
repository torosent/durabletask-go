package backend

import (
	"context"

	"github.com/microsoft/durabletask-go/api"
)

// BackendUnwrapper exposes an underlying backend for optional capability discovery.
type BackendUnwrapper interface {
	UnwrapBackend() Backend
}

// GetBackendCapability finds an optional capability through transparent backend decorators.
func GetBackendCapability[T any](be Backend) (T, bool) {
	var zero T
	for depth := 0; be != nil && depth < 32; depth++ {
		if capability, ok := any(be).(T); ok {
			return capability, true
		}
		unwrapper, ok := be.(BackendUnwrapper)
		if !ok {
			return zero, false
		}
		be = unwrapper.UnwrapBackend()
	}
	return zero, false
}

type orchestrationQueryResultDecorator interface {
	decorateOrchestrationQueryResult(context.Context, api.OrchestrationQuery, *api.OrchestrationQueryResult) error
}

func queryOrchestrations(
	ctx context.Context,
	be Backend,
	query api.OrchestrationQuery,
) (*api.OrchestrationQueryResult, error) {
	capability, ok := GetBackendCapability[OrchestrationQueryBackend](be)
	if !ok {
		return nil, api.ErrFeatureNotSupported
	}
	result, err := capability.QueryOrchestrations(ctx, query)
	if err != nil || result == nil {
		return result, err
	}
	for depth, current := 0, be; current != nil && depth < 32; depth++ {
		if decorator, ok := current.(orchestrationQueryResultDecorator); ok {
			if err := decorator.decorateOrchestrationQueryResult(ctx, query, result); err != nil {
				return nil, err
			}
		}
		unwrapper, ok := current.(BackendUnwrapper)
		if !ok {
			break
		}
		current = unwrapper.UnwrapBackend()
	}
	return result, nil
}

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
