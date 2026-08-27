package sqlite

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/stretchr/testify/require"
)

func TestStopStartPreservesInMemoryTaskHub(t *testing.T) {
	ctx := context.Background()
	be := NewSqliteBackend(NewSqliteOptions(""), backend.DefaultLogger())
	require.NoError(t, be.CreateTaskHub(ctx))
	require.NoError(t, be.CreateOrchestrationInstance(
		ctx,
		helpers.NewExecutionStartedEvent("orchestration", "instance", nil, nil, nil, nil),
	))
	require.NoError(t, be.Stop(ctx))
	require.NoError(t, be.Start(ctx))
	metadata, err := be.GetOrchestrationMetadata(ctx, api.InstanceID("instance"))
	require.NoError(t, err)
	require.Equal(t, api.InstanceID("instance"), metadata.InstanceID)
}
