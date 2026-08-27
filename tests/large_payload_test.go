package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

func TestLargePayloadBackendRoundTripAcrossOrchestrationSurfaces(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("LargePayloadEcho", func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + "-activity", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("LargePayloadChild", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + "-child", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("LargePayloadParent", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		ctx.SetCustomStatus(input)
		var activityOutput string
		if err := ctx.CallActivity("LargePayloadEcho", task.WithActivityInput(input)).Await(&activityOutput); err != nil {
			return nil, err
		}
		var childOutput string
		if err := ctx.CallSubOrchestrator(
			"LargePayloadChild",
			task.WithSubOrchestratorInput(activityOutput),
		).Await(&childOutput); err != nil {
			return nil, err
		}
		return childOutput, nil
	}))

	store := payload.NewMemoryStore()
	payloadOptions := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  8,
		MaxPayloadBytes: 1024 * 1024,
	}
	rawBackend := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), backend.DefaultLogger())
	wrappedBackend, err := backend.NewLargePayloadBackend(rawBackend, payloadOptions)
	require.NoError(t, err)
	executor := task.NewTaskExecutor(registry)
	worker := backend.NewTaskHubWorker(
		wrappedBackend,
		backend.NewOrchestrationWorker(wrappedBackend, executor, backend.DefaultLogger()),
		backend.NewActivityTaskWorker(wrappedBackend, executor, backend.DefaultLogger()),
		backend.DefaultLogger(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
	})

	client := backend.NewTaskHubManagementClient(wrappedBackend)
	input := strings.Repeat("payload-", 64)
	id, err := client.ScheduleNewOrchestration(
		ctx,
		"LargePayloadParent",
		api.WithInstanceID("large-payload-parent"),
		api.WithInput(input),
		api.WithTags(map[string]string{"team": "durable"}),
	)
	require.NoError(t, err)
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, `"`+input+`-activity-child"`, metadata.SerializedOutput)
	require.Equal(t, "durable", metadata.Tags["team"])

	rawMetadata, err := rawBackend.GetOrchestrationMetadata(ctx, id)
	require.NoError(t, err)
	require.Contains(t, rawMetadata.SerializedInput, "durabletask-payload:v1:")
	require.Contains(t, rawMetadata.SerializedOutput, "durabletask-payload:v1:")
	require.Contains(t, rawMetadata.SerializedCustomStatus, "durabletask-payload:v1:")

	query, err := client.QueryInstances(ctx, api.OrchestrationQuery{
		InstanceIDPrefix:      "large-payload-parent",
		FetchInputsAndOutputs: true,
		Tags:                  map[string]string{"team": "durable"},
	})
	require.NoError(t, err)
	require.Len(t, query.Orchestrations, 2)
	for _, item := range query.Orchestrations {
		require.Equal(t, "durable", item.Tags["team"])
		require.NotContains(t, item.SerializedInput, "durabletask-payload:v1:")
	}
}
