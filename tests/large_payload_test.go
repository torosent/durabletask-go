package tests

import (
	"context"
	"fmt"
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

func TestLargePayloadBackendRoundTripAcrossEntitySurfaces(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("payload", func(ctx *task.EntityContext) (any, error) {
		switch ctx.Operation {
		case "set":
			var value string
			if err := ctx.GetInput(&value); err != nil {
				return nil, err
			}
			return value, ctx.SetState(value)
		case "get":
			var value string
			if err := ctx.GetState(&value); err != nil {
				return nil, err
			}
			return value, nil
		default:
			return nil, fmt.Errorf("unknown operation %q", ctx.Operation)
		}
	}))
	entityID := api.NewEntityID("payload", "large")
	require.NoError(t, registry.AddOrchestratorN("LargeEntityRead", func(ctx *task.OrchestrationContext) (any, error) {
		var value string
		if err := ctx.CallEntity(entityID, "get").Await(&value); err != nil {
			return nil, err
		}
		return value, nil
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
	entityBackend := wrappedBackend.(backend.EntityBackend)
	executor := task.NewTaskExecutor(registry)
	worker := backend.NewTaskHubWorker(
		wrappedBackend,
		backend.NewOrchestrationWorker(wrappedBackend, executor, backend.DefaultLogger()),
		backend.NewActivityTaskWorker(wrappedBackend, executor, backend.DefaultLogger()),
		backend.DefaultLogger(),
		backend.NewEntityWorker(entityBackend, executor.(backend.EntityExecutor), backend.DefaultLogger()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
	})

	client := backend.NewTaskHubClient(wrappedBackend).(backend.EntityTaskHubClient)
	input := strings.Repeat("entity-payload-", 64)
	require.NoError(t, client.SignalEntity(ctx, entityID, "set", api.WithSignalInput(input)))
	require.Eventually(t, func() bool {
		metadata, err := client.FetchEntityMetadata(ctx, entityID, true)
		return err == nil && metadata != nil && metadata.SerializedState == `"`+input+`"`
	}, 10*time.Second, 50*time.Millisecond)

	rawEntityBackend := rawBackend.(backend.EntityQueryBackend)
	rawMetadata, err := rawEntityBackend.GetEntityMetadata(ctx, entityID, true)
	require.NoError(t, err)
	require.Contains(t, rawMetadata.SerializedState, "durabletask-payload:v1:")

	instanceID, err := client.ScheduleNewOrchestration(ctx, "LargeEntityRead")
	require.NoError(t, err)
	metadata, err := client.WaitForOrchestrationCompletion(ctx, instanceID)
	require.NoError(t, err)
	require.Equal(t, `"`+input+`"`, metadata.SerializedOutput)

	query, err := client.QueryEntities(ctx, api.EntityQuery{
		InstanceIDStartsWith: entityID.String(),
		IncludeState:         true,
	})
	require.NoError(t, err)
	require.Len(t, query.Entities, 1)
	require.Equal(t, `"`+input+`"`, query.Entities[0].SerializedState)
}
