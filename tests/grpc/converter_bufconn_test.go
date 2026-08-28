package tests_grpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type grpcGobConverter struct{}

func (grpcGobConverter) Serialize(value any) (string, error) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(value); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(buffer.Bytes()), nil
}

func (grpcGobConverter) Deserialize(payload string, target any) error {
	data, err := base64.RawStdEncoding.DecodeString(payload)
	if err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewReader(data)).Decode(target)
}

type grpcConverterPayload struct {
	Value int
}

func Test_Bufconn_CustomConverterAndVersioningEndToEnd(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	server := grpc.NewServer()
	executor, register := backend.NewGrpcExecutor(be, logger)
	register(server)
	entityBackend, ok := backend.GetBackendCapability[backend.EntityBackend](be)
	require.True(t, ok)
	hubWorker := backend.NewTaskHubWorker(
		be,
		backend.NewOrchestrationWorker(be, executor, logger),
		backend.NewActivityTaskWorker(be, executor, logger),
		logger,
		backend.NewEntityWorker(entityBackend, executor.(backend.EntityExecutor), logger),
	)
	require.NoError(t, hubWorker.Start(testCtx))

	connection := serveBufconn(t, server, "converter-bufconn")

	converter := grpcGobConverter{}
	grpcClient := client.NewTaskHubGrpcClient(
		connection,
		logger,
		client.WithLegacyOrchestrationIDReusePolicyWire(),
		client.WithDataConverter(converter),
		client.WithDefaultVersion("v1"),
	)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityNVersion("increment", "v1", func(ctx task.ActivityContext) (any, error) {
		var input grpcConverterPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		input.Value++
		return input, nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("workflow", "v1", func(ctx *task.OrchestrationContext) (any, error) {
		var input grpcConverterPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var result grpcConverterPayload
		if err := ctx.CallActivity("increment", task.WithActivityInput(input)).Await(&result); err != nil {
			return nil, err
		}
		ctx.ContinueAsNew(result, task.WithContinueAsNewVersion("v2"))
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("workflow", "v2", func(ctx *task.OrchestrationContext) (any, error) {
		var input grpcConverterPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		input.Value++
		return input, nil
	}))
	require.NoError(t, registry.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var state grpcConverterPayload
		if ctx.HasState() {
			if err := ctx.GetState(&state); err != nil {
				return nil, err
			}
		}
		var input grpcConverterPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		state.Value += input.Value
		return state, ctx.SetState(state)
	}))
	worker, err := client.NewTaskHubGrpcWorker(
		connection,
		registry,
		logger,
		client.WithWorkerDataConverter(converter),
		client.WithTaskVersioning(task.VersioningOptions{DefaultVersion: "v1"}),
		client.WithAutoWorkItemFilters(),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(testCtx))

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, hubWorker.Shutdown(shutdownCtx))
		require.NoError(t, connection.Close())
		server.Stop()
	})

	instanceID, err := grpcClient.ScheduleNewOrchestration(
		testCtx,
		"workflow",
		api.WithInput(grpcConverterPayload{Value: 1}),
	)
	require.NoError(t, err)
	metadata, err := grpcClient.WaitForOrchestrationCompletion(testCtx, instanceID)
	require.NoError(t, err)
	require.Equal(t, "v2", metadata.Version)
	var output grpcConverterPayload
	require.NoError(t, metadata.ReadOutput(&output))
	require.Equal(t, 3, output.Value)

	entityID := api.NewEntityID("counter", "converter")
	require.NoError(t, grpcClient.SignalEntity(
		testCtx,
		entityID,
		"add",
		api.WithSignalInput(grpcConverterPayload{Value: 5}),
	))
	require.Eventually(t, func() bool {
		entity, fetchErr := grpcClient.FetchEntityMetadata(testCtx, entityID, true)
		if fetchErr != nil || entity == nil {
			return false
		}
		var state grpcConverterPayload
		return entity.ReadState(&state) == nil && state.Value == 5
	}, 10*time.Second, 50*time.Millisecond)
}
