package tests_grpc

import (
	"context"
	"fmt"
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

func Test_Bufconn_DurableEntityEndToEnd(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	server := grpc.NewServer()
	executor, register := backend.NewGrpcExecutor(be, logger)
	register(server)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	entityBackend, ok := backend.GetBackendCapability[backend.EntityBackend](be)
	require.True(t, ok)
	entityWorker := backend.NewEntityWorker(
		entityBackend,
		executor.(backend.EntityExecutor),
		logger,
	)
	hubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger, entityWorker)
	require.NoError(t, hubWorker.Start(testCtx))

	connection := serveBufconn(t, server, "bufconn")
	grpcClient := client.NewTaskHubGrpcClient(connection, logger, client.WithLegacyOrchestrationIDReusePolicyWire())

	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var value int
		if ctx.HasState() {
			if err := ctx.GetState(&value); err != nil {
				return nil, err
			}
		}
		if ctx.Operation != "add" {
			return nil, fmt.Errorf("unknown operation %q", ctx.Operation)
		}
		var amount int
		if err := ctx.GetInput(&amount); err != nil {
			return nil, err
		}
		value += amount
		return value, ctx.SetState(value)
	}))
	worker, err := client.NewTaskHubGrpcWorker(
		connection,
		registry,
		logger,
		client.WithMaxConcurrentEntityWorkItems(4),
		client.WithWorkItemFilters(&client.WorkItemFilters{Entities: []string{"counter"}}),
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

	entityID := api.NewEntityID("counter", "bufconn")
	require.NoError(t, grpcClient.SignalEntity(testCtx, entityID, "add", api.WithSignalInput(9)))
	require.Eventually(t, func() bool {
		metadata, err := grpcClient.FetchEntityMetadata(testCtx, entityID, true)
		return err == nil && metadata != nil && metadata.SerializedState == "9"
	}, 10*time.Second, 50*time.Millisecond)
}
