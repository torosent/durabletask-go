package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestTaskHubGrpcManagementOverBufconn(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), backend.DefaultLogger())
	executor, register := backend.NewGrpcExecutor(be, backend.DefaultLogger())
	register(grpcServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = executor.Shutdown(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	client := NewTaskHubGrpcClient(connection, backend.DefaultLogger())

	require.NoError(t, client.CreateTaskHub(ctx))
	query, err := client.QueryInstances(ctx, api.OrchestrationQuery{PageSize: 10})
	require.NoError(t, err)
	require.Empty(t, query.Orchestrations)
	ids, err := client.ListInstanceIDs(ctx, api.InstanceIDQuery{PageSize: 10})
	require.NoError(t, err)
	require.Empty(t, ids.InstanceIDs)
	require.NoError(t, client.DeleteTaskHub(ctx))
}
