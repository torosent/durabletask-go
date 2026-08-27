package durabletaskscheduler

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/microsoft/durabletask-go/backend"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type recordingCredential struct {
	mu      sync.Mutex
	options []policy.TokenRequestOptions
}

func (c *recordingCredential) GetToken(
	_ context.Context,
	options policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	c.mu.Lock()
	c.options = append(c.options, options)
	c.mu.Unlock()
	return azcore.AccessToken{Token: "token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type metadataServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	metadata chan metadata.MD
	helloErr error
}

func (s *metadataServer) Hello(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if s.helloErr != nil {
		return nil, s.helloErr
	}
	incoming, _ := metadata.FromIncomingContext(ctx)
	s.metadata <- incoming
	return &emptypb.Empty{}, nil
}

func (s *metadataServer) GetWorkItems(
	_ *protos.GetWorkItemsRequest,
	stream protos.TaskHubSidecarService_GetWorkItemsServer,
) error {
	incoming, _ := metadata.FromIncomingContext(stream.Context())
	s.metadata <- incoming
	<-stream.Context().Done()
	return nil
}

func startBufconnServer(t *testing.T, server protos.TaskHubSidecarServiceServer) (*bufconn.Listener, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	protos.RegisterTaskHubSidecarServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	return listener, func() {
		grpcServer.Stop()
		require.NoError(t, listener.Close())
	}
}

func bufconnDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

func TestSchedulerCredentialsUseExpectedScope(t *testing.T) {
	credential := &recordingCredential{}
	perRPC := &schedulerPerRPCCredentials{
		credential: credential,
		scope:      "https://durabletask.io/.default",
		taskHub:    "hub",
		userAgent:  "agent",
		workerID:   "worker",
	}

	values, err := perRPC.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "hub", values["taskhub"])
	require.Equal(t, "agent", values["x-user-agent"])
	require.Equal(t, "worker", values["workerid"])
	require.Equal(t, "Bearer token", values["authorization"])
	require.True(t, perRPC.RequireTransportSecurity())

	credential.mu.Lock()
	defer credential.mu.Unlock()
	require.Equal(t, []string{"https://durabletask.io/.default"}, credential.options[0].Scopes)
}

func TestConfiguredClientAndWorkerUseSeparateMetadataAndConnections(t *testing.T) {
	server := &metadataServer{metadata: make(chan metadata.MD, 4)}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.WorkerID = "worker-id"
	options.dialer = bufconnDialer(listener)

	managementClient, err := NewClient(context.Background(), options, backend.DefaultLogger())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, managementClient.Close())
	}()
	clientMetadata := <-server.metadata
	require.Equal(t, []string{"default"}, clientMetadata.Get("taskhub"))
	require.Empty(t, clientMetadata.Get("workerid"))
	require.Contains(t, clientMetadata.Get("x-user-agent")[0], "DurableTaskClient")

	worker, err := NewWorker(
		options,
		task.NewTaskRegistry(),
		backend.DefaultLogger(),
		durabletaskclient.WithWorkerSilentDisconnectTimeout(time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	workerHelloMetadata := <-server.metadata
	workerStreamMetadata := <-server.metadata
	for _, incoming := range []metadata.MD{workerHelloMetadata, workerStreamMetadata} {
		require.Equal(t, []string{"default"}, incoming.Get("taskhub"))
		require.Equal(t, []string{"worker-id"}, incoming.Get("workerid"))
		require.True(t, strings.Contains(incoming.Get("x-user-agent")[0], "DurableTaskWorker"))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, worker.Shutdown(shutdownCtx))
}

func TestNewClientFailsFastWhenHelloIsRejected(t *testing.T) {
	server := &metadataServer{
		metadata: make(chan metadata.MD, 1),
		helloErr: status.Error(codes.PermissionDenied, "forbidden"),
	}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.dialer = bufconnDialer(listener)

	managementClient, err := NewClient(context.Background(), options, backend.DefaultLogger())
	require.Nil(t, managementClient)
	require.ErrorContains(t, err, "Hello")
	require.ErrorContains(t, err, "PermissionDenied")
}
