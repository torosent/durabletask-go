package durabletaskscheduler

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type recordingCredential struct {
	mu      sync.Mutex
	options []policy.TokenRequestOptions
}

type failingCredential struct{}

func (failingCredential) GetToken(
	context.Context,
	policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, errors.New("temporary token failure")
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
	getErr   error
	state    *protos.OrchestrationState
}

func (s *metadataServer) GetInstance(
	ctx context.Context,
	_ *protos.GetInstanceRequest,
) (*protos.GetInstanceResponse, error) {
	incoming, _ := metadata.FromIncomingContext(ctx)
	s.metadata <- incoming
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: s.state}, nil
}

type recreationDataConverter struct{}

func (recreationDataConverter) Serialize(value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", errors.New("recreation converter only supports strings")
	}
	return "recreated:" + text, nil
}

func (recreationDataConverter) Deserialize(value string, target any) error {
	text, ok := target.(*string)
	if !ok {
		return errors.New("recreation converter target must be *string")
	}
	if !strings.HasPrefix(value, "recreated:") {
		return errors.New("recreation converter prefix is missing")
	}
	*text = strings.TrimPrefix(value, "recreated:")
	return nil
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

func recreationMetadataInterceptor(
	ctx context.Context,
	method string,
	request any,
	reply any,
	connection *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	options ...grpc.CallOption,
) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-recreation-interceptor", "preserved")
	return invoker(ctx, method, request, reply, connection, options...)
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

func TestSchedulerCredentialFailureIsTransient(t *testing.T) {
	perRPC := &schedulerPerRPCCredentials{
		credential: failingCredential{},
		scope:      "https://durabletask.io/.default",
		taskHub:    "hub",
		userAgent:  "agent",
	}
	_, err := perRPC.GetRequestMetadata(context.Background())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "temporary token failure")
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

func TestClientCloseStopsCompatibilityListener(t *testing.T) {
	server := &metadataServer{metadata: make(chan metadata.MD, 4)}
	listener, stop := startBufconnServer(t, server)
	defer stop()

	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.dialer = bufconnDialer(listener)
	managementClient, err := NewClient(context.Background(), options, backend.DefaultLogger())
	require.NoError(t, err)
	<-server.metadata
	require.NoError(t, managementClient.StartWorkItemListener(context.Background(), task.NewTaskRegistry()))
	<-server.metadata
	<-server.metadata
	require.NoError(t, managementClient.Close())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, managementClient.StopWorkItemListener(shutdownCtx))
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

func TestNewClientRecreatesChannelAndPreservesConfiguration(t *testing.T) {
	firstServer := &metadataServer{
		getErr:   status.Error(codes.Unavailable, "replace this channel"),
		metadata: make(chan metadata.MD, 16),
	}
	firstListener, stopFirst := startBufconnServer(t, firstServer)
	defer stopFirst()

	converter := recreationDataConverter{}
	store := payload.NewMemoryStore()
	largePayloads := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	serializedOutput, err := converter.Serialize("preserved")
	require.NoError(t, err)
	externalizedOutput, err := largepayload.Externalize(
		context.Background(),
		largePayloads,
		wrapperspb.String(serializedOutput),
	)
	require.NoError(t, err)
	secondServer := &metadataServer{
		state: &protos.OrchestrationState{
			InstanceId:           "recreated-instance",
			Name:                 "orchestrator",
			OrchestrationStatus:  protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
			Output:               externalizedOutput,
		},
		metadata: make(chan metadata.MD, 16),
	}
	secondListener, stopSecond := startBufconnServer(t, secondServer)
	defer stopSecond()

	var listenerMu sync.RWMutex
	activeListener := firstListener
	options, err := NewOptionsFromConnectionString(
		"Endpoint=http://bufconn;TaskHub=default;Authentication=None",
	)
	require.NoError(t, err)
	options.ChannelRecreateFailureThreshold = 1
	options.ChannelRecreateMinInterval = 0
	options.DataConverter = converter
	options.LargePayloads = largePayloads
	options.UnaryInterceptors = []grpc.UnaryClientInterceptor{recreationMetadataInterceptor}
	options.dialer = func(context.Context, string) (net.Conn, error) {
		listenerMu.RLock()
		listener := activeListener
		listenerMu.RUnlock()
		return listener.Dial()
	}

	client, err := NewClient(context.Background(), options, backend.DefaultLogger())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
	}()
	<-firstServer.metadata

	listenerMu.Lock()
	activeListener = secondListener
	listenerMu.Unlock()

	_, err = client.FetchOrchestrationMetadata(
		context.Background(),
		"recreated-instance",
		api.WithFetchPayloads(true),
	)
	require.Error(t, err)

	var orchestration *api.OrchestrationMetadata
	require.Eventually(t, func() bool {
		orchestration, err = client.FetchOrchestrationMetadata(
			context.Background(),
			"recreated-instance",
			api.WithFetchPayloads(true),
		)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	var output string
	require.NoError(t, orchestration.ReadOutput(&output))
	require.Equal(t, "preserved", output)

	replacementMetadata := <-secondServer.metadata
	require.Equal(t, []string{"preserved"}, replacementMetadata.Get("x-recreation-interceptor"))
	require.Equal(t, []string{"default"}, replacementMetadata.Get("taskhub"))
	require.Contains(t, replacementMetadata.Get("x-user-agent")[0], "DurableTaskClient")
}
