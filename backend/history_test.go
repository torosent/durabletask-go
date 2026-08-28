package backend

import (
	"context"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type orchestrationHistoryBackend struct {
	Backend
	metadata *api.OrchestrationMetadata
	state    *OrchestrationRuntimeState
}

func (b *orchestrationHistoryBackend) GetOrchestrationMetadata(
	context.Context,
	api.InstanceID,
) (*api.OrchestrationMetadata, error) {
	if b.metadata == nil {
		return nil, api.ErrInstanceNotFound
	}
	return b.metadata, nil
}

func (b *orchestrationHistoryBackend) GetOrchestrationRuntimeState(
	context.Context,
	*OrchestrationWorkItem,
) (*OrchestrationRuntimeState, error) {
	return b.state, nil
}

func TestEmbeddedClientGetsAndStreamsHistory(t *testing.T) {
	start := helpers.NewExecutionStartedEvent("orchestrator", "instance", nil, nil, nil, nil)
	completed := helpers.NewExecutionCompletedEvent(1, api.RUNTIME_STATUS_COMPLETED, nil, nil)
	be := &orchestrationHistoryBackend{
		metadata: &api.OrchestrationMetadata{InstanceID: "instance", ExecutionID: "execution"},
		state:    NewOrchestrationRuntimeState("instance", []*HistoryEvent{start, completed}),
	}
	client := NewTaskHubManagementClient(be)
	history, err := client.GetOrchestrationHistory(
		context.Background(),
		"instance",
		api.HistoryQuery{ExecutionID: "execution"},
	)
	require.NoError(t, err)
	require.Len(t, history.Events, 2)
	require.Equal(t, api.HistoryEventExecutionStarted, history.Events[0].Type)
	require.Equal(t, api.HistoryEventExecutionCompleted, history.Events[1].Type)

	var count int
	require.NoError(t, client.StreamOrchestrationHistory(
		context.Background(),
		"instance",
		api.HistoryQuery{},
		func(*api.HistoryEvent) error {
			count++
			return nil
		},
	))
	require.Equal(t, 2, count)
}

func TestEmbeddedClientHistoryValidation(t *testing.T) {
	client := NewTaskHubManagementClient(&orchestrationHistoryBackend{
		metadata: &api.OrchestrationMetadata{ExecutionID: "current"},
		state:    NewOrchestrationRuntimeState("instance", nil),
	})
	_, err := client.GetOrchestrationHistory(
		context.Background(),
		"instance",
		api.HistoryQuery{ExecutionID: "other"},
	)
	require.ErrorIs(t, err, api.ErrInstanceNotFound)

	err = client.StreamOrchestrationHistory(
		context.Background(),
		"instance",
		api.HistoryQuery{},
		nil,
	)
	require.ErrorIs(t, err, api.ErrInvalidArgument)
}

type historyServerStream struct {
	protos.TaskHubSidecarService_StreamInstanceHistoryServer
	ctx    context.Context
	chunks []*protos.HistoryChunk
}

func (s *historyServerStream) Context() context.Context {
	return s.ctx
}

func (s *historyServerStream) Send(chunk *protos.HistoryChunk) error {
	s.chunks = append(s.chunks, chunk)
	return nil
}

func (s *historyServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *historyServerStream) SendHeader(metadata.MD) error { return nil }
func (s *historyServerStream) SetTrailer(metadata.MD)       {}
func (s *historyServerStream) SendMsg(any) error            { return nil }
func (s *historyServerStream) RecvMsg(any) error            { return nil }

func TestGrpcExecutorStreamsHistoryAndChecksExecution(t *testing.T) {
	events := make([]*HistoryEvent, 0, historyChunkSize+1)
	for range historyChunkSize + 1 {
		events = append(events, helpers.NewOrchestratorStartedEvent())
	}
	be := &orchestrationHistoryBackend{
		metadata: &api.OrchestrationMetadata{ExecutionID: "execution"},
		state:    NewOrchestrationRuntimeState("instance", events),
	}
	executor, _ := NewGrpcExecutor(be, DefaultLogger())
	stream := &historyServerStream{ctx: context.Background()}
	err := executor.(*grpcExecutor).StreamInstanceHistory(
		&protos.StreamInstanceHistoryRequest{
			InstanceId:  "instance",
			ExecutionId: wrapperspb.String("execution"),
		},
		stream,
	)
	require.NoError(t, err)
	require.Len(t, stream.chunks, 2)
	require.Len(t, stream.chunks[0].Events, historyChunkSize)
	require.Len(t, stream.chunks[1].Events, 1)

	err = executor.(*grpcExecutor).StreamInstanceHistory(
		&protos.StreamInstanceHistoryRequest{
			InstanceId:  "instance",
			ExecutionId: wrapperspb.String("other"),
		},
		&historyServerStream{ctx: context.Background()},
	)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestGrpcExecutorChunksHistoryBySerializedSize(t *testing.T) {
	payload := strings.Repeat("x", historyChunkByteLimit/2+1)
	events := []*HistoryEvent{
		{
			EventType: &protos.HistoryEvent_GenericEvent{
				GenericEvent: &protos.GenericEvent{Data: wrapperspb.String(payload)},
			},
		},
		{
			EventType: &protos.HistoryEvent_GenericEvent{
				GenericEvent: &protos.GenericEvent{Data: wrapperspb.String(payload)},
			},
		},
	}
	be := &orchestrationHistoryBackend{
		metadata: &api.OrchestrationMetadata{ExecutionID: "execution"},
		state:    NewOrchestrationRuntimeState("instance", events),
	}
	executor, _ := NewGrpcExecutor(be, DefaultLogger())
	stream := &historyServerStream{ctx: context.Background()}
	require.NoError(t, executor.(*grpcExecutor).StreamInstanceHistory(
		&protos.StreamInstanceHistoryRequest{InstanceId: "instance"},
		stream,
	))
	require.Len(t, stream.chunks, 2)
	require.Len(t, stream.chunks[0].Events, 1)
	require.Len(t, stream.chunks[1].Events, 1)
}
