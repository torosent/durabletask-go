package client

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type historySchedulerClient struct {
	protos.TaskHubSidecarServiceClient
	request *protos.StreamInstanceHistoryRequest
	stream  protos.TaskHubSidecarService_StreamInstanceHistoryClient
	err     error
}

func (c *historySchedulerClient) StreamInstanceHistory(
	_ context.Context,
	request *protos.StreamInstanceHistoryRequest,
	_ ...grpc.CallOption,
) (protos.TaskHubSidecarService_StreamInstanceHistoryClient, error) {
	c.request = request
	return c.stream, c.err
}

type historyClientStream struct {
	protos.TaskHubSidecarService_StreamInstanceHistoryClient
	chunks []*protos.HistoryChunk
	err    error
	index  int
}

func (s *historyClientStream) Recv() (*protos.HistoryChunk, error) {
	if s.index < len(s.chunks) {
		chunk := s.chunks[s.index]
		s.index++
		return chunk, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	return nil, io.EOF
}

func TestTaskHubGrpcClientStreamsHistoryInOrder(t *testing.T) {
	scheduler := &historySchedulerClient{stream: &historyClientStream{
		chunks: []*protos.HistoryChunk{
			{Events: []*protos.HistoryEvent{historyGenericEvent(1, `"one"`)}},
			{},
			{Events: []*protos.HistoryEvent{historyGenericEvent(2, `"two"`)}},
		},
	}}
	client := &TaskHubGrpcClient{
		client:    scheduler,
		logger:    backend.DefaultLogger(),
		converter: api.DefaultDataConverter(),
	}
	var values []string
	err := client.StreamOrchestrationHistory(
		context.Background(),
		"instance",
		api.HistoryQuery{ExecutionID: "execution", MaxEvents: 1},
		func(event *api.HistoryEvent) error {
			var value string
			require.NoError(t, event.ReadData(&value))
			values = append(values, value)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"one", "two"}, values)
	require.Equal(t, "instance", scheduler.request.InstanceId)
	require.Equal(t, "execution", scheduler.request.ExecutionId.GetValue())
	require.False(t, scheduler.request.ForWorkItemProcessing)
}

func TestTaskHubGrpcClientHistoryLimitAndErrors(t *testing.T) {
	tests := []struct {
		name      string
		scheduler *historySchedulerClient
		query     api.HistoryQuery
		expected  error
	}{
		{
			name: "limit",
			scheduler: &historySchedulerClient{stream: &historyClientStream{chunks: []*protos.HistoryChunk{
				{Events: []*protos.HistoryEvent{historyGenericEvent(1, "one"), historyGenericEvent(2, "two")}},
			}}},
			query:    api.HistoryQuery{MaxEvents: 1},
			expected: api.ErrHistoryLimitExceeded,
		},
		{
			name: "byte limit",
			scheduler: &historySchedulerClient{stream: &historyClientStream{chunks: []*protos.HistoryChunk{
				{Events: []*protos.HistoryEvent{historyGenericEvent(1, "payload")}},
			}}},
			query:    api.HistoryQuery{MaxBytes: 1},
			expected: api.ErrHistoryLimitExceeded,
		},
		{
			name:      "not found",
			scheduler: &historySchedulerClient{err: status.Error(codes.NotFound, "missing")},
			expected:  api.ErrInstanceNotFound,
		},
		{
			name:      "unimplemented",
			scheduler: &historySchedulerClient{err: status.Error(codes.Unimplemented, "unsupported")},
			expected:  api.ErrFeatureNotSupported,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &TaskHubGrpcClient{
				client:    test.scheduler,
				logger:    backend.DefaultLogger(),
				converter: api.DefaultDataConverter(),
			}
			_, err := client.GetOrchestrationHistory(context.Background(), "instance", test.query)
			require.ErrorIs(t, err, test.expected)
		})
	}
}

func TestTaskHubGrpcClientHistoryValidationAndCallbackError(t *testing.T) {
	client := &TaskHubGrpcClient{
		client:    &historySchedulerClient{},
		logger:    backend.DefaultLogger(),
		converter: api.DefaultDataConverter(),
	}
	err := client.StreamOrchestrationHistory(context.Background(), "", api.HistoryQuery{}, func(*api.HistoryEvent) error {
		return nil
	})
	require.ErrorIs(t, err, api.ErrInvalidArgument)

	err = client.StreamOrchestrationHistory(context.Background(), "instance", api.HistoryQuery{}, nil)
	require.ErrorIs(t, err, api.ErrInvalidArgument)

	callbackErr := errors.New("stop")
	scheduler := &historySchedulerClient{stream: &historyClientStream{
		chunks: []*protos.HistoryChunk{{Events: []*protos.HistoryEvent{historyGenericEvent(1, "one")}}},
	}}
	client.client = scheduler
	err = client.StreamOrchestrationHistory(
		context.Background(),
		"instance",
		api.HistoryQuery{},
		func(*api.HistoryEvent) error { return callbackErr },
	)
	require.ErrorIs(t, err, callbackErr)
}

func TestTaskHubGrpcClientHistoryMapsCanceledReceive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scheduler := &historySchedulerClient{stream: &historyClientStream{
		err: status.Error(codes.Canceled, "canceled"),
	}}
	client := &TaskHubGrpcClient{
		client:    scheduler,
		logger:    backend.DefaultLogger(),
		converter: api.DefaultDataConverter(),
	}
	err := client.StreamOrchestrationHistory(ctx, "instance", api.HistoryQuery{}, func(*api.HistoryEvent) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func historyGenericEvent(id int32, value string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId: id,
		EventType: &protos.HistoryEvent_GenericEvent{
			GenericEvent: &protos.GenericEvent{Data: wrapperspb.String(value)},
		},
	}
}
