package client

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type largePayloadSidecarClient struct {
	protos.TaskHubSidecarServiceClient
	start     *protos.CreateInstanceRequest
	event     *protos.RaiseEventRequest
	terminate *protos.TerminateRequest
	state     *protos.OrchestrationState
}

func (c *largePayloadSidecarClient) StartInstance(
	_ context.Context,
	req *protos.CreateInstanceRequest,
	_ ...grpc.CallOption,
) (*protos.CreateInstanceResponse, error) {
	c.start = req
	return &protos.CreateInstanceResponse{InstanceId: req.InstanceId}, nil
}

func (c *largePayloadSidecarClient) RaiseEvent(
	_ context.Context,
	req *protos.RaiseEventRequest,
	_ ...grpc.CallOption,
) (*protos.RaiseEventResponse, error) {
	c.event = req
	return &protos.RaiseEventResponse{}, nil
}

func (c *largePayloadSidecarClient) TerminateInstance(
	_ context.Context,
	req *protos.TerminateRequest,
	_ ...grpc.CallOption,
) (*protos.TerminateResponse, error) {
	c.terminate = req
	return &protos.TerminateResponse{}, nil
}

func (c *largePayloadSidecarClient) GetInstance(
	context.Context,
	*protos.GetInstanceRequest,
	...grpc.CallOption,
) (*protos.GetInstanceResponse, error) {
	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: c.state}, nil
}

func TestTaskHubGrpcClientLargePayloadManagementFields(t *testing.T) {
	store := payload.NewMemoryStore()
	options := &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  1,
		MaxPayloadBytes: 1024,
	}
	fake := &largePayloadSidecarClient{}
	client := &TaskHubGrpcClient{
		client:        fake,
		logger:        backend.DefaultLogger(),
		largePayloads: options,
	}
	ctx := context.Background()
	id, err := client.ScheduleNewOrchestration(ctx, "orchestrator", api.WithRawInput("create-payload"))
	require.NoError(t, err)
	require.NotEmpty(t, id)
	requireLargePayloadValue(t, options, fake.start.Input, "create-payload")

	require.NoError(t, client.RaiseEvent(ctx, id, "event", api.WithRawEventData("event-payload")))
	requireLargePayloadValue(t, options, fake.event.Input, "event-payload")

	require.NoError(t, client.TerminateOrchestration(ctx, id, api.WithRawOutput("terminate-payload")))
	requireLargePayloadValue(t, options, fake.terminate.Output, "terminate-payload")

	input, err := largepayload.Externalize(ctx, options, wrapperspb.String("metadata-input"))
	require.NoError(t, err)
	output, err := largepayload.Externalize(ctx, options, wrapperspb.String("metadata-output"))
	require.NoError(t, err)
	status, err := largepayload.Externalize(ctx, options, wrapperspb.String("metadata-status"))
	require.NoError(t, err)
	fake.state = &protos.OrchestrationState{
		InstanceId:           string(id),
		Name:                 "orchestrator",
		CreatedTimestamp:     timestamppb.Now(),
		LastUpdatedTimestamp: timestamppb.Now(),
		Input:                input,
		Output:               output,
		CustomStatus:         status,
	}
	metadata, err := client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, "metadata-input", metadata.SerializedInput)
	require.Equal(t, "metadata-output", metadata.SerializedOutput)
	require.Equal(t, "metadata-status", metadata.SerializedCustomStatus)
}

func requireLargePayloadValue(
	t *testing.T,
	options *api.LargePayloadOptions,
	value *wrapperspb.StringValue,
	expected string,
) {
	t.Helper()
	require.NotEqual(t, expected, value.GetValue())
	hydrated, err := largepayload.Hydrate(context.Background(), options, value)
	require.NoError(t, err)
	require.Equal(t, expected, hydrated.GetValue())
}
