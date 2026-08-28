package client

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func TestGrpcClientDefaultVersionAndExplicitUnversionedOverride(t *testing.T) {
	scheduler := new(largePayloadSchedulerClient)
	client := &TaskHubGrpcClient{
		client:         scheduler,
		defaultVersion: "v2",
	}

	_, err := client.ScheduleNewOrchestration(context.Background(), "orchestration")
	require.NoError(t, err)
	require.Equal(t, "v2", scheduler.start.GetVersion().GetValue())

	_, err = client.ScheduleNewOrchestration(context.Background(), "orchestration", api.WithVersion(""))
	require.NoError(t, err)
	require.NotNil(t, scheduler.start.Version)
	require.Empty(t, scheduler.start.GetVersion().GetValue())
}

func TestPrepareOrchestrationIDReusePolicy(t *testing.T) {
	tests := []struct {
		name       string
		action     api.CreateOrchestrationAction
		wantPolicy bool
		wantErr    bool
	}{
		{
			name:       "terminate maps to replaceable statuses",
			action:     api.REUSE_ID_ACTION_TERMINATE,
			wantPolicy: true,
		},
		{
			name:       "error maps to absent wire policy",
			action:     api.REUSE_ID_ACTION_ERROR,
			wantPolicy: false,
		},
		{
			// DTS reads the shared status field as a replacement policy, so
			// IGNORE cannot be expressed on the wire and must fail closed.
			name:    "ignore fails closed for the DTS wire contract",
			action:  api.REUSE_ID_ACTION_IGNORE,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &protos.CreateInstanceRequest{}
			configure := api.WithOrchestrationIdReusePolicy(&api.OrchestrationIdReusePolicy{
				Action:          tt.action,
				OperationStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_RUNNING},
			})
			require.NoError(t, configure(req, api.DefaultDataConverter()))

			c := &TaskHubGrpcClient{}
			err := c.prepareOrchestrationIDReusePolicy(req)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrUnsupportedOrchestrationIDReusePolicy)
				return
			}
			require.NoError(t, err)
			if tt.wantPolicy {
				require.NotNil(t, req.OrchestrationIdReusePolicy)
				require.Equal(t, []protos.OrchestrationStatus{protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING}, req.OrchestrationIdReusePolicy.ReplaceableStatus)
			} else {
				require.Nil(t, req.OrchestrationIdReusePolicy)
			}
		})
	}
}
