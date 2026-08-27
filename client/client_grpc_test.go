package client

import (
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func TestPrepareOrchestrationIDReusePolicy(t *testing.T) {
	tests := []struct {
		name        string
		action      api.CreateOrchestrationAction
		allowLegacy bool
		wantPolicy  bool
		wantErr     bool
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
			name:    "ignore fails closed for current wire",
			action:  api.REUSE_ID_ACTION_IGNORE,
			wantErr: true,
		},
		{
			name:        "ignore is available for known legacy sidecars",
			action:      api.REUSE_ID_ACTION_IGNORE,
			allowLegacy: true,
			wantPolicy:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &protos.CreateInstanceRequest{}
			configure := api.WithOrchestrationIdReusePolicy(&api.OrchestrationIdReusePolicy{
				Action:          tt.action,
				OperationStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_RUNNING},
			})
			require.NoError(t, configure(req))

			c := &TaskHubGrpcClient{allowLegacyIDReusePolicyWire: tt.allowLegacy}
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
