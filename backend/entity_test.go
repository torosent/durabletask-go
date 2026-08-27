package backend

import (
	"testing"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func TestEntityBatchFromRequestV2AllowsMissingParentMetadata(t *testing.T) {
	requestID := uuid.NewString()
	batch, infos, err := EntityBatchFromRequestV2(&protos.EntityRequest{
		InstanceId: "@counter@key",
		OperationRequests: []*protos.HistoryEvent{{
			EventType: &protos.HistoryEvent_EntityOperationCalled{
				EntityOperationCalled: &protos.EntityOperationCalledEvent{
					RequestId: requestID,
					Operation: "get",
				},
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, batch.Operations, 1)
	require.Len(t, infos, 1)
	require.NotNil(t, infos[0].ResponseDestination)
	require.Empty(t, infos[0].ResponseDestination.InstanceId)
}
