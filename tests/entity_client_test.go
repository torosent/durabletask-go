// Tests for entity client operations, ported from the .NET SDK's ShimDurableEntityClientTests.
package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/tests/mocks"
)

func newEntityClient(be backend.Backend) backend.EntityTaskHubClient {
	return backend.NewTaskHubClient(be).(backend.EntityTaskHubClient)
}

type signalCaptureBackend struct {
	backend.Backend
	request *protos.SignalEntityRequest
}

func (be *signalCaptureBackend) SignalEntity(_ context.Context, request *protos.SignalEntityRequest) error {
	be.request = request
	return nil
}

func Test_EntityClient_CleanEntityStorage_Defaults(t *testing.T) {
	be := &mocks.EntityBackend{}
	client := newEntityClient(be)
	ctx := context.Background()

	req := api.CleanEntityStorageRequest{
		RemoveEmptyEntities:  true,
		ReleaseOrphanedLocks: true,
	}

	be.EXPECT().CleanEntityStorage(ctx, req).Return(&api.CleanEntityStorageResult{
		EmptyEntitiesRemoved:  5,
		OrphanedLocksReleased: 3,
	}, nil).Once()

	result, err := client.CleanEntityStorage(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, int32(5), result.EmptyEntitiesRemoved)
	assert.Equal(t, int32(3), result.OrphanedLocksReleased)
	assert.Empty(t, result.ContinuationToken)
}

func Test_EntityClient_CleanEntityStorage_WithOptions(t *testing.T) {
	tests := []struct {
		name                 string
		removeEmpty          bool
		releaseLocks         bool
		hasContinuationToken bool
	}{
		{"remove only", true, false, false},
		{"release only", false, true, false},
		{"both", true, true, false},
		{"with continuation", true, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			be := &mocks.EntityBackend{}
			client := newEntityClient(be)
			ctx := context.Background()

			req := api.CleanEntityStorageRequest{
				RemoveEmptyEntities:  tt.removeEmpty,
				ReleaseOrphanedLocks: tt.releaseLocks,
			}
			if tt.hasContinuationToken {
				req.ContinuationToken = "token123"
			}

			expectedResult := &api.CleanEntityStorageResult{
				EmptyEntitiesRemoved:  2,
				OrphanedLocksReleased: 1,
			}
			if tt.hasContinuationToken {
				expectedResult.ContinuationToken = "nextToken"
			}

			be.EXPECT().CleanEntityStorage(ctx, req).Return(expectedResult, nil).Once()

			result, err := client.CleanEntityStorage(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, expectedResult.EmptyEntitiesRemoved, result.EmptyEntitiesRemoved)
			assert.Equal(t, expectedResult.OrphanedLocksReleased, result.OrphanedLocksReleased)
			assert.Equal(t, expectedResult.ContinuationToken, result.ContinuationToken)
		})
	}
}

func Test_EntityClient_FetchEntityMetadata(t *testing.T) {
	tests := []struct {
		name         string
		includeState bool
	}{
		{"with state", true},
		{"without state", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			be := &mocks.EntityBackend{}
			client := newEntityClient(be)
			ctx := context.Background()

			entityID := api.NewEntityID("counter", "myCounter")
			now := time.Now().Truncate(time.Second)

			expected := &api.EntityMetadata{
				InstanceID:       entityID,
				LastModifiedTime: now,
				BacklogQueueSize: 2,
				LockedBy:         "some-orchestration",
			}
			if tt.includeState {
				expected.SerializedState = `{"value":42}`
			}

			be.EXPECT().GetEntityMetadata(ctx, entityID, tt.includeState).Return(expected, nil).Once()

			result, err := client.FetchEntityMetadata(ctx, entityID, tt.includeState)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, entityID, result.InstanceID)
			assert.Equal(t, now, result.LastModifiedTime)
			assert.Equal(t, int32(2), result.BacklogQueueSize)
			assert.Equal(t, "some-orchestration", result.LockedBy)
			if tt.includeState {
				assert.Equal(t, `{"value":42}`, result.SerializedState)
			}
		})
	}
}

func Test_EntityClient_FetchEntityMetadata_NotFound(t *testing.T) {
	be := &mocks.EntityBackend{}
	client := newEntityClient(be)
	ctx := context.Background()

	entityID := api.NewEntityID("counter", "missing")
	be.EXPECT().GetEntityMetadata(ctx, entityID, true).Return(nil, nil).Once()

	result, err := client.FetchEntityMetadata(ctx, entityID, true)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func Test_EntityClient_QueryEntities_NoFilter(t *testing.T) {
	be := &mocks.EntityBackend{}
	client := newEntityClient(be)
	ctx := context.Background()

	query := api.EntityQuery{IncludeState: true}

	be.EXPECT().QueryEntities(ctx, query).Return(&api.EntityQueryResults{
		Entities: []*api.EntityMetadata{
			{InstanceID: api.NewEntityID("counter", "a"), SerializedState: `1`},
			{InstanceID: api.NewEntityID("counter", "b"), SerializedState: `2`},
			{InstanceID: api.NewEntityID("counter", "c"), SerializedState: `3`},
		},
	}, nil).Once()

	result, err := client.QueryEntities(ctx, query)
	require.NoError(t, err)
	require.Len(t, result.Entities, 3)
	assert.Equal(t, "a", result.Entities[0].InstanceID.Key)
	assert.Equal(t, "b", result.Entities[1].InstanceID.Key)
	assert.Equal(t, "c", result.Entities[2].InstanceID.Key)
}

func Test_EntityClient_QueryEntities_WithFilter(t *testing.T) {
	be := &mocks.EntityBackend{}
	client := newEntityClient(be)
	ctx := context.Background()

	now := time.Now().Truncate(time.Second)
	query := api.EntityQuery{
		InstanceIDStartsWith: "@counter@",
		LastModifiedFrom:     now.Add(-1 * time.Hour),
		LastModifiedTo:       now,
		IncludeState:         true,
		IncludeTransient:     false,
		PageSize:             10,
		ContinuationToken:    "page1",
	}

	be.EXPECT().QueryEntities(ctx, query).Return(&api.EntityQueryResults{
		Entities: []*api.EntityMetadata{
			{InstanceID: api.NewEntityID("counter", "x"), SerializedState: `99`},
		},
		ContinuationToken: "page2",
	}, nil).Once()

	result, err := client.QueryEntities(ctx, query)
	require.NoError(t, err)
	require.Len(t, result.Entities, 1)
	assert.Equal(t, "x", result.Entities[0].InstanceID.Key)
	assert.Equal(t, "page2", result.ContinuationToken)
}

func Test_EntityClient_NonEntityBackendReturnsUnsupported(t *testing.T) {
	be := &mocks.Backend{}
	client := newEntityClient(be)
	ctx := context.Background()

	t.Run("QueryEntities returns error", func(t *testing.T) {
		_, err := client.QueryEntities(ctx, api.EntityQuery{})
		require.ErrorIs(t, err, api.ErrFeatureNotSupported)
	})

	t.Run("CleanEntityStorage returns error", func(t *testing.T) {
		_, err := client.CleanEntityStorage(ctx, api.CleanEntityStorageRequest{})
		require.ErrorIs(t, err, api.ErrFeatureNotSupported)
	})

	t.Run("FetchEntityMetadata returns error", func(t *testing.T) {
		entityID := api.NewEntityID("counter", "fallback")
		_, err := client.FetchEntityMetadata(ctx, entityID, true)
		require.ErrorIs(t, err, api.ErrFeatureNotSupported)
	})

	t.Run("SignalEntity returns error", func(t *testing.T) {
		err := client.SignalEntity(ctx, api.NewEntityID("counter", "fallback"), "increment")
		require.ErrorIs(t, err, api.ErrFeatureNotSupported)
	})
}

func Test_EntityClient_SignalEntity(t *testing.T) {
	be := &signalCaptureBackend{}
	client := newEntityClient(be)
	ctx := context.Background()

	entityID := api.NewEntityID("counter", "signalTest")

	err := client.SignalEntity(ctx, entityID, "increment", api.WithSignalInput(5))
	require.NoError(t, err)
	require.NotNil(t, be.request)
	assert.Equal(t, entityID.String(), be.request.InstanceId)
	assert.Equal(t, "increment", be.request.Name)
	assert.Equal(t, "5", be.request.Input.GetValue())
}

func Test_EntityClient_SignalEntity_PreservesScheduledTime(t *testing.T) {
	be := &signalCaptureBackend{}
	client := newEntityClient(be)
	ctx := context.Background()

	entityID := api.NewEntityID("counter", "signalTest")
	scheduledTime := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Millisecond)

	err := client.SignalEntity(ctx, entityID, "increment", api.WithSignalScheduledTime(scheduledTime))
	require.NoError(t, err)
	require.NotNil(t, be.request.ScheduledTime)
	require.WithinDuration(t, scheduledTime, be.request.ScheduledTime.AsTime(), time.Millisecond)
}
