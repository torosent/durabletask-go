package task

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func TestLockEntitiesSortsAndDeduplicatesEveryPermutation(t *testing.T) {
	source := []api.EntityID{
		api.NewEntityID("cart", "z"),
		api.NewEntityID("account", "b"),
		api.NewEntityID("account", "a"),
		api.NewEntityID("cart", "z"),
	}
	expected := []string{"@account@a", "@account@b", "@cart@z"}
	random := rand.New(rand.NewSource(42))

	for iteration := 0; iteration < 100; iteration++ {
		entities := append([]api.EntityID(nil), source...)
		random.Shuffle(len(entities), func(left, right int) {
			entities[left], entities[right] = entities[right], entities[left]
		})
		registry := NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorN("lock-order", func(ctx *OrchestrationContext) (any, error) {
			_, err := ctx.LockEntities(entities...)
			return nil, err
		}))
		instanceID := api.InstanceID("lock-order-instance")
		result := executeOrchestrationTurn(
			t,
			registry,
			instanceID,
			nil,
			[]*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("lock-order", string(instanceID), nil, nil, nil, nil),
			},
		)
		require.Len(t, result.Actions, 1)
		lock := result.Actions[0].GetSendEntityMessage().GetEntityLockRequested()
		require.NotNil(t, lock)
		require.True(t, slices.Equal(expected, lock.LockSet))
		require.Equal(t, int32(0), lock.Position)
	}
}
