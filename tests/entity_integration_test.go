// Integration tests for in-process entity execution via the orchestration worker.
package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/task"
)

// Test that an entity can be signaled from the client and processed in-process.
func Test_InProcess_Entity_SignalAndQuery(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			_ = ctx.GetState(&count)
		}
		switch ctx.Operation {
		case "add":
			var amount int
			if err := ctx.GetInput(&amount); err != nil {
				return nil, err
			}
			count += amount
		case "get":
			// no-op, just return
		}
		_ = ctx.SetState(count)
		return count, nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	baseClient, worker := initTaskHubWorker(ctx, r)
	client := baseClient.(backend.EntityTaskHubClient)
	defer func() {
		if err := worker.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()

	entityID := api.NewEntityID("counter", "test1")

	// Signal the entity to add 5
	err := client.SignalEntity(ctx, entityID, "add", api.WithSignalInput(5))
	require.NoError(t, err)

	// Signal again to add 3
	err = client.SignalEntity(ctx, entityID, "add", api.WithSignalInput(3))
	require.NoError(t, err)

	// Poll until state contains "8"
	require.Eventually(t, func() bool {
		meta, err := client.FetchEntityMetadata(ctx, entityID, true)
		if err != nil || meta == nil {
			return false
		}
		return assert.ObjectsAreEqual(entityID, meta.InstanceID) &&
			strings.Contains(meta.SerializedState, "8")
	}, 10*time.Second, 200*time.Millisecond)
}

func Test_InProcess_Entity_SignalScheduledTime(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			_ = ctx.GetState(&count)
		}
		if ctx.Operation == "add" {
			var amount int
			if err := ctx.GetInput(&amount); err != nil {
				return nil, err
			}
			count += amount
		}
		_ = ctx.SetState(count)
		return count, nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	baseClient, worker := initTaskHubWorker(ctx, r)
	client := baseClient.(backend.EntityTaskHubClient)
	defer func() {
		if err := worker.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()

	entityID := api.NewEntityID("counter", "scheduled")
	fireAt := time.Now().Add(750 * time.Millisecond)

	require.NoError(t, client.SignalEntity(ctx, entityID, "add", api.WithSignalInput(5), api.WithSignalScheduledTime(fireAt)))

	require.Never(t, func() bool {
		meta, err := client.FetchEntityMetadata(ctx, entityID, true)
		if err != nil || meta == nil {
			return false
		}
		return strings.Contains(meta.SerializedState, "5")
	}, 300*time.Millisecond, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		meta, err := client.FetchEntityMetadata(ctx, entityID, true)
		if err != nil || meta == nil {
			return false
		}
		return strings.Contains(meta.SerializedState, "5")
	}, 10*time.Second, 100*time.Millisecond)
}

// Test that entities work with the auto-dispatch pattern.
func Test_InProcess_Entity_AutoDispatch(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var val int
		if ctx.HasState() {
			_ = ctx.GetState(&val)
		}
		switch ctx.Operation {
		case "increment":
			val++
		case "decrement":
			val--
		case "get":
		}
		_ = ctx.SetState(val)
		return val, nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	baseClient, worker := initTaskHubWorker(ctx, r)
	client := baseClient.(backend.EntityTaskHubClient)
	defer func() {
		if err := worker.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()

	entityID := api.NewEntityID("counter", "auto")

	// Send multiple signals
	require.NoError(t, client.SignalEntity(ctx, entityID, "increment"))
	require.NoError(t, client.SignalEntity(ctx, entityID, "increment"))
	require.NoError(t, client.SignalEntity(ctx, entityID, "increment"))
	require.NoError(t, client.SignalEntity(ctx, entityID, "decrement"))

	// Poll until state contains "2"
	require.Eventually(t, func() bool {
		meta, err := client.FetchEntityMetadata(ctx, entityID, true)
		if err != nil || meta == nil {
			return false
		}
		return strings.Contains(meta.SerializedState, "2")
	}, 10*time.Second, 200*time.Millisecond)
}

// Test that CallEntity works end-to-end: an orchestration calls an entity and gets a response.
func Test_InProcess_Entity_CallEntity(t *testing.T) {
	r := task.NewTaskRegistry()

	// Register a counter entity
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			_ = ctx.GetState(&count)
		}
		switch ctx.Operation {
		case "add":
			var amount int
			if err := ctx.GetInput(&amount); err != nil {
				return nil, err
			}
			count += amount
		case "get":
			// just return
		}
		_ = ctx.SetState(count)
		return count, nil
	}))

	// Register an orchestration that calls the entity and returns the result
	require.NoError(t, r.AddOrchestratorN("CallEntityOrchestrator", func(ctx *task.OrchestrationContext) (any, error) {
		entityID := api.NewEntityID("counter", "fromOrch")

		// Call entity (request-response) to add and get result
		var result int
		if err := ctx.CallEntity(entityID, "add", task.WithEntityInput(15)).Await(&result); err != nil {
			return nil, err
		}

		return result, nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	baseClient, worker := initTaskHubWorker(ctx, r)
	defer func() {
		if err := worker.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()

	// Note: we do NOT pre-create the entity — the orchestration processor
	// auto-creates entity instances when pending messages target entity IDs.

	// Run the orchestration
	id, err := baseClient.ScheduleNewOrchestration(ctx, "CallEntityOrchestrator")
	require.NoError(t, err)

	// Wait for orchestration to complete
	metadata, err := baseClient.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "ORCHESTRATION_STATUS_COMPLETED", metadata.RuntimeStatus.String())
	assert.Contains(t, metadata.SerializedOutput, "15")
}

func Test_InProcess_Entity_CriticalSectionBlocksExternalSignals(t *testing.T) {
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var count int
		if ctx.HasState() {
			if err := ctx.GetState(&count); err != nil {
				return nil, err
			}
		}
		switch ctx.Operation {
		case "add":
			var amount int
			if err := ctx.GetInput(&amount); err != nil {
				return nil, err
			}
			count += amount
		case "get":
		default:
			return nil, fmt.Errorf("unknown operation %q", ctx.Operation)
		}
		if err := ctx.SetState(count); err != nil {
			return nil, err
		}
		return count, nil
	}))

	entityID := api.NewEntityID("counter", "locked")
	require.NoError(t, r.AddOrchestratorN("critical-section", func(ctx *task.OrchestrationContext) (any, error) {
		unlock, err := ctx.LockEntities(entityID)
		if err != nil {
			return nil, err
		}
		defer unlock()

		if err := ctx.CallEntity(entityID, "add", task.WithEntityInput(1)).Await(nil); err != nil {
			return nil, err
		}
		if err := ctx.WaitForSingleEvent("release", -1).Await(nil); err != nil {
			return nil, err
		}
		var value int
		if err := ctx.CallEntity(entityID, "get").Await(&value); err != nil {
			return nil, err
		}
		return value, nil
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	baseClient, worker := initTaskHubWorker(ctx, r)
	client := baseClient.(backend.EntityTaskHubClient)
	defer func() {
		require.NoError(t, worker.Shutdown(context.Background()))
	}()

	instanceID, err := baseClient.ScheduleNewOrchestration(ctx, "critical-section")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		metadata, err := client.FetchEntityMetadata(ctx, entityID, true)
		return err == nil && metadata != nil && metadata.LockedBy == string(instanceID)
	}, 10*time.Second, 50*time.Millisecond)

	require.NoError(t, client.SignalEntity(ctx, entityID, "add", api.WithSignalInput(100)))
	require.NoError(t, baseClient.RaiseEvent(ctx, instanceID, "release"))
	metadata, err := baseClient.WaitForOrchestrationCompletion(ctx, instanceID)
	require.NoError(t, err)
	assert.Equal(t, "1", metadata.SerializedOutput)

	require.Eventually(t, func() bool {
		entity, err := client.FetchEntityMetadata(ctx, entityID, true)
		return err == nil && entity != nil && entity.SerializedState == "101" && entity.LockedBy == ""
	}, 10*time.Second, 50*time.Millisecond)
}

func Test_InProcess_Entity_CriticalSectionAlwaysReleases(t *testing.T) {
	t.Run("failure", func(t *testing.T) {
		entityID := api.NewEntityID("lockable", "failure")
		r := task.NewTaskRegistry()
		require.NoError(t, r.AddEntityN("lockable", func(*task.EntityContext) (any, error) { return nil, nil }))
		require.NoError(t, r.AddOrchestratorN("lock-failure", func(ctx *task.OrchestrationContext) (any, error) {
			if _, err := ctx.LockEntities(entityID); err != nil {
				return nil, err
			}
			return nil, errors.New("fail while locked")
		}))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		baseClient, worker := initTaskHubWorker(ctx, r)
		client := baseClient.(backend.EntityTaskHubClient)
		defer func() { require.NoError(t, worker.Shutdown(context.Background())) }()

		instanceID, err := baseClient.ScheduleNewOrchestration(ctx, "lock-failure")
		require.NoError(t, err)
		metadata, err := baseClient.WaitForOrchestrationCompletion(ctx, instanceID)
		require.NoError(t, err)
		assert.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
		require.Eventually(t, func() bool {
			entity, err := client.FetchEntityMetadata(ctx, entityID, false)
			return err == nil && entity != nil && entity.LockedBy == ""
		}, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("continue as new", func(t *testing.T) {
		entityID := api.NewEntityID("lockable", "continue")
		r := task.NewTaskRegistry()
		require.NoError(t, r.AddEntityN("lockable", func(*task.EntityContext) (any, error) { return nil, nil }))
		require.NoError(t, r.AddOrchestratorN("lock-continue", func(ctx *task.OrchestrationContext) (any, error) {
			var generation int
			if err := ctx.GetInput(&generation); err != nil {
				return nil, err
			}
			if generation == 0 {
				if _, err := ctx.LockEntities(entityID); err != nil {
					return nil, err
				}
				ctx.ContinueAsNew(1)
				return nil, nil
			}
			return "done", nil
		}))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		baseClient, worker := initTaskHubWorker(ctx, r)
		client := baseClient.(backend.EntityTaskHubClient)
		defer func() { require.NoError(t, worker.Shutdown(context.Background())) }()

		instanceID, err := baseClient.ScheduleNewOrchestration(ctx, "lock-continue", api.WithInput(0))
		require.NoError(t, err)
		metadata, err := baseClient.WaitForOrchestrationCompletion(ctx, instanceID)
		require.NoError(t, err)
		assert.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
		require.Eventually(t, func() bool {
			entity, err := client.FetchEntityMetadata(ctx, entityID, false)
			return err == nil && entity != nil && entity.LockedBy == ""
		}, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("terminate", func(t *testing.T) {
		entityID := api.NewEntityID("lockable", "terminate")
		r := task.NewTaskRegistry()
		require.NoError(t, r.AddEntityN("lockable", func(*task.EntityContext) (any, error) { return nil, nil }))
		require.NoError(t, r.AddOrchestratorN("lock-terminate", func(ctx *task.OrchestrationContext) (any, error) {
			if _, err := ctx.LockEntities(entityID); err != nil {
				return nil, err
			}
			return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
		}))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		baseClient, worker := initTaskHubWorker(ctx, r)
		client := baseClient.(backend.EntityTaskHubClient)
		defer func() { require.NoError(t, worker.Shutdown(context.Background())) }()

		instanceID, err := baseClient.ScheduleNewOrchestration(ctx, "lock-terminate")
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			entity, err := client.FetchEntityMetadata(ctx, entityID, false)
			return err == nil && entity != nil && entity.LockedBy == string(instanceID)
		}, 10*time.Second, 50*time.Millisecond)
		require.NoError(t, baseClient.TerminateOrchestration(ctx, instanceID))
		metadata, err := baseClient.WaitForOrchestrationCompletion(ctx, instanceID)
		require.NoError(t, err)
		assert.Equal(t, api.RUNTIME_STATUS_TERMINATED, metadata.RuntimeStatus)
		require.Eventually(t, func() bool {
			entity, err := client.FetchEntityMetadata(ctx, entityID, false)
			return err == nil && entity != nil && entity.LockedBy == ""
		}, 10*time.Second, 50*time.Millisecond)
	})
}
