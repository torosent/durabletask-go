package tests

import (
	"context"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestVersionFailureRejectAbandonsOrchestrationWithoutPersistingState(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorN("versioned", func(*task.OrchestrationContext) (any, error) {
			return "unexpected", nil
		}))
		executor := task.NewTaskExecutor(registry, task.WithVersioning(task.VersioningOptions{
			Version:         "1.0",
			MatchStrategy:   task.VersionMatchCurrentOrOlder,
			FailureStrategy: task.VersionFailureReject,
		}))
		startEvent := helpers.NewExecutionStartedEvent(
			"versioned",
			"version-reject",
			nil,
			nil,
			nil,
			nil,
			wrapperspb.String("2.0"),
		)
		require.NoError(t, be.CreateOrchestrationInstance(ctx, startEvent))

		worker := backend.NewOrchestrationWorker(be, executor, logger)
		ok, err := worker.ProcessNext(context.Background())
		require.NoError(t, err)
		require.True(t, ok)

		var abandoned *backend.OrchestrationWorkItem
		require.Eventually(t, func() bool {
			workItem, fetchErr := be.GetOrchestrationWorkItem(ctx)
			if fetchErr != nil {
				return false
			}
			abandoned = workItem
			return true
		}, 2*time.Second, 10*time.Millisecond)
		worker.StopAndDrain()

		metadata, err := be.GetOrchestrationMetadata(ctx, "version-reject")
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_PENDING, metadata.RuntimeStatus)
		state, err := be.GetOrchestrationRuntimeState(ctx, abandoned)
		require.NoError(t, err)
		require.Zero(t, state.HistoryLength())
		require.NoError(t, be.AbandonOrchestrationWorkItem(ctx, abandoned))
	}
}

func TestVersionFailureFailPersistsNonRetriableOrchestrationFailure(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)
		registry := task.NewTaskRegistry()
		executor := task.NewTaskExecutor(registry, task.WithVersioning(task.VersioningOptions{
			Version:         "1.0",
			MatchStrategy:   task.VersionMatchStrict,
			FailureStrategy: task.VersionFailureFail,
		}))
		startEvent := helpers.NewExecutionStartedEvent(
			"versioned",
			"version-fail",
			nil,
			nil,
			nil,
			nil,
			wrapperspb.String("2.0"),
		)
		require.NoError(t, be.CreateOrchestrationInstance(ctx, startEvent))

		worker := backend.NewOrchestrationWorker(be, executor, logger)
		ok, err := worker.ProcessNext(context.Background())
		require.NoError(t, err)
		require.True(t, ok)
		require.Eventually(t, func() bool {
			metadata, metadataErr := be.GetOrchestrationMetadata(ctx, "version-fail")
			return metadataErr == nil && metadata.RuntimeStatus == api.RUNTIME_STATUS_FAILED
		}, 2*time.Second, 10*time.Millisecond)
		worker.StopAndDrain()

		metadata, err := be.GetOrchestrationMetadata(ctx, "version-fail")
		require.NoError(t, err)
		require.Equal(t, api.ErrorTypeVersionMismatch, metadata.FailureDetails.ErrorType)
		require.True(t, metadata.FailureDetails.IsNonRetriable)
	}
}

func TestVersionFailureStrategiesApplyToLocalActivityDispatch(t *testing.T) {
	for i, be := range backends {
		t.Run("reject", func(t *testing.T) {
			initTest(t, be, i, true)
			enqueueVersionedActivity(t, be, "activity-reject", "2.0")
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddActivityN("versioned-activity", func(task.ActivityContext) (any, error) {
				return "unexpected", nil
			}))
			executor := task.NewTaskExecutor(registry, task.WithVersioning(task.VersioningOptions{
				Version:         "1.0",
				MatchStrategy:   task.VersionMatchStrict,
				FailureStrategy: task.VersionFailureReject,
			}))
			worker := backend.NewActivityTaskWorker(be, executor, logger)
			ok, err := worker.ProcessNext(context.Background())
			require.NoError(t, err)
			require.True(t, ok)

			var abandoned *backend.ActivityWorkItem
			require.Eventually(t, func() bool {
				workItem, fetchErr := be.GetActivityWorkItem(ctx)
				if fetchErr != nil {
					return false
				}
				abandoned = workItem
				return true
			}, 2*time.Second, 10*time.Millisecond)
			worker.StopAndDrain()
			require.Equal(t, int32(1), abandoned.RetryCount)
			require.NoError(t, be.AbandonActivityWorkItem(ctx, abandoned))
		})

		t.Run("fail", func(t *testing.T) {
			initTest(t, be, i, true)
			enqueueVersionedActivity(t, be, "activity-fail", "2.0")
			executor := task.NewTaskExecutor(task.NewTaskRegistry(), task.WithVersioning(task.VersioningOptions{
				Version:         "1.0",
				MatchStrategy:   task.VersionMatchStrict,
				FailureStrategy: task.VersionFailureFail,
			}))
			worker := backend.NewActivityTaskWorker(be, executor, logger)
			ok, err := worker.ProcessNext(context.Background())
			require.NoError(t, err)
			require.True(t, ok)

			var orchestrationWorkItem *backend.OrchestrationWorkItem
			require.Eventually(t, func() bool {
				workItem, fetchErr := be.GetOrchestrationWorkItem(ctx)
				if fetchErr != nil {
					return false
				}
				orchestrationWorkItem = workItem
				return true
			}, 2*time.Second, 10*time.Millisecond)
			worker.StopAndDrain()

			require.Len(t, orchestrationWorkItem.NewEvents, 1)
			failure := orchestrationWorkItem.NewEvents[0].GetTaskFailed().GetFailureDetails()
			require.Equal(t, "VersionMismatch", failure.GetErrorType())
			require.True(t, failure.GetIsNonRetriable())
			require.NoError(t, be.AbandonOrchestrationWorkItem(ctx, orchestrationWorkItem))
		})
	}
}

func enqueueVersionedActivity(t *testing.T, be backend.Backend, instanceID string, version string) {
	t.Helper()
	startEvent := helpers.NewExecutionStartedEvent(
		"parent",
		instanceID,
		nil,
		nil,
		nil,
		nil,
		wrapperspb.String("1.0"),
	)
	require.NoError(t, be.CreateOrchestrationInstance(ctx, startEvent))
	workItem, ok := getOrchestrationWorkItem(t, be, instanceID)
	require.True(t, ok)
	state, ok := getOrchestrationRuntimeState(t, be, workItem)
	require.True(t, ok)
	for _, event := range workItem.NewEvents {
		require.NoError(t, state.AddEvent(event))
	}
	_, err := state.ApplyActions([]*protos.OrchestratorAction{
		helpers.NewScheduleTaskAction(0, "versioned-activity", nil, wrapperspb.String(version)),
	}, nil)
	require.NoError(t, err)
	workItem.State = state
	require.NoError(t, be.CompleteOrchestrationWorkItem(ctx, workItem))
}

func TestVersionChangingContinueAsNewHandsOffBetweenStrictWorkers(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)

		v1Registry := task.NewTaskRegistry()
		require.NoError(t, v1Registry.AddOrchestratorNVersion("rolling", "1.0", func(ctx *task.OrchestrationContext) (any, error) {
			ctx.ContinueAsNew("next", task.WithContinueAsNewVersion("2.0"))
			return nil, nil
		}))
		v2Registry := task.NewTaskRegistry()
		require.NoError(t, v2Registry.AddOrchestratorNVersion("rolling", "2.0", func(ctx *task.OrchestrationContext) (any, error) {
			var input string
			if err := ctx.GetInput(&input); err != nil {
				return nil, err
			}
			return "completed-" + input, nil
		}))

		v1Worker := backend.NewOrchestrationWorker(
			be,
			task.NewTaskExecutor(v1Registry, task.WithVersioning(task.VersioningOptions{
				Version:         "1.0",
				MatchStrategy:   task.VersionMatchStrict,
				FailureStrategy: task.VersionFailureReject,
			})),
			logger,
		)
		v2Worker := backend.NewOrchestrationWorker(
			be,
			task.NewTaskExecutor(v2Registry, task.WithVersioning(task.VersioningOptions{
				Version:         "2.0",
				MatchStrategy:   task.VersionMatchStrict,
				FailureStrategy: task.VersionFailureReject,
			})),
			logger,
		)
		client := backend.NewTaskHubClient(be)
		instanceID, err := client.ScheduleNewOrchestration(
			context.Background(),
			"rolling",
			api.WithVersion("1.0"),
		)
		require.NoError(t, err)
		processed, err := v1Worker.ProcessNext(context.Background())
		require.NoError(t, err)
		require.True(t, processed)
		require.Eventually(t, func() bool {
			metadata, metadataErr := client.FetchOrchestrationMetadata(context.Background(), instanceID)
			return metadataErr == nil &&
				metadata.RuntimeStatus == api.RUNTIME_STATUS_PENDING &&
				metadata.Version == "2.0"
		}, 5*time.Second, 10*time.Millisecond)
		v1Worker.StopAndDrain()

		processed, err = v2Worker.ProcessNext(context.Background())
		require.NoError(t, err)
		require.True(t, processed)
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		metadata, err := client.WaitForOrchestrationCompletion(waitCtx, instanceID)
		waitCancel()
		v2Worker.StopAndDrain()
		require.NoError(t, err)
		require.Equal(t, "2.0", metadata.Version)
		require.Equal(t, `"completed-next"`, metadata.SerializedOutput)
	}
}
