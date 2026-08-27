package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

func TestAdvancedManagementQueriesRestartPurgeAndTermination(t *testing.T) {
	for i, be := range getRunnableBackends() {
		t.Run(fmt.Sprintf("backend-%d", i), func(t *testing.T) {
			initTest(t, be, i, false)
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("ManagementComplete", func(ctx *task.OrchestrationContext) (any, error) {
				var input string
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				return input, nil
			}))
			require.NoError(t, registry.AddOrchestratorN("ManagementWait", func(ctx *task.OrchestrationContext) (any, error) {
				if err := ctx.CreateTimer(time.Hour).Await(nil); err != nil {
					return nil, err
				}
				return "done", nil
			}))
			executor := task.NewTaskExecutor(registry)
			worker := backend.NewTaskHubWorker(
				be,
				backend.NewOrchestrationWorker(be, executor, logger),
				backend.NewActivityTaskWorker(be, executor, logger),
				logger,
			)
			testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, worker.Start(testCtx))
			t.Cleanup(func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer shutdownCancel()
				require.NoError(t, worker.Shutdown(shutdownCtx))
			})
			client := backend.NewTaskHubManagementClient(be)

			completedFrom := time.Now().UTC().Add(-time.Second)
			scheduledStart := time.Now().UTC().Add(100 * time.Millisecond)
			for index := range 5 {
				id := api.InstanceID(fmt.Sprintf("management-%02d", index))
				group := "odd"
				if index%2 == 0 {
					group = "even"
				}
				options := []api.NewOrchestrationOptions{
					api.WithInstanceID(id),
					api.WithInput(fmt.Sprintf("value-%d", index)),
					api.WithTags(map[string]string{"group": group}),
				}
				if index == 0 {
					options = append(options, api.WithStartTime(scheduledStart))
				}
				_, err := client.ScheduleNewOrchestration(
					testCtx,
					"ManagementComplete",
					options...,
				)
				require.NoError(t, err)
				completed, err := client.WaitForOrchestrationCompletion(testCtx, id)
				require.NoError(t, err)
				if index == 0 {
					require.WithinDuration(t, scheduledStart, completed.ScheduledStartAt, time.Millisecond)
				}
			}

			firstPage, err := client.QueryInstances(testCtx, api.OrchestrationQuery{
				RuntimeStatus:    []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
				InstanceIDPrefix: "management-",
				PageSize:         2,
				Tags:             map[string]string{"group": "even"},
			})
			require.NoError(t, err)
			require.Equal(t, []api.InstanceID{"management-00", "management-02"}, metadataIDs(firstPage.Orchestrations))
			require.NotEmpty(t, firstPage.ContinuationToken)
			secondPage, err := client.QueryInstances(testCtx, api.OrchestrationQuery{
				RuntimeStatus:     []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
				InstanceIDPrefix:  "management-",
				PageSize:          2,
				ContinuationToken: firstPage.ContinuationToken,
				Tags:              map[string]string{"group": "even"},
			})
			require.NoError(t, err)
			require.Equal(t, []api.InstanceID{"management-04"}, metadataIDs(secondPage.Orchestrations))
			require.Empty(t, secondPage.ContinuationToken)

			listed := make([]api.InstanceID, 0, 5)
			token := ""
			for {
				page, err := client.ListInstanceIDs(testCtx, api.InstanceIDQuery{
					RuntimeStatus:     []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
					CompletedTimeFrom: completedFrom,
					PageSize:          2,
					ContinuationToken: token,
				})
				require.NoError(t, err)
				listed = append(listed, page.InstanceIDs...)
				token = page.ContinuationToken
				if token == "" {
					break
				}
			}
			require.Equal(t, []api.InstanceID{
				"management-00",
				"management-01",
				"management-02",
				"management-03",
				"management-04",
			}, listed)

			restartedID, err := client.RestartInstance(
				testCtx,
				"management-00",
				api.WithRestartNewInstanceID(true),
			)
			require.NoError(t, err)
			require.NotEqual(t, api.InstanceID("management-00"), restartedID)
			restarted, err := client.WaitForOrchestrationCompletion(testCtx, restartedID)
			require.NoError(t, err)
			require.Equal(t, `"value-0"`, restarted.SerializedOutput)
			require.Equal(t, "even", restarted.Tags["group"])

			sameID, err := client.RestartInstance(testCtx, "management-01")
			require.NoError(t, err)
			require.Equal(t, api.InstanceID("management-01"), sameID)
			restarted, err = client.WaitForOrchestrationCompletion(testCtx, sameID)
			require.NoError(t, err)
			require.Equal(t, "odd", restarted.Tags["group"])

			waitID := api.InstanceID("management-wait")
			_, err = client.ScheduleNewOrchestration(
				testCtx,
				"ManagementWait",
				api.WithInstanceID(waitID),
				api.WithTags(map[string]string{"group": "wait"}),
			)
			require.NoError(t, err)
			_, err = client.WaitForOrchestrationStart(testCtx, waitID)
			require.NoError(t, err)
			unterminated, err := client.SkipGracefulOrchestrationTerminations(testCtx, []api.InstanceID{waitID}, "test")
			require.NoError(t, err)
			require.Empty(t, unterminated)
			terminated, err := client.FetchOrchestrationMetadata(testCtx, waitID)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_TERMINATED, terminated.RuntimeStatus)
			require.Equal(t, `"test"`, terminated.SerializedOutput)
			unterminated, err = client.SkipGracefulOrchestrationTerminations(testCtx, []api.InstanceID{waitID}, "again")
			require.NoError(t, err)
			require.Equal(t, []api.InstanceID{waitID}, unterminated)

			batchResult, err := client.PurgeInstances(testCtx, api.PurgeInstancesRequest{
				InstanceIDs: []api.InstanceID{"management-00", restartedID},
			})
			require.NoError(t, err)
			require.True(t, batchResult.IsComplete)
			require.Equal(t, 2, batchResult.DeletedInstanceCount)

			filterResult, err := client.PurgeInstances(testCtx, api.PurgeInstancesRequest{
				Filter: &api.PurgeInstanceFilter{
					CreatedTimeFrom: completedFrom,
				},
				PollInterval: time.Millisecond,
			})
			require.NoError(t, err)
			require.True(t, filterResult.IsComplete)
			require.GreaterOrEqual(t, filterResult.DeletedInstanceCount, 5)
		})
	}
}

func TestRewindFailedOrchestration(t *testing.T) {
	for i, be := range getRunnableBackends() {
		t.Run(fmt.Sprintf("backend-%d", i), func(t *testing.T) {
			initTest(t, be, i, false)
			registry := task.NewTaskRegistry()
			var attempts atomic.Int32
			require.NoError(t, registry.AddActivityN("RewindActivity", func(task.ActivityContext) (any, error) {
				if attempts.Add(1) == 1 {
					return nil, errors.New("first attempt fails")
				}
				return "recovered", nil
			}))
			require.NoError(t, registry.AddOrchestratorN("RewindOrchestration", func(ctx *task.OrchestrationContext) (any, error) {
				var result string
				if err := ctx.CallActivity("RewindActivity").Await(&result); err != nil {
					return nil, err
				}
				return result, nil
			}))
			executor := task.NewTaskExecutor(registry)
			worker := backend.NewTaskHubWorker(
				be,
				backend.NewOrchestrationWorker(be, executor, logger),
				backend.NewActivityTaskWorker(be, executor, logger),
				logger,
			)
			testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, worker.Start(testCtx))
			t.Cleanup(func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer shutdownCancel()
				require.NoError(t, worker.Shutdown(shutdownCtx))
			})
			client := backend.NewTaskHubManagementClient(be)
			id, err := client.ScheduleNewOrchestration(
				testCtx,
				"RewindOrchestration",
				api.WithInstanceID("rewind-instance"),
				api.WithTags(map[string]string{"scenario": "rewind"}),
			)
			require.NoError(t, err)
			failed, err := client.WaitForOrchestrationCompletion(testCtx, id)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, failed.RuntimeStatus)
			originalExecutionID := failed.ExecutionID

			require.NoError(t, client.RewindInstance(testCtx, id, api.WithRewindReason("retry failed activity")))
			completed, err := client.WaitForOrchestrationCompletion(testCtx, id)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
			require.Equal(t, `"recovered"`, completed.SerializedOutput)
			require.Equal(t, "rewind", completed.Tags["scenario"])
			require.NotEqual(t, originalExecutionID, completed.ExecutionID)
			require.EqualValues(t, 2, attempts.Load())

			err = client.RewindInstance(testCtx, id)
			require.ErrorIs(t, err, api.ErrInvalidState)
		})
	}
}

func TestRewindFailedSubOrchestration(t *testing.T) {
	for i, be := range getRunnableBackends() {
		t.Run(fmt.Sprintf("backend-%d", i), func(t *testing.T) {
			initTest(t, be, i, false)
			registry := task.NewTaskRegistry()
			var attempts atomic.Int32
			require.NoError(t, registry.AddActivityN("RewindChildActivity", func(task.ActivityContext) (any, error) {
				if attempts.Add(1) == 1 {
					return nil, errors.New("first child attempt fails")
				}
				return "child-recovered", nil
			}))
			require.NoError(t, registry.AddOrchestratorN("RewindChild", func(ctx *task.OrchestrationContext) (any, error) {
				var result string
				if err := ctx.CallActivity("RewindChildActivity").Await(&result); err != nil {
					return nil, err
				}
				return result, nil
			}))
			require.NoError(t, registry.AddOrchestratorN("RewindParent", func(ctx *task.OrchestrationContext) (any, error) {
				var result string
				if err := ctx.CallSubOrchestrator(
					"RewindChild",
					task.WithSubOrchestrationInstanceID("rewind-child"),
				).Await(&result); err != nil {
					return nil, err
				}
				return result, nil
			}))
			executor := task.NewTaskExecutor(registry)
			worker := backend.NewTaskHubWorker(
				be,
				backend.NewOrchestrationWorker(be, executor, logger),
				backend.NewActivityTaskWorker(be, executor, logger),
				logger,
			)
			testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, worker.Start(testCtx))
			t.Cleanup(func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer shutdownCancel()
				require.NoError(t, worker.Shutdown(shutdownCtx))
			})
			client := backend.NewTaskHubManagementClient(be)
			parentID, err := client.ScheduleNewOrchestration(
				testCtx,
				"RewindParent",
				api.WithInstanceID("rewind-parent"),
				api.WithTags(map[string]string{"scenario": "sub-rewind"}),
			)
			require.NoError(t, err)
			failedParent, err := client.WaitForOrchestrationCompletion(testCtx, parentID)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, failedParent.RuntimeStatus)
			failedChild, err := client.FetchOrchestrationMetadata(testCtx, "rewind-child")
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, failedChild.RuntimeStatus)

			require.NoError(t, client.RewindInstance(testCtx, parentID, api.WithRewindReason("retry child")))
			completedChild, err := client.WaitForOrchestrationCompletion(testCtx, "rewind-child")
			require.NoError(t, err)
			completedParent, err := client.WaitForOrchestrationCompletion(testCtx, parentID)
			require.NoError(t, err)
			require.Equal(t, `"child-recovered"`, completedParent.SerializedOutput)
			require.Equal(t, parentID, completedChild.ParentInstanceID)
			require.Equal(t, "sub-rewind", completedChild.Tags["scenario"])
			require.NotEqual(t, failedParent.ExecutionID, completedParent.ExecutionID)
			require.NotEqual(t, failedChild.ExecutionID, completedChild.ExecutionID)
			require.EqualValues(t, 2, attempts.Load())
		})
	}
}

func TestTaskHubLifecycleManagement(t *testing.T) {
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(t.TempDir()+"/taskhub.sqlite"), logger)
	client := backend.NewTaskHubManagementClient(be)
	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, client.CreateTaskHub(testCtx))
	require.NoError(t, client.DeleteTaskHub(testCtx))
	require.NoError(t, client.CreateTaskHub(testCtx))
	require.NoError(t, client.DeleteTaskHub(testCtx))
}

func TestSkipGracefulRejectsActiveSubOrchestration(t *testing.T) {
	for i, be := range getRunnableBackends() {
		t.Run(fmt.Sprintf("backend-%d", i), func(t *testing.T) {
			initTest(t, be, i, false)
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("SkipChild", func(ctx *task.OrchestrationContext) (any, error) {
				if err := ctx.CreateTimer(time.Hour).Await(nil); err != nil {
					return nil, err
				}
				return nil, nil
			}))
			require.NoError(t, registry.AddOrchestratorN("SkipParent", func(ctx *task.OrchestrationContext) (any, error) {
				return nil, ctx.CallSubOrchestrator(
					"SkipChild",
					task.WithSubOrchestrationInstanceID("skip-child"),
				).Await(nil)
			}))
			executor := task.NewTaskExecutor(registry)
			worker := backend.NewTaskHubWorker(
				be,
				backend.NewOrchestrationWorker(be, executor, logger),
				backend.NewActivityTaskWorker(be, executor, logger),
				logger,
			)
			testCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			require.NoError(t, worker.Start(testCtx))
			t.Cleanup(func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer shutdownCancel()
				require.NoError(t, worker.Shutdown(shutdownCtx))
			})
			client := backend.NewTaskHubManagementClient(be)
			parentID, err := client.ScheduleNewOrchestration(
				testCtx,
				"SkipParent",
				api.WithInstanceID("skip-parent"),
			)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				metadata, fetchErr := client.FetchOrchestrationMetadata(testCtx, "skip-child")
				return fetchErr == nil && metadata.RuntimeStatus == api.RUNTIME_STATUS_RUNNING
			}, 5*time.Second, 10*time.Millisecond)

			unterminated, err := client.SkipGracefulOrchestrationTerminations(
				testCtx,
				[]api.InstanceID{"skip-child"},
				"unsafe",
			)
			require.NoError(t, err)
			require.Equal(t, []api.InstanceID{"skip-child"}, unterminated)
			require.NoError(t, client.TerminateOrchestration(testCtx, parentID))
			_, err = client.WaitForOrchestrationCompletion(testCtx, parentID)
			require.NoError(t, err)
		})
	}
}

func metadataIDs(metadata []*api.OrchestrationMetadata) []api.InstanceID {
	ids := make([]api.InstanceID, len(metadata))
	for i, item := range metadata {
		ids[i] = item.InstanceID
	}
	return ids
}
