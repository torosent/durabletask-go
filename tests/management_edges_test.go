package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

// initManagementWorker starts a worker over a private in-memory SQLite task hub
// and returns a management client for tests that need history, query, restart,
// or rewind APIs.
func initManagementWorker(
	ctx context.Context,
	t *testing.T,
	registry *task.TaskRegistry,
) (backend.TaskHubManagementClient, backend.TaskHubWorker) {
	t.Helper()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(registry)
	worker := backend.NewTaskHubWorker(
		be,
		backend.NewOrchestrationWorker(be, executor, logger),
		backend.NewActivityTaskWorker(be, executor, logger),
		logger,
	)
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := worker.Shutdown(shutdownCtx); err != nil {
			t.Logf("shutdown: %v", err)
		}
	})
	return backend.NewTaskHubManagementClient(be), worker
}

// managementEdgeRegistry registers the orchestrators used to drive an instance
// into every runtime status the management contracts care about.
func managementEdgeRegistry(t *testing.T) *task.TaskRegistry {
	t.Helper()
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("EdgeComplete", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("EdgeFail", func(*task.OrchestrationContext) (any, error) {
		return nil, errors.New("edge failure")
	}))
	require.NoError(t, registry.AddOrchestratorN("EdgeWait", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("release", -1).Await(nil)
	}))
	return registry
}

// driveToStatus schedules an instance and moves it into the requested runtime
// status, returning once the task hub reports that status.
func driveToStatus(
	ctx context.Context,
	t *testing.T,
	client backend.TaskHubManagementClient,
	id api.InstanceID,
	status api.OrchestrationStatus,
) {
	t.Helper()
	switch status {
	case api.RUNTIME_STATUS_PENDING:
		// A far-future start time keeps the instance queued but unstarted.
		_, err := client.ScheduleNewOrchestration(
			ctx,
			"EdgeComplete",
			api.WithInstanceID(id),
			api.WithInput("pending"),
			api.WithStartTime(time.Now().UTC().Add(time.Hour)),
		)
		require.NoError(t, err)
	case api.RUNTIME_STATUS_COMPLETED:
		_, err := client.ScheduleNewOrchestration(
			ctx,
			"EdgeComplete",
			api.WithInstanceID(id),
			api.WithInput("original"),
		)
		require.NoError(t, err)
		_, err = client.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
	case api.RUNTIME_STATUS_FAILED:
		_, err := client.ScheduleNewOrchestration(ctx, "EdgeFail", api.WithInstanceID(id))
		require.NoError(t, err)
		_, err = client.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
	case api.RUNTIME_STATUS_RUNNING:
		_, err := client.ScheduleNewOrchestration(ctx, "EdgeWait", api.WithInstanceID(id))
		require.NoError(t, err)
		_, err = client.WaitForOrchestrationStart(ctx, id)
		require.NoError(t, err)
	case api.RUNTIME_STATUS_TERMINATED:
		driveToStatus(ctx, t, client, id, api.RUNTIME_STATUS_RUNNING)
		require.NoError(t, client.TerminateOrchestration(ctx, id))
		_, err := client.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
	case api.RUNTIME_STATUS_SUSPENDED:
		driveToStatus(ctx, t, client, id, api.RUNTIME_STATUS_RUNNING)
		require.NoError(t, client.SuspendOrchestration(ctx, id, "matrix"))
	default:
		t.Fatalf("unsupported runtime status %v", status)
	}
	requireStatus(ctx, t, client, id, status)
}

func requireStatus(
	ctx context.Context,
	t *testing.T,
	client backend.TaskHubManagementClient,
	id api.InstanceID,
	status api.OrchestrationStatus,
) {
	t.Helper()
	var observed api.OrchestrationStatus
	require.Eventuallyf(t, func() bool {
		metadata, err := client.FetchOrchestrationMetadata(ctx, id)
		if err != nil {
			return false
		}
		observed = metadata.RuntimeStatus
		return observed == status
	}, 10*time.Second, 10*time.Millisecond, "instance %s never reached %v (last %v)", id, status, observed)
}

// Test_Management_SuspendAndResumeAreNoOpsOutsideTheirState asserts the
// idempotency contracts of the suspend and resume management APIs: suspending a
// completed instance never revives or re-labels it, and resuming an instance
// that is running, never suspended, or already resumed leaves it running.
func Test_Management_SuspendAndResumeAreNoOpsOutsideTheirState(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     api.OrchestrationStatus
		operations []string
	}{
		{name: "suspend-completed", status: api.RUNTIME_STATUS_COMPLETED, operations: []string{"suspend"}},
		{name: "resume-completed", status: api.RUNTIME_STATUS_COMPLETED, operations: []string{"resume"}},
		{
			name:       "suspend-then-resume-completed",
			status:     api.RUNTIME_STATUS_COMPLETED,
			operations: []string{"suspend", "resume"},
		},
		{name: "suspend-failed", status: api.RUNTIME_STATUS_FAILED, operations: []string{"suspend"}},
		{name: "suspend-terminated", status: api.RUNTIME_STATUS_TERMINATED, operations: []string{"suspend"}},
		{name: "resume-running", status: api.RUNTIME_STATUS_RUNNING, operations: []string{"resume"}},
		{
			name:       "resume-running-repeatedly",
			status:     api.RUNTIME_STATUS_RUNNING,
			operations: []string{"resume", "resume", "resume"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			client, _ := initManagementWorker(ctx, t, managementEdgeRegistry(t))
			id := api.InstanceID("edge-noop-" + test.name)
			driveToStatus(ctx, t, client, id, test.status)
			before, err := client.FetchOrchestrationMetadata(ctx, id)
			require.NoError(t, err)

			for _, operation := range test.operations {
				switch operation {
				case "suspend":
					require.NoError(t, client.SuspendOrchestration(ctx, id, "no-op"))
				case "resume":
					require.NoError(t, client.ResumeOrchestration(ctx, id, "no-op"))
				}
			}

			// The status must never drift away from where the instance started.
			require.Never(t, func() bool {
				metadata, fetchErr := client.FetchOrchestrationMetadata(ctx, id)
				return fetchErr != nil || metadata.RuntimeStatus != test.status
			}, 2*time.Second, 100*time.Millisecond)

			after, err := client.FetchOrchestrationMetadata(ctx, id)
			require.NoError(t, err)
			require.Equal(t, before.RuntimeStatus, after.RuntimeStatus)
			require.Equal(t, before.SerializedOutput, after.SerializedOutput)
			require.Equal(t, before.ExecutionID, after.ExecutionID)
			if before.RuntimeStatus == api.RUNTIME_STATUS_RUNNING {
				// A resumed running instance must still make progress.
				require.NoError(t, client.RaiseEvent(ctx, id, "release"))
				completed, err := client.WaitForOrchestrationCompletion(ctx, id)
				require.NoError(t, err)
				require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
			}
		})
	}
}

// Test_Management_SuspendResumeRoundTripStillWorks guards the no-op assertions
// above from silently disabling the real suspend and resume behaviour, and
// asserts suspend/resume is a boolean state rather than a counter: one resume
// always undoes any number of suspends.
func Test_Management_SuspendResumeRoundTripStillWorks(t *testing.T) {
	for _, suspendCount := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("suspends=%d", suspendCount), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			client, _ := initManagementWorker(ctx, t, managementEdgeRegistry(t))
			id := api.InstanceID(fmt.Sprintf("edge-suspend-round-trip-%d", suspendCount))
			driveToStatus(ctx, t, client, id, api.RUNTIME_STATUS_SUSPENDED)
			for i := 1; i < suspendCount; i++ {
				require.NoError(t, client.SuspendOrchestration(ctx, id, "again"))
			}

			// Events raised while suspended stay buffered.
			require.NoError(t, client.RaiseEvent(ctx, id, "release"))
			require.Never(t, func() bool {
				metadata, err := client.FetchOrchestrationMetadata(ctx, id)
				return err != nil || metadata.RuntimeStatus != api.RUNTIME_STATUS_SUSPENDED
			}, 2*time.Second, 100*time.Millisecond)

			require.NoError(t, client.ResumeOrchestration(ctx, id, "go"))
			completed, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
		})
	}
}

// Test_Management_TerminateOverridesSuspension asserts termination is not
// blocked by suspension. Without this, a suspended instance could never be
// terminated and callers waiting on completion would wait forever.
func Test_Management_TerminateOverridesSuspension(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, _ := initManagementWorker(ctx, t, managementEdgeRegistry(t))
	id := api.InstanceID("edge-terminate-suspended")
	driveToStatus(ctx, t, client, id, api.RUNTIME_STATUS_SUSPENDED)

	require.NoError(t, client.TerminateOrchestration(ctx, id, api.WithOutput("stopped")))
	terminated, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_TERMINATED, terminated.RuntimeStatus)
	require.Equal(t, `"stopped"`, terminated.SerializedOutput)

	// A resume after termination must not revive the instance.
	require.NoError(t, client.ResumeOrchestration(ctx, id, "too late"))
	require.Never(t, func() bool {
		metadata, fetchErr := client.FetchOrchestrationMetadata(ctx, id)
		return fetchErr != nil || metadata.RuntimeStatus != api.RUNTIME_STATUS_TERMINATED
	}, 2*time.Second, 100*time.Millisecond)
}

// Test_Management_RestartAndRewindReportTypedErrors asserts the typed error
// contracts for restart and rewind: an unknown instance is ErrInstanceNotFound
// and a still-running instance is ErrNotCompleted.
func Test_Management_RestartAndRewindReportTypedErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, _ := initManagementWorker(ctx, t, managementEdgeRegistry(t))

	for _, test := range []struct {
		name      string
		operation func(api.InstanceID) error
	}{
		{
			name: "restart",
			operation: func(id api.InstanceID) error {
				_, err := client.RestartInstance(ctx, id)
				return err
			},
		},
		{
			name: "restart-with-new-id",
			operation: func(id api.InstanceID) error {
				_, err := client.RestartInstance(ctx, id, api.WithRestartNewInstanceID(true))
				return err
			},
		},
		{
			name: "rewind",
			operation: func(id api.InstanceID) error {
				return client.RewindInstance(ctx, id, api.WithRewindReason("missing"))
			},
		},
	} {
		t.Run(test.name+"-missing", func(t *testing.T) {
			err := test.operation("edge-missing-instance")
			require.ErrorIs(t, err, api.ErrInstanceNotFound)
		})
	}

	runningID := api.InstanceID("edge-restart-running")
	driveToStatus(ctx, t, client, runningID, api.RUNTIME_STATUS_RUNNING)
	_, err := client.RestartInstance(ctx, runningID)
	require.ErrorIs(t, err, api.ErrNotCompleted)
	// Rewind only applies to failed instances.
	require.ErrorIs(t, client.RewindInstance(ctx, runningID), api.ErrInvalidState)

	completedID := api.InstanceID("edge-rewind-completed")
	driveToStatus(ctx, t, client, completedID, api.RUNTIME_STATUS_COMPLETED)
	require.ErrorIs(t, client.RewindInstance(ctx, completedID), api.ErrInvalidState)

	// Restart of a completed instance still succeeds, proving the typed errors
	// above are not masking a broken happy path.
	restartedID, err := client.RestartInstance(ctx, completedID)
	require.NoError(t, err)
	require.Equal(t, completedID, restartedID)
	restarted, err := client.WaitForOrchestrationCompletion(ctx, restartedID)
	require.NoError(t, err)
	require.Equal(t, `"original"`, restarted.SerializedOutput)
}

// Test_Management_OrchestrationIDReuseMatrix walks every combination of reuse
// action and existing runtime status to lock down the dedupe contract.
func Test_Management_OrchestrationIDReuseMatrix(t *testing.T) {
	statuses := []struct {
		name   string
		status api.OrchestrationStatus
	}{
		{name: "pending", status: api.RUNTIME_STATUS_PENDING},
		{name: "running", status: api.RUNTIME_STATUS_RUNNING},
		{name: "suspended", status: api.RUNTIME_STATUS_SUSPENDED},
		{name: "completed", status: api.RUNTIME_STATUS_COMPLETED},
		{name: "failed", status: api.RUNTIME_STATUS_FAILED},
		{name: "terminated", status: api.RUNTIME_STATUS_TERMINATED},
	}
	actions := []struct {
		name   string
		action api.CreateOrchestrationAction
	}{
		{name: "error", action: api.REUSE_ID_ACTION_ERROR},
		{name: "ignore", action: api.REUSE_ID_ACTION_IGNORE},
		{name: "terminate", action: api.REUSE_ID_ACTION_TERMINATE},
	}

	for _, existing := range statuses {
		for _, action := range actions {
			for _, matched := range []bool{true, false} {
				name := fmt.Sprintf("%s/%s/matched=%t", existing.name, action.name, matched)
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
					defer cancel()
					client, _ := initManagementWorker(ctx, t, managementEdgeRegistry(t))
					id := api.InstanceID("edge-reuse")
					driveToStatus(ctx, t, client, id, existing.status)
					before, err := client.FetchOrchestrationMetadata(ctx, id)
					require.NoError(t, err)

					policy := &api.OrchestrationIdReusePolicy{Action: action.action}
					if matched {
						policy.OperationStatus = []api.OrchestrationStatus{existing.status}
					} else {
						// A status the existing instance is guaranteed not to have.
						policy.OperationStatus = []api.OrchestrationStatus{api.RUNTIME_STATUS_CONTINUED_AS_NEW}
					}

					_, err = client.ScheduleNewOrchestration(
						ctx,
						"EdgeComplete",
						api.WithInstanceID(id),
						api.WithInput("replacement"),
						api.WithOrchestrationIdReusePolicy(policy),
					)

					switch {
					case !matched, action.action == api.REUSE_ID_ACTION_ERROR:
						// An unmatched status, or a matched status with the error
						// action, always rejects the duplicate and preserves state.
						require.ErrorIs(t, err, api.ErrDuplicateInstance)
						after, fetchErr := client.FetchOrchestrationMetadata(ctx, id)
						require.NoError(t, fetchErr)
						require.Equal(t, before.RuntimeStatus, after.RuntimeStatus)
						require.Equal(t, before.ExecutionID, after.ExecutionID)
						require.Equal(t, before.SerializedInput, after.SerializedInput)
					case action.action == api.REUSE_ID_ACTION_IGNORE:
						// The duplicate is silently dropped and the original survives.
						require.NoError(t, err)
						require.Never(t, func() bool {
							metadata, fetchErr := client.FetchOrchestrationMetadata(ctx, id)
							return fetchErr != nil || metadata.SerializedInput == `"replacement"`
						}, time.Second, 100*time.Millisecond)
						after, fetchErr := client.FetchOrchestrationMetadata(ctx, id)
						require.NoError(t, fetchErr)
						require.Equal(t, before.SerializedInput, after.SerializedInput)
					default:
						// The existing instance is replaced by the new one.
						require.NoError(t, err)
						completed, waitErr := client.WaitForOrchestrationCompletion(ctx, id)
						require.NoError(t, waitErr)
						require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
						require.Equal(t, `"replacement"`, completed.SerializedInput)
						require.Equal(t, `"replacement"`, completed.SerializedOutput)
					}
				})
			}
		}
	}
}

// Test_Management_DefaultReusePolicyRejectsDuplicates asserts that scheduling
// without an explicit reuse policy rejects any existing instance ID.
func Test_Management_DefaultReusePolicyRejectsDuplicates(t *testing.T) {
	for _, status := range []api.OrchestrationStatus{
		api.RUNTIME_STATUS_PENDING,
		api.RUNTIME_STATUS_RUNNING,
		api.RUNTIME_STATUS_SUSPENDED,
		api.RUNTIME_STATUS_COMPLETED,
		api.RUNTIME_STATUS_FAILED,
		api.RUNTIME_STATUS_TERMINATED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			client, _ := initManagementWorker(ctx, t, managementEdgeRegistry(t))
			id := api.InstanceID("edge-default-reuse")
			driveToStatus(ctx, t, client, id, status)

			_, err := client.ScheduleNewOrchestration(
				ctx,
				"EdgeComplete",
				api.WithInstanceID(id),
				api.WithInput("replacement"),
			)
			require.ErrorIs(t, err, api.ErrDuplicateInstance)
		})
	}
}

// Test_Management_PaginationContinuationEquivalence asserts that paging through
// a query with any page size yields exactly the same ordered result as a single
// unpaged query, including when tag filters are applied.
func Test_Management_PaginationContinuationEquivalence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, _ := initManagementWorker(ctx, t, managementEdgeRegistry(t))

	const instanceCount = 9
	all := make([]api.InstanceID, 0, instanceCount)
	even := make([]api.InstanceID, 0, instanceCount/2+1)
	for index := range instanceCount {
		id := api.InstanceID(fmt.Sprintf("page-%02d", index))
		group := "odd"
		if index%2 == 0 {
			group = "even"
		}
		_, err := client.ScheduleNewOrchestration(
			ctx,
			"EdgeComplete",
			api.WithInstanceID(id),
			api.WithInput(fmt.Sprintf("value-%d", index)),
			api.WithTags(map[string]string{"group": group}),
		)
		require.NoError(t, err)
		completed, err := client.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
		all = append(all, id)
		if group == "even" {
			even = append(even, id)
		}
	}

	for _, test := range []struct {
		name     string
		tags     map[string]string
		expected []api.InstanceID
	}{
		{name: "no-tags", expected: all},
		{name: "tag-filtered", tags: map[string]string{"group": "even"}, expected: even},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, pageSize := range []int{1, 2, 4, instanceCount, instanceCount + 3} {
				query := api.OrchestrationQuery{
					InstanceIDPrefix: "page-",
					RuntimeStatus:    []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
					PageSize:         pageSize,
					Tags:             test.tags,
				}
				collected := drainPages(t, "query", instanceCount+2, pageSize,
					func(token string) ([]api.InstanceID, string, error) {
						query.ContinuationToken = token
						page, err := client.QueryInstances(ctx, query)
						if err != nil {
							return nil, "", err
						}
						return metadataIDs(page.Orchestrations), page.ContinuationToken, nil
					})
				require.Equalf(t, test.expected, collected, "page size %d", pageSize)
			}
		})
	}

	// ListInstanceIDs must page equivalently as well.
	for _, pageSize := range []int{1, 3, instanceCount, instanceCount + 3} {
		query := api.InstanceIDQuery{
			RuntimeStatus: []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
			PageSize:      pageSize,
		}
		collected := drainPages(t, "instance ID", instanceCount+2, pageSize,
			func(token string) ([]api.InstanceID, string, error) {
				query.ContinuationToken = token
				page, err := client.ListInstanceIDs(ctx, query)
				if err != nil {
					return nil, "", err
				}
				return page.InstanceIDs, page.ContinuationToken, nil
			})
		require.Equalf(t, all, collected, "instance ID page size %d", pageSize)
	}
}

// drainPages walks a paginated management query to exhaustion and returns every
// item in service order. It bounds the number of pages so a server that never
// clears the continuation token fails the test instead of hanging.
func drainPages[T any](
	t *testing.T,
	label string,
	maxPages int,
	pageSize int,
	fetch func(token string) ([]T, string, error),
) []T {
	t.Helper()
	collected := []T{}
	token := ""
	for pages := 0; ; pages++ {
		require.Lessf(t, pages, maxPages, "%s pagination did not terminate", label)
		items, next, err := fetch(token)
		require.NoError(t, err)
		require.LessOrEqualf(t, len(items), pageSize, "%s page exceeded the requested page size", label)
		collected = append(collected, items...)
		token = next
		if token == "" {
			return collected
		}
	}
}

// Test_Management_QueryPageSizeValidation asserts the shared page size contract
// used by every management query surface.
func Test_Management_QueryPageSizeValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, _ := initManagementWorker(ctx, t, managementEdgeRegistry(t))

	for _, test := range []struct {
		name      string
		pageSize  int
		wantError bool
	}{
		{name: "negative", pageSize: -1, wantError: true},
		{name: "default", pageSize: 0},
		{name: "max", pageSize: api.MaxInstanceQueryPageSize},
		{name: "above-max", pageSize: api.MaxInstanceQueryPageSize + 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.QueryInstances(ctx, api.OrchestrationQuery{PageSize: test.pageSize})
			if test.wantError {
				require.ErrorIs(t, err, api.ErrInvalidArgument)
			} else {
				require.NoError(t, err)
			}
			_, err = client.ListInstanceIDs(ctx, api.InstanceIDQuery{PageSize: test.pageSize})
			if test.wantError {
				require.ErrorIs(t, err, api.ErrInvalidArgument)
			} else {
				require.NoError(t, err)
			}
		})
	}

	_, err := client.QueryInstances(ctx, api.OrchestrationQuery{ContinuationToken: "not-base64!!"})
	require.Error(t, err)
}
