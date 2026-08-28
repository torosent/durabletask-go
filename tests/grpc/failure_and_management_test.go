package tests_grpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tests/failurechain"
	"github.com/stretchr/testify/require"
)

// grpcLeafError opts into every durable failure enrichment hook so the gRPC
// wire contract can be checked end to end.
var grpcLeafError = &failurechain.LeafError{
	Message:        "grpc leaf boom",
	ErrorType:      "Contoso.GrpcLeafError",
	Stack:          "Contoso.Leaf.Run(leaf.go:7)",
	Properties:     map[string]any{"code": "E7", "attempts": 2},
	IsNonRetriable: true,
}

// Test_Grpc_DeepFailureChain asserts a Parent -> Sub-orchestration -> Activity
// failure survives the gRPC wire contract with its error types, messages,
// custom properties, stack traces, non-retriable flags, and inner failures.
func Test_Grpc_DeepFailureChain(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("GrpcFailureLeaf", func(task.ActivityContext) (any, error) {
		return nil, grpcLeafError
	}))
	require.NoError(t, registry.AddOrchestratorN("GrpcFailureChild", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("GrpcFailureLeaf").Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorN("GrpcFailureParent", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallSubOrchestrator(
			"GrpcFailureChild",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child"),
		).Await(nil)
	}))

	cancelListener := startGrpcListener(t, registry)
	defer cancelListener()

	id, err := grpcClient.ScheduleNewOrchestration(
		ctx,
		"GrpcFailureParent",
		api.WithInstanceID("grpc_failure_chain"),
	)
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(waitCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
	failurechain.Assert(t, metadata.FailureDetails, []failurechain.Frame{
		{
			ErrorType: api.ErrorTypeTaskFailed,
			MessageContains: []string{
				"Task 'GrpcFailureChild' (#0) failed with an unhandled exception",
				"grpc leaf boom",
			},
			NonRetriable: true,
		},
		{
			ErrorType: api.ErrorTypeTaskFailed,
			MessageContains: []string{
				"Task 'GrpcFailureLeaf' (#0) failed with an unhandled exception",
				"grpc leaf boom",
			},
			NonRetriable: true,
		},
		{
			ErrorType:       "Contoso.GrpcLeafError",
			MessageContains: []string{"grpc leaf boom"},
			ExpectStack:     true,
			NonRetriable:    true,
			Properties:      map[string]any{"code": "E7", "attempts": float64(2)},
		},
	})
	require.True(t, metadata.FailureDetails.IsCausedBy("Contoso.GrpcLeafError"))

	childMetadata, err := grpcClient.FetchOrchestrationMetadata(ctx, id+"_child", api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, childMetadata.RuntimeStatus)
	require.Equal(t, api.ErrorTypeTaskFailed, childMetadata.FailureDetails.ErrorType)
	require.Equal(t, api.ErrorType("Contoso.GrpcLeafError"), childMetadata.FailureDetails.InnerFailure.ErrorType)
}

// Test_Grpc_MissingTaskFailureIsNonRetriable asserts unregistered tasks produce
// the canonical non-retriable not-found failure over the gRPC wire and that the
// retry handler is never consulted for it.
func Test_Grpc_MissingTaskFailureIsNonRetriable(t *testing.T) {
	registry := task.NewTaskRegistry()
	handlerCalled := make(chan struct{}, 1)
	require.NoError(t, registry.AddOrchestratorN("GrpcMissingHost", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("GrpcMissingActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Millisecond,
			BackoffCoefficient:   1,
			Handle: func(task.RetryContext) bool {
				select {
				case handlerCalled <- struct{}{}:
				default:
				}
				return true
			},
		})).Await(nil)
	}))

	cancelListener := startGrpcListener(t, registry)
	defer cancelListener()

	id, err := grpcClient.ScheduleNewOrchestration(
		ctx,
		"GrpcMissingHost",
		api.WithInstanceID("grpc_missing_task"),
	)
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	metadata, err := grpcClient.WaitForOrchestrationCompletion(waitCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
	failurechain.Assert(t, metadata.FailureDetails, []failurechain.Frame{
		{
			ErrorType:       api.ErrorTypeTaskFailed,
			MessageContains: []string{"No activity task named 'GrpcMissingActivity' was found."},
			NonRetriable:    true,
		},
		{
			ErrorType:       api.ErrorTypeActivityTaskNotFound,
			MessageContains: []string{"No activity task named 'GrpcMissingActivity' was found."},
			NonRetriable:    true,
		},
	})
	require.True(t, metadata.FailureDetails.Matches(api.ErrTaskNotRegistered))
	require.Empty(t, handlerCalled, "retry handler must be bypassed for non-retriable failures")
}

// Test_Grpc_SuspendResumeAreNoOpsOutsideTheirState asserts the management
// contracts for redundant suspend and resume requests: suspending a completed
// instance and resuming a running or already-resumed instance are no-ops.
func Test_Grpc_SuspendResumeAreNoOpsOutsideTheirState(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("GrpcNoOpComplete", func(*task.OrchestrationContext) (any, error) {
		return "done", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("GrpcNoOpWait", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("release", -1).Await(nil)
	}))

	cancelListener := startGrpcListener(t, registry)
	defer cancelListener()
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	completedID, err := grpcClient.ScheduleNewOrchestration(
		ctx,
		"GrpcNoOpComplete",
		api.WithInstanceID("grpc_noop_completed"),
	)
	require.NoError(t, err)
	completed, err := grpcClient.WaitForOrchestrationCompletion(waitCtx, completedID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)

	// Suspending an already completed instance is accepted and ignored.
	require.NoError(t, grpcClient.SuspendOrchestration(ctx, completedID, "too late"))
	require.NoError(t, grpcClient.ResumeOrchestration(ctx, completedID, "still too late"))
	require.Never(t, func() bool {
		metadata, fetchErr := grpcClient.FetchOrchestrationMetadata(ctx, completedID)
		return fetchErr != nil || metadata.RuntimeStatus != api.RUNTIME_STATUS_COMPLETED
	}, time.Second, 100*time.Millisecond)
	stillCompleted, err := grpcClient.FetchOrchestrationMetadata(ctx, completedID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, completed.SerializedOutput, stillCompleted.SerializedOutput)

	runningID, err := grpcClient.ScheduleNewOrchestration(
		ctx,
		"GrpcNoOpWait",
		api.WithInstanceID("grpc_noop_running"),
	)
	require.NoError(t, err)
	_, err = grpcClient.WaitForOrchestrationStart(waitCtx, runningID)
	require.NoError(t, err)

	// Resuming a running, never-suspended instance leaves it running.
	require.NoError(t, grpcClient.ResumeOrchestration(ctx, runningID, "no-op"))
	require.NoError(t, grpcClient.ResumeOrchestration(ctx, runningID, "still a no-op"))
	require.Never(t, func() bool {
		metadata, fetchErr := grpcClient.FetchOrchestrationMetadata(ctx, runningID)
		return fetchErr != nil || metadata.RuntimeStatus != api.RUNTIME_STATUS_RUNNING
	}, time.Second, 100*time.Millisecond)

	require.NoError(t, grpcClient.RaiseEvent(ctx, runningID, "release"))
	finished, err := grpcClient.WaitForOrchestrationCompletion(waitCtx, runningID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_COMPLETED, finished.RuntimeStatus)
}

// Test_Grpc_RestartMissingInstanceIsTypedNotFound asserts a restart request for
// an unknown instance surfaces as api.ErrInstanceNotFound rather than an opaque
// RPC error, and that restarting a running instance reports ErrNotCompleted.
func Test_Grpc_RestartMissingInstanceIsTypedNotFound(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("GrpcRestartWait", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("release", -1).Await(nil)
	}))
	cancelListener := startGrpcListener(t, registry)
	defer cancelListener()

	_, err := grpcClient.RestartInstance(ctx, "grpc-restart-missing")
	require.ErrorIs(t, err, api.ErrInstanceNotFound)

	err = grpcClient.RewindInstance(ctx, "grpc-restart-missing")
	require.ErrorIs(t, err, api.ErrInstanceNotFound)

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	runningID, err := grpcClient.ScheduleNewOrchestration(
		ctx,
		"GrpcRestartWait",
		api.WithInstanceID("grpc_restart_running"),
	)
	require.NoError(t, err)
	_, err = grpcClient.WaitForOrchestrationStart(waitCtx, runningID)
	require.NoError(t, err)
	_, err = grpcClient.RestartInstance(ctx, runningID)
	require.ErrorIs(t, err, api.ErrNotCompleted)

	require.NoError(t, grpcClient.RaiseEvent(ctx, runningID, "release"))
	_, err = grpcClient.WaitForOrchestrationCompletion(waitCtx, runningID)
	require.NoError(t, err)
}

// Test_Grpc_PaginationContinuationEquivalence asserts that walking a query with
// small pages produces exactly the same ordered instance set as a single large
// page, for both the metadata query and the instance ID query.
func Test_Grpc_PaginationContinuationEquivalence(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("GrpcPage", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	cancelListener := startGrpcListener(t, registry)
	defer cancelListener()
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	const instanceCount = 7
	expected := make([]api.InstanceID, 0, instanceCount)
	for index := range instanceCount {
		id := api.InstanceID(fmt.Sprintf("grpc-page-%02d", index))
		_, err := grpcClient.ScheduleNewOrchestration(
			ctx,
			"GrpcPage",
			api.WithInstanceID(id),
			api.WithInput(fmt.Sprintf("payload-%d", index)),
		)
		require.NoError(t, err)
		completed, err := grpcClient.WaitForOrchestrationCompletion(waitCtx, id)
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
		expected = append(expected, id)
	}

	for _, pageSize := range []int{1, 2, 3, instanceCount, instanceCount + 5} {
		collected := drainPages(t, "query", instanceCount+2, pageSize,
			func(token string) ([]api.InstanceID, string, error) {
				page, err := grpcClient.QueryInstances(ctx, api.OrchestrationQuery{
					InstanceIDPrefix:  "grpc-page-",
					RuntimeStatus:     []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
					PageSize:          pageSize,
					ContinuationToken: token,
				})
				if err != nil {
					return nil, "", err
				}
				ids := make([]api.InstanceID, 0, len(page.Orchestrations))
				for _, item := range page.Orchestrations {
					ids = append(ids, item.InstanceID)
				}
				return ids, page.ContinuationToken, nil
			})
		require.Equalf(t, expected, collected, "page size %d", pageSize)

		// ListInstanceIDs is not prefix filtered, so unrelated instances left by
		// other tests in this suite are dropped after the page is drained.
		collectedIDs := drainPages(t, "instance ID", 100, pageSize,
			func(token string) ([]api.InstanceID, string, error) {
				page, err := grpcClient.ListInstanceIDs(ctx, api.InstanceIDQuery{
					RuntimeStatus:     []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
					PageSize:          pageSize,
					ContinuationToken: token,
				})
				if err != nil {
					return nil, "", err
				}
				return page.InstanceIDs, page.ContinuationToken, nil
			})
		filtered := make([]api.InstanceID, 0, instanceCount)
		for _, id := range collectedIDs {
			if strings.HasPrefix(string(id), "grpc-page-") {
				filtered = append(filtered, id)
			}
		}
		require.Equalf(t, expected, filtered, "instance ID page size %d", pageSize)
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

// Test_Grpc_OrchestrationIDReuseMatrix asserts the instance ID dedupe contract
// over the gRPC wire, including that duplicate rejections keep the typed
// api.ErrDuplicateInstance category after crossing the transport.
func Test_Grpc_OrchestrationIDReuseMatrix(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("GrpcReuseComplete", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("GrpcReuseFail", func(*task.OrchestrationContext) (any, error) {
		return nil, errors.New("reuse failure")
	}))
	require.NoError(t, registry.AddOrchestratorN("GrpcReuseWait", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("release", -1).Await(nil)
	}))
	cancelListener := startGrpcListener(t, registry)
	defer cancelListener()

	statuses := []struct {
		name   string
		status api.OrchestrationStatus
	}{
		{name: "running", status: api.RUNTIME_STATUS_RUNNING},
		{name: "completed", status: api.RUNTIME_STATUS_COMPLETED},
		{name: "failed", status: api.RUNTIME_STATUS_FAILED},
		{name: "terminated", status: api.RUNTIME_STATUS_TERMINATED},
	}
	actions := []struct {
		name   string
		action api.CreateOrchestrationAction
	}{
		{name: "ignore", action: api.REUSE_ID_ACTION_IGNORE},
		{name: "terminate", action: api.REUSE_ID_ACTION_TERMINATE},
	}

	counter := 0
	for _, existing := range statuses {
		for _, action := range actions {
			for _, matched := range []bool{true, false} {
				name := fmt.Sprintf("%s/%s/matched=%t", existing.name, action.name, matched)
				t.Run(name, func(t *testing.T) {
					counter++
					waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
					defer cancel()
					id := api.InstanceID(fmt.Sprintf("grpc_reuse_%02d", counter))
					driveGrpcInstanceToStatus(t, waitCtx, id, existing.status)

					policy := &api.OrchestrationIdReusePolicy{Action: action.action}
					if matched {
						policy.OperationStatus = []api.OrchestrationStatus{existing.status}
					} else {
						policy.OperationStatus = []api.OrchestrationStatus{api.RUNTIME_STATUS_CONTINUED_AS_NEW}
					}
					_, err := grpcClient.ScheduleNewOrchestration(
						waitCtx,
						"GrpcReuseComplete",
						api.WithInstanceID(id),
						api.WithInput("replacement"),
						api.WithOrchestrationIdReusePolicy(policy),
					)

					switch {
					case !matched:
						// The duplicate rejection must keep its typed category
						// after crossing the gRPC transport.
						require.ErrorIs(t, err, api.ErrDuplicateInstance)
						current, fetchErr := grpcClient.FetchOrchestrationMetadata(
							waitCtx,
							id,
							api.WithFetchPayloads(true),
						)
						require.NoError(t, fetchErr)
						require.Equal(t, existing.status, current.RuntimeStatus)
					case action.action == api.REUSE_ID_ACTION_IGNORE:
						require.NoError(t, err)
						require.Never(t, func() bool {
							metadata, fetchErr := grpcClient.FetchOrchestrationMetadata(
								waitCtx,
								id,
								api.WithFetchPayloads(true),
							)
							return fetchErr != nil || metadata.SerializedInput == `"replacement"`
						}, time.Second, 100*time.Millisecond)
					default:
						require.NoError(t, err)
						completed, waitErr := grpcClient.WaitForOrchestrationCompletion(
							waitCtx,
							id,
							api.WithFetchPayloads(true),
						)
						require.NoError(t, waitErr)
						require.Equal(t, api.RUNTIME_STATUS_COMPLETED, completed.RuntimeStatus)
						require.Equal(t, `"replacement"`, completed.SerializedOutput)
					}
				})
			}
		}
	}
}

// driveGrpcInstanceToStatus schedules an instance over gRPC and moves it into
// the requested runtime status.
func driveGrpcInstanceToStatus(
	t *testing.T,
	waitCtx context.Context,
	id api.InstanceID,
	status api.OrchestrationStatus,
) {
	t.Helper()
	switch status {
	case api.RUNTIME_STATUS_COMPLETED:
		_, err := grpcClient.ScheduleNewOrchestration(
			waitCtx,
			"GrpcReuseComplete",
			api.WithInstanceID(id),
			api.WithInput("original"),
		)
		require.NoError(t, err)
		_, err = grpcClient.WaitForOrchestrationCompletion(waitCtx, id)
		require.NoError(t, err)
	case api.RUNTIME_STATUS_FAILED:
		_, err := grpcClient.ScheduleNewOrchestration(waitCtx, "GrpcReuseFail", api.WithInstanceID(id))
		require.NoError(t, err)
		_, err = grpcClient.WaitForOrchestrationCompletion(waitCtx, id)
		require.NoError(t, err)
	case api.RUNTIME_STATUS_RUNNING:
		_, err := grpcClient.ScheduleNewOrchestration(waitCtx, "GrpcReuseWait", api.WithInstanceID(id))
		require.NoError(t, err)
		_, err = grpcClient.WaitForOrchestrationStart(waitCtx, id)
		require.NoError(t, err)
	case api.RUNTIME_STATUS_TERMINATED:
		driveGrpcInstanceToStatus(t, waitCtx, id, api.RUNTIME_STATUS_RUNNING)
		require.NoError(t, grpcClient.TerminateOrchestration(waitCtx, id))
		_, err := grpcClient.WaitForOrchestrationCompletion(waitCtx, id)
		require.NoError(t, err)
	default:
		t.Fatalf("unsupported runtime status %v", status)
	}
	require.Eventually(t, func() bool {
		metadata, err := grpcClient.FetchOrchestrationMetadata(waitCtx, id)
		return err == nil && metadata.RuntimeStatus == status
	}, 20*time.Second, 50*time.Millisecond)
}
