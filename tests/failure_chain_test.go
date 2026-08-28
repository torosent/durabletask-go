package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tests/failurechain"
	"github.com/stretchr/testify/require"
)

// Test_FailureChain_ParentSubOrchestrationActivity asserts the complete
// Parent -> Sub-orchestration -> Activity durable failure chain, including the
// error type, message, custom properties, stack trace, non-retriable flag, and
// inner failure of every frame.
func Test_FailureChain_ParentSubOrchestrationActivity(t *testing.T) {
	for _, test := range []struct {
		name          string
		activityError error
		frames        []failurechain.Frame
		causedBy      []api.ErrorType
	}{
		{
			name: "enriched-non-retriable",
			activityError: &failurechain.LeafError{
				Message:        "leaf boom",
				ErrorType:      "Contoso.LeafError",
				Stack:          "Contoso.Leaf.Run(leaf.go:42)",
				Properties:     map[string]any{"code": "E42", "attempts": 3},
				IsNonRetriable: true,
			},
			frames: []failurechain.Frame{
				{
					ErrorType: api.ErrorTypeTaskFailed,
					MessageContains: []string{
						"Task 'FailureChild' (#0) failed with an unhandled exception",
						"leaf boom",
					},
					NonRetriable: true,
				},
				{
					ErrorType: api.ErrorTypeTaskFailed,
					MessageContains: []string{
						"Task 'FailureLeaf' (#0) failed with an unhandled exception",
						"leaf boom",
					},
					NonRetriable: true,
				},
				{
					ErrorType:       "Contoso.LeafError",
					MessageContains: []string{"leaf boom"},
					StackContains:   "Contoso.Leaf.Run(leaf.go:42)",
					ExpectStack:     true,
					NonRetriable:    true,
					Properties:      map[string]any{"code": "E42", "attempts": float64(3)},
				},
			},
			causedBy: []api.ErrorType{"Contoso.LeafError"},
		},
		{
			name:          "plain-retriable",
			activityError: errors.New("plain leaf failure"),
			frames: []failurechain.Frame{
				{
					ErrorType: api.ErrorTypeTaskFailed,
					MessageContains: []string{
						"Task 'FailureChild' (#0) failed with an unhandled exception",
						"plain leaf failure",
					},
				},
				{
					ErrorType: api.ErrorTypeTaskFailed,
					MessageContains: []string{
						"Task 'FailureLeaf' (#0) failed with an unhandled exception",
						"plain leaf failure",
					},
				},
				{
					ErrorType:       "*errors.errorString",
					MessageContains: []string{"plain leaf failure"},
				},
			},
			causedBy: []api.ErrorType{api.ErrorTypeTaskFailed},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddActivityN("FailureLeaf", func(task.ActivityContext) (any, error) {
				return nil, test.activityError
			}))
			var childErr atomic.Value
			require.NoError(t, registry.AddOrchestratorN("FailureChild", func(ctx *task.OrchestrationContext) (any, error) {
				err := ctx.CallActivity("FailureLeaf").Await(nil)
				if err != nil {
					childErr.Store(err)
				}
				return nil, err
			}))
			var parentErr atomic.Value
			require.NoError(t, registry.AddOrchestratorN("FailureParent", func(ctx *task.OrchestrationContext) (any, error) {
				err := ctx.CallSubOrchestrator(
					"FailureChild",
					task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child"),
				).Await(nil)
				if err != nil {
					parentErr.Store(err)
				}
				return nil, err
			}))

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, worker := initTaskHubWorker(ctx, registry)
			defer func() {
				if err := worker.Shutdown(ctx); err != nil {
					t.Logf("shutdown: %v", err)
				}
			}()

			id, err := client.ScheduleNewOrchestration(ctx, "FailureParent")
			require.NoError(t, err)
			metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
			failurechain.Assert(t, metadata.FailureDetails, test.frames)
			require.True(t, metadata.FailureDetails.IsCausedBy(test.causedBy...))

			childMetadata, err := client.FetchOrchestrationMetadata(ctx, id+"_child")
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, childMetadata.RuntimeStatus)
			// The child instance records the same chain minus the parent frame.
			failurechain.Assert(t, childMetadata.FailureDetails, test.frames[1:])

			// The orchestrator-visible errors are typed durable task failures whose
			// details match the frame the awaiting orchestration observed.
			var childTaskErr *task.TaskFailedError
			require.True(t, errors.As(childErr.Load().(error), &childTaskErr))
			require.Equal(t, "FailureLeaf", childTaskErr.TaskName)
			require.EqualValues(t, 0, childTaskErr.TaskID)
			require.Equal(t, test.frames[2].ErrorType, childTaskErr.FailureDetails.ErrorType)
			require.Equal(t, test.frames[1].NonRetriable, childTaskErr.NonRetriable())

			var parentTaskErr *task.TaskFailedError
			require.True(t, errors.As(parentErr.Load().(error), &parentTaskErr))
			require.Equal(t, "FailureChild", parentTaskErr.TaskName)
			require.Equal(t, api.ErrorTypeTaskFailed, parentTaskErr.FailureDetails.ErrorType)
			require.Equal(t, test.frames[0].NonRetriable, parentTaskErr.NonRetriable())
		})
	}
}

// Test_FailureChain_MissingTasksAreNonRetriable asserts unregistered activities
// and sub-orchestrations produce canonical non-retriable failures that stay
// classified as ErrTaskNotRegistered through the whole chain.
func Test_FailureChain_MissingTasksAreNonRetriable(t *testing.T) {
	for _, test := range []struct {
		name      string
		child     func(ctx *task.OrchestrationContext) (any, error)
		leafType  api.ErrorType
		leafParts []string
	}{
		{
			name: "missing-activity",
			child: func(ctx *task.OrchestrationContext) (any, error) {
				return nil, ctx.CallActivity("MissingLeafActivity").Await(nil)
			},
			leafType:  api.ErrorTypeActivityTaskNotFound,
			leafParts: []string{"No activity task named 'MissingLeafActivity' was found."},
		},
		{
			name: "missing-sub-orchestration",
			child: func(ctx *task.OrchestrationContext) (any, error) {
				return nil, ctx.CallSubOrchestrator(
					"MissingLeafOrchestrator",
					task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_leaf"),
				).Await(nil)
			},
			leafType:  api.ErrorTypeOrchestratorTaskNotFound,
			leafParts: []string{"No orchestrator task named 'MissingLeafOrchestrator' was found."},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddOrchestratorN("MissingChild", test.child))
			require.NoError(t, registry.AddOrchestratorN("MissingParent", func(ctx *task.OrchestrationContext) (any, error) {
				return nil, ctx.CallSubOrchestrator(
					"MissingChild",
					task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child"),
				).Await(nil)
			}))

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, worker := initTaskHubWorker(ctx, registry)
			defer func() {
				if err := worker.Shutdown(ctx); err != nil {
					t.Logf("shutdown: %v", err)
				}
			}()

			id, err := client.ScheduleNewOrchestration(ctx, "MissingParent")
			require.NoError(t, err)
			metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
			failurechain.Assert(t, metadata.FailureDetails, []failurechain.Frame{
				{
					ErrorType:       api.ErrorTypeTaskFailed,
					MessageContains: []string{"Task 'MissingChild' (#0) failed with an unhandled exception"},
					NonRetriable:    true,
				},
				{
					ErrorType:       api.ErrorTypeTaskFailed,
					MessageContains: test.leafParts,
					NonRetriable:    true,
				},
				{
					ErrorType:       test.leafType,
					MessageContains: test.leafParts,
					NonRetriable:    true,
				},
			})
			require.True(t, metadata.FailureDetails.Matches(api.ErrTaskNotRegistered))
			require.True(t, metadata.FailureDetails.IsCausedBy(test.leafType))
		})
	}
}

// Test_FailureChain_NonRetriableBypassesRetryHandlers asserts a non-retriable
// durable failure short-circuits both the retry schedule and the user supplied
// retry handler, while a retriable failure exhausts the policy.
func Test_FailureChain_NonRetriableBypassesRetryHandlers(t *testing.T) {
	const maxAttempts = 3
	for _, test := range []struct {
		name              string
		nonRetriable      bool
		useActivity       bool
		wantAttempts      int32
		wantHandlerCalled bool
	}{
		{name: "activity-non-retriable", nonRetriable: true, useActivity: true, wantAttempts: 1},
		{name: "activity-retriable", useActivity: true, wantAttempts: maxAttempts, wantHandlerCalled: true},
		{name: "sub-orchestration-non-retriable", nonRetriable: true, wantAttempts: 1},
		{name: "sub-orchestration-retriable", wantAttempts: maxAttempts, wantHandlerCalled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var attempts atomic.Int32
			var handlerRuns atomic.Int32
			var handlerSawNonRetriable atomic.Bool
			leafError := func() error {
				if test.nonRetriable {
					return &failurechain.LeafError{
						Message:        "retry me not",
						ErrorType:      "Contoso.FatalError",
						IsNonRetriable: true,
					}
				}
				return errors.New("retry me")
			}
			retryPolicy := &task.RetryPolicy{
				MaxAttempts:          maxAttempts,
				InitialRetryInterval: 10 * time.Millisecond,
				BackoffCoefficient:   1,
				Handle: func(retryCtx task.RetryContext) bool {
					handlerRuns.Add(1)
					if retryCtx.LastFailure.NonRetriable() {
						handlerSawNonRetriable.Store(true)
					}
					return true
				},
			}

			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddActivityN("RetryLeaf", func(task.ActivityContext) (any, error) {
				attempts.Add(1)
				return nil, leafError()
			}))
			require.NoError(t, registry.AddOrchestratorN("RetryChild", func(*task.OrchestrationContext) (any, error) {
				attempts.Add(1)
				return nil, leafError()
			}))
			require.NoError(t, registry.AddOrchestratorN("RetryParent", func(ctx *task.OrchestrationContext) (any, error) {
				if test.useActivity {
					return nil, ctx.CallActivity("RetryLeaf", task.WithActivityRetryPolicy(retryPolicy)).Await(nil)
				}
				return nil, ctx.CallSubOrchestrator(
					"RetryChild",
					task.WithSubOrchestrationRetryPolicy(retryPolicy),
				).Await(nil)
			}))

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, worker := initTaskHubWorker(ctx, registry)
			defer func() {
				if err := worker.Shutdown(ctx); err != nil {
					t.Logf("shutdown: %v", err)
				}
			}()

			id, err := client.ScheduleNewOrchestration(ctx, "RetryParent")
			require.NoError(t, err)
			metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
			require.Equal(t, test.wantAttempts, attempts.Load())
			// Retry handlers are orchestrator logic, so replay may re-run them.
			// The parity contract is only that a non-retriable failure never
			// reaches the handler at all.
			require.Equal(t, test.wantHandlerCalled, handlerRuns.Load() > 0)
			require.False(t, handlerSawNonRetriable.Load())
			require.Equal(t, test.nonRetriable, metadata.FailureDetails.IsNonRetriable)
			if test.nonRetriable {
				require.True(t, metadata.FailureDetails.IsCausedBy("Contoso.FatalError"))
			}
		})
	}
}

// Test_FailureChain_PanicsCaptureStackTraces asserts panic failures surface the
// canonical panic error types with a real Go stack trace, and that wrapper
// frames above them do not duplicate the stack.
func Test_FailureChain_PanicsCaptureStackTraces(t *testing.T) {
	for _, test := range []struct {
		name        string
		useActivity bool
		frames      []failurechain.Frame
	}{
		{
			name:        "activity-panic",
			useActivity: true,
			frames: []failurechain.Frame{
				{
					ErrorType: api.ErrorTypeTaskFailed,
					MessageContains: []string{
						"Task 'PanicLeaf' (#0) failed with an unhandled exception",
						"activity exploded",
					},
				},
				{
					ErrorType:       api.ErrorTypeActivityPanic,
					MessageContains: []string{"panic: activity exploded"},
					StackContains:   "durabletask-go/task",
					ExpectStack:     true,
				},
				{
					ErrorType:       "*errors.errorString",
					MessageContains: []string{"activity exploded"},
				},
			},
		},
		{
			name: "orchestrator-panic",
			frames: []failurechain.Frame{
				{
					ErrorType:       api.ErrorTypeOrchestratorPanic,
					MessageContains: []string{"panicked", "orchestrator exploded"},
					StackContains:   "durabletask-go/task",
					ExpectStack:     true,
				},
				{
					ErrorType:       "*errors.errorString",
					MessageContains: []string{"orchestrator exploded"},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddActivityN("PanicLeaf", func(task.ActivityContext) (any, error) {
				panic(errors.New("activity exploded"))
			}))
			useActivity := test.useActivity
			require.NoError(t, registry.AddOrchestratorN("PanicHost", func(ctx *task.OrchestrationContext) (any, error) {
				if useActivity {
					return nil, ctx.CallActivity("PanicLeaf").Await(nil)
				}
				panic(errors.New("orchestrator exploded"))
			}))

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, worker := initTaskHubWorker(ctx, registry)
			defer func() {
				if err := worker.Shutdown(ctx); err != nil {
					t.Logf("shutdown: %v", err)
				}
			}()

			id, err := client.ScheduleNewOrchestration(ctx, "PanicHost")
			require.NoError(t, err)
			metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
			failurechain.Assert(t, metadata.FailureDetails, test.frames)
		})
	}
}

// Test_FailureChain_ErrorPropertiesProviderEnrichesEveryFrame asserts a worker
// level error properties provider attaches custom properties to each frame it
// produces without corrupting the inner failure chain.
func Test_FailureChain_ErrorPropertiesProviderEnrichesEveryFrame(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("PropertyLeaf", func(task.ActivityContext) (any, error) {
		return nil, errors.New("property leaf failure")
	}))
	require.NoError(t, registry.AddOrchestratorN("PropertyChild", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("PropertyLeaf").Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorN("PropertyParent", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallSubOrchestrator(
			"PropertyChild",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child"),
		).Await(nil)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, worker := initTaskHubWorkerWithExecutorOptions(ctx, registry, []task.TaskExecutorOption{
		task.WithErrorPropertiesProvider(api.ErrorPropertiesProviderFunc(func(err error) map[string]any {
			return map[string]any{"observed": err.Error()}
		})),
	})
	defer func() {
		if err := worker.Shutdown(ctx); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()

	id, err := client.ScheduleNewOrchestration(ctx, "PropertyParent")
	require.NoError(t, err)
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, metadata.RuntimeStatus)
	failurechain.Assert(t, metadata.FailureDetails, []failurechain.Frame{
		{
			ErrorType:       api.ErrorTypeTaskFailed,
			MessageContains: []string{"Task 'PropertyChild' (#0) failed with an unhandled exception"},
			Properties: map[string]any{
				"observed": "Task 'PropertyChild' (#0) failed with an unhandled exception: " +
					"Task 'PropertyLeaf' (#0) failed with an unhandled exception: property leaf failure",
			},
		},
		{
			ErrorType:       api.ErrorTypeTaskFailed,
			MessageContains: []string{"Task 'PropertyLeaf' (#0) failed with an unhandled exception"},
			Properties: map[string]any{
				"observed": "Task 'PropertyLeaf' (#0) failed with an unhandled exception: property leaf failure",
			},
		},
		{
			ErrorType:       "*errors.errorString",
			MessageContains: []string{"property leaf failure"},
			Properties:      map[string]any{"observed": "property leaf failure"},
		},
	})
}

// Test_FailureChain_HistoryRecordsEveryFrame asserts the persisted history keeps
// the same failure chain the client observes, so replays and history exports
// stay consistent with orchestration metadata.
func Test_FailureChain_HistoryRecordsEveryFrame(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("HistoryLeaf", func(task.ActivityContext) (any, error) {
		return nil, &failurechain.LeafError{
			Message:        "history leaf failure",
			ErrorType:      "Contoso.HistoryError",
			IsNonRetriable: true,
		}
	}))
	require.NoError(t, registry.AddOrchestratorN("HistoryChild", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("HistoryLeaf").Await(nil)
	}))
	require.NoError(t, registry.AddOrchestratorN("HistoryParent", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallSubOrchestrator(
			"HistoryChild",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_child"),
		).Await(nil)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, _ := initManagementWorker(ctx, t, registry)

	id, err := client.ScheduleNewOrchestration(ctx, "HistoryParent")
	require.NoError(t, err)
	_, err = client.WaitForOrchestrationCompletion(ctx, id)
	require.NoError(t, err)

	childHistory, err := client.GetOrchestrationHistory(ctx, id+"_child", api.HistoryQuery{})
	require.NoError(t, err)
	var taskFailed *api.FailureDetails
	for _, event := range childHistory.Events {
		if event.TaskFailed != nil {
			taskFailed = event.TaskFailed.FailureDetails
		}
	}
	failurechain.Assert(t, taskFailed, []failurechain.Frame{{
		ErrorType:       "Contoso.HistoryError",
		MessageContains: []string{"history leaf failure"},
		NonRetriable:    true,
	}})

	parentHistory, err := client.GetOrchestrationHistory(ctx, id, api.HistoryQuery{})
	require.NoError(t, err)
	var subFailed *api.FailureDetails
	for _, event := range parentHistory.Events {
		if event.SubOrchestrationInstanceFailed != nil {
			subFailed = event.SubOrchestrationInstanceFailed.FailureDetails
		}
	}
	failurechain.Assert(t, subFailed, []failurechain.Frame{
		{
			ErrorType:       api.ErrorTypeTaskFailed,
			MessageContains: []string{"Task 'HistoryLeaf' (#0) failed with an unhandled exception"},
			NonRetriable:    true,
		},
		{
			ErrorType:       "Contoso.HistoryError",
			MessageContains: []string{"history leaf failure"},
			NonRetriable:    true,
		},
	})
}
