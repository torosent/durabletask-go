package task

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRetryOptionsCloneAndNormalizeCallerPolicy(t *testing.T) {
	caller := &RetryPolicy{InitialRetryInterval: time.Second}
	activityOption := WithActivityRetryPolicy(caller)
	subOrchestrationOption := WithSubOrchestrationRetryPolicy(caller)

	// Mutation after option construction must not affect either captured policy.
	caller.InitialRetryInterval = time.Hour
	caller.MaxAttempts = 99
	activity := new(callActivityOptions)
	if err := activityOption(activity, api.DefaultDataConverter()); err != nil {
		t.Fatal(err)
	}
	subOrchestration := new(callSubOrchestratorOptions)
	if err := subOrchestrationOption(subOrchestration, api.DefaultDataConverter()); err != nil {
		t.Fatal(err)
	}
	for name, policy := range map[string]*RetryPolicy{
		"activity":          activity.retryPolicy,
		"sub-orchestration": subOrchestration.retryPolicy,
	} {
		if policy == caller {
			t.Fatalf("%s option retained caller-owned policy", name)
		}
		if policy.InitialRetryInterval != time.Second || policy.MaxAttempts != 1 ||
			policy.BackoffCoefficient != 1 || policy.MaxRetryInterval != math.MaxInt64 ||
			policy.RetryTimeout != math.MaxInt64 || policy.Handle == nil {
			t.Fatalf("%s policy was not independently normalized: %+v", name, policy)
		}
	}
	if activity.retryPolicy == subOrchestration.retryPolicy {
		t.Fatal("activity and sub-orchestration options share a mutable policy copy")
	}
}

func TestRetryPolicyValidateDoesNotMutateReceiver(t *testing.T) {
	policy := &RetryPolicy{InitialRetryInterval: time.Second}
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	if policy.MaxAttempts != 0 || policy.BackoffCoefficient != 0 ||
		policy.MaxRetryInterval != 0 || policy.RetryTimeout != 0 || policy.Handle != nil {
		t.Fatalf("Validate mutated its receiver: %+v", policy)
	}
}

func TestComputeNextDelayDoesNotCrossRetryDeadline(t *testing.T) {
	firstAttempt := time.Unix(0, 0).UTC()
	failure := &TaskFailedError{
		TaskName:       "activity",
		FailureDetails: &api.FailureDetails{ErrorType: "TestError", ErrorMessage: "failed"},
	}
	policy := RetryPolicy{
		MaxAttempts:          3,
		InitialRetryInterval: 20 * time.Second,
		BackoffCoefficient:   1,
		MaxRetryInterval:     time.Minute,
		RetryTimeout:         time.Minute,
		Handle:               func(RetryContext) bool { return true },
	}

	if delay := computeNextDelay(firstAttempt.Add(50*time.Second), policy, 0, firstAttempt, failure); delay != 0 {
		t.Fatalf("delay %v crosses retry deadline", delay)
	}
	policy.InitialRetryInterval = 10 * time.Second
	if delay := computeNextDelay(firstAttempt.Add(50*time.Second), policy, 0, firstAttempt, failure); delay != 10*time.Second {
		t.Fatalf("delay at retry deadline = %v, want 10s", delay)
	}
	if delay := computeNextDelay(firstAttempt.Add(time.Minute), policy, 0, firstAttempt, failure); delay != 0 {
		t.Fatalf("delay at expired retry deadline = %v, want 0", delay)
	}
}

func TestRetryOptionDoesNotReadCallerPolicyAfterConstruction(t *testing.T) {
	caller := &RetryPolicy{InitialRetryInterval: time.Second, MaxAttempts: 3}
	option := WithActivityRetryPolicy(caller)

	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 1; i <= 10_000; i++ {
			caller.InitialRetryInterval = time.Duration(i) * time.Millisecond
			caller.MaxAttempts = i
		}
	}()
	for range 1_000 {
		configured := new(callActivityOptions)
		if err := option(configured, api.DefaultDataConverter()); err != nil {
			t.Fatal(err)
		}
		if configured.retryPolicy.InitialRetryInterval != time.Second || configured.retryPolicy.MaxAttempts != 3 {
			t.Fatalf("captured policy drifted: %+v", configured.retryPolicy)
		}
	}
	writers.Wait()
}

func TestRetryDoesNotStartAfterDelayedTimerPassesDeadline(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("late-retry", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: 10 * time.Second,
			RetryTimeout:         time.Minute,
		})).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	firstAttempt := time.Unix(1_700_000_000, 0).UTC()
	fireAt := timestamppb.New(firstAttempt.Add(10 * time.Second))
	startedTurn := helpers.NewOrchestratorStartedEvent()
	startedTurn.Timestamp = timestamppb.New(firstAttempt)
	lateTurn := helpers.NewOrchestratorStartedEvent()
	lateTurn.Timestamp = timestamppb.New(firstAttempt.Add(2 * time.Minute))
	instanceID := api.InstanceID("late-retry-instance")
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		[]*protos.HistoryEvent{
			startedTurn,
			helpers.NewExecutionStartedEvent("late-retry", string(instanceID), nil, nil, nil, nil),
			helpers.NewTaskScheduledEvent(0, "flaky", nil, nil, nil),
			helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
				ErrorType:    "TransientFailure",
				ErrorMessage: "retry me",
			}),
			helpers.NewTimerCreatedEvent(1, fireAt),
		},
		[]*protos.HistoryEvent{
			lateTurn,
			helpers.NewTimerFiredEvent(1, fireAt, nil),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range result.Response.Actions {
		if scheduled := action.GetScheduleTask(); scheduled != nil {
			t.Fatalf("late retry scheduled activity %q after RetryTimeout", scheduled.GetName())
		}
	}
	completed := completionAction(t, result.Response)
	if completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		t.Fatalf("orchestration status = %v, want FAILED", completed.GetOrchestrationStatus())
	}
}

func TestRetryReplaysLateAttemptRecordedBeforeDeadlineFix(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("late-retry-replay", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: 10 * time.Second,
			RetryTimeout:         time.Minute,
		})).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	firstAttempt := time.Unix(1_700_000_000, 0).UTC()
	fireAt := timestamppb.New(firstAttempt.Add(10 * time.Second))
	startedTurn := helpers.NewOrchestratorStartedEvent()
	startedTurn.Timestamp = timestamppb.New(firstAttempt)
	lateTurn := helpers.NewOrchestratorStartedEvent()
	lateTurn.Timestamp = timestamppb.New(firstAttempt.Add(2 * time.Minute))
	instanceID := api.InstanceID("late-retry-replay-instance")
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		[]*protos.HistoryEvent{
			startedTurn,
			helpers.NewExecutionStartedEvent("late-retry-replay", string(instanceID), nil, nil, nil, nil),
			helpers.NewTaskScheduledEvent(0, "flaky", nil, nil, nil),
			helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
				ErrorType:    "TransientFailure",
				ErrorMessage: "retry me",
			}),
			helpers.NewTimerCreatedEvent(1, fireAt),
			lateTurn,
			helpers.NewTimerFiredEvent(1, fireAt, nil),
			helpers.NewTaskScheduledEvent(2, "flaky", nil, nil, nil),
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Response.Actions) != 0 {
		t.Fatalf("replay emitted actions for recorded late retry: %v", result.Response.Actions)
	}
}
