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

// TestRetryDoesNotScheduleTimerPastRetryTimeout proves the retry loop stops
// instead of creating a durable timer whose delay would carry the next attempt
// past RetryTimeout. Bounding the timer at creation keeps the decision on the
// failure event, where every input is replayed from history.
func TestRetryDoesNotScheduleTimerPastRetryTimeout(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("bounded-retry", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Second,
			RetryTimeout:         time.Minute,
		})).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	firstAttempt := time.Unix(1_700_000_000, 0).UTC()
	startedTurn := helpers.NewOrchestratorStartedEvent()
	startedTurn.Timestamp = timestamppb.New(firstAttempt)
	// Only five seconds of the retry budget remain, so the ten-second backoff
	// would fire after the deadline.
	failureTurn := helpers.NewOrchestratorStartedEvent()
	failureTurn.Timestamp = timestamppb.New(firstAttempt.Add(55 * time.Second))
	instanceID := api.InstanceID("bounded-retry-instance")
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		[]*protos.HistoryEvent{
			startedTurn,
			helpers.NewExecutionStartedEvent("bounded-retry", string(instanceID), nil, nil, nil, nil),
			helpers.NewTaskScheduledEvent(0, "flaky", nil, nil, nil),
		},
		[]*protos.HistoryEvent{
			failureTurn,
			helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
				ErrorType:    "TransientFailure",
				ErrorMessage: "retry me",
			}),
		}, nil)

	if err != nil {
		t.Fatal(err)
	}
	for _, action := range result.Response.Actions {
		if action.GetCreateTimer() != nil {
			t.Fatal("retry timer scheduled past RetryTimeout")
		}
		if scheduled := action.GetScheduleTask(); scheduled != nil {
			t.Fatalf("retry scheduled activity %q past RetryTimeout", scheduled.GetName())
		}
	}
	if completed := completionAction(t, result.Response); completed.GetOrchestrationStatus() !=
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		t.Fatalf("orchestration status = %v, want FAILED", completed.GetOrchestrationStatus())
	}
}

// TestRetryDecisionIsStableWhenCompletionIsRedelivered pins the replay contract
// the retry loop depends on: when a completion response is lost and DTS
// redelivers the work item, the same events must produce the same retry
// actions even though they arrive as replayed history.
func TestRetryDecisionIsStableWhenCompletionIsRedelivered(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("stable-retry", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Second,
			RetryTimeout:         time.Minute,
		})).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	firstAttempt := time.Unix(1_700_000_000, 0).UTC()
	failureTurn := helpers.NewOrchestratorStartedEvent()
	failureTurn.Timestamp = timestamppb.New(firstAttempt.Add(5 * time.Second))
	startedTurn := helpers.NewOrchestratorStartedEvent()
	startedTurn.Timestamp = timestamppb.New(firstAttempt)
	committed := []*protos.HistoryEvent{
		startedTurn,
		helpers.NewExecutionStartedEvent("stable-retry", string(instanceIDStableRetry), nil, nil, nil, nil),
		helpers.NewTaskScheduledEvent(0, "flaky", nil, nil, nil),
	}
	delivered := []*protos.HistoryEvent{
		failureTurn,
		helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
			ErrorType:    "TransientFailure",
			ErrorMessage: "retry me",
		}),
	}
	executor := NewTaskExecutor(registry)

	// First delivery: the failure arrives as a new event.
	first, err := executor.ExecuteOrchestrator(
		context.Background(), instanceIDStableRetry, committed, delivered, nil)

	if err != nil {
		t.Fatal(err)
	}
	// Redelivery after a lost response: the identical failure is now replayed
	// history, so IsReplaying flips while the decision must not.
	second, err := executor.ExecuteOrchestrator(
		context.Background(), instanceIDStableRetry, append(committed, delivered...), nil, nil)

	if err != nil {
		t.Fatal(err)
	}

	firstTimer := singleRetryTimer(t, first.Response)
	secondTimer := singleRetryTimer(t, second.Response)
	if !firstTimer.GetFireAt().AsTime().Equal(secondTimer.GetFireAt().AsTime()) {
		t.Fatalf("retry timer moved across redelivery: %v then %v",
			firstTimer.GetFireAt().AsTime(), secondTimer.GetFireAt().AsTime())
	}
}

const instanceIDStableRetry = api.InstanceID("stable-retry-instance")

func singleRetryTimer(t *testing.T, response *protos.OrchestratorResponse) *protos.CreateTimerAction {
	t.Helper()
	var timer *protos.CreateTimerAction
	for _, action := range response.GetActions() {
		if created := action.GetCreateTimer(); created != nil {
			if timer != nil {
				t.Fatal("more than one retry timer action")
			}
			timer = created
		}
	}
	if timer == nil {
		t.Fatalf("no retry timer action in %v", response.GetActions())
	}
	return timer
}
