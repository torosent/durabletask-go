package task

import (
	"context"
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestHistoryLimitFailsWithNonRetriableFailureByDefault(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("limited", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("history-limit")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{MaxHistoryEvents: 1}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("limited", string(instanceID), nil, nil, nil, nil),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	completed := completionAction(t, result.Response)
	if got := completed.GetOrchestrationStatus(); got != protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		t.Fatalf("status = %v, want FAILED", got)
	}
	failure := completed.GetFailureDetails()
	if failure.GetErrorType() != "HistoryLimitExceeded" {
		t.Fatalf("failure type = %q, want HistoryLimitExceeded", failure.GetErrorType())
	}
	if !failure.GetIsNonRetriable() {
		t.Fatal("history limit failure must be non-retriable")
	}
}

func TestHistoryLimitHandlerContinuesAsNewAndPreservesExternalEvents(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("limited", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	handlerCalls := 0
	instanceID := api.InstanceID("history-handler")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{
			MaxEventsPerTurn: 2,
			OnHistoryLimitExceeded: func(info HistoryLimitInfo) (any, error) {
				handlerCalls++
				if !info.MaxEventsPerTurnExceeded {
					t.Fatal("expected per-turn history limit to be exceeded")
				}
				var input int
				if err := info.GetInput(&input); err != nil {
					return nil, err
				}
				return input + 1, nil
			},
		}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("limited", string(instanceID), wrapperspb.String("7"), nil, nil, nil),
			helpers.NewEventRaisedEvent("first", wrapperspb.String("1")),
			helpers.NewEventRaisedEvent("second", wrapperspb.String("2")),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}

	completed := completionAction(t, result.Response)
	if got := completed.GetOrchestrationStatus(); got != protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
		t.Fatalf("status = %v, want CONTINUED_AS_NEW", got)
	}
	if got := completed.GetResult().GetValue(); got != "8" {
		t.Fatalf("continue-as-new input = %q, want 8", got)
	}
	if len(completed.GetCarryoverEvents()) != 2 {
		t.Fatalf("carryover count = %d, want 2", len(completed.GetCarryoverEvents()))
	}
	if got := completed.GetCarryoverEvents()[0].GetEventRaised().GetName(); got != "first" {
		t.Fatalf("first carryover event = %q, want first", got)
	}
	if got := completed.GetCarryoverEvents()[1].GetEventRaised().GetName(); got != "second" {
		t.Fatalf("second carryover event = %q, want second", got)
	}
}

func TestOrchestratorCanHandleHistoryLimitExplicitly(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("self-managed", func(ctx *OrchestrationContext) (any, error) {
		if !ctx.HistoryLimitExceeded() {
			return nil, errors.New("expected history limit to be visible")
		}
		if ctx.HistoryLength() != 2 {
			return nil, errors.New("unexpected history length")
		}
		ctx.ContinueAsNew("checkpoint", WithKeepUnprocessedEvents())
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("self-managed-history")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{MaxHistoryEvents: 1}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("self-managed", string(instanceID), nil, nil, nil, nil),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	completed := completionAction(t, result.Response)
	if got := completed.GetOrchestrationStatus(); got != protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
		t.Fatalf("status = %v, want CONTINUED_AS_NEW", got)
	}
	if got := completed.GetResult().GetValue(); got != `"checkpoint"` {
		t.Fatalf("continue-as-new input = %q, want checkpoint", got)
	}
}

func TestHistoryLimitHandlerFailureUsesWellKnownFailure(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("limited", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("history-handler-failure")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{
			MaxHistoryEvents: 1,
			OnHistoryLimitExceeded: func(HistoryLimitInfo) (any, error) {
				return nil, errors.New("checkpoint failed")
			},
		}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("limited", string(instanceID), nil, nil, nil, nil),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	failure := completionAction(t, result.Response).GetFailureDetails()
	if failure.GetErrorType() != "HistoryLimitExceeded" || !failure.GetIsNonRetriable() {
		t.Fatalf("unexpected failure details: %v", failure)
	}
	if !errors.Is(&HistoryLimitError{}, ErrHistoryLimitExceeded) {
		t.Fatal("HistoryLimitError must unwrap to ErrHistoryLimitExceeded")
	}
}

func TestHistoryMetricReportsTurnUsage(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("metrics", func(*OrchestrationContext) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	var metric HistoryMetric
	instanceID := api.InstanceID("history-metric")
	_, err := NewTaskExecutor(
		registry,
		WithMetricsHooks(MetricsHooks{
			History: func(value HistoryMetric) {
				metric = value
			},
		}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("metrics", string(instanceID), nil, nil, nil, nil),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if metric.InstanceID != instanceID || metric.HistoryLength != 2 || metric.ProcessedEvents != 1 {
		t.Fatalf("unexpected history metric: %+v", metric)
	}
}

func TestHistoryLimitDoesNotOverrideCompletedOutcome(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("complete-before-limit", func(ctx *OrchestrationContext) (any, error) {
		var value string
		if err := ctx.WaitForSingleEvent("go", -1).Await(&value); err != nil {
			return nil, err
		}

		return value, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("complete-before-limit")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{MaxEventsPerTurn: 2}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("complete-before-limit", string(instanceID), nil, nil, nil, nil),
			helpers.NewEventRaisedEvent("go", wrapperspb.String(`"done"`)),
			helpers.NewEventRaisedEvent("extra", wrapperspb.String("1")),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	completed := completionAction(t, result.Response)
	if completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
		t.Fatalf("status = %v, want COMPLETED", completed.GetOrchestrationStatus())
	}
	if completed.GetResult().GetValue() != `"done"` {
		t.Fatalf("result = %q", completed.GetResult().GetValue())
	}
}

func TestMaxHistoryLimitDoesNotOverrideCompletedOutcome(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("complete-history-limit", func(*OrchestrationContext) (any, error) {
		return "done", nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("complete-history-limit")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{
			MaxHistoryEvents: 1,
			OnHistoryLimitExceeded: func(HistoryLimitInfo) (any, error) {
				return "should-not-run", nil
			},
		}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("complete-history-limit", string(instanceID), nil, nil, nil, nil),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	completed := completionAction(t, result.Response)
	if completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED ||
		completed.GetResult().GetValue() != `"done"` {
		t.Fatalf("unexpected completion: %v", completed)
	}
}

func TestHistoryLimitDoesNotOverrideTermination(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("terminate-before-limit", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("terminate-before-limit")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{
			MaxEventsPerTurn: 2,
			OnHistoryLimitExceeded: func(HistoryLimitInfo) (any, error) {
				return "should-not-run", nil
			},
		}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("terminate-before-limit", string(instanceID), nil, nil, nil, nil),
			helpers.NewEventRaisedEvent("extra", wrapperspb.String("1")),
			helpers.NewExecutionTerminatedEvent(wrapperspb.String(`"killed"`), false),
			helpers.NewEventRaisedEvent("after", wrapperspb.String("2")),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	completed := completionAction(t, result.Response)
	if completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED {
		t.Fatalf("status = %v, want TERMINATED", completed.GetOrchestrationStatus())
	}
}

func TestContinueAsNewCarryoverIncludesUndrainedResumedEvents(t *testing.T) {
	resumed := helpers.NewEventRaisedEvent("resumed", wrapperspb.String("1"))
	tail := helpers.NewEventRaisedEvent("tail", wrapperspb.String("2"))
	ctx := newTestOrchestrationContext(NewTaskRegistry(), "carryover-resumed", nil, []*protos.HistoryEvent{tail})
	ctx.resumedEvents = []replayEvent{{event: resumed}}
	events := ctx.unprocessedExternalEvents()
	if len(events) != 2 || events[0] != resumed || events[1] != tail {
		t.Fatalf("unexpected carryover: %v", events)
	}
}

func TestContinueAsNewCarryoverIncludesSuspendedEvents(t *testing.T) {
	suspended := helpers.NewEventRaisedEvent("suspended", wrapperspb.String("1"))
	tail := helpers.NewEventRaisedEvent("tail", wrapperspb.String("2"))
	ctx := newTestOrchestrationContext(NewTaskRegistry(), "carryover-suspended", nil, []*protos.HistoryEvent{tail})
	ctx.suspendedEvents = []replayEvent{{event: suspended}}
	events := ctx.unprocessedExternalEvents()
	if len(events) != 2 || events[0] != suspended || events[1] != tail {
		t.Fatalf("unexpected carryover: %v", events)
	}
}

func TestMaxHistoryLimitDoesNotOverrideTermination(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("terminated", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("history-terminated")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{MaxHistoryEvents: 1}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("terminated", string(instanceID), nil, nil, nil, nil),
			helpers.NewExecutionTerminatedEvent(wrapperspb.String("stop"), false),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := completionAction(t, result.Response).GetOrchestrationStatus(); got != protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED {
		t.Fatalf("status = %v, want TERMINATED", got)
	}
}

func TestHistoryLimitHandlerFailsWhenCarryoverCannotFit(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("fixed-point", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("never", -1).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("history-fixed-point")
	result, err := NewTaskExecutor(
		registry,
		WithOrchestrationOptions(OrchestrationOptions{
			MaxHistoryEvents: 3,
			OnHistoryLimitExceeded: func(info HistoryLimitInfo) (any, error) {
				if info.UnprocessedEventCount != 2 {
					t.Fatalf("unprocessed event count = %d, want 2", info.UnprocessedEventCount)
				}
				return "checkpoint", nil
			},
		}),
	).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("fixed-point", string(instanceID), nil, nil, nil, nil),
			helpers.NewEventRaisedEvent("one", wrapperspb.String("1")),
			helpers.NewEventRaisedEvent("two", wrapperspb.String("2")),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	completed := completionAction(t, result.Response)
	if completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED ||
		completed.GetFailureDetails().GetErrorType() != "HistoryLimitExceeded" {
		t.Fatalf("unexpected completion: %v", completed)
	}
}
