package task

import (
	"context"
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestHistoryLimitFailsWithNonRetriableFailureByDefault(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("limited", func(*OrchestrationContext) (any, error) {
		return "done", nil
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
	if err := registry.AddOrchestratorN("limited", func(*OrchestrationContext) (any, error) {
		return nil, nil
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

	var metric backend.HistoryMetric
	instanceID := api.InstanceID("history-metric")
	_, err := NewTaskExecutor(
		registry,
		WithMetricsHooks(backend.MetricsHooks{
			History: func(value backend.HistoryMetric) {
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
