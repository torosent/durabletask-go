package task

import (
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCancelScopeCancelsTaskButKeepsScheduledAction(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-task", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		timer := child.CreateTimer(time.Hour)
		cancel()
		return errors.Is(timer.Await(nil), ErrTaskCanceled), nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("cancel-task-instance")
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("cancel-task", string(instanceID), nil, nil, nil, nil),
		},
	)
	if len(firstTurn.Actions) != 2 {
		t.Fatalf("first-turn action count = %d, want timer and completion", len(firstTurn.Actions))
	}
	if firstTurn.Actions[0].GetCreateTimer() == nil {
		t.Fatal("cancel removed an already-scheduled timer action")
	}
	if got, want := completionResult(t, firstTurn), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}

	oldEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("cancel-task", string(instanceID), nil, nil, nil, nil),
		helpers.NewTimerCreatedEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt()),
	}
	secondTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		oldEvents,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewTimerFiredEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt(), nil),
		},
	)
	if len(secondTurn.Actions) != 1 || secondTurn.Actions[0].GetCompleteOrchestration() == nil {
		t.Fatalf("late timer completion produced unexpected actions: %v", secondTurn.Actions)
	}
}

func TestCancelScopeCancelsNestedScopes(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("nested-cancel", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		grandchild, _ := child.WithCancel()
		timer := grandchild.CreateTimer(time.Hour)
		cancel()
		return errors.Is(timer.Await(nil), ErrTaskCanceled), nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("nested-cancel-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("nested-cancel", string(instanceID), nil, nil, nil, nil),
		},
	)
	if got, want := completionResult(t, result), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCancelScopeUnblocksChildCoroutine(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-coroutine", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		wg := ctx.NewWaitGroup()
		wg.Add(2)
		canceled := false

		child.Go(func(child *OrchestrationContext) {
			defer wg.Done()
			canceled = errors.Is(child.CreateTimer(time.Hour).Await(nil), ErrTaskCanceled)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer wg.Done()
			if err := ctx.CreateTimer(time.Second).Await(nil); err != nil {
				panic(err)
			}
			cancel()
		})

		wg.Wait(ctx)
		return canceled, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("cancel-coroutine-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("cancel-coroutine", string(instanceID), nil, nil, nil, nil)
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	if len(firstTurn.Actions) != 2 {
		t.Fatalf("first-turn action count = %d, want two timers", len(firstTurn.Actions))
	}

	shortTimer := firstTurn.Actions[1].GetCreateTimer()
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			started,
			executionStarted,
			helpers.NewTimerCreatedEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt()),
			helpers.NewTimerCreatedEvent(1, shortTimer.GetFireAt()),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewTimerFiredEvent(1, shortTimer.GetFireAt(), nil),
		},
	)
	if got, want := completionResult(t, result), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCancelScopeCompletionOrderIsDeterministic(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-order", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		first := child.CreateTimer(time.Hour)
		second := child.CreateTimer(2 * time.Hour)
		cancel()
		if ctx.WhenAny(first, second) == first {
			return "first", nil
		}
		return "second", nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("cancel-order-instance")
	events := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("cancel-order", string(instanceID), nil, nil, nil, nil),
	}
	for i := 0; i < 200; i++ {
		result := executeOrchestrationTurn(t, registry, instanceID, nil, events)
		if got, want := completionResult(t, result), `"first"`; got != want {
			t.Fatalf("iteration %d result = %s, want %s", i, got, want)
		}
	}
}

func TestCanceledChildWaitingOnRootWaitGroupUnwinds(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-waitgroup", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		blocked := ctx.NewWaitGroup()
		blocked.Add(1)
		completed := ctx.NewWaitGroup()
		completed.Add(2)

		child.Go(func(child *OrchestrationContext) {
			defer completed.Done()
			blocked.Wait(child)
		})
		ctx.Go(func(*OrchestrationContext) {
			defer completed.Done()
			cancel()
		})
		completed.Wait(ctx)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("cancel-waitgroup-instance")
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("cancel-waitgroup", string(instanceID), nil, nil, nil, nil),
		},
	)
	if got, want := completionResult(t, result), "true"; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCanceledScopeDoesNotConsumeBufferedEvent(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-buffer", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		completed := ctx.NewWaitGroup()
		completed.Add(2)
		childCanceled := false

		child.Go(func(child *OrchestrationContext) {
			defer completed.Done()
			_ = child.CreateTimer(time.Hour).Await(nil)
			var ignored string
			childCanceled = errors.Is(
				child.WaitForSingleEvent("payload", -1).Await(&ignored),
				ErrTaskCanceled,
			)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer completed.Done()
			if err := ctx.CreateTimer(time.Second).Await(nil); err != nil {
				panic(err)
			}
			cancel()
		})

		completed.Wait(ctx)
		var payload string
		if err := ctx.WaitForSingleEvent("payload", 0).Await(&payload); err != nil {
			return nil, err
		}
		return []any{childCanceled, payload}, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("cancel-buffer-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent("cancel-buffer", string(instanceID), nil, nil, nil, nil)
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	shortTimer := firstTurn.Actions[1].GetCreateTimer()
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			started,
			executionStarted,
			helpers.NewTimerCreatedEvent(0, firstTurn.Actions[0].GetCreateTimer().GetFireAt()),
			helpers.NewTimerCreatedEvent(1, shortTimer.GetFireAt()),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewEventRaisedEvent("payload", wrapperspb.String(`"value"`)),
			helpers.NewTimerFiredEvent(1, shortTimer.GetFireAt(), nil),
		},
	)
	if got, want := completionResult(t, result), `[true,"value"]`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}

func TestCanceledSelectRemovesEventSubscription(t *testing.T) {
	var captured *OrchestrationContext
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-select", func(ctx *OrchestrationContext) (any, error) {
		captured = ctx
		child, cancel := ctx.WithCancel()
		completed := ctx.NewWaitGroup()
		completed.Add(2)
		child.Go(func(child *OrchestrationContext) {
			defer completed.Done()
			child.Select(OnEvent(NewEventChannel[int](child, "event"), nil))
		})
		ctx.Go(func(*OrchestrationContext) {
			defer completed.Done()
			cancel()
		})
		completed.Wait(ctx)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("cancel-select-instance")
	executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("cancel-select", string(instanceID), nil, nil, nil, nil),
		},
	)
	if len(captured.eventWaiters) != 0 {
		t.Fatalf("event subscriptions remain after cancellation: %v", captured.eventWaiters)
	}
}

func TestCanceledPendingEventWaiterDoesNotConsumeEvent(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("cancel-pending-event", func(ctx *OrchestrationContext) (any, error) {
		child, cancel := ctx.WithCancel()
		completed := ctx.NewWaitGroup()
		completed.Add(2)

		child.Go(func(child *OrchestrationContext) {
			defer completed.Done()
			_ = child.WaitForSingleEvent("payload", -1).Await(nil)
		})
		ctx.Go(func(ctx *OrchestrationContext) {
			defer completed.Done()
			if err := ctx.CreateTimer(time.Second).Await(nil); err != nil {
				panic(err)
			}
			cancel()
		})

		completed.Wait(ctx)
		var payload string
		if err := ctx.WaitForSingleEvent("payload", -1).Await(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("cancel-pending-event-instance")
	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent(
		"cancel-pending-event",
		string(instanceID),
		nil,
		nil,
		nil,
		nil,
	)
	firstTurn := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		nil,
		[]*protos.HistoryEvent{started, executionStarted},
	)
	timer := firstTurn.Actions[0].GetCreateTimer()
	result := executeOrchestrationTurn(
		t,
		registry,
		instanceID,
		[]*protos.HistoryEvent{
			started,
			executionStarted,
			helpers.NewTimerCreatedEvent(0, timer.GetFireAt()),
		},
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewTimerFiredEvent(0, timer.GetFireAt(), nil),
			helpers.NewEventRaisedEvent("payload", wrapperspb.String(`"value"`)),
		},
	)
	if got, want := completionResult(t, result), `"value"`; got != want {
		t.Fatalf("result = %s, want %s", got, want)
	}
}
