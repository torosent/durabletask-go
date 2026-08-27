package task

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

// Orchestrator is the functional interface for orchestrator functions.
type Orchestrator func(ctx *OrchestrationContext) (any, error)

type replayEvent struct {
	event       *protos.HistoryEvent
	isReplaying bool
}

// OrchestrationContext is the parameter type for orchestrator functions.
type OrchestrationContext struct {
	ID             api.InstanceID
	Name           string
	Version        string
	IsReplaying    bool
	CurrentTimeUtc time.Time

	registry            *TaskRegistry
	rawInput            []byte
	oldEvents           []*protos.HistoryEvent
	newEvents           []*protos.HistoryEvent
	suspendedEvents     []replayEvent
	resumedEvents       []replayEvent
	isSuspended         bool
	isTerminated        bool
	historyIndex        int
	sequenceNumber      int32
	pendingActions      map[int32]*protos.OrchestratorAction
	pendingTasks        map[int32]*completableTask
	continuedAsNew      bool
	continuedAsNewInput any
	customStatus        string
	scheduler           *coroutineScheduler
	root                *OrchestrationContext
	scope               *cancellationScope
	derived             []*OrchestrationContext

	bufferedExternalEvents     map[string]*list.List
	pendingExternalEventTasks  map[string]*list.List
	eventChannels              map[string]any
	eventWaiters               map[string]map[*coroutine]struct{}
	saveBufferedExternalEvents bool
}

// callSubOrchestratorOptions is a struct that holds the options for the CallSubOrchestrator orchestrator method.
type callSubOrchestratorOptions struct {
	instanceID string
	rawInput   *wrapperspb.StringValue
	version    *wrapperspb.StringValue

	retryPolicy *RetryPolicy
}

// subOrchestratorOption is a functional option type for the CallSubOrchestrator orchestrator method.
type subOrchestratorOption func(*callSubOrchestratorOptions) error

// ContinueAsNewOption is a functional option type for the ContinueAsNew orchestrator method.
type ContinueAsNewOption func(*OrchestrationContext)

// WithKeepUnprocessedEvents returns a ContinueAsNewOptions struct that instructs the
// runtime to carry forward any unprocessed external events to the new instance.
func WithKeepUnprocessedEvents() ContinueAsNewOption {
	return func(ctx *OrchestrationContext) {
		ctx.saveBufferedExternalEvents = true
	}
}

// WithSubOrchestratorInput is a functional option type for the CallSubOrchestrator
// orchestrator method that takes an input value and marshals it to JSON.
func WithSubOrchestratorInput(input any) subOrchestratorOption {
	return func(opts *callSubOrchestratorOptions) error {
		bytes, err := marshalData(input)
		if err != nil {
			return fmt.Errorf("failed to marshal input to JSON: %w", err)
		}
		opts.rawInput = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawSubOrchestratorInput is a functional option type for the CallSubOrchestrator
// orchestrator method that takes a raw input value.
func WithRawSubOrchestratorInput(input string) subOrchestratorOption {
	return func(opts *callSubOrchestratorOptions) error {
		opts.rawInput = wrapperspb.String(input)
		return nil
	}
}

// WithSubOrchestrationInstanceID is a functional option type for the CallSubOrchestrator
// orchestrator method that specifies the instance ID of the sub-orchestration.
func WithSubOrchestrationInstanceID(instanceID string) subOrchestratorOption {
	return func(opts *callSubOrchestratorOptions) error {
		opts.instanceID = instanceID
		return nil
	}
}

// WithSubOrchestrationVersion configures the sub-orchestration version.
func WithSubOrchestrationVersion(version string) subOrchestratorOption {
	return func(opts *callSubOrchestratorOptions) error {
		opts.version = wrapperspb.String(version)
		return nil
	}
}

func WithSubOrchestrationRetryPolicy(policy *RetryPolicy) subOrchestratorOption {
	return func(opt *callSubOrchestratorOptions) error {
		if policy == nil {
			return nil
		}
		err := policy.Validate()
		if err != nil {
			return err
		}
		opt.retryPolicy = policy
		return nil
	}
}

// NewOrchestrationContext returns a new [OrchestrationContext] struct with the specified parameters.
func NewOrchestrationContext(registry *TaskRegistry, id api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) *OrchestrationContext {
	ctx := &OrchestrationContext{
		ID:                        id,
		registry:                  registry,
		oldEvents:                 oldEvents,
		newEvents:                 newEvents,
		bufferedExternalEvents:    make(map[string]*list.List),
		pendingExternalEventTasks: make(map[string]*list.List),
		eventChannels:             make(map[string]any),
		eventWaiters:              make(map[string]map[*coroutine]struct{}),
	}
	ctx.scope = newCancellationScope(nil)
	return ctx
}

func (ctx *OrchestrationContext) engineContext() *OrchestrationContext {
	if ctx.root != nil {
		return ctx.root
	}
	return ctx
}

func (ctx *OrchestrationContext) syncDerivedContexts() {
	for _, derived := range ctx.derived {
		derived.ID = ctx.ID
		derived.Name = ctx.Name
		derived.Version = ctx.Version
		derived.IsReplaying = ctx.IsReplaying
		derived.CurrentTimeUtc = ctx.CurrentTimeUtc
		derived.scheduler = ctx.scheduler
	}
}

// WithCancel creates a child orchestration context whose tasks, nested scopes,
// and coroutines are canceled together at the next scheduler step.
func (ctx *OrchestrationContext) WithCancel() (*OrchestrationContext, func()) {
	engine := ctx.engineContext()
	if engine.scheduler == nil {
		panic("cancellation scope created outside orchestrator execution")
	}
	child := &OrchestrationContext{
		ID:             engine.ID,
		Name:           engine.Name,
		Version:        engine.Version,
		IsReplaying:    engine.IsReplaying,
		CurrentTimeUtc: engine.CurrentTimeUtc,
		root:           engine,
		scope:          newCancellationScope(ctx.scope),
		scheduler:      engine.scheduler,
	}
	engine.derived = append(engine.derived, child)
	cancel := func() {
		if engine.scheduler != nil {
			engine.scheduler.requestCancellation(child.scope)
		}
	}
	return child, cancel
}

func (ctx *OrchestrationContext) start() (actions []*protos.OrchestratorAction) {
	ctx.historyIndex = 0
	ctx.sequenceNumber = 0
	ctx.pendingActions = make(map[int32]*protos.OrchestratorAction)
	ctx.pendingTasks = make(map[int32]*completableTask)
	ctx.scheduler = newCoroutineScheduler(ctx)
	defer func() {
		ctx.scheduler.shutdown()
		ctx.scheduler = nil
	}()

	terminal := false
	markTerminal := func() {
		terminal = true
		ctx.scheduler.shutdown()
	}
	for {
		if !terminal && ctx.scheduler.terminalErr != nil {
			_ = ctx.setFailed(ctx.scheduler.terminalErr)
			markTerminal()
		}

		if !terminal && ctx.scheduler.hasRunnable() {
			ctx.scheduler.runNext()
			if ctx.scheduler.terminalErr != nil {
				continue
			}
			if ctx.scheduler.isRootCompleted() && !ctx.scheduler.rootFinalized {
				ctx.scheduler.rootFinalized = true
				if err := ctx.completeRootCoroutine(); err != nil {
					_ = ctx.setFailed(err)
				}
				markTerminal()
			}
			continue
		}

		ok, err := ctx.processNextEvent()
		if err != nil {
			_ = ctx.setFailed(err)
			markTerminal()
			break
		}
		if !ok {
			break
		}
		if ctx.isTerminated && !terminal {
			markTerminal()
		}
	}
	return ctx.actions()
}

func (ctx *OrchestrationContext) completeRootCoroutine() error {
	switch {
	case ctx.scheduler.rootErr != nil:
		return ctx.setFailed(ctx.scheduler.rootErr)
	case ctx.continuedAsNew:
		return ctx.setContinuedAsNew()
	default:
		return ctx.setComplete(ctx.scheduler.rootResult)
	}
}

func (ctx *OrchestrationContext) processNextEvent() (bool, error) {
	e, ok := ctx.getNextHistoryEvent()
	if !ok {
		// No more history
		return false, nil
	}

	if err := ctx.processEvent(e); err != nil {
		// Internal failure processing event
		return true, err
	}
	return true, nil
}

func (ctx *OrchestrationContext) getNextHistoryEvent() (*protos.HistoryEvent, bool) {
	if len(ctx.resumedEvents) > 0 {
		next := ctx.resumedEvents[0]
		ctx.resumedEvents = ctx.resumedEvents[1:]
		ctx.IsReplaying = next.isReplaying
		return next.event, true
	}

	var historyList []*protos.HistoryEvent
	index := ctx.historyIndex
	switch {
	case ctx.historyIndex >= len(ctx.oldEvents)+len(ctx.newEvents):
		return nil, false
	case ctx.historyIndex < len(ctx.oldEvents):
		ctx.IsReplaying = true
		historyList = ctx.oldEvents
	default:
		ctx.IsReplaying = false
		historyList = ctx.newEvents
		index -= len(ctx.oldEvents)
	}

	ctx.historyIndex++
	e := historyList[index]
	return e, true
}

func (ctx *OrchestrationContext) processEvent(e *backend.HistoryEvent) error {
	defer ctx.syncDerivedContexts()
	// Buffer certain events if we're in a suspended state
	if ctx.isSuspended && (e.GetExecutionResumed() == nil && e.GetExecutionTerminated() == nil) {
		ctx.suspendedEvents = append(ctx.suspendedEvents, replayEvent{
			event:       e,
			isReplaying: ctx.IsReplaying,
		})
		return nil
	}

	var err error = nil
	if os := e.GetOrchestratorStarted(); os != nil {
		// OrchestratorStarted is only used to update the current orchestration time
		ctx.CurrentTimeUtc = e.Timestamp.AsTime()
	} else if es := e.GetExecutionStarted(); es != nil {
		err = ctx.onExecutionStarted(es)
	} else if ts := e.GetTaskScheduled(); ts != nil {
		err = ctx.onTaskScheduled(e.EventId, ts)
	} else if tc := e.GetTaskCompleted(); tc != nil {
		err = ctx.onTaskCompleted(tc)
	} else if tf := e.GetTaskFailed(); tf != nil {
		err = ctx.onTaskFailed(tf)
	} else if ts := e.GetSubOrchestrationInstanceCreated(); ts != nil {
		err = ctx.onSubOrchestrationScheduled(e.EventId, ts)
	} else if sc := e.GetSubOrchestrationInstanceCompleted(); sc != nil {
		err = ctx.onSubOrchestrationCompleted(sc)
	} else if sf := e.GetSubOrchestrationInstanceFailed(); sf != nil {
		err = ctx.onSubOrchestrationFailed(sf)
	} else if tc := e.GetTimerCreated(); tc != nil {
		err = ctx.onTimerCreated(e)
	} else if tf := e.GetTimerFired(); tf != nil {
		err = ctx.onTimerFired(tf)
	} else if es := e.GetEventSent(); es != nil {
		err = ctx.onEventSent(e.EventId, es)
	} else if er := e.GetEventRaised(); er != nil {
		err = ctx.onExternalEventRaised(e)
	} else if es := e.GetExecutionSuspended(); es != nil {
		err = ctx.onExecutionSuspended(es)
	} else if er := e.GetExecutionResumed(); er != nil {
		err = ctx.onExecutionResumed(er)
	} else if et := e.GetExecutionTerminated(); et != nil {
		err = ctx.onExecutionTerminated(et)
	} else if oc := e.GetOrchestratorCompleted(); oc != nil {
		// Nothing to do
	} else {
		err = fmt.Errorf("don't know how to handle event: %v", e)
	}
	return err
}

func (octx *OrchestrationContext) SetCustomStatus(cs string) {
	octx.engineContext().customStatus = cs
}

// GetInput unmarshals the serialized orchestration input and stores it in [v].
func (octx *OrchestrationContext) GetInput(v any) error {
	return unmarshalData(octx.engineContext().rawInput, v)
}

// CallActivity schedules an asynchronous invocation of an activity function. The [activity]
// parameter can be either the name of an activity as a string or can be a pointer to the function
// that implements the activity, in which case the name is obtained via reflection.
func (ctx *OrchestrationContext) CallActivity(activity any, opts ...callActivityOption) Task {
	engine := ctx.engineContext()
	options := new(callActivityOptions)
	for _, configure := range opts {
		if err := configure(options); err != nil {
			return ctx.newFailedTask(engine, err)
		}
	}
	if ctx.scope.isCanceled() {
		return newTaskInScope(engine, ctx.scope)
	}

	if options.retryPolicy != nil {
		return engine.internalScheduleTaskWithRetries(engine.CurrentTimeUtc, func() Task {
			return engine.internalScheduleActivity(activity, options, ctx.scope)
		}, *options.retryPolicy, 0, ctx)
	}

	return engine.internalScheduleActivity(activity, options, ctx.scope)
}

// newFailedTask creates a completed, already-failed task in the calling context's
// cancellation scope. Used when option validation fails before any action is scheduled.
func (ctx *OrchestrationContext) newFailedTask(engine *OrchestrationContext, err error) Task {
	failedTask := newTaskInScope(engine, ctx.scope)
	failedTask.fail(helpers.NewTaskFailureDetails(err))
	return failedTask
}

// Go starts a coroutine that is cooperatively scheduled with the orchestration.
// Only one orchestration coroutine runs at a time, in monotonically increasing ID order.
func (ctx *OrchestrationContext) Go(fn func(ctx *OrchestrationContext)) {
	if fn == nil {
		panic("orchestration coroutine function must be non-nil")
	}
	engine := ctx.engineContext()
	if engine.scheduler == nil {
		panic("orchestration coroutine started outside orchestrator execution")
	}
	engine.scheduler.spawn(ctx, func() {
		fn(ctx)
	})
}

// NewWaitGroup creates a deterministic coroutine wait group.
func (ctx *OrchestrationContext) NewWaitGroup() WaitGroup {
	engine := ctx.engineContext()
	if engine.scheduler == nil {
		panic("orchestration wait group created outside orchestrator execution")
	}
	return newOrchestrationWaitGroup(engine.scheduler)
}

func (ctx *OrchestrationContext) internalScheduleActivity(
	activity any,
	options *callActivityOptions,
	scope *cancellationScope,
) Task {
	if scope.isCanceled() {
		return newTaskInScope(ctx, scope)
	}
	scheduleTaskAction := helpers.NewScheduleTaskAction(
		ctx.getNextSequenceNumber(),
		helpers.GetTaskFunctionName(activity),
		options.rawInput,
		options.version)

	ctx.pendingActions[scheduleTaskAction.Id] = scheduleTaskAction

	task := newTaskInScope(ctx, scope)
	ctx.pendingTasks[scheduleTaskAction.Id] = task
	return task
}

func (ctx *OrchestrationContext) CallSubOrchestrator(orchestrator any, opts ...subOrchestratorOption) Task {
	engine := ctx.engineContext()
	options := new(callSubOrchestratorOptions)
	for _, configure := range opts {
		if err := configure(options); err != nil {
			return ctx.newFailedTask(engine, err)
		}
	}
	if ctx.scope.isCanceled() {
		return newTaskInScope(engine, ctx.scope)
	}

	if options.retryPolicy != nil {
		return engine.internalScheduleTaskWithRetries(engine.CurrentTimeUtc, func() Task {
			return engine.internalCallSubOrchestrator(orchestrator, options, ctx.scope)
		}, *options.retryPolicy, 0, ctx)
	}

	return engine.internalCallSubOrchestrator(orchestrator, options, ctx.scope)
}

func (ctx *OrchestrationContext) internalCallSubOrchestrator(
	orchestrator any,
	options *callSubOrchestratorOptions,
	scope *cancellationScope,
) Task {
	if scope.isCanceled() {
		return newTaskInScope(ctx, scope)
	}
	createSubOrchestrationAction := helpers.NewCreateSubOrchestrationAction(
		ctx.getNextSequenceNumber(),
		helpers.GetTaskFunctionName(orchestrator),
		options.instanceID,
		options.rawInput,
		options.version,
	)
	if createSubOrchestrationAction.GetCreateSubOrchestration().GetInstanceId() == "" {
		createSubOrchestrationAction.GetCreateSubOrchestration().InstanceId = fmt.Sprintf(
			"%s:%04x",
			ctx.ID,
			createSubOrchestrationAction.Id,
		)
	}
	ctx.pendingActions[createSubOrchestrationAction.Id] = createSubOrchestrationAction

	task := newTaskInScope(ctx, scope)
	ctx.pendingTasks[createSubOrchestrationAction.Id] = task
	return task
}

func (ctx *OrchestrationContext) internalScheduleTaskWithRetries(
	initialAttempt time.Time,
	schedule func() Task,
	policy RetryPolicy,
	retryCount int,
	owner *OrchestrationContext,
) Task {
	result := newTaskInScope(ctx, owner.scope)
	attempt := schedule()
	ctx.scheduler.spawn(owner, func() {
		current := attempt
		count := retryCount
		for {
			err := current.Await(nil)
			if err == nil {
				result.completeFrom(current, nil)
				return
			}
			if count+1 >= policy.MaxAttempts {
				result.completeFrom(current, err)
				return
			}

			nextDelay := computeNextDelay(ctx.CurrentTimeUtc, policy, count, initialAttempt, err)
			if nextDelay == 0 {
				result.completeFrom(current, err)
				return
			}
			if timerErr := ctx.createTimerInternal(nextDelay, owner.scope).Await(nil); timerErr != nil {
				result.fail(helpers.NewTaskFailureDetails(errors.Join(timerErr, err)))
				return
			}
			count++
			current = schedule()
		}
	})
	return result
}

func (t *completableTask) completeFrom(source Task, fallback error) {
	state, ok := taskState(source)
	if !ok {
		if fallback != nil {
			t.fail(helpers.NewTaskFailureDetails(fallback))
		} else {
			t.complete(nil)
		}
		return
	}
	switch {
	case state.failureDetails != nil:
		t.fail(state.failureDetails)
	case state.isCanceled:
		t.cancel()
	default:
		t.complete(state.rawResult)
	}
}

func computeNextDelay(currentTimeUtc time.Time, policy RetryPolicy, attempt int, firstAttempt time.Time, err error) time.Duration {
	if policy.Handle(err) {
		isExpired := false
		if policy.RetryTimeout != math.MaxInt64 {
			isExpired = currentTimeUtc.After(firstAttempt.Add(policy.RetryTimeout))
		}
		if !isExpired {
			nextDelayMs := float64(policy.InitialRetryInterval.Milliseconds()) * math.Pow(policy.BackoffCoefficient, float64(attempt))
			if nextDelayMs < float64(policy.MaxRetryInterval.Milliseconds()) {
				return time.Duration(int64(nextDelayMs) * int64(time.Millisecond))
			}
			return policy.MaxRetryInterval
		}
	}
	return 0
}

// CreateTimer schedules a durable timer that expires after the specified delay.
func (ctx *OrchestrationContext) CreateTimer(delay time.Duration) Task {
	engine := ctx.engineContext()
	if ctx.scope.isCanceled() {
		return newTaskInScope(engine, ctx.scope)
	}
	return engine.createTimerInternal(delay, ctx.scope)
}

func (ctx *OrchestrationContext) createTimerInternal(
	delay time.Duration,
	scope *cancellationScope,
) *completableTask {
	if scope.isCanceled() {
		return newTaskInScope(ctx, scope)
	}
	fireAt := ctx.CurrentTimeUtc.Add(delay)
	timerAction := helpers.NewCreateTimerAction(ctx.getNextSequenceNumber(), fireAt)
	ctx.pendingActions[timerAction.Id] = timerAction

	task := newTaskInScope(ctx, scope)
	ctx.pendingTasks[timerAction.Id] = task
	return task
}

// WaitForSingleEvent creates a task that is completed only after an event named [eventName] is received by this orchestration
// or when the specified timeout expires.
//
// The [timeout] parameter can be used to define a timeout for receiving the event. If the timeout expires before the
// named event is received, the task will be completed and will return a timeout error value [ErrTaskCanceled] when
// awaited. Otherwise, the awaited task will return the deserialized payload of the received event. A Duration value
// of zero returns a canceled task if the event isn't already available in the history. Use a negative Duration to
// wait indefinitely for the event to be received.
//
// Orchestrators can wait for the same event name multiple times, so waiting for multiple events with the same name
// is allowed. Each event received by an orchestrator will complete just one task returned by this method.
//
// Note that event names are case-insensitive.
func (ctx *OrchestrationContext) WaitForSingleEvent(eventName string, timeout time.Duration) Task {
	engine := ctx.engineContext()
	task := newTaskInScope(engine, ctx.scope)
	if ctx.scope.isCanceled() {
		return task
	}
	key := strings.ToUpper(eventName)
	if eventList, ok := engine.bufferedExternalEvents[key]; ok {
		// An event with this name arrived already and can be consumed immediately.
		next := eventList.Front()
		if eventList.Len() > 1 {
			eventList.Remove(next)
		} else {
			delete(engine.bufferedExternalEvents, key)
		}
		rawValue := []byte(next.Value.(*bufferedEvent).event.GetEventRaised().GetInput().GetValue())
		task.complete(rawValue)
	} else if timeout == 0 {
		// Zero-timeout means fail immediately if the event isn't already buffered.
		task.cancel()
	} else {
		// Keep a reference to this task so we can complete it when the event of this name arrives
		var taskList *list.List
		var ok bool
		if taskList, ok = engine.pendingExternalEventTasks[key]; !ok {
			taskList = list.New()
			engine.pendingExternalEventTasks[key] = taskList
		}
		taskElement := taskList.PushBack(task)

		if timeout > 0 {
			engine.createTimerInternal(timeout, ctx.scope).onCompleted(func() {
				task.cancel()
				if taskList.Len() > 1 {
					taskList.Remove(taskElement)
				} else {
					delete(engine.pendingExternalEventTasks, key)
				}
			})
		}
	}
	return task
}

func (ctx *OrchestrationContext) ContinueAsNew(newInput any, options ...ContinueAsNewOption) {
	engine := ctx.engineContext()
	engine.continuedAsNew = true
	engine.continuedAsNewInput = newInput
	for _, option := range options {
		option(engine)
	}
}

// SendEvent sends an event to another orchestration instance as part of the
// current durable orchestration transaction.
func (ctx *OrchestrationContext) SendEvent(instanceID api.InstanceID, eventName string, payload any) error {
	raw, err := marshalData(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}
	engine := ctx.engineContext()
	if engine.isTerminated || ctx.scope.isCanceled() {
		return ErrTaskCanceled
	}
	action := helpers.NewSendEventAction(string(instanceID), eventName, wrapperspb.String(string(raw)))
	action.Id = engine.getNextSequenceNumber()
	engine.pendingActions[action.Id] = action
	return nil
}

func (ctx *OrchestrationContext) onExecutionStarted(es *protos.ExecutionStartedEvent) error {
	orchestrator, ok := ctx.registry.orchestrators[es.Name]
	if !ok {
		// try looking for a "default" orchestrator
		orchestrator, ok = ctx.registry.orchestrators["*"]
		if !ok {
			return fmt.Errorf("orchestrator named '%s' is not registered", es.Name)
		}
	}
	ctx.Name = es.Name
	ctx.Version = es.GetVersion().GetValue()
	if es.Input != nil {
		ctx.rawInput = []byte(es.Input.Value)
	}

	return ctx.scheduler.startRoot(orchestrator)
}

func (ctx *OrchestrationContext) onTaskScheduled(taskID int32, ts *protos.TaskScheduledEvent) error {
	if a, ok := ctx.pendingActions[taskID]; !ok || a.GetScheduleTask() == nil {
		return fmt.Errorf(
			"a previous execution called CallActivity for '%s' and sequence number %d at this point in the orchestration logic, but the current execution doesn't have this action with this sequence number",
			ts.Name,
			taskID,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}

func (ctx *OrchestrationContext) onTaskCompleted(tc *protos.TaskCompletedEvent) error {
	taskID := tc.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	if tc.Result != nil {
		task.complete([]byte(tc.Result.Value))
	} else {
		task.complete(nil)
	}
	return nil
}

func (ctx *OrchestrationContext) onTaskFailed(tf *protos.TaskFailedEvent) error {
	taskID := tf.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	task.fail(tf.FailureDetails)
	return nil
}

func (ctx *OrchestrationContext) onSubOrchestrationScheduled(taskID int32, ts *protos.SubOrchestrationInstanceCreatedEvent) error {
	if a, ok := ctx.pendingActions[taskID]; !ok || a.GetCreateSubOrchestration() == nil {
		return fmt.Errorf(
			"a previous execution called CallSubOrchestrator for '%s' and sequence number %d at this point in the orchestration logic, but the current execution doesn't have this action with this sequence number",
			ts.Name,
			taskID,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}

func (ctx *OrchestrationContext) onSubOrchestrationCompleted(soc *protos.SubOrchestrationInstanceCompletedEvent) error {
	taskID := soc.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	if soc.Result != nil {
		task.complete([]byte(soc.Result.Value))
	} else {
		task.complete(nil)
	}
	return nil
}

func (ctx *OrchestrationContext) onSubOrchestrationFailed(sof *protos.SubOrchestrationInstanceFailedEvent) error {
	taskID := sof.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	task.fail(sof.FailureDetails)
	return nil
}

func (ctx *OrchestrationContext) onTimerCreated(e *protos.HistoryEvent) error {
	if a, ok := ctx.pendingActions[e.EventId]; !ok || a.GetCreateTimer() == nil {
		return fmt.Errorf(
			"a previous execution called CreateTimer with sequence number %d, but the current execution doesn't have this action with this sequence number",
			e.EventId,
		)
	}
	delete(ctx.pendingActions, e.EventId)
	return nil
}

func (ctx *OrchestrationContext) onTimerFired(tf *protos.TimerFiredEvent) error {
	timerID := tf.TimerId
	task, ok := ctx.pendingTasks[timerID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, timerID)

	// completing a task will resume the corresponding Await() call
	task.complete(nil)
	return nil
}

func (ctx *OrchestrationContext) onExternalEventRaised(e *protos.HistoryEvent) error {
	er := e.GetEventRaised()
	key := strings.ToUpper(er.GetName())
	if pendingTasks, ok := ctx.pendingExternalEventTasks[key]; ok {
		for pendingTasks.Len() > 0 {
			elem := pendingTasks.Front()
			task := elem.Value.(*completableTask)
			pendingTasks.Remove(elem)
			if pendingTasks.Len() == 0 {
				delete(ctx.pendingExternalEventTasks, key)
			}
			if task.isCompleted {
				continue
			}
			task.complete([]byte(er.Input.GetValue()))
			return nil
		}
	}

	// No live waiter consumed the event, so keep it for a future receiver.
	eventList, ok := ctx.bufferedExternalEvents[key]
	if !ok {
		eventList = list.New()
		ctx.bufferedExternalEvents[key] = eventList
	}
	eventList.PushBack(&bufferedEvent{
		event: e,
		order: ctx.scheduler.nextCompletionID(),
	})
	for waiter := range ctx.eventWaiters[key] {
		ctx.scheduler.makeRunnable(waiter)
	}
	return nil
}

func (ctx *OrchestrationContext) onEventSent(eventID int32, event *protos.EventSentEvent) error {
	action, ok := ctx.pendingActions[eventID]
	sent := action.GetSendEvent()
	if !ok || sent == nil ||
		sent.GetInstance().GetInstanceId() != event.GetInstanceId() ||
		sent.GetName() != event.GetName() ||
		sent.GetData().GetValue() != event.GetInput().GetValue() {
		return fmt.Errorf(
			"a previous execution sent event %q to %q with sequence number %d, but the current execution doesn't have this action",
			event.GetName(),
			event.GetInstanceId(),
			eventID,
		)
	}
	delete(ctx.pendingActions, eventID)
	return nil
}

func (ctx *OrchestrationContext) peekBufferedEvent(key string) (*bufferedEvent, bool) {
	eventList, ok := ctx.bufferedExternalEvents[key]
	if !ok || eventList.Len() == 0 {
		return nil, false
	}
	return eventList.Front().Value.(*bufferedEvent), true
}

func (ctx *OrchestrationContext) takeBufferedEvent(key string) (*bufferedEvent, bool) {
	eventList, ok := ctx.bufferedExternalEvents[key]
	if !ok || eventList.Len() == 0 {
		return nil, false
	}
	next := eventList.Front()
	event := next.Value.(*bufferedEvent)
	eventList.Remove(next)
	if eventList.Len() == 0 {
		delete(ctx.bufferedExternalEvents, key)
	}
	return event, true
}

func (ctx *OrchestrationContext) addEventWaiter(key string, co *coroutine) {
	waiters, ok := ctx.eventWaiters[key]
	if !ok {
		waiters = make(map[*coroutine]struct{})
		ctx.eventWaiters[key] = waiters
	}
	waiters[co] = struct{}{}
}

func (ctx *OrchestrationContext) removeEventWaiter(key string, co *coroutine) {
	waiters, ok := ctx.eventWaiters[key]
	if !ok {
		return
	}
	delete(waiters, co)
	if len(waiters) == 0 {
		delete(ctx.eventWaiters, key)
	}
}

func (ctx *OrchestrationContext) onExecutionSuspended(er *protos.ExecutionSuspendedEvent) error {
	ctx.isSuspended = true
	return nil
}

func (ctx *OrchestrationContext) onExecutionResumed(er *protos.ExecutionResumedEvent) error {
	ctx.isSuspended = false
	ctx.resumedEvents = append(ctx.resumedEvents, ctx.suspendedEvents...)
	ctx.suspendedEvents = nil
	return nil
}

func (ctx *OrchestrationContext) onExecutionTerminated(et *protos.ExecutionTerminatedEvent) error {
	ctx.isTerminated = true
	if err := ctx.setCompleteInternal(et.Input, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *OrchestrationContext) setComplete(output any) error {
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
	var rawOutput *wrapperspb.StringValue
	if output != nil {
		bytes, err := json.Marshal(output)
		if err != nil {
			return fmt.Errorf("failed to marshal output to JSON: %w", err)
		}
		rawOutput = wrapperspb.String(string(bytes))
	}
	if err := ctx.setCompleteInternal(rawOutput, status, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *OrchestrationContext) setFailed(appError error) error {
	for id, action := range ctx.pendingActions {
		if action.GetCompleteOrchestration() != nil {
			delete(ctx.pendingActions, id)
		}
	}
	fd := helpers.NewTaskFailureDetails(appError)
	failedStatus := protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED
	if err := ctx.setCompleteInternal(nil, failedStatus, fd); err != nil {
		return err
	}
	return nil
}

func (ctx *OrchestrationContext) setContinuedAsNew() error {
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW
	var newRawInput *wrapperspb.StringValue
	if ctx.continuedAsNewInput != nil {
		bytes, err := json.Marshal(ctx.continuedAsNewInput)
		if err != nil {
			return fmt.Errorf("failed to marshal continue-as-new payload to JSON: %w", err)
		}
		newRawInput = wrapperspb.String(string(bytes))
	}
	if err := ctx.setCompleteInternal(newRawInput, status, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *OrchestrationContext) setCompleteInternal(
	rawResult *wrapperspb.StringValue,
	status protos.OrchestrationStatus,
	failureDetails *protos.TaskFailureDetails,
) error {
	sequenceNumber := ctx.getNextSequenceNumber()
	completedAction := helpers.NewCompleteOrchestrationAction(
		sequenceNumber,
		status,
		rawResult,
		nil, // carryoverEvents is assigned later
		failureDetails,
	)
	ctx.pendingActions[sequenceNumber] = completedAction
	return nil
}

func (ctx *OrchestrationContext) getNextSequenceNumber() int32 {
	current := ctx.sequenceNumber
	ctx.sequenceNumber++
	return current
}

func (ctx *OrchestrationContext) actions() []*protos.OrchestratorAction {
	if ctx.isSuspended {
		return nil
	}

	actions := make([]*protos.OrchestratorAction, 0, len(ctx.pendingActions))
	for id := int32(0); id < ctx.sequenceNumber; id++ {
		a, ok := ctx.pendingActions[id]
		if !ok {
			continue
		}
		actions = append(actions, a)
		if ctx.continuedAsNew && ctx.saveBufferedExternalEvents {
			if co := a.GetCompleteOrchestration(); co != nil {
				events := make([]*bufferedEvent, 0)
				for _, eventList := range ctx.bufferedExternalEvents {
					for item := eventList.Front(); item != nil; item = item.Next() {
						events = append(events, item.Value.(*bufferedEvent))
					}
				}
				sort.Slice(events, func(i, j int) bool {
					return events[i].order < events[j].order
				})
				for _, event := range events {
					co.CarryoverEvents = append(co.CarryoverEvents, event.event)
				}
			}
		}
	}
	return actions
}
