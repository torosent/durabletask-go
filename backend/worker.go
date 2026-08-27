package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/marusama/semaphore/v2"
	"github.com/microsoft/durabletask-go/api"
)

type TaskWorker interface {
	// Start starts background polling for the activity work items.
	Start(context.Context)

	// ProcessNext attempts to fetch and process a work item. This method returns
	// true if a work item was found and processing started; false otherwise. An
	// error is returned if the context is cancelled.
	ProcessNext(context.Context) (bool, error)

	// StopAndDrain stops the worker and waits for all outstanding work items to finish.
	StopAndDrain()
}

type TaskProcessor interface {
	Name() string
	FetchWorkItem(context.Context) (WorkItem, error)
	ProcessWorkItem(context.Context, WorkItem) error
	AbandonWorkItem(context.Context, WorkItem) error
	CompleteWorkItem(context.Context, WorkItem) error
}

type worker struct {
	options *WorkerOptions
	logger  Logger
	// dispatchSemaphore is for throttling orchestration concurrency.
	dispatchSemaphore  semaphore.Semaphore
	activitySemaphores map[string]semaphore.Semaphore

	// pending is for keeping track of outstanding orchestration executions.
	pending           *sync.WaitGroup
	poller            sync.WaitGroup
	inFlight          atomic.Int64
	nextBacklogSample atomic.Int64

	// cancel is used to cancel background polling.
	// It will be nil if background polling isn't started.
	cancel    context.CancelFunc
	processor TaskProcessor
	waiting   bool
	stop      atomic.Bool
}

const (
	activityConcurrencyRetryDelay = 100 * time.Millisecond
	backlogMetricsInterval        = time.Second
)

type NewTaskWorkerOptions func(*WorkerOptions)

type WorkerOptions struct {
	MaxParallelWorkItems      int32
	ActivityConcurrencyLimits map[string]int32
	Metrics                   MetricsHooks
}

func NewWorkerOptions() *WorkerOptions {
	return &WorkerOptions{
		MaxParallelWorkItems: 1,
	}
}

func WithMaxParallelism(n int32) NewTaskWorkerOptions {
	return func(o *WorkerOptions) {
		o.MaxParallelWorkItems = n
	}
}

// WithActivityConcurrencyLimit limits concurrent local executions of one activity name.
func WithActivityConcurrencyLimit(name string, n int32) NewTaskWorkerOptions {
	if name == "" {
		panic("activity name must be non-empty")
	}
	if n <= 0 {
		panic("activity concurrency limit must be greater than zero")
	}
	return func(o *WorkerOptions) {
		if o.ActivityConcurrencyLimits == nil {
			o.ActivityConcurrencyLimits = make(map[string]int32)
		}
		o.ActivityConcurrencyLimits[name] = n
	}
}

// WithWorkerMetrics configures optional local worker metric callbacks.
func WithWorkerMetrics(hooks MetricsHooks) NewTaskWorkerOptions {
	return func(o *WorkerOptions) {
		o.Metrics = hooks
	}
}

func NewTaskWorker(p TaskProcessor, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker {
	options := &WorkerOptions{MaxParallelWorkItems: 1}
	for _, configure := range opts {
		configure(options)
	}
	activitySemaphores := make(map[string]semaphore.Semaphore, len(options.ActivityConcurrencyLimits))
	for name, limit := range options.ActivityConcurrencyLimits {
		activitySemaphores[name] = semaphore.New(int(limit))
	}
	return &worker{
		processor:          p,
		logger:             logger,
		dispatchSemaphore:  semaphore.New(int(options.MaxParallelWorkItems)),
		activitySemaphores: activitySemaphores,
		pending:            &sync.WaitGroup{},
		cancel:             nil, // assigned later
		options:            options,
	}
}

func (w *worker) Name() string {
	return w.processor.Name()
}

func (w *worker) Start(ctx context.Context) {
	// TODO: Check for already started worker
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	w.stop.Store(false)
	w.poller.Add(1)

	go func() {
		defer w.poller.Done()
		var b backoff.BackOff = &backoff.ExponentialBackOff{
			InitialInterval:     50 * time.Millisecond,
			MaxInterval:         5 * time.Second,
			Multiplier:          1.05,
			RandomizationFactor: 0.05,
			Stop:                backoff.Stop,
			Clock:               backoff.SystemClock,
		}
		b = backoff.WithContext(b, ctx)
		b.Reset()

	loop:
		for {
			// returns right away, with "ok" if a work item was found
			ok, err := w.ProcessNext(ctx)

			switch {
			case ok:
				// found a work item - reset the backoff and check for the next item
				b.Reset()
			case err != nil && errors.Is(err, ctx.Err()):
				// there's an error and it's due to the context being canceled
				w.logger.Infof("%v: received cancellation signal", w.Name())
				break loop
			case err != nil:
				// another error was encountered
				// log the error and inject some extra sleep to avoid tight failure loops
				w.logger.Errorf("unexpected worker error: %v. Adding 5 extra seconds of backoff.", err)
				if !sleep(ctx, 5*time.Second) {
					w.logger.Infof("%v: received cancellation signal", w.Name())
					break loop
				}
			default:
				// no work item found, so sleep until the next backoff
				if !sleep(ctx, b.NextBackOff()) {
					w.logger.Infof("%v: received cancellation signal", w.Name())
					break loop
				}
			}
		}

		w.logger.Infof("%v: stopped listening for new work items", w.Name())
	}()
}

// sleep waits for the given duration, returning false if ctx is canceled first.
func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *worker) ProcessNext(ctx context.Context) (bool, error) {
	w.reportBacklog(ctx)
	if !w.dispatchSemaphore.TryAcquire(1) {
		w.logger.Debugf("%v: waiting for one of %v in-flight execution(s) to complete", w.Name(), w.dispatchSemaphore.GetCount())
		if err := w.dispatchSemaphore.Acquire(ctx, 1); err != nil {
			// cancelled
			return false, err
		}
	}
	w.pending.Add(1)

	processing := false
	defer func() {
		if !processing {
			w.pending.Done()
			w.dispatchSemaphore.Release(1)
		}
	}()

	wi, err := w.processor.FetchWorkItem(ctx)
	switch {
	case errors.Is(err, ErrNoWorkItems) || wi == nil:
		if !w.waiting {
			w.logger.Debugf("%v: waiting for new work items...", w.Name())
			w.waiting = true
		}
		return false, nil
	case err != nil:
		if !errors.Is(err, ctx.Err()) {
			w.logger.Errorf("%v: failed to fetch work item: %v", w.Name(), err)
		}
		return false, err
	default:
		activitySemaphore := w.activitySemaphore(wi)
		if activitySemaphore != nil && !activitySemaphore.TryAcquire(1) {
			setActivityAbandonDelay(wi, activityConcurrencyRetryDelay)
			if err := w.processor.AbandonWorkItem(ctx, wi); err != nil {
				return false, fmt.Errorf("%v: failed to defer activity work item at its concurrency limit: %w", w.Name(), err)
			}
			w.waiting = false
			return false, nil
		}
		// process the work-item in the background
		w.waiting = false
		processing = true
		go w.processWorkItem(ctx, wi, activitySemaphore)
		return true, nil
	}
}

func (w *worker) StopAndDrain() {
	w.logger.Debugf("%v: stop and drain...", w.Name())
	defer w.logger.Debugf("%v: finished stop and drain...", w.Name())

	w.stop.Store(true)

	// Cancel the background poller and dispatcher(s)
	if w.cancel != nil {
		w.cancel()
	}

	// Ensure the poller cannot add another item to pending before waiting.
	w.poller.Wait()

	// Wait for outstanding work-items to finish processing.
	// TODO: Need to find a way to cancel this if it takes too long for some reason.
	w.pending.Wait()
}

func (w *worker) processWorkItem(ctx context.Context, wi WorkItem, activitySemaphore semaphore.Semaphore) {
	defer w.dispatchSemaphore.Release(1)
	defer w.pending.Done()
	if activitySemaphore != nil {
		defer activitySemaphore.Release(1)
	}
	setActivityAbandonDelay(wi, 0)

	startedAt := time.Now()
	w.inFlight.Add(1)
	defer w.inFlight.Add(-1)
	w.reportWorkerActivity(wi, WorkerActivityStarted, startedAt)

	w.logger.Debugf("%v: processing work item: %s", w.Name(), wi)

	if w.stop.Load() {
		w.abandonWorkItem(context.Background(), wi, startedAt)
		return
	}

	if err := w.processor.ProcessWorkItem(ctx, wi); err != nil {
		if errors.Is(err, ctx.Err()) {
			w.logger.Warnf("%v: abandoning work item due to cancellation", w.Name())
		} else {
			w.logger.Errorf("%v: failed to process work item: %v", w.Name(), err)
		}
		applyErrorAbandonDelay(wi, err)
		w.abandonWorkItem(w.releaseContext(ctx), wi, startedAt)
		return
	}

	if err := w.processor.CompleteWorkItem(w.releaseContext(ctx), wi); err != nil {
		if errors.Is(err, ctx.Err()) {
			w.logger.Warnf("%v: failed to complete work item due to cancellation", w.Name())
		} else {
			w.logger.Errorf("%v: failed to complete work item: %v", w.Name(), err)
		}
		w.abandonWorkItem(w.releaseContext(ctx), wi, startedAt)
		return
	}

	w.logger.Debugf("%v: work item processed successfully", w.Name())
	w.reportWorkerActivity(wi, WorkerActivityCompleted, startedAt)
}

// releaseContext returns the context to use when releasing a work item. A
// stopping worker releases on a background context so the release isn't dropped
// along with the canceled work item context.
func (w *worker) releaseContext(ctx context.Context) context.Context {
	if w.stop.Load() {
		return context.Background()
	}
	return ctx
}

func (w *worker) abandonWorkItem(ctx context.Context, wi WorkItem, startedAt time.Time) {
	if err := w.processor.AbandonWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to abandon work item: %v", w.Name(), err)
	}
	w.reportWorkerActivity(wi, WorkerActivityAbandoned, startedAt)
}

func (w *worker) activitySemaphore(wi WorkItem) semaphore.Semaphore {
	if len(w.activitySemaphores) == 0 {
		return nil
	}
	activity := asActivityWorkItem(wi)
	if activity == nil || activity.NewEvent == nil {
		return nil
	}
	return w.activitySemaphores[activity.NewEvent.GetTaskScheduled().GetName()]
}

// asOrchestrationWorkItem normalizes the pointer and value forms of an
// orchestration work item, returning nil for any other work item type.
func asOrchestrationWorkItem(wi WorkItem) *OrchestrationWorkItem {
	switch workItem := wi.(type) {
	case *OrchestrationWorkItem:
		return workItem
	case OrchestrationWorkItem:
		return &workItem
	default:
		return nil
	}
}

// asActivityWorkItem normalizes the pointer and value forms of an activity work
// item, returning nil for any other work item type.
func asActivityWorkItem(wi WorkItem) *ActivityWorkItem {
	switch workItem := wi.(type) {
	case *ActivityWorkItem:
		return workItem
	case ActivityWorkItem:
		return &workItem
	default:
		return nil
	}
}

func asEntityWorkItem(wi WorkItem) *EntityWorkItem {
	switch workItem := wi.(type) {
	case *EntityWorkItem:
		return workItem
	case EntityWorkItem:
		return &workItem
	default:
		return nil
	}
}

type backlogMetricSource interface {
	GetBacklogMetric(context.Context) (BacklogMetric, bool, error)
}

func (w *worker) reportBacklog(ctx context.Context) {
	if w.options.Metrics.Backlog == nil {
		return
	}
	source, ok := w.processor.(backlogMetricSource)
	if !ok {
		return
	}
	now := time.Now()
	for {
		next := w.nextBacklogSample.Load()
		if next > now.UnixNano() {
			return
		}
		if w.nextBacklogSample.CompareAndSwap(next, now.Add(backlogMetricsInterval).UnixNano()) {
			break
		}
	}
	metric, supported, err := source.GetBacklogMetric(ctx)
	if err != nil {
		w.logger.Warnf("%v: failed to inspect work-item backlog: %v", w.Name(), err)
		return
	}
	if !supported {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			w.logger.Errorf("%v: backlog metrics callback panicked: %v", w.Name(), recovered)
		}
	}()
	w.options.Metrics.Backlog(metric)
}

func (w *worker) reportWorkerActivity(wi WorkItem, state WorkerActivityState, startedAt time.Time) {
	if w.options.Metrics.WorkerActivity == nil {
		return
	}
	metric := WorkerActivityMetric{
		State:        state,
		InFlight:     w.inFlight.Load(),
		Duration:     time.Since(startedAt),
		QueueLatency: workItemQueueLatency(wi, startedAt),
	}
	if orchestration := asOrchestrationWorkItem(wi); orchestration != nil {
		metric.Kind = WorkItemKindOrchestration
		metric.InstanceID = orchestration.InstanceID
		metric.RetryCount = orchestration.RetryCount
	} else if activity := asActivityWorkItem(wi); activity != nil {
		metric.Kind = WorkItemKindActivity
		metric.InstanceID = activity.InstanceID
		metric.RetryCount = activity.RetryCount
		if activity.NewEvent != nil {
			metric.ActivityName = activity.NewEvent.GetTaskScheduled().GetName()
		}
	} else if entity := asEntityWorkItem(wi); entity != nil {
		metric.Kind = WorkItemKindEntity
		metric.InstanceID = api.InstanceID(entity.InstanceID.String())
		metric.RetryCount = entity.RetryCount
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			w.logger.Errorf("%v: worker activity metrics callback panicked: %v", w.Name(), recovered)
		}
	}()
	w.options.Metrics.WorkerActivity(metric)
}

func setActivityAbandonDelay(wi WorkItem, delay time.Duration) {
	if activity := asActivityWorkItem(wi); activity != nil {
		activity.AbandonDelay = delay
	}
}

func applyErrorAbandonDelay(wi WorkItem, err error) {
	var delayed WorkItemAbandonDelayError
	if errors.As(err, &delayed) {
		setActivityAbandonDelay(wi, delayed.WorkItemAbandonDelay())
	}
}

func workItemQueueLatency(wi WorkItem, now time.Time) time.Duration {
	var enqueuedAt time.Time
	if orchestration := asOrchestrationWorkItem(wi); orchestration != nil {
		enqueuedAt = orchestration.EnqueuedAt
	} else if activity := asActivityWorkItem(wi); activity != nil {
		enqueuedAt = activity.EnqueuedAt
	} else if entity := asEntityWorkItem(wi); entity != nil {
		enqueuedAt = entity.EnqueuedAt
	}
	if enqueuedAt.IsZero() || !enqueuedAt.Before(now) {
		return 0
	}
	return now.Sub(enqueuedAt)
}
