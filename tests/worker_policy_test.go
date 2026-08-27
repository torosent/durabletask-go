package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/stretchr/testify/require"
)

type blockingActivityProcessor struct {
	mu        sync.Mutex
	queue     []backend.WorkItem
	running   map[string]int
	max       map[string]int
	completed int
	release   chan struct{}
	backlog   backend.BacklogMetric
}

func newBlockingActivityProcessor(names ...string) *blockingActivityProcessor {
	processor := &blockingActivityProcessor{
		running: make(map[string]int),
		max:     make(map[string]int),
		release: make(chan struct{}),
		backlog: backend.BacklogMetric{
			Kind:      backend.WorkItemKindActivity,
			Depth:     int64(len(names)),
			OldestAge: time.Second,
		},
	}
	for i, name := range names {
		processor.queue = append(processor.queue, &backend.ActivityWorkItem{
			SequenceNumber: int64(i),
			InstanceID:     "worker-policy",
			NewEvent:       helpers.NewTaskScheduledEvent(int32(i), name, nil, nil, nil),
			RetryCount:     int32(i),
		})
	}
	return processor
}

func (*blockingActivityProcessor) Name() string {
	return "activity-processor"
}

func (p *blockingActivityProcessor) FetchWorkItem(context.Context) (backend.WorkItem, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queue) == 0 {
		return nil, backend.ErrNoWorkItems
	}
	workItem := p.queue[0]
	p.queue = p.queue[1:]
	return workItem, nil
}

func (p *blockingActivityProcessor) ProcessWorkItem(ctx context.Context, workItem backend.WorkItem) error {
	name := workItem.(*backend.ActivityWorkItem).NewEvent.GetTaskScheduled().GetName()
	p.mu.Lock()
	p.running[name]++
	if p.running[name] > p.max[name] {
		p.max[name] = p.running[name]
	}
	p.mu.Unlock()

	select {
	case <-p.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	p.mu.Lock()
	p.running[name]--
	p.mu.Unlock()
	return nil
}

func (*blockingActivityProcessor) AbandonWorkItem(context.Context, backend.WorkItem) error {
	return nil
}

func (p *blockingActivityProcessor) CompleteWorkItem(context.Context, backend.WorkItem) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completed++
	return nil
}

func (p *blockingActivityProcessor) GetBacklogMetric(context.Context) (backend.BacklogMetric, bool, error) {
	return p.backlog, true, nil
}

func (p *blockingActivityProcessor) snapshot(name string) (queued, running, maxRunning, completed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queue), p.running[name], p.max[name], p.completed
}

func TestActivityConcurrencyLimitPreservesDefaultWhenUnset(t *testing.T) {
	processor := newBlockingActivityProcessor("A", "A")
	worker := backend.NewTaskWorker(processor, logger, backend.WithMaxParallelism(2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)

	require.Eventually(t, func() bool {
		_, running, _, _ := processor.snapshot("A")
		return running == 2
	}, time.Second, 10*time.Millisecond)
	close(processor.release)
	require.Eventually(t, func() bool {
		_, _, _, completed := processor.snapshot("A")
		return completed == 2
	}, time.Second, 10*time.Millisecond)
	worker.StopAndDrain()

	_, _, maxRunning, completed := processor.snapshot("A")
	require.Equal(t, 2, maxRunning)
	require.Equal(t, 2, completed)
}

func TestActivityConcurrencyLimitCapsOneActivityName(t *testing.T) {
	processor := newBlockingActivityProcessor("A", "A")
	worker := backend.NewTaskWorker(
		processor,
		logger,
		backend.WithMaxParallelism(2),
		backend.WithActivityConcurrencyLimit("A", 1),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)

	require.Eventually(t, func() bool {
		queued, running, maxRunning, _ := processor.snapshot("A")
		return queued == 0 && running == 1 && maxRunning == 1
	}, time.Second, 10*time.Millisecond)
	close(processor.release)
	require.Eventually(t, func() bool {
		_, _, _, completed := processor.snapshot("A")
		return completed == 2
	}, time.Second, 10*time.Millisecond)
	worker.StopAndDrain()

	_, _, maxRunning, completed := processor.snapshot("A")
	require.Equal(t, 1, maxRunning)
	require.Equal(t, 2, completed)
}

func TestWorkerMetricsHooksReportBacklogAndActivity(t *testing.T) {
	processor := newBlockingActivityProcessor("A")
	close(processor.release)

	var mu sync.Mutex
	backlogMetrics := make([]backend.BacklogMetric, 0, 1)
	activityMetrics := make([]backend.WorkerActivityMetric, 0, 2)
	worker := backend.NewTaskWorker(
		processor,
		logger,
		backend.WithWorkerMetrics(backend.MetricsHooks{
			Backlog: func(metric backend.BacklogMetric) {
				mu.Lock()
				defer mu.Unlock()
				backlogMetrics = append(backlogMetrics, metric)
			},
			WorkerActivity: func(metric backend.WorkerActivityMetric) {
				mu.Lock()
				defer mu.Unlock()
				activityMetrics = append(activityMetrics, metric)
			},
		}),
	)

	ok, err := worker.ProcessNext(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(activityMetrics) == 2
	}, time.Second, 10*time.Millisecond)
	worker.StopAndDrain()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, backlogMetrics, 1)
	require.Equal(t, int64(1), backlogMetrics[0].Depth)
	require.Len(t, activityMetrics, 2)
	require.Equal(t, backend.WorkerActivityStarted, activityMetrics[0].State)
	require.Equal(t, backend.WorkerActivityCompleted, activityMetrics[1].State)
	require.Equal(t, int32(0), activityMetrics[0].RetryCount)
	require.Equal(t, int64(1), activityMetrics[0].InFlight)
}
