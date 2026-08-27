package task

import (
	"errors"
	"fmt"
	"sync"
)

type coroutineState uint8

const (
	coroutineRunnable coroutineState = iota
	coroutineRunning
	coroutineWaiting
	coroutineCompleted
)

type coroutineSignalKind uint8

const (
	coroutineYielded coroutineSignalKind = iota
	coroutineFinished
	coroutineCanceled
	coroutinePanicked
)

type coroutineSignal struct {
	kind       coroutineSignalKind
	panicValue any
}

type coroutine struct {
	id        uint64
	scheduler *coroutineScheduler
	ctx       *OrchestrationContext
	fn        func()
	resume    chan struct{}
	signals   chan coroutineSignal
	stop      chan struct{}
	exited    chan struct{}
	stopOnce  sync.Once
	state     coroutineState
	scope     *cancellationScope
}

func (c *coroutine) run() {
	defer close(c.exited)
	defer func() {
		if value := recover(); value != nil {
			if c.scheduler.isStopping() && isTaskBlocked(value) {
				return
			}
			if c.scope.isCanceled() && isTaskCanceled(value) {
				c.sendSignal(coroutineSignal{kind: coroutineCanceled})
				return
			}
			c.sendSignal(coroutineSignal{kind: coroutinePanicked, panicValue: value})
		}
	}()

	select {
	case <-c.resume:
	case <-c.stop:
		return
	}

	c.fn()
	c.sendSignal(coroutineSignal{kind: coroutineFinished})
}

func (c *coroutine) yield() {
	c.sendSignal(coroutineSignal{kind: coroutineYielded})
	select {
	case <-c.resume:
	case <-c.stop:
		panic(ErrTaskBlocked)
	}
}

func (c *coroutine) sendSignal(signal coroutineSignal) {
	select {
	case c.signals <- signal:
	case <-c.stop:
	}
}

func (c *coroutine) exit() {
	c.stopOnce.Do(func() {
		close(c.stop)
	})
	<-c.exited
	c.state = coroutineCompleted
}

func isTaskBlocked(value any) bool {
	err, ok := value.(error)
	return ok && errors.Is(err, ErrTaskBlocked)
}

func isTaskCanceled(value any) bool {
	err, ok := value.(error)
	return ok && errors.Is(err, ErrTaskCanceled)
}

func coroutinePanicError(id uint64, value any) error {
	if err, ok := value.(error); ok {
		return fmt.Errorf("coroutine %d panicked: %w", id, err)
	}
	return fmt.Errorf("coroutine %d panicked: %v", id, value)
}
