package backend

import (
	"context"
	"errors"
	"sync"
)

type TaskHubWorker interface {
	// Start starts the backend and the configured internal workers.
	Start(context.Context) error

	// Shutdown stops the backend and all internal workers.
	Shutdown(context.Context) error
}

type taskHubWorker struct {
	backend             Backend
	orchestrationWorker TaskWorker
	activityWorker      TaskWorker
	entityWorker        TaskWorker
	logger              Logger
}

func NewTaskHubWorker(be Backend, orchestrationWorker TaskWorker, activityWorker TaskWorker, logger Logger, entityWorkers ...TaskWorker) TaskHubWorker {
	worker := &taskHubWorker{
		backend:             be,
		orchestrationWorker: orchestrationWorker,
		activityWorker:      activityWorker,
		logger:              logger,
	}
	if len(entityWorkers) > 0 {
		worker.entityWorker = entityWorkers[0]
	}
	return worker
}

func (w *taskHubWorker) Start(ctx context.Context) error {
	// TODO: Check for already started worker
	if err := w.backend.CreateTaskHub(ctx); err != nil && !errors.Is(err, ErrTaskHubExists) {
		return err
	}
	if err := w.backend.Start(ctx); err != nil {
		return err
	}
	w.logger.Infof("worker started with backend %v", w.backend)

	w.orchestrationWorker.Start(ctx)
	w.activityWorker.Start(ctx)
	if w.entityWorker != nil {
		w.entityWorker.Start(ctx)
	}
	return nil
}

func (w *taskHubWorker) Shutdown(ctx context.Context) error {
	w.logger.Info("workers stopping and draining...")

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.orchestrationWorker.StopAndDrain()
	}()

	if w.entityWorker != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.entityWorker.StopAndDrain()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.activityWorker.StopAndDrain()
	}()

	wg.Wait()
	w.logger.Info("finished stopping and draining workers!")

	w.logger.Info("backend stopping...")
	return w.backend.Stop(ctx)
}
