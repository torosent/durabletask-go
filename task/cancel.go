package task

type cancellationScope struct {
	parent     *cancellationScope
	children   []*cancellationScope
	tasks      []*completableTask
	coroutines []*coroutine
	canceled   bool
}

func newCancellationScope(parent *cancellationScope) *cancellationScope {
	scope := &cancellationScope{parent: parent}
	if parent != nil {
		parent.children = append(parent.children, scope)
	}
	return scope
}

// isCanceled reports whether the scope has been canceled. A nil scope is
// treated as never canceled, so callers don't need a separate nil check.
func (s *cancellationScope) isCanceled() bool {
	return s != nil && s.canceled
}

func (s *cancellationScope) addTask(task *completableTask) {
	s.tasks = append(s.tasks, task)
	if s.canceled {
		task.cancel()
	}
}

func (s *cancellationScope) addCoroutine(c *coroutine) {
	s.coroutines = append(s.coroutines, c)
}

func (s *cancellationScope) cancel(scheduler *coroutineScheduler) {
	if s.canceled {
		return
	}
	s.canceled = true
	for _, child := range s.children {
		child.cancel(scheduler)
	}
	for _, task := range s.tasks {
		task.cancel()
	}
	for _, c := range s.coroutines {
		scheduler.makeRunnable(c)
	}
}
