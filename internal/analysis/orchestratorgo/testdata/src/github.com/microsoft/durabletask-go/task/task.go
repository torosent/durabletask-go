package task

type OrchestrationContext struct{}

func (*OrchestrationContext) Go(func(*OrchestrationContext)) {}

type Orchestrator func(*OrchestrationContext) (any, error)
type ActivityContext interface{}
type Activity func(ActivityContext) (any, error)

type TaskRegistry struct{}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{}
}

func (*TaskRegistry) AddOrchestrator(Orchestrator) error {
	return nil
}

func (*TaskRegistry) AddOrchestratorN(string, Orchestrator) error {
	return nil
}

func (*TaskRegistry) AddActivity(Activity) error {
	return nil
}
