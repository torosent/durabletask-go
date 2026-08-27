package a

import "github.com/microsoft/durabletask-go/task"

func badNamed(*task.OrchestrationContext) (any, error) {
	go func() {}() // want "raw go statement is not deterministic"
	return nil, nil
}

func badNested(*task.OrchestrationContext) (any, error) {
	nested := func() {
		go func() {}() // want "raw go statement is not deterministic"
	}
	nested()
	return nil, nil
}

func good(ctx *task.OrchestrationContext) (any, error) {
	ctx.Go(func(*task.OrchestrationContext) {})
	return nil, nil
}

func badCoroutine(ctx *task.OrchestrationContext) (any, error) {
	ctx.Go(func(*task.OrchestrationContext) {
		go func() {}() // want "raw go statement is not deterministic"
	})
	return nil, nil
}

func unregistered(*task.OrchestrationContext) (any, error) {
	go func() {}()
	return nil, nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(badNamed)
	_ = registry.AddOrchestrator(badNested)
	_ = registry.AddOrchestrator(good)
	_ = registry.AddOrchestrator(badCoroutine)
	_ = registry.AddOrchestratorN("inline", func(*task.OrchestrationContext) (any, error) {
		go func() {}() // want "raw go statement is not deterministic"
		return nil, nil
	})

	assigned := func(*task.OrchestrationContext) (any, error) {
		go func() {}() // want "raw go statement is not deterministic"
		return nil, nil
	}
	_ = registry.AddOrchestrator(assigned)

	_ = registry.AddActivity(func(task.ActivityContext) (any, error) {
		go func() {}()
		return nil, nil
	})
}
