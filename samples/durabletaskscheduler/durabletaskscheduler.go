package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	connectionString := os.Getenv("DTS_CONNECTION_STRING")
	if connectionString == "" {
		return fmt.Errorf("DTS_CONNECTION_STRING is required")
	}
	options, err := durabletaskscheduler.NewOptionsFromConnectionString(connectionString)
	if err != nil {
		return fmt.Errorf("invalid DTS connection string: %w", err)
	}
	options.Versioning = &task.VersioningOptions{
		DefaultVersion: "1.0",
		MatchStrategy:  task.VersionMatchNone,
	}

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorNVersion("ActivitySequence", "1.0", activitySequence); err != nil {
		return err
	}
	if err := registry.AddActivityNVersion("SayHello", "1.0", sayHello); err != nil {
		return err
	}

	logger := backend.DefaultLogger()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	schedulerClient, err := durabletaskscheduler.NewClient(ctx, options, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := schedulerClient.Close(); err != nil {
			log.Printf("failed to close DTS client: %v", err)
		}
	}()

	worker, err := durabletaskscheduler.NewWorker(
		options,
		registry,
		logger,
		durabletaskclient.WithScheduledTaskCapability(true),
		durabletaskclient.WithAutoWorkItemFilters(),
	)
	if err != nil {
		return err
	}
	if err := worker.Start(ctx); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := worker.Shutdown(shutdownCtx); err != nil {
			log.Printf("failed to stop DTS worker: %v", err)
		}
	}()

	instanceID, err := schedulerClient.ScheduleNewOrchestration(
		ctx,
		"ActivitySequence",
		api.WithTags(map[string]string{"sample": "durable-task-scheduler"}),
	)
	if err != nil {
		return err
	}
	metadata, err := schedulerClient.WaitForOrchestrationCompletion(ctx, instanceID)
	if err != nil {
		return err
	}
	output, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	query, err := schedulerClient.QueryInstances(ctx, api.OrchestrationQuery{
		Tags: map[string]string{"sample": "durable-task-scheduler"},
	})
	if err != nil {
		return err
	}
	fmt.Printf("matched %d tagged orchestration(s)\n", len(query.Orchestrations))
	return nil
}

func activitySequence(ctx *task.OrchestrationContext) (any, error) {
	var results []string
	for _, city := range []string{"Tokyo", "London", "Seattle"} {
		var result string
		if err := ctx.CallActivity("SayHello", task.WithActivityInput(city)).Await(&result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func sayHello(ctx task.ActivityContext) (any, error) {
	var city string
	if err := ctx.GetInput(&city); err != nil {
		return nil, err
	}
	return "Hello, " + city + "!", nil
}
