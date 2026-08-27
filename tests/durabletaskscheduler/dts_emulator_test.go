package durabletaskscheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

func emulatorOptions(t *testing.T) *durabletaskscheduler.Options {
	t.Helper()
	if connectionString := os.Getenv("DTS_CONNECTION_STRING"); connectionString != "" {
		options, err := durabletaskscheduler.NewOptionsFromConnectionString(connectionString)
		require.NoError(t, err)
		return options
	}

	endpoint := os.Getenv("DTS_EMULATOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DTS_CONNECTION_STRING or DTS_EMULATOR_ENDPOINT to run DTS emulator tests")
	}
	taskHub := os.Getenv("DTS_TASK_HUB")
	if taskHub == "" {
		taskHub = "default"
	}
	options, err := durabletaskscheduler.NewOptionsFromConnectionString(
		fmt.Sprintf("Endpoint=%s;TaskHub=%s;Authentication=None", endpoint, taskHub),
	)
	require.NoError(t, err)
	return options
}

func startEmulatorClientAndWorker(
	t *testing.T,
	registry *task.TaskRegistry,
) (*durabletaskscheduler.Client, *durabletaskclient.TaskHubGrpcWorker, *durabletaskscheduler.Options) {
	t.Helper()
	options := emulatorOptions(t)
	logger := backend.DefaultLogger()

	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(
		options,
		registry,
		logger,
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(4),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(8),
		durabletaskclient.WithWorkerSilentDisconnectTimeout(15*time.Second),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})
	return managementClient, worker, options
}

func uniqueInstanceID(prefix string) api.InstanceID {
	return api.InstanceID(prefix + "-" + uuid.NewString())
}

func TestDTSEmulatorSequenceMetadataAndPurge(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSSequence", func(ctx *task.OrchestrationContext) (any, error) {
		var outputs []string
		for _, city := range []string{"Tokyo", "London", "Seattle"} {
			var output string
			if err := ctx.CallActivity("DTSSayHello", task.WithActivityInput(city)).Await(&output); err != nil {
				return nil, err
			}
			outputs = append(outputs, output)
		}
		return outputs, nil
	}))
	require.NoError(t, registry.AddActivityN("DTSSayHello", func(ctx task.ActivityContext) (any, error) {
		var city string
		if err := ctx.GetInput(&city); err != nil {
			return nil, err
		}
		return "Hello, " + city + "!", nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	instanceID := uniqueInstanceID("go-sequence")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSSequence",
		api.WithInstanceID(instanceID),
	)
	require.NoError(t, err)
	require.Equal(t, instanceID, id)

	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	require.Equal(t, "DTSSequence", metadata.Name)
	require.Equal(t, instanceID, metadata.InstanceID)
	require.False(t, metadata.CreatedAt.IsZero())
	require.False(t, metadata.LastUpdatedAt.IsZero())

	var output []string
	require.NoError(t, json.Unmarshal([]byte(metadata.SerializedOutput), &output))
	require.Equal(t, []string{"Hello, Tokyo!", "Hello, London!", "Hello, Seattle!"}, output)

	require.NoError(t, managementClient.PurgeOrchestrationState(ctx, id))
	_, err = managementClient.FetchOrchestrationMetadata(ctx, id)
	require.ErrorIs(t, err, api.ErrInstanceNotFound)
}

func TestDTSEmulatorEventsSuspendResumeAndTerminate(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSEvents", func(ctx *task.OrchestrationContext) (any, error) {
		var values []int
		for range 2 {
			var value int
			if err := ctx.WaitForSingleEvent("value", time.Minute).Await(&value); err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSTerminate", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.CreateTimer(time.Minute).Await(nil); err != nil {
			return nil, err
		}
		return "unexpected", nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	eventID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSEvents",
		api.WithInstanceID(uniqueInstanceID("go-events")),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, eventID)
	require.NoError(t, err)
	require.NoError(t, managementClient.SuspendOrchestration(ctx, eventID, "test"))
	require.NoError(t, managementClient.RaiseEvent(ctx, eventID, "value", api.WithEventPayload(1)))
	require.NoError(t, managementClient.RaiseEvent(ctx, eventID, "value", api.WithEventPayload(2)))

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = managementClient.WaitForOrchestrationCompletion(waitCtx, eventID)
	cancelWait()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	suspended, err := managementClient.FetchOrchestrationMetadata(ctx, eventID)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED, suspended.RuntimeStatus)

	require.NoError(t, managementClient.ResumeOrchestration(ctx, eventID, "test"))
	completed, err := managementClient.WaitForOrchestrationCompletion(ctx, eventID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `[1,2]`, completed.SerializedOutput)

	terminateID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSTerminate",
		api.WithInstanceID(uniqueInstanceID("go-terminate")),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, terminateID)
	require.NoError(t, err)
	require.NoError(t, managementClient.TerminateOrchestration(ctx, terminateID, api.WithOutput("terminated")))
	terminated, err := managementClient.WaitForOrchestrationCompletion(ctx, terminateID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, terminated.RuntimeStatus)
	require.Equal(t, `"terminated"`, terminated.SerializedOutput)
}

func TestDTSEmulatorWorkerStopAndRestart(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSRestart", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		if err := ctx.CallActivity("DTSRestartActivity").Await(&result); err != nil {
			return nil, err
		}
		return result, nil
	}))
	require.NoError(t, registry.AddActivityN("DTSRestartActivity", func(task.ActivityContext) (any, error) {
		return "restarted", nil
	}))

	options := emulatorOptions(t)
	logger := backend.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, managementClient.Close())
	}()

	newWorker := func() *durabletaskclient.TaskHubGrpcWorker {
		worker, workerErr := durabletaskscheduler.NewWorker(
			options,
			registry,
			logger,
			durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(2),
			durabletaskclient.WithMaxConcurrentActivityWorkItems(2),
		)
		require.NoError(t, workerErr)
		require.NoError(t, worker.Start(context.Background()))
		return worker
	}

	worker := newWorker()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	require.NoError(t, worker.Shutdown(shutdownCtx))
	cancelShutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSRestart",
		api.WithInstanceID(uniqueInstanceID("go-restart")),
	)
	require.NoError(t, err)

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = managementClient.WaitForOrchestrationCompletion(waitCtx, instanceID)
	cancelWait()
	require.True(t, errors.Is(err, context.DeadlineExceeded), "orchestration unexpectedly completed without a worker: %v", err)

	worker = newWorker()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
	}()
	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"restarted"`, metadata.SerializedOutput)
}

func TestDTSEmulatorSelectEventCancelsTimer(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSSelect", func(ctx *task.OrchestrationContext) (any, error) {
		timerCtx, cancelTimer := ctx.WithCancel()
		timer := timerCtx.CreateTimer(time.Minute)
		events := task.NewEventChannel[string](ctx, "approval")
		selected := ""
		ctx.Select(
			task.OnTask(timer, func(task.Task) { selected = "timeout" }),
			task.OnEvent(events, func(value string) { selected = value }),
		)
		cancelTimer()
		return selected, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSSelect",
		api.WithInstanceID(uniqueInstanceID("go-select")),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, instanceID)
	require.NoError(t, err)
	require.NoError(t, managementClient.RaiseEvent(
		ctx,
		instanceID,
		"approval",
		api.WithEventPayload("approved"),
	))
	metadata, err := managementClient.WaitForOrchestrationCompletion(
		ctx,
		instanceID,
		api.WithFetchPayloads(true),
	)
	require.NoError(t, err)
	require.Equal(t, `"approved"`, metadata.SerializedOutput)
}

func TestDTSEmulatorConcurrentCoroutineFanOut(t *testing.T) {
	const (
		instanceCount = 16
		fanOut        = 16
		expected      = fanOut * (fanOut - 1)
	)

	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSConcurrentFanOut", func(ctx *task.OrchestrationContext) (any, error) {
		results := make([]int, fanOut)
		waitGroup := ctx.NewWaitGroup()
		waitGroup.Add(fanOut)
		for i := range fanOut {
			i := i
			ctx.Go(func(ctx *task.OrchestrationContext) {
				defer waitGroup.Done()
				if err := ctx.CallActivity(
					"DTSDouble",
					task.WithActivityInput(i),
				).Await(&results[i]); err != nil {
					panic(err)
				}
			})
		}
		waitGroup.Wait(ctx)
		total := 0
		for _, result := range results {
			total += result
		}
		return total, nil
	}))
	require.NoError(t, registry.AddActivityN("DTSDouble", func(ctx task.ActivityContext) (any, error) {
		var value int
		if err := ctx.GetInput(&value); err != nil {
			return nil, err
		}
		return value * 2, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	instanceIDs := make([]api.InstanceID, instanceCount)
	for i := range instanceCount {
		instanceID, err := managementClient.ScheduleNewOrchestration(
			ctx,
			"DTSConcurrentFanOut",
			api.WithInstanceID(uniqueInstanceID("go-fanout")),
		)
		require.NoError(t, err)
		instanceIDs[i] = instanceID
	}

	errs := make(chan error, instanceCount)
	var waitGroup sync.WaitGroup
	waitGroup.Add(instanceCount)
	for _, instanceID := range instanceIDs {
		instanceID := instanceID
		go func() {
			defer waitGroup.Done()
			metadata, err := managementClient.WaitForOrchestrationCompletion(
				ctx,
				instanceID,
				api.WithFetchPayloads(true),
			)
			if err == nil && metadata.SerializedOutput != fmt.Sprintf("%d", expected) {
				err = fmt.Errorf(
					"%s output = %s, want %d",
					instanceID,
					metadata.SerializedOutput,
					expected,
				)
			}
			errs <- err
		}()
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
