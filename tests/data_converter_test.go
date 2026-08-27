package tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

type integrationGobConverter struct{}

func (integrationGobConverter) Serialize(value any) (string, error) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(value); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(buffer.Bytes()), nil
}

func (integrationGobConverter) Deserialize(payload string, target any) error {
	data, err := base64.RawStdEncoding.DecodeString(payload)
	if err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewReader(data)).Decode(target)
}

type integrationPayload struct {
	Value int
}

func TestCustomDataConverterAndVersionMigrationAcrossEmbeddedBackends(t *testing.T) {
	for index, be := range backends {
		t.Run(backendTestName(be), func(t *testing.T) {
			initTest(t, be, index, true)
			converter := integrationGobConverter{}
			registry := task.NewTaskRegistry()
			require.NoError(t, registry.AddActivityNVersion("increment", "v1", func(ctx task.ActivityContext) (any, error) {
				var input integrationPayload
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				input.Value++
				return input, nil
			}))
			require.NoError(t, registry.AddOrchestratorNVersion("workflow", "v1", func(ctx *task.OrchestrationContext) (any, error) {
				var input integrationPayload
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				var activityResult integrationPayload
				if err := ctx.CallActivity(
					"increment",
					task.WithActivityInput(input),
				).Await(&activityResult); err != nil {
					return nil, err
				}
				var event integrationPayload
				if err := ctx.WaitForSingleEvent("complete", time.Minute).Await(&event); err != nil {
					return nil, err
				}
				if err := ctx.SetCustomStatusValue(activityResult); err != nil {
					return nil, err
				}
				ctx.ContinueAsNew(
					integrationPayload{Value: activityResult.Value + event.Value},
					task.WithContinueAsNewVersion("v2"),
				)
				return nil, nil
			}))
			require.NoError(t, registry.AddOrchestratorNVersion("workflow", "v2", func(ctx *task.OrchestrationContext) (any, error) {
				var input integrationPayload
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				return integrationPayload{Value: input.Value + 1}, nil
			}))

			executor := task.NewTaskExecutor(
				registry,
				task.WithDataConverter(converter),
				task.WithVersioning(task.VersioningOptions{DefaultVersion: "v1"}),
			)
			worker := backend.NewTaskHubWorker(
				be,
				backend.NewOrchestrationWorker(be, executor, logger),
				backend.NewActivityTaskWorker(be, executor, logger),
				logger,
			)
			testCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			require.NoError(t, worker.Start(testCtx))
			t.Cleanup(func() {
				require.NoError(t, worker.Shutdown(context.Background()))
			})

			client := backend.NewTaskHubClient(
				be,
				backend.WithDataConverter(converter),
				backend.WithDefaultVersion("v1"),
			)
			instanceID, err := client.ScheduleNewOrchestration(
				testCtx,
				"workflow",
				api.WithInput(integrationPayload{Value: 1}),
			)
			require.NoError(t, err)
			started, err := client.WaitForOrchestrationStart(testCtx, instanceID)
			require.NoError(t, err)
			require.Equal(t, "v1", started.Version)
			require.NoError(t, client.RaiseEvent(
				testCtx,
				instanceID,
				"complete",
				api.WithEventPayload(integrationPayload{Value: 10}),
			))

			metadata, err := client.WaitForOrchestrationCompletion(testCtx, instanceID)
			require.NoError(t, err)
			require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
			require.Equal(t, "v2", metadata.Version)
			var output integrationPayload
			require.NoError(t, metadata.ReadOutput(&output))
			require.Equal(t, 13, output.Value)
		})
	}
}

func backendTestName(be backend.Backend) string {
	return fmt.Sprintf("%T", be)
}
