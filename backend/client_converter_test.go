package backend

import (
	"context"
	"fmt"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/require"
)

type prefixConverter struct{}

func (prefixConverter) Serialize(value any) (string, error) {
	return fmt.Sprintf("converted:%v", value), nil
}

func (prefixConverter) Deserialize(payload string, target any) error {
	value, ok := target.(*string)
	if !ok {
		return fmt.Errorf("unsupported target %T", target)
	}
	*value = payload
	return nil
}

func TestEmbeddedClientUsesConfiguredDataConverter(t *testing.T) {
	storage := new(capturingBackend)
	client := NewTaskHubClient(storage, WithDataConverter(prefixConverter{}))

	instanceID, err := client.ScheduleNewOrchestration(
		context.Background(),
		"orchestration",
		api.WithInput("input"),
	)
	require.NoError(t, err)
	require.NotEmpty(t, instanceID)
	require.Equal(t, "converted:input", storage.created[0].GetExecutionStarted().GetInput().GetValue())

	require.NoError(t, client.RaiseEvent(
		context.Background(),
		instanceID,
		"event",
		api.WithEventPayload("payload"),
	))
	require.Equal(t, "converted:payload", storage.added[0].GetEventRaised().GetInput().GetValue())

	require.NoError(t, client.TerminateOrchestration(
		context.Background(),
		instanceID,
		api.WithOutput("output"),
	))
	require.Equal(t, "converted:output", storage.added[1].GetExecutionTerminated().GetInput().GetValue())
}
