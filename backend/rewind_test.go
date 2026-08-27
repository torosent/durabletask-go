package backend

import (
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRebuildRewindHistoryPreservesIdentityAndRemovesFailure(t *testing.T) {
	parent := helpers.NewParentInfo(7, "parent", "parent-instance")
	parent.OrchestrationInstance.ExecutionId = wrapperspb.String("parent-execution")
	traceContext := &protos.TraceContext{TraceParent: "00-trace-span-01"}
	start := helpers.NewExecutionStartedEvent(
		"orchestration",
		"instance",
		wrapperspb.String(`"input"`),
		parent,
		traceContext,
		nil,
		wrapperspb.String("v1"),
	)
	start.GetExecutionStarted().Tags = map[string]string{"team": "durable"}
	originalExecutionID := start.GetExecutionStarted().GetOrchestrationInstance().GetExecutionId().GetValue()
	history := []*protos.HistoryEvent{
		start,
		helpers.NewTaskScheduledEvent(1, "activity", wrapperspb.String("v1"), wrapperspb.String(`"input"`), traceContext),
		helpers.NewTaskFailedEvent(1, &protos.TaskFailureDetails{ErrorMessage: "failed"}),
		helpers.NewExecutionCompletedEvent(
			2,
			api.RUNTIME_STATUS_FAILED,
			nil,
			&protos.TaskFailureDetails{ErrorMessage: "failed"},
		),
	}

	rebuilt, rebuiltStart, failedChildren, err := RebuildRewindHistory("instance", history)
	require.NoError(t, err)
	require.Empty(t, failedChildren)
	require.Len(t, rebuilt, 1)
	require.NotEqual(t, originalExecutionID, rebuiltStart.GetOrchestrationInstance().GetExecutionId().GetValue())
	require.Equal(t, `"input"`, rebuiltStart.GetInput().GetValue())
	require.Equal(t, "v1", rebuiltStart.GetVersion().GetValue())
	require.Equal(t, parent, rebuiltStart.GetParentInstance())
	require.Equal(t, traceContext, rebuiltStart.GetParentTraceContext())
	require.Equal(t, map[string]string{"team": "durable"}, rebuiltStart.GetTags())
}
