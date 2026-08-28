package backend

import (
	"testing"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

// TestRuntimeStatusPrefersCompletionOverSuspension locks down the precedence
// between the suspended flag and a terminal completion event. A suspended
// orchestration that is terminated (or that completes for any other reason)
// must report the terminal status, otherwise callers waiting for completion
// would wait forever on an instance that can never run again.
func TestRuntimeStatusPrefersCompletionOverSuspension(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []*HistoryEvent
		want   protos.OrchestrationStatus
	}{
		{
			name:   "no-start-event",
			events: nil,
			want:   protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING,
		},
		{
			name: "started",
			events: []*HistoryEvent{
				helpers.NewExecutionStartedEvent("test", "instance", nil, nil, nil, nil),
			},
			want: protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		},
		{
			name: "suspended",
			events: []*HistoryEvent{
				helpers.NewExecutionStartedEvent("test", "instance", nil, nil, nil, nil),
				helpers.NewSuspendOrchestrationEvent("hold"),
			},
			want: protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED,
		},
		{
			name: "suspended-then-resumed",
			events: []*HistoryEvent{
				helpers.NewExecutionStartedEvent("test", "instance", nil, nil, nil, nil),
				helpers.NewSuspendOrchestrationEvent("hold"),
				helpers.NewResumeOrchestrationEvent("go"),
			},
			want: protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		},
		{
			name: "suspended-then-terminated",
			events: []*HistoryEvent{
				helpers.NewExecutionStartedEvent("test", "instance", nil, nil, nil, nil),
				helpers.NewSuspendOrchestrationEvent("hold"),
				helpers.NewExecutionCompletedEvent(
					-1,
					protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
					nil,
					nil,
				),
			},
			want: protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
		},
		{
			name: "suspended-then-completed",
			events: []*HistoryEvent{
				helpers.NewExecutionStartedEvent("test", "instance", nil, nil, nil, nil),
				helpers.NewSuspendOrchestrationEvent("hold"),
				helpers.NewExecutionCompletedEvent(
					-1,
					protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
					nil,
					nil,
				),
			},
			want: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := NewOrchestrationRuntimeState("instance", test.events)
			require.Equal(t, test.want, state.RuntimeStatus())
			require.Equal(t, test.want, state.Snapshot().RuntimeStatus)
		})
	}
}
