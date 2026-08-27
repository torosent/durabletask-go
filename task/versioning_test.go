package task

import (
	"context"
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCompareVersionsMatchesDurableTaskRules(t *testing.T) {
	tests := []struct {
		left  string
		right string
		want  int
	}{
		{"", "", 0},
		{"", "1.0", -1},
		{"1.0", "", 1},
		{"1.2", "1.10", -1},
		{"2.0.0", "2.0", 1},
		{"preview-A", "preview-a", 0},
		{"preview-b", "preview-a", 1},
	}
	for _, test := range tests {
		got := compareVersions(test.left, test.right)
		if got < 0 {
			got = -1
		} else if got > 0 {
			got = 1
		}
		if got != test.want {
			t.Fatalf("compareVersions(%q, %q) = %d, want %d", test.left, test.right, got, test.want)
		}
	}
}

func TestVersionMismatchRejectsOrchestration(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("versioned", func(*OrchestrationContext) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("version-reject")
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		Version:         "1.0",
		MatchStrategy:   VersionMatchCurrentOrOlder,
		FailureStrategy: VersionFailureReject,
	}))
	_, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent(
				"versioned",
				string(instanceID),
				nil,
				nil,
				nil,
				nil,
				wrapperspb.String("2.0"),
			),
		},
	)
	var mismatch *VersionMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want VersionMismatchError", err)
	}
}

func TestVersionMismatchFailsOrchestration(t *testing.T) {
	registry := NewTaskRegistry()
	instanceID := api.InstanceID("version-fail")
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		Version:         "1.0",
		MatchStrategy:   VersionMatchStrict,
		FailureStrategy: VersionFailureFail,
	}))
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent(
				"versioned",
				string(instanceID),
				nil,
				nil,
				nil,
				nil,
				wrapperspb.String("2.0"),
			),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	completed := result.Response.Actions[0].GetCompleteOrchestration()
	if completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		t.Fatalf("status = %v", completed.GetOrchestrationStatus())
	}
	if !completed.GetFailureDetails().GetIsNonRetriable() {
		t.Fatal("version mismatch failure must be non-retriable")
	}
}

func TestVersionMismatchFailsActivity(t *testing.T) {
	registry := NewTaskRegistry()
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		Version:         "1.0",
		MatchStrategy:   VersionMatchStrict,
		FailureStrategy: VersionFailureFail,
	}))
	event := helpers.NewTaskScheduledEvent(
		1,
		"activity",
		wrapperspb.String("2.0"),
		nil,
		nil,
	)
	result, err := executor.ExecuteActivity(context.Background(), "instance", event)
	if err != nil {
		t.Fatal(err)
	}
	failed := result.GetTaskFailed()
	if failed == nil || !failed.GetFailureDetails().GetIsNonRetriable() {
		t.Fatalf("activity result = %v", result)
	}
}
