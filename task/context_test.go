package task

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestReplaySafeLoggerSuppressesReplayOutput(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	ctx := NewOrchestrationContext(NewTaskRegistry(), "logging", nil, nil)
	ctx.Name = "logger-test"
	ctx.Version = "v1"
	ctx.logger = logger

	ctx.IsReplaying = true
	ctx.Logger().Info("replayed")
	ctx.IsReplaying = false
	ctx.Logger().Info("live")

	if strings.Contains(output.String(), "replayed") {
		t.Fatalf("replay log was emitted: %s", output.String())
	}
	if !strings.Contains(output.String(), "live") {
		t.Fatalf("live log was not emitted: %s", output.String())
	}
}

func TestOrchestrationContextPropagatesIdentityAndFields(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("context-test", func(ctx *OrchestrationContext) (any, error) {
		info, ok := api.OrchestrationContextInfoFromContext(ctx.Context())
		if !ok {
			return nil, nil
		}
		return struct {
			Info   api.OrchestrationContextInfo
			Fields api.ContextFields
		}{
			Info:   info,
			Fields: api.ContextFieldsFromContext(ctx.Context()),
		}, nil
	}); err != nil {
		t.Fatal(err)
	}

	fields := api.ContextFields{"tenant": "alpha"}
	executor := NewTaskExecutor(registry, WithContextFields(fields))
	fields["tenant"] = "mutated"
	instanceID := api.InstanceID("context-instance")
	parent := helpers.NewParentInfo(4, "parent", "parent-instance")
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent(
				"context-test",
				string(instanceID),
				nil,
				parent,
				nil,
				nil,
				wrapperspb.String("v2"),
			),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	var output struct {
		Info   api.OrchestrationContextInfo
		Fields api.ContextFields
	}
	if err := json.Unmarshal([]byte(completionAction(t, result.Response).GetResult().GetValue()), &output); err != nil {
		t.Fatal(err)
	}
	if output.Info.InstanceID != instanceID ||
		output.Info.Name != "context-test" ||
		output.Info.Version != "v2" ||
		output.Info.ParentInstanceID != "parent-instance" {
		t.Fatalf("unexpected orchestration info: %+v", output.Info)
	}
	if output.Fields["tenant"] != "alpha" {
		t.Fatalf("context field = %q, want alpha", output.Fields["tenant"])
	}
}

func TestActivityContextPropagatesIdentityFieldsAndLogger(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddActivityN("inspect", func(ctx ActivityContext) (any, error) {
		orchestration, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
		activity, _ := api.ActivityContextInfoFromContext(ctx.Context())
		LoggerFromContext(ctx.Context()).Info("activity log")
		return struct {
			Orchestration api.OrchestrationContextInfo
			Activity      api.ActivityContextInfo
			Fields        api.ContextFields
		}{
			Orchestration: orchestration,
			Activity:      activity,
			Fields:        api.ContextFieldsFromContext(ctx.Context()),
		}, nil
	}); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	executor := NewTaskExecutor(
		registry,
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
		WithContextFields(api.ContextFields{"feature": "enabled"}),
	)
	instanceID := api.InstanceID("activity-context")
	base := api.WithOrchestrationContextInfo(context.Background(), api.OrchestrationContextInfo{
		InstanceID: instanceID,
		Name:       "parent",
		Version:    "v3",
	})
	response, err := executor.ExecuteActivity(
		base,
		instanceID,
		helpers.NewTaskScheduledEvent(9, "inspect", wrapperspb.String("a1"), nil, nil),
	)
	if err != nil {
		t.Fatal(err)
	}

	var output struct {
		Orchestration api.OrchestrationContextInfo
		Activity      api.ActivityContextInfo
		Fields        api.ContextFields
	}
	if err := json.Unmarshal([]byte(response.GetTaskCompleted().GetResult().GetValue()), &output); err != nil {
		t.Fatal(err)
	}
	if output.Orchestration.Name != "parent" || output.Orchestration.Version != "v3" {
		t.Fatalf("unexpected orchestration context: %+v", output.Orchestration)
	}
	if output.Activity.Name != "inspect" || output.Activity.Version != "a1" || output.Activity.TaskID != 9 {
		t.Fatalf("unexpected activity context: %+v", output.Activity)
	}
	if output.Fields["feature"] != "enabled" {
		t.Fatalf("context field = %q, want enabled", output.Fields["feature"])
	}
	if !strings.Contains(logs.String(), "activity log") {
		t.Fatalf("activity logger output missing: %s", logs.String())
	}
}
