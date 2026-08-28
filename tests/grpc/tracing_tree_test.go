package tests_grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/microsoft/durabletask-go/tests/tracingtree"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

func startCallerSpan(t *testing.T, parent context.Context, name string) (context.Context, trace.Span) {
	t.Helper()
	return tracingtree.StartCallerSpan(t, "tests/grpc", parent, name)
}

func requireCallerSpan(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	return tracingtree.RequireOne(t, spans, tracingtree.Name(name))
}

// Test_Grpc_TracingTree_ActivityFailureAndClientRaisedEvent covers the failure half of the
// span tree over gRPC: a client-raised external event annotation on the orchestration span,
// a failed activity child span, and error status propagation onto the orchestration span.
func Test_Grpc_TracingTree_ActivityFailureAndClientRaisedEvent(t *testing.T) {
	exporter := tracingtree.Init()

	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("TraceActivityFailure", func(ctx *task.OrchestrationContext) (any, error) {
		var signal string
		if err := ctx.WaitForSingleEvent("proceed", 30*time.Second).Await(&signal); err != nil {
			return nil, err
		}
		return nil, ctx.CallActivity("TraceFailingActivity", task.WithActivityInput(signal)).Await(nil)
	}))
	require.NoError(t, r.AddActivityN("TraceFailingActivity", func(task.ActivityContext) (any, error) {
		return nil, errors.New("activity exploded")
	}))
	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 60*time.Second)
	defer cancelTimeout()

	const callerSpanName = "caller/activity-failure"
	callerCtx, callerSpan := startCallerSpan(t, timeoutCtx, callerSpanName)
	id, err := grpcClient.ScheduleNewOrchestration(
		callerCtx,
		"TraceActivityFailure",
		api.WithInstanceID("trace-activity-failure"),
	)
	require.NoError(t, err)
	callerSpan.End()

	_, err = grpcClient.WaitForOrchestrationStart(timeoutCtx, id)
	require.NoError(t, err)
	require.NoError(t, grpcClient.RaiseEvent(timeoutCtx, id, "proceed", api.WithEventPayload("go")))
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)

	spans := exporter.GetSpans().Snapshots()
	caller := requireCallerSpan(t, spans, callerSpanName)

	created := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceActivityFailure", "")),
		tracingtree.Instance(id))
	tracingtree.RequireKind(t, created, trace.SpanKindClient)
	tracingtree.RequireStringAttribute(t, created, tracingtree.AttributeType, "orchestration")
	tracingtree.RequireChildOf(t, caller, created)

	// The orchestration executed at least three work items (start, external event,
	// activity result) but must still export exactly one, non-replayed span.
	orchestration := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceActivityFailure", "")),
		tracingtree.Instance(id))
	tracingtree.RequireKind(t, orchestration, trace.SpanKindServer)
	tracingtree.RequireChildOf(t, created, orchestration)
	tracingtree.RequireStringAttribute(t, orchestration, tracingtree.AttributeRuntimeStatus, "FAILED")
	tracingtree.RequireStatus(t, orchestration, codes.Error, "activity exploded")
	tracingtree.RequireExternalEvent(t, orchestration, "proceed", len(`"go"`))
	tracingtree.RequireEvents(t, orchestration, tracingtree.EventSuspended, 0)
	tracingtree.RequireEvents(t, orchestration, tracingtree.EventResumed, 0)

	activity := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.ActivitySpanName("TraceFailingActivity", "")),
		tracingtree.Instance(id))
	tracingtree.RequireKind(t, activity, trace.SpanKindServer)
	tracingtree.RequireChildOf(t, orchestration, activity)
	tracingtree.RequireStringAttribute(t, activity, tracingtree.AttributeType, "activity")
	tracingtree.RequireStatus(t, activity, codes.Error, "activity exploded")
	// Unversioned tasks must not advertise a version attribute.
	tracingtree.RequireNoAttribute(t, activity, tracingtree.AttributeVersion)
}

// Test_Grpc_TracingTree_SubOrchestrations covers sub-orchestration success and failure,
// which must appear as child orchestration spans of the parent orchestration span.
func Test_Grpc_TracingTree_SubOrchestrations(t *testing.T) {
	exporter := tracingtree.Init()

	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("TraceChildOK", func(ctx *task.OrchestrationContext) (any, error) {
		var output string
		if err := ctx.CallActivity("TraceEcho", task.WithActivityInput("child")).Await(&output); err != nil {
			return nil, err
		}
		return output, nil
	}))
	require.NoError(t, r.AddOrchestratorN("TraceChildFailed", func(*task.OrchestrationContext) (any, error) {
		return nil, errors.New("child exploded")
	}))
	require.NoError(t, r.AddActivityN("TraceEcho", func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	require.NoError(t, r.AddOrchestratorN("TraceSubOrchestrationParent", func(ctx *task.OrchestrationContext) (any, error) {
		var completed string
		if err := ctx.CallSubOrchestrator(
			"TraceChildOK",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_ok"),
		).Await(&completed); err != nil {
			return nil, err
		}
		if err := ctx.CallSubOrchestrator(
			"TraceChildFailed",
			task.WithSubOrchestrationInstanceID(string(ctx.ID)+"_failed"),
		).Await(nil); err == nil {
			return nil, errors.New("expected the child orchestration to fail")
		}
		return completed, nil
	}))
	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 60*time.Second)
	defer cancelTimeout()

	const callerSpanName = "caller/sub-orchestrations"
	callerCtx, callerSpan := startCallerSpan(t, timeoutCtx, callerSpanName)
	id, err := grpcClient.ScheduleNewOrchestration(
		callerCtx,
		"TraceSubOrchestrationParent",
		api.WithInstanceID("trace-sub-orchestration"),
	)
	require.NoError(t, err)
	callerSpan.End()

	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	require.Equal(t, `"child"`, metadata.SerializedOutput)

	spans := exporter.GetSpans().Snapshots()
	caller := requireCallerSpan(t, spans, callerSpanName)
	created := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceSubOrchestrationParent", "")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, caller, created)

	parent := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceSubOrchestrationParent", "")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, created, parent)
	tracingtree.RequireStringAttribute(t, parent, tracingtree.AttributeRuntimeStatus, "COMPLETED")
	tracingtree.RequireStatus(t, parent, codes.Unset, "")

	// Sub-orchestrations are started by the parent orchestration, so their execution spans
	// are direct children of the parent's span, not of the create_orchestration span.
	completedChild := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceChildOK", "")),
		tracingtree.Instance(id+"_ok"))
	tracingtree.RequireChildOf(t, parent, completedChild)
	tracingtree.RequireKind(t, completedChild, trace.SpanKindServer)
	tracingtree.RequireStringAttribute(t, completedChild, tracingtree.AttributeRuntimeStatus, "COMPLETED")
	tracingtree.RequireStatus(t, completedChild, codes.Unset, "")

	failedChild := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceChildFailed", "")),
		tracingtree.Instance(id+"_failed"))
	tracingtree.RequireChildOf(t, parent, failedChild)
	tracingtree.RequireStringAttribute(t, failedChild, tracingtree.AttributeRuntimeStatus, "FAILED")
	tracingtree.RequireStatus(t, failedChild, codes.Error, "child exploded")

	// Activities scheduled by a sub-orchestration belong to the sub-orchestration's span
	// and carry the sub-orchestration's instance ID.
	activity := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.ActivitySpanName("TraceEcho", "")),
		tracingtree.Instance(id+"_ok"))
	tracingtree.RequireChildOf(t, completedChild, activity)
	tracingtree.RequireInt64Attribute(t, activity, tracingtree.AttributeTaskID, 0)

	// Sub-orchestrations are not scheduled through the client, so they never produce
	// their own create_orchestration span.
	tracingtree.RequireNone(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceChildOK", "")))
	tracingtree.RequireNone(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceChildFailed", "")))
}

// Test_Grpc_TracingTree_TimersAndOrchestrationSentEvents covers durable timer spans and
// events sent from one orchestration to another.
func Test_Grpc_TracingTree_TimersAndOrchestrationSentEvents(t *testing.T) {
	exporter := tracingtree.Init()

	const receiverID = api.InstanceID("trace-event-receiver")
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("TraceEventReceiver", func(ctx *task.OrchestrationContext) (any, error) {
		var payload string
		if err := ctx.WaitForSingleEvent("ping", 30*time.Second).Await(&payload); err != nil {
			return nil, err
		}
		return payload, nil
	}))
	require.NoError(t, r.AddOrchestratorN("TraceEventSender", func(ctx *task.OrchestrationContext) (any, error) {
		// Two sequential timers must produce two distinct timer spans.
		if err := ctx.CreateTimer(time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		if err := ctx.SendEvent(receiverID, "ping", "pong"); err != nil {
			return nil, err
		}
		if err := ctx.CreateTimer(time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		return "sent", nil
	}))
	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 60*time.Second)
	defer cancelTimeout()

	const callerSpanName = "caller/timers-and-sent-events"
	callerCtx, callerSpan := startCallerSpan(t, timeoutCtx, callerSpanName)
	receiver, err := grpcClient.ScheduleNewOrchestration(
		callerCtx,
		"TraceEventReceiver",
		api.WithInstanceID(receiverID),
	)
	require.NoError(t, err)
	_, err = grpcClient.WaitForOrchestrationStart(timeoutCtx, receiver)
	require.NoError(t, err)
	sender, err := grpcClient.ScheduleNewOrchestration(
		callerCtx,
		"TraceEventSender",
		api.WithInstanceID("trace-event-sender"),
	)
	require.NoError(t, err)
	callerSpan.End()

	senderMetadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, sender, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"sent"`, senderMetadata.SerializedOutput)
	receiverMetadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, receiver, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"pong"`, receiverMetadata.SerializedOutput)

	spans := exporter.GetSpans().Snapshots()
	caller := requireCallerSpan(t, spans, callerSpanName)

	senderCreated := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceEventSender", "")),
		tracingtree.Instance(sender))
	tracingtree.RequireChildOf(t, caller, senderCreated)
	senderSpan := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceEventSender", "")),
		tracingtree.Instance(sender))
	tracingtree.RequireChildOf(t, senderCreated, senderSpan)

	// Timer spans are emitted once per fired timer, as children of the orchestration that
	// scheduled them, and are always internal (no remote work is involved).
	timers := tracingtree.RequireCount(t, spans, 2,
		tracingtree.Name(tracingtree.TimerSpanName),
		tracingtree.Instance(sender))
	for _, timer := range timers {
		tracingtree.RequireKind(t, timer, trace.SpanKindInternal)
		tracingtree.RequireChildOf(t, senderSpan, timer)
		tracingtree.RequireStringAttribute(t, timer, tracingtree.AttributeType, "timer")
		tracingtree.RequireTimerFiredAt(t, timer)
	}
	// Timer IDs are the scheduling task IDs, so the two timers must be distinct spans.
	tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.TimerSpanName),
		tracingtree.Instance(sender),
		tracingtree.TaskID(0))
	tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.TimerSpanName),
		tracingtree.Instance(sender),
		tracingtree.TaskID(2))

	// An event sent by an orchestration is annotated on the receiving orchestration's span.
	// The sender's span carries no annotation for it, and no span links the two
	// orchestrations: cross-instance event causality is not modeled by OTel spans today.
	receiverSpan := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceEventReceiver", "")),
		tracingtree.Instance(receiver))
	tracingtree.RequireExternalEvent(t, receiverSpan, "ping", len(`"pong"`))
	tracingtree.RequireEvents(t, senderSpan, tracingtree.EventExternalEvent, 0)
	receiverCreated := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceEventReceiver", "")),
		tracingtree.Instance(receiver))
	tracingtree.RequireChildOf(t, receiverCreated, receiverSpan)
	tracingtree.RequireNone(t, spans,
		tracingtree.Name(tracingtree.TimerSpanName),
		tracingtree.Instance(receiver))
}

// Test_Grpc_TracingTree_VersionMigrationContinueAsNew covers version-aware span naming and
// the continue-as-new span chain. It uses a dedicated versioned stack because the shared
// gRPC client and worker are unversioned.
func Test_Grpc_TracingTree_VersionMigrationContinueAsNew(t *testing.T) {
	exporter := tracingtree.Init()

	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	server := grpc.NewServer()
	executor, register := backend.NewGrpcExecutor(be, logger)
	register(server)
	hubWorker := backend.NewTaskHubWorker(
		be,
		backend.NewOrchestrationWorker(be, executor, logger),
		backend.NewActivityTaskWorker(be, executor, logger),
		logger,
	)
	require.NoError(t, hubWorker.Start(testCtx))

	connection := serveBufconn(t, server, "tracing-tree-bufconn")

	versionedClient := client.NewTaskHubGrpcClient(
		connection,
		logger,
		client.WithLegacyOrchestrationIDReusePolicyWire(),
		client.WithDefaultVersion("1.0"),
	)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityNVersion("TraceVersionedActivity", "1.0", func(task.ActivityContext) (any, error) {
		return "v1-activity", nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("TraceVersioned", "1.0", func(ctx *task.OrchestrationContext) (any, error) {
		var output string
		if err := ctx.CallActivity("TraceVersionedActivity").Await(&output); err != nil {
			return nil, err
		}
		ctx.ContinueAsNew(output, task.WithContinueAsNewVersion("2.0"))
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("TraceVersioned", "2.0", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + "+v2", nil
	}))
	worker, err := client.NewTaskHubGrpcWorker(
		connection,
		registry,
		logger,
		client.WithTaskVersioning(task.VersioningOptions{DefaultVersion: "1.0"}),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(testCtx))
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, hubWorker.Shutdown(shutdownCtx))
		require.NoError(t, connection.Close())
		server.Stop()
	})

	const callerSpanName = "caller/version-migration"
	callerCtx, callerSpan := startCallerSpan(t, testCtx, callerSpanName)
	id, err := versionedClient.ScheduleNewOrchestration(
		callerCtx,
		"TraceVersioned",
		api.WithInstanceID("trace-version-migration"),
	)
	require.NoError(t, err)
	callerSpan.End()

	metadata, err := versionedClient.WaitForOrchestrationCompletion(testCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	require.Equal(t, "2.0", metadata.Version)
	require.Equal(t, `"v1-activity+v2"`, metadata.SerializedOutput)

	spans := exporter.GetSpans().Snapshots()
	caller := requireCallerSpan(t, spans, callerSpanName)

	// Versioned tasks encode the version in the span name and in an attribute.
	created := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceVersioned", "1.0")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, caller, created)
	tracingtree.RequireStringAttribute(t, created, tracingtree.AttributeVersion, "1.0")

	firstGeneration := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceVersioned", "1.0")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, created, firstGeneration)
	tracingtree.RequireStringAttribute(t, firstGeneration, tracingtree.AttributeRuntimeStatus, "CONTINUED_AS_NEW")
	tracingtree.RequireStringAttribute(t, firstGeneration, tracingtree.AttributeVersion, "1.0")

	activity := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.ActivitySpanName("TraceVersionedActivity", "1.0")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, firstGeneration, activity)
	tracingtree.RequireStringAttribute(t, activity, tracingtree.AttributeVersion, "1.0")

	// The migrated generation is a sibling execution span that stays inside the caller's
	// trace by inheriting the original create_orchestration span as its parent.
	secondGeneration := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceVersioned", "2.0")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, created, secondGeneration)
	tracingtree.RequireStringAttribute(t, secondGeneration, tracingtree.AttributeRuntimeStatus, "COMPLETED")
	tracingtree.RequireStringAttribute(t, secondGeneration, tracingtree.AttributeVersion, "2.0")

	// Exactly two execution spans for the instance: one per generation. Replays of either
	// generation must not export additional spans, and the migrated generation must not
	// re-run the 1.0 activity.
	tracingtree.RequireCount(t, spans, 2, tracingtree.OrchestrationExecutions(id)...)
	tracingtree.RequireOne(t, spans, append(
		tracingtree.OrchestrationExecutions(id),
		tracingtree.RuntimeStatus("CONTINUED_AS_NEW"))...)
	tracingtree.RequireOne(t, spans, tracingtree.Type("activity"), tracingtree.Instance(id))
	tracingtree.RequireNone(t, spans,
		tracingtree.Name(tracingtree.ActivitySpanName("TraceVersionedActivity", "2.0")))
}

// Test_Grpc_TracingTree_ContinueAsNewGenerations covers plain (same-version)
// continue-as-new: every generation exports exactly one execution span, all rooted in the
// single create_orchestration span, and each generation's timer belongs to its own span.
func Test_Grpc_TracingTree_ContinueAsNewGenerations(t *testing.T) {
	exporter := tracingtree.Init()

	const generations = 2
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("TraceContinueAsNew", func(ctx *task.OrchestrationContext) (any, error) {
		var generation int
		if err := ctx.GetInput(&generation); err != nil {
			return nil, err
		}
		if generation < generations {
			if err := ctx.CreateTimer(time.Millisecond).Await(nil); err != nil {
				return nil, err
			}
			ctx.ContinueAsNew(generation + 1)
			return nil, nil
		}
		return generation, nil
	}))
	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 60*time.Second)
	defer cancelTimeout()

	const callerSpanName = "caller/continue-as-new"
	callerCtx, callerSpan := startCallerSpan(t, timeoutCtx, callerSpanName)
	id, err := grpcClient.ScheduleNewOrchestration(
		callerCtx,
		"TraceContinueAsNew",
		api.WithInstanceID("trace-continue-as-new"),
		api.WithInput(0),
	)
	require.NoError(t, err)
	callerSpan.End()

	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	require.Equal(t, "2", metadata.SerializedOutput)

	spans := exporter.GetSpans().Snapshots()
	caller := requireCallerSpan(t, spans, callerSpanName)

	// Continue-as-new never re-creates the orchestration, so there is exactly one
	// create_orchestration span for all generations.
	created := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceContinueAsNew", "")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, caller, created)

	executions := tracingtree.RequireCount(t, spans, generations+1, tracingtree.OrchestrationExecutions(id)...)
	for _, execution := range executions {
		tracingtree.RequireChildOf(t, created, execution)
		tracingtree.RequireKind(t, execution, trace.SpanKindServer)
	}
	tracingtree.RequireCount(t, spans, generations, append(
		tracingtree.OrchestrationExecutions(id),
		tracingtree.RuntimeStatus("CONTINUED_AS_NEW"))...)
	tracingtree.RequireOne(t, spans, append(
		tracingtree.OrchestrationExecutions(id),
		tracingtree.RuntimeStatus("COMPLETED"))...)

	// One timer span per continuing generation, each parented to a distinct execution span.
	timers := tracingtree.RequireCount(t, spans, generations,
		tracingtree.Name(tracingtree.TimerSpanName),
		tracingtree.Instance(id))
	timerParents := map[string]bool{}
	for _, timer := range timers {
		tracingtree.RequireTimerFiredAt(t, timer)
		parentID := timer.Parent().SpanID().String()
		require.Falsef(t, timerParents[parentID], "two timer spans share the parent span %s", parentID)
		timerParents[parentID] = true
	}
}

// Test_Grpc_TracingTree_UnparentedCallerStartsNewTrace documents that scheduling an
// orchestration without an ambient span still produces a well-formed tree; the
// create_orchestration span simply becomes the trace root.
func Test_Grpc_TracingTree_UnparentedCallerStartsNewTrace(t *testing.T) {
	exporter := tracingtree.Init()

	r := task.NewTaskRegistry()
	require.NoError(t, r.AddOrchestratorN("TraceRootless", func(ctx *task.OrchestrationContext) (any, error) {
		var output string
		if err := ctx.CallActivity("TraceRootlessActivity").Await(&output); err != nil {
			return nil, err
		}
		return output, nil
	}))
	require.NoError(t, r.AddActivityN("TraceRootlessActivity", func(task.ActivityContext) (any, error) {
		return "done", nil
	}))
	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 60*time.Second)
	defer cancelTimeout()
	id, err := grpcClient.ScheduleNewOrchestration(
		timeoutCtx,
		"TraceRootless",
		api.WithInstanceID("trace-rootless"),
	)
	require.NoError(t, err)
	metadata, err := grpcClient.WaitForOrchestrationCompletion(timeoutCtx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

	spans := exporter.GetSpans().Snapshots()
	created := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.CreateOrchestrationSpanName("TraceRootless", "")),
		tracingtree.Instance(id))
	require.False(t, created.Parent().IsValid(), "create_orchestration should be a trace root without a caller span")

	orchestration := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.OrchestrationSpanName("TraceRootless", "")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, created, orchestration)
	activity := tracingtree.RequireOne(t, spans,
		tracingtree.Name(tracingtree.ActivitySpanName("TraceRootlessActivity", "")),
		tracingtree.Instance(id))
	tracingtree.RequireChildOf(t, orchestration, activity)
	tracingtree.RequireStatus(t, activity, codes.Unset, "")
}
