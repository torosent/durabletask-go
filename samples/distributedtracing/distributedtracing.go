// Command distributedtracing starts an application caller span, propagates its
// W3C trace context into Durable Task Scheduler, and exports application-process
// spans to a local Zipkin collector. DTS emits orchestration, activity, and timer
// telemetry service-side.
//
//	export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
//	go run ./samples/distributedtracing
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Tracing can be configured independently of the orchestration code.
	tp, err := ConfigureZipkinTracing()
	if err != nil {
		return fmt.Errorf("failed to create tracer: %w", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Failed to stop tracer: %v", err)
		}
	}()

	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("DistributedTraceSampleOrchestrator", DistributedTraceSampleOrchestrator); err != nil {
		return fmt.Errorf("failed to register orchestrator: %w", err)
	}
	if err := r.AddActivityN("DoWorkActivity", DoWorkActivity); err != nil {
		return fmt.Errorf("failed to register activity: %w", err)
	}
	if err := r.AddActivityN("CallHttpEndpointActivity", CallHttpEndpointActivity); err != nil {
		return fmt.Errorf("failed to register activity: %w", err)
	}

	// Connect a client and worker to the Durable Task Scheduler task hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, r)
	if err != nil {
		return err
	}
	defer func() {
		if err := app.Shutdown(); err != nil {
			log.Printf("Failed to shut down: %v", err)
		}
	}()

	// A sampled caller span is propagated to DTS when the orchestration is
	// scheduled, allowing service-side spans to join the application's trace.
	callerCtx, callerSpan := otel.Tracer("durabletask-sample").Start(
		ctx,
		"schedule_distributed_trace_sample",
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
	)
	defer callerSpan.End()
	id, err := app.Client.ScheduleNewOrchestration(callerCtx, "DistributedTraceSampleOrchestrator")
	if err != nil {
		return fmt.Errorf("failed to schedule new orchestration: %w", err)
	}

	// Wait for the orchestration to complete
	metadata, err := app.Client.WaitForOrchestrationCompletion(callerCtx, id)
	if err != nil {
		return fmt.Errorf("failed to wait for orchestration to complete: %w", err)
	}

	// Print the results
	metadataEnc, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode result to JSON: %w", err)
	}
	log.Printf("Orchestration completed: %v", string(metadataEnc))
	return nil
}

func ConfigureZipkinTracing() (*sdktrace.TracerProvider, error) {
	// Inspired by this sample: https://github.com/open-telemetry/opentelemetry-go/blob/main/example/zipkin/main.go
	exp, err := zipkin.New("http://localhost:9411/api/v2/spans")
	if err != nil {
		return nil, err
	}

	// NOTE: The simple span processor is not recommended for production.
	//       Instead, the batch span processor should be used for production.
	processor := sdktrace.NewSimpleSpanProcessor(exp)
	// processor := sdktrace.NewBatchSpanProcessor(exp)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(
			"durabletask.io",
			attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("sample-app")},
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}

// DistributedTraceSampleOrchestrator is a simple orchestration that's intended to generate
// distributed trace output to the configured exporter (e.g. zipkin).
func DistributedTraceSampleOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	if err := ctx.CallActivity("DoWorkActivity", task.WithActivityInput(1*time.Second)).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(2 * time.Second).Await(nil); err != nil {
		return nil, err
	}
	if err := ctx.CallActivity("CallHttpEndpointActivity", task.WithActivityInput("https://bing.com")).Await(nil); err != nil {
		return nil, err
	}
	return nil, nil
}

// DoWorkActivity is a no-op activity function that sleeps for a specified amount of time.
func DoWorkActivity(ctx task.ActivityContext) (any, error) {
	var duration time.Duration
	if err := ctx.GetInput(&duration); err != nil {
		return "", err
	}

	// Simulate doing work
	select {
	case <-time.After(duration):
		// Ok
	case <-ctx.Context().Done():
		return nil, ctx.Context().Err()
	}

	return nil, nil
}

func CallHttpEndpointActivity(ctx task.ActivityContext) (any, error) {
	var url string
	if err := ctx.GetInput(&url); err != nil {
		return "", err
	}

	// The OTel HTTP client records the outbound request in the worker process.
	// DTS owns the service-side activity span.
	_, err := otelhttp.Get(ctx.Context(), url)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
