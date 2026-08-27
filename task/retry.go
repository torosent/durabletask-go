package task

import (
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/backend"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type retryTaskInfo struct {
	kind    backend.WorkItemKind
	name    string
	version string
}

func (ctx *OrchestrationContext) reportRetry(
	info retryTaskInfo,
	failedAttempt int,
	policy RetryPolicy,
	delay time.Duration,
	err error,
) {
	engine := ctx.engineContext()
	trace.SpanFromContext(engine.Context()).AddEvent(
		"Retry scheduled",
		trace.WithTimestamp(engine.CurrentTimeUtc),
		trace.WithAttributes(
			attribute.String("durabletask.retry.task_kind", string(info.kind)),
			attribute.String("durabletask.retry.task_name", info.name),
			attribute.String("durabletask.retry.task_version", info.version),
			attribute.Int("durabletask.retry.failed_attempt", failedAttempt),
			attribute.Int("durabletask.retry.next_attempt", failedAttempt+1),
			attribute.Int("durabletask.retry.max_attempts", policy.MaxAttempts),
			attribute.Int64("durabletask.retry.delay_ms", delay.Milliseconds()),
			attribute.String("error.type", fmt.Sprintf("%T", err)),
			attribute.String("error.message", err.Error()),
		),
	)

	if engine.IsReplaying || engine.metrics.Retry == nil {
		return
	}
	metric := backend.RetryMetric{
		InstanceID:           engine.ID,
		OrchestrationName:    engine.Name,
		OrchestrationVersion: engine.Version,
		TaskKind:             info.kind,
		TaskName:             info.name,
		TaskVersion:          info.version,
		FailedAttempt:        failedAttempt,
		NextAttempt:          failedAttempt + 1,
		MaxAttempts:          policy.MaxAttempts,
		Delay:                delay,
		ErrorType:            fmt.Sprintf("%T", err),
		ErrorMessage:         err.Error(),
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			engine.Logger().Error("retry metrics callback panicked", "error", recovered)
		}
	}()
	engine.metrics.Retry(metric)
}
