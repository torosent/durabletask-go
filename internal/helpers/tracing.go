package helpers

import (
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/protos"
)

// TraceContextFromSpan converts a sampled OpenTelemetry span into the W3C trace
// context sent to Durable Task Scheduler. Unsampled or invalid spans return nil.
func TraceContextFromSpan(span trace.Span) *protos.TraceContext {
	if span == nil || !span.SpanContext().IsSampled() {
		return nil
	}
	spanContext := span.SpanContext()
	if !spanContext.IsValid() {
		return nil
	}
	traceContext := &protos.TraceContext{
		TraceParent: "00-" + spanContext.TraceID().String() + "-" +
			spanContext.SpanID().String() + "-" + spanContext.TraceFlags().String(),
	}
	if traceState := spanContext.TraceState().String(); traceState != "" {
		traceContext.TraceState = wrapperspb.String(traceState)
	}
	return traceContext
}
