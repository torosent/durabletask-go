// Package tracingtree provides shared OpenTelemetry span-tree assertions so the
// embedded, gRPC, and Durable Task Scheduler surfaces can validate the same
// distributed tracing contract.
//
// The helpers deliberately match spans by attribute instead of by export order.
// Orchestration, activity, and timer spans are produced by concurrent workers,
// so any assertion that depends on the global export order is inherently flaky
// once more than one instance is in flight. Matching by instance ID and task ID
// keeps the assertions robust while still allowing exact parent/child, span
// kind, status, and event checks.
package tracingtree

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/microsoft/durabletask-go/api"
)

// Span attribute keys emitted by internal/helpers/tracing.go.
const (
	AttributeType          = "durabletask.type"
	AttributeTaskID        = "durabletask.task.task_id"
	attributeInstanceID    = "durabletask.task.instance_id"
	AttributeVersion       = "durabletask.task.version"
	AttributeRuntimeStatus = "durabletask.runtime_status"
	attributeFireAt        = "durabletask.fire_at"
)

// Span event names emitted by backend/orchestration.go.
const (
	EventExternalEvent = "Received external event"
	EventSuspended     = "Execution suspended"
	EventResumed       = "Execution resumed"
)

// TimerSpanName is the span name used for durable timers, which are unnamed tasks.
const TimerSpanName = "timer"

var (
	initOnce       sync.Once
	sharedExporter = tracetest.NewInMemoryExporter()
)

// Init installs an in-memory tracer provider as the global OTel provider and
// resets the previously collected spans. The global provider can only be
// installed once per process, so the exporter is shared and reset per test.
//
// Only exported spans are observable, which is intentional: replayed
// orchestration executions are marked as unsampled and must never be exported.
func Init() *tracetest.InMemoryExporter {
	initOnce.Do(func() {
		processor := sdktrace.NewSimpleSpanProcessor(sharedExporter)
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor)))
	})
	sharedExporter.Reset()
	return sharedExporter
}

// StartCallerSpan starts the client span that stands in for the application
// code scheduling an orchestration. The orchestration's whole span tree must
// hang off it, so the span is asserted to be sampled: an unsampled caller span
// would silently stop propagating trace context.
func StartCallerSpan(
	t require.TestingT,
	tracerName string,
	parent context.Context,
	name string,
) (context.Context, trace.Span) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	callerCtx, span := otel.Tracer(tracerName).Start(parent, name, trace.WithSpanKind(trace.SpanKindClient))
	require.True(t, span.SpanContext().IsSampled(), "the caller span must be sampled for trace propagation")
	return callerCtx, span
}

// CreateOrchestrationSpanName returns the expected create_orchestration span name.
func CreateOrchestrationSpanName(name string, version string) string {
	return spanName("create_orchestration", name, version)
}

// OrchestrationSpanName returns the expected orchestration execution span name.
func OrchestrationSpanName(name string, version string) string {
	return spanName("orchestration", name, version)
}

// ActivitySpanName returns the expected activity execution span name.
func ActivitySpanName(name string, version string) string {
	return spanName("activity", name, version)
}

func spanName(spanType string, taskName string, version string) string {
	switch {
	case version != "":
		return spanType + "||" + taskName + "||" + version
	case taskName != "":
		return spanType + "||" + taskName
	default:
		return spanType
	}
}

// Matcher narrows a set of exported spans.
type Matcher struct {
	description string
	match       func(sdktrace.ReadOnlySpan) bool
}

func attributeMatcher(key string, value attribute.Value) Matcher {
	return Matcher{
		description: fmt.Sprintf("%s=%s", key, value.Emit()),
		match: func(span sdktrace.ReadOnlySpan) bool {
			for _, kv := range span.Attributes() {
				if string(kv.Key) == key {
					return kv.Value == value
				}
			}
			return false
		},
	}
}

// Name matches a span by its exact span name.
func Name(name string) Matcher {
	return Matcher{
		description: fmt.Sprintf("name=%s", name),
		match:       func(span sdktrace.ReadOnlySpan) bool { return span.Name() == name },
	}
}

// namePrefix matches spans whose name starts with the supplied prefix. It is the
// reliable way to select execution spans of a given kind, because the durabletask.type
// attribute is "orchestration" for both create_orchestration and orchestration spans.
func namePrefix(prefix string) Matcher {
	return Matcher{
		description: fmt.Sprintf("name~%s*", prefix),
		match:       func(span sdktrace.ReadOnlySpan) bool { return strings.HasPrefix(span.Name(), prefix) },
	}
}

// OrchestrationExecutions matches every orchestration execution span (but not the
// create_orchestration span) belonging to an instance.
func OrchestrationExecutions(id api.InstanceID) []Matcher {
	return []Matcher{namePrefix("orchestration||"), Instance(id)}
}

// Instance matches spans belonging to a specific orchestration instance.
func Instance(id api.InstanceID) Matcher {
	return attributeMatcher(attributeInstanceID, attribute.StringValue(string(id)))
}

// TaskID matches spans by the durabletask.task.task_id attribute.
func TaskID(taskID int64) Matcher {
	return attributeMatcher(AttributeTaskID, attribute.Int64Value(taskID))
}

// Type matches spans by the durabletask.type attribute ("orchestration",
// "activity", or "timer").
func Type(taskType string) Matcher {
	return attributeMatcher(AttributeType, attribute.StringValue(taskType))
}

// RuntimeStatus matches orchestration spans by their terminal runtime status.
func RuntimeStatus(status string) Matcher {
	return attributeMatcher(AttributeRuntimeStatus, attribute.StringValue(status))
}

// all returns every span matching all of the supplied matchers.
func all(spans []sdktrace.ReadOnlySpan, matchers ...Matcher) []sdktrace.ReadOnlySpan {
	matched := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, span := range spans {
		if matchesAll(span, matchers) {
			matched = append(matched, span)
		}
	}
	return matched
}

func matchesAll(span sdktrace.ReadOnlySpan, matchers []Matcher) bool {
	for _, matcher := range matchers {
		if !matcher.match(span) {
			return false
		}
	}
	return true
}

// RequireOne asserts that exactly one exported span matches, which doubles as
// the assertion that replayed executions never export duplicate spans.
func RequireOne(t require.TestingT, spans []sdktrace.ReadOnlySpan, matchers ...Matcher) sdktrace.ReadOnlySpan {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	matched := all(spans, matchers...)
	require.Equalf(t, 1, len(matched), "expected exactly one span matching [%s]; exported spans: %s",
		describe(matchers), Describe(spans))
	return matched[0]
}

// RequireCount asserts the exact number of spans matching the supplied matchers.
func RequireCount(t require.TestingT, spans []sdktrace.ReadOnlySpan, want int, matchers ...Matcher) []sdktrace.ReadOnlySpan {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	matched := all(spans, matchers...)
	require.Equalf(t, want, len(matched), "expected %d spans matching [%s]; exported spans: %s",
		want, describe(matchers), Describe(spans))
	return matched
}

// RequireNone asserts that no exported span matches the supplied matchers.
func RequireNone(t require.TestingT, spans []sdktrace.ReadOnlySpan, matchers ...Matcher) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	RequireCount(t, spans, 0, matchers...)
}

// RequireChildOf asserts that child is a direct descendant of parent within the
// same trace.
func RequireChildOf(t require.TestingT, parent sdktrace.ReadOnlySpan, child sdktrace.ReadOnlySpan) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	require.Equalf(t, parent.SpanContext().TraceID().String(), child.SpanContext().TraceID().String(),
		"span %q is not in the same trace as %q", child.Name(), parent.Name())
	require.Equalf(t, parent.SpanContext().SpanID().String(), child.Parent().SpanID().String(),
		"span %q is not a child of %q", child.Name(), parent.Name())
}

// RequireKind asserts the OTel span kind, which distinguishes client-side
// scheduling spans from server-side execution spans.
func RequireKind(t require.TestingT, span sdktrace.ReadOnlySpan, kind trace.SpanKind) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	require.Equalf(t, kind, span.SpanKind(), "unexpected span kind for %q", span.Name())
}

// RequireStatus asserts the OTel status code and, when messageContains is not
// empty, that the status description contains the supplied fragment.
func RequireStatus(t require.TestingT, span sdktrace.ReadOnlySpan, code codes.Code, messageContains string) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	require.Equalf(t, code, span.Status().Code, "unexpected status code for %q (description=%q)",
		span.Name(), span.Status().Description)
	if messageContains != "" {
		require.Containsf(t, span.Status().Description, messageContains, "unexpected status description for %q", span.Name())
	}
}

// requireAttribute asserts a single span attribute value.
func requireAttribute(t require.TestingT, span sdktrace.ReadOnlySpan, key string, value attribute.Value) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	require.Truef(t, attributeMatcher(key, value).match(span),
		"span %q is missing attribute %s=%s; attributes: %v", span.Name(), key, value.Emit(), span.Attributes())
}

// RequireStringAttribute asserts a string-valued span attribute.
func RequireStringAttribute(t require.TestingT, span sdktrace.ReadOnlySpan, key string, value string) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	requireAttribute(t, span, key, attribute.StringValue(value))
}

// RequireInt64Attribute asserts an int64-valued span attribute.
func RequireInt64Attribute(t require.TestingT, span sdktrace.ReadOnlySpan, key string, value int64) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	requireAttribute(t, span, key, attribute.Int64Value(value))
}

// RequireTimerFiredAt asserts that a timer span carries a parsable durabletask.fire_at
// timestamp in a plausible range. The exact instant is timing dependent and is not asserted.
func RequireTimerFiredAt(t require.TestingT, span sdktrace.ReadOnlySpan) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	var fireAt string
	for _, kv := range span.Attributes() {
		if string(kv.Key) == attributeFireAt {
			fireAt = kv.Value.AsString()
			break
		}
	}
	require.NotEmptyf(t, fireAt, "timer span %q is missing the %s attribute", span.Name(), attributeFireAt)
	parsed, err := time.Parse(time.RFC3339, fireAt)
	require.NoErrorf(t, err, "timer span %q has an unparsable %s value %q", span.Name(), attributeFireAt, fireAt)
	now := time.Now().UTC()
	require.Truef(t, parsed.After(now.Add(-time.Hour)) && parsed.Before(now.Add(time.Hour)),
		"timer span %q fired at %s, which is outside the expected range around %s", span.Name(), parsed, now)
}

// RequireNoAttribute asserts that a span does not carry the supplied attribute key.
func RequireNoAttribute(t require.TestingT, span sdktrace.ReadOnlySpan, key string) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	for _, kv := range span.Attributes() {
		require.NotEqualf(t, key, string(kv.Key), "span %q unexpectedly carries attribute %s", span.Name(), key)
	}
}

// RequireEvents asserts the exact number of span events with the supplied name
// and returns them in recorded order.
func RequireEvents(t require.TestingT, span sdktrace.ReadOnlySpan, name string, want int) []sdktrace.Event {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	matched := eventsNamed(span, name)
	require.Equalf(t, want, len(matched), "expected %d %q events on span %q; events: %s",
		want, name, span.Name(), describeEvents(span))
	return matched
}

// RequireExternalEvent asserts that a span carries exactly one "Received
// external event" annotation for the supplied event name and payload size.
func RequireExternalEvent(t require.TestingT, span sdktrace.ReadOnlySpan, eventName string, payloadSize int) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	matched := make([]sdktrace.Event, 0, 1)
	for _, event := range eventsNamed(span, EventExternalEvent) {
		if hasAttribute(event, "name", attribute.StringValue(eventName)) {
			matched = append(matched, event)
		}
	}
	require.Equalf(t, 1, len(matched), "expected exactly one %q annotation named %q on span %q; events: %s",
		EventExternalEvent, eventName, span.Name(), describeEvents(span))
	require.Truef(t, hasAttribute(matched[0], "size", attribute.IntValue(payloadSize)),
		"unexpected payload size for external event %q; attributes: %v", eventName, matched[0].Attributes)
}

func eventsNamed(span sdktrace.ReadOnlySpan, name string) []sdktrace.Event {
	matched := make([]sdktrace.Event, 0, len(span.Events()))
	for _, event := range span.Events() {
		if event.Name == name {
			matched = append(matched, event)
		}
	}
	return matched
}

func hasAttribute(event sdktrace.Event, key string, value attribute.Value) bool {
	for _, kv := range event.Attributes {
		if string(kv.Key) == key {
			return kv.Value == value
		}
	}
	return false
}

// Describe renders exported spans for assertion failure messages.
func Describe(spans []sdktrace.ReadOnlySpan) string {
	if len(spans) == 0 {
		return "(none)"
	}
	descriptions := make([]string, 0, len(spans))
	for _, span := range spans {
		instanceID := ""
		for _, kv := range span.Attributes() {
			if string(kv.Key) == attributeInstanceID {
				instanceID = kv.Value.AsString()
				break
			}
		}
		descriptions = append(descriptions, fmt.Sprintf("%s(instance=%s)", span.Name(), instanceID))
	}
	return strings.Join(descriptions, ", ")
}

func describeEvents(span sdktrace.ReadOnlySpan) string {
	if len(span.Events()) == 0 {
		return "(none)"
	}
	descriptions := make([]string, 0, len(span.Events()))
	for _, event := range span.Events() {
		descriptions = append(descriptions, fmt.Sprintf("%s%v", event.Name, event.Attributes))
	}
	return strings.Join(descriptions, ", ")
}

func describe(matchers []Matcher) string {
	descriptions := make([]string, 0, len(matchers))
	for _, matcher := range matchers {
		descriptions = append(descriptions, matcher.description)
	}
	return strings.Join(descriptions, " && ")
}
