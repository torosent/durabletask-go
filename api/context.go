package api

import "context"

// ContextFields are immutable caller-supplied values propagated into task contexts.
type ContextFields map[string]string

// OrchestrationContextInfo identifies the orchestration associated with a task context.
type OrchestrationContextInfo struct {
	InstanceID       InstanceID
	Name             string
	Version          string
	ParentInstanceID InstanceID
}

// ActivityContextInfo identifies the activity associated with a task context.
type ActivityContextInfo struct {
	InstanceID InstanceID
	Name       string
	Version    string
	TaskID     int32
}

type orchestrationContextInfoKey struct{}
type activityContextInfoKey struct{}
type contextFieldsKey struct{}

// WithOrchestrationContextInfo returns a context containing orchestration identity.
func WithOrchestrationContextInfo(ctx context.Context, info OrchestrationContextInfo) context.Context {
	return context.WithValue(ctx, orchestrationContextInfoKey{}, info)
}

// OrchestrationContextInfoFromContext returns orchestration identity from ctx.
func OrchestrationContextInfoFromContext(ctx context.Context) (OrchestrationContextInfo, bool) {
	info, ok := ctx.Value(orchestrationContextInfoKey{}).(OrchestrationContextInfo)
	return info, ok
}

// WithActivityContextInfo returns a context containing activity identity.
func WithActivityContextInfo(ctx context.Context, info ActivityContextInfo) context.Context {
	return context.WithValue(ctx, activityContextInfoKey{}, info)
}

// ActivityContextInfoFromContext returns activity identity from ctx.
func ActivityContextInfoFromContext(ctx context.Context) (ActivityContextInfo, bool) {
	info, ok := ctx.Value(activityContextInfoKey{}).(ActivityContextInfo)
	return info, ok
}

// WithContextFields returns a context containing a defensive copy of fields.
func WithContextFields(ctx context.Context, fields ContextFields) context.Context {
	if len(fields) == 0 {
		return ctx
	}
	merged := ContextFieldsFromContext(ctx)
	if merged == nil {
		merged = make(ContextFields, len(fields))
	}
	for key, value := range fields {
		merged[key] = value
	}
	return context.WithValue(ctx, contextFieldsKey{}, merged)
}

// ContextFieldsFromContext returns a defensive copy of caller-supplied fields.
func ContextFieldsFromContext(ctx context.Context) ContextFields {
	fields, ok := ctx.Value(contextFieldsKey{}).(ContextFields)
	if !ok || len(fields) == 0 {
		return nil
	}
	copyOfFields := make(ContextFields, len(fields))
	for key, value := range fields {
		copyOfFields[key] = value
	}
	return copyOfFields
}
