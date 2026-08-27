package task

import (
	"context"
	"log/slog"

	"github.com/microsoft/durabletask-go/api"
)

type taskLoggerKey struct{}

func mergeContextFields(base, overrides api.ContextFields) api.ContextFields {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}
	merged := make(api.ContextFields, len(base)+len(overrides))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}
	return merged
}

// Context returns an immutable Go context with orchestration identity and caller fields.
func (ctx *OrchestrationContext) Context() context.Context {
	engine := ctx.engineContext()
	base := engine.baseContext
	if base == nil {
		base = context.Background()
	}
	base = api.ContextWithFields(base, engine.contextFields)
	base = api.WithOrchestrationContextInfo(base, api.OrchestrationContextInfo{
		InstanceID:       engine.ID,
		Name:             engine.Name,
		Version:          engine.Version,
		ParentInstanceID: engine.parentInstanceID,
	})
	return context.WithValue(base, taskLoggerKey{}, ctx.Logger())
}

// Logger returns a slog logger that suppresses output while replaying history.
func (ctx *OrchestrationContext) Logger() *slog.Logger {
	engine := ctx.engineContext()
	logger := engine.logger
	if logger == nil {
		logger = slog.Default()
	}
	handler := &replaySafeHandler{
		handler: logger.Handler(),
		replaying: func() bool {
			return engine.IsReplaying
		},
	}
	return slog.New(handler).With(
		slog.String("durabletask.instance_id", string(engine.ID)),
		slog.String("durabletask.orchestration.name", engine.Name),
		slog.String("durabletask.orchestration.version", engine.Version),
	)
}

// LoggerFromContext returns the task logger associated with ctx, or slog.Default.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(taskLoggerKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return slog.Default()
}

func withActivityLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		logger = slog.Default()
	}
	attrs := make([]any, 0, 10)
	if orchestration, ok := api.OrchestrationContextInfoFromContext(ctx); ok {
		attrs = append(attrs,
			slog.String("durabletask.instance_id", string(orchestration.InstanceID)),
			slog.String("durabletask.orchestration.name", orchestration.Name),
			slog.String("durabletask.orchestration.version", orchestration.Version),
		)
	}
	if activity, ok := api.ActivityContextInfoFromContext(ctx); ok {
		attrs = append(attrs,
			slog.String("durabletask.activity.name", activity.Name),
			slog.String("durabletask.activity.version", activity.Version),
			slog.Int("durabletask.activity.task_id", int(activity.TaskID)),
		)
	}
	return context.WithValue(ctx, taskLoggerKey{}, logger.With(attrs...))
}

type replaySafeHandler struct {
	handler   slog.Handler
	replaying func() bool
}

func (h *replaySafeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return !h.replaying() && h.handler.Enabled(ctx, level)
}

func (h *replaySafeHandler) Handle(ctx context.Context, record slog.Record) error {
	if h.replaying() {
		return nil
	}
	return h.handler.Handle(ctx, record)
}

func (h *replaySafeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &replaySafeHandler{handler: h.handler.WithAttrs(attrs), replaying: h.replaying}
}

func (h *replaySafeHandler) WithGroup(name string) slog.Handler {
	return &replaySafeHandler{handler: h.handler.WithGroup(name), replaying: h.replaying}
}
