package task

import (
	"errors"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
)

// ErrHistoryLimitExceeded is the sentinel error for deterministic history-budget failures.
var ErrHistoryLimitExceeded = errors.New("orchestration history limit exceeded")

// HistoryLimitError describes the history budget that was exceeded.
type HistoryLimitError struct {
	InstanceID              api.InstanceID
	HistoryLength           int
	MaxHistoryEvents        int
	ProcessedEventsThisTurn int
	MaxEventsPerTurn        int
	PolicyError             error
}

func (e *HistoryLimitError) Error() string {
	message := fmt.Sprintf(
		"%v for instance %q: history=%d max_history=%d processed_this_turn=%d max_per_turn=%d",
		ErrHistoryLimitExceeded,
		e.InstanceID,
		e.HistoryLength,
		e.MaxHistoryEvents,
		e.ProcessedEventsThisTurn,
		e.MaxEventsPerTurn,
	)
	if e.PolicyError != nil {
		return fmt.Sprintf("%s: history limit handler failed: %v", message, e.PolicyError)
	}
	return message
}

func (e *HistoryLimitError) Unwrap() error {
	return ErrHistoryLimitExceeded
}

func (*HistoryLimitError) DurableTaskErrorType() api.ErrorType {
	return api.ErrorTypeHistoryLimitExceeded
}

func (*HistoryLimitError) NonRetriable() bool {
	return true
}

// HistoryLimitInfo is the immutable input passed to a history limit handler.
type HistoryLimitInfo struct {
	InstanceID               api.InstanceID
	OrchestrationName        string
	OrchestrationVersion     string
	HistoryLength            int
	MaxHistoryEvents         int
	ProcessedEventsThisTurn  int
	MaxEventsPerTurn         int
	MaxHistoryEventsExceeded bool
	MaxEventsPerTurnExceeded bool
	UnprocessedEventCount    int
	SerializedInput          string
	Converter                api.DataConverter
}

// GetInput unmarshals the current orchestration input.
func (info HistoryLimitInfo) GetInput(v any) error {
	if v == nil || info.SerializedInput == "" {
		return nil
	}
	return api.NormalizeDataConverter(info.Converter).Deserialize(info.SerializedInput, v)
}

// HistoryLimitHandler supplies safe input for a ContinueAsNew transition.
// It must be deterministic. Unconsumed external events are preserved automatically.
type HistoryLimitHandler func(HistoryLimitInfo) (any, error)

// OrchestrationOptions configures deterministic orchestration engine policies.
type OrchestrationOptions struct {
	// MaxEventsPerTurn limits new history events processed in one execution turn.
	// Zero disables the limit. Old replay history is always processed.
	MaxEventsPerTurn int

	// MaxHistoryEvents limits the total old and new history supplied to an execution.
	// Zero disables the limit.
	MaxHistoryEvents int

	// OnHistoryLimitExceeded opts into ContinueAsNew by supplying serializable state.
	// When nil, exceeding either limit fails with a non-retriable HistoryLimitExceeded failure.
	OnHistoryLimitExceeded HistoryLimitHandler
}
