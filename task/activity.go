package task

import (
	"context"
	"fmt"
	"maps"
	"math"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// CallActivityOption configures an activity invocation.
type CallActivityOption func(*callActivityOptions, api.DataConverter) error

type callActivityOptions struct {
	rawInput    *wrapperspb.StringValue
	version     *wrapperspb.StringValue
	retryPolicy *RetryPolicy
	tags        map[string]string
}

func (options *callActivityOptions) versionOrInherited(inheritedVersion string) *wrapperspb.StringValue {
	if options.version != nil {
		return options.version
	}
	// Preserve nil for implicit unversioned calls; an explicit empty version remains non-nil.
	if inheritedVersion == "" {
		return nil
	}
	return wrapperspb.String(inheritedVersion)
}

// WithActivityVersion configures the activity version.
func WithActivityVersion(version string) CallActivityOption {
	return func(opt *callActivityOptions, _ api.DataConverter) error {
		opt.version = wrapperspb.String(version)
		return nil
	}
}

type RetryPolicy struct {
	// Max number of attempts to try the activity call, first execution inclusive
	MaxAttempts int
	// Timespan to wait for the first retry
	InitialRetryInterval time.Duration
	// Used to determine rate of increase of back-off
	BackoffCoefficient float64
	// Max timespan to wait for a retry
	MaxRetryInterval time.Duration
	// Total timeout across all the retries performed
	RetryTimeout time.Duration
	// Optional deterministic function that controls whether retries should proceed.
	Handle func(RetryContext) bool
}

// RetryContext contains durable inputs for a retry decision.
// Retry handlers execute during replay and must not perform I/O or depend on wall-clock time.
type RetryContext struct {
	LastAttemptNumber int
	// LastFailure is never nil while Handle is running and must be treated as read-only.
	LastFailure    *api.FailureDetails
	TotalRetryTime time.Duration
}

func (policy *RetryPolicy) Validate() error {
	if policy.InitialRetryInterval <= 0 {
		return fmt.Errorf("%w: InitialRetryInterval must be greater than 0", api.ErrInvalidArgument)
	}
	if policy.MaxAttempts <= 0 {
		// setting 1 max attempt is equivalent to not retrying
		policy.MaxAttempts = 1
	}
	if policy.BackoffCoefficient <= 0 {
		policy.BackoffCoefficient = 1
	}
	if policy.MaxRetryInterval <= 0 {
		policy.MaxRetryInterval = math.MaxInt64
	}
	if policy.RetryTimeout <= 0 {
		policy.RetryTimeout = math.MaxInt64
	}
	if policy.Handle == nil {
		policy.Handle = func(RetryContext) bool {
			return true
		}
	}
	return nil
}

// WithActivityInput configures an input for an activity invocation.
// The configured data converter must be able to serialize the input.
func WithActivityInput(input any) CallActivityOption {
	return func(opt *callActivityOptions, converter api.DataConverter) error {
		data, err := marshalData(converter, input)
		if err != nil {
			return err
		}
		opt.rawInput = wrapperspb.String(string(data))
		return nil
	}
}

// WithRawActivityInput configures a raw input for an activity invocation.
func WithRawActivityInput(input string) CallActivityOption {
	return func(opt *callActivityOptions, _ api.DataConverter) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}

// WithActivityTags adds user tags to an activity invocation. Explicit activity
// tags override orchestration tags with the same key.
func WithActivityTags(tags map[string]string) CallActivityOption {
	return func(opt *callActivityOptions, _ api.DataConverter) error {
		if err := validateUnreservedKeys("activity tag", tags); err != nil {
			return err
		}
		opt.tags = maps.Clone(tags)
		return nil
	}
}

func WithActivityRetryPolicy(policy *RetryPolicy) CallActivityOption {
	return func(opt *callActivityOptions, _ api.DataConverter) error {
		if policy == nil {
			return nil
		}
		err := policy.Validate()
		if err != nil {
			return err
		}
		opt.retryPolicy = policy
		return nil
	}
}

// ActivityContext is the context parameter type for activity implementations.
type ActivityContext interface {
	GetInput(resultPtr any) error
	Context() context.Context
}

type activityContext struct {
	TaskID int32
	Name   string

	rawInput  []byte
	ctx       context.Context
	converter api.DataConverter
}

// Activity is the functional interface for activity implementations.
type Activity func(ctx ActivityContext) (any, error)

func newTaskActivityContext(
	ctx context.Context,
	taskID int32,
	ts *protos.TaskScheduledEvent,
	converter api.DataConverter,
) *activityContext {
	return &activityContext{
		TaskID:    taskID,
		Name:      ts.Name,
		rawInput:  []byte(ts.Input.GetValue()),
		ctx:       ctx,
		converter: converter,
	}
}

// GetInput unmarshals the serialized activity input and saves the result into [v].
func (actx *activityContext) GetInput(v any) error {
	return unmarshalData(actx.converter, actx.rawInput, v)
}

func (actx *activityContext) Context() context.Context {
	return actx.ctx
}
