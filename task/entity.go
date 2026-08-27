package task

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Entity is the functional interface for entity implementations.
// An entity function receives an EntityContext and returns a result and error.
type Entity func(ctx *EntityContext) (any, error)

// EntityContext provides the execution context for an entity operation.
type EntityContext struct {
	ID        api.EntityID
	Operation string
	RequestID string
	IsSignal  bool

	rawInput    []byte
	state       entityState
	stateDirty  bool
	actions     []*protos.OperationAction
	actionIDSeq int32
	currentTime time.Time
	ctx         context.Context
	logger      *slog.Logger
}

type entityState struct {
	value    []byte
	hasValue bool
}

// GetInput unmarshals the serialized entity operation input and saves the result into [v].
func (ctx *EntityContext) GetInput(v any) error {
	return unmarshalData(ctx.rawInput, v)
}

// HasState returns true if the entity has state set.
func (ctx *EntityContext) HasState() bool {
	return ctx.state.hasValue
}

// GetState unmarshals the entity state and saves the result into [v].
func (ctx *EntityContext) GetState(v any) error {
	if !ctx.state.hasValue {
		return fmt.Errorf("entity has no state")
	}
	return unmarshalData(ctx.state.value, v)
}

// SetState sets the entity state. The state must be JSON-serializable.
// Passing nil deletes the entity state.
func (ctx *EntityContext) SetState(state any) error {
	if state == nil {
		ctx.DeleteState()
		return nil
	}
	bytes, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal entity state: %w", err)
	}
	ctx.state.value = bytes
	ctx.state.hasValue = true
	ctx.stateDirty = true
	return nil
}

// DeleteState removes the entity state.
func (ctx *EntityContext) DeleteState() {
	ctx.stateDirty = true
	ctx.state.value = nil
	ctx.state.hasValue = false
}

// Context returns the Go context for the current entity operation.
func (ctx *EntityContext) Context() context.Context {
	if ctx.ctx == nil {
		return context.Background()
	}
	return ctx.ctx
}

// Logger returns a logger enriched with entity operation identity.
func (ctx *EntityContext) Logger() *slog.Logger {
	if ctx.logger == nil {
		ctx.logger = slog.Default()
	}
	return ctx.logger
}

// CurrentTimeUTC returns the durable timestamp associated with this operation.
func (ctx *EntityContext) CurrentTimeUTC() time.Time {
	return ctx.currentTime
}

// SignalEntity sends a fire-and-forget signal to another entity.
func (ctx *EntityContext) SignalEntity(entityID api.EntityID, operationName string, input any) error {
	return ctx.signalEntity(entityID, operationName, input, time.Time{})
}

// SignalEntityAt schedules a fire-and-forget signal to another entity.
func (ctx *EntityContext) SignalEntityAt(entityID api.EntityID, scheduledTime time.Time, operationName string, input any) error {
	if scheduledTime.IsZero() {
		return fmt.Errorf("scheduled entity signal time must not be zero")
	}
	return ctx.signalEntity(entityID, operationName, input, scheduledTime)
}

func (ctx *EntityContext) signalEntity(entityID api.EntityID, operationName string, input any, scheduledTime time.Time) error {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return err
	}
	if operationName == "" {
		return fmt.Errorf("entity operation name must not be empty")
	}

	var rawInput *wrapperspb.StringValue
	if input != nil {
		bytes, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("failed to marshal signal input: %w", err)
		}
		rawInput = wrapperspb.String(string(bytes))
	}

	action := &protos.OperationAction{
		Id: ctx.nextActionID(),
		OperationActionType: &protos.OperationAction_SendSignal{
			SendSignal: &protos.SendSignalAction{
				InstanceId:    entityID.String(),
				Name:          operationName,
				Input:         rawInput,
				RequestTime:   timestampOrNil(ctx.currentTime),
				ScheduledTime: timestampOrNil(scheduledTime),
			},
		},
	}
	ctx.actions = append(ctx.actions, action)
	return nil
}

// StartNewOrchestration schedules a new orchestration from within an entity operation.
func (ctx *EntityContext) StartNewOrchestration(name string, opts ...entityStartOrchestrationOption) error {
	if name == "" {
		return fmt.Errorf("orchestration name must not be empty")
	}
	options := &entityStartOrchestrationOptions{}
	for _, configure := range opts {
		if err := configure(options); err != nil {
			return err
		}
	}
	if options.instanceID == "" {
		seed := fmt.Sprintf("%s|%s|%d|start", ctx.ID.String(), ctx.RequestID, ctx.actionIDSeq)
		id := uuid.NewSHA1(entityActionNamespace, []byte(seed))
		options.instanceID = hex.EncodeToString(id[:])
	} else if err := helpers.ValidateOrchestrationInstanceID(options.instanceID); err != nil {
		return err
	}

	action := &protos.OperationAction{
		Id: ctx.nextActionID(),
		OperationActionType: &protos.OperationAction_StartNewOrchestration{
			StartNewOrchestration: &protos.StartNewOrchestrationAction{
				InstanceId:    options.instanceID,
				Name:          name,
				Version:       options.version,
				Input:         options.rawInput,
				ScheduledTime: timestampOrNil(options.scheduledTime),
				RequestTime:   timestampOrNil(ctx.currentTime),
			},
		},
	}
	ctx.actions = append(ctx.actions, action)
	return nil
}

func (ctx *EntityContext) nextActionID() int32 {
	id := ctx.actionIDSeq
	ctx.actionIDSeq++
	return id
}

// entityStartOrchestrationOptions holds options for starting orchestrations from entities.
type entityStartOrchestrationOptions struct {
	instanceID    string
	version       *wrapperspb.StringValue
	rawInput      *wrapperspb.StringValue
	scheduledTime time.Time
}

// entityStartOrchestrationOption is a functional option for StartNewOrchestration.
type entityStartOrchestrationOption func(*entityStartOrchestrationOptions) error

// WithEntityStartOrchestrationInput sets the input for the new orchestration.
func WithEntityStartOrchestrationInput(input any) entityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions) error {
		bytes, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("failed to marshal orchestration input: %w", err)
		}
		opts.rawInput = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithEntityStartOrchestrationInstanceID sets the instance ID for the new orchestration.
func WithEntityStartOrchestrationInstanceID(instanceID string) entityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions) error {
		opts.instanceID = instanceID
		return nil
	}
}

// WithEntityStartOrchestrationVersion sets the version for the new orchestration.
func WithEntityStartOrchestrationVersion(version string) entityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions) error {
		opts.version = wrapperspb.String(version)
		return nil
	}
}

// WithEntityStartOrchestrationScheduledTime schedules the new orchestration.
func WithEntityStartOrchestrationScheduledTime(scheduledTime time.Time) entityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions) error {
		if scheduledTime.IsZero() {
			return fmt.Errorf("scheduled orchestration time must not be zero")
		}
		opts.scheduledTime = scheduledTime
		return nil
	}
}

// callEntityOption is a functional option type for the CallEntity orchestrator method.
type callEntityOption func(*callEntityOptions) error

type callEntityOptions struct {
	rawInput *wrapperspb.StringValue
}

// WithEntityInput configures an input for an entity operation invocation.
func WithEntityInput(input any) callEntityOption {
	return func(opt *callEntityOptions) error {
		data, err := marshalData(input)
		if err != nil {
			return err
		}
		opt.rawInput = wrapperspb.String(string(data))
		return nil
	}
}

// WithRawEntityInput configures a raw input for an entity operation invocation.
func WithRawEntityInput(input string) callEntityOption {
	return func(opt *callEntityOptions) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}

// signalEntityOption is a functional option type for the SignalEntity orchestrator method.
type signalEntityOption func(*signalEntityOptions) error

type signalEntityOptions struct {
	rawInput      *wrapperspb.StringValue
	scheduledTime *timestamppb.Timestamp
}

// WithSignalEntityInput configures an input for a signal entity invocation.
func WithSignalEntityInput(input any) signalEntityOption {
	return func(opt *signalEntityOptions) error {
		data, err := marshalData(input)
		if err != nil {
			return err
		}
		opt.rawInput = wrapperspb.String(string(data))
		return nil
	}
}

// WithRawSignalEntityInput configures a raw input for a signal entity invocation.
func WithRawSignalEntityInput(input string) signalEntityOption {
	return func(opt *signalEntityOptions) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}

// WithSignalEntityScheduledTime configures a scheduled time for an entity signal.
func WithSignalEntityScheduledTime(scheduledTime time.Time) signalEntityOption {
	return func(opt *signalEntityOptions) error {
		if scheduledTime.IsZero() {
			return fmt.Errorf("scheduled entity signal time must not be zero")
		}
		opt.scheduledTime = timestamppb.New(scheduledTime)
		return nil
	}
}

var entityActionNamespace = uuid.MustParse("bd4e8d71-45f8-5f6d-a302-b9a785e665c6")

func timestampOrNil(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}
