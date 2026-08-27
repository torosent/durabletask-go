package largepayload

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const referencePrefix = "durabletask-payload:v1:"

type reference struct {
	Location string `json:"location"`
	Size     int    `json:"size"`
	SHA256   string `json:"sha256"`
}

func Externalize(ctx context.Context, options *api.LargePayloadOptions, value *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if value == nil {
		return value, nil
	}
	if options == nil {
		if strings.HasPrefix(value.GetValue(), referencePrefix) {
			return nil, fmt.Errorf("%w: large payload reference requires a configured store and resolver", api.ErrFeatureNotSupported)
		}
		return value, nil
	}
	normalized, err := api.NormalizeLargePayloadOptions(options)
	if err != nil {
		return nil, err
	}
	if _, ok, err := parseReference(value.GetValue(), normalized.MaxPayloadBytes); ok || err != nil {
		return value, err
	}
	payload := []byte(value.GetValue())
	if len(payload) > normalized.MaxPayloadBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", api.ErrLargePayloadTooLarge, len(payload), normalized.MaxPayloadBytes)
	}
	if len(payload) <= normalized.ThresholdBytes {
		return value, nil
	}
	location, err := normalized.Store.Store(ctx, append([]byte(nil), payload...))
	if err != nil {
		return nil, fmt.Errorf("failed to store large payload: %w", err)
	}
	if strings.TrimSpace(location) == "" {
		return nil, errors.New("large payload store returned an empty location")
	}
	digest := sha256.Sum256(payload)
	descriptor, err := json.Marshal(reference{
		Location: location,
		Size:     len(payload),
		SHA256:   hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode large payload reference: %w", err)
	}
	return wrapperspb.String(referencePrefix + base64.RawURLEncoding.EncodeToString(descriptor)), nil
}

func Hydrate(ctx context.Context, options *api.LargePayloadOptions, value *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if value == nil {
		return value, nil
	}
	if options == nil {
		if strings.HasPrefix(value.GetValue(), referencePrefix) {
			return nil, fmt.Errorf("%w: large payload reference requires a configured resolver", api.ErrFeatureNotSupported)
		}
		return value, nil
	}
	normalized, err := api.NormalizeLargePayloadOptions(options)
	if err != nil {
		return nil, err
	}
	ref, ok, err := parseReference(value.GetValue(), normalized.MaxPayloadBytes)
	if err != nil || !ok {
		return value, err
	}
	payload, err := normalized.Resolver.Resolve(ctx, ref.Location)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve large payload: %w", err)
	}
	if len(payload) != ref.Size {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", api.ErrLargePayloadIntegrity, ref.Size, len(payload))
	}
	digest := sha256.Sum256(payload)
	expectedDigest, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(expectedDigest) != sha256.Size {
		return nil, fmt.Errorf("%w: invalid SHA-256 digest", api.ErrLargePayloadReference)
	}
	if subtle.ConstantTimeCompare(digest[:], expectedDigest) != 1 {
		return nil, fmt.Errorf("%w: SHA-256 mismatch", api.ErrLargePayloadIntegrity)
	}
	return wrapperspb.String(string(payload)), nil
}

func TransformHistoryEvent(
	ctx context.Context,
	options *api.LargePayloadOptions,
	event *protos.HistoryEvent,
	externalize bool,
) error {
	if event == nil {
		return nil
	}
	transform := Hydrate
	if externalize {
		transform = Externalize
	}
	var target **wrapperspb.StringValue
	switch {
	case event.GetExecutionStarted() != nil:
		target = &event.GetExecutionStarted().Input
	case event.GetExecutionCompleted() != nil:
		target = &event.GetExecutionCompleted().Result
	case event.GetExecutionTerminated() != nil:
		target = &event.GetExecutionTerminated().Input
	case event.GetTaskScheduled() != nil:
		target = &event.GetTaskScheduled().Input
	case event.GetTaskCompleted() != nil:
		target = &event.GetTaskCompleted().Result
	case event.GetSubOrchestrationInstanceCreated() != nil:
		target = &event.GetSubOrchestrationInstanceCreated().Input
	case event.GetSubOrchestrationInstanceCompleted() != nil:
		target = &event.GetSubOrchestrationInstanceCompleted().Result
	case event.GetEventSent() != nil:
		target = &event.GetEventSent().Input
	case event.GetEventRaised() != nil:
		target = &event.GetEventRaised().Input
	case event.GetContinueAsNew() != nil:
		target = &event.GetContinueAsNew().Input
	case event.GetExecutionRewound() != nil:
		target = &event.GetExecutionRewound().Input
	case event.GetEntityOperationSignaled() != nil:
		target = &event.GetEntityOperationSignaled().Input
	case event.GetEntityOperationCalled() != nil:
		target = &event.GetEntityOperationCalled().Input
	case event.GetEntityOperationCompleted() != nil:
		target = &event.GetEntityOperationCompleted().Output
	default:
		return nil
	}
	transformed, err := transform(ctx, options, *target)
	if err != nil {
		return err
	}
	*target = transformed
	return nil
}

func TransformOrchestratorRequest(
	ctx context.Context,
	options *api.LargePayloadOptions,
	request *protos.OrchestratorRequest,
) error {
	if request == nil {
		return nil
	}
	for _, events := range [][]*protos.HistoryEvent{request.PastEvents, request.NewEvents} {
		for _, event := range events {
			if err := TransformHistoryEvent(ctx, options, event, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TransformOrchestratorResponse(
	ctx context.Context,
	options *api.LargePayloadOptions,
	response *protos.OrchestratorResponse,
) error {
	if response == nil {
		return nil
	}
	var err error
	response.CustomStatus, err = Externalize(ctx, options, response.CustomStatus)
	if err != nil {
		return err
	}
	for _, action := range response.Actions {
		if action == nil {
			continue
		}
		switch {
		case action.GetScheduleTask() != nil:
			action.GetScheduleTask().Input, err = Externalize(ctx, options, action.GetScheduleTask().Input)
		case action.GetCreateSubOrchestration() != nil:
			action.GetCreateSubOrchestration().Input, err = Externalize(ctx, options, action.GetCreateSubOrchestration().Input)
		case action.GetSendEvent() != nil:
			action.GetSendEvent().Data, err = Externalize(ctx, options, action.GetSendEvent().Data)
		case action.GetCompleteOrchestration() != nil:
			action.GetCompleteOrchestration().Result, err = Externalize(ctx, options, action.GetCompleteOrchestration().Result)
		case action.GetTerminateOrchestration() != nil:
			action.GetTerminateOrchestration().Reason, err = Externalize(ctx, options, action.GetTerminateOrchestration().Reason)
		case action.GetSendEntityMessage() != nil:
			message := action.GetSendEntityMessage()
			switch {
			case message.GetEntityOperationSignaled() != nil:
				message.GetEntityOperationSignaled().Input, err = Externalize(
					ctx,
					options,
					message.GetEntityOperationSignaled().Input,
				)
			case message.GetEntityOperationCalled() != nil:
				message.GetEntityOperationCalled().Input, err = Externalize(
					ctx,
					options,
					message.GetEntityOperationCalled().Input,
				)
			}
		case action.GetRewindOrchestration() != nil:
			for _, event := range action.GetRewindOrchestration().NewHistory {
				if transformErr := TransformHistoryEvent(ctx, options, event, true); transformErr != nil {
					return transformErr
				}
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func TransformActivityRequest(ctx context.Context, options *api.LargePayloadOptions, request *protos.ActivityRequest) error {
	if request == nil {
		return nil
	}
	var err error
	request.Input, err = Hydrate(ctx, options, request.Input)
	return err
}

func TransformActivityResponse(ctx context.Context, options *api.LargePayloadOptions, response *protos.ActivityResponse) error {
	if response == nil {
		return nil
	}
	var err error
	response.Result, err = Externalize(ctx, options, response.Result)
	return err
}

func TransformEntityBatchRequest(
	ctx context.Context,
	options *api.LargePayloadOptions,
	request *protos.EntityBatchRequest,
) error {
	if request == nil || options == nil {
		return nil
	}
	var err error
	if request.EntityState, err = Hydrate(ctx, options, request.EntityState); err != nil {
		return err
	}
	for _, operation := range request.Operations {
		if operation == nil {
			continue
		}
		if operation.Input, err = Hydrate(ctx, options, operation.Input); err != nil {
			return err
		}
	}
	return nil
}

func TransformEntityBatchResult(
	ctx context.Context,
	options *api.LargePayloadOptions,
	result *protos.EntityBatchResult,
) error {
	if result == nil || options == nil {
		return nil
	}
	var err error
	if result.EntityState, err = Externalize(ctx, options, result.EntityState); err != nil {
		return err
	}
	for _, operationResult := range result.Results {
		if operationResult == nil || operationResult.GetSuccess() == nil {
			continue
		}
		if operationResult.GetSuccess().Result, err = Externalize(
			ctx,
			options,
			operationResult.GetSuccess().Result,
		); err != nil {
			return err
		}
	}
	for _, action := range result.Actions {
		if action == nil {
			continue
		}
		switch {
		case action.GetSendSignal() != nil:
			action.GetSendSignal().Input, err = Externalize(ctx, options, action.GetSendSignal().Input)
		case action.GetStartNewOrchestration() != nil:
			action.GetStartNewOrchestration().Input, err = Externalize(
				ctx,
				options,
				action.GetStartNewOrchestration().Input,
			)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func TransformOrchestrationState(ctx context.Context, options *api.LargePayloadOptions, state *protos.OrchestrationState) error {
	if state == nil {
		return nil
	}
	var err error
	if state.Input, err = Hydrate(ctx, options, state.Input); err != nil {
		return err
	}
	if state.Output, err = Hydrate(ctx, options, state.Output); err != nil {
		return err
	}
	state.CustomStatus, err = Hydrate(ctx, options, state.CustomStatus)
	return err
}

func parseReference(value string, maxPayloadBytes int) (reference, bool, error) {
	if !strings.HasPrefix(value, referencePrefix) {
		return reference{}, false, nil
	}
	encoded := strings.TrimPrefix(value, referencePrefix)
	descriptor, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return reference{}, true, fmt.Errorf("%w: invalid base64 descriptor", api.ErrLargePayloadReference)
	}
	var ref reference
	if err := json.Unmarshal(descriptor, &ref); err != nil {
		return reference{}, true, fmt.Errorf("%w: invalid JSON descriptor", api.ErrLargePayloadReference)
	}
	if strings.TrimSpace(ref.Location) == "" || ref.Size < 0 || ref.Size > maxPayloadBytes {
		return reference{}, true, fmt.Errorf("%w: invalid location or size", api.ErrLargePayloadReference)
	}
	if len(ref.SHA256) != sha256.Size*2 {
		return reference{}, true, fmt.Errorf("%w: invalid SHA-256 digest", api.ErrLargePayloadReference)
	}
	return ref, true, nil
}
