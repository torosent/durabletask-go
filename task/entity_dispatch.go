package task

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/microsoft/durabletask-go/api"
)

var (
	entityContextType = reflect.TypeFor[*EntityContext]()
	entityIDType      = reflect.TypeFor[api.EntityID]()
	errorType         = reflect.TypeFor[error]()
)

// NewEntityFor creates an entity function that dispatches operations to methods
// on a JSON-serializable state struct of type S.
//
// Supported method parameters are an optional *EntityContext, an optional
// api.EntityID, and at most one operation input value, in any order. Supported
// return shapes are (), (result), (error), and (result, error).
func NewEntityFor[S any]() Entity {
	stateType := reflect.TypeFor[S]()
	if stateType.Kind() == reflect.Ptr {
		panic("NewEntityFor does not support pointer state types")
	}

	methods := make(map[string]reflect.Method)
	pointerType := reflect.PointerTo(stateType)
	for i := 0; i < pointerType.NumMethod(); i++ {
		method := pointerType.Method(i)
		methods[strings.ToLower(method.Name)] = method
	}

	return func(ctx *EntityContext) (any, error) {
		var state S
		if ctx.HasState() {
			if err := ctx.GetState(&state); err != nil {
				return nil, fmt.Errorf("failed to deserialize entity state: %w", err)
			}
		}

		method, found := methods[strings.ToLower(ctx.Operation)]
		if !found {
			if strings.EqualFold(ctx.Operation, "delete") {
				ctx.DeleteState()
				return nil, nil
			}
			return nil, fmt.Errorf("entity does not support operation %q", ctx.Operation)
		}

		result, err := callEntityMethod(ctx, reflect.ValueOf(&state), method)
		if err != nil {
			return nil, err
		}
		if !ctx.stateDirty {
			if err := ctx.SetState(state); err != nil {
				return nil, fmt.Errorf("failed to save entity state: %w", err)
			}
		}
		return result, nil
	}
}

func callEntityMethod(ctx *EntityContext, receiver reflect.Value, method reflect.Method) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("entity operation %q has an invalid method signature: %v", ctx.Operation, recovered)
		}
	}()

	methodType := method.Type
	if methodType.IsVariadic() {
		return nil, fmt.Errorf("entity operation %q must not be variadic", ctx.Operation)
	}

	args := make([]reflect.Value, 1, methodType.NumIn())
	args[0] = receiver
	inputCount := 0
	contextCount := 0
	entityIDCount := 0
	for i := 1; i < methodType.NumIn(); i++ {
		parameterType := methodType.In(i)
		switch parameterType {
		case entityContextType:
			contextCount++
			if contextCount > 1 {
				return nil, fmt.Errorf("entity operation %q accepts *EntityContext more than once", ctx.Operation)
			}
			args = append(args, reflect.ValueOf(ctx))
		case entityIDType:
			entityIDCount++
			if entityIDCount > 1 {
				return nil, fmt.Errorf("entity operation %q accepts api.EntityID more than once", ctx.Operation)
			}
			args = append(args, reflect.ValueOf(ctx.ID))
		default:
			inputCount++
			if inputCount > 1 {
				return nil, fmt.Errorf("entity operation %q accepts more than one input parameter", ctx.Operation)
			}
			input := reflect.New(parameterType)
			if err := ctx.GetInput(input.Interface()); err != nil {
				return nil, fmt.Errorf("failed to deserialize input for operation %q: %w", ctx.Operation, err)
			}
			args = append(args, input.Elem())
		}
	}

	switch methodType.NumOut() {
	case 0:
	case 1:
		if methodType.Out(0).Implements(errorType) {
			results := method.Func.Call(args)
			return nil, reflectError(results[0])
		}
	case 2:
		if !methodType.Out(1).Implements(errorType) {
			return nil, fmt.Errorf("entity operation %q second return value must implement error", ctx.Operation)
		}
	default:
		return nil, fmt.Errorf("entity operation %q has unsupported return count %d", ctx.Operation, methodType.NumOut())
	}

	results := method.Func.Call(args)
	switch len(results) {
	case 0:
		return nil, nil
	case 1:
		return reflectResult(results[0]), nil
	case 2:
		return reflectResult(results[0]), reflectError(results[1])
	default:
		panic("validated entity method returned an unexpected result count")
	}
}

func reflectResult(value reflect.Value) any {
	if isNilable(value.Kind()) && value.IsNil() {
		return nil
	}
	return value.Interface()
}

func reflectError(value reflect.Value) error {
	if isNilable(value.Kind()) && value.IsNil() {
		return nil
	}
	return value.Interface().(error)
}

func isNilable(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return true
	default:
		return false
	}
}
