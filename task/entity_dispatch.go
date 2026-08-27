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
// on a converter-serializable state struct of type S.
//
// Supported method parameters are an optional *EntityContext, an optional
// api.EntityID, and at most one operation input value, in any order. Supported
// return shapes are (), (result), (error), and (result, error).
func NewEntityFor[S any]() Entity {
	stateType := reflect.TypeFor[S]()
	if stateType.Kind() == reflect.Pointer {
		panic("NewEntityFor does not support pointer state types")
	}

	methods := make(map[string]reflect.Method)
	pointerType := reflect.PointerTo(stateType)
	for i := 0; i < pointerType.NumMethod(); i++ {
		method := pointerType.Method(i)
		name := strings.ToLower(method.Name)
		if existing, ok := methods[name]; ok {
			panic(fmt.Sprintf(
				"NewEntityFor found case-insensitive operation collision between %s and %s",
				existing.Name,
				method.Name,
			))
		}
		methods[name] = method
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
	var sawContext, sawEntityID, sawInput bool
	for i := 1; i < methodType.NumIn(); i++ {
		parameterType := methodType.In(i)
		switch parameterType {
		case entityContextType:
			if sawContext {
				return nil, fmt.Errorf("entity operation %q accepts *EntityContext more than once", ctx.Operation)
			}
			sawContext = true
			args = append(args, reflect.ValueOf(ctx))
		case entityIDType:
			if sawEntityID {
				return nil, fmt.Errorf("entity operation %q accepts api.EntityID more than once", ctx.Operation)
			}
			sawEntityID = true
			args = append(args, reflect.ValueOf(ctx.ID))
		default:
			if sawInput {
				return nil, fmt.Errorf("entity operation %q accepts more than one input parameter", ctx.Operation)
			}
			sawInput = true
			input := reflect.New(parameterType)
			if err := ctx.GetInput(input.Interface()); err != nil {
				return nil, fmt.Errorf("failed to deserialize input for operation %q: %w", ctx.Operation, err)
			}
			args = append(args, input.Elem())
		}
	}

	numOut := methodType.NumOut()
	returnsError := numOut > 0 && methodType.Out(numOut-1).Implements(errorType)
	switch numOut {
	case 0, 1:
	case 2:
		if !returnsError {
			return nil, fmt.Errorf("entity operation %q second return value must implement error", ctx.Operation)
		}
	default:
		return nil, fmt.Errorf("entity operation %q has unsupported return count %d", ctx.Operation, numOut)
	}

	results := method.Func.Call(args)
	switch {
	case numOut == 0:
		return nil, nil
	case numOut == 1 && returnsError:
		return nil, reflectError(results[0])
	case numOut == 1:
		return reflectResult(results[0]), nil
	default:
		return reflectResult(results[0]), reflectError(results[1])
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
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}
