// Package orchestratorgo rejects raw goroutines in orchestrators registered
// with task.TaskRegistry in the same package. It does not perform whole-program
// determinism analysis or inspect helper implementations in other packages.
package orchestratorgo

import (
	"go/ast"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

const taskPackagePath = "github.com/microsoft/durabletask-go/task"

// Analyzer rejects raw go statements in registered orchestrator bodies.
var Analyzer = &analysis.Analyzer{
	Name: "orchestratorgo",
	Doc:  "reject raw go statements in durable task orchestrators",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	functions := make(map[types.Object]ast.Node)
	values := make(map[types.Object]ast.Expr)
	for _, file := range pass.Files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.FuncDecl:
				if object := pass.TypesInfo.Defs[node.Name]; object != nil {
					functions[object] = node
				}
			case *ast.ValueSpec:
				for i, name := range node.Names {
					if i >= len(node.Values) {
						continue
					}
					if object := pass.TypesInfo.Defs[name]; object != nil {
						values[object] = node.Values[i]
					}
				}
			case *ast.AssignStmt:
				for i, left := range node.Lhs {
					if i >= len(node.Rhs) {
						continue
					}
					identifier, ok := left.(*ast.Ident)
					if !ok {
						continue
					}
					object := pass.TypesInfo.ObjectOf(identifier)
					if object != nil {
						values[object] = node.Rhs[i]
					}
				}
			}
			return true
		})
	}

	orchestrators := make(map[ast.Node]struct{})
	for _, file := range pass.Files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			argument, ok := registeredOrchestratorArgument(pass, call)
			if !ok {
				return true
			}
			if function := resolveFunction(pass, argument, functions, values, nil); function != nil {
				orchestrators[function] = struct{}{}
			}
			return true
		})
	}

	reported := make(map[token.Pos]struct{})
	for orchestrator := range orchestrators {
		ast.Inspect(orchestrator, func(node ast.Node) bool {
			goStatement, ok := node.(*ast.GoStmt)
			if !ok {
				return true
			}
			if _, seen := reported[goStatement.Go]; seen {
				return true
			}
			reported[goStatement.Go] = struct{}{}
			pass.Reportf(
				goStatement.Go,
				"raw go statement is not deterministic in an orchestrator; use (*task.OrchestrationContext).Go",
			)
			return true
		})
	}
	return nil, nil
}

func registeredOrchestratorArgument(pass *analysis.Pass, call *ast.CallExpr) (ast.Expr, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil, false
	}
	selection := pass.TypesInfo.Selections[selector]
	if selection == nil || !isTaskRegistry(selection.Recv()) {
		return nil, false
	}
	method, ok := selection.Obj().(*types.Func)
	if !ok || method.Pkg() == nil || method.Pkg().Path() != taskPackagePath {
		return nil, false
	}
	switch method.Name() {
	case "AddOrchestrator":
		if len(call.Args) == 1 {
			return call.Args[0], true
		}
	case "AddOrchestratorN":
		if len(call.Args) == 2 {
			return call.Args[1], true
		}
	}
	return nil, false
}

func isTaskRegistry(value types.Type) bool {
	if pointer, ok := value.(*types.Pointer); ok {
		value = pointer.Elem()
	}
	named, ok := value.(*types.Named)
	if !ok {
		return false
	}
	object := named.Obj()
	return object.Pkg() != nil &&
		object.Pkg().Path() == taskPackagePath &&
		object.Name() == "TaskRegistry"
}

func resolveFunction(
	pass *analysis.Pass,
	expression ast.Expr,
	functions map[types.Object]ast.Node,
	values map[types.Object]ast.Expr,
	seen map[types.Object]struct{},
) ast.Node {
	switch expression := expression.(type) {
	case *ast.FuncLit:
		return expression
	case *ast.ParenExpr:
		return resolveFunction(pass, expression.X, functions, values, seen)
	case *ast.Ident:
		object := pass.TypesInfo.ObjectOf(expression)
		return resolveObject(pass, object, functions, values, seen)
	case *ast.SelectorExpr:
		if selection := pass.TypesInfo.Selections[expression]; selection != nil {
			return resolveObject(pass, selection.Obj(), functions, values, seen)
		}
		return resolveObject(pass, pass.TypesInfo.ObjectOf(expression.Sel), functions, values, seen)
	case *ast.CallExpr:
		if typeInfo, ok := pass.TypesInfo.Types[expression.Fun]; ok &&
			typeInfo.IsType() &&
			len(expression.Args) == 1 {
			return resolveFunction(pass, expression.Args[0], functions, values, seen)
		}
	}
	return nil
}

func resolveObject(
	pass *analysis.Pass,
	object types.Object,
	functions map[types.Object]ast.Node,
	values map[types.Object]ast.Expr,
	seen map[types.Object]struct{},
) ast.Node {
	if object == nil {
		return nil
	}
	if function := functions[object]; function != nil {
		return function
	}
	if seen == nil {
		seen = make(map[types.Object]struct{})
	}
	if _, ok := seen[object]; ok {
		return nil
	}
	seen[object] = struct{}{}
	if value := values[object]; value != nil {
		return resolveFunction(pass, value, functions, values, seen)
	}
	return nil
}
