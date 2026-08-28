package orchestratorgo

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// packageIndex holds the whole-package declaration information the analyzer
// needs to follow call graphs without loading other packages. Everything it
// records is derived from the syntax and type information of a single
// [analysis.Pass], so the analyzer never depends on facts from dependencies.
type packageIndex struct {
	pass *analysis.Pass

	// functions maps a declared function or method object to its declaration.
	functions map[types.Object]*ast.FuncDecl

	// functionValues maps a variable or constant object to the expressions it is
	// initialized or assigned with. Objects with more than one recorded
	// expression are treated as unresolved so that dynamic reassignment never
	// produces a diagnostic.
	functionValues map[types.Object][]ast.Expr

	// fileOf maps every function declaration and literal the analyzer can reach
	// to the file that contains it, which suggested fixes use to qualify types
	// with the import name that file actually uses.
	fileOf map[ast.Node]*ast.File

	// enclosingFunc maps every function literal to the nearest function
	// declaration or literal that lexically contains it, and is absent for a
	// literal written outside any function, such as a package-level variable
	// initializer. Walking a function body already visits the literals nested
	// in it, so this is what lets the checker walk each one exactly once.
	enclosingFunc map[ast.Node]ast.Node

	// registrationCandidates holds, in source order, every call whose selector
	// name matches a task.TaskRegistry registration method. Collecting them
	// during the single indexing walk keeps packages that register nothing --
	// which is almost every package -- down to one pass over the syntax.
	registrationCandidates []*ast.CallExpr
}

func newPackageIndex(pass *analysis.Pass) *packageIndex {
	index := &packageIndex{
		pass:           pass,
		functions:      make(map[types.Object]*ast.FuncDecl),
		functionValues: make(map[types.Object][]ast.Expr),
		fileOf:         make(map[ast.Node]*ast.File),
		enclosingFunc:  make(map[ast.Node]ast.Node),
	}
	for _, file := range pass.Files {
		if !includeTestFiles &&
			strings.HasSuffix(pass.Fset.PositionFor(file.Pos(), false).Filename, "_test.go") {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if node == nil {
				return true
			}
			switch node := node.(type) {
			case *ast.FuncDecl:
				index.fileOf[node] = file
				if object := pass.TypesInfo.Defs[node.Name]; object != nil {
					index.functions[object] = node
				}
				index.walkFunction(node, file, node.Body)
				// walkFunction already indexed the body while tracking the
				// enclosing function, so the outer traversal stops here.
				return false
			case *ast.FuncLit:
				// A literal reached from file scope, such as a package-level
				// variable initializer, has no enclosing function and is a
				// root the checker must walk on its own.
				index.fileOf[node] = file
				index.walkFunction(node, file, node.Body)
				return false
			}
			index.record(node)
			return true
		})
	}
	return index
}

// walkFunction indexes one function body, recording the enclosing function of
// every literal nested in it.
func (index *packageIndex) walkFunction(owner ast.Node, file *ast.File, body *ast.BlockStmt) {
	if body == nil {
		return
	}
	ast.Inspect(body, func(node ast.Node) bool {
		if node == nil {
			return true
		}
		if literal, ok := node.(*ast.FuncLit); ok {
			index.fileOf[literal] = file
			index.enclosingFunc[literal] = owner
			index.walkFunction(literal, file, literal.Body)
			return false
		}
		index.record(node)
		return true
	})
}

// record captures the declaration facts the analyzer needs from one node.
func (index *packageIndex) record(node ast.Node) {
	pass := index.pass
	switch node := node.(type) {
	case *ast.ValueSpec:
		for i, name := range node.Names {
			if i >= len(node.Values) {
				continue
			}
			if object := pass.TypesInfo.Defs[name]; object != nil {
				index.functionValues[object] = append(index.functionValues[object], node.Values[i])
			}
		}
	case *ast.AssignStmt:
		if len(node.Lhs) != len(node.Rhs) {
			// Tuple assignment such as `a, b := f()` cannot be split into
			// per-name expressions, so nothing is recorded and the names
			// stay unresolved.
			return
		}
		for i, left := range node.Lhs {
			identifier, ok := left.(*ast.Ident)
			if !ok {
				continue
			}
			if object := pass.TypesInfo.ObjectOf(identifier); object != nil {
				index.functionValues[object] = append(index.functionValues[object], node.Rhs[i])
			}
		}
	case *ast.CallExpr:
		if selector, ok := node.Fun.(*ast.SelectorExpr); ok {
			if _, ok := registrationShapes[selector.Sel.Name]; ok {
				index.registrationCandidates = append(index.registrationCandidates, node)
			}
		}
	}
}

// singleValue returns the only expression an object is ever initialized or
// assigned with, or nil when the object is unset or written more than once.
func (index *packageIndex) singleValue(object types.Object) ast.Expr {
	if object == nil {
		return nil
	}
	values := index.functionValues[object]
	if len(values) != 1 {
		return nil
	}
	return values[0]
}

// resolveFunction returns the function declaration or literal that expression
// evaluates to, or nil when the target cannot be proven statically.
func (index *packageIndex) resolveFunction(expression ast.Expr, seen map[types.Object]struct{}) ast.Node {
	switch expression := expression.(type) {
	case *ast.FuncLit:
		return expression
	case *ast.ParenExpr:
		return index.resolveFunction(expression.X, seen)
	case *ast.Ident:
		return index.resolveObject(index.pass.TypesInfo.ObjectOf(expression), seen)
	case *ast.SelectorExpr:
		if selection := index.pass.TypesInfo.Selections[expression]; selection != nil {
			return index.resolveObject(selection.Obj(), seen)
		}
		return index.resolveObject(index.pass.TypesInfo.ObjectOf(expression.Sel), seen)
	case *ast.IndexExpr:
		// Instantiation of a generic function, such as helper[int].
		return index.resolveFunction(expression.X, seen)
	case *ast.IndexListExpr:
		return index.resolveFunction(expression.X, seen)
	case *ast.CallExpr:
		// Conversions such as task.Orchestrator(fn) forward to their operand.
		if typeInfo, ok := index.pass.TypesInfo.Types[expression.Fun]; ok &&
			typeInfo.IsType() &&
			len(expression.Args) == 1 {
			return index.resolveFunction(expression.Args[0], seen)
		}
	}
	return nil
}

func (index *packageIndex) resolveObject(object types.Object, seen map[types.Object]struct{}) ast.Node {
	if object == nil {
		return nil
	}
	if function := index.functions[object]; function != nil {
		return function
	}
	if seen == nil {
		seen = make(map[types.Object]struct{})
	}
	if _, ok := seen[object]; ok {
		return nil
	}
	seen[object] = struct{}{}
	value := index.singleValue(object)
	if value == nil {
		// Unset, or reassigned more than once: the target is ambiguous.
		return nil
	}
	return index.resolveFunction(value, seen)
}

// callee returns the function declaration or literal invoked by call when that
// target is declared in the package under analysis.
//
// Calls into the durable task package are never followed. Its channels,
// goroutines, and locks implement the deterministic orchestration scheduler, so
// auditing them would report the very primitives the analyzer recommends. This
// only has an effect when the durable task package analyzes itself; for every
// other package the target is already outside the index.
func (index *packageIndex) callee(call *ast.CallExpr) ast.Node {
	if function := staticFunc(index.pass, call.Fun); function != nil {
		if pkg := function.Pkg(); pkg != nil && pkg.Path() == taskPackagePath {
			return nil
		}
	}
	return index.resolveFunction(call.Fun, nil)
}

// staticFunc returns the function object an expression denotes, which may be a
// package-level function or a method.
func staticFunc(pass *analysis.Pass, expression ast.Expr) *types.Func {
	switch expression := expression.(type) {
	case *ast.ParenExpr:
		return staticFunc(pass, expression.X)
	case *ast.Ident:
		function, _ := pass.TypesInfo.ObjectOf(expression).(*types.Func)
		return function
	case *ast.SelectorExpr:
		if selection := pass.TypesInfo.Selections[expression]; selection != nil {
			function, _ := selection.Obj().(*types.Func)
			return function
		}
		function, _ := pass.TypesInfo.ObjectOf(expression.Sel).(*types.Func)
		return function
	case *ast.IndexExpr:
		return staticFunc(pass, expression.X)
	case *ast.IndexListExpr:
		return staticFunc(pass, expression.X)
	}
	return nil
}

// funcBody returns the body of a function declaration or literal.
func funcBody(node ast.Node) *ast.BlockStmt {
	switch node := node.(type) {
	case *ast.FuncDecl:
		return node.Body
	case *ast.FuncLit:
		return node.Body
	}
	return nil
}

// funcType returns the signature syntax of a function declaration or literal.
func funcType(node ast.Node) *ast.FuncType {
	switch node := node.(type) {
	case *ast.FuncDecl:
		return node.Type
	case *ast.FuncLit:
		return node.Type
	}
	return nil
}
