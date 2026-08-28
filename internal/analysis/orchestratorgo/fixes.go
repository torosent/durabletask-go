package orchestratorgo

import (
	"go/ast"
	"go/token"
	"go/types"
	"strconv"

	"golang.org/x/tools/go/analysis"
)

// currentTimeFix rewrites time.Now() as ctx.CurrentTimeUtc when an orchestration
// context is in scope. Only the zero-argument clock reads have a one-to-one
// replacement, so no other wall-clock call is offered a fix.
//
// When the rewritten call is the file's only use of the time package, the import
// is deleted along with it, so the fixed file still compiles.
func (c *checker) currentTimeFix(
	call *ast.CallExpr,
	name, ctxName string,
	file *ast.File,
) []analysis.SuggestedFix {
	if ctxName == "" || name != "Now" || len(call.Args) != 0 {
		return nil
	}
	edits := []analysis.TextEdit{{
		Pos:     call.Pos(),
		End:     call.End(),
		NewText: []byte(ctxName + ".CurrentTimeUtc"),
	}}
	if removal, ok := c.soleUseImportRemoval(call.Fun, file); ok {
		edits = append(edits, removal)
	}
	return []analysis.SuggestedFix{{
		Message:   "use (*task.OrchestrationContext).CurrentTimeUtc",
		TextEdits: edits,
	}}
}

// goStatementFix rewrites `go func() { ... }()` as an orchestration coroutine.
// It only applies to an immediately invoked literal with no parameters, no
// results, and no arguments, because any other form would change the meaning of
// the captured values or the call.
//
// The literal's body is never reprinted. Comments live on the file rather than
// on the statements they annotate, so printing the block would silently drop
// them; editing only the text around the braces leaves the body, and everything
// written inside it, exactly as the author wrote it.
func (c *checker) goStatementFix(
	statement *ast.GoStmt,
	ctxName string,
	file *ast.File,
) []analysis.SuggestedFix {
	if ctxName == "" || file == nil {
		return nil
	}
	literal, ok := statement.Call.Fun.(*ast.FuncLit)
	if !ok || len(statement.Call.Args) != 0 || statement.Call.Ellipsis.IsValid() {
		return nil
	}
	if literal.Body == nil || !literal.Body.Lbrace.IsValid() || !literal.Body.Rbrace.IsValid() {
		return nil
	}
	signature := literal.Type
	if signature.Params != nil && len(signature.Params.List) != 0 {
		return nil
	}
	if signature.Results != nil && len(signature.Results.List) != 0 {
		return nil
	}
	qualifier, ok := taskImportName(file)
	if !ok {
		return nil
	}
	return []analysis.SuggestedFix{{
		Message: "use (*task.OrchestrationContext).Go",
		TextEdits: []analysis.TextEdit{
			{
				// `go func() ` becomes `ctx.Go(func(*task.OrchestrationContext) `,
				// stopping short of the brace so the body is untouched.
				Pos:     statement.Pos(),
				End:     literal.Body.Lbrace,
				NewText: []byte(ctxName + ".Go(func(*" + qualifier + ".OrchestrationContext) "),
			},
			{
				// The trailing `()` of the immediate invocation becomes the
				// closing paren of the Go call.
				Pos:     literal.Body.Rbrace + 1,
				End:     statement.End(),
				NewText: []byte(")"),
			},
		},
	}}
}

// soleUseImportRemoval returns an edit deleting the import that a package
// selector refers to, but only when the whole file has no other reference to
// that package. Any other use, including one the analyzer does not rewrite,
// leaves the import in place.
func (c *checker) soleUseImportRemoval(
	selector ast.Expr,
	file *ast.File,
) (analysis.TextEdit, bool) {
	if file == nil {
		return analysis.TextEdit{}, false
	}
	qualified, ok := selector.(*ast.SelectorExpr)
	if !ok {
		return analysis.TextEdit{}, false
	}
	identifier, ok := qualified.X.(*ast.Ident)
	if !ok {
		return analysis.TextEdit{}, false
	}
	name, ok := c.pass.TypesInfo.Uses[identifier].(*types.PkgName)
	if !ok {
		return analysis.TextEdit{}, false
	}
	uses := 0
	ast.Inspect(file, func(node ast.Node) bool {
		if candidate, ok := node.(*ast.Ident); ok && c.pass.TypesInfo.Uses[candidate] == name {
			uses++
		}
		return true
	})
	if uses != 1 {
		return analysis.TextEdit{}, false
	}
	specification := importSpecFor(file, name)
	if specification == nil {
		return analysis.TextEdit{}, false
	}
	return analysis.TextEdit{
		Pos: specification.Pos(),
		End: importSpecEnd(c.pass.Fset, file, specification),
	}, true
}

// importSpecFor returns the import declaration that introduced a package name.
func importSpecFor(file *ast.File, name *types.PkgName) *ast.ImportSpec {
	for _, specification := range file.Imports {
		path, err := strconv.Unquote(specification.Path.Value)
		if err != nil || path != name.Imported().Path() {
			continue
		}
		if specification.Name != nil && specification.Name.Name != name.Name() {
			continue
		}
		return specification
	}
	return nil
}

// importSpecEnd extends an import spec's end through the rest of its line, so
// deleting it does not leave a blank line behind. A grouped import that shares a
// line with another spec is trimmed to its own extent.
func importSpecEnd(fset *token.FileSet, file *ast.File, specification *ast.ImportSpec) token.Pos {
	end := specification.End()
	tokenFile := fset.File(specification.Pos())
	if tokenFile == nil {
		return end
	}
	line := tokenFile.Line(end)
	if line >= tokenFile.LineCount() {
		return end
	}
	nextLineStart := tokenFile.LineStart(line + 1)
	for _, other := range file.Imports {
		if other == specification {
			continue
		}
		if other.Pos() >= end && other.Pos() < nextLineStart {
			// Another import shares the line, so only this spec is removed.
			return end
		}
	}
	return nextLineStart
}

// taskImportName returns the identifier the file uses for the durable task
// package, so generated code compiles under dot-free aliases too.
func taskImportName(file *ast.File) (string, bool) {
	for _, specification := range file.Imports {
		path, err := strconv.Unquote(specification.Path.Value)
		if err != nil || path != taskPackagePath {
			continue
		}
		if specification.Name == nil {
			return "task", true
		}
		if specification.Name.Name == "." || specification.Name.Name == "_" {
			return "", false
		}
		return specification.Name.Name, true
	}
	return "", false
}
