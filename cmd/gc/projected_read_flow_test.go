package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// projectedReadFlow registers a production read whose rows carry no free-form
// text: a read that asks a store for beads.FieldsCore, or a bd invocation run
// through beads.ReadWithBdBrief.
type projectedReadFlow struct {
	name string
	// sites are the functions that issue the read, as "<dir>.<func>".
	sites []string
	// boundary is the function the read's rows never travel past: its
	// results carry no bead, and it stores none.
	boundary string
}

// projectedReadFlows is every projected read outside the beads package, which
// implements the projection.
var projectedReadFlows = []projectedReadFlow{
	{
		name:     "control-dispatcher readiness scan",
		sites:    []string{"cmd/gc.controlReadyLegReady", "cmd/gc.controlReadyScopeShellReady"},
		boundary: "cmd/gc.tryControlReadyScan",
	},
	{
		name:     "closed named-session index",
		sites:    []string{"internal/session.BuildClosedNamedSessionBeadIndex"},
		boundary: "internal/session.BuildClosedNamedSessionBeadIndex",
	},
}

// TestProjectedReadFlowsReadNoOmittedField fails when a projected read's rows
// can reach code that reads a field the projection leaves empty.
//
// Every projected read must be registered in projectedReadFlows with the
// boundary its rows stay inside. From the boundary the check walks every call
// to a package-level function of this module whose parameter or result types
// carry a bead (a beads.Bead, directly or inside a type this module declares).
// It fails when a walked function selects a field named Description, when the
// boundary's results carry a bead, when a site is not reached from its
// boundary, and when a projected read is unregistered or a registration names
// no read.
//
// It reads syntax only, so it over-reports rather than under-reports within
// what it follows: any field named Description counts, and every function of
// a called name is walked. What it cannot see is named here so a boundary is
// chosen where it holds: it does not follow method calls or calls through
// function values, and it does not see a row stored into a variable or a
// struct field.
func TestProjectedReadFlowsReadNoOmittedField(t *testing.T) {
	root := repoRootForLint(t)
	problems, err := checkProjectedReadFlows(root, projectedReadFlows)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// TestProjectedReadFlowCheckCatchesAConsumerThatReadsText runs the check over a
// synthetic module whose projected read hands its rows to a function that
// reads Description, past a call through a type that wraps the rows.
func TestProjectedReadFlowCheckCatchesAConsumerThatReadsText(t *testing.T) {
	root := t.TempDir()
	writeFlowFixture(t, root, "go.mod", "module "+gascityModulePath+"\n\ngo 1.26\n")
	writeFlowFixture(t, root, "internal/beads/beads.go", `package beads

type Bead struct {
	ID          string
	Description string
}

type FieldSet int

const FieldsCore FieldSet = 1

type Store interface{ List(FieldSet) ([]Bead, error) }
`)
	writeFlowFixture(t, root, "cmd/gc/flow.go", `package main

import "github.com/gastownhall/gascity/internal/beads"

type wrapped struct{ rows []beads.Bead }

func scan(s beads.Store) []string {
	rows, _ := read(s)
	return summarize(wrapped{rows: rows})
}

func read(s beads.Store) ([]beads.Bead, error) { return s.List(beads.FieldsCore) }

func summarize(w wrapped) []string {
	var out []string
	for _, b := range w.rows {
		out = append(out, describe(b))
	}
	return out
}

func describe(b beads.Bead) string { return b.ID + b.Description }

func unregistered(s beads.Store) int {
	rows, _ := s.List(beads.FieldsCore)
	return len(rows)
}

func leaks(s beads.Store) []beads.Bead {
	rows, _ := read(s)
	return rows
}
`)
	problems, err := checkProjectedReadFlows(root, []projectedReadFlow{
		{name: "fixture", sites: []string{"cmd/gc.read"}, boundary: "cmd/gc.scan"},
		{name: "leaking", sites: []string{"cmd/gc.read"}, boundary: "cmd/gc.leaks"},
		{name: "stale", sites: []string{"cmd/gc.describe"}, boundary: "cmd/gc.describe"},
	})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(problems, "\n")
	for _, want := range []string{
		`fixture: cmd/gc.describe selects Description`,
		`cmd/gc.unregistered issues a projected read that no flow registers`,
		`leaking: boundary cmd/gc.leaks returns a bead-carrying value`,
		`stale: site cmd/gc.describe issues no projected read`,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("problems do not report %q; got:\n%s", want, joined)
		}
	}
}

func writeFlowFixture(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

const (
	gascityModulePath = "github.com/gastownhall/gascity"
	// projectionPkgDir is the package that implements the projection; its
	// own reads are its stores' business and its tests'.
	projectionPkgDir = "internal/beads"
)

// projectionMarkers are the beads identifiers whose use makes a read
// projected.
var projectionMarkers = []string{"FieldsCore", "ReadWithBdBrief"}

func checkProjectedReadFlows(root string, flows []projectedReadFlow) ([]string, error) {
	x := &flowSource{root: root, fset: token.NewFileSet(), pkgs: map[string]*flowPkg{}, names: map[string]string{}}
	var problems []string

	found, err := x.projectedReadSites()
	if err != nil {
		return nil, err
	}
	registered := map[string]bool{}
	for _, f := range flows {
		for _, site := range f.sites {
			registered[site] = true
			if !found[site] {
				problems = append(problems, fmt.Sprintf("%s: site %s issues no projected read", f.name, site))
			}
		}
	}
	for _, site := range sortedKeys(found) {
		if !registered[site] {
			problems = append(problems, fmt.Sprintf("%s issues a projected read that no flow registers in projectedReadFlows", site))
		}
	}

	for _, f := range flows {
		boundary, err := x.funcs(f.boundary)
		if err != nil {
			return nil, err
		}
		if len(boundary) == 0 {
			problems = append(problems, fmt.Sprintf("%s: boundary %s does not exist", f.name, f.boundary))
			continue
		}
		for _, fn := range boundary {
			if fn.decl.Type.Results != nil && x.fieldsCarry(fn.file, fn.decl.Type.Results) {
				problems = append(problems, fmt.Sprintf("%s: boundary %s returns a bead-carrying value, so the read's rows travel past it", f.name, f.boundary))
			}
		}
		visited, reads, err := x.walk(f.boundary)
		if err != nil {
			return nil, err
		}
		for _, site := range f.sites {
			if !visited[site] {
				problems = append(problems, fmt.Sprintf("%s: site %s is not reached from boundary %s through bead-carrying calls", f.name, site, f.boundary))
			}
		}
		for _, r := range reads {
			problems = append(problems, fmt.Sprintf("%s: %s selects Description at %s, but rows of a projected read reach it", f.name, r.fn, r.pos))
		}
	}
	return problems, nil
}

type flowSource struct {
	root  string
	fset  *token.FileSet
	pkgs  map[string]*flowPkg
	names map[string]string
}

type flowPkg struct {
	dir   string
	funcs map[string][]flowFunc
	types map[string][]flowType
}

type flowFile struct {
	pkg     *flowPkg
	imports map[string]string // local name -> module-relative package dir
}

type flowFunc struct {
	decl *ast.FuncDecl
	file *flowFile
}

type flowType struct {
	spec *ast.TypeSpec
	file *flowFile
}

type descriptionRead struct {
	fn  string
	pos token.Position
}

// projectedReadSites returns the functions outside projectionPkgDir that use
// a projection marker of the beads package, keyed "<dir>.<func>".
func (x *flowSource) projectedReadSites() (map[string]bool, error) {
	sites := map[string]bool{}
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(x.root, top), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			rel, _ := filepath.Rel(x.root, p)
			rel = filepath.ToSlash(rel)
			if d.IsDir() {
				if d.Name() == "testdata" || rel == projectionPkgDir || strings.HasPrefix(rel, projectionPkgDir+"/") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			marked := false
			for _, m := range projectionMarkers {
				if bytes.Contains(data, []byte(m)) {
					marked = true
				}
			}
			if !marked {
				return nil
			}
			dir := path.Dir(rel)
			pkg, err := x.pkg(dir)
			if err != nil {
				return err
			}
			f, err := parser.ParseFile(x.fset, p, data, parser.SkipObjectResolution)
			if err != nil {
				return fmt.Errorf("parse %s: %w", rel, err)
			}
			file, err := x.fileOf(pkg, f)
			if err != nil {
				return err
			}
			for _, decl := range f.Decls {
				owner := dir + ".<package scope>"
				if fd, ok := decl.(*ast.FuncDecl); ok {
					owner = dir + "." + funcKey(fd)
				}
				ast.Inspect(decl, func(n ast.Node) bool {
					sel, ok := n.(*ast.SelectorExpr)
					if !ok || !slices.Contains(projectionMarkers, sel.Sel.Name) {
						return true
					}
					if id, ok := sel.X.(*ast.Ident); ok && file.imports[id.Name] == projectionPkgDir {
						sites[owner] = true
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return sites, nil
}

// walk follows bead-carrying calls from the boundary and returns every
// function it reached and every Description selector inside them.
func (x *flowSource) walk(boundary string) (map[string]bool, []descriptionRead, error) {
	visited := map[string]bool{boundary: true}
	queue := []string{boundary}
	var reads []descriptionRead
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		fns, err := x.funcs(key)
		if err != nil {
			return nil, nil, err
		}
		for _, fn := range fns {
			if fn.decl.Body == nil {
				continue
			}
			var walkErr error
			ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.SelectorExpr:
					if n.Sel.Name == "Description" {
						reads = append(reads, descriptionRead{fn: key, pos: x.fset.Position(n.Sel.Pos())})
					}
				case *ast.CallExpr:
					callee, ok := x.callee(fn.file, n.Fun)
					if !ok || visited[callee] {
						return true
					}
					targets, err := x.funcs(callee)
					if err != nil {
						walkErr = err
						return false
					}
					for _, target := range targets {
						if x.signatureCarries(target) {
							visited[callee] = true
							queue = append(queue, callee)
							break
						}
					}
				}
				return true
			})
			if walkErr != nil {
				return nil, nil, walkErr
			}
		}
	}
	return visited, reads, nil
}

// callee resolves a call to a package-level function of this module, keyed
// "<dir>.<func>". Method calls and calls through function values do not
// resolve.
func (x *flowSource) callee(file *flowFile, fun ast.Expr) (string, bool) {
	for {
		switch e := fun.(type) {
		case *ast.ParenExpr:
			fun = e.X
			continue
		case *ast.IndexExpr:
			fun = e.X
			continue
		case *ast.IndexListExpr:
			fun = e.X
			continue
		case *ast.Ident:
			return file.pkg.dir + "." + e.Name, true
		case *ast.SelectorExpr:
			id, ok := e.X.(*ast.Ident)
			if !ok {
				return "", false
			}
			dir, ok := file.imports[id.Name]
			if !ok {
				return "", false
			}
			return dir + "." + e.Sel.Name, true
		}
		return "", false
	}
}

func (x *flowSource) signatureCarries(fn flowFunc) bool {
	if fn.decl.Recv != nil {
		return false
	}
	return x.fieldsCarry(fn.file, fn.decl.Type.Params) || (fn.decl.Type.Results != nil && x.fieldsCarry(fn.file, fn.decl.Type.Results))
}

func (x *flowSource) fieldsCarry(file *flowFile, fields *ast.FieldList) bool {
	if fields == nil {
		return false
	}
	for _, field := range fields.List {
		if x.typeCarries(file, field.Type, map[string]bool{}) {
			return true
		}
	}
	return false
}

// typeCarries reports whether a type expression holds a beads.Bead, directly
// or through a type this module declares.
func (x *flowSource) typeCarries(file *flowFile, expr ast.Expr, seen map[string]bool) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		if file.pkg.dir == projectionPkgDir && e.Name == "Bead" {
			return true
		}
		return x.namedCarries(file.pkg.dir, e.Name, seen)
	case *ast.SelectorExpr:
		id, ok := e.X.(*ast.Ident)
		if !ok {
			return false
		}
		dir, ok := file.imports[id.Name]
		if !ok {
			return false
		}
		if dir == projectionPkgDir && e.Sel.Name == "Bead" {
			return true
		}
		return x.namedCarries(dir, e.Sel.Name, seen)
	case *ast.ParenExpr:
		return x.typeCarries(file, e.X, seen)
	case *ast.StarExpr:
		return x.typeCarries(file, e.X, seen)
	case *ast.ArrayType:
		return x.typeCarries(file, e.Elt, seen)
	case *ast.Ellipsis:
		return x.typeCarries(file, e.Elt, seen)
	case *ast.MapType:
		return x.typeCarries(file, e.Key, seen) || x.typeCarries(file, e.Value, seen)
	case *ast.ChanType:
		return x.typeCarries(file, e.Value, seen)
	case *ast.IndexExpr:
		return x.typeCarries(file, e.X, seen) || x.typeCarries(file, e.Index, seen)
	case *ast.IndexListExpr:
		if x.typeCarries(file, e.X, seen) {
			return true
		}
		for _, idx := range e.Indices {
			if x.typeCarries(file, idx, seen) {
				return true
			}
		}
	case *ast.StructType:
		return x.fieldsCarry(file, e.Fields)
	}
	return false
}

func (x *flowSource) namedCarries(dir, name string, seen map[string]bool) bool {
	key := dir + "." + name
	if seen[key] {
		return false
	}
	seen[key] = true
	pkg, err := x.pkg(dir)
	if err != nil || pkg == nil {
		return false
	}
	for _, t := range pkg.types[name] {
		if x.typeCarries(t.file, t.spec.Type, seen) {
			return true
		}
	}
	return false
}

// funcs returns the package-level functions named by "<dir>.<func>", one per
// file that declares it (files under different build constraints may).
func (x *flowSource) funcs(key string) ([]flowFunc, error) {
	i := strings.LastIndex(key, ".")
	if i < 0 {
		return nil, fmt.Errorf("function key %q has no package directory", key)
	}
	pkg, err := x.pkg(key[:i])
	if err != nil || pkg == nil {
		return nil, err
	}
	return pkg.funcs[key[i+1:]], nil
}

// pkg parses the non-test Go files of a module package once. A directory
// outside the module or without Go files is nil.
func (x *flowSource) pkg(dir string) (*flowPkg, error) {
	if pkg, ok := x.pkgs[dir]; ok {
		return pkg, nil
	}
	x.pkgs[dir] = nil
	entries, err := os.ReadDir(filepath.Join(x.root, filepath.FromSlash(dir)))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	pkg := &flowPkg{dir: dir, funcs: map[string][]flowFunc{}, types: map[string][]flowType{}}
	parsed := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(x.fset, filepath.Join(x.root, filepath.FromSlash(dir), name), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, fmt.Errorf("parse %s/%s: %w", dir, name, err)
		}
		parsed++
		file, err := x.fileOf(pkg, f)
		if err != nil {
			return nil, err
		}
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil {
					pkg.funcs[d.Name.Name] = append(pkg.funcs[d.Name.Name], flowFunc{decl: d, file: file})
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok {
						pkg.types[ts.Name.Name] = append(pkg.types[ts.Name.Name], flowType{spec: ts, file: file})
					}
				}
			}
		}
	}
	if parsed == 0 {
		return nil, nil
	}
	x.pkgs[dir] = pkg
	return pkg, nil
}

// fileOf records a file's module imports under the names the file uses for
// them.
func (x *flowSource) fileOf(pkg *flowPkg, f *ast.File) (*flowFile, error) {
	file := &flowFile{pkg: pkg, imports: map[string]string{}}
	for _, imp := range f.Imports {
		importPath, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return nil, err
		}
		rel, ok := strings.CutPrefix(importPath, gascityModulePath+"/")
		if !ok {
			continue
		}
		local := ""
		if imp.Name != nil {
			local = imp.Name.Name
		} else {
			local, err = x.packageName(rel)
			if err != nil {
				return nil, err
			}
		}
		if local == "" || local == "_" || local == "." {
			continue
		}
		file.imports[local] = rel
	}
	return file, nil
}

// packageName reads the package clause of a module package directory.
func (x *flowSource) packageName(dir string) (string, error) {
	if name, ok := x.names[dir]; ok {
		return name, nil
	}
	entries, err := os.ReadDir(filepath.Join(x.root, filepath.FromSlash(dir)))
	if err != nil {
		x.names[dir] = path.Base(dir)
		return x.names[dir], nil
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(x.root, filepath.FromSlash(dir), name), nil, parser.PackageClauseOnly)
		if err != nil {
			return "", fmt.Errorf("parse package clause %s/%s: %w", dir, name, err)
		}
		x.names[dir] = f.Name.Name
		return f.Name.Name, nil
	}
	x.names[dir] = path.Base(dir)
	return x.names[dir], nil
}

func funcKey(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	recv := fd.Recv.List[0].Type
	for {
		switch e := recv.(type) {
		case *ast.StarExpr:
			recv = e.X
			continue
		case *ast.IndexExpr:
			recv = e.X
			continue
		case *ast.IndexListExpr:
			recv = e.X
			continue
		}
		break
	}
	if id, ok := recv.(*ast.Ident); ok {
		return id.Name + "." + fd.Name.Name
	}
	return fd.Name.Name
}
