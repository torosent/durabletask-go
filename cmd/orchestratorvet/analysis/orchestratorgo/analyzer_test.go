package orchestratorgo_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/cmd/orchestratorvet/analysis/orchestratorgo"
	"golang.org/x/tools/go/analysis/analysistest"
	"golang.org/x/tools/go/packages"
)

// fixPackages are the fixture packages that carry a .golden file and are
// therefore driven by RunWithSuggestedFixes rather than by Run.
var fixPackages = []string{"fixes", "fixesimport"}

// stubPackageRoot is the fixture tree holding the stand-in dependencies the
// scenario packages import. It is not a scenario itself.
const stubPackageRoot = "github.com"

const fixtureLoadMode = packages.NeedName |
	packages.NeedFiles |
	packages.NeedCompiledGoFiles |
	packages.NeedImports |
	packages.NeedTypes |
	packages.NeedSyntax |
	packages.NeedTypesInfo |
	packages.NeedDeps

func fixtureEnvironment(root string) []string {
	return append(os.Environ(), "GOPATH="+root, "GO111MODULE=off", "GOWORK=off")
}

// scenarioPackages discovers the analysistest fixture packages, one per
// concern, so a new fixture directory is exercised without also having to be
// listed here. Discovery failing is a test failure: a silently empty list would
// make the whole suite pass without analyzing anything.
func scenarioPackages(t *testing.T) []string {
	t.Helper()
	root := filepath.Join(analysistest.TestData(), "src")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read fixture root %s: %v", root, err)
	}
	var scenarios []string
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == stubPackageRoot {
			continue
		}
		if slices.Contains(fixPackages, entry.Name()) {
			continue
		}
		scenarios = append(scenarios, entry.Name())
	}
	if len(scenarios) == 0 {
		t.Fatalf("no fixture packages found under %s", root)
	}
	slices.Sort(scenarios)
	return scenarios
}

func TestAnalyzer(t *testing.T) {
	for _, scenario := range scenarioPackages(t) {
		t.Run(scenario, func(t *testing.T) {
			analysistest.Run(t, analysistest.TestData(), orchestratorgo.Analyzer, scenario)
		})
	}
}

func TestAnalyzerSuggestedFixes(t *testing.T) {
	analysistest.RunWithSuggestedFixes(t, analysistest.TestData(), orchestratorgo.Analyzer, fixPackages...)
}

// TestSuggestedFixesCompile type-checks each golden file in place of the fixture
// it replaces. A fix that leaves an unused import or an unbalanced rewrite
// produces code that no longer builds, which the golden comparison alone cannot
// catch.
func TestSuggestedFixesCompile(t *testing.T) {
	for _, name := range fixPackages {
		t.Run(name, func(t *testing.T) {
			testdata := analysistest.TestData()
			source := filepath.Join(testdata, "src", name, name+".go")
			fixed, err := os.ReadFile(source + ".golden")
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			config := &packages.Config{
				Mode:    fixtureLoadMode,
				Dir:     testdata,
				Env:     fixtureEnvironment(testdata),
				Overlay: map[string][]byte{source: fixed},
			}
			loaded, err := packages.Load(config, name)
			if err != nil {
				t.Fatalf("load fixed package: %v", err)
			}
			if len(loaded) != 1 {
				t.Fatalf("loaded %d packages, want 1", len(loaded))
			}
			for _, loadError := range loaded[0].Errors {
				t.Errorf("fixed %s does not compile: %v", name, loadError)
			}
		})
	}
}

// TestAnalyzerMetadata guards the identity the vet tool and documentation use.
func TestAnalyzerMetadata(t *testing.T) {
	if orchestratorgo.Analyzer.Name != "orchestratorgo" {
		t.Fatalf("analyzer name = %q, want %q", orchestratorgo.Analyzer.Name, "orchestratorgo")
	}
	if orchestratorgo.Analyzer.Doc == "" {
		t.Fatal("analyzer doc must be set so go vet can describe the check")
	}
	if orchestratorgo.Analyzer.Run == nil {
		t.Fatal("analyzer must have a run function")
	}
	if !strings.Contains(orchestratorgo.Analyzer.URL, "orchestratorgo") {
		t.Fatalf("analyzer URL = %q, want it to point at the analyzer", orchestratorgo.Analyzer.URL)
	}
}
