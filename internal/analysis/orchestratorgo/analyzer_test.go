package orchestratorgo_test

import (
	"testing"

	"github.com/microsoft/durabletask-go/internal/analysis/orchestratorgo"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), orchestratorgo.Analyzer, "a")
}
