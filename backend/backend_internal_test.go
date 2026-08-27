package backend

import (
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
)

func TestOrchestrationIDReusePolicyDefaultsToError(t *testing.T) {
	policy, err := orchestrationIDReusePolicyFromProto(&protos.OrchestrationIdReusePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if policy.Action != api.REUSE_ID_ACTION_ERROR {
		t.Fatalf("action = %v, want ERROR", policy.Action)
	}
}

func TestOrchestrationIDReusePolicyRejectsAmbiguousWireShape(t *testing.T) {
	_, err := orchestrationIDReusePolicyFromProto(&protos.OrchestrationIdReusePolicy{
		ReplaceableStatus: []protos.OrchestrationStatus{
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		},
	})
	if err == nil {
		t.Fatal("expected ambiguous policy error")
	}
}

func TestOrchestrationIDReusePolicyPreservesExplicitError(t *testing.T) {
	wirePolicy := &protos.OrchestrationIdReusePolicy{
		ReplaceableStatus: []protos.OrchestrationStatus{
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		},
	}
	if err := protos.SetLegacyOrchestrationIDReuseAction(wirePolicy, int32(api.REUSE_ID_ACTION_ERROR)); err != nil {
		t.Fatal(err)
	}
	policy, err := orchestrationIDReusePolicyFromProto(wirePolicy)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Action != api.REUSE_ID_ACTION_ERROR {
		t.Fatalf("action = %v, want ERROR", policy.Action)
	}
}
