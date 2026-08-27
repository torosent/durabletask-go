package contextprop

import (
	"testing"

	"github.com/microsoft/durabletask-go/api"
)

func TestEncodeDecode(t *testing.T) {
	tags := Encode(api.OrchestrationContextInfo{
		InstanceID:       "instance",
		Name:             "orchestration",
		Version:          "v1",
		ParentInstanceID: "parent",
	}, api.ContextFields{"tenant": "alpha"})

	info, fields := Decode(tags)
	if info.InstanceID != "instance" ||
		info.Name != "orchestration" ||
		info.Version != "v1" ||
		info.ParentInstanceID != "parent" {
		t.Fatalf("unexpected info: %+v", info)
	}
	if fields["tenant"] != "alpha" {
		t.Fatalf("tenant = %q, want alpha", fields["tenant"])
	}
}
