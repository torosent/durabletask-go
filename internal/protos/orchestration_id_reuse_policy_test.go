package protos

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestLegacyOrchestrationIDReusePolicyWireCompatibility(t *testing.T) {
	var oldWire []byte
	oldWire = protowire.AppendTag(oldWire, 1, protowire.BytesType)
	var statuses []byte
	statuses = protowire.AppendVarint(statuses, uint64(OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING))
	statuses = protowire.AppendVarint(statuses, uint64(OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED))
	oldWire = protowire.AppendBytes(oldWire, statuses)
	oldWire = protowire.AppendTag(oldWire, legacyOrchestrationIDReuseActionField, protowire.VarintType)
	oldWire = protowire.AppendVarint(oldWire, 1)

	var current OrchestrationIdReusePolicy
	require.NoError(t, proto.Unmarshal(oldWire, &current))
	require.Equal(t, []OrchestrationStatus{
		OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
	}, current.ReplaceableStatus)

	action, found, err := GetLegacyOrchestrationIDReuseAction(&current)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 1, action)

	roundTrip, err := proto.Marshal(&current)
	require.NoError(t, err)
	require.Equal(t, oldWire, roundTrip)
}

func TestSetLegacyOrchestrationIDReuseActionRejectsConflicts(t *testing.T) {
	policy := &OrchestrationIdReusePolicy{}
	unknown := protowire.AppendTag(nil, legacyOrchestrationIDReuseActionField, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	unknown = protowire.AppendTag(unknown, legacyOrchestrationIDReuseActionField, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 2)
	policy.ProtoReflect().SetUnknown(unknown)

	_, _, err := GetLegacyOrchestrationIDReuseAction(policy)
	require.ErrorContains(t, err, "conflicting")

	require.NoError(t, SetLegacyOrchestrationIDReuseAction(policy, 2))
	action, found, err := GetLegacyOrchestrationIDReuseAction(policy)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 2, action)
}
