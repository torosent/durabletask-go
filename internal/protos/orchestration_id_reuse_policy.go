package protos

import (
	"fmt"
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

const legacyOrchestrationIDReuseActionField protowire.Number = 2

// SetLegacyOrchestrationIDReuseAction writes the pre-2026 action field so new clients
// remain compatible with older sidecars while using the current generated message.
func SetLegacyOrchestrationIDReuseAction(policy *OrchestrationIdReusePolicy, action int32) error {
	if policy == nil {
		return nil
	}

	unknown, err := removeLegacyOrchestrationIDReuseAction(policy.ProtoReflect().GetUnknown())
	if err != nil {
		return err
	}
	unknown = protowire.AppendTag(unknown, legacyOrchestrationIDReuseActionField, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, uint64(action))
	policy.ProtoReflect().SetUnknown(unknown)
	return nil
}

// GetLegacyOrchestrationIDReuseAction reads the action field used by older
// OrchestrationIdReusePolicy messages. Conflicting values are rejected.
func GetLegacyOrchestrationIDReuseAction(policy *OrchestrationIdReusePolicy) (int32, bool, error) {
	if policy == nil {
		return 0, false, nil
	}

	var (
		action int32
		found  bool
	)
	raw := policy.ProtoReflect().GetUnknown()
	for len(raw) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(raw)
		if tagLen < 0 {
			return 0, false, protowire.ParseError(tagLen)
		}
		valueLen := protowire.ConsumeFieldValue(num, typ, raw[tagLen:])
		if valueLen < 0 {
			return 0, false, protowire.ParseError(valueLen)
		}
		if num == legacyOrchestrationIDReuseActionField {
			if typ != protowire.VarintType {
				return 0, false, fmt.Errorf("legacy orchestration ID reuse action has wire type %d, want varint", typ)
			}
			value, n := protowire.ConsumeVarint(raw[tagLen:])
			if n < 0 {
				return 0, false, protowire.ParseError(n)
			}
			if value > uint64(math.MaxInt32) {
				return 0, false, fmt.Errorf("legacy orchestration ID reuse action %d overflows int32", value)
			}
			current := int32(value)
			if found && current != action {
				return 0, false, fmt.Errorf("conflicting legacy orchestration ID reuse actions %d and %d", action, current)
			}
			action = current
			found = true
		}
		raw = raw[tagLen+valueLen:]
	}
	return action, found, nil
}

func removeLegacyOrchestrationIDReuseAction(raw []byte) ([]byte, error) {
	filtered := make([]byte, 0, len(raw))
	for len(raw) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(raw)
		if tagLen < 0 {
			return nil, protowire.ParseError(tagLen)
		}
		valueLen := protowire.ConsumeFieldValue(num, typ, raw[tagLen:])
		if valueLen < 0 {
			return nil, protowire.ParseError(valueLen)
		}
		fieldLen := tagLen + valueLen
		if num != legacyOrchestrationIDReuseActionField {
			filtered = append(filtered, raw[:fieldLen]...)
		}
		raw = raw[fieldLen:]
	}
	return filtered, nil
}
