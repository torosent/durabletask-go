package contextprop

import (
	"maps"

	"github.com/microsoft/durabletask-go/api"
)

const (
	instanceIDTag       = api.ReservedContextFieldPrefix + "instance_id"
	nameTag             = api.ReservedContextFieldPrefix + "orchestration_name"
	versionTag          = api.ReservedContextFieldPrefix + "orchestration_version"
	parentInstanceIDTag = api.ReservedContextFieldPrefix + "parent_instance_id"
)

// Encode returns a new tag map containing immutable fields and orchestration identity.
func Encode(info api.OrchestrationContextInfo, fields api.ContextFields) map[string]string {
	tags := Clone(fields)
	if tags == nil {
		tags = make(map[string]string, 4)
	}
	tags[instanceIDTag] = string(info.InstanceID)
	tags[nameTag] = info.Name
	tags[versionTag] = info.Version
	tags[parentInstanceIDTag] = string(info.ParentInstanceID)
	return tags
}

// Decode separates orchestration identity from caller-supplied immutable fields.
func Decode(tags map[string]string) (api.OrchestrationContextInfo, api.ContextFields) {
	info := api.OrchestrationContextInfo{
		InstanceID:       api.InstanceID(tags[instanceIDTag]),
		Name:             tags[nameTag],
		Version:          tags[versionTag],
		ParentInstanceID: api.InstanceID(tags[parentInstanceIDTag]),
	}
	fields := make(api.ContextFields)
	for key, value := range tags {
		switch key {
		case instanceIDTag, nameTag, versionTag, parentInstanceIDTag:
			continue
		default:
			fields[key] = value
		}
	}
	if len(fields) == 0 {
		fields = nil
	}
	return info, fields
}

// Clone returns a defensive copy of tags, or nil when there is nothing to copy.
func Clone[T ~map[string]string](tags T) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	copyOfTags := make(map[string]string, len(tags))
	maps.Copy(copyOfTags, tags)
	return copyOfTags
}
