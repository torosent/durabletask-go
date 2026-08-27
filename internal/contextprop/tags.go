package contextprop

import "github.com/microsoft/durabletask-go/api"

const (
	instanceIDTag       = "__durabletask.context.instance_id"
	nameTag             = "__durabletask.context.orchestration_name"
	versionTag          = "__durabletask.context.orchestration_version"
	parentInstanceIDTag = "__durabletask.context.parent_instance_id"
)

// Encode returns a new tag map containing immutable fields and orchestration identity.
func Encode(info api.OrchestrationContextInfo, fields api.ContextFields) map[string]string {
	tags := Clone(fields)
	if tags == nil {
		tags = make(map[string]string, 4)
	}
	if info.InstanceID != "" {
		tags[instanceIDTag] = string(info.InstanceID)
	}
	if info.Name != "" {
		tags[nameTag] = info.Name
	}
	if info.Version != "" {
		tags[versionTag] = info.Version
	}
	if info.ParentInstanceID != "" {
		tags[parentInstanceIDTag] = string(info.ParentInstanceID)
	}
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

// Clone returns a defensive copy of tags.
func Clone[T ~map[string]string](tags T) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	copyOfTags := make(map[string]string, len(tags))
	for key, value := range tags {
		copyOfTags[key] = value
	}
	return copyOfTags
}
