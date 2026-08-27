package api

import (
	"errors"
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	DefaultInstanceQueryPageSize = 100
	MaxInstanceQueryPageSize     = 1000
	MaxInstanceBatchSize         = 500
	DefaultPurgePollInterval     = 100 * time.Millisecond
)

var ErrFeatureNotSupported = errors.New("feature is not supported by this task hub")

// OrchestrationQuery describes a bounded query for orchestration instances.
// ContinuationToken values are opaque and must only be reused with the same query.
type OrchestrationQuery struct {
	RuntimeStatus         []OrchestrationStatus
	CreatedTimeFrom       time.Time
	CreatedTimeTo         time.Time
	TaskHubNames          []string
	PageSize              int
	ContinuationToken     string
	InstanceIDPrefix      string
	FetchInputsAndOutputs bool
	Tags                  map[string]string
}

type OrchestrationQueryResult struct {
	Orchestrations    []*OrchestrationMetadata
	ContinuationToken string
}

// InstanceIDQuery describes a bounded query that returns only instance IDs.
type InstanceIDQuery struct {
	RuntimeStatus     []OrchestrationStatus
	CompletedTimeFrom time.Time
	CompletedTimeTo   time.Time
	PageSize          int
	ContinuationToken string
}

type InstanceIDQueryResult struct {
	InstanceIDs       []InstanceID
	ContinuationToken string
}

type RestartOptions func(*protos.RestartInstanceRequest) error

func WithRestartNewInstanceID(restartWithNewInstanceID bool) RestartOptions {
	return func(req *protos.RestartInstanceRequest) error {
		req.RestartWithNewInstanceId = restartWithNewInstanceID
		return nil
	}
}

type RewindOptions func(*protos.RewindInstanceRequest) error

func WithRewindReason(reason string) RewindOptions {
	return func(req *protos.RewindInstanceRequest) error {
		req.Reason = wrapperspb.String(reason)
		return nil
	}
}

type PurgeInstanceFilter struct {
	CreatedTimeFrom time.Time
	CreatedTimeTo   time.Time
	RuntimeStatus   []OrchestrationStatus
	Timeout         time.Duration
}

// PurgeInstancesRequest selects either a bounded list of instance IDs or a filter.
// Filter requests are polled until the service reports completion.
type PurgeInstancesRequest struct {
	InstanceIDs  []InstanceID
	Filter       *PurgeInstanceFilter
	Recursive    bool
	PollInterval time.Duration
}

type PurgeInstancesResult struct {
	DeletedInstanceCount int
	IsComplete           bool
}

type CreateTaskHubOptions func(*protos.CreateTaskHubRequest) error

func WithRecreateTaskHub(recreateIfExists bool) CreateTaskHubOptions {
	return func(req *protos.CreateTaskHubRequest) error {
		req.RecreateIfExists = recreateIfExists
		return nil
	}
}

func NormalizeInstanceQueryPageSize(pageSize int) (int, error) {
	switch {
	case pageSize < 0:
		return 0, errors.New("page size cannot be negative")
	case pageSize == 0:
		return DefaultInstanceQueryPageSize, nil
	case pageSize > MaxInstanceQueryPageSize:
		return 0, fmt.Errorf("page size cannot exceed %d", MaxInstanceQueryPageSize)
	default:
		return pageSize, nil
	}
}

func ValidateTimeRange(from, to time.Time) error {
	if !from.IsZero() && !to.IsZero() && from.After(to) {
		return errors.New("start time must not be after end time")
	}
	return nil
}
