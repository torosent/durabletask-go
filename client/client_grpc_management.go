package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (c *TaskHubGrpcClient) QueryInstances(ctx context.Context, query api.OrchestrationQuery) (*api.OrchestrationQueryResult, error) {
	pageSize, err := api.NormalizeInstanceQueryPageSize(query.PageSize)
	if err != nil {
		return nil, err
	}
	if err := api.ValidateTimeRange(query.CreatedTimeFrom, query.CreatedTimeTo); err != nil {
		return nil, fmt.Errorf("invalid orchestration query: %w", err)
	}

	result := &api.OrchestrationQueryResult{
		Orchestrations: make([]*api.OrchestrationMetadata, 0, pageSize),
	}
	continuationToken := query.ContinuationToken
	for len(result.Orchestrations) < pageSize {
		remaining := pageSize - len(result.Orchestrations)
		wireQuery := &protos.InstanceQuery{
			RuntimeStatus:         slices.Clone(query.RuntimeStatus),
			MaxInstanceCount:      int32(remaining),
			InstanceIdPrefix:      stringValue(query.InstanceIDPrefix),
			ContinuationToken:     stringValue(continuationToken),
			FetchInputsAndOutputs: query.FetchInputsAndOutputs,
		}
		if !query.CreatedTimeFrom.IsZero() {
			wireQuery.CreatedTimeFrom = timestamppb.New(query.CreatedTimeFrom)
		}
		if !query.CreatedTimeTo.IsZero() {
			wireQuery.CreatedTimeTo = timestamppb.New(query.CreatedTimeTo)
		}
		if len(query.TaskHubNames) > 0 {
			wireQuery.TaskHubNames = make([]*wrapperspb.StringValue, 0, len(query.TaskHubNames))
			for _, taskHubName := range query.TaskHubNames {
				wireQuery.TaskHubNames = append(wireQuery.TaskHubNames, wrapperspb.String(taskHubName))
			}
		}

		resp, err := c.client.QueryInstances(ctx, &protos.QueryInstancesRequest{Query: wireQuery})
		if err != nil {
			return nil, managementClientError(ctx, "failed to query orchestration instances", err)
		}

		for _, state := range resp.GetOrchestrationState() {
			if err := largepayload.TransformOrchestrationState(ctx, c.largePayloads, state); err != nil {
				return nil, fmt.Errorf("failed to hydrate orchestration query result: %w", err)
			}
			metadata, err := orchestrationMetadataFromState(state)
			if err != nil {
				return nil, err
			}
			if matchesTags(metadata.Tags, query.Tags) {
				result.Orchestrations = append(result.Orchestrations, metadata)
			}
		}

		nextToken := resp.GetContinuationToken().GetValue()
		if nextToken == "" {
			return result, nil
		}
		if nextToken == continuationToken {
			return nil, errors.New("query service returned a non-advancing continuation token")
		}
		continuationToken = nextToken
	}
	result.ContinuationToken = continuationToken
	return result, nil
}

func (c *TaskHubGrpcClient) ListInstanceIDs(ctx context.Context, query api.InstanceIDQuery) (*api.InstanceIDQueryResult, error) {
	pageSize, err := api.NormalizeInstanceQueryPageSize(query.PageSize)
	if err != nil {
		return nil, err
	}
	if err := api.ValidateTimeRange(query.CompletedTimeFrom, query.CompletedTimeTo); err != nil {
		return nil, fmt.Errorf("invalid instance ID query: %w", err)
	}
	req := &protos.ListInstanceIdsRequest{
		RuntimeStatus:   slices.Clone(query.RuntimeStatus),
		PageSize:        int32(pageSize),
		LastInstanceKey: stringValue(query.ContinuationToken),
	}
	if !query.CompletedTimeFrom.IsZero() {
		req.CompletedTimeFrom = timestamppb.New(query.CompletedTimeFrom)
	}
	if !query.CompletedTimeTo.IsZero() {
		req.CompletedTimeTo = timestamppb.New(query.CompletedTimeTo)
	}

	resp, err := c.client.ListInstanceIds(ctx, req)
	if err != nil {
		return nil, managementClientError(ctx, "failed to list orchestration instance IDs", err)
	}
	result := &api.InstanceIDQueryResult{
		InstanceIDs:       make([]api.InstanceID, 0, len(resp.GetInstanceIds())),
		ContinuationToken: resp.GetLastInstanceKey().GetValue(),
	}
	for _, id := range resp.GetInstanceIds() {
		result.InstanceIDs = append(result.InstanceIDs, api.InstanceID(id))
	}
	return result, nil
}

func (c *TaskHubGrpcClient) RestartInstance(ctx context.Context, id api.InstanceID, opts ...api.RestartOptions) (api.InstanceID, error) {
	req := &protos.RestartInstanceRequest{InstanceId: string(id)}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure restart request: %w", err)
		}
	}
	resp, err := c.client.RestartInstance(ctx, req)
	if err != nil {
		return api.EmptyInstanceID, managementClientError(ctx, "failed to restart orchestration instance", err)
	}
	return api.InstanceID(resp.GetInstanceId()), nil
}

func (c *TaskHubGrpcClient) RewindInstance(ctx context.Context, id api.InstanceID, opts ...api.RewindOptions) error {
	req := &protos.RewindInstanceRequest{InstanceId: string(id)}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure rewind request: %w", err)
		}
	}
	if _, err := c.client.RewindInstance(ctx, req); err != nil {
		return managementClientError(ctx, "failed to rewind orchestration instance", err)
	}
	return nil
}

func (c *TaskHubGrpcClient) PurgeInstances(ctx context.Context, request api.PurgeInstancesRequest) (*api.PurgeInstancesResult, error) {
	hasInstanceIDs := len(request.InstanceIDs) > 0
	hasFilter := request.Filter != nil
	if hasInstanceIDs == hasFilter {
		return nil, errors.New("purge request must specify exactly one of instance IDs or a filter")
	}
	pollInterval := request.PollInterval
	if pollInterval <= 0 {
		pollInterval = api.DefaultPurgePollInterval
	}
	result := &api.PurgeInstancesResult{IsComplete: true}
	if request.Filter != nil {
		req, err := makePurgeFilterRequest(request)
		if err != nil {
			return nil, err
		}
		return c.pollPurgeInstances(ctx, req, pollInterval)
	}

	for start := 0; start < len(request.InstanceIDs); start += api.MaxInstanceBatchSize {
		end := min(start+api.MaxInstanceBatchSize, len(request.InstanceIDs))
		ids := request.InstanceIDs[start:end]
		instanceIDs := make([]string, len(ids))
		for i, id := range ids {
			if id == api.EmptyInstanceID {
				return nil, errors.New("purge instance ID cannot be empty")
			}
			instanceIDs[i] = string(id)
		}
		req := &protos.PurgeInstancesRequest{
			Request: &protos.PurgeInstancesRequest_InstanceBatch{
				InstanceBatch: &protos.InstanceBatch{InstanceIds: instanceIDs},
			},
			Recursive:       request.Recursive,
			IsOrchestration: true,
		}
		batchResult, err := c.pollPurgeInstances(ctx, req, pollInterval)
		if err != nil {
			return nil, err
		}
		result.DeletedInstanceCount += batchResult.DeletedInstanceCount
		result.IsComplete = result.IsComplete && batchResult.IsComplete
	}
	return result, nil
}

func (c *TaskHubGrpcClient) pollPurgeInstances(ctx context.Context, req *protos.PurgeInstancesRequest, pollInterval time.Duration) (*api.PurgeInstancesResult, error) {
	result := &api.PurgeInstancesResult{}
	for {
		resp, err := c.client.PurgeInstances(ctx, req)
		if err != nil {
			return nil, managementClientError(ctx, "failed to purge orchestration instances", err)
		}
		result.DeletedInstanceCount += int(resp.GetDeletedInstanceCount())
		if resp.GetIsComplete() == nil || resp.GetIsComplete().GetValue() {
			result.IsComplete = true
			return result, nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *TaskHubGrpcClient) SkipGracefulOrchestrationTerminations(ctx context.Context, ids []api.InstanceID, reason string) ([]api.InstanceID, error) {
	if len(ids) == 0 {
		return nil, errors.New("at least one instance ID is required")
	}
	if len(ids) > api.MaxInstanceBatchSize {
		return nil, fmt.Errorf("instance batch cannot exceed %d IDs", api.MaxInstanceBatchSize)
	}
	instanceIDs := make([]string, len(ids))
	for i, id := range ids {
		if id == api.EmptyInstanceID {
			return nil, errors.New("instance ID cannot be empty")
		}
		instanceIDs[i] = string(id)
	}
	resp, err := c.client.SkipGracefulOrchestrationTerminations(ctx, &protos.SkipGracefulOrchestrationTerminationsRequest{
		InstanceBatch: &protos.InstanceBatch{InstanceIds: instanceIDs},
		Reason:        stringValue(reason),
	})
	if err != nil {
		return nil, managementClientError(ctx, "failed to skip graceful orchestration terminations", err)
	}
	unterminated := make([]api.InstanceID, 0, len(resp.GetUnterminatedInstanceIds()))
	for _, id := range resp.GetUnterminatedInstanceIds() {
		unterminated = append(unterminated, api.InstanceID(id))
	}
	return unterminated, nil
}

func (c *TaskHubGrpcClient) CreateTaskHub(ctx context.Context, opts ...api.CreateTaskHubOptions) error {
	req := &protos.CreateTaskHubRequest{}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure task hub creation request: %w", err)
		}
	}
	if _, err := c.client.CreateTaskHub(ctx, req); err != nil {
		return managementClientError(ctx, "failed to create task hub", err)
	}
	return nil
}

func (c *TaskHubGrpcClient) DeleteTaskHub(ctx context.Context) error {
	if _, err := c.client.DeleteTaskHub(ctx, &protos.DeleteTaskHubRequest{}); err != nil {
		return managementClientError(ctx, "failed to delete task hub", err)
	}
	return nil
}

func makePurgeFilterRequest(request api.PurgeInstancesRequest) (*protos.PurgeInstancesRequest, error) {
	filter := request.Filter
	if err := api.ValidateTimeRange(filter.CreatedTimeFrom, filter.CreatedTimeTo); err != nil {
		return nil, fmt.Errorf("invalid purge filter: %w", err)
	}
	if filter.Timeout < 0 {
		return nil, errors.New("purge timeout cannot be negative")
	}
	wireFilter := &protos.PurgeInstanceFilter{
		RuntimeStatus: slices.Clone(filter.RuntimeStatus),
	}
	if !filter.CreatedTimeFrom.IsZero() {
		wireFilter.CreatedTimeFrom = timestamppb.New(filter.CreatedTimeFrom)
	}
	if !filter.CreatedTimeTo.IsZero() {
		wireFilter.CreatedTimeTo = timestamppb.New(filter.CreatedTimeTo)
	}
	if filter.Timeout > 0 {
		wireFilter.Timeout = durationpb.New(filter.Timeout)
	}
	return &protos.PurgeInstancesRequest{
		Request:         &protos.PurgeInstancesRequest_PurgeInstanceFilter{PurgeInstanceFilter: wireFilter},
		Recursive:       request.Recursive,
		IsOrchestration: true,
	}, nil
}

func matchesTags(actual, expected map[string]string) bool {
	for key, value := range expected {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func stringValue(value string) *wrapperspb.StringValue {
	if value == "" {
		return nil
	}
	return wrapperspb.String(value)
}

func managementClientError(ctx context.Context, operation string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("%s: %w", operation, api.ErrInstanceNotFound)
	case codes.FailedPrecondition:
		return fmt.Errorf("%s: %w: %s", operation, api.ErrInvalidState, status.Convert(err).Message())
	case codes.Unimplemented:
		return fmt.Errorf("%s: %w", operation, api.ErrFeatureNotSupported)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}
