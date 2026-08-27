package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	maxRewindHistoryEvents = 100_000
	maxRewindInstances     = 10_000
	maxRewindDepth         = 100
)

func (be *sqliteBackend) QueryOrchestrations(ctx context.Context, query api.OrchestrationQuery) (*api.OrchestrationQueryResult, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	if len(query.TaskHubNames) > 0 {
		return nil, fmt.Errorf("%w: task hub name filters", api.ErrFeatureNotSupported)
	}
	pageSize, err := api.NormalizeInstanceQueryPageSize(query.PageSize)
	if err != nil {
		return nil, err
	}
	if err := api.ValidateTimeRange(query.CreatedTimeFrom, query.CreatedTimeTo); err != nil {
		return nil, err
	}
	lastInstanceID, err := decodeContinuationToken(query.ContinuationToken)
	if err != nil {
		return nil, err
	}

	var sqlBuilder strings.Builder
	inputColumn, outputColumn, customStatusColumn, failureDetailsColumn := "NULL", "NULL", "NULL", "NULL"
	if query.FetchInputsAndOutputs {
		inputColumn, outputColumn, customStatusColumn, failureDetailsColumn = "i.Input", "i.Output", "i.CustomStatus", "i.FailureDetails"
	}
	fmt.Fprintf(
		&sqlBuilder,
		`SELECT i.InstanceID, i.ExecutionID, i.Name, i.Version, i.ScheduledStartTime, i.ParentInstanceID, i.RuntimeStatus,
			i.CreatedTime, i.LastUpdatedTime, i.CompletedTime, %s, %s, %s, %s
		FROM Instances AS i WHERE i.InstanceID > ?`,
		inputColumn,
		outputColumn,
		customStatusColumn,
		failureDetailsColumn,
	)
	args := []any{lastInstanceID}
	if len(query.RuntimeStatus) > 0 {
		sqlBuilder.WriteString(" AND i.RuntimeStatus IN (")
		for i, status := range query.RuntimeStatus {
			if i > 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString("?")
			args = append(args, helpers.ToRuntimeStatusString(status))
		}
		sqlBuilder.WriteString(")")
	}
	if !query.CreatedTimeFrom.IsZero() {
		sqlBuilder.WriteString(" AND i.CreatedTime >= ?")
		args = append(args, query.CreatedTimeFrom.UTC())
	}
	if !query.CreatedTimeTo.IsZero() {
		sqlBuilder.WriteString(" AND i.CreatedTime <= ?")
		args = append(args, query.CreatedTimeTo.UTC())
	}
	if query.InstanceIDPrefix != "" {
		sqlBuilder.WriteString(` AND i.InstanceID LIKE ? ESCAPE '\'`)
		args = append(args, escapeLikePrefix(query.InstanceIDPrefix)+"%")
	}
	tagKeys := make([]string, 0, len(query.Tags))
	for key := range query.Tags {
		if key == "" {
			return nil, errors.New("tag filter key cannot be empty")
		}
		tagKeys = append(tagKeys, key)
	}
	sort.Strings(tagKeys)
	for i, key := range tagKeys {
		fmt.Fprintf(
			&sqlBuilder,
			" AND EXISTS (SELECT 1 FROM InstanceTags AS t%d WHERE t%d.InstanceID = i.InstanceID AND t%d.TagKey = ? AND t%d.TagValue = ?)",
			i,
			i,
			i,
			i,
		)
		args = append(args, key, query.Tags[key])
	}
	sqlBuilder.WriteString(" ORDER BY i.InstanceID ASC LIMIT ?")
	args = append(args, pageSize+1)

	rows, err := be.db.QueryContext(ctx, sqlBuilder.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query orchestration instances: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query cleanup

	metadata := make([]*api.OrchestrationMetadata, 0, pageSize+1)
	for rows.Next() {
		item, err := scanOrchestrationMetadata(rows)
		if err != nil {
			return nil, err
		}
		metadata = append(metadata, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while reading orchestration query results: %w", err)
	}

	hasMore := len(metadata) > pageSize
	if hasMore {
		metadata = metadata[:pageSize]
	}
	ids := make([]api.InstanceID, len(metadata))
	for i, item := range metadata {
		ids[i] = item.InstanceID
	}
	tags, err := be.getInstanceTags(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, item := range metadata {
		item.Tags = tags[item.InstanceID]
	}

	result := &api.OrchestrationQueryResult{Orchestrations: metadata}
	if hasMore && len(metadata) > 0 {
		result.ContinuationToken = encodeContinuationToken(string(metadata[len(metadata)-1].InstanceID))
	}
	return result, nil
}

func (be *sqliteBackend) ListInstanceIDs(ctx context.Context, query api.InstanceIDQuery) (*api.InstanceIDQueryResult, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	pageSize, err := api.NormalizeInstanceQueryPageSize(query.PageSize)
	if err != nil {
		return nil, err
	}
	if err := api.ValidateTimeRange(query.CompletedTimeFrom, query.CompletedTimeTo); err != nil {
		return nil, err
	}
	lastInstanceID, err := decodeContinuationToken(query.ContinuationToken)
	if err != nil {
		return nil, err
	}

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("SELECT i.InstanceID FROM Instances AS i WHERE i.InstanceID > ?")
	args := []any{lastInstanceID}
	if len(query.RuntimeStatus) > 0 {
		sqlBuilder.WriteString(" AND i.RuntimeStatus IN (")
		for i, status := range query.RuntimeStatus {
			if i > 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString("?")
			args = append(args, helpers.ToRuntimeStatusString(status))
		}
		sqlBuilder.WriteString(")")
	}
	if !query.CompletedTimeFrom.IsZero() {
		sqlBuilder.WriteString(" AND i.CompletedTime >= ?")
		args = append(args, query.CompletedTimeFrom.UTC())
	}
	if !query.CompletedTimeTo.IsZero() {
		sqlBuilder.WriteString(" AND i.CompletedTime <= ?")
		args = append(args, query.CompletedTimeTo.UTC())
	}
	sqlBuilder.WriteString(" ORDER BY i.InstanceID ASC LIMIT ?")
	args = append(args, pageSize+1)

	rows, err := be.db.QueryContext(ctx, sqlBuilder.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list orchestration instance IDs: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query cleanup

	ids := make([]api.InstanceID, 0, pageSize+1)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan orchestration instance ID: %w", err)
		}
		ids = append(ids, api.InstanceID(id))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while reading orchestration instance IDs: %w", err)
	}

	hasMore := len(ids) > pageSize
	if hasMore {
		ids = ids[:pageSize]
	}
	result := &api.InstanceIDQueryResult{InstanceIDs: ids}
	if hasMore && len(ids) > 0 {
		result.ContinuationToken = encodeContinuationToken(string(ids[len(ids)-1]))
	}
	return result, nil
}

func (be *sqliteBackend) RestartInstance(ctx context.Context, id api.InstanceID, restartWithNewInstanceID bool) (api.InstanceID, error) {
	metadata, err := be.GetOrchestrationMetadata(ctx, id)
	if err != nil {
		return api.EmptyInstanceID, err
	}
	if !metadata.IsComplete() {
		return api.EmptyInstanceID, api.ErrNotCompleted
	}
	startEvent, err := be.getExecutionStartedEvent(ctx, id)
	if err != nil {
		return api.EmptyInstanceID, err
	}

	restartedID := id
	if restartWithNewInstanceID {
		u, err := uuid.NewV7()
		if err != nil {
			u = uuid.New()
		}
		restartedID = api.InstanceID(u.String())
	}
	restartedEvent := helpers.NewExecutionStartedEvent(
		startEvent.Name,
		string(restartedID),
		startEvent.Input,
		startEvent.ParentInstance,
		startEvent.ParentTraceContext,
		nil,
		startEvent.Version,
	)
	restartedEvent.GetExecutionStarted().Tags = maps.Clone(startEvent.Tags)

	policy := &api.OrchestrationIdReusePolicy{
		Action: api.REUSE_ID_ACTION_TERMINATE,
		OperationStatus: []api.OrchestrationStatus{
			api.RUNTIME_STATUS_COMPLETED,
			api.RUNTIME_STATUS_FAILED,
			api.RUNTIME_STATUS_TERMINATED,
			api.RUNTIME_STATUS_CANCELED,
		},
	}
	if err := be.CreateOrchestrationInstance(ctx, restartedEvent, backend.WithOrchestrationIdReusePolicy(policy)); err != nil {
		return api.EmptyInstanceID, fmt.Errorf("failed to restart orchestration instance: %w", err)
	}
	return restartedID, nil
}

func (be *sqliteBackend) RewindInstance(ctx context.Context, id api.InstanceID, reason string) error {
	if err := be.ensureDB(); err != nil {
		return err
	}
	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	if _, err := be.rewindInstanceTx(ctx, tx, id, reason, make(map[api.InstanceID]struct{}), 0); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit orchestration rewind: %w", err)
	}
	return nil
}

func (be *sqliteBackend) PurgeInstances(ctx context.Context, request api.PurgeInstancesRequest) (*api.PurgeInstancesResult, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if len(request.InstanceIDs) > api.MaxInstanceBatchSize {
		return nil, fmt.Errorf("instance batch cannot exceed %d IDs", api.MaxInstanceBatchSize)
	}
	if request.Filter != nil && request.Filter.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.Filter.Timeout)
		defer cancel()
	}

	if len(request.InstanceIDs) > 0 {
		result := &api.PurgeInstancesResult{IsComplete: true}
		for _, id := range request.InstanceIDs {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			count, err := be.purgeInstance(ctx, id, request.Recursive)
			if errors.Is(err, api.ErrInstanceNotFound) {
				continue
			}
			if err != nil {
				return result, err
			}
			result.DeletedInstanceCount += count
		}
		return result, nil
	}

	filter := request.Filter
	if err := api.ValidateTimeRange(filter.CreatedTimeFrom, filter.CreatedTimeTo); err != nil {
		return nil, err
	}
	statuses := append([]api.OrchestrationStatus(nil), filter.RuntimeStatus...)
	if len(statuses) == 0 {
		statuses = terminalStatuses()
	}
	for _, status := range statuses {
		if !isTerminalStatus(status) {
			return nil, api.ErrNotCompleted
		}
	}
	result := &api.PurgeInstancesResult{}
	page, err := be.QueryOrchestrations(ctx, api.OrchestrationQuery{
		RuntimeStatus:   statuses,
		CreatedTimeFrom: filter.CreatedTimeFrom,
		CreatedTimeTo:   filter.CreatedTimeTo,
		PageSize:        api.DefaultInstanceQueryPageSize,
	})
	if err != nil {
		return result, err
	}
	for _, metadata := range page.Orchestrations {
		count, err := be.purgeInstance(ctx, metadata.InstanceID, request.Recursive)
		if errors.Is(err, api.ErrInstanceNotFound) {
			continue
		}
		if err != nil {
			return result, err
		}
		result.DeletedInstanceCount += count
	}
	remaining, err := be.QueryOrchestrations(ctx, api.OrchestrationQuery{
		RuntimeStatus:   statuses,
		CreatedTimeFrom: filter.CreatedTimeFrom,
		CreatedTimeTo:   filter.CreatedTimeTo,
		PageSize:        1,
	})
	if err != nil {
		return nil, err
	}
	result.IsComplete = len(remaining.Orchestrations) == 0
	return result, nil
}

func (be *sqliteBackend) SkipGracefulOrchestrationTerminations(ctx context.Context, ids []api.InstanceID, reason string) ([]api.InstanceID, error) {
	if len(ids) == 0 {
		return nil, errors.New("at least one instance ID is required")
	}
	if len(ids) > api.MaxInstanceBatchSize {
		return nil, fmt.Errorf("instance batch cannot exceed %d IDs", api.MaxInstanceBatchSize)
	}
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	serializedReason, err := json.Marshal(reason)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize termination reason: %w", err)
	}
	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	now := time.Now().UTC()
	unterminated := make([]api.InstanceID, 0)
	for _, id := range ids {
		var runtimeStatus string
		var parentInstanceID sql.NullString
		err := tx.QueryRowContext(
			ctx,
			"SELECT RuntimeStatus, ParentInstanceID FROM Instances WHERE InstanceID = ?",
			string(id),
		).Scan(&runtimeStatus, &parentInstanceID)
		if errors.Is(err, sql.ErrNoRows) {
			unterminated = append(unterminated, id)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to inspect orchestration before immediate termination: %w", err)
		}
		if parentInstanceID.Valid ||
			(runtimeStatus != "PENDING" && runtimeStatus != "RUNNING" && runtimeStatus != "SUSPENDED") {
			unterminated = append(unterminated, id)
			continue
		}
		var activeChildren int
		if err := tx.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM Instances
			WHERE ParentInstanceID = ? AND RuntimeStatus IN ('PENDING', 'RUNNING', 'SUSPENDED')`,
			string(id),
		).Scan(&activeChildren); err != nil {
			return nil, fmt.Errorf("failed to inspect sub-orchestrations before immediate termination: %w", err)
		}
		if activeChildren > 0 {
			unterminated = append(unterminated, id)
			continue
		}
		result, err := tx.ExecContext(
			ctx,
			`UPDATE Instances SET RuntimeStatus = 'TERMINATED', CompletedTime = COALESCE(CompletedTime, ?),
				LastUpdatedTime = ?, Output = ?, FailureDetails = NULL, LockedBy = NULL, LockExpiration = NULL
			WHERE InstanceID = ? AND RuntimeStatus IN ('PENDING', 'RUNNING', 'SUSPENDED')`,
			now,
			now,
			string(serializedReason),
			string(id),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to terminate orchestration instance: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("failed to inspect termination result: %w", err)
		}
		if affected == 0 {
			unterminated = append(unterminated, id)
			continue
		}
		var nextSequenceNumber int
		if err := tx.QueryRowContext(
			ctx,
			"SELECT COALESCE(MAX(SequenceNumber), -1) + 1 FROM History WHERE InstanceID = ?",
			string(id),
		).Scan(&nextSequenceNumber); err != nil {
			return nil, fmt.Errorf("failed to allocate termination history sequence: %w", err)
		}
		terminationEvent := helpers.NewExecutionCompletedEvent(
			-1,
			api.RUNTIME_STATUS_TERMINATED,
			wrapperspb.String(string(serializedReason)),
			nil,
		)
		payload, err := backend.MarshalHistoryEvent(terminationEvent)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO History (InstanceID, SequenceNumber, EventPayload) VALUES (?, ?, ?)",
			string(id),
			nextSequenceNumber,
			payload,
		); err != nil {
			return nil, fmt.Errorf("failed to append immediate termination history: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM NewEvents WHERE InstanceID = ?", string(id)); err != nil {
			return nil, fmt.Errorf("failed to delete pending orchestration events: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM NewTasks WHERE InstanceID = ?", string(id)); err != nil {
			return nil, fmt.Errorf("failed to delete pending activity tasks: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit immediate terminations: %w", err)
	}
	return unterminated, nil
}

func (be *sqliteBackend) purgeInstance(ctx context.Context, id api.InstanceID, recursive bool) (int, error) {
	return be.purgeInstanceRecursive(ctx, id, recursive, make(map[api.InstanceID]struct{}), 0)
}

func (be *sqliteBackend) purgeInstanceRecursive(
	ctx context.Context,
	id api.InstanceID,
	recursive bool,
	visited map[api.InstanceID]struct{},
	depth int,
) (int, error) {
	if depth > maxRewindDepth {
		return 0, fmt.Errorf("%w: purge depth exceeds %d", api.ErrInvalidState, maxRewindDepth)
	}
	if _, ok := visited[id]; ok {
		return 0, fmt.Errorf("%w: cycle detected while purging %q", api.ErrInvalidState, id)
	}
	visited[id] = struct{}{}
	deleted := 0
	if recursive {
		for {
			var childID string
			err := be.db.QueryRowContext(
				ctx,
				"SELECT InstanceID FROM Instances WHERE ParentInstanceID = ? ORDER BY InstanceID LIMIT 1",
				string(id),
			).Scan(&childID)
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			if err != nil {
				return deleted, fmt.Errorf("failed to query sub-orchestration: %w", err)
			}
			count, err := be.purgeInstanceRecursive(ctx, api.InstanceID(childID), true, visited, depth+1)
			deleted += count
			if err != nil {
				return deleted, err
			}
		}
	}
	if err := be.PurgeOrchestrationState(ctx, id); err != nil {
		return deleted, err
	}
	return deleted + 1, nil
}

func (be *sqliteBackend) rewindInstanceTx(
	ctx context.Context,
	tx *sql.Tx,
	id api.InstanceID,
	reason string,
	visited map[api.InstanceID]struct{},
	depth int,
) (bool, error) {
	if depth > maxRewindDepth {
		return false, fmt.Errorf("%w: rewind depth exceeds %d", api.ErrInvalidState, maxRewindDepth)
	}
	if _, ok := visited[id]; ok {
		return false, fmt.Errorf("%w: cycle detected while rewinding %q", api.ErrInvalidState, id)
	}
	if len(visited) >= maxRewindInstances {
		return false, fmt.Errorf("%w: rewind instance count exceeds %d", api.ErrInvalidState, maxRewindInstances)
	}
	visited[id] = struct{}{}

	var runtimeStatus string
	var lockedBy sql.NullString
	var lockExpiration sql.NullTime
	err := tx.QueryRowContext(
		ctx,
		"SELECT RuntimeStatus, LockedBy, LockExpiration FROM Instances WHERE InstanceID = ?",
		string(id),
	).Scan(&runtimeStatus, &lockedBy, &lockExpiration)
	if errors.Is(err, sql.ErrNoRows) {
		return false, api.ErrInstanceNotFound
	}
	if err != nil {
		return false, fmt.Errorf("failed to read orchestration rewind state: %w", err)
	}
	if helpers.FromRuntimeStatusString(runtimeStatus) != api.RUNTIME_STATUS_FAILED {
		return false, fmt.Errorf("%w: only failed orchestrations can be rewound", api.ErrInvalidState)
	}
	if lockedBy.Valid && lockExpiration.Valid && lockExpiration.Time.After(time.Now().UTC()) {
		return false, fmt.Errorf("%w: orchestration has an active work-item lock", api.ErrInvalidState)
	}

	history, err := loadRewindHistorySQLite(ctx, tx, id)
	if err != nil {
		return false, err
	}
	rebuilt, startEvent, failedChildren, err := backend.RebuildRewindHistory(id, history)
	if err != nil {
		return false, err
	}

	childIDs := make([]string, 0, len(failedChildren))
	for _, childID := range failedChildren {
		childIDs = append(childIDs, childID)
	}
	sort.Strings(childIDs)
	for _, childID := range childIDs {
		if _, err := be.rewindInstanceTx(ctx, tx, api.InstanceID(childID), reason, visited, depth+1); err != nil {
			return false, fmt.Errorf("failed to rewind sub-orchestration %q: %w", childID, err)
		}
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM History WHERE InstanceID = ?", string(id)); err != nil {
		return false, fmt.Errorf("failed to clear orchestration history for rewind: %w", err)
	}
	for sequenceNumber, event := range rebuilt {
		payload, err := backend.MarshalHistoryEvent(event)
		if err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO History (InstanceID, SequenceNumber, EventPayload) VALUES (?, ?, ?)",
			string(id),
			sequenceNumber,
			payload,
		); err != nil {
			return false, fmt.Errorf("failed to rebuild orchestration history: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM NewEvents WHERE InstanceID = ?", string(id)); err != nil {
		return false, fmt.Errorf("failed to clear pending orchestration events for rewind: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM NewTasks WHERE InstanceID = ?", string(id)); err != nil {
		return false, fmt.Errorf("failed to clear pending activity tasks for rewind: %w", err)
	}

	executionID := startEvent.GetOrchestrationInstance().GetExecutionId().GetValue()
	now := time.Now().UTC()
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE Instances SET ExecutionID = ?, RuntimeStatus = 'PENDING', LastUpdatedTime = ?,
			CompletedTime = NULL, Output = NULL, FailureDetails = NULL, LockedBy = NULL, LockExpiration = NULL
		WHERE InstanceID = ?`,
		executionID,
		now,
		string(id),
	); err != nil {
		return false, fmt.Errorf("failed to update orchestration state for rewind: %w", err)
	}

	leaf := len(failedChildren) == 0
	if leaf {
		marker := backend.NewExecutionRewoundEvent(id, reason, startEvent)
		payload, err := backend.MarshalHistoryEvent(marker)
		if err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO NewEvents (InstanceID, ExecutionID, EventPayload) VALUES (?, ?, ?)",
			string(id),
			executionID,
			payload,
		); err != nil {
			return false, fmt.Errorf("failed to enqueue orchestration rewind marker: %w", err)
		}
	}
	return leaf, nil
}

func loadRewindHistorySQLite(ctx context.Context, tx *sql.Tx, id api.InstanceID) ([]*protos.HistoryEvent, error) {
	rows, err := tx.QueryContext(
		ctx,
		"SELECT EventPayload FROM History WHERE InstanceID = ? ORDER BY SequenceNumber ASC LIMIT ?",
		string(id),
		maxRewindHistoryEvents+1,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to read orchestration history for rewind: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query cleanup
	history := make([]*protos.HistoryEvent, 0, 128)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("failed to scan orchestration history for rewind: %w", err)
		}
		event, err := backend.UnmarshalHistoryEvent(payload)
		if err != nil {
			return nil, err
		}
		history = append(history, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while reading orchestration history for rewind: %w", err)
	}
	if len(history) > maxRewindHistoryEvents {
		return nil, fmt.Errorf("%w: history exceeds %d events", api.ErrInvalidState, maxRewindHistoryEvents)
	}
	return history, nil
}

func (be *sqliteBackend) getExecutionStartedEvent(ctx context.Context, id api.InstanceID) (*protos.ExecutionStartedEvent, error) {
	rows, err := be.db.QueryContext(
		ctx,
		"SELECT EventPayload FROM History WHERE InstanceID = ? ORDER BY SequenceNumber ASC LIMIT 16",
		string(id),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to read orchestration history: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query cleanup
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("failed to scan orchestration history: %w", err)
		}
		event, err := backend.UnmarshalHistoryEvent(payload)
		if err != nil {
			return nil, err
		}
		if startEvent := event.GetExecutionStarted(); startEvent != nil {
			return proto.Clone(startEvent).(*protos.ExecutionStartedEvent), nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while reading orchestration history: %w", err)
	}
	return nil, errors.New("orchestration history does not contain an execution started event")
}

func (be *sqliteBackend) getInstanceTags(ctx context.Context, ids []api.InstanceID) (map[api.InstanceID]map[string]string, error) {
	result := make(map[api.InstanceID]map[string]string, len(ids))
	for start := 0; start < len(ids); start += api.MaxInstanceBatchSize {
		end := min(start+api.MaxInstanceBatchSize, len(ids))
		batch := ids[start:end]
		args := make([]any, len(batch))
		for i, id := range batch {
			args[i] = string(id)
		}
		query := "SELECT InstanceID, TagKey, TagValue FROM InstanceTags WHERE InstanceID IN (?" +
			strings.Repeat(", ?", len(batch)-1) + ") ORDER BY InstanceID, TagKey"
		rows, err := be.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to query instance tags: %w", err)
		}
		for rows.Next() {
			var instanceID, key, value string
			if err := rows.Scan(&instanceID, &key, &value); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("failed to scan instance tag: %w", err)
			}
			id := api.InstanceID(instanceID)
			if result[id] == nil {
				result[id] = make(map[string]string)
			}
			result[id][key] = value
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("failed while reading instance tags: %w", err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("failed to close instance tag query: %w", err)
		}
	}
	return result, nil
}

func scanOrchestrationMetadata(scanner interface{ Scan(...any) error }) (*api.OrchestrationMetadata, error) {
	var instanceID, executionID, name, runtimeStatus string
	var version, parentInstanceID, input, output, customStatus sql.NullString
	var createdAt, lastUpdatedAt time.Time
	var scheduledStartAt, completedAt sql.NullTime
	var failureDetailsPayload []byte
	if err := scanner.Scan(
		&instanceID,
		&executionID,
		&name,
		&version,
		&scheduledStartAt,
		&parentInstanceID,
		&runtimeStatus,
		&createdAt,
		&lastUpdatedAt,
		&completedAt,
		&input,
		&output,
		&customStatus,
		&failureDetailsPayload,
	); err != nil {
		return nil, fmt.Errorf("failed to scan orchestration metadata: %w", err)
	}
	var failureDetails *protos.TaskFailureDetails
	if len(failureDetailsPayload) > 0 {
		failureDetails = new(protos.TaskFailureDetails)
		if err := proto.Unmarshal(failureDetailsPayload, failureDetails); err != nil {
			return nil, fmt.Errorf("failed to unmarshal failure details: %w", err)
		}
	}
	metadata := api.NewOrchestrationMetadata(
		api.InstanceID(instanceID),
		name,
		helpers.FromRuntimeStatusString(runtimeStatus),
		createdAt,
		lastUpdatedAt,
		input.String,
		output.String,
		customStatus.String,
		failureDetails,
	)
	metadata.ExecutionID = executionID
	if scheduledStartAt.Valid {
		metadata.ScheduledStartAt = scheduledStartAt.Time
	}
	if version.Valid {
		metadata.Version = version.String
	}
	if parentInstanceID.Valid {
		metadata.ParentInstanceID = api.InstanceID(parentInstanceID.String)
	}
	if completedAt.Valid {
		metadata.CompletedAt = completedAt.Time
	}
	return metadata, nil
}

func encodeContinuationToken(instanceID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(instanceID))
}

func decodeContinuationToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("invalid continuation token: %w", err)
	}
	return string(decoded), nil
}

func escapeLikePrefix(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(value)
}

func terminalStatuses() []api.OrchestrationStatus {
	return []api.OrchestrationStatus{
		api.RUNTIME_STATUS_COMPLETED,
		api.RUNTIME_STATUS_FAILED,
		api.RUNTIME_STATUS_TERMINATED,
		api.RUNTIME_STATUS_CANCELED,
	}
}

func isTerminalStatus(status api.OrchestrationStatus) bool {
	return slices.Contains(terminalStatuses(), status)
}
