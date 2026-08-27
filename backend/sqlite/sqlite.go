package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

var emptyString string = ""

type SqliteOptions struct {
	OrchestrationLockTimeout    time.Duration
	ActivityLockTimeout         time.Duration
	EntityLockTimeout           time.Duration
	MaxEntityOperationBatchSize int
	FilePath                    string
}

type sqliteBackend struct {
	dsn        string
	db         *sql.DB
	workerName string
	logger     backend.Logger
	options    *SqliteOptions
}

// NewSqliteOptions creates a new options object for the sqlite backend provider.
//
// Specify "" for filePath to configure an in-memory database.
func NewSqliteOptions(filePath string) *SqliteOptions {
	// Default values are provided for required options
	return &SqliteOptions{
		FilePath:                    filePath,
		OrchestrationLockTimeout:    2 * time.Minute,
		ActivityLockTimeout:         2 * time.Minute,
		EntityLockTimeout:           2 * time.Minute,
		MaxEntityOperationBatchSize: 100,
	}
}

// NewSqliteBackend creates a new sqlite-based Backend object.
func NewSqliteBackend(opts *SqliteOptions, logger backend.Logger) backend.Backend {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	uuidStr := uuid.NewString()

	if opts == nil {
		opts = NewSqliteOptions("")
	}
	be := &sqliteBackend{
		db:         nil,
		workerName: fmt.Sprintf("%s,%d,%s", hostname, pid, uuidStr),
		options:    opts,
		logger:     logger,
	}

	switch {
	case opts.FilePath == "":
		be.dsn = fmt.Sprintf("file:durabletask-%s?mode=memory&cache=shared", uuidStr)
	case !strings.HasPrefix(opts.FilePath, "file:"):
		be.dsn = "file:" + opts.FilePath
	default:
		be.dsn = opts.FilePath
	}

	// used for local debug
	// be.dsn = "file:file.sqlite"

	return be
}

// CreateTaskHub creates the sqlite database and applies the schema
func (be *sqliteBackend) CreateTaskHub(ctx context.Context) error {
	if err := be.Start(ctx); err != nil {
		return fmt.Errorf("failed to start the backend: %w", err)
	}

	// Initialize database
	if _, err := be.db.Exec(schema); err != nil {
		return fmt.Errorf("failed to initialize the database: %w", err)
	}

	return nil
}

func (be *sqliteBackend) DeleteTaskHub(ctx context.Context) error {
	if be.db != nil {
		if err := be.Stop(ctx); err != nil {
			return fmt.Errorf("failed to stop the backend: %w", err)
		}
	}

	if be.options.FilePath == "" {
		// In-memory DB
		return nil
	}

	// File-system DB
	err := os.Remove(be.options.FilePath)
	switch {
	case err == nil:
		return nil
	case os.IsNotExist(err):
		return backend.ErrTaskHubNotFound
	default:
		return fmt.Errorf("failed to delete the database: %w", err)
	}
}

// AbandonOrchestrationWorkItem implements backend.Backend
func (be *sqliteBackend) AbandonOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	var visibleTime *time.Time = nil
	if delay := wi.GetAbandonDelay(); delay > 0 {
		t := time.Now().UTC().Add(delay)
		visibleTime = &t
	}

	dbResult, err := tx.ExecContext(
		ctx,
		"UPDATE NewEvents SET [LockedBy] = NULL, [VisibleTime] = ? WHERE [InstanceID] = ? AND [LockedBy] = ?",
		visibleTime,
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update NewEvents table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE NewEvents statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	dbResult, err = tx.ExecContext(
		ctx,
		"UPDATE Instances SET [LockedBy] = NULL, [LockExpiration] = NULL WHERE [InstanceID] = ? AND [LockedBy] = ?",
		string(wi.InstanceID),
		wi.LockedBy,
	)

	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	rowsAffected, err = dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE Instances statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CompleteOrchestrationWorkItem implements backend.Backend
func (be *sqliteBackend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	now := time.Now().UTC()

	// Dynamically generate the UPDATE statement for the Instances table
	var sqlSB strings.Builder
	sqlSB.WriteString("UPDATE Instances SET ")

	sqlUpdateArgs := make([]any, 0, 10)
	isCreated := false
	isCompleted := false

	for _, e := range wi.State.NewEvents() {
		if es := e.GetExecutionStarted(); es != nil {
			if isCreated {
				// TODO: Log warning about duplicate start event
				continue
			}
			isCreated = true
			sqlSB.WriteString("[CreatedTime] = ?, [Input] = ?, ")
			sqlUpdateArgs = append(sqlUpdateArgs, e.Timestamp.AsTime())
			sqlUpdateArgs = append(sqlUpdateArgs, es.Input.GetValue())
		} else if ec := e.GetExecutionCompleted(); ec != nil {
			if isCompleted {
				// TODO: Log warning about duplicate completion event
				continue
			}
			isCompleted = true
			sqlSB.WriteString("[CompletedTime] = ?, [Output] = ?, [FailureDetails] = ?, ")
			sqlUpdateArgs = append(sqlUpdateArgs, now)
			sqlUpdateArgs = append(sqlUpdateArgs, ec.Result.GetValue())
			if ec.FailureDetails != nil {
				bytes, err := proto.Marshal(ec.FailureDetails)
				if err != nil {
					return fmt.Errorf("failed to marshal FailureDetails: %w", err)
				}
				sqlUpdateArgs = append(sqlUpdateArgs, &bytes)
			} else {
				sqlUpdateArgs = append(sqlUpdateArgs, nil)
			}
		}
		// TODO: Execution suspended & resumed
	}

	if wi.State.CustomStatus != nil {
		sqlSB.WriteString("[CustomStatus] = ?, ")
		sqlUpdateArgs = append(sqlUpdateArgs, wi.State.CustomStatus.Value)
	}

	// TODO: Support for stickiness, which would extend the LockExpiration
	sqlSB.WriteString("[RuntimeStatus] = ?, [LastUpdatedTime] = ?, [LockExpiration] = NULL WHERE [InstanceID] = ? AND [LockedBy] = ?")
	sqlUpdateArgs = append(sqlUpdateArgs, helpers.ToRuntimeStatusString(wi.State.RuntimeStatus()), now, string(wi.InstanceID), wi.LockedBy)

	result, err := tx.ExecContext(ctx, sqlSB.String(), sqlUpdateArgs...)
	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get the number of rows affected by the Instance table update: %w", err)
	} else if count == 0 {
		return fmt.Errorf("instance '%s' no longer exists or was locked by a different worker", string(wi.InstanceID))
	}

	// If continue-as-new, delete all existing history
	if wi.State.ContinuedAsNew() {
		if _, err := tx.ExecContext(ctx, "DELETE FROM History WHERE InstanceID = ?", string(wi.InstanceID)); err != nil {
			return fmt.Errorf("failed to delete from History table: %w", err)
		}
	}

	// Save new history events
	newHistoryCount := len(wi.State.NewEvents())
	if newHistoryCount > 0 {
		query := "INSERT INTO History ([InstanceID], [SequenceNumber], [EventPayload]) VALUES (?, ?, ?)" +
			strings.Repeat(", (?, ?, ?)", newHistoryCount-1)

		args := make([]any, 0, newHistoryCount*3)
		nextSequenceNumber := len(wi.State.OldEvents())
		for _, e := range wi.State.NewEvents() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			args = append(args, string(wi.InstanceID), nextSequenceNumber, eventPayload)
			nextSequenceNumber++
		}

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to insert into the History table: %w", err)
		}
	}

	// Save outbound activity tasks
	newActivityCount := len(wi.State.PendingTasks())
	if newActivityCount > 0 {
		insertSql := "INSERT INTO NewTasks ([InstanceID], [EventPayload]) VALUES (?, ?)" +
			strings.Repeat(", (?, ?)", newActivityCount-1)

		sqlInsertArgs := make([]any, 0, newActivityCount*2)
		for _, e := range wi.State.PendingTasks() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload)
		}

		_, err = tx.ExecContext(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewTasks table: %w", err)
		}
	}

	// Save outbound orchestrator events
	newEventCount := len(wi.State.PendingTimers()) + len(wi.State.PendingMessages())
	if newEventCount > 0 {
		insertSql := "INSERT INTO NewEvents ([InstanceID], [EventPayload], [VisibleTime]) VALUES (?, ?, ?)" +
			strings.Repeat(", (?, ?, ?)", newEventCount-1)

		sqlInsertArgs := make([]any, 0, newEventCount*3)
		for _, e := range wi.State.PendingTimers() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			visibileTime := e.GetTimerFired().GetFireAt().AsTime()
			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload, visibileTime)
		}

		for _, msg := range wi.State.PendingMessages() {
			if es := msg.HistoryEvent.GetExecutionStarted(); es != nil {
				// Need to insert a new row into the DB
				if _, err := be.createOrchestrationInstanceInternal(ctx, msg.HistoryEvent, tx, backend.WithOrchestrationIdReusePolicy(&api.OrchestrationIdReusePolicy{
					OperationStatus: []protos.OrchestrationStatus{protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED},
					Action:          api.REUSE_ID_ACTION_TERMINATE,
				})); err != nil {
					if errors.Is(err, backend.ErrDuplicateEvent) {
						be.logger.Warnf(
							"%v: dropping sub-orchestration creation event because an instance with the target ID (%v) already exists.",
							wi.InstanceID,
							es.OrchestrationInstance.InstanceId)
					} else {
						return err
					}
				}
			}

			eventPayload, err := backend.MarshalHistoryEvent(msg.HistoryEvent)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, msg.TargetInstanceID, eventPayload, nil)
		}

		_, err = tx.ExecContext(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewEvents table: %w", err)
		}
	}

	for _, message := range wi.State.PendingEntityMessages() {
		if err := be.addEntityMessageTx(ctx, tx, message.TargetInstanceID, message.HistoryEvent); err != nil {
			return fmt.Errorf("failed to enqueue entity message: %w", err)
		}
	}

	// Delete inbound events
	dbResult, err := tx.ExecContext(
		ctx,
		"DELETE FROM NewEvents WHERE [InstanceID] = ? AND [LockedBy] = ?",
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err != nil {
		return fmt.Errorf("failed to delete from the NewEvents table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CreateOrchestrationInstance implements backend.Backend
func (be *sqliteBackend) CreateOrchestrationInstance(ctx context.Context, e *backend.HistoryEvent, opts ...backend.OrchestrationIdReusePolicyOptions) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	var instanceID string
	if instanceID, err = be.createOrchestrationInstanceInternal(ctx, e, tx, opts...); errors.Is(err, api.ErrIgnoreInstance) {
		// choose to ignore, do nothing
		return nil
	} else if err != nil {
		return err
	}

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)`,
		instanceID,
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into [NewEvents] table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to create orchestration: %w", err)
	}

	return nil
}

func (be *sqliteBackend) createOrchestrationInstanceInternal(ctx context.Context, e *backend.HistoryEvent, tx *sql.Tx, opts ...backend.OrchestrationIdReusePolicyOptions) (string, error) {
	if e == nil {
		return "", backend.ErrNilHistoryEvent
	} else if e.Timestamp == nil {
		return "", backend.ErrNilEventTimestamp
	}

	startEvent := e.GetExecutionStarted()
	if startEvent == nil {
		return "", backend.ErrNotExecutionStarted
	}
	instanceID := startEvent.OrchestrationInstance.InstanceId

	policy := &api.OrchestrationIdReusePolicy{}

	for _, opt := range opts {
		if err := opt(policy); err != nil {
			return "", err
		}
	}

	rows, err := insertOrIgnoreInstanceTableInternal(ctx, tx, e, startEvent)
	if err != nil {
		return "", err
	}

	// instance with same ID already exists
	if rows <= 0 {
		return instanceID, be.handleInstanceExists(ctx, tx, startEvent, policy, e)
	}
	return instanceID, nil
}

func insertOrIgnoreInstanceTableInternal(ctx context.Context, tx *sql.Tx, e *backend.HistoryEvent, startEvent *protos.ExecutionStartedEvent) (int64, error) {
	var parentInstanceID *string
	if pi := startEvent.GetParentInstance(); pi != nil {
		if instanceID := pi.GetOrchestrationInstance().GetInstanceId(); instanceID != "" {
			parentInstanceID = &instanceID
		}
	}
	res, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO [Instances] (
			[Name],
			[Version],
			[InstanceID],
			[ExecutionID],
			[Input],
			[RuntimeStatus],
			[CreatedTime],
			[ParentInstanceID]
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		startEvent.Name,
		startEvent.Version.GetValue(),
		startEvent.OrchestrationInstance.InstanceId,
		startEvent.OrchestrationInstance.ExecutionId.GetValue(),
		startEvent.Input.GetValue(),
		"PENDING",
		e.Timestamp.AsTime(),
		parentInstanceID,
	)
	if err != nil {
		return -1, fmt.Errorf("failed to insert into [Instances] table: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return -1, fmt.Errorf("failed to count the rows affected: %w", err)
	}
	return rows, nil
}

func (be *sqliteBackend) handleInstanceExists(ctx context.Context, tx *sql.Tx, startEvent *protos.ExecutionStartedEvent, policy *api.OrchestrationIdReusePolicy, e *backend.HistoryEvent) error {
	// query RuntimeStatus for the existing instance
	queryRow := tx.QueryRowContext(
		ctx,
		`SELECT [RuntimeStatus] FROM Instances WHERE [InstanceID] = ?`,
		startEvent.OrchestrationInstance.InstanceId,
	)
	var runtimeStatus *string
	err := queryRow.Scan(&runtimeStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return api.ErrInstanceNotFound
	} else if err != nil {
		return fmt.Errorf("failed to scan the Instances table result: %w", err)
	}

	// status not match, return instance duplicate error
	if !isStatusMatch(policy.OperationStatus, helpers.FromRuntimeStatusString(*runtimeStatus)) {
		return api.ErrDuplicateInstance
	}

	// status match
	switch policy.Action {
	case api.REUSE_ID_ACTION_IGNORE:
		// Log an warning message and ignore creating new instance
		be.logger.Warnf("An instance with ID '%s' already exists; dropping duplicate create request", startEvent.OrchestrationInstance.InstanceId)
		return api.ErrIgnoreInstance
	case api.REUSE_ID_ACTION_TERMINATE:
		// terminate existing instance
		if err := be.cleanupOrchestrationStateInternal(ctx, tx, api.InstanceID(startEvent.OrchestrationInstance.InstanceId), false); err != nil {
			return fmt.Errorf("failed to cleanup orchestration status: %w", err)
		}
		// create a new instance
		var rows int64
		if rows, err = insertOrIgnoreInstanceTableInternal(ctx, tx, e, startEvent); err != nil {
			return err
		}

		// should never happen, because we clean up instance before create new one
		if rows <= 0 {
			return fmt.Errorf("failed to insert into [Instances] table because entry already exists")
		}
		return nil
	}
	// default behavior
	return api.ErrDuplicateInstance
}

func isStatusMatch(statuses []protos.OrchestrationStatus, runtimeStatus protos.OrchestrationStatus) bool {
	for _, status := range statuses {
		if status == runtimeStatus {
			return true
		}
	}
	return false
}

func (be *sqliteBackend) cleanupOrchestrationStateInternal(ctx context.Context, tx *sql.Tx, id api.InstanceID, requireCompleted bool) error {
	row := tx.QueryRowContext(ctx, "SELECT 1 FROM Instances WHERE [InstanceID] = ?", string(id))
	if err := row.Err(); err != nil {
		return fmt.Errorf("failed to query for instance existence: %w", err)
	}

	var unused int
	if err := row.Scan(&unused); errors.Is(err, sql.ErrNoRows) {
		return api.ErrInstanceNotFound
	} else if err != nil {
		return fmt.Errorf("failed to scan instance existence: %w", err)
	}

	if requireCompleted {
		// purge orchestration in ['COMPLETED', 'FAILED', 'TERMINATED']
		dbResult, err := tx.ExecContext(ctx, "DELETE FROM Instances WHERE [InstanceID] = ? AND [RuntimeStatus] IN ('COMPLETED', 'FAILED', 'TERMINATED')", string(id))
		if err != nil {
			return fmt.Errorf("failed to delete from the Instances table: %w", err)
		}

		rowsAffected, err := dbResult.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected in Instances delete operation: %w", err)
		}
		if rowsAffected == 0 {
			return api.ErrNotCompleted
		}
	} else {
		// clean up orchestration in all [RuntimeStatus]
		_, err := tx.ExecContext(ctx, "DELETE FROM Instances WHERE [InstanceID] = ?", string(id))
		if err != nil {
			return fmt.Errorf("failed to delete from the Instances table: %w", err)
		}
	}

	_, err := tx.ExecContext(ctx, "DELETE FROM History WHERE [InstanceID] = ?", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from History table: %w", err)
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM NewEvents WHERE [InstanceID] = ?", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM NewTasks WHERE [InstanceID] = ?", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}
	return nil
}

func (be *sqliteBackend) AddNewOrchestrationEvent(ctx context.Context, iid api.InstanceID, e *backend.HistoryEvent) error {
	if e == nil {
		return backend.ErrNilHistoryEvent
	} else if e.Timestamp == nil {
		return backend.ErrNilEventTimestamp
	}

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	_, err = be.db.ExecContext(
		ctx,
		`INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)`,
		string(iid),
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into [NewEvents] table: %w", err)
	}

	return nil
}

// GetOrchestrationMetadata implements backend.Backend
func (be *sqliteBackend) GetOrchestrationMetadata(ctx context.Context, iid api.InstanceID) (*api.OrchestrationMetadata, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	row := be.db.QueryRowContext(
		ctx,
		`SELECT [InstanceID], [Name], [Version], [ParentInstanceID], [RuntimeStatus], [CreatedTime], [LastUpdatedTime], [Input], [Output], [CustomStatus], [FailureDetails]
		FROM Instances WHERE [InstanceID] = ?`,
		string(iid),
	)

	err := row.Err()
	if errors.Is(err, sql.ErrNoRows) {
		return nil, api.ErrInstanceNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to query the Instances table: %w", row.Err())
	}

	var instanceID *string
	var name *string
	var version *string
	var parentInstanceID *string
	var runtimeStatus *string
	var createdAt *time.Time
	var lastUpdatedAt *time.Time
	var input *string
	var output *string
	var customStatus *string
	var failureDetails *protos.TaskFailureDetails

	var failureDetailsPayload []byte
	err = row.Scan(&instanceID, &name, &version, &parentInstanceID, &runtimeStatus, &createdAt, &lastUpdatedAt, &input, &output, &customStatus, &failureDetailsPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, api.ErrInstanceNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to scan the Instances table result: %w", err)
	}

	if input == nil {
		input = &emptyString
	}

	if output == nil {
		output = &emptyString
	}

	if customStatus == nil {
		customStatus = &emptyString
	}

	if len(failureDetailsPayload) > 0 {
		failureDetails = new(protos.TaskFailureDetails)
		if err := proto.Unmarshal(failureDetailsPayload, failureDetails); err != nil {
			return nil, fmt.Errorf("failed to unmarshal failure details: %w", err)
		}
	}

	metadata := api.NewOrchestrationMetadata(
		iid,
		*name,
		helpers.FromRuntimeStatusString(*runtimeStatus),
		*createdAt,
		*lastUpdatedAt,
		*input,
		*output,
		*customStatus,
		failureDetails,
	)
	if version != nil {
		metadata.Version = *version
	}
	if parentInstanceID != nil {
		metadata.ParentInstanceID = api.InstanceID(*parentInstanceID)
	}
	return metadata, nil
}

// GetOrchestrationRuntimeState implements backend.Backend
func (be *sqliteBackend) GetOrchestrationRuntimeState(ctx context.Context, wi *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	rows, err := be.db.QueryContext(
		ctx,
		"SELECT [EventPayload] FROM History WHERE [InstanceID] = ? ORDER BY [SequenceNumber] ASC",
		string(wi.InstanceID),
	)
	if err != nil {
		return nil, err
	}

	existingEvents := make([]*protos.HistoryEvent, 0, 50)
	for rows.Next() {
		var eventPayload []byte
		if err := rows.Scan(&eventPayload); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}

		e, err := backend.UnmarshalHistoryEvent(eventPayload)
		if err != nil {
			return nil, err
		}

		existingEvents = append(existingEvents, e)
	}

	state := backend.NewOrchestrationRuntimeState(wi.InstanceID, existingEvents)
	return state, nil
}

// GetOrchestrationWorkItem implements backend.Backend
func (be *sqliteBackend) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.OrchestrationLockTimeout)

	// Place a lock on an orchestration instance that has new events that are ready to be executed.
	row := tx.QueryRowContext(
		ctx,
		`UPDATE Instances SET [LockedBy] = ?, [LockExpiration] = ?
		WHERE [rowid] = (
			SELECT [rowid] FROM Instances I
			WHERE (I.[LockExpiration] IS NULL OR I.[LockExpiration] < ?) AND EXISTS (
				SELECT 1 FROM NewEvents E
				WHERE E.[InstanceID] = I.[InstanceID] AND (E.[VisibleTime] IS NULL OR E.[VisibleTime] < ?)
			)
			LIMIT 1
		) RETURNING [InstanceID]`,
		be.workerName,     // LockedBy for Instances table
		newLockExpiration, // Updated LockExpiration for Instances table
		now,               // LockExpiration for Instances table
		now,               // VisibleTime for NewEvents table
	)

	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}

	var instanceID string
	if err := row.Scan(&instanceID); err != nil {
		if err == sql.ErrNoRows {
			// No new events to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the orchestration work-item: %w", err)
	}

	// TODO: Get all the unprocessed events associated with the locked instance
	events, err := tx.QueryContext(
		ctx,
		`UPDATE NewEvents SET [DequeueCount] = [DequeueCount] + 1, [LockedBy] = ? WHERE rowid IN (
			SELECT rowid FROM NewEvents
			WHERE [InstanceID] = ? AND ([VisibleTime] IS NULL OR [VisibleTime] <= ?)
			LIMIT 1000
		)
		RETURNING [EventPayload], [DequeueCount], [Timestamp]`,
		be.workerName,
		instanceID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}

	maxDequeueCount := int32(0)
	var enqueuedAt time.Time

	newEvents := make([]*protos.HistoryEvent, 0, 10)
	for events.Next() {
		var eventPayload []byte
		var dequeueCount int32
		var timestamp time.Time
		if err := events.Scan(&eventPayload, &dequeueCount, &timestamp); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}

		if dequeueCount > maxDequeueCount {
			maxDequeueCount = dequeueCount
		}
		if enqueuedAt.IsZero() || timestamp.Before(enqueuedAt) {
			enqueuedAt = timestamp
		}

		e, err := backend.UnmarshalHistoryEvent(eventPayload)
		if err != nil {
			return nil, err
		}

		newEvents = append(newEvents, e)
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to update orchestration work-item: %w", err)
	}

	wi := &backend.OrchestrationWorkItem{
		InstanceID: api.InstanceID(instanceID),
		NewEvents:  newEvents,
		LockedBy:   be.workerName,
		RetryCount: maxDequeueCount - 1,
		EnqueuedAt: enqueuedAt,
	}

	return wi, nil
}

func (be *sqliteBackend) GetActivityWorkItem(ctx context.Context) (*backend.ActivityWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.ActivityLockTimeout)

	row := be.db.QueryRowContext(
		ctx,
		`UPDATE NewTasks SET [LockedBy] = ?, [LockExpiration] = ?, [DequeueCount] = [DequeueCount] + 1
		WHERE [SequenceNumber] = (
			SELECT [SequenceNumber] FROM NewTasks T
			WHERE T.[LockExpiration] IS NULL OR T.[LockExpiration] < ?
			ORDER BY T.[SequenceNumber] ASC
			LIMIT 1
		) RETURNING [SequenceNumber], [InstanceID], [EventPayload], [Timestamp], [DequeueCount]`,
		be.workerName,
		newLockExpiration,
		now,
	)

	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("failed to query for activity work-items: %w", err)
	}

	var sequenceNumber int64
	var instanceID string
	var eventPayload []byte
	var enqueuedAt time.Time
	var dequeueCount int32

	if err := row.Scan(&sequenceNumber, &instanceID, &eventPayload, &enqueuedAt, &dequeueCount); err != nil {
		if err == sql.ErrNoRows {
			// No new activity tasks to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the activity work-item: %w", err)
	}

	e, err := backend.UnmarshalHistoryEvent(eventPayload)
	if err != nil {
		return nil, err
	}

	wi := &backend.ActivityWorkItem{
		SequenceNumber: sequenceNumber,
		InstanceID:     api.InstanceID(instanceID),
		NewEvent:       e,
		LockedBy:       be.workerName,
		RetryCount:     dequeueCount - 1,
		EnqueuedAt:     enqueuedAt,
	}
	return wi, nil
}

func (be *sqliteBackend) GetOrchestrationBacklog(ctx context.Context) (backend.BacklogMetric, error) {
	if err := be.ensureDB(); err != nil {
		return backend.BacklogMetric{}, err
	}
	now := time.Now().UTC()
	row := be.db.QueryRowContext(
		ctx,
		`SELECT COUNT(DISTINCT E.[InstanceID]),
			COALESCE((julianday('now') - julianday(MIN(E.[Timestamp]))) * 86400.0, 0)
		FROM NewEvents E
		INNER JOIN Instances I ON I.[InstanceID] = E.[InstanceID]
		WHERE (I.[LockExpiration] IS NULL OR I.[LockExpiration] < ?)
			AND (E.[VisibleTime] IS NULL OR E.[VisibleTime] <= ?)`,
		now,
		now,
	)
	return scanSqliteBacklog(row, backend.WorkItemKindOrchestration)
}

func (be *sqliteBackend) GetActivityBacklog(ctx context.Context) (backend.BacklogMetric, error) {
	if err := be.ensureDB(); err != nil {
		return backend.BacklogMetric{}, err
	}
	now := time.Now().UTC()
	row := be.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*),
			COALESCE((julianday('now') - julianday(MIN([Timestamp]))) * 86400.0, 0)
		FROM NewTasks
		WHERE [LockExpiration] IS NULL OR [LockExpiration] < ?`,
		now,
	)
	return scanSqliteBacklog(row, backend.WorkItemKindActivity)
}

func scanSqliteBacklog(row *sql.Row, kind backend.WorkItemKind) (backend.BacklogMetric, error) {
	metric := backend.BacklogMetric{Kind: kind}
	var oldestAgeSeconds float64
	if err := row.Scan(&metric.Depth, &oldestAgeSeconds); err != nil {
		return backend.BacklogMetric{}, fmt.Errorf("failed to inspect %s backlog: %w", kind, err)
	}
	if oldestAgeSeconds > 0 {
		metric.OldestAge = time.Duration(oldestAgeSeconds * float64(time.Second))
	}
	return metric, nil
}

func (be *sqliteBackend) CompleteActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	bytes, err := backend.MarshalHistoryEvent(wi.Result)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)", string(wi.InstanceID), bytes)
	if err != nil {
		return fmt.Errorf("failed to insert into NewEvents table: %w", err)
	}

	dbResult, err := tx.ExecContext(ctx, "DELETE FROM NewTasks WHERE [SequenceNumber] = ? AND [LockedBy] = ?", wi.SequenceNumber, wi.LockedBy)
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (be *sqliteBackend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	var lockExpiration *time.Time
	if delay := wi.GetAbandonDelay(); delay > 0 {
		visibleAt := time.Now().UTC().Add(delay)
		lockExpiration = &visibleAt
	}
	dbResult, err := be.db.ExecContext(
		ctx,
		"UPDATE NewTasks SET [LockedBy] = NULL, [LockExpiration] = ? WHERE [SequenceNumber] = ? AND [LockedBy] = ?",
		lockExpiration,
		wi.SequenceNumber,
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update the NewTasks table for abandon: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by update statement for abandon: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	return nil
}

func (be *sqliteBackend) SignalEntity(ctx context.Context, request *protos.SignalEntityRequest) error {
	if err := be.ensureDB(); err != nil {
		return err
	}
	event, err := backend.NewEntitySignalEvent(request)
	if err != nil {
		return err
	}
	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := be.addEntityMessageTx(ctx, tx, request.InstanceId, event); err != nil {
		return err
	}
	return tx.Commit()
}

func (be *sqliteBackend) GetEntityWorkItem(ctx context.Context) (*backend.EntityWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 32; attempt++ {
		workItem, progressed, err := be.getEntityWorkItemOnce(ctx)
		if err != nil || workItem != nil {
			return workItem, err
		}
		if !progressed {
			return nil, backend.ErrNoWorkItems
		}
	}
	return nil, backend.ErrNoWorkItems
}

type sqliteEntityMessage struct {
	sequenceNumber int64
	event          *protos.HistoryEvent
	descriptor     backend.EntityMessageDescriptor
	dequeueCount   int32
	enqueuedAt     time.Time
}

func (be *sqliteBackend) getEntityWorkItemOnce(ctx context.Context) (*backend.EntityWorkItem, bool, error) {
	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().UTC()
	lockExpiration := now.Add(be.options.EntityLockTimeout)
	row := tx.QueryRowContext(
		ctx,
		`UPDATE Entities SET [WorkItemLockedBy] = ?, [WorkItemLockExpiration] = ?
		WHERE [rowid] = (
			SELECT E.[rowid] FROM Entities E
			WHERE (E.[WorkItemLockExpiration] IS NULL OR E.[WorkItemLockExpiration] < ?)
			AND EXISTS (
				SELECT 1 FROM EntityMessages M
				WHERE M.[InstanceID] = E.[InstanceID]
				AND (M.[VisibleTime] IS NULL OR M.[VisibleTime] <= ?)
				AND (
					E.[LockedBy] IS NULL
					OR (M.[MessageKind] = 'call' AND M.[ParentInstanceID] = E.[LockedBy])
					OR (M.[MessageKind] = 'unlock' AND M.[ParentInstanceID] = E.[LockedBy])
				)
			)
			ORDER BY (
				SELECT MIN(M2.[SequenceNumber]) FROM EntityMessages M2
				WHERE M2.[InstanceID] = E.[InstanceID]
				AND (M2.[VisibleTime] IS NULL OR M2.[VisibleTime] <= ?)
				AND (
					E.[LockedBy] IS NULL
					OR (M2.[MessageKind] = 'call' AND M2.[ParentInstanceID] = E.[LockedBy])
					OR (M2.[MessageKind] = 'unlock' AND M2.[ParentInstanceID] = E.[LockedBy])
				)
			)
			LIMIT 1
		)
		RETURNING [InstanceID], [ExecutionID], [State], [LockedBy]`,
		be.workerName,
		lockExpiration,
		now,
		now,
		now,
	)

	var instanceID string
	var executionID string
	var state sql.NullString
	var criticalSectionOwner sql.NullString
	if err := row.Scan(&instanceID, &executionID, &state, &criticalSectionOwner); errors.Is(err, sql.ErrNoRows) {
		return nil, false, backend.ErrNoWorkItems
	} else if err != nil {
		return nil, false, fmt.Errorf("failed to lock entity work item: %w", err)
	}

	query := `SELECT [SequenceNumber], [EventPayload], [DequeueCount], [Timestamp]
		FROM EntityMessages
		WHERE [InstanceID] = ? AND ([VisibleTime] IS NULL OR [VisibleTime] <= ?)`
	args := []any{instanceID, now}
	if criticalSectionOwner.Valid {
		query += ` AND (([MessageKind] = 'call' AND [ParentInstanceID] = ?)
			OR ([MessageKind] = 'unlock' AND [ParentInstanceID] = ?))`
		args = append(args, criticalSectionOwner.String, criticalSectionOwner.String)
	}
	query += " ORDER BY [SequenceNumber] ASC LIMIT ?"
	args = append(args, max(be.options.MaxEntityOperationBatchSize+1, 2))
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load entity messages: %w", err)
	}
	defer rows.Close()

	messages := make([]sqliteEntityMessage, 0, be.options.MaxEntityOperationBatchSize+1)
	for rows.Next() {
		var raw sqliteEntityMessage
		var payload []byte
		if err := rows.Scan(&raw.sequenceNumber, &payload, &raw.dequeueCount, &raw.enqueuedAt); err != nil {
			return nil, false, err
		}
		raw.event, err = backend.UnmarshalHistoryEvent(payload)
		if err != nil {
			return nil, false, err
		}
		raw.descriptor, err = backend.DescribeEntityMessage(raw.event)
		if err != nil {
			return nil, false, err
		}
		messages = append(messages, raw)
	}

	selected := make([]sqliteEntityMessage, 0, be.options.MaxEntityOperationBatchSize)
	for _, message := range messages {
		switch message.descriptor.Kind {
		case "signal", "call":
			selected = append(selected, message)
			if len(selected) == be.options.MaxEntityOperationBatchSize {
				break
			}
		case "lock":
			if len(selected) > 0 {
				break
			}
			if err := be.processEntityLockTx(ctx, tx, instanceID, message); err != nil {
				return nil, false, err
			}
			if err := be.releaseEntityWorkLockTx(ctx, tx, instanceID); err != nil {
				return nil, false, err
			}
			if err := tx.Commit(); err != nil {
				return nil, false, err
			}
			return nil, true, nil
		case "unlock":
			if len(selected) > 0 {
				break
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM EntityMessages WHERE [SequenceNumber] = ?", message.sequenceNumber); err != nil {
				return nil, false, err
			}
			if criticalSectionOwner.Valid && criticalSectionOwner.String == message.descriptor.ParentInstanceID {
				if _, err := tx.ExecContext(ctx, "UPDATE Entities SET [LockedBy] = NULL WHERE [InstanceID] = ?", instanceID); err != nil {
					return nil, false, err
				}
			}
			if err := be.releaseEntityWorkLockTx(ctx, tx, instanceID); err != nil {
				return nil, false, err
			}
			if err := tx.Commit(); err != nil {
				return nil, false, err
			}
			return nil, true, nil
		}
		if len(selected) == be.options.MaxEntityOperationBatchSize ||
			(message.descriptor.Kind == "lock" || message.descriptor.Kind == "unlock") {
			break
		}
	}
	if len(selected) == 0 {
		if err := be.releaseEntityWorkLockTx(ctx, tx, instanceID); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return nil, false, backend.ErrNoWorkItems
	}

	workItem := &backend.EntityWorkItem{
		ExecutionID: executionID,
		LockedBy:    be.workerName,
		Operations:  make([]*protos.HistoryEvent, 0, len(selected)),
		MessageIDs:  make([]int64, 0, len(selected)),
	}
	entityID, err := api.EntityIDFromString(instanceID)
	if err != nil {
		return nil, false, err
	}
	workItem.InstanceID = entityID
	if state.Valid {
		workItem.State = &state.String
	}
	for _, message := range selected {
		result, err := tx.ExecContext(
			ctx,
			`UPDATE EntityMessages SET [LockedBy] = ?, [DequeueCount] = [DequeueCount] + 1
			WHERE [SequenceNumber] = ?`,
			be.workerName,
			message.sequenceNumber,
		)
		if err != nil {
			return nil, false, err
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			return nil, false, backend.ErrWorkItemLockLost
		}
		workItem.Operations = append(workItem.Operations, message.event)
		workItem.MessageIDs = append(workItem.MessageIDs, message.sequenceNumber)
		if message.dequeueCount > workItem.RetryCount {
			workItem.RetryCount = message.dequeueCount
		}
		if workItem.EnqueuedAt.IsZero() || message.enqueuedAt.Before(workItem.EnqueuedAt) {
			workItem.EnqueuedAt = message.enqueuedAt
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return workItem, false, nil
}

func (be *sqliteBackend) processEntityLockTx(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
	message sqliteEntityMessage,
) error {
	lockRequest := message.event.GetEntityLockRequested()
	if lockRequest == nil || message.descriptor.ParentInstanceID == "" {
		return fmt.Errorf("invalid entity lock request")
	}
	if _, err := tx.ExecContext(
		ctx,
		"UPDATE Entities SET [LockedBy] = ? WHERE [InstanceID] = ?",
		message.descriptor.ParentInstanceID,
		instanceID,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM EntityMessages WHERE [SequenceNumber] = ?", message.sequenceNumber); err != nil {
		return err
	}
	next := proto.Clone(message.event).(*protos.HistoryEvent)
	nextRequest := next.GetEntityLockRequested()
	nextRequest.Position++
	if int(nextRequest.Position) < len(nextRequest.LockSet) {
		return be.addEntityMessageTx(ctx, tx, nextRequest.LockSet[nextRequest.Position], next)
	}
	return be.insertOrchestrationEventTx(
		ctx,
		tx,
		message.descriptor.ParentInstanceID,
		backend.NewEntityLockGrantedEvent(nextRequest.CriticalSectionId),
		nil,
	)
}

func (be *sqliteBackend) CompleteEntityWorkItem(ctx context.Context, wi *backend.EntityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}
	if wi == nil || wi.Result == nil {
		return fmt.Errorf("entity work item result is required")
	}
	if wi.Result.FailureDetails != nil {
		return fmt.Errorf("entity batch failure cannot be committed: %s", wi.Result.FailureDetails.ErrorMessage)
	}
	if len(wi.Result.Results) > len(wi.MessageIDs) {
		return fmt.Errorf("entity result count exceeds operation count")
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	var state any
	if wi.Result.EntityState != nil {
		state = wi.Result.EntityState.Value
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE Entities SET [State] = ?, [LastModifiedTime] = ?, [ExecutionID] = ?,
			[WorkItemLockedBy] = NULL, [WorkItemLockExpiration] = NULL
		WHERE [InstanceID] = ? AND [WorkItemLockedBy] = ?`,
		state,
		time.Now().UTC(),
		uuid.NewString(),
		wi.InstanceID.String(),
		wi.LockedBy,
	)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return backend.ErrWorkItemLockLost
	}

	for index, messageID := range wi.MessageIDs {
		if index < len(wi.Result.Results) {
			result, err := tx.ExecContext(
				ctx,
				"DELETE FROM EntityMessages WHERE [SequenceNumber] = ? AND [LockedBy] = ?",
				messageID,
				wi.LockedBy,
			)
			if err != nil {
				return err
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				return backend.ErrWorkItemLockLost
			}
		} else if _, err := tx.ExecContext(
			ctx,
			"UPDATE EntityMessages SET [LockedBy] = NULL WHERE [SequenceNumber] = ? AND [LockedBy] = ?",
			messageID,
			wi.LockedBy,
		); err != nil {
			return err
		}
	}

	responseCount := min(len(wi.Result.Results), len(wi.Result.OperationInfos))
	for index := 0; index < responseCount; index++ {
		event, target, err := backend.NewEntityOperationResponseEvent(
			wi.Result.OperationInfos[index],
			wi.Result.Results[index],
		)
		if err != nil {
			return err
		}
		if event != nil {
			if err := be.insertOrchestrationEventTx(ctx, tx, target, event, nil); err != nil {
				return err
			}
		}
	}

	for index, action := range wi.Result.Actions {
		switch {
		case action.GetSendSignal() != nil:
			signal := action.GetSendSignal()
			event := backend.NewEntitySignalMessage(wi.InstanceID.String(), wi.ExecutionID, index, signal)
			if err := be.addEntityMessageTx(ctx, tx, signal.InstanceId, event); err != nil {
				return err
			}
		case action.GetStartNewOrchestration() != nil:
			start := action.GetStartNewOrchestration()
			event := helpers.NewExecutionStartedEvent(
				start.Name,
				start.InstanceId,
				start.Input,
				nil,
				start.ParentTraceContext,
				start.ScheduledTime,
				start.Version,
			)
			instanceID, err := be.createOrchestrationInstanceInternal(ctx, event, tx)
			if err != nil && !errors.Is(err, api.ErrDuplicateInstance) {
				return err
			}
			if err == nil {
				if err := be.insertOrchestrationEventTx(ctx, tx, instanceID, event, nil); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown entity operation action")
		}
	}

	return tx.Commit()
}

func (be *sqliteBackend) AbandonEntityWorkItem(ctx context.Context, wi *backend.EntityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}
	if wi == nil {
		return fmt.Errorf("entity work item is required")
	}
	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for _, messageID := range wi.MessageIDs {
		if _, err := tx.ExecContext(
			ctx,
			"UPDATE EntityMessages SET [LockedBy] = NULL WHERE [SequenceNumber] = ? AND [LockedBy] = ?",
			messageID,
			wi.LockedBy,
		); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE Entities SET [WorkItemLockedBy] = NULL, [WorkItemLockExpiration] = NULL
		WHERE [InstanceID] = ? AND [WorkItemLockedBy] = ?`,
		wi.InstanceID.String(),
		wi.LockedBy,
	)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return backend.ErrWorkItemLockLost
	}
	return tx.Commit()
}

func (be *sqliteBackend) GetEntityMetadata(ctx context.Context, entityID api.EntityID, includeState bool) (*api.EntityMetadata, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	stateColumn := "NULL"
	if includeState {
		stateColumn = "E.[State]"
	}
	row := be.db.QueryRowContext(
		ctx,
		`SELECT E.[LastModifiedTime], E.[LockedBy], `+stateColumn+`,
			(SELECT COUNT(*) FROM EntityMessages M WHERE M.[InstanceID] = E.[InstanceID])
		FROM Entities E WHERE E.[InstanceID] = ?`,
		entityID.String(),
	)
	var lastModified time.Time
	var lockedBy sql.NullString
	var state sql.NullString
	var backlog int32
	if err := row.Scan(&lastModified, &lockedBy, &state, &backlog); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &api.EntityMetadata{
		InstanceID:       entityID,
		LastModifiedTime: lastModified,
		BacklogQueueSize: backlog,
		LockedBy:         lockedBy.String,
		SerializedState:  state.String,
	}, nil
}

func (be *sqliteBackend) QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	pageSize = min(pageSize, 1000)
	sqlQuery := `SELECT E.[InstanceID], E.[LastModifiedTime], E.[LockedBy], E.[State],
		(SELECT COUNT(*) FROM EntityMessages M WHERE M.[InstanceID] = E.[InstanceID])
		FROM Entities E WHERE E.[InstanceID] > ?`
	args := []any{query.ContinuationToken}
	if query.InstanceIDStartsWith != "" {
		sqlQuery += " AND E.[InstanceID] LIKE ?"
		args = append(args, query.InstanceIDStartsWith+"%")
	}
	if !query.LastModifiedFrom.IsZero() {
		sqlQuery += " AND E.[LastModifiedTime] >= ?"
		args = append(args, query.LastModifiedFrom)
	}
	if !query.LastModifiedTo.IsZero() {
		sqlQuery += " AND E.[LastModifiedTime] < ?"
		args = append(args, query.LastModifiedTo)
	}
	if !query.IncludeTransient {
		sqlQuery += " AND E.[State] IS NOT NULL"
	}
	sqlQuery += " ORDER BY E.[InstanceID] ASC LIMIT ?"
	args = append(args, pageSize+1)
	rows, err := be.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &api.EntityQueryResults{Entities: make([]*api.EntityMetadata, 0, pageSize)}
	for rows.Next() {
		var instanceID string
		var lastModified time.Time
		var lockedBy sql.NullString
		var state sql.NullString
		var backlog int32
		if err := rows.Scan(&instanceID, &lastModified, &lockedBy, &state, &backlog); err != nil {
			return nil, err
		}
		if int32(len(result.Entities)) == pageSize {
			result.ContinuationToken = result.Entities[len(result.Entities)-1].InstanceID.String()
			break
		}
		entityID, err := api.EntityIDFromString(instanceID)
		if err != nil {
			return nil, err
		}
		metadata := &api.EntityMetadata{
			InstanceID:       entityID,
			LastModifiedTime: lastModified,
			BacklogQueueSize: backlog,
			LockedBy:         lockedBy.String,
		}
		if query.IncludeState {
			metadata.SerializedState = state.String
		}
		result.Entities = append(result.Entities, metadata)
	}
	return result, rows.Err()
}

func (be *sqliteBackend) CleanEntityStorage(ctx context.Context, request api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	rows, err := tx.QueryContext(
		ctx,
		"SELECT [InstanceID] FROM Entities WHERE [InstanceID] > ? ORDER BY [InstanceID] LIMIT 1001",
		request.ContinuationToken,
	)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	result := &api.CleanEntityStorageResult{}
	if len(ids) > 1000 {
		result.ContinuationToken = ids[999]
		ids = ids[:1000]
	}
	for _, id := range ids {
		if request.ReleaseOrphanedLocks {
			dbResult, err := tx.ExecContext(
				ctx,
				`UPDATE Entities SET [LockedBy] = NULL
				WHERE [InstanceID] = ? AND [LockedBy] IS NOT NULL
				AND NOT EXISTS (
					SELECT 1 FROM Instances I WHERE I.[InstanceID] = Entities.[LockedBy]
					AND I.[RuntimeStatus] IN ('PENDING', 'RUNNING', 'SUSPENDED')
				)`,
				id,
			)
			if err != nil {
				return nil, err
			}
			if count, err := dbResult.RowsAffected(); err == nil {
				result.OrphanedLocksReleased += int32(count)
			}
		}
		if request.RemoveEmptyEntities {
			dbResult, err := tx.ExecContext(
				ctx,
				`DELETE FROM Entities WHERE [InstanceID] = ? AND [State] IS NULL
				AND [LockedBy] IS NULL AND [WorkItemLockedBy] IS NULL
				AND NOT EXISTS (SELECT 1 FROM EntityMessages M WHERE M.[InstanceID] = Entities.[InstanceID])`,
				id,
			)
			if err != nil {
				return nil, err
			}
			if count, err := dbResult.RowsAffected(); err == nil {
				result.EmptyEntitiesRemoved += int32(count)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (be *sqliteBackend) GetEntityBacklog(ctx context.Context) (backend.BacklogMetric, error) {
	if err := be.ensureDB(); err != nil {
		return backend.BacklogMetric{}, err
	}
	row := be.db.QueryRowContext(
		ctx,
		`SELECT COUNT(DISTINCT [InstanceID]),
			COALESCE((julianday('now') - julianday(MIN([Timestamp]))) * 86400.0, 0)
		FROM EntityMessages WHERE [VisibleTime] IS NULL OR [VisibleTime] <= ?`,
		time.Now().UTC(),
	)
	return scanSqliteBacklog(row, backend.WorkItemKindEntity)
}

func (be *sqliteBackend) addEntityMessageTx(
	ctx context.Context,
	tx *sql.Tx,
	targetInstanceID string,
	event *protos.HistoryEvent,
) error {
	entityID, err := api.EntityIDFromString(targetInstanceID)
	if err != nil {
		return err
	}
	descriptor, err := backend.DescribeEntityMessage(event)
	if err != nil {
		return err
	}
	payload, err := backend.MarshalHistoryEvent(event)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO Entities ([InstanceID], [ExecutionID], [CreatedTime], [LastModifiedTime])
		VALUES (?, ?, ?, ?)`,
		entityID.String(),
		uuid.NewString(),
		now,
		now,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO EntityMessages
			([InstanceID], [RequestID], [MessageKind], [ParentInstanceID], [Timestamp], [VisibleTime], [EventPayload])
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		entityID.String(),
		descriptor.RequestID,
		descriptor.Kind,
		nullString(descriptor.ParentInstanceID),
		event.Timestamp.AsTime(),
		descriptor.VisibleTime,
		payload,
	); err != nil {
		return err
	}
	_, err = tx.ExecContext(
		ctx,
		"UPDATE Entities SET [LastModifiedTime] = ? WHERE [InstanceID] = ?",
		now,
		entityID.String(),
	)
	return err
}

func (be *sqliteBackend) insertOrchestrationEventTx(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
	event *protos.HistoryEvent,
	visibleTime *time.Time,
) error {
	payload, err := backend.MarshalHistoryEvent(event)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO NewEvents ([InstanceID], [EventPayload], [VisibleTime]) VALUES (?, ?, ?)",
		instanceID,
		payload,
		visibleTime,
	)
	return err
}

func (be *sqliteBackend) releaseEntityWorkLockTx(ctx context.Context, tx *sql.Tx, instanceID string) error {
	result, err := tx.ExecContext(
		ctx,
		`UPDATE Entities SET [WorkItemLockedBy] = NULL, [WorkItemLockExpiration] = NULL
		WHERE [InstanceID] = ? AND [WorkItemLockedBy] = ?`,
		instanceID,
		be.workerName,
	)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return backend.ErrWorkItemLockLost
	}
	return nil
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (be *sqliteBackend) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	if err := be.cleanupOrchestrationStateInternal(ctx, tx, id, true); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Start implements backend.Backend
func (be *sqliteBackend) Start(context.Context) error {
	if be.db == nil {
		db, err := sql.Open("sqlite", be.dsn)
		if err != nil {
			return fmt.Errorf("failed to open the database: %w", err)
		}

		// TODO: This is to avoid SQLITE_BUSY errors when there are concurrent
		//       operations on the database. However, it can hurt performance.
		//	     We should consider removing this and looking for alternate
		//       solutions if sqlite performance becomes a problem for users.
		db.SetMaxOpenConns(1)

		be.db = db
	}

	return nil
}

// Stop implements backend.Backend
func (be *sqliteBackend) Stop(context.Context) error {
	if be.db != nil {
		db := be.db
		be.db = nil
		return db.Close()
	}

	return nil
}

func (be *sqliteBackend) ensureDB() error {
	if be.db == nil {
		return backend.ErrNotInitialized
	}
	return nil
}

func (be *sqliteBackend) String() string {
	return fmt.Sprintf("sqlite::%s", be.options.FilePath)
}
