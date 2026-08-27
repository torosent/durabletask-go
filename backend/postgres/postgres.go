package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

var emptyString string = ""

type PostgresOptions struct {
	PgOptions                   *pgxpool.Config
	OrchestrationLockTimeout    time.Duration
	ActivityLockTimeout         time.Duration
	EntityLockTimeout           time.Duration
	MaxEntityOperationBatchSize int
}

type postgresBackend struct {
	db         *pgxpool.Pool
	workerName string
	logger     backend.Logger
	options    *PostgresOptions
}

// NewPostgresOptions creates a new options object for the postgres backend provider.
func NewPostgresOptions(host string, port uint16, database string, user string, password string) *PostgresOptions {
	conf, err := pgxpool.ParseConfig(fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", user, password, host, port, database))
	if err != nil {
		panic(fmt.Errorf("failed to parse the postgres connection string: %w", err))
	}
	conf.ConnConfig.ConnectTimeout = 2 * time.Minute
	conf.MaxConnLifetime = 2 * time.Minute
	conf.MaxConnIdleTime = 2 * time.Minute
	conf.MaxConns = int32(max(4, runtime.GOMAXPROCS(0)))

	return &PostgresOptions{
		PgOptions:                   conf,
		OrchestrationLockTimeout:    2 * time.Minute,
		ActivityLockTimeout:         2 * time.Minute,
		EntityLockTimeout:           2 * time.Minute,
		MaxEntityOperationBatchSize: 100,
	}
}

// NewPostgresBackend creates a new postgres-based Backend object.
func NewPostgresBackend(opts *PostgresOptions, logger backend.Logger) backend.Backend {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	u, err := uuid.NewV7()
	if err != nil {
		u = uuid.New()
	}
	uuidStr := u.String()

	if opts == nil {
		opts = NewPostgresOptions("localhost", 5432, "postgres", "postgres", "postgres")
	}

	return &postgresBackend{
		db:         nil,
		workerName: fmt.Sprintf("%s,%d,%s", hostname, pid, uuidStr),
		options:    opts,
		logger:     logger,
	}
}

// CreateTaskHub creates the postgres database and applies the schema
func (be *postgresBackend) CreateTaskHub(ctx context.Context) error {
	if err := be.Start(ctx); err != nil {
		be.logger.Error("CreateTaskHub", "failed to start the backend", err)
		return fmt.Errorf("failed to start the backend: %w", err)
	}

	// Initialize database
	if _, err := be.db.Exec(ctx, schema); err != nil {
		be.logger.Error("CreateTaskHub", "failed to initialize the database", err)
		return fmt.Errorf("failed to initialize the database: %w", err)
	}

	return nil
}

func (be *postgresBackend) DeleteTaskHub(ctx context.Context) error {
	if be.db == nil {
		return nil
	}

	_, err := be.db.Exec(ctx, "DROP TABLE IF EXISTS Instances CASCADE")
	if err != nil {
		be.logger.Error("DeleteTaskHub", "failed to drop Instances table", err)
		return fmt.Errorf("failed to drop Instances table: %w", err)
	}
	_, err = be.db.Exec(ctx, "DROP TABLE IF EXISTS History CASCADE")
	if err != nil {
		be.logger.Error("DeleteTaskHub", "failed to drop History table", err)
		return fmt.Errorf("failed to drop History table: %w", err)
	}
	_, err = be.db.Exec(ctx, "DROP TABLE IF EXISTS NewEvents CASCADE")
	if err != nil {
		be.logger.Error("DeleteTaskHub", "failed to drop NewEvents table", err)
		return fmt.Errorf("failed to drop NewEvents table: %w", err)
	}
	_, err = be.db.Exec(ctx, "DROP TABLE IF EXISTS NewTasks CASCADE")
	if err != nil {
		be.logger.Error("DeleteTaskHub", "failed to drop NewTasks table", err)
		return fmt.Errorf("failed to drop NewTasks table: %w", err)
	}
	_, err = be.db.Exec(ctx, "DROP TABLE IF EXISTS EntityMessages CASCADE")
	if err != nil {
		return fmt.Errorf("failed to drop EntityMessages table: %w", err)
	}
	_, err = be.db.Exec(ctx, "DROP TABLE IF EXISTS Entities CASCADE")
	if err != nil {
		return fmt.Errorf("failed to drop Entities table: %w", err)
	}

	if err := be.Stop(ctx); err != nil {
		be.logger.Error("DeleteTaskHub", "failed to stop the backend", err)
		return fmt.Errorf("failed to stop the backend: %w", err)
	}

	return nil
}

// AbandonOrchestrationWorkItem implements backend.Backend
func (be *postgresBackend) AbandonOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	var visibleTime *time.Time = nil
	if delay := wi.GetAbandonDelay(); delay > 0 {
		t := time.Now().UTC().Add(delay)
		visibleTime = &t
	}

	dbResult, err := tx.Exec(
		ctx,
		"UPDATE NewEvents SET LockedBy = NULL, VisibleTime = $1 WHERE InstanceID = $2 AND LockedBy = $3",
		visibleTime,
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update NewEvents table: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE NewEvents statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	dbResult, err = tx.Exec(
		ctx,
		"UPDATE Instances SET LockedBy = NULL, LockExpiration = NULL WHERE InstanceID = $1 AND LockedBy = $2",
		string(wi.InstanceID),
		wi.LockedBy,
	)

	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	rowsAffected = dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE Instances statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CompleteOrchestrationWorkItem implements backend.Backend
func (be *postgresBackend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	now := time.Now().UTC()

	// Dynamically generate the UPDATE statement for the Instances table
	var sqlSB strings.Builder
	sqlSB.WriteString("UPDATE Instances SET ")

	sqlUpdateArgs := make([]any, 0, 10)
	isCreated := false
	isCompleted := false

	currIndex := 1
	for _, e := range wi.State.NewEvents() {
		if es := e.GetExecutionStarted(); es != nil {
			if isCreated {
				// TODO: Log warning about duplicate start event
				continue
			}
			isCreated = true
			fmt.Fprintf(&sqlSB, "CreatedTime = $%d, Input = $%d, ", currIndex, currIndex+1)
			currIndex += 2
			sqlUpdateArgs = append(sqlUpdateArgs, e.Timestamp.AsTime())
			sqlUpdateArgs = append(sqlUpdateArgs, es.Input.GetValue())
		} else if ec := e.GetExecutionCompleted(); ec != nil {
			if isCompleted {
				// TODO: Log warning about duplicate completion event
				continue
			}
			isCompleted = true
			fmt.Fprintf(&sqlSB, "CompletedTime = $%d, Output = $%d, FailureDetails = $%d, ", currIndex, currIndex+1, currIndex+2)
			currIndex += 3
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
		fmt.Fprintf(&sqlSB, "CustomStatus = $%d, ", currIndex)
		currIndex++
		sqlUpdateArgs = append(sqlUpdateArgs, wi.State.CustomStatus.Value)
	}

	// TODO: Support for stickiness, which would extend the LockExpiration
	fmt.Fprintf(&sqlSB, "RuntimeStatus = $%d, LastUpdatedTime = $%d, LockExpiration = NULL WHERE InstanceID = $%d AND LockedBy = $%d", currIndex, currIndex+1, currIndex+2, currIndex+3)
	sqlUpdateArgs = append(sqlUpdateArgs, helpers.ToRuntimeStatusString(wi.State.RuntimeStatus()), now, string(wi.InstanceID), wi.LockedBy)

	result, err := tx.Exec(ctx, sqlSB.String(), sqlUpdateArgs...)
	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	count := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get the number of rows affected by the Instance table update: %w", err)
	} else if count == 0 {
		return fmt.Errorf("instance '%s' no longer exists or was locked by a different worker", string(wi.InstanceID))
	}

	// If continue-as-new, delete all existing history
	if wi.State.ContinuedAsNew() {
		if _, err := tx.Exec(ctx, "DELETE FROM History WHERE InstanceID = $1", string(wi.InstanceID)); err != nil {
			return fmt.Errorf("failed to delete from History table: %w", err)
		}
	}

	// Save new history events
	newHistoryCount := len(wi.State.NewEvents())
	if newHistoryCount > 0 {
		builder := strings.Builder{}
		builder.WriteString("INSERT INTO History (InstanceID, SequenceNumber, EventPayload) VALUES ")
		for i := 0; i < newHistoryCount; i++ {
			fmt.Fprintf(&builder, "($%d, $%d, $%d)", 3*i+1, 3*i+2, 3*i+3)
			if i < newHistoryCount-1 {
				builder.WriteString(", ")
			}
		}
		query := builder.String()

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

		_, err = tx.Exec(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to insert into the History table: %w", err)
		}
	}

	// Save outbound activity tasks
	newActivityCount := len(wi.State.PendingTasks())
	if newActivityCount > 0 {
		builder := strings.Builder{}
		builder.WriteString("INSERT INTO NewTasks (InstanceID, EventPayload) VALUES ")
		for i := 0; i < newActivityCount; i++ {
			fmt.Fprintf(&builder, "($%d, $%d)", 2*i+1, 2*i+2)
			if i < newActivityCount-1 {
				builder.WriteString(", ")
			}
		}
		insertSql := builder.String()

		sqlInsertArgs := make([]any, 0, newActivityCount*2)
		for _, e := range wi.State.PendingTasks() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload)
		}

		_, err = tx.Exec(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewTasks table: %w", err)
		}
	}

	// Save outbound orchestrator events
	newEventCount := len(wi.State.PendingTimers()) + len(wi.State.PendingMessages())
	if newEventCount > 0 {
		builder := strings.Builder{}
		builder.WriteString("INSERT INTO NewEvents (InstanceID, EventPayload, VisibleTime) VALUES ")
		for i := 0; i < newEventCount; i++ {
			fmt.Fprintf(&builder, "($%d, $%d, $%d)", 3*i+1, 3*i+2, 3*i+3)
			if i < newEventCount-1 {
				builder.WriteString(", ")
			}
		}
		insertSql := builder.String()

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

		_, err = tx.Exec(ctx, insertSql, sqlInsertArgs...)
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
	dbResult, err := tx.Exec(
		ctx,
		"DELETE FROM NewEvents WHERE InstanceID = $1 AND LockedBy = $2",
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err != nil {
		return fmt.Errorf("failed to delete from the NewEvents table: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CreateOrchestrationInstance implements backend.Backend
func (be *postgresBackend) CreateOrchestrationInstance(ctx context.Context, e *backend.HistoryEvent, opts ...backend.OrchestrationIdReusePolicyOptions) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

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

	_, err = tx.Exec(
		ctx,
		`INSERT INTO NewEvents (InstanceID, EventPayload) VALUES ($1, $2)`,
		instanceID,
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into NewEvents table: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to create orchestration: %w", err)
	}

	return nil
}

func (be *postgresBackend) createOrchestrationInstanceInternal(ctx context.Context, e *backend.HistoryEvent, tx pgx.Tx, opts ...backend.OrchestrationIdReusePolicyOptions) (string, error) {
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

func insertOrIgnoreInstanceTableInternal(ctx context.Context, tx pgx.Tx, e *backend.HistoryEvent, startEvent *protos.ExecutionStartedEvent) (int64, error) {
	var parentInstanceID *string
	if pi := startEvent.GetParentInstance(); pi != nil {
		if instanceID := pi.GetOrchestrationInstance().GetInstanceId(); instanceID != "" {
			parentInstanceID = &instanceID
		}
	}
	res, err := tx.Exec(
		ctx,
		`INSERT INTO Instances (
			Name,
			Version,
			InstanceID,
			ExecutionID,
			Input,
			RuntimeStatus,
			CreatedTime,
			ParentInstanceID
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING`,
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
		return -1, fmt.Errorf("failed to insert into Instances table: %w", err)
	}

	rows := res.RowsAffected()
	if err != nil {
		return -1, fmt.Errorf("failed to count the rows affected: %w", err)
	}
	return rows, nil
}

func (be *postgresBackend) handleInstanceExists(ctx context.Context, tx pgx.Tx, startEvent *protos.ExecutionStartedEvent, policy *api.OrchestrationIdReusePolicy, e *backend.HistoryEvent) error {
	// query RuntimeStatus for the existing instance
	queryRow := tx.QueryRow(
		ctx,
		`SELECT RuntimeStatus FROM Instances WHERE InstanceID = $1`,
		startEvent.OrchestrationInstance.InstanceId,
	)
	var runtimeStatus *string
	err := queryRow.Scan(&runtimeStatus)
	if errors.Is(err, pgx.ErrNoRows) {
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
			return fmt.Errorf("failed to insert into Instances table because entry already exists")
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

func (be *postgresBackend) cleanupOrchestrationStateInternal(ctx context.Context, tx pgx.Tx, id api.InstanceID, requireCompleted bool) error {
	row := tx.QueryRow(ctx, "SELECT 1 FROM Instances WHERE InstanceID = $1", string(id))
	var unused int
	if err := row.Scan(&unused); errors.Is(err, pgx.ErrNoRows) {
		return api.ErrInstanceNotFound
	} else if err != nil {
		return fmt.Errorf("failed to scan instance existence: %w", err)
	}

	if requireCompleted {
		// purge orchestration in ['COMPLETED', 'FAILED', 'TERMINATED']
		dbResult, err := tx.Exec(ctx, "DELETE FROM Instances WHERE InstanceID = $1 AND RuntimeStatus IN ('COMPLETED', 'FAILED', 'TERMINATED')", string(id))
		if err != nil {
			return fmt.Errorf("failed to delete from the Instances table: %w", err)
		}

		rowsAffected := dbResult.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected in Instances delete operation: %w", err)
		}
		if rowsAffected == 0 {
			return api.ErrNotCompleted
		}
	} else {
		// clean up orchestration in all RuntimeStatus
		_, err := tx.Exec(ctx, "DELETE FROM Instances WHERE InstanceID = $1", string(id))
		if err != nil {
			return fmt.Errorf("failed to delete from the Instances table: %w", err)
		}
	}

	_, err := tx.Exec(ctx, "DELETE FROM History WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from History table: %w", err)
	}

	_, err = tx.Exec(ctx, "DELETE FROM NewEvents WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	_, err = tx.Exec(ctx, "DELETE FROM NewTasks WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}
	return nil
}

func (be *postgresBackend) AddNewOrchestrationEvent(ctx context.Context, iid api.InstanceID, e *backend.HistoryEvent) error {
	if e == nil {
		return backend.ErrNilHistoryEvent
	} else if e.Timestamp == nil {
		return backend.ErrNilEventTimestamp
	}

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	_, err = be.db.Exec(
		ctx,
		`INSERT INTO NewEvents (InstanceID, EventPayload) VALUES ($1, $2)`,
		string(iid),
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into NewEvents table: %w", err)
	}

	return nil
}

// GetOrchestrationMetadata implements backend.Backend
func (be *postgresBackend) GetOrchestrationMetadata(ctx context.Context, iid api.InstanceID) (*api.OrchestrationMetadata, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	row := be.db.QueryRow(
		ctx,
		`SELECT InstanceID, Name, Version, ParentInstanceID, RuntimeStatus, CreatedTime, LastUpdatedTime, Input, Output, CustomStatus, FailureDetails
		FROM Instances WHERE InstanceID = $1`,
		string(iid),
	)

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
	err := row.Scan(&instanceID, &name, &version, &parentInstanceID, &runtimeStatus, &createdAt, &lastUpdatedAt, &input, &output, &customStatus, &failureDetailsPayload)
	if errors.Is(err, pgx.ErrNoRows) {
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
func (be *postgresBackend) GetOrchestrationRuntimeState(ctx context.Context, wi *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	rows, err := be.db.Query(
		ctx,
		"SELECT EventPayload FROM History WHERE InstanceID = $1 ORDER BY SequenceNumber ASC",
		string(wi.InstanceID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
func (be *postgresBackend) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.OrchestrationLockTimeout)

	// Place a lock on an orchestration instance that has new events that are ready to be executed.
	row := tx.QueryRow(
		ctx,
		`UPDATE Instances SET LockedBy = $1, LockExpiration = $2
		WHERE SequenceNumber = (
			SELECT SequenceNumber FROM Instances I
			WHERE (I.LockExpiration IS NULL OR I.LockExpiration < $3) AND EXISTS (
				SELECT 1 FROM NewEvents E
				WHERE E.InstanceID = I.InstanceID AND (E.VisibleTime IS NULL OR E.VisibleTime < $4)
			)
			ORDER BY I.InstanceID, I.SequenceNumber ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		) RETURNING InstanceID`,
		be.workerName,     // LockedBy for Instances table
		newLockExpiration, // Updated LockExpiration for Instances table
		now,               // LockExpiration for Instances table
		now,               // VisibleTime for NewEvents table
	)

	var instanceID string
	if err := row.Scan(&instanceID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No new events to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the orchestration work-item: %w", err)
	}

	// TODO: Get all the unprocessed events associated with the locked instance
	events, err := tx.Query(
		ctx,
		`UPDATE NewEvents SET DequeueCount = DequeueCount + 1, LockedBy = $1 WHERE SequenceNumber IN (
			SELECT SequenceNumber FROM NewEvents
			WHERE InstanceID = $2 AND (VisibleTime IS NULL OR VisibleTime <= $3)
			LIMIT 1000
		)
		RETURNING EventPayload, DequeueCount, Timestamp`,
		be.workerName,
		instanceID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}
	defer events.Close()

	type rawEvent struct {
		payload    []byte
		dequeue    int32
		enqueuedAt time.Time
	}

	rawEvents := []rawEvent{}
	for events.Next() {
		var eventPayload []byte
		var dequeueCount int32
		var enqueuedAt time.Time
		if err := events.Scan(&eventPayload, &dequeueCount, &enqueuedAt); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}
		rawEvents = append(rawEvents, rawEvent{
			payload:    eventPayload,
			dequeue:    dequeueCount,
			enqueuedAt: enqueuedAt,
		})
	}
	events.Close()

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to update orchestration work-item: %w", err)
	}

	maxDequeueCount := int32(0)
	var enqueuedAt time.Time
	newEvents := make([]*protos.HistoryEvent, 0, len(rawEvents))
	for _, e := range rawEvents {
		if e.dequeue > maxDequeueCount {
			maxDequeueCount = e.dequeue
		}
		if enqueuedAt.IsZero() || e.enqueuedAt.Before(enqueuedAt) {
			enqueuedAt = e.enqueuedAt
		}

		evt, err := backend.UnmarshalHistoryEvent(e.payload)
		if err != nil {
			return nil, err
		}

		newEvents = append(newEvents, evt)
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

func (be *postgresBackend) GetActivityWorkItem(ctx context.Context) (*backend.ActivityWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.ActivityLockTimeout)

	row := be.db.QueryRow(
		ctx,
		`UPDATE NewTasks SET LockedBy = $1, LockExpiration = $2, DequeueCount = DequeueCount + 1
		WHERE SequenceNumber = (
			SELECT SequenceNumber FROM NewTasks T
			WHERE T.LockExpiration IS NULL OR T.LockExpiration < $3
			ORDER BY T.InstanceID, T.SequenceNumber ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		) RETURNING SequenceNumber, InstanceID, EventPayload, Timestamp, DequeueCount`,
		be.workerName,
		newLockExpiration,
		now,
	)

	var sequenceNumber int64
	var instanceID string
	var eventPayload []byte
	var enqueuedAt time.Time
	var dequeueCount int32

	if err := row.Scan(&sequenceNumber, &instanceID, &eventPayload, &enqueuedAt, &dequeueCount); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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

func (be *postgresBackend) GetOrchestrationBacklog(ctx context.Context) (backend.BacklogMetric, error) {
	if err := be.ensureDB(); err != nil {
		return backend.BacklogMetric{}, err
	}
	now := time.Now().UTC()
	row := be.db.QueryRow(
		ctx,
		`SELECT COUNT(DISTINCT E.InstanceID), MIN(E.Timestamp)
		FROM NewEvents E
		INNER JOIN Instances I ON I.InstanceID = E.InstanceID
		WHERE (I.LockExpiration IS NULL OR I.LockExpiration < $1)
			AND (E.VisibleTime IS NULL OR E.VisibleTime <= $1)`,
		now,
	)
	return scanPostgresBacklog(row, backend.WorkItemKindOrchestration, now)
}

func (be *postgresBackend) GetActivityBacklog(ctx context.Context) (backend.BacklogMetric, error) {
	if err := be.ensureDB(); err != nil {
		return backend.BacklogMetric{}, err
	}
	now := time.Now().UTC()
	row := be.db.QueryRow(
		ctx,
		`SELECT COUNT(*), MIN(Timestamp)
		FROM NewTasks
		WHERE LockExpiration IS NULL OR LockExpiration < $1`,
		now,
	)
	return scanPostgresBacklog(row, backend.WorkItemKindActivity, now)
}

func scanPostgresBacklog(row pgx.Row, kind backend.WorkItemKind, now time.Time) (backend.BacklogMetric, error) {
	metric := backend.BacklogMetric{Kind: kind}
	var oldest *time.Time
	if err := row.Scan(&metric.Depth, &oldest); err != nil {
		return backend.BacklogMetric{}, fmt.Errorf("failed to inspect %s backlog: %w", kind, err)
	}
	if oldest != nil && oldest.Before(now) {
		metric.OldestAge = now.Sub(*oldest)
	}
	return metric, nil
}

func (be *postgresBackend) CompleteActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	bytes, err := backend.MarshalHistoryEvent(wi.Result)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "INSERT INTO NewEvents (InstanceID, EventPayload) VALUES ($1, $2)", string(wi.InstanceID), bytes)
	if err != nil {
		return fmt.Errorf("failed to insert into NewEvents table: %w", err)
	}

	dbResult, err := tx.Exec(ctx, "DELETE FROM NewTasks WHERE SequenceNumber = $1 AND LockedBy = $2", wi.SequenceNumber, wi.LockedBy)
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (be *postgresBackend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	var lockExpiration *time.Time
	if delay := wi.GetAbandonDelay(); delay > 0 {
		visibleAt := time.Now().UTC().Add(delay)
		lockExpiration = &visibleAt
	}
	dbResult, err := be.db.Exec(
		ctx,
		"UPDATE NewTasks SET LockedBy = NULL, LockExpiration = $1 WHERE SequenceNumber = $2 AND LockedBy = $3",
		lockExpiration,
		wi.SequenceNumber,
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update the NewTasks table for abandon: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by update statement for abandon: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	return nil
}

func (be *postgresBackend) SignalEntity(ctx context.Context, request *protos.SignalEntityRequest) error {
	if err := be.ensureDB(); err != nil {
		return err
	}
	event, err := backend.NewEntitySignalEvent(request)
	if err != nil {
		return err
	}
	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if err := be.addEntityMessageTx(ctx, tx, request.InstanceId, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (be *postgresBackend) GetEntityWorkItem(ctx context.Context) (*backend.EntityWorkItem, error) {
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

type postgresEntityMessage struct {
	sequenceNumber int64
	event          *protos.HistoryEvent
	descriptor     backend.EntityMessageDescriptor
	dequeueCount   int32
	enqueuedAt     time.Time
}

func (be *postgresBackend) getEntityWorkItemOnce(ctx context.Context) (*backend.EntityWorkItem, bool, error) {
	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	now := time.Now().UTC()
	lockExpiration := now.Add(be.options.EntityLockTimeout)
	row := tx.QueryRow(
		ctx,
		`WITH candidate AS (
			SELECT E.InstanceID FROM Entities E
			WHERE (E.WorkItemLockExpiration IS NULL OR E.WorkItemLockExpiration < $1)
			AND EXISTS (
				SELECT 1 FROM EntityMessages M
				WHERE M.InstanceID = E.InstanceID
				AND (M.VisibleTime IS NULL OR M.VisibleTime <= $1)
				AND (
					E.LockedBy IS NULL
					OR (M.MessageKind = 'call' AND M.ParentInstanceID = E.LockedBy)
					OR (M.MessageKind = 'unlock' AND M.ParentInstanceID = E.LockedBy)
				)
			)
			ORDER BY (
				SELECT MIN(M2.SequenceNumber) FROM EntityMessages M2
				WHERE M2.InstanceID = E.InstanceID
				AND (M2.VisibleTime IS NULL OR M2.VisibleTime <= $1)
				AND (
					E.LockedBy IS NULL
					OR (M2.MessageKind = 'call' AND M2.ParentInstanceID = E.LockedBy)
					OR (M2.MessageKind = 'unlock' AND M2.ParentInstanceID = E.LockedBy)
				)
			)
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE Entities E SET WorkItemLockedBy = $2, WorkItemLockExpiration = $3
		FROM candidate C WHERE E.InstanceID = C.InstanceID
		RETURNING E.InstanceID, E.ExecutionID, E.State, E.LockedBy`,
		now,
		be.workerName,
		lockExpiration,
	)

	var instanceID string
	var executionID string
	var state *string
	var criticalSectionOwner *string
	if err := row.Scan(&instanceID, &executionID, &state, &criticalSectionOwner); errors.Is(err, pgx.ErrNoRows) {
		return nil, false, backend.ErrNoWorkItems
	} else if err != nil {
		return nil, false, fmt.Errorf("failed to lock entity work item: %w", err)
	}

	query := `SELECT SequenceNumber, EventPayload, DequeueCount, Timestamp
		FROM EntityMessages
		WHERE InstanceID = $1 AND (VisibleTime IS NULL OR VisibleTime <= $2)`
	args := []any{instanceID, now}
	nextArgument := 3
	if criticalSectionOwner != nil {
		query += fmt.Sprintf(` AND ((MessageKind = 'call' AND ParentInstanceID = $%d)
			OR (MessageKind = 'unlock' AND ParentInstanceID = $%d))`, nextArgument, nextArgument+1)
		args = append(args, *criticalSectionOwner, *criticalSectionOwner)
		nextArgument += 2
	}
	query += fmt.Sprintf(" ORDER BY SequenceNumber ASC LIMIT $%d", nextArgument)
	args = append(args, max(be.options.MaxEntityOperationBatchSize+1, 2))
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load entity messages: %w", err)
	}

	messages := make([]postgresEntityMessage, 0, be.options.MaxEntityOperationBatchSize+1)
	for rows.Next() {
		var raw postgresEntityMessage
		var payload []byte
		if err := rows.Scan(&raw.sequenceNumber, &payload, &raw.dequeueCount, &raw.enqueuedAt); err != nil {
			rows.Close()
			return nil, false, err
		}
		raw.event, err = backend.UnmarshalHistoryEvent(payload)
		if err != nil {
			rows.Close()
			return nil, false, err
		}
		raw.descriptor, err = backend.DescribeEntityMessage(raw.event)
		if err != nil {
			rows.Close()
			return nil, false, err
		}
		messages = append(messages, raw)
	}
	rows.Close()

	selected := make([]postgresEntityMessage, 0, be.options.MaxEntityOperationBatchSize)
	for _, message := range messages {
		switch message.descriptor.Kind {
		case "signal", "call":
			selected = append(selected, message)
		case "lock":
			if len(selected) == 0 {
				if err := be.processEntityLockTx(ctx, tx, instanceID, message); err != nil {
					return nil, false, err
				}
				if err := be.releaseEntityWorkLockTx(ctx, tx, instanceID); err != nil {
					return nil, false, err
				}
				if err := tx.Commit(ctx); err != nil {
					return nil, false, err
				}
				return nil, true, nil
			}
		case "unlock":
			if len(selected) == 0 {
				if _, err := tx.Exec(ctx, "DELETE FROM EntityMessages WHERE SequenceNumber = $1", message.sequenceNumber); err != nil {
					return nil, false, err
				}
				if criticalSectionOwner != nil && *criticalSectionOwner == message.descriptor.ParentInstanceID {
					if _, err := tx.Exec(ctx, "UPDATE Entities SET LockedBy = NULL WHERE InstanceID = $1", instanceID); err != nil {
						return nil, false, err
					}
				}
				if err := be.releaseEntityWorkLockTx(ctx, tx, instanceID); err != nil {
					return nil, false, err
				}
				if err := tx.Commit(ctx); err != nil {
					return nil, false, err
				}
				return nil, true, nil
			}
		}
		if len(selected) == be.options.MaxEntityOperationBatchSize ||
			message.descriptor.Kind == "lock" || message.descriptor.Kind == "unlock" {
			break
		}
	}
	if len(selected) == 0 {
		if err := be.releaseEntityWorkLockTx(ctx, tx, instanceID); err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return nil, false, backend.ErrNoWorkItems
	}

	entityID, err := api.EntityIDFromString(instanceID)
	if err != nil {
		return nil, false, err
	}
	workItem := &backend.EntityWorkItem{
		InstanceID:  entityID,
		ExecutionID: executionID,
		State:       state,
		LockedBy:    be.workerName,
		Operations:  make([]*protos.HistoryEvent, 0, len(selected)),
		MessageIDs:  make([]int64, 0, len(selected)),
	}
	for _, message := range selected {
		result, err := tx.Exec(
			ctx,
			`UPDATE EntityMessages SET LockedBy = $1, DequeueCount = DequeueCount + 1
			WHERE SequenceNumber = $2`,
			be.workerName,
			message.sequenceNumber,
		)
		if err != nil {
			return nil, false, err
		}
		if result.RowsAffected() != 1 {
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
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return workItem, false, nil
}

func (be *postgresBackend) processEntityLockTx(
	ctx context.Context,
	tx pgx.Tx,
	instanceID string,
	message postgresEntityMessage,
) error {
	lockRequest := message.event.GetEntityLockRequested()
	if lockRequest == nil || message.descriptor.ParentInstanceID == "" {
		return fmt.Errorf("invalid entity lock request")
	}
	if _, err := tx.Exec(
		ctx,
		"UPDATE Entities SET LockedBy = $1 WHERE InstanceID = $2",
		message.descriptor.ParentInstanceID,
		instanceID,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "DELETE FROM EntityMessages WHERE SequenceNumber = $1", message.sequenceNumber); err != nil {
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

func (be *postgresBackend) CompleteEntityWorkItem(ctx context.Context, wi *backend.EntityWorkItem) error {
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
	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var state any
	if wi.Result.EntityState != nil {
		state = wi.Result.EntityState.Value
	}
	result, err := tx.Exec(
		ctx,
		`UPDATE Entities SET State = $1, LastModifiedTime = $2, ExecutionID = $3,
			WorkItemLockedBy = NULL, WorkItemLockExpiration = NULL
		WHERE InstanceID = $4 AND WorkItemLockedBy = $5`,
		state,
		time.Now().UTC(),
		uuid.NewString(),
		wi.InstanceID.String(),
		wi.LockedBy,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return backend.ErrWorkItemLockLost
	}
	for index, messageID := range wi.MessageIDs {
		if index < len(wi.Result.Results) {
			result, err := tx.Exec(
				ctx,
				"DELETE FROM EntityMessages WHERE SequenceNumber = $1 AND LockedBy = $2",
				messageID,
				wi.LockedBy,
			)
			if err != nil {
				return err
			}
			if result.RowsAffected() != 1 {
				return backend.ErrWorkItemLockLost
			}
		} else if _, err := tx.Exec(
			ctx,
			"UPDATE EntityMessages SET LockedBy = NULL WHERE SequenceNumber = $1 AND LockedBy = $2",
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
	return tx.Commit(ctx)
}

func (be *postgresBackend) AbandonEntityWorkItem(ctx context.Context, wi *backend.EntityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}
	if wi == nil {
		return fmt.Errorf("entity work item is required")
	}
	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	for _, messageID := range wi.MessageIDs {
		if _, err := tx.Exec(
			ctx,
			"UPDATE EntityMessages SET LockedBy = NULL WHERE SequenceNumber = $1 AND LockedBy = $2",
			messageID,
			wi.LockedBy,
		); err != nil {
			return err
		}
	}
	result, err := tx.Exec(
		ctx,
		`UPDATE Entities SET WorkItemLockedBy = NULL, WorkItemLockExpiration = NULL
		WHERE InstanceID = $1 AND WorkItemLockedBy = $2`,
		wi.InstanceID.String(),
		wi.LockedBy,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return backend.ErrWorkItemLockLost
	}
	return tx.Commit(ctx)
}

func (be *postgresBackend) GetEntityMetadata(ctx context.Context, entityID api.EntityID, includeState bool) (*api.EntityMetadata, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	stateColumn := "NULL"
	if includeState {
		stateColumn = "E.State"
	}
	row := be.db.QueryRow(
		ctx,
		`SELECT E.LastModifiedTime, E.LockedBy, `+stateColumn+`,
			(SELECT COUNT(*) FROM EntityMessages M WHERE M.InstanceID = E.InstanceID)
		FROM Entities E WHERE E.InstanceID = $1`,
		entityID.String(),
	)
	var lastModified time.Time
	var lockedBy *string
	var state *string
	var backlog int32
	if err := row.Scan(&lastModified, &lockedBy, &state, &backlog); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	metadata := &api.EntityMetadata{
		InstanceID:       entityID,
		LastModifiedTime: lastModified,
		BacklogQueueSize: backlog,
	}
	if lockedBy != nil {
		metadata.LockedBy = *lockedBy
	}
	if state != nil {
		metadata.SerializedState = *state
	}
	return metadata, nil
}

func (be *postgresBackend) QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	pageSize = min(pageSize, 1000)
	var builder strings.Builder
	builder.WriteString(`SELECT E.InstanceID, E.LastModifiedTime, E.LockedBy, E.State,
		(SELECT COUNT(*) FROM EntityMessages M WHERE M.InstanceID = E.InstanceID)
		FROM Entities E WHERE E.InstanceID > $1`)
	args := []any{query.ContinuationToken}
	nextArgument := 2
	if query.InstanceIDStartsWith != "" {
		fmt.Fprintf(&builder, " AND E.InstanceID LIKE $%d", nextArgument)
		args = append(args, query.InstanceIDStartsWith+"%")
		nextArgument++
	}
	if !query.LastModifiedFrom.IsZero() {
		fmt.Fprintf(&builder, " AND E.LastModifiedTime >= $%d", nextArgument)
		args = append(args, query.LastModifiedFrom)
		nextArgument++
	}
	if !query.LastModifiedTo.IsZero() {
		fmt.Fprintf(&builder, " AND E.LastModifiedTime < $%d", nextArgument)
		args = append(args, query.LastModifiedTo)
		nextArgument++
	}
	if !query.IncludeTransient {
		builder.WriteString(" AND E.State IS NOT NULL")
	}
	fmt.Fprintf(&builder, " ORDER BY E.InstanceID ASC LIMIT $%d", nextArgument)
	args = append(args, pageSize+1)
	rows, err := be.db.Query(ctx, builder.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &api.EntityQueryResults{Entities: make([]*api.EntityMetadata, 0, pageSize)}
	for rows.Next() {
		var instanceID string
		var lastModified time.Time
		var lockedBy *string
		var state *string
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
		}
		if lockedBy != nil {
			metadata.LockedBy = *lockedBy
		}
		if query.IncludeState && state != nil {
			metadata.SerializedState = *state
		}
		result.Entities = append(result.Entities, metadata)
	}
	return result, rows.Err()
}

func (be *postgresBackend) CleanEntityStorage(ctx context.Context, request api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}
	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	rows, err := tx.Query(
		ctx,
		"SELECT InstanceID FROM Entities WHERE InstanceID > $1 ORDER BY InstanceID LIMIT 1001",
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
			dbResult, err := tx.Exec(
				ctx,
				`UPDATE Entities E SET LockedBy = NULL
				WHERE E.InstanceID = $1 AND E.LockedBy IS NOT NULL
				AND NOT EXISTS (
					SELECT 1 FROM Instances I WHERE I.InstanceID = E.LockedBy
					AND I.RuntimeStatus IN ('PENDING', 'RUNNING', 'SUSPENDED')
				)`,
				id,
			)
			if err != nil {
				return nil, err
			}
			result.OrphanedLocksReleased += int32(dbResult.RowsAffected())
		}
		if request.RemoveEmptyEntities {
			dbResult, err := tx.Exec(
				ctx,
				`DELETE FROM Entities E WHERE E.InstanceID = $1 AND E.State IS NULL
				AND E.LockedBy IS NULL AND E.WorkItemLockedBy IS NULL
				AND NOT EXISTS (SELECT 1 FROM EntityMessages M WHERE M.InstanceID = E.InstanceID)`,
				id,
			)
			if err != nil {
				return nil, err
			}
			result.EmptyEntitiesRemoved += int32(dbResult.RowsAffected())
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (be *postgresBackend) GetEntityBacklog(ctx context.Context) (backend.BacklogMetric, error) {
	if err := be.ensureDB(); err != nil {
		return backend.BacklogMetric{}, err
	}
	now := time.Now().UTC()
	row := be.db.QueryRow(
		ctx,
		`SELECT COUNT(DISTINCT InstanceID), MIN(Timestamp)
		FROM EntityMessages WHERE VisibleTime IS NULL OR VisibleTime <= $1`,
		now,
	)
	return scanPostgresBacklog(row, backend.WorkItemKindEntity, now)
}

func (be *postgresBackend) addEntityMessageTx(
	ctx context.Context,
	tx pgx.Tx,
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
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO Entities (InstanceID, ExecutionID, CreatedTime, LastModifiedTime)
		VALUES ($1, $2, $3, $4) ON CONFLICT (InstanceID) DO NOTHING`,
		entityID.String(),
		uuid.NewString(),
		now,
		now,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO EntityMessages
			(InstanceID, RequestID, MessageKind, ParentInstanceID, Timestamp, VisibleTime, EventPayload)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (InstanceID, MessageKind, RequestID) DO NOTHING`,
		entityID.String(),
		descriptor.RequestID,
		descriptor.Kind,
		nullableString(descriptor.ParentInstanceID),
		event.Timestamp.AsTime(),
		descriptor.VisibleTime,
		payload,
	); err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		"UPDATE Entities SET LastModifiedTime = $1 WHERE InstanceID = $2",
		now,
		entityID.String(),
	)
	return err
}

func (be *postgresBackend) insertOrchestrationEventTx(
	ctx context.Context,
	tx pgx.Tx,
	instanceID string,
	event *protos.HistoryEvent,
	visibleTime *time.Time,
) error {
	payload, err := backend.MarshalHistoryEvent(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		"INSERT INTO NewEvents (InstanceID, EventPayload, VisibleTime) VALUES ($1, $2, $3)",
		instanceID,
		payload,
		visibleTime,
	)
	return err
}

func (be *postgresBackend) releaseEntityWorkLockTx(ctx context.Context, tx pgx.Tx, instanceID string) error {
	result, err := tx.Exec(
		ctx,
		`UPDATE Entities SET WorkItemLockedBy = NULL, WorkItemLockExpiration = NULL
		WHERE InstanceID = $1 AND WorkItemLockedBy = $2`,
		instanceID,
		be.workerName,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return backend.ErrWorkItemLockLost
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (be *postgresBackend) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	if err := be.cleanupOrchestrationStateInternal(ctx, tx, id, true); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Start implements backend.Backend
func (be *postgresBackend) Start(ctx context.Context) error {
	if be.db == nil {
		pool, err := pgxpool.NewWithConfig(ctx, be.options.PgOptions)
		if err != nil {
			be.logger.Error("Start", "failed to create a new postgres pool", err)
			return fmt.Errorf("failed to create a new postgres pool %w", err)
		}
		be.db = pool
	}

	return nil
}

// Stop implements backend.Backend
func (be *postgresBackend) Stop(context.Context) error {
	if be.db != nil {
		be.db.Close()
		be.db = nil
	}

	return nil
}

func (be *postgresBackend) ensureDB() error {
	if be.db == nil {
		return backend.ErrNotInitialized
	}
	return nil
}

func (be *postgresBackend) String() string {
	maskedPassword := strings.Repeat("*", len(be.options.PgOptions.ConnConfig.Password))
	connectionURI := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", be.options.PgOptions.ConnConfig.User, maskedPassword, be.options.PgOptions.ConnConfig.Host, be.options.PgOptions.ConnConfig.Port, be.options.PgOptions.ConnConfig.Database)
	return connectionURI
}
