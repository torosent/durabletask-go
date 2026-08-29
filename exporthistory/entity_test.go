package exporthistory

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// entityHarness drives the ExportJob entity through the production entity
// executor, so operation dispatch, state persistence, and the emitted signal and
// start-orchestration actions are exercised exactly as the worker runs them.
type entityHarness struct {
	t        *testing.T
	executor task.EntityExecutor
	jobID    string
	state    *wrapperspb.StringValue
	actions  []*protos.OperationAction
}

func newEntityHarness(t *testing.T, jobID string) *entityHarness {
	t.Helper()
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN(ExportJobEntityName, exportJobEntity))
	executor, ok := task.NewTaskExecutor(registry).(task.EntityExecutor)
	require.True(t, ok)
	return &entityHarness{t: t, executor: executor, jobID: jobID}
}

// call runs one operation and returns its serialized result. It fails the test
// when the operation reports a failure.
func (h *entityHarness) call(operation string, input any) string {
	h.t.Helper()
	result, err := h.tryCall(operation, input)
	require.NoError(h.t, err)
	return result
}

// tryCall runs one operation and returns the entity's failure as a Go error.
func (h *entityHarness) tryCall(operation string, input any) (string, error) {
	h.t.Helper()
	request := &protos.EntityBatchRequest{
		InstanceId:  EntityID(h.jobID).String(),
		EntityState: h.state,
		Operations: []*protos.OperationRequest{{
			Operation: operation,
			RequestId: operation + "-request",
		}},
	}
	if input != nil {
		payload, err := json.Marshal(input)
		require.NoError(h.t, err)
		request.Operations[0].Input = wrapperspb.String(string(payload))
	}
	result, err := h.executor.ExecuteEntity(context.Background(), request)
	require.NoError(h.t, err)
	require.Len(h.t, result.Results, 1)
	h.state = result.EntityState
	h.actions = result.Actions

	if failure := result.Results[0].GetFailure(); failure != nil {
		return "", entityFailure{details: failure.GetFailureDetails()}
	}
	return result.Results[0].GetSuccess().GetResult().GetValue(), nil
}

// entityFailure adapts a protobuf failure into an error so tests can assert on
// the error type the entity produced.
type entityFailure struct{ details *protos.TaskFailureDetails }

func (e entityFailure) Error() string {
	if e.details == nil {
		return "entity operation failed"
	}
	return e.details.GetErrorType() + ": " + e.details.GetErrorMessage()
}

// assertEntityErrorType asserts the durable error type the entity reported.
func assertEntityErrorType(t *testing.T, err error, expected api.ErrorType) {
	t.Helper()
	var failure entityFailure
	require.ErrorAs(t, err, &failure)
	assert.Equal(t, string(expected), failure.details.GetErrorType())
}

func (h *entityHarness) jobState() *ExportJobState {
	h.t.Helper()
	if h.state == nil {
		return nil
	}
	var state ExportJobState
	require.NoError(h.t, json.Unmarshal([]byte(h.state.GetValue()), &state))
	return &state
}

func (h *entityHarness) hasState() bool { return h.state != nil }

func (h *entityHarness) runToken() string {
	h.t.Helper()
	state := h.jobState()
	require.NotNil(h.t, state)
	return state.RunToken
}

func batchOptions(jobID string) JobCreationOptions {
	options, err := JobCreationOptions{
		JobID:             jobID,
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-24 * time.Hour),
		CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
		Destination:       &ExportDestination{Container: "test-container"},
	}.Normalize()
	if err != nil {
		panic(err)
	}
	return options
}

// TestEntityCreate ports the upstream ExportJobTests create scenarios.
func TestEntityCreate(t *testing.T) {
	t.Run("valid options activate the job and start the orchestration", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))

		state := harness.jobState()
		require.NotNil(t, state)
		assert.Equal(t, ExportJobStatusActive, state.Status)
		require.NotNil(t, state.Config)
		assert.Equal(t, ExportModeBatch, state.Config.Mode)
		assert.Equal(t, "test-container", state.Config.Destination.Container)
		require.NotNil(t, state.CreatedAt)
		require.NotNil(t, state.LastModifiedAt)
		assert.Empty(t, state.LastError)
		assert.Zero(t, state.ScannedInstances)
		assert.Zero(t, state.ExportedInstances)
		assert.Nil(t, state.Checkpoint)

		// Create signals Run rather than starting the orchestration directly, so
		// the start action only appears after the signal is delivered.
		require.Len(t, harness.actions, 1)
		signal := harness.actions[0].GetSendSignal()
		require.NotNil(t, signal)
		assert.Equal(t, runOperation, signal.GetName())
		assert.Equal(t, EntityID("job-1").String(), signal.GetInstanceId())
	})

	t.Run("missing options are rejected", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(createOperation, nil)
		require.Error(t, err)
		assertEntityErrorType(t, err, validationErrorType)
		assert.Contains(t, err.Error(), "creation options are required")
		assert.False(t, harness.hasState())
	})

	t.Run("mismatched job ID is rejected", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(createOperation, batchOptions("other-job"))
		require.Error(t, err)
		assertEntityErrorType(t, err, validationErrorType)
		assert.Contains(t, err.Error(), "does not match entity key")
	})

	t.Run("invalid options are rejected inside the entity", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(createOperation, JobCreationOptions{
			JobID:       "job-1",
			Mode:        ExportModeBatch,
			Destination: &ExportDestination{Container: "test-container"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CompletedTimeFrom is required")
	})

	t.Run("a destination is required", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(createOperation, JobCreationOptions{
			JobID:             "job-1",
			Mode:              ExportModeBatch,
			CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
			CompletedTimeTo:   time.Now().UTC().Add(-time.Minute),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "export destination is required")
	})

	t.Run("recreating an active job is rejected", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		_, err := harness.tryCall(createOperation, batchOptions("job-1"))
		require.Error(t, err)
		assertEntityErrorType(t, err, invalidTransitionErrorType)
		assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
	})

	t.Run("recreating a failed job resets progress and keeps CreatedAt", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  5,
			ExportedInstances: 5,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "cursor"},
			RunToken:          harness.runToken(),
		})
		harness.call(markAsFailedOperation, MarkAsFailedRequest{
			RunToken: harness.runToken(),
			Error:    "test error",
		})
		originalCreatedAt := *harness.jobState().CreatedAt

		harness.call(createOperation, batchOptions("job-1"))
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusActive, state.Status)
		assert.Zero(t, state.ScannedInstances)
		assert.Zero(t, state.ExportedInstances)
		assert.Nil(t, state.Checkpoint)
		assert.Nil(t, state.LastCheckpointTime)
		assert.Empty(t, state.LastError)
		require.NotNil(t, state.CreatedAt)
		assert.True(t, state.CreatedAt.Equal(originalCreatedAt))
	})

	t.Run("recreating a completed job is allowed", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: harness.runToken()})
		harness.call(createOperation, batchOptions("job-1"))
		assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
	})
}

func TestEntityGet(t *testing.T) {
	t.Run("returns the current state", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		result := harness.call(getOperation, nil)

		var state ExportJobState
		require.NoError(t, json.Unmarshal([]byte(result), &state))
		assert.Equal(t, ExportJobStatusActive, state.Status)
		require.NotNil(t, state.Config)
	})

	t.Run("does not resurrect a deleted entity", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(deleteOperation, nil)
		require.False(t, harness.hasState())

		result := harness.call(getOperation, nil)
		assert.Empty(t, result)
		assert.False(t, harness.hasState())
	})

	t.Run("reads emit no actions", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(getOperation, nil)
		assert.Empty(t, harness.actions)
	})
}

func TestEntityRun(t *testing.T) {
	t.Run("starts the orchestration with a deterministic instance ID", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(runOperation, RunJobRequest{RunToken: harness.runToken()})

		require.Len(t, harness.actions, 1)
		start := harness.actions[0].GetStartNewOrchestration()
		require.NotNil(t, start)
		assert.Equal(t, ExportJobOrchestratorName, start.GetName())
		assert.Equal(t, string(GetOrchestratorInstanceID("job-1")), start.GetInstanceId())
		// System orchestrations are started explicitly unversioned.
		assert.Equal(t, task.UnversionedTaskVersion, start.GetVersion().GetValue())

		var request ExportJobRunRequest
		require.NoError(t, json.Unmarshal([]byte(start.GetInput().GetValue()), &request))
		assert.Equal(t, EntityID("job-1"), request.JobEntityID)
		assert.Zero(t, request.ProcessedCycles)

		assert.Equal(t, string(GetOrchestratorInstanceID("job-1")), harness.jobState().OrchestratorInstanceID)
	})

	t.Run("rejects a job without configuration", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		_, err := harness.tryCall(runOperation, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "configuration is missing")
	})

	t.Run("rejects a job that is not active", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: harness.runToken()})
		_, err := harness.tryCall(runOperation, RunJobRequest{RunToken: harness.runToken()})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be in Active status to run")
	})
}

// TestEntityCommitCheckpoint covers durable checkpointing and the implicit
// failure a failed batch produces.
func TestEntityCommitCheckpoint(t *testing.T) {
	t.Run("advances the cursor and accumulates progress", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  100,
			ExportedInstances: 95,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "last-key"},
			RunToken:          harness.runToken(),
		})
		state := harness.jobState()
		assert.Equal(t, int64(100), state.ScannedInstances)
		assert.Equal(t, int64(95), state.ExportedInstances)
		require.NotNil(t, state.Checkpoint)
		assert.Equal(t, "last-key", state.Checkpoint.LastInstanceKey)
		require.NotNil(t, state.LastCheckpointTime)
		assert.Equal(t, ExportJobStatusActive, state.Status)

		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  10,
			ExportedInstances: 10,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "next-key"},
			RunToken:          harness.runToken(),
		})
		state = harness.jobState()
		assert.Equal(t, int64(110), state.ScannedInstances)
		assert.Equal(t, int64(105), state.ExportedInstances)
		assert.Equal(t, "next-key", state.Checkpoint.LastInstanceKey)
	})

	t.Run("a nil checkpoint keeps the cursor", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  1,
			ExportedInstances: 1,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "keep-me"},
			RunToken:          harness.runToken(),
		})
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{RunToken: harness.runToken()})
		state := harness.jobState()
		require.NotNil(t, state.Checkpoint)
		assert.Equal(t, "keep-me", state.Checkpoint.LastInstanceKey)
		assert.Equal(t, ExportJobStatusActive, state.Status)
	})

	t.Run("failures without a checkpoint implicitly fail the job", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			Failures: []ExportFailure{
				{InstanceID: "instance-1", Reason: "error1", AttemptCount: 1, LastAttempt: time.Now().UTC()},
				{InstanceID: "instance-2", Reason: "error2", AttemptCount: 2, LastAttempt: time.Now().UTC()},
			},
			RunToken: harness.runToken(),
		})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusFailed, state.Status)
		assert.Contains(t, state.LastError, "Batch export failed after retries")
		assert.Contains(t, state.LastError, "InstanceId: instance-1, Reason: error1")
		assert.Contains(t, state.LastError, "InstanceId: instance-2, Reason: error2")
	})

	t.Run("failures cannot be silently discarded by a checkpoint", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		before := harness.jobState()
		_, err := harness.tryCall(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances: 1,
			Checkpoint:       &ExportCheckpoint{LastInstanceKey: "cursor"},
			Failures:         []ExportFailure{{InstanceID: "i", Reason: "r"}},
			RunToken:         harness.runToken(),
		})
		require.ErrorContains(t, err, "checkpoint and failures cannot be committed together")
		after := harness.jobState()
		assert.Equal(t, before.Status, after.Status)
		assert.Equal(t, before.ScannedInstances, after.ScannedInstances)
		assert.Equal(t, before.ExportedInstances, after.ExportedInstances)
		assert.Equal(t, before.Checkpoint, after.Checkpoint)
	})

	t.Run("a checkpoint for a deleted job does not resurrect it", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(deleteOperation, nil)
		require.False(t, harness.hasState())

		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  5,
			ExportedInstances: 5,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "cursor"},
		})
		assert.False(t, harness.hasState(), "a deleted export job must stay deleted")
	})

	t.Run("negative progress is rejected", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		_, err := harness.tryCall(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances: -1,
			RunToken:         harness.runToken(),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not be negative")
	})

	t.Run("the persisted failure summary is bounded", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		failures := make([]ExportFailure, 0, 25)
		for i := 0; i < 25; i++ {
			failures = append(failures, ExportFailure{InstanceID: "instance", Reason: "boom"})
		}
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			Failures: failures,
			RunToken: harness.runToken(),
		})
		assert.Contains(t, harness.jobState().LastError, "and 15 more failures")
	})
}

func TestEntityMarkAsCompletedAndFailed(t *testing.T) {
	t.Run("active jobs complete", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: harness.runToken()})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusCompleted, state.Status)
		assert.Empty(t, state.LastError)
	})

	t.Run("completing a non-active job is an invalid transition", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsFailedOperation, MarkAsFailedRequest{
			RunToken: harness.runToken(),
			Error:    "test error",
		})
		_, err := harness.tryCall(
			markAsCompletedOperation,
			MarkAsCompletedRequest{RunToken: harness.runToken()},
		)
		require.Error(t, err)
		assertEntityErrorType(t, err, invalidTransitionErrorType)
	})

	t.Run("active jobs fail with a message", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsFailedOperation, MarkAsFailedRequest{
			RunToken: harness.runToken(),
			Error:    "Test error",
		})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusFailed, state.Status)
		assert.Equal(t, "Test error", state.LastError)
	})

	t.Run("failing without a message is allowed", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsFailedOperation, MarkAsFailedRequest{RunToken: harness.runToken()})
		assert.Equal(t, ExportJobStatusFailed, harness.jobState().Status)
	})

	t.Run("failing a non-active job is an invalid transition", func(t *testing.T) {
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: harness.runToken()})
		_, err := harness.tryCall(markAsFailedOperation, MarkAsFailedRequest{
			RunToken: harness.runToken(),
			Error:    "boom",
		})
		require.Error(t, err)
		assertEntityErrorType(t, err, invalidTransitionErrorType)
	})
}

// TestEntityRunGenerationFencing covers run fencing on the entity side: every
// orchestration-originated mutation carrying a stale generation token is dropped
// so a run left over from a deleted-and-recreated job cannot alter the new one.
func TestEntityRunGenerationFencing(t *testing.T) {
	// newGeneration deletes and recreates the job, returning the token of the
	// previous generation and of the new one.
	newGeneration := func(t *testing.T) (*entityHarness, string, string) {
		t.Helper()
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		stale := harness.jobState().RunToken
		require.NotEmpty(t, stale)

		harness.call(deleteOperation, nil)
		harness.call(createOperation, batchOptions("job-1"))
		current := harness.jobState().RunToken
		require.NotEmpty(t, current)
		require.NotEqual(t, stale, current,
			"a delete-and-recreate must mint a new run generation")
		return harness, stale, current
	}

	t.Run("a stale checkpoint is dropped", func(t *testing.T) {
		harness, stale, current := newGeneration(t)
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  5,
			ExportedInstances: 5,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "stale-cursor"},
			RunToken:          stale,
		})
		state := harness.jobState()
		assert.Zero(t, state.ScannedInstances)
		assert.Zero(t, state.ExportedInstances)
		assert.Nil(t, state.Checkpoint)
		assert.Equal(t, current, state.RunToken)

		// The current generation's checkpoint still applies.
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances:  2,
			ExportedInstances: 2,
			Checkpoint:        &ExportCheckpoint{LastInstanceKey: "fresh-cursor"},
			RunToken:          current,
		})
		state = harness.jobState()
		assert.Equal(t, int64(2), state.ScannedInstances)
		require.NotNil(t, state.Checkpoint)
		assert.Equal(t, "fresh-cursor", state.Checkpoint.LastInstanceKey)
	})

	t.Run("a stale implicit failure is dropped", func(t *testing.T) {
		harness, stale, _ := newGeneration(t)
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			Failures: []ExportFailure{{InstanceID: "i1", Reason: "boom"}},
			RunToken: stale,
		})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusActive, state.Status)
		assert.Empty(t, state.LastError)
	})

	t.Run("a stale completion is dropped", func(t *testing.T) {
		harness, stale, current := newGeneration(t)
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: stale})
		assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)

		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: current})
		assert.Equal(t, ExportJobStatusCompleted, harness.jobState().Status)
	})

	t.Run("a stale failure is dropped", func(t *testing.T) {
		harness, stale, current := newGeneration(t)
		harness.call(markAsFailedOperation, MarkAsFailedRequest{RunToken: stale, Error: "stale boom"})
		state := harness.jobState()
		assert.Equal(t, ExportJobStatusActive, state.Status)
		assert.Empty(t, state.LastError)

		harness.call(markAsFailedOperation, MarkAsFailedRequest{RunToken: current, Error: "fresh boom"})
		state = harness.jobState()
		assert.Equal(t, ExportJobStatusFailed, state.Status)
		assert.Equal(t, "fresh boom", state.LastError)
	})

	t.Run("a stale run signal starts no orchestration", func(t *testing.T) {
		harness, stale, current := newGeneration(t)
		harness.call(runOperation, RunJobRequest{RunToken: stale})
		assert.Empty(t, harness.actions, "a stale run signal must not start an orchestration")

		harness.call(runOperation, RunJobRequest{RunToken: current})
		require.Len(t, harness.actions, 1)
		require.NotNil(t, harness.actions[0].GetStartNewOrchestration())
	})

	t.Run("an untokenized caller is stale once fencing is active", func(t *testing.T) {
		harness, _, current := newGeneration(t)
		harness.call(commitCheckpointOperation, CommitCheckpointRequest{ScannedInstances: 1})
		assert.Zero(t, harness.jobState().ScannedInstances)
		harness.call(markAsCompletedOperation, nil)
		assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
		harness.call(markAsCompletedOperation, MarkAsCompletedRequest{RunToken: current})
		assert.Equal(t, ExportJobStatusCompleted, harness.jobState().Status)
	})

	t.Run("a job without a token cannot be fenced", func(t *testing.T) {
		// State written before run fencing existed has no token, so a tokenized
		// mutation still applies rather than being silently dropped.
		harness := newEntityHarness(t, "job-1")
		harness.call(createOperation, batchOptions("job-1"))
		state := harness.jobState()
		state.RunToken = ""
		payload, err := json.Marshal(state)
		require.NoError(t, err)
		harness.state = wrapperspb.String(string(payload))

		harness.call(commitCheckpointOperation, CommitCheckpointRequest{
			ScannedInstances: 3,
			RunToken:         "any-token",
		})
		assert.Equal(t, int64(3), harness.jobState().ScannedInstances)
	})
}

// TestEntityRunTokenTravelsToTheOrchestration keeps the generation the entity
// minted and the generation the orchestration runs under in sync.
func TestEntityRunTokenTravelsToTheOrchestration(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, batchOptions("job-1"))
	token := harness.jobState().RunToken
	require.NotEmpty(t, token)

	require.Len(t, harness.actions, 1)
	signal := harness.actions[0].GetSendSignal()
	require.NotNil(t, signal)
	var runRequest RunJobRequest
	require.NoError(t, json.Unmarshal([]byte(signal.GetInput().GetValue()), &runRequest))
	assert.Equal(t, token, runRequest.RunToken)

	harness.call(runOperation, runRequest)
	require.Len(t, harness.actions, 1)
	start := harness.actions[0].GetStartNewOrchestration()
	require.NotNil(t, start)
	var request ExportJobRunRequest
	require.NoError(t, json.Unmarshal([]byte(start.GetInput().GetValue()), &request))
	assert.Equal(t, token, request.RunToken)
	assert.False(t, request.ContinuedExecution)
}

// TestEntityCreationToleratesBoundedClockSkew pins the documented tolerance the
// entity applies to a batch window's upper bound. A client validates strictly
// against its own clock, so a worker running slightly behind must not reject a
// window that client accepted.
func TestEntityCreationToleratesBoundedClockSkew(t *testing.T) {
	windowEnd := time.Now().UTC().Add(MaxCreationClockSkew / 2)
	options := JobCreationOptions{
		JobID:             "job-1",
		Mode:              ExportModeBatch,
		CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
		CompletedTimeTo:   windowEnd,
		Destination:       &ExportDestination{Container: "test-container"},
	}

	// The client is strict: an upper bound ahead of its clock is rejected.
	require.ErrorIs(t, options.Validate(), ErrValidation)

	// The entity absorbs the skew so a job the client accepted on a slightly
	// faster clock still activates.
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, options)
	assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)

	t.Run("beyond the documented skew it is still rejected", func(t *testing.T) {
		beyond := options
		beyond.CompletedTimeTo = time.Now().UTC().Add(2 * MaxCreationClockSkew)
		rejected := newEntityHarness(t, "job-1")
		_, err := rejected.tryCall(createOperation, beyond)
		require.Error(t, err)
		assertEntityErrorType(t, err, validationErrorType)
		assert.Contains(t, err.Error(), "cannot be in the future")
	})
}

func TestEntityDelete(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call(createOperation, batchOptions("job-1"))
	require.True(t, harness.hasState())

	harness.call(deleteOperation, nil)
	assert.False(t, harness.hasState())

	// Deleting an already-deleted job is a no-op rather than an error.
	harness.call(deleteOperation, nil)
	assert.False(t, harness.hasState())

	// A deleted job can be created again from scratch.
	harness.call(createOperation, batchOptions("job-1"))
	assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
}

func TestEntityUnknownOperation(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	_, err := harness.tryCall("NotAnOperation", nil)
	require.Error(t, err)
	assertEntityErrorType(t, err, validationErrorType)
	assert.Contains(t, err.Error(), `does not support operation "NotAnOperation"`)
}

// TestEntityOperationNamesAreCaseInsensitive keeps the entity reachable from
// SDKs that normalize operation names differently.
func TestEntityOperationNamesAreCaseInsensitive(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.call("create", batchOptions("job-1"))
	assert.Equal(t, ExportJobStatusActive, harness.jobState().Status)
	harness.call("MARKASCOMPLETED", MarkAsCompletedRequest{RunToken: harness.runToken()})
	assert.Equal(t, ExportJobStatusCompleted, harness.jobState().Status)
	harness.call("DELETE", nil)
	assert.False(t, harness.hasState())
}

// TestEntityRejectsCorruptState surfaces unreadable state instead of silently
// starting a second export from a blank slate.
func TestEntityRejectsCorruptState(t *testing.T) {
	harness := newEntityHarness(t, "job-1")
	harness.state = wrapperspb.String("not json")
	_, err := harness.tryCall(getOperation, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to deserialize export job state")
}
