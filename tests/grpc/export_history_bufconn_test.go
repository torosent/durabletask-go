package tests_grpc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/exporthistory"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// memoryExportStore captures exported objects so the end-to-end test can assert
// on real gzip-compressed JSONL payloads.
type memoryExportStore struct {
	mu      sync.Mutex
	objects map[string]exporthistory.ExportObject
	// failInstances forces the export of specific instances to fail, which
	// drives the failure-collection path. A positive count fails only that many
	// attempts so the activity's own retry policy can recover.
	failInstances map[string]int
	attempts      map[string]int
}

func newMemoryExportStore() *memoryExportStore {
	return &memoryExportStore{
		objects:       map[string]exporthistory.ExportObject{},
		failInstances: map[string]int{},
		attempts:      map[string]int{},
	}
}

func (s *memoryExportStore) Write(_ context.Context, object exporthistory.ExportObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	instanceID := object.Metadata["instanceId"]
	s.attempts[instanceID]++
	if remaining, ok := s.failInstances[instanceID]; ok && (remaining < 0 || s.attempts[instanceID] <= remaining) {
		return assert.AnError
	}
	s.objects[object.Name] = object
	return nil
}

func (s *memoryExportStore) exported() map[string]exporthistory.ExportObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.objects)
}

// failInstance fails the next attempts writes for instanceID. A negative count
// fails every attempt.
func (s *memoryExportStore) failInstance(instanceID string, attempts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failInstances[instanceID] = attempts
}

func (s *memoryExportStore) attemptCount(instanceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[instanceID]
}

// exportHistoryHarness runs a full task hub over an in-memory gRPC transport,
// with the export history system tasks registered on a worker.
type exportHistoryHarness struct {
	client       *client.TaskHubGrpcClient
	exportClient *exporthistory.Client
	store        *memoryExportStore
}

func startExportHistoryHarness(
	t *testing.T,
	container string,
	workerOptions ...client.TaskHubGrpcWorkerOption,
) *exportHistoryHarness {
	t.Helper()
	testCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	server := grpc.NewServer()
	executor, register := backend.NewGrpcExecutor(be, logger)
	register(server)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	entityBackend, ok := backend.GetBackendCapability[backend.EntityBackend](be)
	require.True(t, ok)
	entityWorker := backend.NewEntityWorker(entityBackend, executor.(backend.EntityExecutor), logger)
	hubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger, entityWorker)
	require.NoError(t, hubWorker.Start(testCtx))

	connection := serveBufconn(t, server, "export-history-bufconn")
	grpcClient := client.NewTaskHubGrpcClient(
		connection, logger, client.WithLegacyOrchestrationIDReusePolicyWire())

	store := newMemoryExportStore()
	registry := task.NewTaskRegistry()
	// A user orchestration produces the terminal instances the export job reads.
	require.NoError(t, registry.AddOrchestratorN("ExportSubject", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var echoed string
		if err := ctx.CallActivity("ExportEcho", task.WithActivityInput(input)).Await(&echoed); err != nil {
			return nil, err
		}
		return echoed, nil
	}))
	require.NoError(t, registry.AddActivityN("ExportEcho", func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return "echo:" + input, nil
	}))
	require.NoError(t, exporthistory.Register(registry, exporthistory.WorkerOptions{
		Source: grpcClient,
		Store:  store,
	}))

	options := []client.TaskHubGrpcWorkerOption{
		client.WithMaxConcurrentOrchestrationWorkItems(4),
		client.WithMaxConcurrentActivityWorkItems(8),
		client.WithMaxConcurrentEntityWorkItems(4),
	}
	if len(workerOptions) == 0 {
		options = append(options, client.WithAutoWorkItemFilters(), exporthistory.WithExportHistory())
	} else {
		options = append(options, workerOptions...)
	}
	worker, err := client.NewTaskHubGrpcWorker(connection, registry, logger, options...)
	require.NoError(t, err)
	require.NoError(t, worker.Start(testCtx))

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer shutdownCancel()
		// An export activity issues management RPCs back to this sidecar, so a
		// shutdown that races an in-flight call can outlast its budget on a
		// saturated machine. Shutdown latency is not what these tests assert, so
		// it is logged rather than failed, matching the DTS harness.
		if err := worker.Shutdown(shutdownCtx); err != nil {
			t.Logf("failed to shut down the export history worker: %v", err)
		}
		if err := hubWorker.Shutdown(shutdownCtx); err != nil {
			t.Logf("failed to shut down the task hub worker: %v", err)
		}
		require.NoError(t, connection.Close())
		server.Stop()
	})

	exportClient, err := exporthistory.NewClient(grpcClient, exporthistory.ClientOptions{
		ContainerName: container,
	})
	require.NoError(t, err)
	return &exportHistoryHarness{client: grpcClient, exportClient: exportClient, store: store}
}

// runSubjects starts and drains count subject orchestrations, returning their
// terminal instance IDs.
func (h *exportHistoryHarness) runSubjects(t *testing.T, testCtx context.Context, count int) []api.InstanceID {
	t.Helper()
	ids := make([]api.InstanceID, 0, count)
	for i := 0; i < count; i++ {
		id, err := h.client.ScheduleNewOrchestration(testCtx, "ExportSubject", api.WithInput("subject"))
		require.NoError(t, err)
		metadata, err := h.client.WaitForOrchestrationCompletion(testCtx, id)
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, metadata.RuntimeStatus)
		ids = append(ids, id)
	}
	return ids
}

// Test_Bufconn_ExportHistoryBatchJobEndToEnd drives a batch export job through
// the real entity, orchestration, and activity plumbing and asserts on the
// exported objects.
func Test_Bufconn_ExportHistoryBatchJobEndToEnd(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startExportHistoryHarness(t, "history-exports")

	from := time.Now().UTC().Add(-time.Hour)
	subjects := harness.runSubjects(t, testCtx, 3)

	job, err := harness.exportClient.CreateJob(testCtx, exporthistory.JobCreationOptions{
		JobID:                "batch-job",
		Mode:                 exporthistory.ExportModeBatch,
		CompletedTimeFrom:    from,
		CompletedTimeTo:      time.Now().UTC(),
		MaxInstancesPerBatch: 2,
	})
	require.NoError(t, err)
	assert.Equal(t, "batch-job", job.ID())

	// The job runs to completion on its own.
	var description *exporthistory.ExportJobDescription
	require.Eventually(t, func() bool {
		description, err = job.Describe(testCtx)
		return err == nil && description.Status == exporthistory.ExportJobStatusCompleted
	}, 60*time.Second, 200*time.Millisecond, "export job did not complete")

	assert.GreaterOrEqual(t, description.ScannedInstances, int64(len(subjects)))
	assert.Equal(t, description.ScannedInstances, description.ExportedInstances)
	assert.Empty(t, description.LastError)
	require.NotNil(t, description.Config)
	assert.Equal(t, exporthistory.ExportModeBatch, description.Config.Mode)
	assert.Equal(t, "history-exports", description.Config.Destination.Container)
	assert.Equal(t, "batch-batch-job/", description.Config.Destination.Prefix)
	assert.Equal(t, 2, description.Config.MaxInstancesPerBatch)
	assert.Equal(t, string(exporthistory.GetOrchestratorInstanceID("batch-job")), description.OrchestratorInstanceID)
	require.NotNil(t, description.Checkpoint)

	objects := harness.store.exported()
	require.GreaterOrEqual(t, len(objects), len(subjects))
	exportedInstances := map[string]bool{}
	for name, object := range objects {
		assert.True(t, strings.HasPrefix(name, "batch-batch-job/"), name)
		assert.True(t, strings.HasSuffix(name, ".jsonl.gz"), name)
		assert.Equal(t, "history-exports", object.Container)
		// The object is an opaque gzip file: its name and content type agree and
		// no content coding invites a reader to decompress it transparently.
		assert.Equal(t, "application/gzip", object.ContentType)
		instanceID := object.Metadata["instanceId"]
		exportedInstances[instanceID] = true

		events := decodeExportedHistory(t, object.Content)
		require.NotEmpty(t, events)
		// The export must carry the real durable history, not a summary.
		var sawExecutionStarted, sawTaskScheduled bool
		for _, event := range events {
			switch event.Type {
			case api.HistoryEventExecutionStarted:
				sawExecutionStarted = true
			case api.HistoryEventTaskScheduled:
				sawTaskScheduled = true
			}
		}
		assert.True(t, sawExecutionStarted, "instance %s history is missing ExecutionStarted", instanceID)
		assert.True(t, sawTaskScheduled, "instance %s history is missing TaskScheduled", instanceID)
	}
	for _, id := range subjects {
		assert.True(t, exportedInstances[string(id)], "instance %s was not exported", id)
	}

	// The job is visible through Get and List.
	fetched, err := harness.exportClient.GetJob(testCtx, "batch-job")
	require.NoError(t, err)
	assert.Equal(t, exporthistory.ExportJobStatusCompleted, fetched.Status)

	listed, err := harness.exportClient.ListJobs(testCtx, exporthistory.ExportJobQuery{JobIDPrefix: "batch-"})
	require.NoError(t, err)
	require.Len(t, listed.Jobs, 1)
	assert.Equal(t, "batch-job", listed.Jobs[0].JobID)

	completed := exporthistory.ExportJobStatusCompleted
	filtered, err := harness.exportClient.ListJobs(testCtx, exporthistory.ExportJobQuery{Status: &completed})
	require.NoError(t, err)
	require.Len(t, filtered.Jobs, 1)

	active := exporthistory.ExportJobStatusActive
	empty, err := harness.exportClient.ListJobs(testCtx, exporthistory.ExportJobQuery{Status: &active})
	require.NoError(t, err)
	assert.Empty(t, empty.Jobs)
}

// Test_Bufconn_ExportHistoryJobLifecycle covers recreate rules, delete, and the
// typed errors clients observe.
func Test_Bufconn_ExportHistoryJobLifecycle(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startExportHistoryHarness(t, "history-exports")

	_, err := harness.exportClient.GetJob(testCtx, "missing-job")
	require.ErrorIs(t, err, exporthistory.ErrJobNotFound)

	options := exporthistory.JobCreationOptions{
		JobID:             "lifecycle-job",
		Mode:              exporthistory.ExportModeContinuous,
		CompletedTimeFrom: time.Now().UTC(),
	}
	job, err := harness.exportClient.CreateJob(testCtx, options)
	require.NoError(t, err)

	// A continuous job stays active because it never runs out of window.
	require.Eventually(t, func() bool {
		description, describeErr := job.Describe(testCtx)
		return describeErr == nil &&
			description.Status == exporthistory.ExportJobStatusActive &&
			description.OrchestratorInstanceID != ""
	}, 90*time.Second, 200*time.Millisecond)

	// Recreating a running job is rejected with the typed transition error.
	err = job.Create(testCtx, options)
	require.ErrorIs(t, err, exporthistory.ErrJobInvalidTransition)
	var transition *exporthistory.InvalidTransitionError
	require.ErrorAs(t, err, &transition)
	assert.Equal(t, exporthistory.ExportJobStatusActive, transition.From)
	assert.Equal(t, exporthistory.ExportJobStatusActive, transition.To)
	assert.Equal(t, "Create", transition.Operation)

	// Delete removes the entity and stops the orchestration.
	require.NoError(t, job.Delete(testCtx))
	require.Eventually(t, func() bool {
		_, describeErr := job.Describe(testCtx)
		return describeErr != nil
	}, 90*time.Second, 200*time.Millisecond)
	_, err = harness.exportClient.GetJob(testCtx, "lifecycle-job")
	require.ErrorIs(t, err, exporthistory.ErrJobNotFound)

	// The same ID can then be created again.
	require.NoError(t, job.Create(testCtx, options))
	description, err := job.Describe(testCtx)
	require.NoError(t, err)
	assert.Equal(t, exporthistory.ExportJobStatusActive, description.Status)
	require.NoError(t, job.Delete(testCtx))
}

// Test_Bufconn_ExportHistoryRecreatedJobRunsAgain covers recreate-in-place end
// to end: a completed batch job that is recreated without being deleted must
// start a second run that actually executes and reaches a terminal state again,
// rather than only flipping the entity back to Active.
func Test_Bufconn_ExportHistoryRecreatedJobRunsAgain(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startExportHistoryHarness(t, "history-exports")

	from := time.Now().UTC().Add(-time.Hour)
	subjects := harness.runSubjects(t, testCtx, 2)

	options := exporthistory.JobCreationOptions{
		JobID:             "recreate-job",
		Mode:              exporthistory.ExportModeBatch,
		CompletedTimeFrom: from,
		CompletedTimeTo:   time.Now().UTC(),
	}
	job, err := harness.exportClient.CreateJob(testCtx, options)
	require.NoError(t, err)

	first := awaitCompletedExportJob(t, testCtx, job, int64(len(subjects)))
	assert.Equal(t, first.ScannedInstances, first.ExportedInstances)

	// Recreate in place. The entity resets progress and mints a new run
	// generation, so the second run is observable as a fresh completion.
	options.CompletedTimeTo = time.Now().UTC()
	require.NoError(t, job.Create(testCtx, options))

	recreated, err := job.Describe(testCtx)
	require.NoError(t, err)
	assert.Equal(t, exporthistory.ExportJobStatusActive, recreated.Status)
	assert.Zero(t, recreated.ScannedInstances, "recreating a job resets its progress")
	assert.Nil(t, recreated.Checkpoint, "recreating a job resets its cursor")
	assert.Equal(t,
		string(exporthistory.GetOrchestratorInstanceID("recreate-job")),
		recreated.OrchestratorInstanceID)

	// The second run must genuinely execute: it re-scans the window and
	// completes again with fresh progress, which a job whose orchestration never
	// restarted could not do.
	second := awaitCompletedExportJob(t, testCtx, job, int64(len(subjects)))
	assert.Empty(t, second.LastError)
	assert.Equal(t, second.ScannedInstances, second.ExportedInstances)
	assert.True(t, second.LastModifiedAt.After(first.LastModifiedAt),
		"the recreated run must have made progress after the first one finished")
}

// awaitCompletedExportJob waits until the job reports Completed with at least
// minScanned instances scanned, which together prove a run executed rather than
// the entity merely reporting a terminal status.
func awaitCompletedExportJob(
	t *testing.T,
	testCtx context.Context,
	job *exporthistory.JobClient,
	minScanned int64,
) *exporthistory.ExportJobDescription {
	t.Helper()
	var description *exporthistory.ExportJobDescription
	require.Eventually(t, func() bool {
		current, err := job.Describe(testCtx)
		if err != nil {
			return false
		}
		description = current
		return current.Status == exporthistory.ExportJobStatusCompleted &&
			current.ScannedInstances >= minScanned
	}, 120*time.Second, 200*time.Millisecond,
		"export job did not complete a run that scanned at least %d instances", minScanned)
	return description
}

// Test_Bufconn_ExportHistoryActivityRetryRecovers covers the per-instance export
// retry policy: an instance whose first attempts fail is retried by the activity
// and the job still completes without recording a failure.
func Test_Bufconn_ExportHistoryActivityRetryRecovers(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startExportHistoryHarness(t, "history-exports")

	from := time.Now().UTC().Add(-time.Hour)
	subjects := harness.runSubjects(t, testCtx, 1)
	// The activity retry policy allows three attempts, so failing two of them
	// exercises recovery rather than the whole-page retry ladder.
	harness.store.failInstance(string(subjects[0]), 2)

	job, err := harness.exportClient.CreateJob(testCtx, exporthistory.JobCreationOptions{
		JobID:             "retrying-job",
		Mode:              exporthistory.ExportModeBatch,
		CompletedTimeFrom: from,
		CompletedTimeTo:   time.Now().UTC(),
	})
	require.NoError(t, err)

	var description *exporthistory.ExportJobDescription
	require.Eventually(t, func() bool {
		description, err = job.Describe(testCtx)
		return err == nil && description.Status == exporthistory.ExportJobStatusCompleted
	}, 120*time.Second, 500*time.Millisecond, "export job did not recover from transient export failures")

	assert.Empty(t, description.LastError)
	assert.Equal(t, description.ScannedInstances, description.ExportedInstances)
	assert.GreaterOrEqual(t, harness.store.attemptCount(string(subjects[0])), 3)
	assert.Contains(t, harness.store.exported(), exportedObjectFor(t, harness, string(subjects[0])))
}

// Test_Bufconn_ExportHistoryFailureHoldsTheCursor covers the durable checkpoint
// contract for a page that keeps failing: the job stays on the same page and
// never advances its cursor past a failing instance.
//
// The terminal transition to Failed happens only after the whole-page retry
// ladder elapses, which is minutes of durable timers; that end state is covered
// deterministically by the orchestration replay tests in the exporthistory
// package.
func Test_Bufconn_ExportHistoryFailureHoldsTheCursor(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startExportHistoryHarness(t, "history-exports")

	from := time.Now().UTC().Add(-time.Hour)
	subjects := harness.runSubjects(t, testCtx, 1)
	harness.store.failInstance(string(subjects[0]), -1)

	job, err := harness.exportClient.CreateJob(testCtx, exporthistory.JobCreationOptions{
		JobID:             "failing-job",
		Mode:              exporthistory.ExportModeBatch,
		CompletedTimeFrom: from,
		CompletedTimeTo:   time.Now().UTC(),
	})
	require.NoError(t, err)

	// Wait until the activity has exhausted its own retries for the instance.
	require.Eventually(t, func() bool {
		return harness.store.attemptCount(string(subjects[0])) >= 3
	}, 120*time.Second, 500*time.Millisecond, "the export activity was not retried")

	description, err := job.Describe(testCtx)
	require.NoError(t, err)
	assert.Nil(t, description.Checkpoint, "a failing page must not advance the durable cursor")
	assert.Zero(t, description.ExportedInstances)
	assert.NotEqual(t, exporthistory.ExportJobStatusCompleted, description.Status)

	// A job stuck on a failing page can still be deleted and recreated.
	require.NoError(t, job.Delete(testCtx))
	_, err = harness.exportClient.GetJob(testCtx, "failing-job")
	require.ErrorIs(t, err, exporthistory.ErrJobNotFound)
}

// Test_Bufconn_ExportHistoryStrictVersioningKeepsSystemTasksRoutable pins the
// interaction between application default versioning and the unversioned export
// history system tasks: with [exporthistory.WithExportHistory] the derived
// work-item filters keep advertising them unversioned, so the job runs; without
// it the strict worker version is advertised for them and no work is dispatched.
func Test_Bufconn_ExportHistoryStrictVersioningKeepsSystemTasksRoutable(t *testing.T) {
	strictVersioning := client.WithTaskVersioning(task.VersioningOptions{
		Version:         "2.0",
		MatchStrategy:   task.VersionMatchStrict,
		FailureStrategy: task.VersionFailureReject,
	})

	t.Run("routable with the export history worker option", func(t *testing.T) {
		testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		harness := startExportHistoryHarness(t, "history-exports",
			strictVersioning, client.WithAutoWorkItemFilters(), exporthistory.WithExportHistory())

		job, err := harness.exportClient.CreateJob(testCtx, exporthistory.JobCreationOptions{
			JobID:             "strict-job",
			Mode:              exporthistory.ExportModeBatch,
			CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
			CompletedTimeTo:   time.Now().UTC(),
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			description, describeErr := job.Describe(testCtx)
			return describeErr == nil && description.Status == exporthistory.ExportJobStatusCompleted
		}, 60*time.Second, 200*time.Millisecond, "export job did not complete under strict versioning")
	})

	t.Run("unroutable without it", func(t *testing.T) {
		harness := startExportHistoryHarness(t, "history-exports",
			strictVersioning, client.WithAutoWorkItemFilters())

		// The operation orchestrator is filtered out, so the create never
		// completes rather than failing fast.
		testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := harness.exportClient.CreateJob(testCtx, exporthistory.JobCreationOptions{
			JobID:             "strict-unroutable-job",
			Mode:              exporthistory.ExportModeBatch,
			CompletedTimeFrom: time.Now().UTC().Add(-time.Hour),
			CompletedTimeTo:   time.Now().UTC(),
		})
		require.Error(t, err)
		assert.True(t,
			errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded,
			"expected a deadline error, got %v", err)
	})
}

func decodeExportedHistory(t *testing.T, content []byte) []api.HistoryEvent {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(content))
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	decoded, err := io.ReadAll(reader)
	require.NoError(t, err)

	events := []api.HistoryEvent{}
	for _, line := range strings.Split(strings.TrimRight(string(decoded), "\n"), "\n") {
		if line == "" {
			continue
		}
		var event api.HistoryEvent
		require.NoError(t, json.Unmarshal([]byte(line), &event))
		events = append(events, event)
	}
	return events
}

// exportedObjectFor returns the object name the export job writes for
// instanceID, so a test can assert the object exists without duplicating the
// naming rule.
func exportedObjectFor(t *testing.T, harness *exportHistoryHarness, instanceID string) string {
	t.Helper()
	for name, object := range harness.store.exported() {
		if object.Metadata["instanceId"] == instanceID {
			return name
		}
	}
	t.Fatalf("no exported object for instance %s", instanceID)
	return ""
}
