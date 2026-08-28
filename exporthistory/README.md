# History export (preview)

`exporthistory` exports terminal orchestration histories out of a task hub and
into Azure Blob Storage. It is the Go counterpart of the .NET
`Microsoft.DurableTask.ExportHistory` preview package.

> **Maturity: preview.** The exported API, the serialized shape of the
> `ExportJob` entity state, and the names of the system entity, orchestrators,
> and activities may change in a future release without a major version bump.
> Drain in-flight export jobs before upgrading.

## Model

An export job is a durable entity keyed by job ID.

| Component | Name | Role |
| --- | --- | --- |
| Entity | `ExportJob` | Owns lifecycle, configuration, checkpoint, and progress |
| Orchestrator | `ExecuteExportJobOperationOrchestrator` | Runs one entity operation on behalf of a client |
| Orchestrator | `ExportJobOrchestrator` | Lists, exports, and checkpoints; one per job |
| Activity | `ListTerminalInstancesActivity` | Returns one page of terminal instance IDs |
| Activity | `ExportInstanceHistoryActivity` | Exports one instance's history |

Creating a job moves the entity to `Active` and signals it to start
`ExportJobOrchestrator` with the deterministic instance ID
`ExportJob-<jobId>`, so a job never runs two orchestrations at once. The
orchestration repeatedly lists terminal instances matching the job's filter,
exports each instance's history in bounded parallel windows, and commits a
checkpoint back to the entity. A batch job completes when the task hub reports
no more pages; a continuous job idles for a minute and lists again.

### Lifecycle

| Operation | From | To |
| --- | --- | --- |
| `Create` | `Pending`, `Failed`, `Completed` | `Active` |
| `MarkAsCompleted` | `Active` | `Completed` |
| `MarkAsFailed` | `Active` | `Failed` |

Every other transition raises an `InvalidTransitionError`, including recreating
a job that is already `Active`. Recreating a `Failed` or `Completed` job resets
its progress counters and checkpoint but preserves its original creation time.
Because a job reuses its deterministic orchestration instance ID, `Create`
terminates and purges the previous run's orchestration before recreating a
terminal job; a task hub that refuses to start an instance ID it already knows
would otherwise silently drop the new run.

A page whose exports keep failing commits without a checkpoint and with the
collected failures, which implicitly moves the job to `Failed` and leaves the
cursor on the failing page so a fix can resume from it. That implicit failure
goes through the same lifecycle transition as an explicit `MarkAsFailed`.

`Delete` clears the entity state and then terminates and purges the job's
orchestration. The two steps are not atomic, so a checkpoint racing the delete
is dropped rather than resurrecting the job.

### Run fencing

Each `Create` mints a random run token that is stored on the job and carried by
the run it starts. Every orchestration-originated mutation — `CommitCheckpoint`,
`MarkAsCompleted`, `MarkAsFailed`, and the `Run` signal — carries that token, and
the entity drops any that names a different generation. A run left over from a
job that was deleted and recreated, or from a prior in-place recreate, therefore
cannot checkpoint, complete, or fail the new job. The run also stops itself as
soon as it reads a job whose token no longer matches.

An operation that carries no token is stale once the stored job has a token. A
job whose stored state predates run fencing has no token and continues accepting
both tokenized and untokenized operations, so legacy jobs keep working. The token
is omitted entirely when that legacy state is serialized.

## Output layout

Each exported instance becomes one object named
`<prefix><sha256(completedAt|instanceId)>.<extension>`:

| Format | Extension | Content type | Content-Encoding |
| --- | --- | --- | --- |
| `ExportFormatJSONL` (default) | `jsonl.gz` | `application/gzip` | not set |
| `ExportFormatJSON` | `json` | `application/json` | not set |

A JSONL object is gzip-compressed and stored as an opaque gzip file: the name and
content type agree and no content coding is declared, so every reader downloads
exactly the gzip stream the name promises. Declaring the compression as
`Content-Encoding: gzip` instead would let some clients transparently decompress
the download while others would not, leaving a reader unable to tell what it
received.

JSONL objects carry one `api.HistoryEvent` per line. Every object carries
`instanceId` and `schemaVersion` metadata, plus `executionId` when the task hub
returns one. The name is derived deterministically, so re-exporting an instance
overwrites its object instead of duplicating it.

When no destination is supplied, a job writes to the client's configured
container under the prefix `<mode>-<jobId>/`.

## Worker setup

```go
store, err := exporthistory.NewAzureBlobStore(exporthistory.AzureBlobStoreOptions{
    ConnectionString: storageConnectionString,
    ContainerName:    "history-exports",
})
err = exporthistory.Register(registry, exporthistory.WorkerOptions{
    Source: taskHubClient,
    Store:  store,
})
worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
    durabletaskclient.WithAutoWorkItemFilters(),
    exporthistory.WithExportHistory(),
)
```

`Source` supplies the three management reads the export performs:
`ListInstanceIDs`, `FetchOrchestrationMetadata`, and `GetOrchestrationHistory`.
`*client.TaskHubGrpcClient` and the Durable Task Scheduler client satisfy it.

`Store` is a narrow interface with a single `Write` method. `AzureBlobStore` is
the production implementation; supply your own to export elsewhere. It is
deliberately separate from `payload.AzureBlobStore`, whose large-payload
contract assigns random object names inside a single container. Its endpoint
validation is at least as strict as that store's: an `AccountURL` carrying
userinfo, a query string, or a fragment is rejected outright, and plaintext HTTP
is confined to loopback endpoints behind `AllowInsecureHTTP`.

### Versioning

Every system task is registered unversioned so it stays reachable when an
application enables default versioning. Because a strict worker advertises its
own version for unversioned registrations, `WithExportHistory()` allow-lists
both the system orchestrators and their activities, so the derived work-item
filters keep advertising them unversioned. Without it, a strict-version worker
constructs successfully but the service never dispatches export work to it.

## Client

```go
exportClient, err := exporthistory.NewClient(taskHubClient, exporthistory.ClientOptions{
    ContainerName: "history-exports",
})

job, err := exportClient.CreateJob(ctx, exporthistory.JobCreationOptions{
    Mode:                 exporthistory.ExportModeBatch,
    CompletedTimeFrom:    from,
    CompletedTimeTo:      to,
    MaxInstancesPerBatch: 200,
})
description, err := job.Describe(ctx)
page, err := exportClient.ListJobs(ctx, exporthistory.ExportJobQuery{JobIDPrefix: "nightly-"})
err = job.Delete(ctx)
```

### Validation

`JobCreationOptions.Normalize` applies the same rules as .NET:

- Batch mode requires `CompletedTimeFrom` and `CompletedTimeTo`, requires
  `CompletedTimeTo` to be strictly greater than `CompletedTimeFrom`, and rejects
  an upper bound in the future.
- Continuous mode rejects `CompletedTimeTo` and defaults `CompletedTimeFrom` to
  now.
- `MaxInstancesPerBatch` must be between 1 and 1000; it defaults to 100.
- `RuntimeStatus` accepts only `COMPLETED`, `FAILED`, and `TERMINATED`; an empty
  filter selects all three.
- A missing job ID is generated as a 32-character GUID.

The `ExportJob` entity re-validates creation options on the worker, whose clock
is independent of the client's. Everything is checked identically except the
"upper bound is not in the future" rule, which the entity relaxes by
`MaxCreationClockSkew` (5 minutes) so a worker running slightly behind does not
reject a window the client accepted. Clients stay strict, and the tolerance never
shifts the window a continuous job starts from.

### Errors

| Error | Sentinel | Raised when |
| --- | --- | --- |
| `ValidationError` | `ErrValidation` | Invalid options, destination, or client configuration |
| `NotFoundError` | `ErrJobNotFound` | Reading a job that does not exist |
| `InvalidTransitionError` | `ErrJobInvalidTransition` | An operation the lifecycle does not allow |
| `OperationError` | `ErrJobOperationFailed` | An operation orchestration failed for another reason |

Errors raised inside the entity carry stable cross-language error types, so a
client reconstructs the typed error across the orchestration boundary and
`errors.Is`/`errors.As` keep working.

## Service behavior and limitations

- **Extended sessions are not supported.**
- **Pagination.** A task hub signals the end of the stream either by omitting the
  continuation token (embedded sqlite and Postgres) or by returning an empty page
  with a token (Durable Task Scheduler). Both are handled. A backend that omits
  the token on a non-empty final page makes a continuous job re-scan that final
  page on each idle cycle; deterministic blob names make the writes idempotent,
  but scanned and exported counters include the re-scan.
- **List visibility lag.** The service's instance-ID index can lag orchestration
  completion. A batch job whose first page is empty legitimately completes with
  nothing exported, so schedule a job after the window's instances are listable.
- **Retry semantics.** A per-instance export is attempted up to three times
  (retry delays 15s, then 30s) for transient failures such as a storage write
  error. Conditions retrying cannot fix, such as a missing or non-terminal
  instance, are collected immediately. A page whose instances still fail is
  attempted three times in total, waiting 1 minute before the second attempt and
  2 minutes before the third; the third attempt fails the page instead of
  waiting again.
- **Delete is not atomic** with terminating and purging the job's orchestration.

## Tests

Unit and orchestration-replay tests run with no external services. Live tests
skip unless their environment is configured:

```bash
# Azure Blob write path against Azurite
AZURITE_CONNECTION_STRING="..." go test ./exporthistory/

# End-to-end over the in-memory gRPC transport
go test ./tests/grpc/ -run Test_Bufconn_ExportHistory

# End-to-end against a live Durable Task Scheduler plus Azurite
DTS_EMULATOR_ENDPOINT="http://127.0.0.1:8080" DTS_TASK_HUB=default \
  AZURITE_CONNECTION_STRING="..." \
  go test ./tests/durabletaskscheduler/ -run TestDTSExportHistory
```
