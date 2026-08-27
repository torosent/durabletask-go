# Durable Task Scheduler transport

The `durabletaskscheduler` package connects Go orchestrations and activities to
Durable Task Scheduler (DTS). It is a transport/configuration package, not a
storage `backend.Backend`.

## Configuration

Use a connection string from the environment:

```bash
export DTS_CONNECTION_STRING='Endpoint=https://<scheduler-host>;TaskHub=<task-hub>;Authentication=DefaultAzure'
go run ./samples/durabletaskscheduler
```

For the local DTS emulator:

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/durabletaskscheduler
```

Connection strings support the Azure Identity modes that have Go equivalents:
`DefaultAzure`, `ManagedIdentity`, `WorkloadIdentity`, `Environment`,
`AzureCLI`, `AzurePowerShell`, and `InteractiveBrowser`. Authentication-specific
keys include `ClientID`, `TenantID`, `TokenFilePath`, and
`AdditionallyAllowedTenants`. Applications can instead call
`NewOptionsWithCredential` with any `azcore.TokenCredential`. Plaintext
`http://` endpoints are accepted only with `Authentication=None`.

`NewClient` creates and owns a management connection; call `Close` when done.
`NewWorker` creates a separate worker with its own connection factory. The
worker recreates channels after transient disconnects and closes retired
channels only after their in-flight completions have drained.

### Advanced management

`TaskHubGrpcClient` exposes bounded `QueryInstances` and `ListInstanceIDs`
operations with opaque continuation tokens, plus `RestartInstance`,
`RewindInstance`, batch/filter `PurgeInstances`,
`SkipGracefulOrchestrationTerminations`, and task-hub lifecycle RPCs. Queries
can filter locally by exact tag key/value pairs when the current wire contract
does not carry tag filters.

The embedded sqlite and Postgres sidecar implementations support these
operations directly. The current DTS emulator supports query, restart, and
batch purge, but has known service limitations: `SkipGracefulOrchestrationTerminations`
is unimplemented, rewind can return success without transitioning the failed
instance, filtered purge can complete without deleting matches, and
`ListInstanceIds` can omit matching IDs. The emulator integration tests record
these limitations explicitly.

Embedded gRPC hosts must opt in to destructive task-hub create/delete RPCs with
`backend.WithTaskHubLifecycleManagement()`. They remain disabled by default.

### Worker routing and capabilities

Use `client.WithWorkItemFilters` to advertise and locally enforce accepted
orchestration/activity names and versions. Local enforcement is a fallback for
services that ignore the filter request; the embedded sidecar routes work using
the same filters. An empty orchestration or activity filter list means no
restriction for that kind. Capability advertisement is explicit:
history streaming is enabled by default, while scheduled tasks use
`client.WithScheduledTaskCapability(true)`.

### Large payloads

Large payload support uses an opaque, integrity-checked reference encoded in
the existing string payload fields, so no cloud storage dependency is required.
Configure a shared store/resolver for both the management client and worker:

```go
store := payload.NewMemoryStore()
options.LargePayloads = &api.LargePayloadOptions{
    Store:            store,
    Resolver:         store,
    ThresholdBytes:   64 * 1024,
    MaxPayloadBytes:  64 * 1024 * 1024,
}
```

`payload.NewFileStore` provides a bounded local-file implementation. Workers
advertise `LARGE_PAYLOADS` only when `LargePayloads` is configured. The same
abstraction can be used with embedded backends through
`backend.NewLargePayloadBackend`. Resolver implementations must treat reference
locations as untrusted and enforce their own scheme/account/path allow lists.

## Worker lifecycle

Use `Start` for background execution or `Run` for blocking execution. `Shutdown`
stops intake, allows in-flight execution and completion RPCs to drain, and
cancels them only if the shutdown context expires.

## Feature matrix

| Feature | Status |
| --- | --- |
| Schedule, bounded query/list, and wait for orchestrations | Supported |
| Tags on schedule, metadata, query, sub-orchestration, continue-as-new, restart, and rewind | Supported; distinct from immutable context fields |
| Restart and batch/filter purge | Supported; see emulator limitations above |
| Rewind | Supported by embedded sqlite/Postgres; current emulator does not transition instances |
| Skip-graceful termination | Supported by embedded sqlite/Postgres; current emulator returns `Unimplemented` |
| Task-hub create/delete | Embedded sidecars require `backend.WithTaskHubLifecycleManagement`; remote-service behavior is provider-specific |
| Raise events, suspend/resume, terminate, and single-instance purge | Supported |
| Orchestration and activity execution | Supported |
| Bounded orchestration/activity/entity concurrency | Supported |
| Work-item filters for orchestrations, activities, and entities | Supported |
| Completion tokens and abandon RPCs | Supported |
| Health pings, silent-disconnect detection, and channel recreation | Supported |
| Streamed orchestration history | Supported and advertised |
| Version-aware dispatch | Supported with `client.WithTaskExecutorOptions(task.WithVersioning(...))` |
| Name/version work-item filters | Supported, advertised, and locally enforced |
| Scheduled-task capability | Supported; opt in with `client.WithScheduledTaskCapability(true)` |
| Large-payload capability | Supported and advertised only when a store/resolver is configured |
| Durable entities | Supported: legacy and V2 work items, scheduled signals, calls, queries, and critical sections |
| DTS instance-ID replacement (`TERMINATE`) | Supported |
| Legacy instance-ID `IGNORE` | Known-compatible local sidecars only, with `client.WithLegacyOrchestrationIDReusePolicyWire`; rejected for DTS because the current wire format is ambiguous |

The current V2 protobuf cannot carry per-operation trace context or request time
to an entity worker, and it has no properties map for legacy extended-session
state elision. V2 backends therefore send entity state on every work item; causal
trace metadata on entity-emitted actions is best-effort.

## Emulator tests

On Apple silicon with Apple Container, the current MCR emulator image runs
under Rosetta:

```bash
container image pull mcr.microsoft.com/dts/dts-emulator:latest
container run --detach --name dts-emulator \
  --arch amd64 --rosetta \
  --publish 8080:8080 --publish 8082:8082 \
  --env DTS_TASK_HUB_NAMES=default \
  mcr.microsoft.com/dts/dts-emulator:latest
```

The integration suite is environment-gated:

```bash
DTS_EMULATOR_ENDPOINT=http://127.0.0.1:8080 \
DTS_TASK_HUB=default \
go test ./tests/durabletaskscheduler -count=1
```
