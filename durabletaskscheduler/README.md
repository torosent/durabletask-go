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

## Worker lifecycle

Use `Start` for background execution or `Run` for blocking execution. `Shutdown`
stops intake, allows in-flight execution and completion RPCs to drain, and
cancels them only if the shutdown context expires.

## Feature matrix

| Feature | Status |
| --- | --- |
| Schedule, query, and wait for orchestrations | Supported |
| Raise events, suspend/resume, terminate, and purge | Supported |
| Orchestration and activity execution | Supported |
| Bounded orchestration/activity concurrency | Supported |
| Completion tokens and abandon RPCs | Supported |
| Health pings, silent-disconnect detection, and channel recreation | Supported |
| Streamed orchestration history | Supported and advertised |
| Version-aware dispatch | Supported with `client.WithTaskExecutorOptions(task.WithVersioning(...))` |
| Scheduled-task capability | Not advertised |
| Large-payload capability | Not advertised |
| Durable entities | Not implemented; tokenized entity work is abandoned |
| DTS instance-ID replacement (`TERMINATE`) | Supported |
| Legacy instance-ID `IGNORE` | Known-compatible local sidecars only, with `client.WithLegacyOrchestrationIDReusePolicyWire`; rejected for DTS because the current wire format is ambiguous |

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
