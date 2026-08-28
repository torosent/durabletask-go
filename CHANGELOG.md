# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Added the top-level `durabletaskscheduler` transport package, a dedicated resilient gRPC worker, DTS emulator tests, and an environment-driven sample.
- Added advanced management APIs for bounded instance queries/listing, restart, rewind, batch/filter purge polling, immediate termination, and task-hub lifecycle operations.
- Added optional sqlite/Postgres backend capabilities and embedded gRPC implementations for advanced management operations, stable continuation tokens, indexed tag queries, and safe rewind history rebuilding.
- Added orchestration tags to scheduling, metadata, queries, sub-orchestrations, continue-as-new, restart, and rewind.
- Added explicit worker capability advertisement and orchestration/activity name/version filters with local fallback enforcement.
- Added pluggable large-payload store/resolver support with size limits, SHA-256 integrity validation, memory/file implementations, and opt-in DTS capability advertisement.
- Added API-owned structured failure details, typed task and entity operation errors, stable cross-language error types, bounded panic stacks, nested causes, and custom error-property enrichment.
- Added centralized request/response gRPC error mapping that preserves both `errors.Is` categories and original gRPC status codes.
- Added version-keyed orchestrator and activity registrations, exact-match dispatch with controlled unversioned fallback, client/worker default versions, activity inheritance, registry-derived filters, and ContinueAsNew version migration.
- Added API-owned pluggable data conversion across orchestration, activity, entity, event, status, management, metadata, and ContinueAsNew payloads, with JSON as the compatibility default and raw API bypasses.
- Added API-owned orchestration history retrieval for embedded and gRPC/DTS management clients, including bounded callback streaming, execution selection, native entity and rewind events, typed payload readers, converter integration, and large-payload hydration.
- Added Azure Blob large-payload storage with production Azure SDK configuration, gzip support, bounded and validated reads, legacy `blob:v1` reads, and interoperable self-describing .NET `blob:v2` tokens.
- Added DTS recurring interval schedules with create/get/list and per-schedule describe, update, pause, resume, and delete operations, durable execution-token state, worker registration/capability helpers, versions, tags, context, retries, and converter-aware inputs.
- Added deterministic long-timer splitting with a configurable three-day default across embedded, generic gRPC, and DTS workers. Histories written before splitting that contain one long timer remain replay-compatible after that timer reaches its logical deadline; changing the configured split interval between releases can still break affected in-flight orchestrations.
- Added failure-threshold-based gRPC channel recreation for the owned DTS management client, with long-poll deadline exemptions and in-flight protection.
- Expanded scheduled-task parity across boundary times, missed intervals, lifecycle transitions, stale tokens, no-op updates, typed client errors, and arbitrary text inputs.
- Hardened Azure Blob container initialization and deletion recovery under concurrency, and added bounded concurrent payload transformation with deterministic failure ordering.
- Added shared distributed-tracing tree assertions (`tests/tracingtree`) and end-to-end span-tree coverage over the gRPC and live Durable Task Scheduler emulator surfaces for activity failure, sub-orchestration success and failure, orchestration-sent events, durable timers, client-raised events, version migration via ContinueAsNew, and scheduled tasks.
- Expanded the `orchestratorgo` analyzer behind `cmd/orchestratorvet` from raw-goroutine detection into whole-package replay-hazard analysis of the orchestrators a package registers with `task.TaskRegistry`. It now reports wall-clock reads and host timers, nondeterministic UUID and random sources, unsafe parallelism and synchronization, direct filesystem/network/process/environment I/O, replay-unsafe logging, provably non-progressing unbounded loops, activity and sub-orchestration names a complete registration set proves are missing, and invalid or unstable registration forms. Analysis follows the call graph through same-package named helpers, methods, resolvable function variables, and nested literals, and reports nothing outside registered orchestrator reachability.
- Added `analysis.SuggestedFix` rewrites from `time.Now()` to `ctx.CurrentTimeUtc`, removing the `time` import when the rewritten call was its only use, and from an immediately invoked `go func() { ... }()` to `ctx.Go`, preserving the body and its comments verbatim. Both are applicable with `gopls` or `go vet -fix`.
- Added `cmd/orchestratorvet/README.md` documenting every analyzer check, its suggested fixes, its false-positive guardrails, and its limitations.
- Added benchmarks for analyzer scaling and allocations across orchestrator count and call-graph depth, for `task.TaskRegistry` orchestrator, activity, versioned, snapshot, and concurrent lookups, and for client work-item filter construction, wire conversion, validation, and per-work-item matching.
- Added the preview top-level `exporthistory` package, which exports terminal orchestration histories to Azure Blob Storage as gzip-compressed JSONL. It provides a public client for creating, getting, listing, and per-job create/describe/delete operations; batch and continuous jobs; the `ExportJob` durable entity with `Pending`/`Active`/`Failed`/`Completed` lifecycle transitions; durable checkpoints; terminal-status filters; a per-batch instance limit; per-instance failure collection with implicit job failure when a checkpoint batch fails; delete-and-recreate rules; and typed validation, not-found, invalid-transition, and operation errors carrying stable cross-language error types. Each `Create` mints a run-generation token that fences every orchestration-originated mutation, so a run left over from a deleted or recreated job cannot checkpoint, complete, or fail the new one, and recreating a terminal job clears the previous run's orchestration so the new run actually starts. Exported objects are stored as opaque gzip files, named `.jsonl.gz` with content type `application/gzip` and no `Content-Encoding`, so a reader always receives exactly the bytes the object name promises. The system entity, orchestrators, and activities are registered unversioned, and `exporthistory.WithExportHistory()` keeps them routable under strict worker versioning. Destination writes go through a narrow `exporthistory.Store` interface with a production `AzureBlobStore` implementation that validates containers and prefixes, rejects account URLs carrying userinfo, a query string, or a fragment, and creates each container once. The package is preview: its API, the serialized `ExportJob` state, and its system task names may change without a major version bump, and extended sessions remain unsupported.
- Added `client.WithUnversionedActivityNames` and `task.WithUnversionedActivityNames`, the activity counterparts of the existing unversioned-orchestrator allow-lists. An activity inherits its caller's version, so a system component whose orchestration runs unversioned schedules unversioned activities that a strict-version worker would otherwise refuse to accept and would advertise under its own version.

### Changed

- Removed the `submodules/durabletask-protobuf` git submodule. The `orchestrator_service.proto` file is now vendored under `vendored/durabletask-protobuf/protos/`, with the source provenance (URL, branch/ref, commit hash) tracked in `vendored/durabletask-protobuf/PROTO_SOURCE_COMMIT_HASH` and a helper script (`vendored/durabletask-protobuf/update-proto.sh`) for refreshing the proto from upstream.
- Refreshed the vendored Durable Task protobuf contract. `api.OrchestrationIdReusePolicy` and `api.CreateOrchestrationAction` are now API-owned types instead of aliases for generated protobuf types; callers should construct policies with the `api` types.
- `TaskHubGrpcClient` now rejects the ambiguous legacy `IGNORE` reuse action by default. Known-compatible local sidecars can opt in with `client.WithLegacyOrchestrationIDReusePolicyWire`; DTS callers must use the representable `ERROR` or `TERMINATE` semantics.
- The standalone gRPC sidecar enables current-proto `replaceableStatus` semantics. Embedded hosts must opt in with `backend.WithCurrentOrchestrationIDReusePolicyWire`; without it, field-1-only policies fail closed because legacy `ERROR` and current `TERMINATE` requests are wire-identical.
- Orchestration metadata now includes execution ID, completion/scheduled timestamps, parent ID, failure details, and tags when provided by the backend or service.
- Replaced protobuf failure details in public metadata with `api.FailureDetails`. `RetryPolicy.Handle` now receives deterministic `task.RetryContext`, and non-retriable or canceled failures bypass the handler.
- `FetchEntityMetadata` now returns `api.ErrInstanceNotFound` when the entity does not exist.
- Large-payload externalization now uses an inclusive threshold boundary to match the .NET Azure Blob payload extension.
- Same-name live external-event waiters now use the Durable Task .NET LIFO replay contract, while events buffered before a waiter remain FIFO. This replay-contract change can reassign events for in-flight orchestrations that already have multiple concurrent same-name waits, so drain those instances before upgrading.
- `durabletaskscheduler.Options.Validate` now rejects `ClientID`, `TenantID`, `TokenFilePath`, and `AdditionallyAllowedTenants` when `Authentication` is `None` or `TokenCredential`, because neither mode constructs an Azure Identity credential and the values can never be used. It also rejects a blank or newline-bearing `ResourceID`.
- `durabletaskscheduler.NewOptionsFromConnectionString` now rejects `Authentication=TokenCredential` with guidance to use `NewOptionsWithCredential`, instead of reporting it as a generic unsupported value.
- Documented that `client.NewTaskHubGrpcWorker` and `TaskHubGrpcClient.StartWorkItemListener` borrow the caller's connection and never replace or close it, so recovery from a permanently wedged channel requires `client.NewTaskHubGrpcWorkerWithConnectionFactory` or `durabletaskscheduler.NewWorker`.

### Fixed

- gRPC worker reconnect and transient RPC retry delays are now deterministic and always within the configured `[baseDelay, maxDelay]` bounds. The previous randomized exponential backoff could wait up to 50% less than the configured base delay and up to 50% longer than the configured maximum, and doubling now saturates instead of overflowing to a non-positive duration for very large configured maximums, which would have produced an unthrottled reconnect loop.
- A work item that the service delivers concurrently with the gRPC worker's silent-disconnect timeout is now processed instead of dropped. Previously the racing message was discarded without being abandoned, so the service considered it dispatched and it could only be redelivered after its lock expired.
- `client.WithMaxConcurrentOrchestrationWorkItems`, `WithMaxConcurrentActivityWorkItems`, and `WithMaxConcurrentEntityWorkItems` now reject limits above `math.MaxInt32`. Previously such a limit silently wrapped to a negative value in the 32-bit `GetWorkItems` fields advertised to the service.
- `client.WithTaskExecutorOptions` now rejects nil options instead of panicking when the worker builds its task executor.
- `client.WithWorkItemFilters` now rejects contradictory configurations that combine a `RejectAll*` flag with entries of the same kind, and rejects duplicate orchestration, activity, or entity filter names whose effective version sets would otherwise depend on iteration order.
- A redundant `ExecutionSuspended` event received while an orchestration is already suspended is now dropped instead of buffered. Previously the buffered suspend was replayed when the matching resume drained the buffer, immediately re-suspending the orchestration, so N suspends required N resumes.
- Terminating a suspended orchestration now completes it. The terminate event clears the suspended flag so the completion action is emitted, and a terminal completion event now takes precedence over the suspended flag when reporting runtime status. Previously such an instance stayed `SUSPENDED` forever and `WaitForOrchestrationCompletion` never returned.
- Termination now replaces a natural completion action when both become ready in the same orchestration turn. Previously the worker could emit two completion actions and repeatedly fail to persist the duplicate terminal event.
- The gRPC `StartInstance` handler now maps backend errors through the shared management error mapping, so a rejected duplicate instance ID reaches gRPC clients as `codes.AlreadyExists` and stays matchable with `errors.Is(err, api.ErrDuplicateInstance)`. Previously it surfaced as an untyped `codes.Unknown` error.
- `durabletaskscheduler` `Authentication=DefaultAzure` now applies `TenantID` and `AdditionallyAllowedTenants` to `DefaultAzureCredential`. Previously both were accepted from options and connection strings and then silently dropped, so tenant-scoped `DefaultAzure` configurations authenticated against the wrong tenant set.
- `durabletaskscheduler` identity fields are now trimmed for every authentication mode, and blank `AdditionallyAllowedTenants` entries are dropped. Previously only `ManagedIdentity` trimmed `ClientID`, so padded values reached Azure Identity verbatim.
- `durabletaskscheduler` token scopes now trim whitespace and all trailing slashes from `Options.ResourceID`. Previously only one trailing slash was removed and untrimmed values produced a malformed scope.
- The owned DTS management client now observes both unary and streaming RPC outcomes for channel recreation. Caller cancellation, caller deadlines, and expected instance-wait deadlines are neutral and no longer reset or poison an existing transport-failure streak.
- `TaskHubGrpcClient.ScheduleNewOrchestration` now sends the caller's W3C trace context on `CreateInstanceRequest`, and the gRPC sidecar's `StartInstance` handler now starts the `create_orchestration` span from it. Previously every orchestration scheduled over gRPC or the Durable Task Scheduler began a disconnected root trace instead of joining the caller's trace.
- Activity spans now report `Error` status with the activity's failure message when an activity fails. Previously a failed activity was reported as a task failure without a Go error, so its span was indistinguishable from a successful one in a distributed trace.

## [v0.6.0] - 2025-02-05

### Added

- Add API to set custom status ([#81](https://github.com/microsoft/durabletask-go/pull/81)) - by [@famarting](https://github.com/famarting)
- Add missing purge orchestration options ([#82](https://github.com/microsoft/durabletask-go/pull/82)) - by [@famarting](https://github.com/famarting)
- Add support for activity retry policies ([#83](https://github.com/microsoft/durabletask-go/pull/83)) - by [@famarting](https://github.com/famarting)
- Add support for sub-orchestration retry policies ([#84](https://github.com/microsoft/durabletask-go/pull/84)) - by [@famarting](https://github.com/famarting)
- Add postgres support ([#86](https://github.com/microsoft/durabletask-go/pull/86)) - by [@acx1729](https://github.com/acx1729)

### Changed

- Make WaitForOrchestrationXXX gRPC APIs resilient ([#80](https://github.com/microsoft/durabletask-go/pull/80)) - by [@famarting](https://github.com/famarting)
- Improve worker shutdown logic ([#77](https://github.com/microsoft/durabletask-go/pull/77)) - by [@famarting](https://github.com/famarting)
- Fix GetInstance gRPC API to return not found when instance is not found ([#87](https://github.com/microsoft/durabletask-go/pull/87)) - by [@cgillum](https://github.com/cgillum)
- Bump golang.org/x/crypto from 0.27.0 to 0.31.0 ([#91](https://github.com/microsoft/durabletask-go/pull/91)) - by [@dependabot](https://github.com/apps/dependabot)
- Bump golang.org/x/net from 0.25.0 to 0.33.0 ([#92](https://github.com/microsoft/durabletask-go/pull/92)) - by [@dependabot](https://github.com/apps/dependabot)

## [v0.5.0] - 2024-06-28

### Added

- Cascading Terminate and Purge support ([#47](https://github.com/microsoft/durabletask-go/pull/47) and [#63](https://github.com/microsoft/durabletask-go/pull/63)) - by [@shivamkm07](https://github.com/shivamkm07)
- Support for scheduled orchestration starts ([#60](https://github.dev/microsoft/durabletask-go/pull/60)) - by [@shivamkm07](https://github.com/shivamkm07)

### Changed

- Bump google.golang.org/grpc from 1.53.0 to 1.56.3 ([#39](https://github.com/microsoft/durabletask-go/pull/39))
- Updated durabletask-protobuf submodule to [`4207e1d`](https://github.com/microsoft/durabletask-protobuf/commit/4207e1dbd14cedc268f69c3befee60fcaad19367)
- Add retries to GetWorkItems stream connection ([#72](https://github.com/microsoft/durabletask-go/pull/72)) - by [@famarting](https://github.com/famarting)
- Fix orchestration hang caused by worker disconnect ([#61](https://github.com/microsoft/durabletask-go/pull/61))

## [v0.4.0] - 2023-12-18

### Changed

- Support reusing orchestration id ([#46](https://github.com/microsoft/durabletask-go/pull/46)) - contributed by [@kaibocai](https://github.com/kaibocai)

### Fixed

- Fix nil pointer dereference when consuming events ([#48](https://github.com/microsoft/durabletask-go/pull/48)) - contributed by [@impl](https://github.com/impl)

## [v0.3.1] - 2023-09-08

### Fixed

- Fixed another ticker memory leak ([#30](https://github.com/microsoft/durabletask-go/pull/30)) - contributed by [@DeepanshuA](https://github.com/DeepanshuA) and [@ItalyPaleAle](https://github.com/ItalyPaleAle)

### Changed

- Small tweak to IsDurableTaskGrpcRequest ([#29](https://github.com/microsoft/durabletask-go/pull/29)) - contributed by [@ItalyPaleAle](https://github.com/ItalyPaleAle)

## [v0.3.0] - 2023-07-13

This is a breaking change release that introduces the ability to run workflows in 
Go in an out-of-process worker process. It also contains various minor improvements.

### Added

- Added `client` package with `TaskHubGrpcClient` and related functions
- Added otel span events for external events, suspend, and resume operations
- Added termination support to task module
- Added sub-orchestration support to task module
- (Tests) Added test suite starter for Go-based orchestration execution logic

### Changed

- Renamed `WithJsonSerializableEventData` to `WithJsonEventPayload`
- Moved gRPC client and related functions from `api` package to `client` package
- Switched SQLite driver to pure-Go implementation (no CGO dependency) ([#17](https://github.com/microsoft/durabletask-go/pull/17)) - contributed by [@ItalyPaleAle](https://github.com/ItalyPaleAle)
- Orchestration metadata fetching now gets input and output data by default (previously had to opt-in)
- Removed "input" parameter from CallActivity APIs and replaced with options pattern
- Removed "reason" parameter from Termination APIs and replaced with options pattern
- Renamed api.WithJsonEventPayload to api.WithEventPayload
- Separate gRPC service registration from NewGrpcExecutor ([#26](https://github.com/microsoft/durabletask-go/pull/26)) - contributed by [@ItalyPaleAle](https://github.com/ItalyPaleAle)
- Default to using in-memory databases for sqlite backend ([#28](https://github.com/microsoft/durabletask-go/pull/28))
- Bump google.golang.org/grpc from 1.50.0 to 1.53.0 ([#23](https://github.com/microsoft/durabletask-go/pull/23))
- (Tests) Switched from `assert` to `require` in several tests to simplify code

### Fixed

- Timeout error propagation in gRPC client
- Various static analysis warnings

## [v0.2.4] - 2023-05-25

### Fixed

- Fix ticker memory leak - contributed by [@yaron2](https://github.com/yaron2)
