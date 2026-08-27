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

### Changed

- Removed the `submodules/durabletask-protobuf` git submodule. The `orchestrator_service.proto` file is now vendored under `vendored/durabletask-protobuf/protos/`, with the source provenance (URL, branch/ref, commit hash) tracked in `vendored/durabletask-protobuf/PROTO_SOURCE_COMMIT_HASH` and a helper script (`vendored/durabletask-protobuf/update-proto.sh`) for refreshing the proto from upstream.
- Refreshed the vendored Durable Task protobuf contract. `api.OrchestrationIdReusePolicy` and `api.CreateOrchestrationAction` are now API-owned types instead of aliases for generated protobuf types; callers should construct policies with the `api` types.
- `TaskHubGrpcClient` now rejects the ambiguous legacy `IGNORE` reuse action by default. Known-compatible local sidecars can opt in with `client.WithLegacyOrchestrationIDReusePolicyWire`; DTS callers must use the representable `ERROR` or `TERMINATE` semantics.
- The standalone gRPC sidecar enables current-proto `replaceableStatus` semantics. Embedded hosts must opt in with `backend.WithCurrentOrchestrationIDReusePolicyWire`; without it, field-1-only policies fail closed because legacy `ERROR` and current `TERMINATE` requests are wire-identical.
- Orchestration metadata now includes execution ID, completion/scheduled timestamps, parent ID, failure details, and tags when provided by the backend or service.
- Replaced protobuf failure details in public metadata with `api.FailureDetails`. `RetryPolicy.Handle` now receives deterministic `task.RetryContext`, and non-retriable or canceled failures bypass the handler.
- `FetchEntityMetadata` now returns `api.ErrInstanceNotFound` when the entity does not exist.

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
