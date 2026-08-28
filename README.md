# Durable Task Scheduler SDK for Go

[![Build](https://github.com/microsoft/durabletask-go/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-go/actions/workflows/pr-validation.yml)

The Durable Task Scheduler SDK for Go provides management and worker APIs for
writing durable, fault-tolerant business logic (*orchestrations*) as ordinary Go
code and running it on [Azure Durable Task Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler)
(DTS). Orchestrations, activities, and durable entities are written in Go, while
the scheduler owns durable state, dispatch, and recovery.

The API follows the same SDK model as
[`microsoft/durabletask-dotnet`](https://github.com/microsoft/durabletask-dotnet).
It also takes inspiration from [Go Workflows](https://github.com/cschleiden/go-workflows)
and [Temporal](https://github.com/temporalio/temporal).

> This project is a work-in-progress and should not be used for production workloads. The public API surface is also not yet stable. The project itself is also in the very early stages and is missing some of the basics, such as contribution guidelines, etc.

## Durable Task Scheduler

DTS is the only supported runtime. The top-level
[`durabletaskscheduler`](./durabletaskscheduler) package is the SDK integration
surface. It provides validated connection-string configuration, Azure token
credentials, separately owned management and worker connections, resilient
worker streaming, and environment-gated emulator tests.
DTS management clients also expose API-owned orchestration history and recurring
interval schedules without leaking generated protobuf messages.

```go
options, err := durabletaskscheduler.NewOptionsFromConnectionString(
    os.Getenv("DTS_CONNECTION_STRING"))
if err != nil {
    return err
}

registry := task.NewTaskRegistry()
if err := registry.AddOrchestratorN("ActivitySequence", ActivitySequence); err != nil {
    return err
}
if err := registry.AddActivityN("SayHello", SayHello); err != nil {
    return err
}

logger := backend.DefaultLogger()

// The management client schedules orchestrations and reads their state.
client, err := durabletaskscheduler.NewClient(ctx, options, logger)
if err != nil {
    return err
}
defer client.Close()

// The worker executes the registered orchestrators and activities.
worker, err := durabletaskscheduler.NewWorker(
    options, registry, logger, durabletaskclient.WithAutoWorkItemFilters())
if err != nil {
    return err
}
if err := worker.Start(ctx); err != nil {
    return err
}
defer worker.Shutdown(ctx)

id, err := client.ScheduleNewOrchestration(ctx, "ActivitySequence")
if err != nil {
    return err
}
metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
```

Most samples that connect to a task hub use this setup through the shared
[`samples/internal/dtssample`](./samples/internal/dtssample) helper, which reads
`DTS_CONNECTION_STRING`, opens the client, and starts the worker.
`samples/exporthistory` wires the SDK directly because it needs the client
before registering the export system tasks.

See the [DTS transport guide and feature matrix](./durabletaskscheduler/README.md)
and the [environment-driven sample](./samples/durabletaskscheduler).

### Package layout

| Package | Purpose |
| - | - |
| [`durabletaskscheduler`](./durabletaskscheduler) | `NewClient` and `NewWorker` — the DTS integration surface most applications use |
| [`task`](./task) | Registering and authoring orchestrators, activities, and entities |
| [`api`](./api) | Instance IDs, orchestration metadata, queries, and client option types |
| [`client`](./client) | Lower-level gRPC management client and worker over a caller-owned connection |
| [`payload`](./payload) | Large payload stores (memory, file, Azure Blob Storage) |
| [`exporthistory`](./exporthistory) | Preview history export to Azure Blob Storage |
| [`backend`](./backend) | Internal runtime plumbing shared by the above — logger, metric hooks, executor contract |

DTS owns durable state, dispatch, and recovery, so this SDK has no task hub host
and no pluggable storage. `backend` is shared runtime plumbing that appears in
current public signatures for logging, metrics, and task execution; it is not
an extension point, and nothing in it lets an application implement or host a
task hub.

## History export (preview)

The top-level [`exporthistory`](./exporthistory) package exports terminal
orchestration histories to Azure Blob Storage as gzip-compressed JSONL, using a
durable entity for job state, an operation orchestrator, an export orchestrator,
and two activities for listing and exporting. It supports batch and continuous jobs,
durable checkpoints, terminal-status filters, per-batch instance limits, failure
collection, and typed validation, not-found, and invalid-transition errors.
Exported objects are stored as opaque gzip files (`.jsonl.gz` with content type
`application/gzip` and no `Content-Encoding`), so every reader gets exactly the
bytes the object name promises.

```go
store, err := exporthistory.NewAzureBlobStore(exporthistory.AzureBlobStoreOptions{
    ConnectionString: storageConnectionString,
    ContainerName:    "history-exports",
})
err = exporthistory.Register(registry, exporthistory.WorkerOptions{
    Source: taskHubClient, // supplies ListInstanceIDs and orchestration history
    Store:  store,
})
worker, err := durabletaskscheduler.NewWorker(options, registry, logger,
    durabletaskclient.WithAutoWorkItemFilters(),
    exporthistory.WithExportHistory(),
)

exportClient, err := exporthistory.NewClient(taskHubClient, exporthistory.ClientOptions{
    ContainerName: "history-exports",
})
job, err := exportClient.CreateJob(ctx, exporthistory.JobCreationOptions{
    Mode:              exporthistory.ExportModeBatch,
    CompletedTimeFrom: from,
    CompletedTimeTo:   to,
})
description, err := job.Describe(ctx)
```

This package is **preview**: its exported API, the serialized shape of the
`ExportJob` entity state, and the names of its system tasks may change without a
major version bump. Extended sessions are not supported.

See the [export history guide](./exporthistory/README.md) and the
[sample](./samples/exporthistory).

## Task versions

Orchestrators and activities can register multiple implementations under the
same logical name. Registry identity is case-insensitive and includes both name
and version:

```go
registry.AddOrchestratorNVersion("orders", "v1", ordersV1)
registry.AddOrchestratorNVersion("orders", "v2", ordersV2)
registry.AddActivityNVersion("charge", "v2", chargeV2)
```

Dispatch prefers an exact `(name, version)` match. A versioned request falls
back to an unversioned registration only when that logical name has no versioned
registrations. Activities inherit the parent orchestration version unless
`task.WithActivityVersion` is supplied; passing `""` explicitly requests the
unversioned activity. Sub-orchestrations use `VersioningOptions.DefaultVersion`
unless explicitly overridden. `task.WithContinueAsNewVersion` moves the next
execution across a deterministic ContinueAsNew boundary; passing
`task.UnversionedTaskVersion` migrates back to an unversioned registration.

Set `durabletaskscheduler.Options.Versioning` to configure both management and
worker defaults. Reject/fail mismatch strategies remain available for rolling
deployments.

## Data converters

`api.DataConverter` owns application payload serialization. The default
`api.JSONDataConverter` preserves the existing `encoding/json` wire format.
Set `durabletaskscheduler.Options.DataConverter` once; it is propagated to both
the management client and worker. Typed orchestration, activity, entity, event,
status, management, metadata, and ContinueAsNew payloads use the converter.
`WithRaw*`, serialized metadata fields, failure metadata, and large-payload
reference descriptors remain raw protocol values and bypass conversion.
Converter identity is not persisted, so a replacement converter must continue
to decode payloads written by earlier deployments.

## Durable entities

Durable entities are addressable stateful objects that execute operations one at
a time. The Go SDK supports raw entity functions, struct-based dispatch,
scheduled signals, orchestration calls, entity-to-entity signals, queries and
cleanup, and ordered multi-entity critical sections. The DTS worker accepts
legacy `EntityBatchRequest` and current `EntityRequestV2` work items.

```go
registry.AddEntityN("counter", task.NewEntityFor[Counter]())
counter := api.NewEntityID("counter", "orders")

client.SignalEntity(ctx, counter, "Add", api.WithSignalInput(1))

registry.AddOrchestratorN("read-counter", func(ctx *task.OrchestrationContext) (any, error) {
    var value int
    err := ctx.CallEntity(counter, "Get").Await(&value)
    return value, err
})
```

See the complete [durable entities sample](./samples/entity).

Advanced management includes bounded queries and ID listing, restart/rewind,
batch and filtered purge, immediate termination, tags, and explicit worker
capabilities, subject to what the connected service implements. History can be
buffered with a validated event cap or consumed incrementally with
`StreamOrchestrationHistory`. Large payloads are externalized by configuring an
`api.LargePayloadOptions` store/resolver on DTS clients and workers; the
`payload` package includes production Azure Blob support that emits the same
self-describing `blob:v2` tokens as the .NET SDK.

## Language SDKs for gRPC

Durable Task Scheduler speaks the same gRPC contract to every Durable Task SDK, so a single task hub can host orchestrations written in any of the following languages:

| Language/Stack | Package | Project Home | Samples |
| - | - | - | - |
| .NET | [![NuGet](https://img.shields.io/nuget/v/Microsoft.DurableTask.Client.svg?style=flat)](https://www.nuget.org/packages/Microsoft.DurableTask.Client/) | [GitHub](https://github.com/microsoft/durabletask-dotnet) | [Samples](https://github.com/microsoft/durabletask-dotnet/tree/main/samples) |
| Java | [![Maven Central](https://img.shields.io/maven-central/v/com.microsoft/durabletask-client?label=durabletask-client)](https://search.maven.org/artifact/com.microsoft/durabletask-client) | [GitHub](https://github.com/microsoft/durabletask-java) | [Samples](https://github.com/microsoft/durabletask-java/tree/main/samples/src/main/java/io/durabletask/samples) |
| Python | [![PyPI version](https://badge.fury.io/py/durabletask.svg)](https://badge.fury.io/py/durabletask) | [GitHub](https://github.com/microsoft/durabletask-python) | [Samples](https://github.com/microsoft/durabletask-python/tree/main/examples) |

The gRPC API is defined [here](https://github.com/microsoft/durabletask-protobuf).

## Writing orchestrations in Go

> You can find code samples in the [samples](./samples/) directory.
> Each one connects to the task hub named by `DTS_CONNECTION_STRING`; to run
> them, set that variable and run `go run ./samples/<name>`. The
> [`azurefunctions`](./samples/azurefunctions) sample is the exception: it is an
> Azure Functions custom handler, hosted by the Durable Functions extension.
> [`exporthistory`](./samples/exporthistory) also requires
> `EXPORT_STORAGE_CONNECTION_STRING` and optionally accepts `EXPORT_CONTAINER`.

### Activity sequence example

Activity sequences like the following are the simplest and most common pattern used in the Durable Task Framework.

```go
// ActivitySequenceOrchestrator makes three activity calls in sequence and results the results
// as an array.
func ActivitySequenceOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var helloTokyo string
	if err := ctx.CallActivity("SayHello", task.WithActivityInput("Tokyo")).Await(&helloTokyo); err != nil {
		return nil, err
	}
	var helloLondon string
	if err := ctx.CallActivity("SayHello", task.WithActivityInput("London")).Await(&helloLondon); err != nil {
		return nil, err
	}
	var helloSeattle string
	if err := ctx.CallActivity("SayHello", task.WithActivityInput("Seattle")).Await(&helloSeattle); err != nil {
		return nil, err
	}
	return []string{helloTokyo, helloLondon, helloSeattle}, nil
}

// SayHelloActivity can be called by an orchestrator function and will return a friendly greeting.
func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}
```

You can find the full sample [here](./samples/durabletaskscheduler).

### Fan-out / fan-in execution example

The next most common pattern is "fan-out / fan-in" where multiple activities are run in parallel, as shown in the snippet below (note that the `GetDevicesToUpdate` and `UpdateDevice` activity definitions are left out of the snippet below for brevity):

```go
// UpdateDevicesOrchestrator is an orchestrator that runs activities in parallel
func UpdateDevicesOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	// Get a dynamic list of devices to perform updates on
	var devices []string
	if err := ctx.CallActivity("GetDevicesToUpdate").Await(&devices); err != nil {
		return nil, err
	}

	// Start a dynamic number of tasks in parallel, not waiting for any to complete (yet)
	tasks := make([]task.Task, len(devices))
	for i, id := range devices {
		tasks[i] = ctx.CallActivity("UpdateDevice", task.WithActivityInput(id))
	}

	// Now that all are started, wait for them to complete and then return the success rate
	successCount := 0
	for _, task := range tasks {
		var succeeded bool
		if err := task.Await(&succeeded); err == nil && succeeded {
			successCount++
		}
	}

	return float32(successCount) / float32(len(devices)), nil
}
```

The full sample can be found [here](./samples/parallel).

### Failure handling and retries

Activity and sub-orchestration failures return `*task.TaskFailedError`. Entity calls return
`*task.EntityOperationFailedError`. Both include API-owned `api.FailureDetails` with the stable error type,
message, stack trace, inner failure, non-retriable marker, and custom properties received over the Durable Task
protocol.

```go
err := ctx.CallActivity("ChargeCard").Await(nil)
var failed *task.TaskFailedError
if errors.As(err, &failed) {
	fmt.Printf("%s failed: %s\n", failed.TaskName, failed.FailureDetails)
}
```

Retry handlers receive deterministic failure data and run during orchestration replay, so handlers must not
perform I/O or depend on wall-clock time.

```go
policy := &task.RetryPolicy{
	MaxAttempts:          3,
	InitialRetryInterval: time.Second,
	Handle: func(retry task.RetryContext) bool {
		return !retry.LastFailure.IsCausedBy(api.ErrorTypeActivityTaskNotFound)
	},
}
```

Failures marked non-retriable, including missing activity/entity registrations and version mismatches, bypass
custom retry handlers.

### External orchestration inputs (events) example

Sometimes orchestrations need asynchronous input from external systems. For example, an approval workflow may require a manual approval signal from an authorized user. Or perhaps an orchestration pauses and waits for a command from an operator. The `WaitForSingleEvent` method can be used in an orchestrator function to pause execution and wait for such inputs. You an even specify a timeout value indicating how long to wait for the input before resuming execution (use `-1` to indicate infinite timeout).

```go
// ExternalEventOrchestrator is an orchestrator function that blocks for 30 seconds or
// until a "Name" event is sent to it.
func ExternalEventOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var nameInput string
	if err := ctx.WaitForSingleEvent("Name", 30*time.Second).Await(&nameInput); err != nil {
		// Timeout expired
		return nil, err
	}

	return fmt.Sprintf("Hello, %s!", nameInput), nil
}
```

Sending an event to a waiting orchestration can be done using the `RaiseEvent` method of the task hub client. These events are durably buffered in the orchestration state and are consumed as soon as the target orchestration calls `WaitForSingleEvent` with a matching event name. The following code shows how to use the `RaiseEvent` method to send an event with a payload to a running orchestration. See [Managing orchestrations](#managing-orchestrations) for more information on how to interact with orchestrations in Go.

When multiple live waits use the same event name, the newest waiter receives
the next event (LIFO), matching the Durable Task .NET replay contract. Events
that arrived before any waiter remain buffered and are consumed in arrival
order (FIFO).

```go
id, _ := client.ScheduleNewOrchestration(ctx, "ExternalEventOrchestrator")

// Prompt the user for their name and send that to the orchestrator
go func() {
	fmt.Println("Enter your first name: ")
	var nameInput string
	fmt.Scanln(&nameInput)

	client.RaiseEvent(ctx, id, "Name", api.WithEventPayload(nameInput))
}()
```

The full sample can be found [here](./samples/externalevents).

### Managing orchestrations

The `TaskRegistry` type allows you to register orchestrator, activity, and entity functions, and the client returned by `durabletaskscheduler.NewClient` allows you to start, query, terminate, suspend, resume, and wait for orchestrations to complete.

The code snippet below demonstrates how to register and start a new instance of the `ActivitySequence` orchestration and wait for it to complete. Connecting the client and worker is left out for brevity; see [Durable Task Scheduler](#durable-task-scheduler) above.

```go
r := task.NewTaskRegistry()
r.AddOrchestratorN("ActivitySequence", ActivitySequenceOrchestrator)
r.AddActivityN("SayHello", SayHelloActivity)

id, err := client.ScheduleNewOrchestration(ctx, "ActivitySequence")
if err != nil {
  panic(err)
}

metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
if err != nil {
  panic(err)
}

fmt.Printf("orchestration completed: %v\n", metadata)
```

Each sample linked above has a full implementation you can use as a reference.

## Distributed tracing support

The SDK propagates a sampled caller's W3C trace context when scheduling an
orchestration. DTS owns orchestration, activity, timer, and sub-orchestration
span emission service-side. Application code can use standard
[OpenTelemetry](https://opentelemetry.io/) instrumentation for caller spans,
custom activity spans, and outbound dependencies.

The following example configures application-process trace export to
[Zipkin](https://zipkin.io/). Configure DTS telemetry separately for the
service-owned durable operation spans.

```go
func ConfigureZipkinTracing() (*trace.TracerProvider, error) {
	// Inspired by this sample: https://github.com/open-telemetry/opentelemetry-go/blob/main/example/zipkin/main.go
	exp, err := zipkin.New("http://localhost:9411/api/v2/spans")
	if err != nil {
		return nil, err
	}

	// NOTE: The simple span processor is not recommended for production.
	//       Instead, the batch span processor should be used for production.
	processor := trace.NewSimpleSpanProcessor(exp)
	// processor := trace.NewBatchSpanProcessor(exp)

	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(processor),
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(resource.NewWithAttributes(
			"durabletask.io",
			attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("sample-app")},
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}
```

The [distributed tracing sample](./samples/distributedtracing) starts an
application caller span before `ScheduleNewOrchestration`, allowing DTS to join
its service-side spans to the same trace. It also instruments an HTTP request
made by an activity.

## Cloning this repository

Clone the repository as you normally would:

```bash
git clone https://github.com/microsoft/durabletask-go
```

The protocol buffer definitions used to generate the gRPC bindings are vendored under [`vendored/durabletask-protobuf/protos`](./vendored/durabletask-protobuf/protos). See [`vendored/durabletask-protobuf/README.md`](./vendored/durabletask-protobuf/README.md) for details on how to refresh them from the upstream [microsoft/durabletask-protobuf](https://github.com/microsoft/durabletask-protobuf) repository.

## Building the project

This project requires Go 1.23 or greater. It is a library, so build every package with `go build ./...` at the project root.

### Generating protobuf

Use the following command to regenerate the protobuf bindings from the vendored proto file. Use this whenever updating the proto file under [`vendored/durabletask-protobuf/protos`](./vendored/durabletask-protobuf/protos).

```bash
# NOTE: assumes the .proto file defines: option go_package = "/internal/protos"
protoc --go_out=. --go-grpc_out=. -I vendored/durabletask-protobuf/protos orchestrator_service.proto
```

### Test doubles

There is no generated mock package. Because DTS is the only supported runtime,
the pieces worth faking are the generated gRPC surfaces, so tests use small
hand-written fakes instead:

- Client tests embed `protos.UnimplementedTaskHubSidecarServiceServer` and serve
  it over [`bufconn`](https://pkg.go.dev/google.golang.org/grpc/test/bufconn), or
  embed `protos.TaskHubSidecarServiceClient` and override the few RPCs under test.
  (`TaskHubSidecarService` is the generated wire-contract name of the DTS gRPC
  service; it does not imply a sidecar deployment.)
- Worker and executor tests implement `backend.Executor` and
  `backend.EntityExecutor` directly.
- Orchestration behavior is exercised by feeding hand-built histories to
  `task.NewTaskExecutor`.

## Running tests

Package tests live alongside the code they exercise, while the `./tests`
hierarchy provides [black box testing](https://en.wikipedia.org/wiki/Black-box_testing)
that helps catch accidental breaking API changes. `./tests` holds deterministic
runtime tests driven by hand-built histories, and
`./tests/durabletaskscheduler` holds the end-to-end suite that runs against a
live Durable Task Scheduler.

Run the complete repository test suite with the following command.

```bash
go test ./... -count=1 -coverpkg=./api,./task,./client,./durabletaskscheduler,./exporthistory,./payload,./backend,./internal/analysis/orchestratorgo,./internal/contextprop,./internal/failure,./internal/grpcerrors,./internal/helpers,./internal/historyconv,./internal/largepayload,./internal/tagcodec
```

Durable Task Scheduler and Azurite tests skip themselves unless their
environment variables point at running services (see
[Running locally](#running-locally)). PR validation runs the complete suite
against both services and repeats it with the race detector on the latest
supported Go version.

### Checking orchestrators for replay hazards

Orchestrator code is replayed from history on every turn, so it must be
deterministic and free of side effects. `cmd/orchestratorvet` is a
[`go vet`](https://pkg.go.dev/cmd/vet)-compatible driver for the
`orchestratorgo` analyzer, which performs whole-package analysis of the
orchestrators a package registers with `task.TaskRegistry`.

```bash
go build -o ./bin/orchestratorvet ./cmd/orchestratorvet
go vet -vettool=$PWD/bin/orchestratorvet ./...
```

The analyzer starts from every `AddOrchestrator`, `AddOrchestratorN`,
`AddOrchestratorVersion`, and `AddOrchestratorNVersion` call it can resolve, and
follows the call graph through same-package named functions, methods, resolvable
function variables, and nested function literals. Code that is not reachable
from a registered orchestrator, including activity and entity bodies, is never
reported. It reports wall-clock reads and host timers, nondeterministic
identifier and random sources, unsafe parallelism and synchronization, direct
filesystem, network, process, and environment I/O, replay-unsafe logging,
provably non-progressing unbounded loops, task names a complete registration set
proves are missing, and registration forms that `task.TaskRegistry` rejects or
that derive an unstable name.

`time.Now()` and `go func() { ... }()` carry `analysis.SuggestedFix` rewrites to
`ctx.CurrentTimeUtc` and `ctx.Go`, which `gopls` and `go vet -fix` can apply.
Other diagnostics have no fix because no single rewrite is always correct.

The analyzer reports only what it can prove and stays silent otherwise, so
enabling it on an existing codebase does not produce a wave of diagnostics.
Files ending in `_test.go` are excluded by default because tests commonly
register intentionally invalid or nondeterministic orchestrators. Pass
`-orchestratorgo.test-files` to include them.

See [`cmd/orchestratorvet/README.md`](cmd/orchestratorvet/README.md) for the
full list of checks, the suggested fixes, the false-positive guardrails, and the
limitations.

## Running locally

The [Durable Task Scheduler emulator](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/quickstart-durable-task-scheduler) provides a local task hub for development and for this repository's end-to-end tests. Start it, and Azurite for the blob-backed payload and history-export tests, with any OCI runtime:

```bash
docker run -d -p 8080:8080 -p 8082:8082 \
  -e DTS_TASK_HUB_NAMES=default \
  mcr.microsoft.com/dts/dts-emulator:latest
docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite:3.35.0
```

Point the samples at it with a connection string:

```bash
export DTS_CONNECTION_STRING="Endpoint=http://localhost:8080;TaskHub=default;Authentication=None"
go run ./samples/durabletaskscheduler
```

The emulator's dashboard is served on port `8082`.

The end-to-end tests read the following environment variables and skip themselves when they are unset:

* `DTS_CONNECTION_STRING`: a full connection string. Takes precedence over the two variables below.
* `DTS_EMULATOR_ENDPOINT`: the emulator's gRPC endpoint, for example `http://127.0.0.1:8080`.
* `DTS_TASK_HUB`: the task hub name to use with `DTS_EMULATOR_ENDPOINT`. Defaults to `default`.
* `AZURITE_CONNECTION_STRING`: an Azure Storage connection string used by the blob payload and history-export tests.

```bash
DTS_EMULATOR_ENDPOINT="http://127.0.0.1:8080" \
DTS_TASK_HUB="default" \
AZURITE_CONNECTION_STRING="UseDevelopmentStorage=true" \
go test ./... -count=1
```

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
