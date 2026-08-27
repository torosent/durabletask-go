package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	ErrTaskHubGrpcWorkerAlreadyRunning = errors.New("gRPC worker is already running")
	errSilentDisconnect                = errors.New("work item stream was silent beyond the configured timeout")
)

type workItemsStream interface {
	Recv() (*protos.WorkItem, error)
}

type grpcWorkerClientFactory func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error)

// TaskHubGrpcWorkerConnectionFactory creates a gRPC connection for a worker
// stream generation. A non-nil closer transfers ownership to the worker.
type TaskHubGrpcWorkerConnectionFactory func(context.Context) (grpc.ClientConnInterface, io.Closer, error)

type TaskHubGrpcWorkerOption func(*taskHubGrpcWorkerOptions) error

type WorkerCapability = protos.WorkerCapability

const (
	WorkerCapabilityHistoryStreaming WorkerCapability = protos.WorkerCapability_WORKER_CAPABILITY_HISTORY_STREAMING
	WorkerCapabilityScheduledTasks   WorkerCapability = protos.WorkerCapability_WORKER_CAPABILITY_SCHEDULED_TASKS
	WorkerCapabilityLargePayloads    WorkerCapability = protos.WorkerCapability_WORKER_CAPABILITY_LARGE_PAYLOADS
)

type WorkItemFilter struct {
	Name     string
	Versions []string
}

type WorkItemFilters struct {
	Orchestrations []WorkItemFilter
	Activities     []WorkItemFilter
	Entities       []string
}

type taskHubGrpcWorkerOptions struct {
	maxConcurrentOrchestrations int
	maxConcurrentActivities     int
	maxConcurrentEntities       int
	helloTimeout                time.Duration
	silentDisconnectTimeout     time.Duration
	rpcTimeout                  time.Duration
	reconnectBaseDelay          time.Duration
	reconnectMaxDelay           time.Duration
	transientRetryMaxAttempts   int
	transientRetryBaseDelay     time.Duration
	transientRetryMaxDelay      time.Duration
	taskExecutorOptions         []task.TaskExecutorOption
	capabilities                []WorkerCapability
	workItemFilters             *WorkItemFilters
	largePayloads               *api.LargePayloadOptions
}

func defaultTaskHubGrpcWorkerOptions() taskHubGrpcWorkerOptions {
	defaultConcurrency := 100 * runtime.GOMAXPROCS(0)
	return taskHubGrpcWorkerOptions{
		maxConcurrentOrchestrations: defaultConcurrency,
		maxConcurrentActivities:     defaultConcurrency,
		maxConcurrentEntities:       defaultConcurrency,
		helloTimeout:                30 * time.Second,
		silentDisconnectTimeout:     2 * time.Minute,
		rpcTimeout:                  30 * time.Second,
		reconnectBaseDelay:          200 * time.Millisecond,
		reconnectMaxDelay:           15 * time.Second,
		transientRetryMaxAttempts:   10,
		transientRetryBaseDelay:     200 * time.Millisecond,
		transientRetryMaxDelay:      15 * time.Second,
		capabilities:                []WorkerCapability{WorkerCapabilityHistoryStreaming},
	}
}

func WithMaxConcurrentOrchestrationWorkItems(n int) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if n <= 0 {
			return fmt.Errorf("maximum concurrent orchestration work items must be greater than zero")
		}
		options.maxConcurrentOrchestrations = n
		return nil
	}
}

func WithMaxConcurrentActivityWorkItems(n int) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if n <= 0 {
			return fmt.Errorf("maximum concurrent activity work items must be greater than zero")
		}
		options.maxConcurrentActivities = n
		return nil
	}
}

// WithMaxConcurrentEntityWorkItems limits concurrent entity batch execution.
func WithMaxConcurrentEntityWorkItems(n int) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if n <= 0 {
			return fmt.Errorf("maximum concurrent entity work items must be greater than zero")
		}
		options.maxConcurrentEntities = n
		return nil
	}
}

func WithWorkerHelloTimeout(timeout time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("worker Hello timeout must be greater than zero")
		}
		options.helloTimeout = timeout
		return nil
	}
}

func WithWorkerSilentDisconnectTimeout(timeout time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("worker silent disconnect timeout must be greater than zero")
		}
		options.silentDisconnectTimeout = timeout
		return nil
	}
}

func WithWorkerRPCTimeout(timeout time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("worker RPC timeout must be greater than zero")
		}
		options.rpcTimeout = timeout
		return nil
	}
}

func WithWorkerReconnectBackoff(baseDelay, maxDelay time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if baseDelay <= 0 || maxDelay < baseDelay {
			return fmt.Errorf("worker reconnect delays must be positive and max delay must be at least the base delay")
		}
		options.reconnectBaseDelay = baseDelay
		options.reconnectMaxDelay = maxDelay
		return nil
	}
}

func WithWorkerTransientRetryPolicy(maxAttempts int, baseDelay, maxDelay time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if maxAttempts <= 0 {
			return fmt.Errorf("worker transient retry attempts must be greater than zero")
		}
		if baseDelay <= 0 || maxDelay < baseDelay {
			return fmt.Errorf("worker transient retry delays must be positive and max delay must be at least the base delay")
		}
		options.transientRetryMaxAttempts = maxAttempts
		options.transientRetryBaseDelay = baseDelay
		options.transientRetryMaxDelay = maxDelay
		return nil
	}
}

// WithTaskExecutorOptions configures the task executor used by the gRPC worker.
// This allows task options such as task.WithVersioning to participate in DTS dispatch.
func WithTaskExecutorOptions(options ...task.TaskExecutorOption) TaskHubGrpcWorkerOption {
	return func(workerOptions *taskHubGrpcWorkerOptions) error {
		workerOptions.taskExecutorOptions = append(workerOptions.taskExecutorOptions, options...)
		return nil
	}
}

// WithWorkerCapabilities explicitly configures the capabilities advertised to the sidecar.
func WithWorkerCapabilities(capabilities ...WorkerCapability) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		seen := make(map[WorkerCapability]struct{}, len(capabilities))
		configured := make([]WorkerCapability, 0, len(capabilities))
		for _, capability := range capabilities {
			switch capability {
			case WorkerCapabilityHistoryStreaming, WorkerCapabilityScheduledTasks, WorkerCapabilityLargePayloads:
			default:
				return fmt.Errorf("unsupported worker capability: %d", capability)
			}
			if _, ok := seen[capability]; ok {
				continue
			}
			seen[capability] = struct{}{}
			configured = append(configured, capability)
		}
		options.capabilities = configured
		return nil
	}
}

// WithScheduledTaskCapability controls scheduled-task capability advertisement.
func WithScheduledTaskCapability(enabled bool) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		options.capabilities = setWorkerCapability(options.capabilities, WorkerCapabilityScheduledTasks, enabled)
		return nil
	}
}

// WithWorkItemFilters restricts orchestration/activity names and versions and entity names accepted by the worker.
// A nil filter means no restrictions. An explicitly empty kind rejects all work items of that kind.
func WithWorkItemFilters(filters *WorkItemFilters) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if filters == nil {
			options.workItemFilters = nil
			return nil
		}
		clone, err := cloneWorkItemFilters(filters)
		if err != nil {
			return err
		}
		options.workItemFilters = clone
		return nil
	}
}

// WithWorkerLargePayloads configures worker payload hydration/externalization and advertises support.
func WithWorkerLargePayloads(options *api.LargePayloadOptions) TaskHubGrpcWorkerOption {
	return func(workerOptions *taskHubGrpcWorkerOptions) error {
		normalized, err := api.NormalizeLargePayloadOptions(options)
		if err != nil {
			return err
		}
		workerOptions.largePayloads = normalized
		workerOptions.capabilities = setWorkerCapability(
			workerOptions.capabilities,
			WorkerCapabilityLargePayloads,
			normalized != nil,
		)
		return nil
	}
}

// TaskHubGrpcWorker executes orchestration and activity work received from a
// TaskHubSidecarService work-item stream.
type TaskHubGrpcWorker struct {
	clientFactory grpcWorkerClientFactory
	executor      backend.Executor
	logger        backend.Logger
	options       taskHubGrpcWorkerOptions

	mu      sync.Mutex
	run     *grpcWorkerRun
	lastErr error
}

type grpcWorkerRun struct {
	intakeCtx        context.Context
	cancelIntake     context.CancelFunc
	processingCtx    context.Context
	cancelProcessing context.CancelFunc
	done             chan struct{}

	orchestrationSlots chan struct{}
	activitySlots      chan struct{}
	entitySlots        chan struct{}
	pending            sync.WaitGroup
	retired            sync.WaitGroup
	err                error
}

type grpcWorkerConnection struct {
	client       protos.TaskHubSidecarServiceClient
	stream       workItemsStream
	cancelStream context.CancelFunc
	closer       io.Closer

	pending sync.WaitGroup
}

// NewTaskHubGrpcWorker creates a worker that borrows a caller-owned connection.
// Reconnects recreate the work-item stream on the same connection.
func NewTaskHubGrpcWorker(
	cc grpc.ClientConnInterface,
	registry *task.TaskRegistry,
	logger backend.Logger,
	opts ...TaskHubGrpcWorkerOption,
) (*TaskHubGrpcWorker, error) {
	if cc == nil {
		return nil, fmt.Errorf("gRPC connection is required")
	}
	return newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			return protos.NewTaskHubSidecarServiceClient(cc), nil, nil
		},
		registry,
		logger,
		opts...,
	)
}

// NewTaskHubGrpcWorkerWithConnectionFactory creates a worker whose connection
// can be recreated after disconnects. Each returned closer is called only after
// all work dispatched through that connection has completed or been abandoned.
func NewTaskHubGrpcWorkerWithConnectionFactory(
	factory TaskHubGrpcWorkerConnectionFactory,
	registry *task.TaskRegistry,
	logger backend.Logger,
	opts ...TaskHubGrpcWorkerOption,
) (*TaskHubGrpcWorker, error) {
	if factory == nil {
		return nil, fmt.Errorf("gRPC worker connection factory is required")
	}
	return newTaskHubGrpcWorker(
		func(ctx context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			cc, closer, err := factory(ctx)
			if err != nil {
				return nil, nil, err
			}
			if cc == nil {
				if closer != nil {
					_ = closer.Close()
				}
				return nil, nil, fmt.Errorf("gRPC worker connection factory returned a nil connection")
			}
			return protos.NewTaskHubSidecarServiceClient(cc), closer, nil
		},
		registry,
		logger,
		opts...,
	)
}

func newTaskHubGrpcWorker(
	factory grpcWorkerClientFactory,
	registry *task.TaskRegistry,
	logger backend.Logger,
	opts ...TaskHubGrpcWorkerOption,
) (*TaskHubGrpcWorker, error) {
	if factory == nil {
		return nil, fmt.Errorf("gRPC worker client factory is required")
	}
	if registry == nil {
		return nil, fmt.Errorf("task registry is required")
	}
	if logger == nil {
		logger = backend.DefaultLogger()
	}

	options := defaultTaskHubGrpcWorkerOptions()
	for _, configure := range opts {
		if configure == nil {
			continue
		}
		if err := configure(&options); err != nil {
			return nil, err
		}
	}
	hasLargePayloadCapability := slices.Contains(options.capabilities, WorkerCapabilityLargePayloads)
	if hasLargePayloadCapability && options.largePayloads == nil {
		return nil, fmt.Errorf("large-payload capability requires worker large-payload options")
	}
	if options.largePayloads != nil {
		options.capabilities = setWorkerCapability(options.capabilities, WorkerCapabilityLargePayloads, true)
	}
	return &TaskHubGrpcWorker{
		clientFactory: factory,
		executor:      task.NewTaskExecutor(registry, options.taskExecutorOptions...),
		logger:        logger,
		options:       options,
	}, nil
}

// Start connects to the service, performs the Hello handshake, and starts the
// worker in the background.
func (w *TaskHubGrpcWorker) Start(ctx context.Context) error {
	run, connection, err := w.begin(ctx)
	if err != nil {
		return err
	}
	go w.execute(run, connection)
	return nil
}

// Run connects to the service, performs the Hello handshake, and blocks until
// the worker stops.
func (w *TaskHubGrpcWorker) Run(ctx context.Context) error {
	run, connection, err := w.begin(ctx)
	if err != nil {
		return err
	}
	w.execute(run, connection)
	return run.err
}

// Shutdown stops intake and waits for in-flight work to finish. If ctx expires,
// in-flight execution and completion RPCs are canceled.
func (w *TaskHubGrpcWorker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	run := w.run
	if run == nil {
		w.mu.Unlock()
		return nil
	}
	run.cancelIntake()
	w.mu.Unlock()

	select {
	case <-run.done:
		return run.err
	case <-ctx.Done():
		run.cancelProcessing()
		return ctx.Err()
	}
}

// Wait waits for a worker started with Start to stop.
func (w *TaskHubGrpcWorker) Wait(ctx context.Context) error {
	w.mu.Lock()
	run := w.run
	lastErr := w.lastErr
	w.mu.Unlock()
	if run == nil {
		return lastErr
	}

	select {
	case <-run.done:
		return run.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *TaskHubGrpcWorker) Running() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.run != nil
}

func (w *TaskHubGrpcWorker) begin(ctx context.Context) (*grpcWorkerRun, *grpcWorkerConnection, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	w.mu.Lock()
	if w.run != nil {
		w.mu.Unlock()
		return nil, nil, ErrTaskHubGrpcWorkerAlreadyRunning
	}
	intakeCtx, cancelIntake := context.WithCancel(ctx)
	processingCtx, cancelProcessing := context.WithCancel(context.WithoutCancel(ctx))
	run := &grpcWorkerRun{
		intakeCtx:        intakeCtx,
		cancelIntake:     cancelIntake,
		processingCtx:    processingCtx,
		cancelProcessing: cancelProcessing,
		done:             make(chan struct{}),
		orchestrationSlots: make(
			chan struct{},
			w.options.maxConcurrentOrchestrations,
		),
		activitySlots: make(chan struct{}, w.options.maxConcurrentActivities),
		entitySlots:   make(chan struct{}, w.options.maxConcurrentEntities),
	}
	w.run = run
	w.lastErr = nil
	w.mu.Unlock()

	connection, err := w.connect(run.intakeCtx)
	if err != nil {
		run.err = err
		run.cancelIntake()
		run.cancelProcessing()
		close(run.done)
		w.mu.Lock()
		if w.run == run {
			w.run = nil
			w.lastErr = err
		}
		w.mu.Unlock()
		return nil, nil, err
	}
	return run, connection, nil
}

func (w *TaskHubGrpcWorker) execute(run *grpcWorkerRun, connection *grpcWorkerConnection) {
	defer func() {
		run.cancelIntake()
		run.pending.Wait()
		run.cancelProcessing()
		run.retired.Wait()
		if err := w.executor.Shutdown(context.Background()); err != nil && run.err == nil {
			run.err = fmt.Errorf("failed to shut down worker executor: %w", err)
		}
		if run.err != nil {
			w.logger.Errorf("gRPC worker stopped: %v", run.err)
		}

		w.mu.Lock()
		if w.run == run {
			w.run = nil
			w.lastErr = run.err
		}
		close(run.done)
		w.mu.Unlock()
	}()

	run.err = w.runLoop(run, connection)
}

func (w *TaskHubGrpcWorker) runLoop(run *grpcWorkerRun, connection *grpcWorkerConnection) error {
	reconnectBackoff := newInfiniteBackoff(w.options.reconnectBaseDelay, w.options.reconnectMaxDelay)
	for {
		observedMessage, err := w.consumeConnection(run, connection)
		w.retireConnection(run, connection)

		if run.intakeCtx.Err() != nil {
			return nil
		}
		if !isTransientWorkerError(err) {
			return fmt.Errorf("work item stream stopped with a non-retryable error: %w", err)
		}
		if observedMessage {
			reconnectBackoff.Reset()
		}

		for {
			delay := reconnectBackoff.NextBackOff()
			if err := waitForRetry(run.intakeCtx, delay); err != nil {
				return nil
			}

			next, connectErr := w.connect(run.intakeCtx)
			if connectErr == nil {
				w.logger.Info("reconnected gRPC work item stream")
				connection = next
				break
			}
			if run.intakeCtx.Err() != nil {
				return nil
			}
			if !isTransientWorkerError(connectErr) {
				return fmt.Errorf("failed to reconnect gRPC work item stream: %w", connectErr)
			}
			w.logger.Warnf("transient gRPC worker reconnect failure: %v", connectErr)
		}
	}
}

func (w *TaskHubGrpcWorker) connect(ctx context.Context) (*grpcWorkerConnection, error) {
	client, closer, err := w.clientFactory(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC worker connection: %w", err)
	}
	closeOnError := func() {
		if closer != nil {
			_ = closer.Close()
		}
	}

	helloCtx, cancelHello := context.WithTimeout(ctx, w.options.helloTimeout)
	_, err = client.Hello(helloCtx, &emptypb.Empty{})
	cancelHello()
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("gRPC worker Hello failed: %w", err)
	}

	streamCtx, cancelStream := context.WithCancel(ctx)
	stream, err := client.GetWorkItems(streamCtx, &protos.GetWorkItemsRequest{
		MaxConcurrentOrchestrationWorkItems: int32(w.options.maxConcurrentOrchestrations),
		MaxConcurrentActivityWorkItems:      int32(w.options.maxConcurrentActivities),
		MaxConcurrentEntityWorkItems:        int32(w.options.maxConcurrentEntities),
		Capabilities:                        slices.Clone(w.options.capabilities),
		WorkItemFilters:                     workItemFiltersToProto(w.options.workItemFilters),
	})
	if err != nil {
		cancelStream()
		closeOnError()
		return nil, fmt.Errorf("failed to open gRPC work item stream: %w", err)
	}

	return &grpcWorkerConnection{
		client:       client,
		stream:       stream,
		cancelStream: cancelStream,
		closer:       closer,
	}, nil
}

func setWorkerCapability(capabilities []WorkerCapability, capability WorkerCapability, enabled bool) []WorkerCapability {
	index := slices.Index(capabilities, capability)
	switch {
	case enabled && index < 0:
		return append(capabilities, capability)
	case !enabled && index >= 0:
		return slices.Delete(capabilities, index, index+1)
	default:
		return capabilities
	}
}

func cloneWorkItemFilters(filters *WorkItemFilters) (*WorkItemFilters, error) {
	result := &WorkItemFilters{
		Orchestrations: make([]WorkItemFilter, len(filters.Orchestrations)),
		Activities:     make([]WorkItemFilter, len(filters.Activities)),
		Entities:       slices.Clone(filters.Entities),
	}
	for i, filter := range filters.Orchestrations {
		if filter.Name == "" {
			return nil, errors.New("orchestration filter name cannot be empty")
		}
		result.Orchestrations[i] = WorkItemFilter{Name: filter.Name, Versions: slices.Clone(filter.Versions)}
	}
	for i, filter := range filters.Activities {
		if filter.Name == "" {
			return nil, errors.New("activity filter name cannot be empty")
		}
		result.Activities[i] = WorkItemFilter{Name: filter.Name, Versions: slices.Clone(filter.Versions)}
	}
	for _, name := range result.Entities {
		if strings.TrimSpace(name) == "" {
			return nil, errors.New("entity filter name cannot be empty")
		}
	}
	slices.SortFunc(result.Orchestrations, func(left, right WorkItemFilter) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(result.Activities, func(left, right WorkItemFilter) int {
		return strings.Compare(left.Name, right.Name)
	})
	for i := range result.Orchestrations {
		slices.Sort(result.Orchestrations[i].Versions)
	}
	for i := range result.Activities {
		slices.Sort(result.Activities[i].Versions)
	}
	slices.Sort(result.Entities)
	return result, nil
}

func workItemFiltersToProto(filters *WorkItemFilters) *protos.WorkItemFilters {
	if filters == nil {
		return nil
	}
	result := &protos.WorkItemFilters{
		Orchestrations: make([]*protos.OrchestrationFilter, 0, len(filters.Orchestrations)),
		Activities:     make([]*protos.ActivityFilter, 0, len(filters.Activities)),
		Entities:       make([]*protos.EntityFilter, 0, len(filters.Entities)),
	}
	for _, filter := range filters.Orchestrations {
		result.Orchestrations = append(result.Orchestrations, &protos.OrchestrationFilter{
			Name:     filter.Name,
			Versions: slices.Clone(filter.Versions),
		})
	}
	for _, filter := range filters.Activities {
		result.Activities = append(result.Activities, &protos.ActivityFilter{
			Name:     filter.Name,
			Versions: slices.Clone(filter.Versions),
		})
	}
	for _, name := range filters.Entities {
		result.Entities = append(result.Entities, &protos.EntityFilter{Name: name})
	}
	return result
}

func (w *TaskHubGrpcWorker) retireConnection(run *grpcWorkerRun, connection *grpcWorkerConnection) {
	connection.cancelStream()
	run.retired.Add(1)
	go func() {
		defer run.retired.Done()
		connection.pending.Wait()
		if connection.closer != nil {
			if err := connection.closer.Close(); err != nil {
				w.logger.Warnf("failed to close retired gRPC worker connection: %v", err)
			}
		}
	}()
}

func isTransientWorkerError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, errSilentDisconnect) {
		return true
	}
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return false
	}
	if grpcStatus.Code() == codes.Canceled &&
		strings.Contains(grpcStatus.Message(), "client connection is closing") {
		return false
	}
	return isTransientWorkerGRPCCode(grpcStatus.Code())
}

func isTransientWorkerGRPCCode(code codes.Code) bool {
	switch code {
	case codes.Canceled,
		codes.DeadlineExceeded,
		codes.NotFound,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal,
		codes.Unavailable,
		codes.Unknown:
		return true
	default:
		return false
	}
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// newInfiniteBackoff returns an exponential backoff that never gives up, so the
// caller decides when to stop retrying.
func newInfiniteBackoff(initialInterval, maxInterval time.Duration) *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initialInterval
	b.MaxInterval = maxInterval
	b.MaxElapsedTime = 0
	b.Reset()
	return b
}

func newInfiniteRetries() *backoff.ExponentialBackOff {
	return newInfiniteBackoff(backoff.DefaultInitialInterval, 15*time.Second)
}
