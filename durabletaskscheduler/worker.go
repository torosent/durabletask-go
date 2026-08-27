package durabletaskscheduler

import (
	"context"
	"io"

	"github.com/microsoft/durabletask-go/backend"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
)

// NewWorker creates a DTS worker that owns and recreates its gRPC connections.
// The returned worker connects and performs its fail-fast Hello in Start or Run.
func NewWorker(
	options *Options,
	registry *task.TaskRegistry,
	logger backend.Logger,
	workerOptions ...durabletaskclient.TaskHubGrpcWorkerOption,
) (*durabletaskclient.TaskHubGrpcWorker, error) {
	prepared, err := prepareOptions(options)
	if err != nil {
		return nil, err
	}
	workerID := prepared.WorkerID
	if workerID == "" {
		workerID = defaultWorkerID()
	}

	factory := func(ctx context.Context) (grpc.ClientConnInterface, io.Closer, error) {
		connection, err := connect(&prepared, workerRole, workerID)
		if err != nil {
			return nil, nil, err
		}
		return connection, connection, nil
	}
	configuredOptions := []durabletaskclient.TaskHubGrpcWorkerOption{
		durabletaskclient.WithWorkerHelloTimeout(prepared.HelloTimeout),
	}
	configuredOptions = append(configuredOptions, workerOptions...)
	return durabletaskclient.NewTaskHubGrpcWorkerWithConnectionFactory(
		factory,
		registry,
		logger,
		configuredOptions...,
	)
}
