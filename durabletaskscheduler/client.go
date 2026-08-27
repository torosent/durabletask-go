package durabletaskscheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/backend"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client owns the management connection it creates.
type Client struct {
	*durabletaskclient.TaskHubGrpcClient

	connection *grpc.ClientConn
	closeOnce  sync.Once
	closeErr   error
}

// NewClient creates an independently owned management connection and validates
// it with a deadline-bound Hello call.
func NewClient(ctx context.Context, options *Options, logger backend.Logger) (*Client, error) {
	prepared, err := prepareOptions(options)
	if err != nil {
		return nil, err
	}
	connection, err := connect(&prepared, clientRole, "")
	if err != nil {
		return nil, err
	}

	helloCtx, cancel := context.WithTimeout(ctx, prepared.HelloTimeout)
	_, err = protos.NewTaskHubSidecarServiceClient(connection).Hello(helloCtx, &emptypb.Empty{})
	cancel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("DTS client Hello failed: %w", err)
	}
	clientOptions := []durabletaskclient.TaskHubGrpcClientOption{
		durabletaskclient.WithLargePayloads(prepared.LargePayloads),
		durabletaskclient.WithDataConverter(prepared.DataConverter),
	}
	if prepared.Versioning != nil && prepared.Versioning.DefaultVersion != "" {
		clientOptions = append(clientOptions, durabletaskclient.WithDefaultVersion(prepared.Versioning.DefaultVersion))
	}
	return &Client{
		TaskHubGrpcClient: durabletaskclient.NewTaskHubGrpcClient(connection, logger, clientOptions...),
		connection:        connection,
	}, nil
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		stopErr := c.StopWorkItemListener(shutdownCtx)
		cancel()
		c.closeErr = errors.Join(stopErr, c.connection.Close())
	})
	return c.closeErr
}
