package durabletaskscheduler

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const modulePath = "github.com/microsoft/durabletask-go"

type connectionRole string

const (
	clientRole connectionRole = "DurableTaskClient"
	workerRole connectionRole = "DurableTaskWorker"
)

type schedulerPerRPCCredentials struct {
	credential azcore.TokenCredential
	scope      string
	taskHub    string
	userAgent  string
	workerID   string
}

func (c *schedulerPerRPCCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	metadata := map[string]string{
		"taskhub":      c.taskHub,
		"x-user-agent": c.userAgent,
	}
	if c.workerID != "" {
		metadata["workerid"] = c.workerID
	}
	if c.credential != nil {
		token, err := c.credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{c.scope}})
		if err != nil {
			return nil, fmt.Errorf("failed to acquire DTS access token: %w", err)
		}
		metadata["authorization"] = "Bearer " + token.Token
	}
	return metadata, nil
}

func (c *schedulerPerRPCCredentials) RequireTransportSecurity() bool {
	return c.credential != nil
}

func resolveCredential(options *Options) (azcore.TokenCredential, error) {
	switch options.Authentication {
	case AuthenticationNone:
		return nil, nil
	case AuthenticationTokenCredential:
		return options.Credential, nil
	case AuthenticationDefaultAzure:
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create DefaultAzureCredential: %w", err)
		}
		return credential, nil
	default:
		return nil, fmt.Errorf("unsupported DTS authentication type %q", options.Authentication)
	}
}

func prepareOptions(options *Options) (Options, error) {
	if err := options.Validate(); err != nil {
		return Options{}, err
	}
	prepared := *options
	credential, err := resolveCredential(&prepared)
	if err != nil {
		return Options{}, err
	}
	if credential != nil {
		prepared.Authentication = AuthenticationTokenCredential
		prepared.Credential = credential
	}
	return prepared, nil
}

func connect(
	ctx context.Context,
	options *Options,
	role connectionRole,
	workerID string,
) (*grpc.ClientConn, error) {
	endpoint, err := normalizeEndpoint(options.EndpointAddress)
	if err != nil {
		return nil, err
	}
	credential, err := resolveCredential(options)
	if err != nil {
		return nil, err
	}

	host := endpoint.Host
	if endpoint.Port() == "" {
		port := "443"
		if endpoint.Scheme == "http" {
			port = "80"
		}
		host = net.JoinHostPort(endpoint.Hostname(), port)
	}

	var transportCredentials credentials.TransportCredentials
	if endpoint.Scheme == "https" {
		transportCredentials = credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: endpoint.Hostname(),
		})
	} else {
		transportCredentials = insecure.NewCredentials()
	}

	userAgent := options.UserAgent
	if userAgent == "" {
		userAgent = fmt.Sprintf("durabletask-go/%s (%s)", sdkVersion(), role)
	}
	perRPCCredentials := &schedulerPerRPCCredentials{
		credential: credential,
		scope:      strings.TrimSuffix(options.ResourceID, "/") + "/.default",
		taskHub:    options.TaskHubName,
		userAgent:  userAgent,
		workerID:   workerID,
	}
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithPerRPCCredentials(perRPCCredentials),
	}
	if options.dialer != nil {
		dialOptions = append(dialOptions, grpc.WithContextDialer(options.dialer))
	}

	connection, err := grpc.DialContext(ctx, host, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create DTS gRPC connection: %w", err)
	}
	return connection, nil
}

func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s,%d,%s", hostname, os.Getpid(), strings.ReplaceAll(uuid.NewString(), "-", ""))
}

func sdkVersion() string {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	if buildInfo.Main.Path == modulePath && buildInfo.Main.Version != "" && buildInfo.Main.Version != "(devel)" {
		return strings.TrimPrefix(buildInfo.Main.Version, "v")
	}
	for _, dependency := range buildInfo.Deps {
		if dependency.Path == modulePath && dependency.Version != "" {
			return strings.TrimPrefix(dependency.Version, "v")
		}
	}
	return "dev"
}
