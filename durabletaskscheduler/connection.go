package durabletaskscheduler

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const modulePath = "github.com/microsoft/durabletask-go"

const retryServiceConfig = `{
  "methodConfig": [{
    "name": [{}],
    "retryPolicy": {
      "maxAttempts": 5,
      "initialBackoff": "0.050s",
      "maxBackoff": "0.250s",
      "backoffMultiplier": 2,
      "retryableStatusCodes": ["UNAVAILABLE"]
    }
  }]
}`

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
			return nil, status.Errorf(codes.Unavailable, "failed to acquire DTS access token: %v", err)
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
	case AuthenticationManagedIdentity:
		credentialOptions := &azidentity.ManagedIdentityCredentialOptions{}
		if clientID := strings.TrimSpace(options.ClientID); clientID != "" {
			credentialOptions.ID = azidentity.ClientID(clientID)
		}
		credential, err := azidentity.NewManagedIdentityCredential(credentialOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create ManagedIdentityCredential: %w", err)
		}
		return credential, nil
	case AuthenticationWorkloadIdentity:
		credential, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID:                   options.ClientID,
			TenantID:                   options.TenantID,
			TokenFilePath:              options.TokenFilePath,
			AdditionallyAllowedTenants: append([]string(nil), options.AdditionallyAllowedTenants...),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create WorkloadIdentityCredential: %w", err)
		}
		return credential, nil
	case AuthenticationEnvironment:
		credential, err := azidentity.NewEnvironmentCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create EnvironmentCredential: %w", err)
		}
		return credential, nil
	case AuthenticationAzureCLI:
		credential, err := azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{
			TenantID:                   options.TenantID,
			AdditionallyAllowedTenants: append([]string(nil), options.AdditionallyAllowedTenants...),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create AzureCLICredential: %w", err)
		}
		return credential, nil
	case AuthenticationAzurePowerShell:
		credential, err := azidentity.NewAzurePowerShellCredential(&azidentity.AzurePowerShellCredentialOptions{
			TenantID:                   options.TenantID,
			AdditionallyAllowedTenants: append([]string(nil), options.AdditionallyAllowedTenants...),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create AzurePowerShellCredential: %w", err)
		}
		return credential, nil
	case AuthenticationInteractiveBrowser:
		credential, err := azidentity.NewInteractiveBrowserCredential(&azidentity.InteractiveBrowserCredentialOptions{
			ClientID:                   options.ClientID,
			TenantID:                   options.TenantID,
			AdditionallyAllowedTenants: append([]string(nil), options.AdditionallyAllowedTenants...),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create InteractiveBrowserCredential: %w", err)
		}
		return credential, nil
	default:
		return nil, fmt.Errorf("unsupported DTS authentication type %q", options.Authentication)
	}
}

func prepareOptions(options *Options) (Options, error) {
	if options == nil {
		return Options{}, fmt.Errorf("DTS options are required")
	}
	prepared := *options
	prepared.AdditionallyAllowedTenants = slices.Clone(options.AdditionallyAllowedTenants)
	if prepared.Authentication == "" {
		prepared.Authentication = AuthenticationDefaultAzure
	}
	if prepared.ResourceID == "" {
		prepared.ResourceID = DefaultResourceID
	}
	if prepared.HelloTimeout == 0 {
		prepared.HelloTimeout = 30 * time.Second
	}
	if err := prepared.Validate(); err != nil {
		return Options{}, err
	}
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
	options *Options,
	role connectionRole,
	workerID string,
) (*grpc.ClientConn, error) {
	endpoint, err := normalizeEndpoint(options.EndpointAddress)
	if err != nil {
		return nil, err
	}
	credential := options.Credential

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
	if role == clientRole {
		dialOptions = append(dialOptions, grpc.WithDefaultServiceConfig(retryServiceConfig))
	}
	if options.dialer != nil {
		dialOptions = append(dialOptions, grpc.WithContextDialer(options.dialer))
	}

	target := "dns:///" + host
	if options.dialer != nil {
		target = "passthrough:///" + host
	}
	connection, err := grpc.NewClient(target, dialOptions...)
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
