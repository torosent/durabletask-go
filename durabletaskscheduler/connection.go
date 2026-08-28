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
	"github.com/microsoft/durabletask-go/task"
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

// credentialSpec is the normalized set of Azure Identity inputs a single DTS
// authentication mode can actually use. Fields the selected mode cannot use are
// left empty so credential construction is deterministic and reviewable.
type credentialSpec struct {
	authentication             AuthenticationType
	clientID                   string
	tenantID                   string
	tokenFilePath              string
	additionallyAllowedTenants []string
}

// credentialFactory builds the Azure Identity credential for a resolved spec.
// Production code always uses newAzureIdentityCredential; it is threaded as a
// parameter rather than a package variable so tests can exercise credential
// selection without contacting Azure and without mutating process-global state.
type credentialFactory func(credentialSpec) (azcore.TokenCredential, error)

// newCredentialSpec maps options onto the fields the selected authentication
// mode supports in Azure Identity for Go. Values are trimmed, and fields a mode
// cannot consume are dropped rather than forwarded.
func newCredentialSpec(options *Options) (credentialSpec, error) {
	clientID := strings.TrimSpace(options.ClientID)
	tenantID := strings.TrimSpace(options.TenantID)
	tokenFilePath := strings.TrimSpace(options.TokenFilePath)
	additionalTenants := normalizeAdditionallyAllowedTenants(options.AdditionallyAllowedTenants)

	spec := credentialSpec{authentication: options.Authentication}
	switch options.Authentication {
	case AuthenticationNone, AuthenticationTokenCredential:
		// No Azure Identity credential is constructed for these modes.
	case AuthenticationDefaultAzure:
		spec.tenantID = tenantID
		spec.additionallyAllowedTenants = additionalTenants
	case AuthenticationManagedIdentity:
		spec.clientID = clientID
	case AuthenticationWorkloadIdentity:
		spec.clientID = clientID
		spec.tenantID = tenantID
		spec.tokenFilePath = tokenFilePath
		spec.additionallyAllowedTenants = additionalTenants
	case AuthenticationEnvironment:
		// EnvironmentCredential is configured entirely by environment variables.
	case AuthenticationAzureCLI, AuthenticationAzurePowerShell:
		spec.tenantID = tenantID
		spec.additionallyAllowedTenants = additionalTenants
	case AuthenticationInteractiveBrowser:
		spec.clientID = clientID
		spec.tenantID = tenantID
		spec.additionallyAllowedTenants = additionalTenants
	default:
		return credentialSpec{}, fmt.Errorf("unsupported DTS authentication type %q", options.Authentication)
	}
	return spec, nil
}

func normalizeAdditionallyAllowedTenants(tenants []string) []string {
	var normalized []string
	for _, tenant := range tenants {
		if tenant = strings.TrimSpace(tenant); tenant != "" {
			normalized = append(normalized, tenant)
		}
	}
	return normalized
}

func newAzureIdentityCredential(spec credentialSpec) (azcore.TokenCredential, error) {
	switch spec.authentication {
	case AuthenticationDefaultAzure:
		credential, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
			TenantID:                   spec.tenantID,
			AdditionallyAllowedTenants: spec.additionallyAllowedTenants,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create DefaultAzureCredential: %w", err)
		}
		return credential, nil
	case AuthenticationManagedIdentity:
		credentialOptions := &azidentity.ManagedIdentityCredentialOptions{}
		if spec.clientID != "" {
			credentialOptions.ID = azidentity.ClientID(spec.clientID)
		}
		credential, err := azidentity.NewManagedIdentityCredential(credentialOptions)
		if err != nil {
			return nil, fmt.Errorf("failed to create ManagedIdentityCredential: %w", err)
		}
		return credential, nil
	case AuthenticationWorkloadIdentity:
		credential, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
			ClientID:                   spec.clientID,
			TenantID:                   spec.tenantID,
			TokenFilePath:              spec.tokenFilePath,
			AdditionallyAllowedTenants: spec.additionallyAllowedTenants,
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
			TenantID:                   spec.tenantID,
			AdditionallyAllowedTenants: spec.additionallyAllowedTenants,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create AzureCLICredential: %w", err)
		}
		return credential, nil
	case AuthenticationAzurePowerShell:
		credential, err := azidentity.NewAzurePowerShellCredential(&azidentity.AzurePowerShellCredentialOptions{
			TenantID:                   spec.tenantID,
			AdditionallyAllowedTenants: spec.additionallyAllowedTenants,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create AzurePowerShellCredential: %w", err)
		}
		return credential, nil
	case AuthenticationInteractiveBrowser:
		credential, err := azidentity.NewInteractiveBrowserCredential(&azidentity.InteractiveBrowserCredentialOptions{
			ClientID:                   spec.clientID,
			TenantID:                   spec.tenantID,
			AdditionallyAllowedTenants: spec.additionallyAllowedTenants,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create InteractiveBrowserCredential: %w", err)
		}
		return credential, nil
	default:
		return nil, fmt.Errorf("unsupported DTS authentication type %q", spec.authentication)
	}
}

func resolveCredential(options *Options, factory credentialFactory) (azcore.TokenCredential, error) {
	switch options.Authentication {
	case AuthenticationNone:
		return nil, nil
	case AuthenticationTokenCredential:
		return options.Credential, nil
	}
	spec, err := newCredentialSpec(options)
	if err != nil {
		return nil, err
	}
	return factory(spec)
}

// tokenScope builds the OAuth scope requested for a DTS resource ID.
func tokenScope(resourceID string) string {
	return strings.TrimRight(strings.TrimSpace(resourceID), "/") + "/.default"
}

func prepareOptions(options *Options) (Options, error) {
	return prepareOptionsWith(options, newAzureIdentityCredential)
}

// prepareOptionsWith normalizes, validates, and resolves the credential for a
// caller-supplied option set. The credential factory is a parameter so tests can
// cover every authentication mode offline.
func prepareOptionsWith(options *Options, factory credentialFactory) (Options, error) {
	if options == nil {
		return Options{}, fmt.Errorf("DTS options are required")
	}
	prepared := *options
	prepared.AdditionallyAllowedTenants = slices.Clone(options.AdditionallyAllowedTenants)
	prepared.UnaryInterceptors = slices.Clone(options.UnaryInterceptors)
	prepared.StreamInterceptors = slices.Clone(options.StreamInterceptors)
	if options.Versioning != nil {
		versioning := *options.Versioning
		prepared.Versioning = &versioning
	}
	if prepared.Authentication == "" {
		prepared.Authentication = AuthenticationDefaultAzure
	}
	// Only an exactly empty resource ID defaults. A whitespace-only value is
	// left intact so Validate below rejects it as documented instead of
	// silently collapsing to the default resource.
	if trimmed := strings.TrimSpace(prepared.ResourceID); trimmed != "" {
		prepared.ResourceID = trimmed
	} else if prepared.ResourceID == "" {
		prepared.ResourceID = DefaultResourceID
	}
	if prepared.HelloTimeout == 0 {
		prepared.HelloTimeout = 30 * time.Second
	}
	if prepared.MaximumTimerInterval == 0 {
		prepared.MaximumTimerInterval = task.DefaultMaximumTimerInterval
	}
	if err := prepared.Validate(); err != nil {
		return Options{}, err
	}
	credential, err := resolveCredential(&prepared, factory)
	if err != nil {
		return Options{}, err
	}
	if credential != nil {
		prepared.Authentication = AuthenticationTokenCredential
		prepared.Credential = credential
	}
	return prepared, nil
}

// newPerRPCCredentials builds the DTS metadata carried on every RPC for a role.
func newPerRPCCredentials(
	options *Options,
	role connectionRole,
	workerID string,
) *schedulerPerRPCCredentials {
	userAgent := options.UserAgent
	if userAgent == "" {
		userAgent = fmt.Sprintf("durabletask-go/%s (%s)", sdkVersion(), role)
	}
	return &schedulerPerRPCCredentials{
		credential: options.Credential,
		scope:      tokenScope(options.ResourceID),
		taskHub:    options.TaskHubName,
		userAgent:  userAgent,
		workerID:   workerID,
	}
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

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithPerRPCCredentials(newPerRPCCredentials(options, role, workerID)),
	}
	if len(options.UnaryInterceptors) > 0 {
		dialOptions = append(dialOptions, grpc.WithChainUnaryInterceptor(options.UnaryInterceptors...))
	}
	if len(options.StreamInterceptors) > 0 {
		dialOptions = append(dialOptions, grpc.WithChainStreamInterceptor(options.StreamInterceptors...))
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
