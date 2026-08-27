// Package durabletaskscheduler configures Durable Task Scheduler (DTS)
// management clients and workers.
package durabletaskscheduler

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

const DefaultResourceID = "https://durabletask.io"

type AuthenticationType string

const (
	AuthenticationNone            AuthenticationType = "None"
	AuthenticationDefaultAzure    AuthenticationType = "DefaultAzure"
	AuthenticationTokenCredential AuthenticationType = "TokenCredential"
)

// Options configures connections to a Durable Task Scheduler endpoint.
type Options struct {
	EndpointAddress string
	TaskHubName     string
	Authentication  AuthenticationType
	Credential      azcore.TokenCredential
	ResourceID      string

	// AllowInsecureConnection must be true to use an http:// endpoint. Plaintext
	// connections are supported only when Authentication is None.
	AllowInsecureConnection bool

	// WorkerID overrides the generated worker identity. It is ignored by clients.
	WorkerID string

	// UserAgent overrides the x-user-agent metadata value.
	UserAgent string

	// HelloTimeout controls fail-fast connectivity checks.
	HelloTimeout time.Duration

	dialer func(context.Context, string) (net.Conn, error)
}

func NewOptions(endpointAddress, taskHubName string) *Options {
	return &Options{
		EndpointAddress: endpointAddress,
		TaskHubName:     taskHubName,
		Authentication:  AuthenticationDefaultAzure,
		ResourceID:      DefaultResourceID,
		HelloTimeout:    30 * time.Second,
	}
}

func NewOptionsWithCredential(endpointAddress, taskHubName string, credential azcore.TokenCredential) *Options {
	options := NewOptions(endpointAddress, taskHubName)
	options.Authentication = AuthenticationTokenCredential
	options.Credential = credential
	return options
}

// NewOptionsFromConnectionString parses the DTS connection-string form:
//
//	Endpoint=<url>;TaskHub=<name>;Authentication=DefaultAzure|None
func NewOptionsFromConnectionString(connectionString string) (*Options, error) {
	values := make(map[string]string)
	for _, segment := range strings.Split(connectionString, ";") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		keyValue := strings.SplitN(segment, "=", 2)
		if len(keyValue) != 2 {
			return nil, fmt.Errorf("invalid connection string segment %q", segment)
		}
		key := strings.ToLower(strings.TrimSpace(keyValue[0]))
		value := strings.TrimSpace(keyValue[1])
		switch key {
		case "endpoint", "taskhub", "authentication":
			values[key] = value
		default:
			return nil, fmt.Errorf("unsupported connection string key %q", keyValue[0])
		}
	}

	endpoint, ok := values["endpoint"]
	if !ok || endpoint == "" {
		return nil, fmt.Errorf("connection string is missing required Endpoint")
	}
	taskHub, ok := values["taskhub"]
	if !ok || taskHub == "" {
		return nil, fmt.Errorf("connection string is missing required TaskHub")
	}
	authentication, ok := values["authentication"]
	if !ok || authentication == "" {
		return nil, fmt.Errorf("connection string is missing required Authentication")
	}

	options := NewOptions(endpoint, taskHub)
	switch {
	case strings.EqualFold(authentication, string(AuthenticationDefaultAzure)):
		options.Authentication = AuthenticationDefaultAzure
	case strings.EqualFold(authentication, string(AuthenticationNone)):
		options.Authentication = AuthenticationNone
		options.AllowInsecureConnection = true
	default:
		return nil, fmt.Errorf("unsupported Authentication value %q", authentication)
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return options, nil
}

func (o *Options) Validate() error {
	if o == nil {
		return fmt.Errorf("DTS options are required")
	}
	if strings.TrimSpace(o.TaskHubName) == "" {
		return fmt.Errorf("DTS task hub name is required")
	}
	if o.TaskHubName != strings.TrimSpace(o.TaskHubName) {
		return fmt.Errorf("DTS task hub name cannot have leading or trailing whitespace")
	}
	if strings.ContainsAny(o.TaskHubName, "\r\n") {
		return fmt.Errorf("DTS task hub name cannot contain newlines")
	}
	if o.WorkerID != strings.TrimSpace(o.WorkerID) || strings.ContainsAny(o.WorkerID, "\r\n") {
		return fmt.Errorf("DTS worker ID cannot contain leading/trailing whitespace or newlines")
	}
	if o.ResourceID == "" {
		return fmt.Errorf("DTS resource ID is required")
	}
	if o.HelloTimeout <= 0 {
		return fmt.Errorf("DTS Hello timeout must be greater than zero")
	}

	endpoint, err := normalizeEndpoint(o.EndpointAddress)
	if err != nil {
		return err
	}
	switch o.Authentication {
	case AuthenticationNone:
		if o.Credential != nil {
			return fmt.Errorf("DTS credential must be nil when Authentication is None")
		}
	case AuthenticationDefaultAzure:
		if o.Credential != nil {
			return fmt.Errorf("use Authentication TokenCredential for an explicit DTS credential")
		}
	case AuthenticationTokenCredential:
		if o.Credential == nil {
			return fmt.Errorf("DTS TokenCredential authentication requires a credential")
		}
	default:
		return fmt.Errorf("unsupported DTS authentication type %q", o.Authentication)
	}
	if endpoint.Scheme == "http" {
		if !o.AllowInsecureConnection {
			return fmt.Errorf("plaintext DTS endpoint requires AllowInsecureConnection")
		}
		if o.Authentication != AuthenticationNone {
			return fmt.Errorf("plaintext DTS endpoint cannot be used with credentials")
		}
	}
	return nil
}

func normalizeEndpoint(endpointAddress string) (*url.URL, error) {
	endpointAddress = strings.TrimSpace(endpointAddress)
	if endpointAddress == "" {
		return nil, fmt.Errorf("DTS endpoint is required")
	}
	if !strings.Contains(endpointAddress, "://") {
		endpointAddress = "https://" + endpointAddress
	}
	endpoint, err := url.Parse(endpointAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid DTS endpoint: %w", err)
	}
	if endpoint.Scheme != "https" && endpoint.Scheme != "http" {
		return nil, fmt.Errorf("DTS endpoint scheme must be https or http")
	}
	if endpoint.Hostname() == "" {
		return nil, fmt.Errorf("DTS endpoint must include a host")
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, fmt.Errorf("DTS endpoint cannot include user info, query parameters, or a fragment")
	}
	if endpoint.Path != "" && endpoint.Path != "/" {
		return nil, fmt.Errorf("DTS endpoint cannot include a path")
	}
	return endpoint, nil
}
