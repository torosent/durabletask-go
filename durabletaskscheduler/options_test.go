package durabletaskscheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewOptionsFromConnectionString(t *testing.T) {
	tests := []struct {
		name          string
		value         string
		wantAuth      AuthenticationType
		wantUnsafe    bool
		wantClientID  string
		wantTenantID  string
		wantTokenFile string
		wantTenants   []string
		wantErr       string
	}{
		{
			name:     "default azure",
			value:    "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=DefaultAzure",
			wantAuth: AuthenticationDefaultAzure,
		},
		{
			name:       "emulator",
			value:      "Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None",
			wantAuth:   AuthenticationNone,
			wantUnsafe: true,
		},
		{
			name:         "managed identity",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=ManagedIdentity;ClientID=client",
			wantAuth:     AuthenticationManagedIdentity,
			wantClientID: "client",
		},
		{
			name:          "workload identity",
			value:         "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=WorkloadIdentity;ClientID=client;TenantID=tenant;TokenFilePath=/token;AdditionallyAllowedTenants=one, two",
			wantAuth:      AuthenticationWorkloadIdentity,
			wantClientID:  "client",
			wantTenantID:  "tenant",
			wantTokenFile: "/token",
			wantTenants:   []string{"one", "two"},
		},
		{
			name:         "Azure CLI",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=AzureCLI;TenantID=tenant",
			wantAuth:     AuthenticationAzureCLI,
			wantTenantID: "tenant",
		},
		{
			name:    "unsupported Visual Studio",
			value:   "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=VisualStudio",
			wantErr: "no Azure Identity for Go equivalent",
		},
		{
			name:    "missing authentication",
			value:   "Endpoint=https://scheduler.example.com;TaskHub=hub",
			wantErr: "Authentication",
		},
		{
			name:    "unknown key",
			value:   "Endpoint=https://scheduler.example.com;TaskHub=hub;Authentication=None;Typo=value",
			wantErr: "unsupported",
		},
		{
			name:    "unsupported authentication",
			value:   "Endpoint=https://scheduler.example.com;TaskHub=hub;Authentication=Password",
			wantErr: "unsupported Authentication",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options, err := NewOptionsFromConnectionString(tt.value)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantAuth, options.Authentication)
			require.Equal(t, tt.wantUnsafe, options.AllowInsecureConnection)
			require.Equal(t, DefaultResourceID, options.ResourceID)
			require.Equal(t, tt.wantClientID, options.ClientID)
			require.Equal(t, tt.wantTenantID, options.TenantID)
			require.Equal(t, tt.wantTokenFile, options.TokenFilePath)
			require.Equal(t, tt.wantTenants, options.AdditionallyAllowedTenants)
		})
	}
}

func TestOptionsValidatePlaintextGuard(t *testing.T) {
	options := NewOptions("http://127.0.0.1:8080", "default")
	options.AllowInsecureConnection = true
	require.ErrorContains(t, options.Validate(), "cannot be used with credentials")

	options.Authentication = AuthenticationNone
	options.AllowInsecureConnection = false
	require.ErrorContains(t, options.Validate(), "AllowInsecureConnection")

	options.AllowInsecureConnection = true
	require.NoError(t, options.Validate())
}

func TestOptionsValidateEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		wantErr  string
	}{
		{endpoint: "scheduler.example.com"},
		{endpoint: "https://scheduler.example.com:443"},
		{endpoint: "ftp://scheduler.example.com", wantErr: "scheme"},
		{endpoint: "https://scheduler.example.com/path", wantErr: "path"},
		{endpoint: "https://user@scheduler.example.com", wantErr: "user info"},
	}

	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			options := NewOptions(tt.endpoint, "hub")
			err := options.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestPrepareOptionsAppliesDefaultsAndCopiesTenants(t *testing.T) {
	options := &Options{
		EndpointAddress:            "scheduler.example.com",
		TaskHubName:                "hub",
		Authentication:             AuthenticationDefaultAzure,
		AdditionallyAllowedTenants: []string{"tenant"},
	}
	prepared, err := prepareOptions(options)
	require.NoError(t, err)
	require.Equal(t, DefaultResourceID, prepared.ResourceID)
	require.Equal(t, 30*time.Second, prepared.HelloTimeout)
	options.AdditionallyAllowedTenants[0] = "changed"
	require.Equal(t, []string{"tenant"}, prepared.AdditionallyAllowedTenants)
}

func TestOptionsValidateRejectsInvalidUserAgent(t *testing.T) {
	options := NewOptions("scheduler.example.com", "hub")
	options.UserAgent = "agent\r\ninjected"
	require.ErrorContains(t, options.Validate(), "user agent")
}
