package durabletaskscheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOptionsFromConnectionString(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		wantAuth   AuthenticationType
		wantUnsafe bool
		wantErr    string
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
