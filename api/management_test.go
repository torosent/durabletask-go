package api

import (
	"context"
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
)

func TestWithTagsMergesAndRejectsReservedKeys(t *testing.T) {
	req := &protos.CreateInstanceRequest{Tags: map[string]string{"existing": "value"}}
	require.NoError(t, WithTags(map[string]string{"team": "durable"})(req))
	require.Equal(t, map[string]string{"existing": "value", "team": "durable"}, req.Tags)
	require.Error(t, WithTags(map[string]string{ReservedContextFieldPrefix + "name": "invalid"})(req))
}

func TestNormalizeLargePayloadOptions(t *testing.T) {
	store := testPayloadStore{}
	normalized, err := NormalizeLargePayloadOptions(&LargePayloadOptions{
		Store:    store,
		Resolver: store,
	})
	require.NoError(t, err)
	require.Equal(t, DefaultLargePayloadThresholdBytes, normalized.ThresholdBytes)
	require.Equal(t, DefaultLargePayloadMaxBytes, normalized.MaxPayloadBytes)
}

type testPayloadStore struct{}

func (testPayloadStore) Store(context.Context, []byte) (string, error) {
	return "test", nil
}

func (testPayloadStore) Resolve(context.Context, string) ([]byte, error) {
	return nil, nil
}
