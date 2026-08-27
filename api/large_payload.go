package api

import (
	"context"
	"errors"
	"fmt"
)

const (
	DefaultLargePayloadThresholdBytes = 64 * 1024
	DefaultLargePayloadMaxBytes       = 64 * 1024 * 1024
)

var (
	ErrLargePayloadTooLarge  = errors.New("large payload exceeds the configured size limit")
	ErrLargePayloadReference = errors.New("invalid large payload reference")
	ErrLargePayloadIntegrity = errors.New("large payload integrity check failed")
)

// LargePayloadStore externalizes payload bytes and returns an opaque location.
type LargePayloadStore interface {
	Store(context.Context, []byte) (string, error)
}

// LargePayloadResolver resolves payload bytes from an opaque, untrusted location.
// Implementations must validate schemes, accounts, paths, and other allow-list
// constraints before reading data.
type LargePayloadResolver interface {
	Resolve(context.Context, string) ([]byte, error)
}

// LargePayloadOptions configures payload externalization and hydration.
type LargePayloadOptions struct {
	Store    LargePayloadStore
	Resolver LargePayloadResolver
	// ThresholdBytes uses DefaultLargePayloadThresholdBytes when zero.
	ThresholdBytes int
	// MaxPayloadBytes uses DefaultLargePayloadMaxBytes when zero.
	MaxPayloadBytes int
}

func NormalizeLargePayloadOptions(options *LargePayloadOptions) (*LargePayloadOptions, error) {
	if options == nil {
		return nil, nil
	}
	normalized := *options
	if normalized.Store == nil {
		return nil, errors.New("large payload store is required")
	}
	if normalized.Resolver == nil {
		return nil, errors.New("large payload resolver is required")
	}
	if normalized.ThresholdBytes == 0 {
		normalized.ThresholdBytes = DefaultLargePayloadThresholdBytes
	}
	if normalized.MaxPayloadBytes == 0 {
		normalized.MaxPayloadBytes = DefaultLargePayloadMaxBytes
	}
	if normalized.ThresholdBytes < 0 {
		return nil, errors.New("large payload threshold cannot be negative")
	}
	if normalized.MaxPayloadBytes <= 0 {
		return nil, errors.New("large payload maximum must be greater than zero")
	}
	if normalized.ThresholdBytes > normalized.MaxPayloadBytes {
		return nil, fmt.Errorf("large payload threshold cannot exceed maximum payload size")
	}
	return &normalized, nil
}
