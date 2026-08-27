package payload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/microsoft/durabletask-go/api"
)

type MemoryStore struct {
	mu       sync.RWMutex
	payloads map[string][]byte
	maxBytes int
}

func NewMemoryStore(maxBytes ...int) *MemoryStore {
	limit := api.DefaultLargePayloadMaxBytes
	if len(maxBytes) > 0 && maxBytes[0] > 0 {
		limit = maxBytes[0]
	}
	return &MemoryStore{
		payloads: make(map[string][]byte),
		maxBytes: limit,
	}
}

func (s *MemoryStore) Store(ctx context.Context, payload []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(payload) > s.maxBytes {
		return "", fmt.Errorf("%w: %d bytes exceeds %d", api.ErrLargePayloadTooLarge, len(payload), s.maxBytes)
	}
	digest := sha256.Sum256(payload)
	location := "memory://sha256/" + hex.EncodeToString(digest[:])
	s.mu.Lock()
	s.payloads[location] = append([]byte(nil), payload...)
	s.mu.Unlock()
	return location, nil
}

func (s *MemoryStore) Resolve(ctx context.Context, location string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(location, "memory://sha256/") {
		return nil, errors.New("invalid memory payload location")
	}
	s.mu.RLock()
	payload, ok := s.payloads[location]
	s.mu.RUnlock()
	if !ok {
		return nil, errors.New("large payload was not found")
	}
	if len(payload) > s.maxBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", api.ErrLargePayloadTooLarge, len(payload), s.maxBytes)
	}
	return append([]byte(nil), payload...), nil
}
