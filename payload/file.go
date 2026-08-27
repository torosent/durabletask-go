package payload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/microsoft/durabletask-go/api"
)

type FileStore struct {
	root     string
	maxBytes int64
}

func NewFileStore(root string, maxBytes ...int64) (*FileStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("file payload store root is required")
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve payload store root: %w", err)
	}
	if err := os.MkdirAll(absoluteRoot, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create payload store root: %w", err)
	}
	limit := int64(api.DefaultLargePayloadMaxBytes)
	if len(maxBytes) > 0 && maxBytes[0] > 0 {
		limit = maxBytes[0]
	}
	return &FileStore{root: absoluteRoot, maxBytes: limit}, nil
}

func (s *FileStore) Store(ctx context.Context, payload []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if int64(len(payload)) > s.maxBytes {
		return "", fmt.Errorf("%w: %d bytes exceeds %d", api.ErrLargePayloadTooLarge, len(payload), s.maxBytes)
	}
	digest := sha256.Sum256(payload)
	hash := hex.EncodeToString(digest[:])
	location := "file://sha256/" + hash
	path := filepath.Join(s.root, hash+".payload")
	if _, err := os.Stat(path); err == nil {
		return location, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("failed to inspect payload file: %w", err)
	}

	temp, err := os.CreateTemp(s.root, ".payload-*")
	if err != nil {
		return "", fmt.Errorf("failed to create payload file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath) //nolint:errcheck // best-effort cleanup after rename or failure
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("failed to secure payload file: %w", err)
	}
	if _, err := temp.Write(payload); err != nil {
		_ = temp.Close()
		return "", fmt.Errorf("failed to write payload file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("failed to close payload file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := os.Rename(tempPath, path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			return location, nil
		}
		return "", fmt.Errorf("failed to publish payload file: %w", err)
	}
	return location, nil
}

func (s *FileStore) Resolve(ctx context.Context, location string) ([]byte, error) {
	hash, err := parseFileLocation(location)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(s.root, hash+".payload"))
	if err != nil {
		return nil, fmt.Errorf("failed to open payload file: %w", err)
	}
	defer file.Close() //nolint:errcheck // read-only file cleanup
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to inspect payload file: %w", err)
	}
	if info.Size() > s.maxBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", api.ErrLargePayloadTooLarge, info.Size(), s.maxBytes)
	}
	payload := make([]byte, info.Size())
	if _, err := io.ReadFull(&contextReader{ctx: ctx, reader: file}, payload); err != nil {
		return nil, fmt.Errorf("failed to read payload file: %w", err)
	}
	return payload, nil
}

func parseFileLocation(location string) (string, error) {
	const prefix = "file://sha256/"
	if !strings.HasPrefix(location, prefix) {
		return "", errors.New("invalid file payload location")
	}
	hash := strings.TrimPrefix(location, prefix)
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != sha256.Size {
		return "", errors.New("invalid file payload hash")
	}
	return hash, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}
