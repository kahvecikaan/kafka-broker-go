package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// ReadPartition reads a partition's log segment from disk and returns its raw
// bytes. The bytes are record batches in wire format, passed through unchanged.
// A missing log file means an empty partition, so it returns no bytes, no error.
func ReadPartition(logDir string, topic string, partition int32) ([]byte, error) {
	dir := fmt.Sprintf("%s-%d", topic, partition)
	path := filepath.Join(logDir, dir, "00000000000000000000.log")
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	return b, nil
}
