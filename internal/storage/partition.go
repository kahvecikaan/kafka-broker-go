package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const segmentFileName = "00000000000000000000.log"

// getPartitionPath is a helper function that resolves both the parent directory
// path and the absolute file path for a given topic partition.
func getPartitionPath(logDir string, topic string, partition int32) (dirPath string, filePath string) {
	dirName := fmt.Sprintf("%s-%d", topic, partition)
	dirPath = filepath.Join(logDir, dirName)
	filePath = filepath.Join(dirPath, segmentFileName)
	return dirPath, filePath
}

// ReadPartition reads a partition's log segment from disk and returns its raw
// bytes. The bytes are record batches in wire format, passed through unchanged.
// A missing log file means an empty partition, so it returns no bytes, no error.
func ReadPartition(logDir string, topic string, partition int32) ([]byte, error) {
	_, filePath := getPartitionPath(logDir, topic, partition)

	b, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read partition file: %w", err)
	}

	return b, nil
}

// WritePartition appends records to a partition log file.
func WritePartition(logDir string, topic string, partition int32, records []byte) error {
	dirPath, filePath := getPartitionPath(logDir, topic, partition)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	if _, err := file.Write(records); err != nil {
		return fmt.Errorf("failed to write records to %s: %w", filePath, err)
	}

	return nil
}
