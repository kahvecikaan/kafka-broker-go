package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

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
