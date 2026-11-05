package aggregator

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	safeio "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/safe_socket"
	utilsProto "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type Storage struct {
	name string
	dir  string
	mu   sync.Mutex
}

func NewStorage(name, dir string) *Storage {
	os.MkdirAll(dir, 0755)
	return &Storage{
		name: name,
		dir:  dir,
	}
}

func (s *Storage) getFilePath(clientID uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s_%d_results.txt", s.name, clientID))
}

func (s *Storage) Write(clientID uint64, batchBytes []byte) error {
	return utils.WithLockAndReturn(&s.mu, func() error {
		return s.writeToFile(clientID, batchBytes)
	})
}

func (s *Storage) Read(clientID uint64) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readFromFile(clientID)
}

func (s *Storage) readFromFile(clientID uint64) ([][]byte, error) {
	filePath := s.getFilePath(clientID)
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		slog.Error("Error opening file for reading", "file", filePath, "error", err)
		return nil, err
	}
	defer f.Close()

	var batches [][]byte
	for {
		length, err := utilsProto.ReadUint32(f)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		batchBytes, err := safeio.RecvAll(f, int(length))
		if err != nil {
			slog.Error("Error receiving batch from file", "file", filePath, "error", err)
			return nil, err
		}
		batches = append(batches, batchBytes)
	}

	s.deleteFile(clientID)
	return batches, nil
}

func (s *Storage) writeToFile(clientID uint64, batchBytes []byte) error {
	filePath := s.getFilePath(clientID)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Error opening file for writing", "file", filePath, "error", err)
		return err
	}
	defer f.Close()

	lengthBytes := utilsProto.SerializeUint32(uint32(len(batchBytes)))
	if _, err := f.Write(lengthBytes); err != nil {
		slog.Error("Error writing length to file", "file", filePath, "error", err)
		return err
	}
	if _, err := f.Write(batchBytes); err != nil {
		slog.Error("Error writing batch to file", "file", filePath, "error", err)
		return err
	}
	return nil
}

func (s *Storage) deleteFile(clientID uint64) {
	filePath := s.getFilePath(clientID)
	if err := os.Remove(filePath); err != nil {
		slog.Error("Error deleting file", "file", filePath, "error", err)
	}
}
