package main

import (
	"context"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"sync"
)

const (
	committedPrefix = "committed_"
	offsetPrefix    = "offset_"
	valuePrefix     = "value_"
)

type Storage struct {
	logs    map[string][]int
	storage *maelstrom.KV
	mu      sync.Mutex
}

func NewStorage(node *maelstrom.Node) *Storage {
	return &Storage{
		logs:    map[string][]int{},
		storage: maelstrom.NewLinKV(node),
	}
}

func (s *Storage) Append(key string, value int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.logs[key]; !ok {
		s.logs[key] = make([]int, 0)
	}

	offset, err := s.storage.ReadInt(context.Background(), fmt.Sprintf("%s%s", offsetPrefix, key))
	if err != nil {
		offset = 0
	} else {
		offset += 1
	}

	s.storage.Write(context.Background(), fmt.Sprintf("%s%s", offsetPrefix, key), offset)
	s.storage.Write(context.Background(), fmt.Sprintf("%s%s_%d", valuePrefix, key, offset), value)

	return offset
}

func (s *Storage) GetFromOffset(key string, offset int) [][]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([][]int, 0)
	currentOffset, err := s.storage.ReadInt(context.Background(), fmt.Sprintf("%s%s", offsetPrefix, key))
	if err != nil {
		return result
	}

	for i := offset; i <= currentOffset; i++ {
		value, err := s.storage.ReadInt(context.Background(), fmt.Sprintf("%s%s_%d", valuePrefix, key, i))
		if err == nil {
			result = append(result, []int{i, value})
		}
	}

	return result
}

func (s *Storage) CommitOffset(key string, offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storage.Write(context.Background(), fmt.Sprintf("%s%s", committedPrefix, key), offset)
}

func (s *Storage) GetCommittedOffset(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	i, _ := s.storage.ReadInt(context.Background(), fmt.Sprintf("%s%s", committedPrefix, key))
	return i
}
