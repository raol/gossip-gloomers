package main

import "sync"

type Storage struct {
	logs             map[string][]int
	committedOffsets map[string]int
	mu               sync.Mutex
}

func NewStorage() *Storage {
	return &Storage{
		logs:             map[string][]int{},
		committedOffsets: map[string]int{},
	}
}

func (s *Storage) Append(key string, value int) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.logs[key]; !ok {
		s.logs[key] = make([]int, 0)
	}

	s.logs[key] = append(s.logs[key], value)

	return len(s.logs[key]) - 1
}

func (s *Storage) GetFromOffset(key string, offset int) [][]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.logs[key]; !ok {
		return make([][]int, 0)
	}

	result := make([][]int, 0)
	for i, v := range s.logs[key][offset:] {
		result = append(result, []int{i + offset, v})
	}

	return result
}

func (s *Storage) CommitOffset(key string, offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committedOffsets[key] = offset
}

func (s *Storage) GetCommittedOffset(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committedOffsets[key]
}
