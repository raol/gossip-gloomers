package main

import "sync"

type SimpleStorage struct {
	db map[float64]float64
	mu sync.Mutex
}

func NewStorage() *SimpleStorage {
	return &SimpleStorage{
		db: map[float64]float64{},
	}
}

func (s *SimpleStorage) Apply(ops []any) [][]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var res [][]any

	for _, v := range ops {
		op := v.([]any)
		if op[0].(string) == "r" {
			res = append(res, []any{"r", op[1], s.read(op[1].(float64))})
		} else {
			s.write(op[1].(float64), op[2].(float64))
			res = append(res, op)
		}
	}

	return res
}

func (s *SimpleStorage) read(id float64) any {
	v, err := s.db[id]
	if err {
		return nil
	}

	return v
}

func (s *SimpleStorage) write(id float64, value float64) {
	s.db[id] = value
}
