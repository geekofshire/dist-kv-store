package main

import (
	"sync"
)

type Store struct {
	mu    sync.RWMutex
	store map[string]string
}

func NewStore() *Store {
	return &Store{
		mu:    sync.RWMutex{},
		store: make(map[string]string),
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.store[key]
	return value, ok
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[key] = value
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.store[key]; !ok {
		return false
	}

	delete(s.store, key)
	return true
}

func (s *Store) ApplyLog(entry Entry) bool {
	switch entry.Cmd {
	case Set:
		s.Set(entry.Key, entry.Value)
		return true

	case Delete:
		return s.Delete(entry.Key)

	default:
		return false
	}
}
