package api

import (
	"strings"
	"sync"
)

// KeyStore keeps a key obtained through POST /api-keys so the next manual API
// job can use it. The value never leaves Go through the HTTP response.
type KeyStore struct {
	mu  sync.RWMutex
	key string
}

func NewKeyStore() *KeyStore { return &KeyStore{} }

func (s *KeyStore) Set(key string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.key = strings.TrimSpace(key)
	s.mu.Unlock()
}

func (s *KeyStore) Get() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.key
}

func (s *KeyStore) Has() bool { return s.Get() != "" }
