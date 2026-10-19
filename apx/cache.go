package main

import (
	"sync"
	"time"
)

type cachedEntry struct {
	body      []byte
	expiresAt time.Time
}

func (c *cachedEntry) get() ([]byte, bool) {
	if c.body == nil {
		return nil, false
	}
	if !c.expiresAt.IsZero() && time.Now().After(c.expiresAt) {
		return nil, false
	}
	return c.body, true
}

func (c *cachedEntry) set(b []byte, ttl time.Duration) {
	c.body = b
	if ttl > 0 {
		c.expiresAt = time.Now().Add(ttl)
	}
}

type RoomApiCache struct {
	mu      sync.RWMutex
	entries map[string]*cachedEntry
}

func newRoomApiCache() *RoomApiCache {
	return &RoomApiCache{entries: make(map[string]*cachedEntry)}
}

func (c *RoomApiCache) Invalidate(key string) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}
