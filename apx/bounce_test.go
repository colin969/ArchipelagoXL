package main

import (
	"testing"
	"time"
)

// Tests

func TestDeathlinkCooldown(t *testing.T) {
	ds := newBounceInfoStore()
	now := time.Now()

	// First call should succeed
	if !ds.CanSendDeathlink(now) {
		t.Fatal("expected first deathlink to be allowed")
	}

	// Immediate second call should be throttled
	if ds.CanSendDeathlink(now) {
		t.Fatal("expected second deathlink within cooldown to be blocked")
	}

	// Call after cooldown expires should succeed
	if !ds.CanSendDeathlink(now.Add(deathlinkThrottle)) {
		t.Fatal("expected deathlink after cooldown to be allowed")
	}
}
