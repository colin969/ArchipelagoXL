package main

import (
	"testing"
	"time"
)

func TestDeathlink(t *testing.T) {
	t.Run("Cooldown", func(t *testing.T) {
		ds := newBounceInfoStore()
		now := time.Now()

		if !ds.CanSendDeathlink(now) {
			t.Fatal("expected first deathlink to be allowed")
		}

		if ds.CanSendDeathlink(now) {
			t.Fatal("expected second deathlink within cooldown to be blocked")
		}

		if !ds.CanSendDeathlink(now.Add(deathlinkThrottle)) {
			t.Fatal("expected deathlink after cooldown to be allowed")
		}
	})

	// This feels pointless but still
	t.Run("ProbabilityDefault", func(t *testing.T) {
		ds := newBounceInfoStore()
		if ds.GetProbability() != 1.0 {
			t.Fatalf("expected default probability 1.0, got %f", ds.GetProbability())
		}
	})

	t.Run("ProbabilitySet", func(t *testing.T) {
		ds := newBounceInfoStore()
		ds.SetProbability(0.5)
		if ds.GetProbability() != 0.5 {
			t.Fatalf("expected probability 0.5, got %f", ds.GetProbability())
		}
	})

	t.Run("ProbabilityZero", func(t *testing.T) {
		ds := newBounceInfoStore()
		ds.SetProbability(0.0)
		if ds.GetProbability() != 0.0 {
			t.Fatalf("expected probability 0.0, got %f", ds.GetProbability())
		}
	})

	t.Run("ProbabilityOne", func(t *testing.T) {
		ds := newBounceInfoStore()
		ds.SetProbability(1.0)
		if ds.GetProbability() != 1.0 {
			t.Fatalf("expected probability 1.0, got %f", ds.GetProbability())
		}
	})
}
