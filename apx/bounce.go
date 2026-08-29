package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// 2 Second cooldown to help prevent double-deaths
const deathlinkThrottle = 2 * time.Second

type bounceInfoStore struct {
	mu sync.RWMutex
	// Maps slot name to deathlink count
	counts map[int]int
	// Probability to accept a deathlink message
	deathlinkProbability float64
	// Slots that aren't allowed to send certain tags
	// Absurd typing, but should be O(1) because it's all keys in a map, fite me
	excluded      map[int]map[string]struct{}
	lastDeathlink time.Time
}

func newBounceInfoStore() *bounceInfoStore {
	return &bounceInfoStore{
		counts: make(map[int]int),
		// Default off
		deathlinkProbability: 1,
		excluded:             make(map[int]map[string]struct{}),
		// Why is Sub a duration by Add a time?
		lastDeathlink: time.Now().Add(-deathlinkThrottle),
	}
}

func (ds *bounceInfoStore) Get() map[int]int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	result := make(map[int]int, len(ds.counts))
	maps.Copy(result, ds.counts)
	return result
}

func (ds *bounceInfoStore) GetProbability() float64 {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.deathlinkProbability
}

func (ds *bounceInfoStore) SetProbability(probability float64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.deathlinkProbability = probability
}

func (ds *bounceInfoStore) GetExclusions() map[int][]string {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	// Make a safe copy of it to return
	result := make(map[int][]string, len(ds.excluded))
	for slot, tags := range ds.excluded {
		tagList := make([]string, 0, len(tags))
		for tag := range tags {
			tagList = append(tagList, tag)
		}
		result[slot] = tagList
	}
	return result
}

func (ds *bounceInfoStore) Exclude(slotId int, tag string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.excluded[slotId] == nil {
		ds.excluded[slotId] = make(map[string]struct{})
	}
	ds.excluded[slotId][tag] = struct{}{}
}

func (ds *bounceInfoStore) Unexclude(slotId int, tag string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	delete(ds.excluded[slotId], tag)
	if len(ds.excluded[slotId]) == 0 {
		delete(ds.excluded, slotId)
	}
}

func (ds *bounceInfoStore) IsExcluded(slotId int, tag string) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	_, ok := ds.excluded[slotId][tag]
	return ok
}

func (ds *bounceInfoStore) CanSendDeathlink(now time.Time) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if now.Sub(ds.lastDeathlink) < deathlinkThrottle {
		return false
	}
	ds.lastDeathlink = now
	return true
}

// Add to the slot's deathlink count
func (ds *bounceInfoStore) Add(slotId int) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.counts[slotId]++
}

func (s ApxRoom) handleBounce(ctx context.Context, connState *connectionState, raw map[string]any) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshalling bounce message: %w", err)
	}

	var msg BounceMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshalling bounce message: %w", err)
	}

	// Record types of tags sent by each slot
	for _, tag := range msg.Tags {
		s.metrics.bouncePackets.WithLabelValues(s.lobbyRoomId, *connState.slotName, *connState.registeredClient.game, tag).Inc()
	}

	// Deathlink packets have extra options for blocking and probability limiting, handle seperate
	if slices.Contains(msg.Tags, "DeathLink") {
		return s.handleDeathLink(ctx, connState, msg)
	}

	// Strip any excluded tags
	msg.Tags = slices.DeleteFunc(msg.Tags, func(tag string) bool {
		return s.bounceInfo.IsExcluded(connState.registeredClient.slotId, tag)
	})

	s.connections.BroadcastBounceFromSlot(ctx, s.bounceInfo, connState.registeredClient.slotId, msg)

	return nil
}

func (s ApxRoom) handleDeathLink(ctx context.Context, connState *connectionState, msg BounceMessage) error {
	// Validate this is a valid packet. Some apworlds send bad packets, some can't handle being sent bad packets.
	data, err := json.Marshal(msg.Data)
	if err != nil {
		return fmt.Errorf("marshalling deathlink data: %w", err)
	}

	var dl BounceDataDeathlink
	if err := json.Unmarshal(data, &dl); err != nil {
		return fmt.Errorf("unmarshalling deathlink data: %w", err)
	}

	if dl.Time == 0 {
		// We can't trust their own client will handle a Bounced packet with a real time field, so drop the incoming
		log.Printf("deathlink malformed: slot=%q cause='bad time field'", *connState.slotName)
		return nil
	}

	// For clarity, the Source will always contain slot name unless otherwise given
	if dl.Source == "" {
		dl.Source = *connState.slotName
	}

	s.bounceInfo.Add(connState.registeredClient.slotId)
	log.Printf("deathlink: slot=%q source=%q cause=%v", *connState.slotName, dl.Source, dl.Cause)

	// Strip any excluded tags
	msg.Tags = slices.DeleteFunc(msg.Tags, func(tag string) bool {
		return s.bounceInfo.IsExcluded(connState.registeredClient.slotId, tag)
	})

	if s.bounceInfo.IsExcluded(connState.registeredClient.slotId, "DeathLink") {
		log.Printf("deathlink blocked for excluded slot %q", *connState.slotName)
		return nil
	}

	probability := s.bounceInfo.GetProbability()
	if probability != 1 && rand.Float64() >= probability {
		log.Println("deathlink dropped by probability func")
		return nil
	}

	if !s.bounceInfo.CanSendDeathlink(time.Now()) {
		log.Println("deathlink dropped by cooldown")
		return nil
	}

	// Put the patched deathlink data back onto the packet
	modifiedData, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("marshalling modified deathlink data: %w", err)
	}
	if err := json.Unmarshal(modifiedData, &msg.Data); err != nil {
		return fmt.Errorf("updating bounce message data: %w", err)
	}

	s.connections.BroadcastBounceFromSlot(ctx, s.bounceInfo, connState.registeredClient.slotId, msg)

	return nil
}
