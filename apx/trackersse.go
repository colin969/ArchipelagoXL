package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type TrackerSSE struct {
	subscribe   chan chan []byte
	unsubscribe chan chan []byte
	dirty       chan TeamSlot
}

func newTrackerSSE() *TrackerSSE {
	return &TrackerSSE{
		subscribe:   make(chan chan []byte, 16),
		unsubscribe: make(chan chan []byte, 16),
		dirty:       make(chan TeamSlot, 256),
	}
}

func (h *TrackerSSE) Run(ctx context.Context, apx *ApxRoom) {
	listeners := make(map[chan []byte]struct{})
	dirty := make(map[TeamSlot]struct{})
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ch := <-h.subscribe:
			listeners[ch] = struct{}{}
		case ch := <-h.unsubscribe:
			delete(listeners, ch)
			close(ch)
		case ts := <-h.dirty:
			dirty[ts] = struct{}{}
		case <-ticker.C:
			if len(dirty) == 0 || len(listeners) == 0 {
				dirty = make(map[TeamSlot]struct{})
				continue
			}
			msg := h.buildPatch(apx, dirty)
			dirty = make(map[TeamSlot]struct{})
			if msg == nil {
				continue
			}
			for ch := range listeners {
				select {
				case ch <- msg:
				default:
				}
			}
		}
	}
}

func (h *TrackerSSE) MarkDirty(ts TeamSlot) {
	select {
	case h.dirty <- ts:
	default:
	}
}

func (h *TrackerSSE) buildPatch(apx *ApxRoom, dirty map[TeamSlot]struct{}) []byte {
	state := apx.state
	activityTimers := state.SlotActivityTimers.GetAll()
	connectionTimers := state.SlotConnectionTimers.GetAll()
	clientStatuses := state.ClientStatuses.GetAll()

	patches := make([]TrackerPlayerState, 0, len(dirty))
	for ts := range dirty {
		checked := state.Checks.GetChecked(ts)
		lastActivity := ""
		if v, ok := activityTimers[ts]; ok {
			lastActivity = time.Unix(v, 0).UTC().Format(time.RFC3339)
		}
		lastConn := ""
		if v, ok := connectionTimers[ts]; ok {
			lastConn = time.Unix(v, 0).UTC().Format(time.RFC3339)
		}
		patches = append(patches, TrackerPlayerState{
			Team:           ts.Team,
			Slot:           ts.Slot,
			TotalLocations: len(state.Locations[ts]),
			ChecksDone:     len(checked),
			Status:         int(clientStatuses[ts]),
			LastActivity:   lastActivity,
			LastConn:       lastConn,
		})
	}

	data, err := json.Marshal(patches)
	if err != nil {
		return nil
	}
	return fmt.Appendf(nil, "event: patch\ndata: %s\n\n", data)
}

func (h *TrackerSSE) buildSnapshot(apx *ApxRoom) []byte {
	state := apx.state
	activityTimers := state.SlotActivityTimers.GetAll()
	connectionTimers := state.SlotConnectionTimers.GetAll()
	clientStatuses := state.ClientStatuses.GetAll()

	playerStates := make([]TrackerPlayerState, 0, len(state.SlotInfo))
	for ts := range state.SlotInfo {
		checked := state.Checks.GetChecked(ts)
		lastActivity := ""
		if v, ok := activityTimers[ts]; ok {
			lastActivity = time.Unix(v, 0).UTC().Format(time.RFC3339)
		}
		lastConn := ""
		if v, ok := connectionTimers[ts]; ok {
			lastConn = time.Unix(v, 0).UTC().Format(time.RFC3339)
		}
		playerStates = append(playerStates, TrackerPlayerState{
			Team:           ts.Team,
			Slot:           ts.Slot,
			TotalLocations: len(state.Locations[ts]),
			ChecksDone:     len(checked),
			Status:         int(clientStatuses[ts]),
			LastActivity:   lastActivity,
			LastConn:       lastConn,
		})
	}

	data, _ := json.Marshal(playerStates)
	return fmt.Appendf(nil, "event: snapshot\ndata: %s\n\n", data)
}

// SlotTrackerSSE handles per-slot SSE — no batching, events are discrete.
type SlotTrackerSSE struct {
	subscribe   chan slotSSESub
	unsubscribe chan chan []byte
	publish     chan slotSSEEvent
}

type slotSSESub struct {
	slot int
	ch   chan []byte
}

type slotSSEEvent struct {
	slot int
	msg  []byte
}

func newSlotTrackerSSE() *SlotTrackerSSE {
	return &SlotTrackerSSE{
		subscribe:   make(chan slotSSESub, 16),
		unsubscribe: make(chan chan []byte, 16),
		publish:     make(chan slotSSEEvent, 512),
	}
}

func (h *SlotTrackerSSE) Run(ctx context.Context) {
	listeners := make(map[int]map[chan []byte]struct{})

	for {
		select {
		case <-ctx.Done():
			return
		case sub := <-h.subscribe:
			if listeners[sub.slot] == nil {
				listeners[sub.slot] = make(map[chan []byte]struct{})
			}
			listeners[sub.slot][sub.ch] = struct{}{}
		case ch := <-h.unsubscribe:
			for slot, chans := range listeners {
				if _, ok := chans[ch]; ok {
					delete(chans, ch)
					if len(chans) == 0 {
						delete(listeners, slot)
					}
					close(ch)
					break
				}
			}
		case ev := <-h.publish:
			for ch := range listeners[ev.slot] {
				select {
				case ch <- ev.msg:
				default:
				}
			}
		}
	}
}

func (h *SlotTrackerSSE) Publish(slot int, event string, data []byte) {
	msg := fmt.Appendf(nil, "event: %s\ndata: %s\n\n", event, data)
	select {
	case h.publish <- slotSSEEvent{slot: slot, msg: msg}:
	default:
	}
}

func (h *SlotTrackerSSE) buildSnapshot(apx *ApxRoom, slot int) []byte {
	state := apx.state
	ts := TeamSlot{0, slot}

	type slotSnapshot struct {
		Items     []NetworkItem `json:"items"`
		Checks    []int64       `json:"checks"`
		Status    int           `json:"status"`
		TotalLocs int           `json:"total_locs"`
	}

	snap := slotSnapshot{
		Items:     state.ReceivedItems.Get(ReceivedItemsKey{0, slot, true}),
		Checks:    state.Checks.GetChecked(ts),
		Status:    int(state.ClientStatuses.Get(0, slot)),
		TotalLocs: len(state.Locations[ts]),
	}
	data, _ := json.Marshal(snap)
	return fmt.Appendf(nil, "event: snapshot\ndata: %s\n\n", data)
}
