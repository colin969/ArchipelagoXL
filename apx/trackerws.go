package main

import (
	"context"
	"encoding/json"
	"time"
)

type wsEnvelope struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

func wrapEvent(event string, data []byte) []byte {
	msg, _ := json.Marshal(wsEnvelope{Event: event, Data: json.RawMessage(data)})
	return msg
}

type TrackerWS struct {
	subscribe   chan chan []byte
	unsubscribe chan chan []byte
	dirty       chan TeamSlot
	interval    time.Duration
}

func newTrackerWS(_ int) *TrackerWS {
	interval := 2
	// switch {
	// case playerCount > 300:
	// 	interval = 10
	// case playerCount > 200:
	// 	interval = 8
	// case playerCount > 100:
	// 	interval = 6
	// case playerCount > 50:
	// 	interval = 4
	// case playerCount > 20:
	// 	interval = 3
	// }
	return &TrackerWS{
		subscribe:   make(chan chan []byte, 16),
		unsubscribe: make(chan chan []byte, 16),
		dirty:       make(chan TeamSlot, 256),
		interval:    time.Duration(interval) * time.Second,
	}
}

func (h *TrackerWS) Run(ctx context.Context, apx *ApxRoom) {
	listeners := make(map[chan []byte]struct{})
	dirty := make(map[TeamSlot]struct{})
	ticker := time.NewTicker(h.interval)
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

func (h *TrackerWS) MarkDirty(ts TeamSlot) {
	select {
	case h.dirty <- ts:
	default:
	}
}

func (h *TrackerWS) buildPatch(apx *ApxRoom, dirty map[TeamSlot]struct{}) []byte {
	state := apx.state
	activityTimers := state.SlotActivityTimers.GetAll()
	connectionTimers := state.SlotConnectionTimers.GetAll()
	clientStatuses := state.ClientStatuses.GetAll()

	patches := make([]TrackerPlayerState, 0, len(dirty))
	for ts := range dirty {
		lastActivity := int64(0)
		if v, ok := activityTimers[ts]; ok {
			lastActivity = v
		}
		lastConn := int64(0)
		if v, ok := connectionTimers[ts]; ok {
			lastConn = v
		}
		patches = append(patches, TrackerPlayerState{
			Team:           ts.Team,
			Slot:           ts.Slot,
			TotalLocations: len(state.Locations[ts]),
			ChecksDone:     state.Checks.GetTotalChecked(ts),
			Status:         int(clientStatuses[ts]),
			LastActivity:   lastActivity,
			LastConn:       lastConn,
		})
	}

	data, err := json.Marshal(patches)
	if err != nil {
		return nil
	}
	return wrapEvent("patch", data)
}

func (h *TrackerWS) buildSnapshot(apx *ApxRoom) []byte {
	state := apx.state
	activityTimers := state.SlotActivityTimers.GetAll()
	connectionTimers := state.SlotConnectionTimers.GetAll()
	clientStatuses := state.ClientStatuses.GetAll()

	playerStates := make([]TrackerPlayerState, 0, len(state.SlotInfo))
	for ts := range state.SlotInfo {
		lastActivity := int64(0)
		if v, ok := activityTimers[ts]; ok {
			lastActivity = v
		}
		lastConn := int64(0)
		if v, ok := connectionTimers[ts]; ok {
			lastConn = v
		}
		playerStates = append(playerStates, TrackerPlayerState{
			Team:           ts.Team,
			Slot:           ts.Slot,
			TotalLocations: len(state.Locations[ts]),
			ChecksDone:     state.Checks.GetTotalChecked(ts),
			Status:         int(clientStatuses[ts]),
			LastActivity:   lastActivity,
			LastConn:       lastConn,
		})
	}

	data, _ := json.Marshal(TrackerOverview{PlayerChecks: playerStates})
	return wrapEvent("snapshot", data)
}

type SlotTrackerDirtyFlags uint8

const (
	SlotTrackerDirtyChecks SlotTrackerDirtyFlags = 1 << iota
	SlotTrackerDirtyItems
	SlotTrackerDirtyHints
)

type SlotTrackerWS struct {
	subscribe   chan slotWSSub
	unsubscribe chan chan []byte
	dirty       chan slotWSDirty
	interval    time.Duration
}

type slotWSDirty struct {
	slot  int
	flags SlotTrackerDirtyFlags
}

type slotWSSub struct {
	slot int
	ch   chan []byte
}

type slotWSEvent struct {
	slot int
	msg  []byte
}

func newSlotTrackerWS() *SlotTrackerWS {
	return &SlotTrackerWS{
		subscribe:   make(chan slotWSSub, 16),
		unsubscribe: make(chan chan []byte, 16),
		dirty:       make(chan slotWSDirty, 512),
		interval:    2 * time.Second,
	}
}

func (h *SlotTrackerWS) MarkDirty(slot int, flags SlotTrackerDirtyFlags) {
	select {
	case h.dirty <- slotWSDirty{slot: slot, flags: flags}:
	default:
	}
}

func (h *SlotTrackerWS) Run(ctx context.Context, apx *ApxRoom) {
	listeners := make(map[int]map[chan []byte]struct{})
	dirty := make(map[int]SlotTrackerDirtyFlags)
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

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
		case d := <-h.dirty:
			dirty[d.slot] |= d.flags
		case <-ticker.C:
			for slot, flags := range dirty {
				chans := listeners[slot]
				if len(chans) == 0 {
					continue
				}
				msg := h.buildPatch(apx, slot, flags)
				if msg == nil {
					continue
				}
				for ch := range chans {
					select {
					case ch <- msg:
					default:
					}
				}
			}
			dirty = make(map[int]SlotTrackerDirtyFlags)
		}
	}
}

type WsTrackerHint struct {
	Entrance        string     `json:"entrance"`
	Location        string     `json:"location"`
	Item            string     `json:"item"`
	Status          HintStatus `json:"status"`
	ReceivingPlayer int32      `json:"receiving_player"`
	FindingPlayer   int32      `json:"finding_player"`
	ItemFlags       int16      `json:"item_flags"`
	Found           bool       `json:"found"`
}

type SlotPatch struct {
	Checks *[]int64         `json:"checks,omitempty"`
	Items  *[]NetworkItem   `json:"items,omitempty"`
	Hints  *[]WsTrackerHint `json:"hints,omitempty"`
	Status *int             `json:"status,omitempty"`
}

func (h *SlotTrackerWS) buildPatch(apx *ApxRoom, slot int, flags SlotTrackerDirtyFlags) []byte {
	state := apx.state
	ts := TeamSlot{0, slot}
	patch := SlotPatch{}

	if flags&SlotTrackerDirtyChecks != 0 {
		checks := state.Checks.GetChecked(ts)
		patch.Checks = &checks
	}
	if flags&SlotTrackerDirtyItems != 0 {
		items := state.ReceivedItems.Get(ReceivedItemsKey{0, slot, true})
		patch.Items = &items
	}
	if flags&SlotTrackerDirtyHints != 0 {
		rawHints := state.Hints.GetSlotHints(ts)
		trackerHints := make([]WsTrackerHint, 0, len(rawHints))
		for _, rawHint := range rawHints {
			locGame := apx.state.SlotInfo[TeamSlot{0, int(rawHint.FindingPlayer)}].Game
			itemGame := apx.state.SlotInfo[TeamSlot{0, int(rawHint.ReceivingPlayer)}].Game
			locName := apx.datapackages.LocationIDToName[locGame][rawHint.Location]
			itemName := apx.datapackages.ItemIDToName[itemGame][rawHint.Item]
			trackerHints = append(trackerHints, WsTrackerHint{
				Entrance:        rawHint.Entrance,
				Location:        locName,
				Item:            itemName,
				Status:          rawHint.Status,
				ReceivingPlayer: rawHint.ReceivingPlayer,
				FindingPlayer:   rawHint.FindingPlayer,
				ItemFlags:       rawHint.ItemFlags,
				Found:           rawHint.Found,
			})
		}
		patch.Hints = &trackerHints
	}

	data, err := json.Marshal(patch)
	if err != nil {
		return nil
	}
	return wrapEvent("patch", data)
}

func (h *SlotTrackerWS) buildSnapshot(apx *ApxRoom, slot int) []byte {
	state := apx.state
	ts := TeamSlot{0, slot}

	type slotSnapshot struct {
		Locations [][2]int64      `json:"locations"`
		Checks    []int64         `json:"checks"`
		Items     []NetworkItem   `json:"items"`
		Hints     []WsTrackerHint `json:"hints"`
		Status    int             `json:"status"`
		TotalLocs int             `json:"total_locs"`
	}

	locs := make([][2]int64, 0, len(state.Locations[ts]))
	spheresShown := apx.state.ServerOptions.GetTrackerSpheres()
	if spheresShown == 0 {
		// Spheres not shown, just return locs
		for locId := range state.Locations[ts] {
			locs = append(locs, [2]int64{-1, locId})
		}
	} else {
		// Spheres shown, hide any below the allowed limit
		for idx, sphere := range state.SphereLocs {
			storedSphere := int64(idx + 1)
			if int64(spheresShown) < storedSphere {
				storedSphere = -1
			}
			for locId := range sphere[ts] {
				locs = append(locs, [2]int64{storedSphere, locId})
			}
		}
	}
	rawHints := state.Hints.GetSlotHints(ts)
	trackerHints := make([]WsTrackerHint, 0, len(rawHints))
	for _, rawHint := range rawHints {
		locGame := apx.state.SlotInfo[TeamSlot{0, int(rawHint.FindingPlayer)}].Game
		itemGame := apx.state.SlotInfo[TeamSlot{0, int(rawHint.ReceivingPlayer)}].Game
		locName := apx.datapackages.LocationIDToName[locGame][rawHint.Location]
		itemName := apx.datapackages.ItemIDToName[itemGame][rawHint.Item]
		trackerHints = append(trackerHints, WsTrackerHint{
			Entrance:        rawHint.Entrance,
			Location:        locName,
			Item:            itemName,
			Status:          rawHint.Status,
			ReceivingPlayer: rawHint.ReceivingPlayer,
			FindingPlayer:   rawHint.FindingPlayer,
			ItemFlags:       rawHint.ItemFlags,
			Found:           rawHint.Found,
		})
	}
	snap := slotSnapshot{
		Items:     state.ReceivedItems.Get(ReceivedItemsKey{0, slot, true}),
		Locations: locs,
		Checks:    state.Checks.GetChecked(ts),
		Hints:     trackerHints,
		Status:    int(state.ClientStatuses.Get(0, slot)),
		TotalLocs: len(state.Locations[ts]),
	}
	data, _ := json.Marshal(snap)
	return wrapEvent("snapshot", data)
}
