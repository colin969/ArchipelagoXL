package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

// Tests

func TestAllocateAndRegisterHandlerPair(t *testing.T) {
	newRegistry := func() *RoomRegistry {
		return &RoomRegistry{
			idToHandler: make(map[int]*apxHandler),
		}
	}

	t.Run("allocates a valid pair when registry is empty", func(t *testing.T) {
		r := newRegistry()
		normal := &apxHandler{}
		reduced := &apxHandler{}

		if err := r.AllocateAndRegisterHandlerPair(normal, reduced); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if normal.id == 0 || reduced.id == 0 {
			t.Error("expected non-zero IDs")
		}
		if normal.id == reduced.id {
			t.Errorf("normal and reduced got the same ID: %d", normal.id)
		}
		if normal.id < roomMinID || normal.id > roomMaxID {
			t.Errorf("normal ID %d out of range [%d, %d]", normal.id, roomMinID, roomMaxID)
		}
		if reduced.id < roomMinID || reduced.id > roomMaxID {
			t.Errorf("reduced ID %d out of range [%d, %d]", reduced.id, roomMinID, roomMaxID)
		}
		if r.idToHandler[normal.id] != normal {
			t.Error("normal handler not registered in idToHandler")
		}
		if r.idToHandler[reduced.id] != reduced {
			t.Error("reduced handler not registered in idToHandler")
		}
	})

	t.Run("reuses valid pre-existing IDs", func(t *testing.T) {
		r := newRegistry()
		normal := &apxHandler{id: roomMinID + 1}
		reduced := &apxHandler{id: roomMinID + 2}

		if err := r.AllocateAndRegisterHandlerPair(normal, reduced); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if normal.id != roomMinID+1 {
			t.Errorf("expected normal ID %d, got %d", roomMinID+1, normal.id)
		}
		if reduced.id != roomMinID+2 {
			t.Errorf("expected reduced ID %d, got %d", roomMinID+2, reduced.id)
		}
	})

	t.Run("falls back to random allocation when pre-existing IDs are in use", func(t *testing.T) {
		r := newRegistry()
		existing := &apxHandler{id: roomMinID + 1}
		r.idToHandler[roomMinID+1] = existing

		normal := &apxHandler{id: roomMinID + 1}
		reduced := &apxHandler{id: roomMinID + 2}

		if err := r.AllocateAndRegisterHandlerPair(normal, reduced); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if normal.id == roomMinID+1 {
			t.Error("expected normal to get a new ID since it was in use")
		}
		if normal.id == reduced.id {
			t.Errorf("normal and reduced got the same ID: %d", normal.id)
		}
	})

	t.Run("falls back to random allocation when pre-existing IDs are out of range", func(t *testing.T) {
		r := newRegistry()
		normal := &apxHandler{id: roomMinID - 1}  // below min
		reduced := &apxHandler{id: roomMaxID + 1} // above max

		if err := r.AllocateAndRegisterHandlerPair(normal, reduced); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if normal.id < roomMinID || normal.id > roomMaxID {
			t.Errorf("normal ID %d out of range", normal.id)
		}
		if reduced.id < roomMinID || reduced.id > roomMaxID {
			t.Errorf("reduced ID %d out of range", reduced.id)
		}
	})

	t.Run("reuses valid normal ID when reduced needs allocation", func(t *testing.T) {
		r := newRegistry()
		normal := &apxHandler{id: roomMinID + 10000}
		reduced := &apxHandler{id: 0} // needs allocation

		if err := r.AllocateAndRegisterHandlerPair(normal, reduced); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if normal.id != roomMinID+10000 {
			t.Errorf("expected normal to keep ID %d, got %d", roomMinID+10000, normal.id)
		}
		if reduced.id == roomMinID+10000 {
			t.Error("reduced got the same ID as normal")
		}
		if reduced.id < roomMinID || reduced.id > roomMaxID {
			t.Errorf("reduced ID %d out of range", reduced.id)
		}
	})

	t.Run("reuses valid reduced ID when normal needs allocation", func(t *testing.T) {
		r := newRegistry()
		normal := &apxHandler{id: 0} // needs allocation
		reduced := &apxHandler{id: roomMinID + 10000}

		if err := r.AllocateAndRegisterHandlerPair(normal, reduced); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if reduced.id != roomMinID+10000 {
			t.Errorf("expected reduced to keep ID %d, got %d", roomMinID+10000, reduced.id)
		}
		if normal.id == roomMinID+10000 {
			t.Error("normal got the same ID as reduced")
		}
		if normal.id < roomMinID || normal.id > roomMaxID {
			t.Errorf("normal ID %d out of range", normal.id)
		}
	})
}

func TestUploadRoomRequestFromForm(t *testing.T) {
	cases := []struct {
		name              string
		form              url.Values
		wantPerSlot       bool
		wantDLDisabled    bool
		wantReducedAccess bool
	}{
		{"defaults", url.Values{}, true, false, false},
		{"per_slot_passwords false", url.Values{"per_slot_passwords": {"false"}}, false, false, false},
		{"per_slot_passwords true", url.Values{"per_slot_passwords": {"true"}}, true, false, false},
		{"deathlink_disabled true", url.Values{"deathlink_disabled": {"true"}}, true, true, false},
		{"reduced_access true", url.Values{"reduced_access": {"true"}}, true, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.form.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			got := uploadRoomRequestFromForm(r)
			if got.PerSlotPasswords != tc.wantPerSlot {
				t.Errorf("PerSlotPasswords: got %v, want %v", got.PerSlotPasswords, tc.wantPerSlot)
			}
			if got.DeathlinkDisabled != tc.wantDLDisabled {
				t.Errorf("DeathlinkDisabled: got %v, want %v", got.DeathlinkDisabled, tc.wantDLDisabled)
			}
			if got.ReducedAccess != tc.wantReducedAccess {
				t.Errorf("ReducedAccess: got %v, want %v", got.ReducedAccess, tc.wantReducedAccess)
			}
		})
	}
}

func TestIsSphere1Incomplete(t *testing.T) {
	cases := []struct {
		name    string
		spheres TrackerSpheres
		checked map[int64]bool
		slotId  int32
		want    bool
	}{
		{
			name: "all checked",
			spheres: TrackerSpheres{
				TrackerSphereLocations{1: []int64{100, 200, 300}},
			},
			checked: map[int64]bool{100: true, 200: true, 300: true},
			slotId:  1,
			want:    false,
		},
		{
			name: "one unchecked",
			spheres: TrackerSpheres{
				TrackerSphereLocations{1: []int64{100, 200, 300}},
			},
			checked: map[int64]bool{100: true, 200: true},
			slotId:  1,
			want:    true,
		},
		{
			name: "no locations for slot",
			spheres: TrackerSpheres{
				TrackerSphereLocations{2: []int64{100}},
			},
			checked: map[int64]bool{},
			slotId:  1,
			want:    false,
		},
		{
			name: "nil checked map",
			spheres: TrackerSpheres{
				TrackerSphereLocations{1: []int64{100}},
			},
			checked: nil,
			slotId:  1,
			want:    true,
		},
		{
			name:    "empty sphere",
			spheres: TrackerSpheres{TrackerSphereLocations{}},
			checked: map[int64]bool{},
			slotId:  1,
			want:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sphere1 := tc.spheres[0]
			locIDs := sphere1[int(tc.slotId)]
			if got := isSphere1Incomplete(locIDs, tc.checked); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMaxRoomPlayerId(t *testing.T) {
	cases := []struct {
		name     string
		nameToId map[string]int
		want     int
	}{
		{"empty", map[string]int{}, 0},
		{"single", map[string]int{"Alice": 5}, 5},
		{"multiple", map[string]int{"Alice": 3, "Bob": 7, "Carol": 2}, 7},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maxRoomPlayerId(tc.nameToId); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func BenchmarkHandleRoomWebhostApiTracker(b *testing.B) {
	const (
		numSlots       = 2500
		totalRecvItems = 500_000
		totalLocations = 1_000_000
	)

	locsPerSlot := totalLocations / numSlots
	itemsPerSlot := totalRecvItems / numSlots

	slotInfo := make(map[TeamSlot]SlotInfo, numSlots)
	nameToSlot := make(map[string]*SlotInfo, numSlots)
	locations := make(map[TeamSlot]map[int64]Location, numSlots)

	for i := 1; i <= numSlots; i++ {
		ts := TeamSlot{Team: 0, Slot: i}
		si := SlotInfo{
			Team: 0,
			Slot: i,
			Name: fmt.Sprintf("Player%d", i),
			Game: "TestGame",
			Type: SlotTypePlayer,
		}
		slotInfo[ts] = si
		nameToSlot[si.Name] = &si

		slotLocs := make(map[int64]Location, locsPerSlot)
		baseLocId := int64(i) * int64(locsPerSlot)
		for j := 0; j < locsPerSlot; j++ {
			locId := baseLocId + int64(j)
			slotLocs[locId] = Location{
				Item:    int64(j % 1000),
				Player:  int16(i),
				Flags:   0,
				Checked: j%3 == 0,
			}
		}
		locations[ts] = slotLocs
	}

	so := defaultServerOptions()
	clientStatuses := &ClientStatuses{status: make(map[int]map[int]ClientStatus)}
	activityTimers := newSlotTimers()
	connectionTimers := newSlotTimers()
	now := time.Now().Unix()
	for i := 1; i <= numSlots; i++ {
		activityTimers.SetSlot(0, i, now-int64(i*10))
		connectionTimers.SetSlot(0, i, now-int64(i*5))
		clientStatuses.Set(0, i, ClientStatusPlaying)
	}

	state := &ApState{}

	// Checks
	state.Checks.locations = locations
	state.Checks.checked = make(map[TeamSlot]map[int64]struct{}, numSlots)
	for ts, locs := range locations {
		checked := make(map[int64]struct{}, len(locs))
		for locId, loc := range locs {
			if loc.Checked {
				checked[locId] = struct{}{}
			}
		}
		state.Checks.checked[ts] = checked
	}

	// ReceivedItems
	state.ReceivedItems.receivedItems = make(map[ReceivedItemsKey][]NetworkItem, numSlots)
	for i := 1; i <= numSlots; i++ {
		items := make([]NetworkItem, itemsPerSlot)
		for j := 0; j < itemsPerSlot; j++ {
			items[j] = NetworkItem{
				Item:     int64(j % 500),
				Location: int64(i)*int64(locsPerSlot) + int64(j%locsPerSlot),
				Player:   int16(i),
				Flags:    0,
			}
		}
		state.ReceivedItems.receivedItems[ReceivedItemsKey{Team: 0, Slot: i, RemoteItems: true}] = items
	}

	// Hints
	state.Hints.hints = make([]Hint, 0, numSlots*5)
	state.Hints.byTeamSlot = make(map[TeamSlot][]int)
	state.Hints.hintsUsed = make(map[TeamSlot]int)
	for i := 1; i <= numSlots; i++ {
		baseLocId := int64(i) * int64(locsPerSlot)
		for j := range 5 {
			idx := len(state.Hints.hints)
			h := Hint{
				ReceivingPlayer: int32(i),
				FindingPlayer:   int32((i % numSlots) + 1),
				Location:        baseLocId + int64(j),
				Item:            int64(j),
				Found:           j%2 == 0,
				Status:          HintStatusPriority,
			}
			state.Hints.hints = append(state.Hints.hints, h)
			state.Hints.indexHint(idx, h)
		}
	}

	// NetworkPlayers
	for i := 1; i <= numSlots; i++ {
		state.PlayersNetwork.players = append(state.PlayersNetwork.players, NetworkPlayer{
			Team:  0,
			Slot:  i,
			Name:  fmt.Sprintf("Player%d", i),
			Alias: fmt.Sprintf("Player%d", i),
		})
	}
	state.PlayersNetwork.invalidateCache()

	state.ServerOptions = &so
	state.DataStorage = newDataStorage()
	state.ClientStatuses = clientStatuses
	state.SlotActivityTimers = activityTimers
	state.SlotConnectionTimers = connectionTimers
	state.Locations = locations
	state.NameToSlot = nameToSlot
	state.SlotInfo = slotInfo

	var lastActivity atomic.Int64
	lastActivity.Store(now)

	apx := &ApxRoom{
		lastActivity: &lastActivity,
		state:        state,
		lobbyRoomId:  "bench-room",
	}

	room := &HostedRoom{
		lastActivity: &lastActivity,
		lobbyRoomId:  "bench-room",
		apx:          apx,
	}

	registry := newRoomRegistry()
	_ = registry.Add(room)

	rm := &RoomManager{
		registry: registry,
		config:   &Config{},
	}

	router := mux.NewRouter()
	router.HandleFunc("/api/tracker/{roomId}", rm.handleRoomWebhostApiTracker).Methods(http.MethodGet)

	b.ReportAllocs()

	for b.Loop() {
		req := httptest.NewRequest(http.MethodGet, "/api/tracker/bench-room", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", w.Code)
		}
	}
}
