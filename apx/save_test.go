package main

import (
	"context"
	"os"
	"testing"
)

func makeTestSaveState() *SaveState {
	return &SaveState{
		Checked: map[string][]int64{
			"0:1": {100, 200, 300},
			"0:2": {400, 500},
		},
		ReceivedItems: map[string][]NetworkItem{
			"0:1:1": {
				{Item: 10, Location: 100, Player: 1, Flags: 0},
				{Item: 11, Location: 200, Player: 2, Flags: 1},
			},
		},
		Hints: []Hint{
			{
				ReceivingPlayer: 1,
				FindingPlayer:   2,
				Location:        100,
				Item:            10,
				ItemFlags:       0,
				Found:           false,
				Status:          HintStatusPriority,
			},
		},
		HintsUsed: map[string]int{
			"0:1": 2,
			"0:2": 1,
		},
		Statuses: map[string]map[int]ClientStatus{
			"0": {
				1: ClientStatusGoal,
				2: ClientStatusPlaying,
			},
		},
		DataStorage: map[string]SavedDSEntry{
			"my_key": {
				Value:       []byte(`42`),
				SlotWriters: []string{"0:1"},
			},
			"my_map": {
				Value:       []byte(`{"foo":"bar"}`),
				SlotWriters: []string{"0:1", "0:2"},
			},
		},
	}
}

func TestFileSaveRoundTrip(t *testing.T) {
	dir := t.TempDir()
	handler := &SaveFileHandler{Dir: dir}
	ctx := context.Background()
	roomId := "test-room"

	original := makeTestSaveState()

	if err := handler.Save(ctx, roomId, original); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(handler.path(roomId)); err != nil {
		t.Fatalf("save file not found: %v", err)
	}

	loaded, err := handler.Load(ctx, roomId)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("Load returned nil")
	}

	assertSaveStateEqual(t, original, loaded)
}

func TestFileLoadMissing(t *testing.T) {
	handler := &SaveFileHandler{Dir: t.TempDir()}
	ss, err := handler.Load(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("expected nil error for missing file, got: %v", err)
	}
	if ss != nil {
		t.Fatal("expected nil SaveState for missing file")
	}
}

func TestApplySnapshot(t *testing.T) {
	ss := makeTestSaveState()

	// Build a minimal ApState to apply into
	locations := map[TeamSlot]map[int64]Location{
		{0, 1}: {100: {}, 200: {}, 300: {}},
		{0, 2}: {400: {}, 500: {}},
	}
	state := &ApState{
		Checks:        newChecksState(locations),
		ReceivedItems: newReceivedItems(),
		Hints:         newHintsState(),
		DataStorage:   newDataStorage(),
		ClientStatuses: &ClientStatuses{
			status: make(map[int]map[int]ClientStatus),
		},
	}

	if err := applySnapshot(state, ss); err != nil {
		t.Fatalf("applySnapshot failed: %v", err)
	}

	// Checks
	for _, id := range []int64{100, 200, 300} {
		if !state.Checks.IsChecked(TeamSlot{0, 1}, id) {
			t.Errorf("expected loc %d to be checked for slot 1", id)
		}
	}
	for _, id := range []int64{400, 500} {
		if !state.Checks.IsChecked(TeamSlot{0, 2}, id) {
			t.Errorf("expected loc %d to be checked for slot 2", id)
		}
	}

	// ReceivedItems
	items := state.ReceivedItems.Get(ReceivedItemsKey{0, 1, true})
	if len(items) != 2 {
		t.Errorf("expected 2 received items, got %d", len(items))
	}

	// Hints
	state.Hints.mu.RLock()
	if len(state.Hints.hints) != 1 {
		t.Errorf("expected 1 hint, got %d", len(state.Hints.hints))
	}
	if state.Hints.hints[0].FindingPlayer != 2 {
		t.Errorf("expected finding player 2, got %d", state.Hints.hints[0].FindingPlayer)
	}
	state.Hints.mu.RUnlock()

	// HintsUsed
	if used := state.Hints.GetHintsUsed(TeamSlot{0, 1}); used != 2 {
		t.Errorf("expected 2 hints used for slot 1, got %d", used)
	}

	// ClientStatuses
	if s := state.ClientStatuses.Get(0, 1); s != ClientStatusGoal {
		t.Errorf("expected ClientStatusGoal for slot 1, got %v", s)
	}

	// DataStorage — int value
	val, ok := state.DataStorage.Get("my_key")
	if !ok {
		t.Error("expected my_key in DataStorage")
	}
	if val != int64(42) {
		t.Errorf("expected int64(42), got %T(%v)", val, val)
	}

	// DataStorage — keyCount restored
	state.DataStorage.mu.RLock()
	if state.DataStorage.keyCount[TeamSlot{0, 1}] != 2 { // my_key + my_map
		t.Errorf("expected keyCount 2 for slot 1, got %d", state.DataStorage.keyCount[TeamSlot{0, 1}])
	}
	state.DataStorage.mu.RUnlock()
}

func TestFileSaveApplyRoundTrip(t *testing.T) {
	dir := t.TempDir()
	handler := &SaveFileHandler{Dir: dir}
	ctx := context.Background()

	original := makeTestSaveState()
	if err := handler.Save(ctx, "room", original); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := handler.Load(ctx, "room")
	if err != nil || loaded == nil {
		t.Fatalf("Load: %v", err)
	}

	locations := map[TeamSlot]map[int64]Location{
		{0, 1}: {100: {}, 200: {}, 300: {}},
		{0, 2}: {400: {}, 500: {}},
	}
	state := &ApState{
		Checks:        newChecksState(locations),
		ReceivedItems: newReceivedItems(),
		Hints:         newHintsState(),
		DataStorage:   newDataStorage(),
		ClientStatuses: &ClientStatuses{
			status: make(map[int]map[int]ClientStatus),
		},
	}

	if err := applySnapshot(state, loaded); err != nil {
		t.Fatalf("applySnapshot: %v", err)
	}

	// Spot check: hint index rebuilt correctly
	if !state.Hints.HintExists(2, 100) {
		t.Error("hint index not rebuilt: HintExists(2, 100) returned false")
	}
}

// --- helpers ---

func assertSaveStateEqual(t *testing.T, want, got *SaveState) {
	t.Helper()

	// Checked
	for key, wantIDs := range want.Checked {
		gotIDs := got.Checked[key]
		if len(wantIDs) != len(gotIDs) {
			t.Errorf("Checked[%s]: want %d ids, got %d", key, len(wantIDs), len(gotIDs))
		}
	}

	// Hints
	if len(want.Hints) != len(got.Hints) {
		t.Errorf("Hints: want %d, got %d", len(want.Hints), len(got.Hints))
	} else {
		for i, wh := range want.Hints {
			gh := got.Hints[i]
			if wh.FindingPlayer != gh.FindingPlayer || wh.Location != gh.Location || wh.Status != gh.Status {
				t.Errorf("Hints[%d] mismatch: want %+v, got %+v", i, wh, gh)
			}
		}
	}

	// HintsUsed
	for key, wantUsed := range want.HintsUsed {
		if got.HintsUsed[key] != wantUsed {
			t.Errorf("HintsUsed[%s]: want %d, got %d", key, wantUsed, got.HintsUsed[key])
		}
	}

	// Statuses
	for teamKey, wantSlots := range want.Statuses {
		for slot, wantStatus := range wantSlots {
			if got.Statuses[teamKey][slot] != wantStatus {
				t.Errorf("Statuses[%s][%d]: want %v, got %v", teamKey, slot, wantStatus, got.Statuses[teamKey][slot])
			}
		}
	}

	// DataStorage keys present
	for k := range want.DataStorage {
		if _, ok := got.DataStorage[k]; !ok {
			t.Errorf("DataStorage missing key %q", k)
		}
	}
}
