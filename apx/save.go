package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
)

type SavedDSEntry struct {
	Value       json.RawMessage `json:"value"`
	SlotWriters []string        `json:"slot_writers"`
}

type SaveState struct {
	Checked       map[string][]int64              `json:"checked"`
	ReceivedItems map[string][]NetworkItem        `json:"received_items"`
	Hints         []Hint                          `json:"hints"`
	HintsUsed     map[string]int                  `json:"hints_used"`
	Statuses      map[string]map[int]ClientStatus `json:"statuses"`
	DataStorage   map[string]SavedDSEntry         `json:"data_storage"`
}

type SafeSaveHandler struct {
	mu      sync.Mutex
	handler SaveStateHandler
}

func newSafeSaveHandler(h SaveStateHandler) *SafeSaveHandler {
	return &SafeSaveHandler{handler: h}
}

func (l *SafeSaveHandler) Save(ctx context.Context, roomId string, state *SaveState) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.handler.Save(ctx, roomId, state)
}

func (l *SafeSaveHandler) Load(ctx context.Context, roomId string) (*SaveState, error) {
	// Load doesn't need the lock — only called once at startup before any saves
	return l.handler.Load(ctx, roomId)
}

type SaveStateHandler interface {
	Save(ctx context.Context, roomId string, state *SaveState) error
	Load(ctx context.Context, roomId string) (*SaveState, error)
}

func snapshotState(s *ApState) *SaveState {
	ss := &SaveState{
		Checked:       make(map[string][]int64),
		ReceivedItems: make(map[string][]NetworkItem),
		HintsUsed:     make(map[string]int),
		Statuses:      make(map[string]map[int]ClientStatus),
		DataStorage:   make(map[string]SavedDSEntry),
	}

	s.Checks.mu.RLock()
	for ts, locs := range s.Checks.checked {
		ss.Checked[teamSlotKey(ts)] = locIDsToArray(locs)
	}
	s.Checks.mu.RUnlock()

	s.ReceivedItems.mu.RLock()
	for k, items := range s.ReceivedItems.receivedItems {
		cp := make([]NetworkItem, len(items))
		copy(cp, items)
		ss.ReceivedItems[receivedItemsKey(k)] = cp
	}
	s.ReceivedItems.mu.RUnlock()

	s.Hints.mu.RLock()
	ss.Hints = make([]Hint, len(s.Hints.hints))
	copy(ss.Hints, s.Hints.hints)
	for ts, used := range s.Hints.hintsUsed {
		ss.HintsUsed[teamSlotKey(ts)] = used
	}
	s.Hints.mu.RUnlock()

	s.ClientStatuses.mu.RLock()
	for team, slots := range s.ClientStatuses.status {
		key := fmt.Sprintf("%d", team)
		ss.Statuses[key] = make(map[int]ClientStatus, len(slots))
		for slot, status := range slots {
			ss.Statuses[key][slot] = status
		}
	}
	s.ClientStatuses.mu.RUnlock()

	s.DataStorage.mu.RLock()
	for k, entry := range s.DataStorage.data {
		if strings.HasPrefix(k, "_read_") {
			continue
		}
		raw, err := json.Marshal(entry.value)
		if err != nil {
			continue
		}
		writers := make([]string, 0, len(entry.slotWriters))
		for ts := range entry.slotWriters {
			writers = append(writers, teamSlotKey(ts))
		}
		ss.DataStorage[k] = SavedDSEntry{Value: raw, SlotWriters: writers}
	}
	s.DataStorage.mu.RUnlock()

	return ss
}

// --- File save system ---

type SaveFileHandler struct {
	Dir string
}

func newSaveFileHandler(dir string) *SaveFileHandler {
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Panicf("failed to create save directory %q: %v", dir, err)
	}
	return &SaveFileHandler{Dir: dir}
}

func (f *SaveFileHandler) path(roomId string) string {
	return filepath.Join(f.Dir, roomId+".apstate.zst")
}

func (f *SaveFileHandler) Save(_ context.Context, roomId string, ss *SaveState) error {
	tmp, err := os.CreateTemp(f.Dir, "apstate-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	w, err := zstd.NewWriter(tmp, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := json.NewEncoder(w).Encode(ss); err != nil {
		w.Close()
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := w.Close(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	dest := f.path(roomId)
	if err := copyFile(tmpPath, dest); err != nil {
		os.Remove(tmpPath)
		return err
	}
	os.Remove(tmpPath)
	return nil
}

func (f *SaveFileHandler) Load(_ context.Context, roomId string) (*SaveState, error) {
	file, err := os.Open(f.path(roomId))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	r, err := zstd.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var ss SaveState
	if err := json.NewDecoder(r).Decode(&ss); err != nil {
		return nil, err
	}
	return &ss, nil
}

// --- Postgres save system ---

type SaveDBHandler struct {
	DB *pgxpool.Pool
}

func (d *SaveDBHandler) Save(ctx context.Context, roomId string, ss *SaveState) error {
	tx, err := d.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
        INSERT INTO rooms (id, closed_at)
        VALUES ($1, NOW())
        ON CONFLICT (id) DO UPDATE SET closed_at = NOW()`,
		roomId,
	)
	if err != nil {
		return err
	}

	for key, ids := range ss.Checked {
		var team, slot int
		fmt.Sscanf(key, "%d:%d", &team, &slot)
		_, err = tx.Exec(ctx, `
            INSERT INTO slot_checks (room_id, team, slot, checked_locations)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (room_id, team, slot) DO UPDATE SET checked_locations = $4`,
			roomId, team, slot, ids,
		)
		if err != nil {
			return err
		}
	}

	for _, hint := range ss.Hints {
		_, err = tx.Exec(ctx, `
            INSERT INTO hints (room_id, finding_player, receiving_player, location, item, found, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			roomId, hint.FindingPlayer, hint.ReceivingPlayer,
			hint.Location, hint.Item, hint.Found, hint.Status,
		)
		if err != nil {
			return err
		}
	}

	for teamStr, slots := range ss.Statuses {
		var team int
		fmt.Sscanf(teamStr, "%d", &team)
		for slot, status := range slots {
			_, err = tx.Exec(ctx, `
                INSERT INTO client_statuses (room_id, team, slot, status)
                VALUES ($1, $2, $3, $4)`,
				roomId, team, slot, status,
			)
			if err != nil {
				return err
			}
		}
	}

	dsJSON, err := json.Marshal(ss.DataStorage)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
        UPDATE rooms SET data_storage = $1 WHERE id = $2`,
		dsJSON, roomId,
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (d *SaveDBHandler) Load(ctx context.Context, roomId string) (*SaveState, error) {
	ss := &SaveState{
		Checked:       make(map[string][]int64),
		ReceivedItems: make(map[string][]NetworkItem),
		HintsUsed:     make(map[string]int),
		Statuses:      make(map[string]map[int]ClientStatus),
		DataStorage:   make(map[string]SavedDSEntry),
	}

	var dsJSON []byte
	err := d.DB.QueryRow(ctx, `SELECT data_storage FROM rooms WHERE id = $1`, roomId).Scan(&dsJSON)
	if err != nil {
		return nil, nil // room not found
	}
	if dsJSON != nil {
		json.Unmarshal(dsJSON, &ss.DataStorage)
	}

	rows, err := d.DB.Query(ctx, `SELECT team, slot, checked_locations FROM slot_checks WHERE room_id = $1`, roomId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var team, slot int
		var ids []int64
		if err := rows.Scan(&team, &slot, &ids); err != nil {
			return nil, err
		}
		ss.Checked[fmt.Sprintf("%d:%d", team, slot)] = ids
	}

	hintRows, err := d.DB.Query(ctx, `
        SELECT finding_player, receiving_player, location, item, found, status
        FROM hints WHERE room_id = $1`, roomId)
	if err != nil {
		return nil, err
	}
	defer hintRows.Close()
	for hintRows.Next() {
		var h Hint
		if err := hintRows.Scan(&h.FindingPlayer, &h.ReceivingPlayer, &h.Location, &h.Item, &h.Found, &h.Status); err != nil {
			return nil, err
		}
		ss.Hints = append(ss.Hints, h)
	}

	statusRows, err := d.DB.Query(ctx, `SELECT team, slot, status FROM client_statuses WHERE room_id = $1`, roomId)
	if err != nil {
		return nil, err
	}
	defer statusRows.Close()
	for statusRows.Next() {
		var team, slot int
		var status ClientStatus
		if err := statusRows.Scan(&team, &slot, &status); err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%d", team)
		if ss.Statuses[key] == nil {
			ss.Statuses[key] = make(map[int]ClientStatus)
		}
		ss.Statuses[key][slot] = status
	}

	return ss, nil
}

// --- ApxRoom impls ---

func (s *ApxRoom) Persist(ctx context.Context) error {
	ss := snapshotState(s.state)
	return s.saveHandler.Save(ctx, s.lobbyRoomId, ss)
}

func (s *ApxRoom) RestoreFrom(ctx context.Context) error {
	ss, err := s.saveHandler.Load(ctx, s.lobbyRoomId)
	if err != nil || ss == nil {
		return err
	}
	return applySnapshot(s.state, ss)
}

// --- Helpers and stuff ---

func applySnapshot(s *ApState, ss *SaveState) error {
	s.Checks.mu.Lock()
	for key, ids := range ss.Checked {
		var team, slot int
		fmt.Sscanf(key, "%d:%d", &team, &slot)
		ts := TeamSlot{team, slot}
		if s.Checks.checked[ts] == nil {
			s.Checks.checked[ts] = make(map[int64]struct{})
		}
		for _, id := range ids {
			s.Checks.checked[ts][id] = struct{}{}
		}
	}
	s.Checks.mu.Unlock()

	s.ReceivedItems.mu.Lock()
	for key, items := range ss.ReceivedItems {
		var team, slot, remote int
		fmt.Sscanf(key, "%d:%d:%d", &team, &slot, &remote)
		k := ReceivedItemsKey{Team: team, Slot: slot, RemoteItems: remote == 1}
		cp := make([]NetworkItem, len(items))
		copy(cp, items)
		s.ReceivedItems.receivedItems[k] = cp
	}
	s.ReceivedItems.mu.Unlock()

	s.Hints.mu.Lock()
	s.Hints.hints = make([]Hint, len(ss.Hints))
	copy(s.Hints.hints, ss.Hints)
	s.Hints.rebuildIndices()
	for key, used := range ss.HintsUsed {
		var team, slot int
		fmt.Sscanf(key, "%d:%d", &team, &slot)
		s.Hints.hintsUsed[TeamSlot{team, slot}] = used
	}
	s.Hints.mu.Unlock()

	s.ClientStatuses.mu.Lock()
	for teamStr, slots := range ss.Statuses {
		var team int
		fmt.Sscanf(teamStr, "%d", &team)
		if s.ClientStatuses.status[team] == nil {
			s.ClientStatuses.status[team] = make(map[int]ClientStatus)
		}
		for slot, status := range slots {
			s.ClientStatuses.status[team][slot] = status
		}
	}
	s.ClientStatuses.mu.Unlock()

	// Skip _read_ keys, they're repopulated by populateReadData
	s.DataStorage.mu.Lock()
	for k, saved := range ss.DataStorage {
		if strings.HasPrefix(k, "_read_") {
			continue
		}
		var v any
		if err := json.Unmarshal(saved.Value, &v); err != nil {
			continue
		}
		// Restore numeric types — JSON unmarshals numbers as float64 by default
		v = normalizeStorageValue(v)

		writers := make(map[TeamSlot]struct{}, len(saved.SlotWriters))
		for _, key := range saved.SlotWriters {
			var team, slot int
			fmt.Sscanf(key, "%d:%d", &team, &slot)
			ts := TeamSlot{team, slot}
			writers[ts] = struct{}{}
			s.DataStorage.keyCount[ts]++
		}
		s.DataStorage.data[k] = &StorageEntry{
			value:       v,
			slotWriters: writers,
		}
	}
	s.DataStorage.mu.Unlock()

	return nil
}

func teamSlotKey(ts TeamSlot) string {
	return fmt.Sprintf("%d:%d", ts.Team, ts.Slot)
}

func receivedItemsKey(k ReceivedItemsKey) string {
	remote := 0
	if k.RemoteItems {
		remote = 1
	}
	return fmt.Sprintf("%d:%d:%d", k.Team, k.Slot, remote)
}

func locIDsToArray(checked map[int64]struct{}) []int64 {
	ids := make([]int64, 0, len(checked))
	for id := range checked {
		ids = append(ids, id)
	}
	return ids
}

func normalizeStorageValue(v any) any {
	switch val := v.(type) {
	case float64:
		// If it's a whole number, restore as int64
		if val == math.Trunc(val) && !math.IsInf(val, 0) {
			return int64(val)
		}
		return val
	case []any:
		for i, item := range val {
			val[i] = normalizeStorageValue(item)
		}
		return val
	case map[string]any:
		for k, item := range val {
			val[k] = normalizeStorageValue(item)
		}
		return val
	default:
		return v
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}

	return out.Close()
}
