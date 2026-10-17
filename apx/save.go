package main

import (
	"apx/multidata"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
)

// Mutable

type SavedDSEntry struct {
	Value       json.RawMessage `json:"value"`
	SlotWriters []string        `json:"slot_writers"`
}

type SaveState struct {
	Checked              map[string][]int64              `json:"checked"`
	ReceivedItems        map[string][]NetworkItem        `json:"received_items"`
	HintsUsed            map[string]int                  `json:"hints_used"`
	Statuses             map[string]map[int]ClientStatus `json:"statuses"`
	DataStorage          map[string]SavedDSEntry         `json:"data_storage"`
	SlotActivityTimers   map[string]int64                `json:"slot_activity_timers"`
	SlotConnectionTimers map[string]int64                `json:"slot_connection_timers"`
	SeedName             string                          `json:"seed_name"`
	Hints                []Hint                          `json:"hints"`
}

type SafeSaveHandler struct {
	handler SaveStateHandler
	mu      sync.Mutex
}

func newSafeSaveHandler(h SaveStateHandler) *SafeSaveHandler {
	return &SafeSaveHandler{handler: h}
}

func (l *SafeSaveHandler) Save(ctx context.Context, roomId string, state *SaveState) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.handler.Save(ctx, roomId, state)
}

func (l *SafeSaveHandler) Load(ctx context.Context, roomId string, seedName string) (*SaveState, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.handler.Load(ctx, roomId, seedName)
}

type SaveStateHandler interface {
	Save(ctx context.Context, roomId string, state *SaveState) error
	Load(ctx context.Context, roomId string, seedName string) (*SaveState, error)
}

func snapshotState(s *ApState) *SaveState {
	ss := &SaveState{
		SeedName:             s.Seed,
		Checked:              make(map[string][]int64),
		ReceivedItems:        make(map[string][]NetworkItem),
		HintsUsed:            make(map[string]int),
		Statuses:             make(map[string]map[int]ClientStatus),
		DataStorage:          make(map[string]SavedDSEntry),
		SlotActivityTimers:   make(map[string]int64),
		SlotConnectionTimers: make(map[string]int64),
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

	s.SlotActivityTimers.mu.RLock()
	for ts, entry := range s.SlotActivityTimers.timers {
		ss.SlotActivityTimers[teamSlotKey(ts)] = entry
	}
	s.SlotActivityTimers.mu.RUnlock()

	s.SlotConnectionTimers.mu.RLock()
	for ts, entry := range s.SlotConnectionTimers.timers {
		ss.SlotConnectionTimers[teamSlotKey(ts)] = entry
	}
	s.SlotConnectionTimers.mu.RUnlock()

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
	return filepath.Join(f.Dir, roomId+".apxsave")
}

func (f *SaveFileHandler) Save(_ context.Context, roomId string, ss *SaveState) error {
	tmp, err := os.CreateTemp(f.Dir, "apxsave-*.tmp")
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

func (f *SaveFileHandler) Load(_ context.Context, roomId string, seedName string) (*SaveState, error) {
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

	if ss.SeedName != seedName {
		log.Printf("save file seed mismatch for room %q: got %q, expected %q — deleting", roomId, ss.SeedName, seedName)
		os.Remove(f.path(roomId))
		return nil, nil
	}

	return &ss, nil
}

// --- Postgres save system ---

type SaveDBHandler struct {
	pool *pgxpool.Pool
}

func newSaveDBHandler(pool *pgxpool.Pool) *SaveDBHandler {
	return &SaveDBHandler{pool: pool}
}

func (d *SaveDBHandler) Save(ctx context.Context, roomId string, ss *SaveState) error {
	data, err := json.Marshal(ss)
	if err != nil {
		return err
	}

	_, err = d.pool.Exec(ctx, `
			INSERT INTO saved_rooms (lobby_room_id, state)
			VALUES ($1, $2)
			ON CONFLICT (lobby_room_id) DO UPDATE SET state = $2`,
		roomId, data,
	)
	return err
}

func (d *SaveDBHandler) Load(ctx context.Context, roomId string, seedName string) (*SaveState, error) {
	var data []byte
	err := d.pool.QueryRow(ctx, `SELECT state FROM saved_rooms WHERE lobby_room_id = $1`, roomId).Scan(&data)
	if err != nil {
		return nil, nil // room not found
	}

	var ss SaveState
	if err := json.Unmarshal(data, &ss); err != nil {
		return nil, err
	}

	if ss.SeedName != seedName {
		log.Printf("DB seed mismatch for room %q: got %q, expected %q — deleting", roomId, ss.SeedName, seedName)
		_, err := d.pool.Exec(ctx, `DELETE FROM saved_rooms WHERE lobby_room_id = $1`, roomId)
		if err != nil {
			log.Printf("failed to delete room %q after seed mismatch: %v", roomId, err)
		}
		return nil, nil
	}

	return &ss, nil
}

// --- ApxRoom impls ---

func (s *ApxRoom) Persist(ctx context.Context) error {
	ss := snapshotState(s.state)
	return s.saveHandler.Save(ctx, s.lobbyRoomId, ss)
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

	s.SlotActivityTimers.mu.Lock()
	for key, entry := range ss.SlotActivityTimers {
		s.SlotActivityTimers.timers[teamSlotFromKey(key)] = entry
	}
	s.SlotActivityTimers.mu.Unlock()

	s.SlotConnectionTimers.mu.Lock()
	for key, entry := range ss.SlotConnectionTimers {
		s.SlotConnectionTimers.timers[teamSlotFromKey(key)] = entry
	}
	s.SlotConnectionTimers.mu.Unlock()

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

// Immutable

type RoomDefinition struct {
	ConnectNames      map[string][2]int             `json:"connect_names"`
	PrecollectedItems map[string][]int64            `json:"precollected_items"`
	DatapackChecksums map[string]string             `json:"datapack_checksums"`
	SlotData          map[string]map[string]any     `json:"slot_data"`
	ServerOptions     map[string]any                `json:"server_options"`
	Locations         map[string]map[int64][3]int64 `json:"locations"`
	SlotMinVersions   map[string][3]int             `json:"slot_min_versions"`
	SlotInfo          map[string]SlotInfoDef        `json:"slot_info"`
	SeedName          string                        `json:"seed_name"`
	Spheres           []map[string][]int64          `json:"spheres"`
	PrecollectedHints []Hint                        `json:"precollected_hints"`
	Tags              []string                      `json:"tags"`
	Version           [3]int                        `json:"version"`
	RaceMode          int                           `json:"race_mode"`
	SlotDataIndex     map[int][2]int                `json:"slot_data_entry"` // offset, length
}

type SlotInfoDef struct {
	Name         string `json:"name"`
	Game         string `json:"game"`
	GroupMembers []int  `json:"group_members"`
	Type         int    `json:"type"`
}

func roomDefinitionFromMultidata(md *multidata.MultiData) *RoomDefinition {
	dpChecksums := make(map[string]string, len(md.DataPackage))
	for game, dp := range md.DataPackage {
		dpChecksums[game] = dp.Checksum
	}
	def := &RoomDefinition{
		SeedName:          md.SeedName,
		Tags:              make([]string, len(md.Tags)),
		RaceMode:          md.RaceMode,
		Version:           [3]int{md.Version[0], md.Version[1], md.Version[2]},
		DatapackChecksums: dpChecksums,
		Locations:         make(map[string]map[int64][3]int64),
		SlotInfo:          make(map[string]SlotInfoDef),
		SlotData:          make(map[string]map[string]any),
		SlotMinVersions:   make(map[string][3]int),
		ConnectNames:      make(map[string][2]int),
		PrecollectedItems: make(map[string][]int64),
		PrecollectedHints: make([]Hint, 0),
		Spheres:           make([]map[string][]int64, len(md.Spheres)),
		ServerOptions:     make(map[string]any),
	}

	copy(def.Tags, md.Tags)

	for slot, locs := range md.Locations {
		key := teamSlotKey(TeamSlot{Team: 0, Slot: slot})
		inner := make(map[int64][3]int64, len(locs))
		for locID, t := range locs {
			inner[locID] = [3]int64{t[0], t[1], t[2]}
		}
		def.Locations[key] = inner
	}

	for slot, info := range md.SlotInfo {
		members := make([]int, len(info.GroupMembers))
		copy(members, info.GroupMembers)
		def.SlotInfo[teamSlotKey(TeamSlot{Team: 0, Slot: slot})] = SlotInfoDef{
			Name:         info.Name,
			Game:         info.Game,
			Type:         int(info.Type),
			GroupMembers: members,
		}
	}

	for slot, data := range md.SlotData {
		cp := make(map[string]any, len(data))
		maps.Copy(cp, data)
		def.SlotData[teamSlotKey(TeamSlot{Team: 0, Slot: slot})] = cp
	}

	for slot, v := range md.MinimumVersions.Clients {
		def.SlotMinVersions[teamSlotKey(TeamSlot{Team: 0, Slot: slot})] = [3]int{v[0], v[1], v[2]}
	}

	for name, pair := range md.ConnectNames {
		def.ConnectNames[name] = [2]int{pair[0], pair[1]}
	}

	for slot, itemIDs := range md.PrecollectedItems {
		ids := make([]int64, len(itemIDs))
		copy(ids, itemIDs)
		def.PrecollectedItems[teamSlotKey(TeamSlot{Team: 0, Slot: slot})] = ids
	}

	for i, sphere := range md.Spheres {
		s := make(map[string][]int64, len(sphere))
		for slot, locIDs := range sphere {
			cp := make([]int64, len(locIDs))
			copy(cp, locIDs)
			s[teamSlotKey(TeamSlot{Team: 0, Slot: slot})] = cp
		}
		def.Spheres[i] = s
	}

	var hints []Hint
	for _, setHints := range md.PrecollectedHints {
		for _, h := range setHints {
			status := HintStatusPriority
			if h.Found {
				status = HintStatusFound
			}
			hints = append(hints, Hint{
				ReceivingPlayer: int32(h.ReceivingPlayer),
				FindingPlayer:   int32(h.FindingPlayer),
				Location:        h.Location,
				Item:            h.Item,
				ItemFlags:       int16(h.ItemFlags),
				Found:           h.Found,
				Entrance:        h.Entrance,
				Status:          status,
			})
		}
	}
	def.PrecollectedHints = hints

	maps.Copy(def.ServerOptions, md.ServerOptions)

	return def
}

func apStateFromDefinition(def *RoomDefinition) (*ApState, error) {
	locations := make(map[TeamSlot]map[int64]Location, len(def.Locations))
	for key, locs := range def.Locations {
		ts := teamSlotFromKey(key)
		inner := make(map[int64]Location, len(locs))
		for locID, t := range locs {
			inner[locID] = Location{
				Item:    t[0],
				Player:  int32(t[1]),
				Flags:   int16(t[2]),
				Checked: false,
			}
		}
		locations[ts] = inner
	}

	slotInfoMap := make(map[TeamSlot]SlotInfo, len(def.SlotInfo))
	for key, info := range def.SlotInfo {
		ts := teamSlotFromKey(key)
		members := make([]int, len(info.GroupMembers))
		copy(members, info.GroupMembers)
		slotInfoMap[ts] = SlotInfo{
			Team:         ts.Team,
			Slot:         ts.Slot,
			Name:         info.Name,
			Game:         info.Game,
			Type:         SlotType(info.Type),
			GroupMembers: members,
		}
	}

	nameToSlot := make(map[string]*SlotInfo, len(def.ConnectNames))
	for name, pair := range def.ConnectNames {
		ts := TeamSlot{Team: pair[0], Slot: pair[1]}
		if info, ok := slotInfoMap[TeamSlot{Team: 0, Slot: ts.Slot}]; ok {
			infoCopy := info
			nameToSlot[name] = &infoCopy
		}
	}

	slotMinVersions := make(map[TeamSlot]Version, len(def.SlotMinVersions))
	for key, v := range def.SlotMinVersions {
		slotMinVersions[teamSlotFromKey(key)] = Version{Major: v[0], Minor: v[1], Build: v[2]}
	}

	startInv := make(map[TeamSlot][]NetworkItem, len(def.PrecollectedItems))
	for key, ids := range def.PrecollectedItems {
		items := make([]NetworkItem, len(ids))
		for i, id := range ids {
			items[i] = NetworkItem{Item: id, Location: -2, Player: 0}
		}
		startInv[teamSlotFromKey(key)] = items
	}

	spheres := make(Spheres, len(def.Spheres))
	sphereLocs := make(SphereLocs, len(def.Spheres))
	for i, s := range def.Spheres {
		sphere := make(Sphere, len(s))
		group := make(SphereLocGroup, len(s))
		for key, locIDs := range s {
			ts := teamSlotFromKey(key)
			cp := make([]int64, len(locIDs))
			copy(cp, locIDs)
			sphere[ts] = cp
			slotLocs := locations[ts]
			locs := make(map[int64]*Location, len(locIDs))
			for _, locID := range locIDs {
				if loc, ok := slotLocs[locID]; ok {
					locCopy := loc
					locs[locID] = &locCopy
				}
			}
			group[ts] = locs
		}
		spheres[i] = sphere
		sphereLocs[i] = group
	}

	// Change with teams support in future
	playersNetwork := make([]NetworkPlayer, 0, len(def.SlotInfo))
	slotInfoNetwork := make(map[int]NetworkSlot, len(def.SlotInfo))
	for key, info := range def.SlotInfo {
		ts := teamSlotFromKey(key)
		members := make([]int, len(info.GroupMembers))
		copy(members, info.GroupMembers)
		playersNetwork = append(playersNetwork, NetworkPlayer{
			Team:  ts.Team,
			Slot:  ts.Slot,
			Alias: info.Name,
			Name:  info.Name,
		})
		slotInfoNetwork[ts.Slot] = NetworkSlot{
			Name:         info.Name,
			Game:         info.Game,
			Type:         info.Type,
			GroupMembers: members,
		}
	}

	slotInfoNetworkJson, err := json.Marshal(slotInfoNetwork)
	if err != nil {
		return nil, err
	}
	networkPlayersJson, err := json.Marshal(playersNetwork)
	if err != nil {
		return nil, err
	}

	clientStatuses := ClientStatuses{status: make(map[int]map[int]ClientStatus)}
	for key := range def.SlotInfo {
		ts := teamSlotFromKey(key)
		if clientStatuses.status[ts.Team] == nil {
			clientStatuses.status[ts.Team] = make(map[int]ClientStatus)
		}
		clientStatuses.status[ts.Team][ts.Slot] = ClientStatusUnknown
	}

	apState := &ApState{
		Checks:               newChecksState(locations),
		ReceivedItems:        newReceivedItems(),
		Hints:                newHintsState(),
		DataStorage:          newDataStorage(),
		SlotDataIndex:        def.SlotDataIndex,
		ClientStatuses:       &clientStatuses,
		SlotActivityTimers:   newSlotTimers(),
		SlotConnectionTimers: newSlotTimers(),
		Locations:            locations,
		Spheres:              spheres,
		SphereLocs:           sphereLocs,
		NameToSlot:           nameToSlot,
		SlotInfo:             slotInfoMap,
		SlotMinVersions:      slotMinVersions,
		SlotStartInventory:   startInv,
		Version:              Version{Major: def.Version[0], Minor: def.Version[1], Build: def.Version[2]},
		ServerOptions:        parseEmbeddedServerOptions(def.ServerOptions),
		Tags:                 def.Tags,
		Seed:                 def.SeedName,
		RaceMode:             def.RaceMode,
		PlayersNetwork:       NetworkPlayers{players: playersNetwork, rawJson: networkPlayersJson},
		SlotInfoNetwork:      slotInfoNetwork,
		SlotInfoNetworkJson:  slotInfoNetworkJson,
	}
	for _, hint := range def.PrecollectedHints {
		apState.Hints.AddSlotHint(hint)
	}
	return apState, nil
}

func (s *ApxRoom) populateReadData() {
	state := s.state
	ds := state.DataStorage

	// race_mode — static scalar from multidata
	raceMode := int64(state.RaceMode)
	ds.RegisterReadKey("race_mode", func() StorageValue {
		return raceMode
	})

	// Per-slot keys — all team 0 at load time
	for ts := range state.SlotInfo {
		ts := ts // capture
		slot := ts.Slot

		// slot_data_{slot} — static, captured at load
		ds.RegisterReadKey(fmt.Sprintf("slot_data_%d", slot), func() StorageValue {
			data, err := s.GetSlotData(ts)
			if err != nil {
				return []byte{}
			}
			return data
		})

		// hints_{team}_{slot} — dynamic, reads live hint state
		ds.RegisterReadKey(fmt.Sprintf("hints_%d_%d", ts.Team, slot), func() StorageValue {
			return state.Hints.GetSlotHints(ts)
		})

		// client_status_{team}_{slot} — not tracked in ApState yet, return 0 (CLIENT_UNKNOWN)
		// TODO: wire up to client_game_state equivalent when implemented
		ds.RegisterReadKey(fmt.Sprintf("client_status_%d_%d", ts.Team, slot), func() StorageValue {
			return int64(0)
		})
	}
}

func teamSlotFromKey(key string) TeamSlot {
	var team, slot int
	fmt.Sscanf(key, "%d:%d", &team, &slot)
	return TeamSlot{Team: team, Slot: slot}
}
