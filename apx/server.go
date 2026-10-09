package main

import (
	"apx/multidata"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/time/rate"
)

type fullFeedStore struct {
	mu            sync.RWMutex
	fullFeedSlots map[int]struct{}
}

func newFullFeedStore() *fullFeedStore {
	return &fullFeedStore{fullFeedSlots: make(map[int]struct{})}
}

func (ffs *fullFeedStore) Get() map[int]struct{} {
	ffs.mu.RLock()
	defer ffs.mu.RUnlock()
	result := make(map[int]struct{}, len(ffs.fullFeedSlots))
	maps.Copy(result, ffs.fullFeedSlots)
	return result
}

func (ffs *fullFeedStore) Allowed(slotId int) bool {
	ffs.mu.RLock()
	defer ffs.mu.RUnlock()
	_, ok := ffs.fullFeedSlots[slotId]
	return ok
}

func (ffs *fullFeedStore) Set(slotId int) {
	ffs.mu.Lock()
	defer ffs.mu.Unlock()
	ffs.fullFeedSlots[slotId] = struct{}{}
}

func (ffs *fullFeedStore) Delete(slotId int) {
	ffs.mu.Lock()
	defer ffs.mu.Unlock()
	delete(ffs.fullFeedSlots, slotId)
}

type passwordStore struct {
	mu        sync.RWMutex
	passwords map[int]string
}

func newPasswordStore() *passwordStore {
	return &passwordStore{passwords: make(map[int]string)}
}

func (ps *passwordStore) Get(slotId int) (string, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	p, ok := ps.passwords[slotId]
	return p, ok
}

func (ps *passwordStore) Set(slotId int, password string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.passwords[slotId] = password
}

func (ps *passwordStore) Delete(slotId int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.passwords, slotId)
}

type debugTap struct {
	slots []debugTapSlot
}

type debugTapSlot struct {
	mu        sync.RWMutex
	listeners []chan []byte
}

func newDebugTap(slotCount int) *debugTap {
	// + 1 since slots start at 1.
	return &debugTap{slots: make([]debugTapSlot, slotCount+1)}
}

type ConnectNames struct {
	mu    sync.RWMutex
	names map[string]string
}

func newAltConnectNames() *ConnectNames {
	return &ConnectNames{
		names: make(map[string]string),
	}
}

func (cn *ConnectNames) SetAltName(realName string, connectName string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.names[connectName] = realName
}

func (cn *ConnectNames) RemoveAltName(connectName string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	delete(cn.names, connectName)
}

func (cn *ConnectNames) GetAltName(connectName string) *string {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	realName, ok := cn.names[connectName]
	if ok {
		return &realName
	}
	return nil
}

func (cn *ConnectNames) GetAltNamesBySlot(realName string) []string {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	altNames := make([]string, 0)
	for connectName, slot := range cn.names {
		if slot == realName {
			altNames = append(altNames, connectName)
		}
	}
	return altNames
}

type RoomInfoStore struct {
	mu  sync.RWMutex
	msg RoomInfoMessage
	// immutable
	DatapackageChecksums map[string]string
}

func newRoomInfoStore(dpChecksums map[string]string) *RoomInfoStore {
	checksums := make(map[string]string, len(dpChecksums))
	maps.Copy(checksums, dpChecksums)
	return &RoomInfoStore{
		DatapackageChecksums: checksums,
	}
}

func (ris *RoomInfoStore) Load() RoomInfoMessage {
	ris.mu.RLock()
	defer ris.mu.RUnlock()
	msg := ris.msg
	msg.Time = float64(time.Now().UnixNano()) / 1e9
	return msg
}

func (ris *RoomInfoStore) GetDataPackageChecksums() map[string]string {
	ris.mu.RLock()
	defer ris.mu.RUnlock()
	copy := make(map[string]string, len(ris.DatapackageChecksums))
	for k, v := range ris.DatapackageChecksums {
		copy[k] = v
	}
	return copy
}

func (ris *RoomInfoStore) Store(msg RoomInfoMessage) {
	ris.mu.Lock()
	defer ris.mu.Unlock()
	ris.msg = msg
}

func (ris *RoomInfoStore) HasPassword() bool {
	ris.mu.RLock()
	defer ris.mu.RUnlock()
	return ris.msg.Password
}

type ApxRoom struct {
	perSlotPasswords  bool
	lastActivity      *atomic.Int64
	logf              func(f string, v ...any)
	config            *Config
	roomInfo          *RoomInfoStore
	altConnectNames   *ConnectNames
	passwords         *passwordStore
	fullFeed          *fullFeedStore
	bounceInfo        *bounceInfoStore
	connections       *connectionRegistry
	datapackages      *DataPackageStore
	metrics           *metrics
	lobbyRoomId       string
	debugTap          *debugTap
	lokiLogger        *LokiLogger
	logDeath          func(slotId int)
	ipLimiter         *IPRateLimiter
	chatCommandRouter *chatCommandRouter
	bcUnorderedCh     chan func()
	bcOrderedCh       chan func()
	state             *ApState
}

type Location struct {
	Item    int64
	Player  int16
	Flags   int16
	Checked bool
}

type Version struct {
	Major int
	Minor int
	Build int
}

type NetworkItem struct {
	Item     int64
	Location int64
	Player   int16
	Flags    int16
}

type Sphere map[TeamSlot][]int64
type Spheres []Sphere

type SphereLocGroup map[TeamSlot]map[int64]*Location
type SphereLocs []SphereLocGroup

type ServerOptions struct {
	mu                  sync.RWMutex
	password            string // "password" - default ""
	hintCost            int    // "hint_cost" - default 10
	locationCheckPoints int    // "location_check_points" - default 1
	releaseMode         string // "release_mode" - default "auto"
	collectMode         string // "collect_mode" - default "auto"
	remainingMode       string // "remaining_mode" - default "goal"
	countdownMode       string // "countdown_mode" - default "auto"
	disableItemCheat    bool   // "disable_item_cheat" - default false
	compatibility       int    // "compatibility" - default 2
}

func (so *ServerOptions) GetPassword() string {
	so.mu.RLock()
	defer so.mu.RUnlock()
	return so.password
}

func (so *ServerOptions) GetHintCost() int {
	so.mu.RLock()
	defer so.mu.RUnlock()
	return so.hintCost
}

func (so *ServerOptions) SetHintCost(hintCost int) {
	so.mu.Lock()
	defer so.mu.Unlock()
	so.hintCost = hintCost
}

func (so *ServerOptions) PasswordCheck(password *string) bool {
	so.mu.RLock()
	defer so.mu.RUnlock()
	if so.password == "" {
		return true
	}
	if password == nil {
		return false
	}
	return so.password == *password
}

type TeamSlot struct {
	Team int
	Slot int
}

type Hint struct {
	ReceivingPlayer int32
	FindingPlayer   int32
	Location        int64
	Item            int64
	ItemFlags       int16
	Status          HintStatus
	Found           bool
	Entrance        string
}

type SlotType int

const (
	SlotTypeSpectator SlotType = 0
	SlotTypePlayer    SlotType = 1
	SlotTypeGroup     SlotType = 2
)

type SlotInfo struct {
	Team         int
	Slot         int
	Name         string
	Game         string
	Type         SlotType
	GroupMembers []int
}

type NetworkPlayers struct {
	mu      sync.RWMutex
	players []NetworkPlayer
}

func (np *NetworkPlayers) SetAlias(slotId int, alias string) {
	np.mu.Lock()
	defer np.mu.Unlock()
	for i := range np.players {
		if np.players[i].Slot == slotId {
			if alias == "" {
				np.players[i].Alias = np.players[i].Name
			} else {
				np.players[i].Alias = alias
			}
			return
		}
	}
}

func (np *NetworkPlayers) Get() []NetworkPlayer {
	np.mu.RLock()
	defer np.mu.RUnlock()
	out := make([]NetworkPlayer, len(np.players))
	copy(out, np.players)
	return out
}

type Checks struct {
	mu        sync.RWMutex
	locations map[TeamSlot]map[int64]Location // ref to immutable
	checked   map[TeamSlot]map[int64]struct{}
}

func newChecksState(locations map[TeamSlot]map[int64]Location) Checks {
	checked := make(map[TeamSlot]map[int64]struct{}, len(locations))
	for ts := range locations {
		checked[ts] = make(map[int64]struct{})
	}
	return Checks{locations: locations, checked: checked}
}

func (ls *Checks) IsChecked(ts TeamSlot, locID int64) bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	_, ok := ls.checked[ts][locID]
	return ok
}

func (ls *Checks) GetChecked(ts TeamSlot) []int64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	ids := make([]int64, 0, len(ls.checked[ts]))
	for id := range ls.checked[ts] {
		ids = append(ids, id)
	}
	return ids
}

func (ls *Checks) GetMissing(ts TeamSlot) []int64 {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	checked := ls.checked[ts]
	missing := make([]int64, 0, len(ls.locations[ts])-len(checked))
	for id := range ls.locations[ts] {
		if _, ok := checked[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func (s *ApxRoom) submitBroadcast(fn func()) {
	select {
	case s.bcUnorderedCh <- fn:
	default:
		go fn()
	}
}

func (s *ApxRoom) submitOrdered(fn func()) {
	s.bcOrderedCh <- fn
}

func (s *ApxRoom) notifyHintsChanged(ctx context.Context, affectedSlots map[TeamSlot]struct{}) {
	for ts := range affectedSlots {
		key := fmt.Sprintf("hints_%d_%d", ts.Team, ts.Slot)
		val, _ := s.state.DataStorage.Get("_read_" + key)
		clients := s.state.DataStorage.GetNotifyClients(key)
		if len(clients) == 0 {
			continue
		}
		reply := SetReplyMessage{
			Key:   "_read_" + key,
			Value: val,
		}
		s.submitOrdered(func() {
			for _, c := range clients {
				wsjson.Write(ctx, c.clientConn, []any{reply})
			}
		})
	}
}

func (s *ApxRoom) RegisterChecks(ctx context.Context, connState *connectionState, teamSlot TeamSlot, locations []int64) error {
	newChecks := make(map[int64]struct{})

	// Mark new locations as found, and collect the items found
	s.state.Checks.mu.Lock()

	checked := s.state.Checks.checked[teamSlot]
	known := s.state.Locations[teamSlot]

	var newItems []NetworkItem
	for _, locID := range locations {
		if _, alreadyChecked := checked[locID]; alreadyChecked {
			continue
		}
		loc, ok := known[locID]
		if !ok {
			continue // ignore unknown location IDs
		}
		checked[locID] = struct{}{}
		newChecks[locID] = struct{}{}
		newItems = append(newItems, NetworkItem{
			Item:     loc.Item,
			Location: locID,
			Player:   loc.Player,
			Flags:    loc.Flags,
		})
	}

	s.state.Checks.mu.Unlock()

	// Send items out to relevant clients
	if len(newItems) > 0 {
		s.state.ReceivedItems.mu.Lock()
		for _, item := range newItems {
			keyAll := ReceivedItemsKey{Team: teamSlot.Team, Slot: int(item.Player), RemoteItems: true}
			s.state.ReceivedItems.receivedItems[keyAll] = append(s.state.ReceivedItems.receivedItems[keyAll], item)
			if int(item.Player) != teamSlot.Slot {
				keyForeign := ReceivedItemsKey{Team: teamSlot.Team, Slot: int(item.Player), RemoteItems: false}
				s.state.ReceivedItems.receivedItems[keyForeign] = append(s.state.ReceivedItems.receivedItems[keyForeign], item)
			}
		}
		s.state.ReceivedItems.mu.Unlock()

		affectedHintSlots := make(map[TeamSlot]struct{})
		for _, item := range newItems {
			if hint, changed := s.state.Hints.UpdateStatusIfExists(int32(teamSlot.Slot), item.Location, HintStatusFound, true); changed {
				affectedHintSlots[TeamSlot{0, int(hint.ReceivingPlayer)}] = struct{}{}
				affectedHintSlots[TeamSlot{0, int(hint.FindingPlayer)}] = struct{}{}
			}
		}
		if len(affectedHintSlots) > 0 {
			s.notifyHintsChanged(ctx, affectedHintSlots)
		}

		var printJsonMsgs []any
		for _, item := range newItems {
			printJsonMsgs = append(printJsonMsgs, formatSendItemMessage(item, int(item.Player)))
		}

		if len(printJsonMsgs) > 0 {
			s.connections.mu.RLock()
			var allClients []*RegisteredClient
			for _, clients := range s.connections.clients {
				allClients = append(allClients, clients...)
			}
			s.connections.mu.RUnlock()

			s.submitBroadcast(func() {
				BroadcastJSON(ctx, allClients, printJsonMsgs)
			})
		}

		affectedSlots := make(map[TeamSlot]struct{})
		for _, item := range newItems {
			affectedSlots[TeamSlot{0, int(item.Player)}] = struct{}{}
		}

		if len(affectedSlots) > 0 {
			s.SendNewItems(ctx, affectedSlots)
		}
	}

	// Send room update to sender
	if len(newChecks) > 0 {
		checkedList := make([]int64, 0, len(newChecks))
		for locID := range newChecks {
			checkedList = append(checkedList, locID)
		}
		return wsjson.Write(ctx, connState.clientConn, []any{RoomUpdateMessage{
			CheckedLocations: checkedList,
			HintPoints:       new(100),
		}})
	}

	return nil
}

func formatSendItemMessage(item NetworkItem, receivingPlayer int) PrintJsonMessage {
	sender := int(item.Player)

	// TODO: item flags
	var parts []JsonMessagePart
	if sender == receivingPlayer {
		parts = []JsonMessagePart{
			{Type: "player_id", Text: fmt.Sprintf("%d", sender)},
			{Type: "text", Text: " found their "},
			{Type: "item_id", Text: fmt.Sprintf("%d", item.Item), Player: sender},
			{Type: "text", Text: " ("},
			{Type: "location_id", Text: fmt.Sprintf("%d", item.Location), Player: sender},
			{Type: "text", Text: ")"},
		}
	} else {
		parts = []JsonMessagePart{
			{Type: "player_id", Text: fmt.Sprintf("%d", sender)},
			{Type: "text", Text: " sent "},
			{Type: "item_id", Text: fmt.Sprintf("%d", item.Item), Player: receivingPlayer},
			{Type: "text", Text: " to "},
			{Type: "player_id", Text: fmt.Sprintf("%d", receivingPlayer)},
			{Type: "text", Text: " ("},
			{Type: "location_id", Text: fmt.Sprintf("%d", item.Location), Player: sender},
			{Type: "text", Text: ")"},
		}
	}

	return PrintJsonMessage{
		Type:      "ItemSend",
		Data:      parts,
		Item:      &item,
		Receiving: new(receivingPlayer),
	}
}

func (s *ApxRoom) SendNewItems(ctx context.Context, affectedSlots map[TeamSlot]struct{}) {
	type pendingSend struct {
		conn *websocket.Conn
		msg  ReceivedItemsMessage
	}
	var sends []pendingSend

	s.connections.mu.RLock()
	for teamSlot := range affectedSlots {
		for _, client := range s.connections.clients[teamSlot.Slot] {
			clientItemsHandling := int(client.itemsHandling.Load())
			if clientItemsHandling == ItemsHandlingNone {
				continue
			}

			var startInv, receivedItems []NetworkItem
			if clientItemsHandling&ItemsHandlingStartingInventory != 0 {
				startInv = s.state.SlotStartInventory[TeamSlot{client.Team, client.Slot}]
			}
			receivedItems = s.state.ReceivedItems.Get(ReceivedItemsKey{teamSlot.Team, teamSlot.Slot, clientItemsHandling&ItemsHandlingOwn != 0})

			sendIndex := int(client.sendIndex.Load())
			if len(startInv)+len(receivedItems) > sendIndex {
				firstNewItem := max(0, sendIndex-len(startInv))
				var startInvSlice []NetworkItem
				if sendIndex < len(startInv) {
					startInvSlice = startInv[sendIndex:]
				}
				items := append(startInvSlice, receivedItems[firstNewItem:]...)
				sends = append(sends, pendingSend{
					conn: client.clientConn,
					msg:  ReceivedItemsMessage{Index: sendIndex, Items: items},
				})
				client.sendIndex.Store(int32(len(startInv) + len(receivedItems)))
			}
		}
	}
	s.connections.mu.RUnlock()

	s.submitOrdered(func() {
		for _, s := range sends {
			wsjson.Write(ctx, s.conn, []any{s.msg})
		}
	})
}

type ReceivedItemsKey struct {
	Team        int
	Slot        int
	RemoteItems bool
}

type ReceivedItems struct {
	mu            sync.RWMutex
	receivedItems map[ReceivedItemsKey][]NetworkItem
}

func newReceivedItems() ReceivedItems {
	return ReceivedItems{receivedItems: make(map[ReceivedItemsKey][]NetworkItem)}
}

func (ri *ReceivedItems) Get(key ReceivedItemsKey) []NetworkItem {
	ri.mu.RLock()
	defer ri.mu.RUnlock()
	return ri.receivedItems[key]
}

type HintsState struct {
	mu         sync.RWMutex
	hints      []Hint
	byTeamSlot map[TeamSlot][]int
	hintsUsed  map[TeamSlot]int
}

func newHintsState() HintsState {
	return HintsState{
		hints:      make([]Hint, 0),
		byTeamSlot: make(map[TeamSlot][]int),
		hintsUsed:  make(map[TeamSlot]int),
	}
}

func (h *HintsState) rebuildIndices() {
	h.byTeamSlot = make(map[TeamSlot][]int)
	for i, hint := range h.hints {
		h.indexHint(i, hint)
	}
}

func (h *HintsState) indexHint(i int, hint Hint) {
	finderTS := TeamSlot{Team: 0, Slot: int(hint.FindingPlayer)}
	receiverTS := TeamSlot{Team: 0, Slot: int(hint.ReceivingPlayer)}
	h.byTeamSlot[finderTS] = append(h.byTeamSlot[finderTS], i)
	if receiverTS != finderTS {
		h.byTeamSlot[receiverTS] = append(h.byTeamSlot[receiverTS], i)
	}
}

func (h *HintsState) findIndex(findingPlayer int32, location int64) int {
	ts := TeamSlot{Team: 0, Slot: int(findingPlayer)}
	for _, i := range h.byTeamSlot[ts] {
		if h.hints[i].FindingPlayer == findingPlayer && h.hints[i].Location == location {
			return i
		}
	}
	return -1
}

func (h *HintsState) HintExists(findingPlayer int32, location int64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.findIndex(findingPlayer, location) >= 0
}

func (h *HintsState) GetSlotHints(ts TeamSlot) []Hint {
	h.mu.RLock()
	defer h.mu.RUnlock()
	indices := h.byTeamSlot[ts]
	out := make([]Hint, len(indices))
	for j, i := range indices {
		out[j] = h.hints[i]
	}
	return out
}

func (h *HintsState) AddSlotHint(hint Hint) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.findIndex(hint.FindingPlayer, hint.Location) >= 0 {
		return false
	}
	i := len(h.hints)
	h.hints = append(h.hints, hint)
	h.indexHint(i, hint)
	return true
}

func (h *HintsState) UpdateStatusIfExists(findingPlayer int32, location int64, status HintStatus, found bool) (Hint, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	i := h.findIndex(findingPlayer, location)
	if i < 0 {
		return Hint{}, false
	}
	if h.hints[i].Status == status && h.hints[i].Found == found {
		return h.hints[i], false // no change
	}
	h.hints[i].Status = status
	h.hints[i].Found = found
	return h.hints[i], true
}

func (h *HintsState) GetItemHints(ts TeamSlot, itemId int64) []Hint {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var hints []Hint
	for _, i := range h.byTeamSlot[ts] {
		if h.hints[i].Item == itemId {
			hints = append(hints, h.hints[i])
		}
	}
	return hints
}

func (h *HintsState) CollectItemHint(ts TeamSlot, player int16, itemId int64, sphereLocs *SphereLocs, cost, points int) []Hint {
	// Collect already-hinted locations for this item
	h.mu.RLock()
	var hints []Hint
	hintedLocs := make(map[int64]struct{})
	for _, i := range h.byTeamSlot[ts] {
		hint := h.hints[i]
		if hint.ReceivingPlayer == int32(player) && hint.Item == itemId {
			hints = append(hints, hint)
			hintedLocs[hint.Location] = struct{}{}
		}
	}
	h.mu.RUnlock()

	if cost > 0 && points < cost {
		return hints
	}

	// Find the earliest unhinted location in sphere order
	for _, sphereLoc := range *sphereLocs {
		for teamSlot, teamSlotLocs := range sphereLoc {
			for locID, loc := range teamSlotLocs {
				if loc.Player != player || loc.Item != itemId {
					continue
				}
				if _, alreadyHinted := hintedLocs[locID]; alreadyHinted {
					continue
				}
				hintStatus := HintStatusUnspecified
				if loc != nil && loc.Checked {
					hintStatus = HintStatusFound
				}
				hint := Hint{
					ReceivingPlayer: int32(loc.Player),
					FindingPlayer:   int32(teamSlot.Slot),
					Location:        locID,
					Item:            loc.Item,
					ItemFlags:       loc.Flags,
					Found:           loc.Checked,
					Status:          hintStatus,
				}
				h.AddSlotHint(hint)
				hints = append(hints, hint)
				// If cost is 0, hint all copies?
				if cost > 0 && !loc.Checked {
					h.mu.Lock()
					h.hintsUsed[ts]++
					h.mu.Unlock()
					return hints
				}
			}
		}
	}

	return hints
}

// This shouldn't be able to be called for the same player concurrently, so we can probably trust some input
// Cost is only taken if it's a new hint
func (h *HintsState) AddPaidSlotHint(ctx context.Context, ts TeamSlot, hint Hint, client *RegisteredClient, cost, points int) (Hint, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if i := h.findIndex(hint.FindingPlayer, hint.Location); i >= 0 {
		return h.hints[i], true
	}

	if !hint.Found && cost > 0 && points < cost {
		SendChatMessageToClient(ctx, client.clientConn, client.Slot,
			fmt.Sprintf("A hint costs %d points. You have %d points.", cost, points))
		return hint, false
	}

	if !hint.Found {
		h.hintsUsed[ts]++
	}

	i := len(h.hints)
	h.hints = append(h.hints, hint)
	h.indexHint(i, hint)
	return hint, true
}

func (h *HintsState) UpdateSlotHint(hint Hint) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if i := h.findIndex(hint.FindingPlayer, hint.Location); i >= 0 {
		h.hints[i] = hint
	}
}

func (h *HintsState) GetHintsUsed(ts TeamSlot) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.hintsUsed[ts]
}

type ApState struct {
	Checks        Checks
	ReceivedItems ReceivedItems
	Hints         HintsState
	ServerOptions *ServerOptions
	DataStorage   *DataStorage
	// Immutable
	Locations          map[TeamSlot]map[int64]Location
	Spheres            Spheres
	SphereLocs         SphereLocs
	NameToSlot         map[string]*SlotInfo
	SlotInfo           map[TeamSlot]SlotInfo
	SlotData           map[TeamSlot]map[string]any
	SlotMinVersions    map[TeamSlot]Version
	SlotStartInventory map[TeamSlot][]NetworkItem
	Version            Version
	Tags               []string
	Seed               string
	RaceMode           int
	// Immutable for v3 Network Sending
	PlayersNetwork  NetworkPlayers
	SlotInfoNetwork map[int]NetworkSlot
}

func (s *ApState) GetSlotHintCost(teamSlot TeamSlot) int {
	hintCost := s.ServerOptions.GetHintCost()
	if hintCost == 0 {
		return 0
	}
	locCount := len(s.Locations[teamSlot])
	cost := int(float64(hintCost) * 0.01 * float64(locCount))
	if cost < 1 {
		return 1
	}
	return cost
}

func (s *ApState) GetSlotRemainingPoints(teamSlot TeamSlot) int {
	checked := len(s.Checks.GetChecked(teamSlot))
	used := s.Hints.GetHintsUsed(teamSlot)
	return s.ServerOptions.locationCheckPoints*checked - s.GetSlotHintCost(teamSlot)*used
}

// Simple per-ip rate limiter for the WS rooms
type IPRateLimiter struct {
	mu      sync.Mutex
	clients map[string]*rate.Limiter
}

func (rl *IPRateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	lim, ok := rl.clients[ip]
	if !ok {
		lim = rate.NewLimiter(rate.Every(time.Second), 5)
		rl.clients[ip] = lim
	}
	return lim.Allow()
}

func newIPRateLimiter() *IPRateLimiter {
	return &IPRateLimiter{
		clients: make(map[string]*rate.Limiter),
	}
}

// Fields are immutable, but individual fields within may have their own callable functions to handle a mutable state
type RegisteredClient struct {
	Team                   int
	Slot                   int
	slotName               *string
	game                   *string
	cancel                 context.CancelFunc
	clientConn             *websocket.Conn
	textConcernsSelf       bool
	forcedTextConcernsSelf bool
	noText                 bool
	notifyKeys             []string
	sendIndex              atomic.Int32
	itemsHandling          atomic.Int32
}

func (c *RegisteredClient) GetItemsHandling() int {
	return int(c.itemsHandling.Load())
}

func (c *RegisteredClient) SetItemsHandling(v int) {
	c.itemsHandling.Store(int32(v))
}

func removeClient(clients []*RegisteredClient, target *RegisteredClient) []*RegisteredClient {
	i := slices.Index(clients, target)
	if i < 0 {
		return clients
	}
	return slices.Delete(clients, i, i+1)
}

// Stores data from all connected clients which is needed globally
type connectionRegistry struct {
	mu      sync.RWMutex
	clients map[int][]*RegisteredClient
	// Tags being covered here means registeredClient can stay immutable
	tags                map[*RegisteredClient][]string
	clientsByGame       map[string][]*RegisteredClient
	clientsByTag        map[string][]*RegisteredClient
	fullClients         []*RegisteredClient
	concernsSelfClients []*RegisteredClient
	// Just a copy of the static data so we can use it during register / unregister etc
	lobbyRoomId *string
	metrics     *metrics
}

func newConnectionRegistry(lobbyRoomId *string, metrics *metrics) *connectionRegistry {
	return &connectionRegistry{
		clients:       make(map[int][]*RegisteredClient),
		tags:          make(map[*RegisteredClient][]string),
		clientsByGame: make(map[string][]*RegisteredClient),
		clientsByTag:  make(map[string][]*RegisteredClient),
		lobbyRoomId:   lobbyRoomId,
		metrics:       metrics,
	}
}

func (cr *connectionRegistry) Register(slotId int, client *RegisteredClient, game string, tags []string) {
	cr.mu.Lock()
	cr.clients[slotId] = append(cr.clients[slotId], client)
	cr.tags[client] = tags
	cr.clientsByGame[game] = append(cr.clientsByGame[game], client)
	for _, tag := range tags {
		cr.clientsByTag[tag] = append(cr.clientsByTag[tag], client)
	}
	cr.applyTextIndices(client, cr.tags[client])

	slotsConnected := len(cr.clients)

	cr.mu.Unlock()

	if cr.metrics != nil && cr.lobbyRoomId != nil {
		cr.metrics.connectedSlots.WithLabelValues(*cr.lobbyRoomId).Set(float64(slotsConnected))
	}
}

// There HAS to be a safer way of doing this surely
func (cr *connectionRegistry) UpdateTags(client *RegisteredClient, tags []string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	// Remove from old tag indices
	for _, tag := range cr.tags[client] {
		cr.clientsByTag[tag] = removeClient(cr.clientsByTag[tag], client)
		if len(cr.clientsByTag[tag]) == 0 {
			delete(cr.clientsByTag, tag)
		}
	}
	// Add to new tag indices
	cr.tags[client] = tags
	for _, tag := range tags {
		cr.clientsByTag[tag] = append(cr.clientsByTag[tag], client)
	}
	cr.applyTextIndices(client, cr.tags[client])
}

func (cr *connectionRegistry) applyTextIndices(client *RegisteredClient, tags []string) {
	// Remove from both indices
	cr.fullClients = removeClient(cr.fullClients, client)
	cr.concernsSelfClients = removeClient(cr.concernsSelfClients, client)

	// Set client flags
	client.noText = slices.Contains(tags, "NoText")
	if !client.forcedTextConcernsSelf {
		client.textConcernsSelf = slices.Contains(tags, "TextConcernsSelf")
	} else {
		client.textConcernsSelf = true
	}

	// Add to relevant index
	if client.textConcernsSelf && !client.noText {
		cr.concernsSelfClients = append(cr.concernsSelfClients, client)
	} else if !client.noText {
		cr.fullClients = append(cr.fullClients, client)
	}
}

func (cr *connectionRegistry) ConnectedSlotCount() int {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return len(cr.clients)
}

func (cr *connectionRegistry) Kick(slotId int) {
	cr.mu.Lock()
	for _, client := range cr.clients[slotId] {
		client.cancel()
		cr.removeFromAllIndices(client)
	}
	delete(cr.clients, slotId)
	slotsConnected := len(cr.clients)
	cr.mu.Unlock()

	if cr.metrics != nil && cr.lobbyRoomId != nil {
		cr.metrics.connectedSlots.WithLabelValues(*cr.lobbyRoomId).Set(float64(slotsConnected))
	}
}

func (cr *connectionRegistry) Unregister(client *RegisteredClient) {
	cr.mu.Lock()

	clients := cr.clients[client.Slot]
	i := slices.Index(clients, client)
	if i < 0 {
		cr.mu.Unlock()
		return
	}
	cr.clients[client.Slot] = slices.Delete(clients, i, i+1)
	if len(cr.clients[client.Slot]) == 0 {
		delete(cr.clients, client.Slot)
	}
	cr.removeFromAllIndices(client)

	slotsConnected := len(cr.clients)
	cr.mu.Unlock()

	if cr.metrics != nil && cr.lobbyRoomId != nil {
		cr.metrics.connectedSlots.WithLabelValues(*cr.lobbyRoomId).Set(float64(slotsConnected))
	}
}

func (cr *connectionRegistry) removeFromAllIndices(client *RegisteredClient) {
	game := *client.game
	cr.clientsByGame[game] = removeClient(cr.clientsByGame[game], client)
	if len(cr.clientsByGame[game]) == 0 {
		delete(cr.clientsByGame, game)
	}
	for _, tag := range cr.tags[client] {
		cr.clientsByTag[tag] = removeClient(cr.clientsByTag[tag], client)
		if len(cr.clientsByTag[tag]) == 0 {
			delete(cr.clientsByTag, tag)
		}
	}
	delete(cr.tags, client)
	cr.fullClients = removeClient(cr.fullClients, client)
	cr.concernsSelfClients = removeClient(cr.concernsSelfClients, client)
}

func (cr *connectionRegistry) BroadcastBounceFromSlot(ctx context.Context, msg BounceMessage, bounceInfo *bounceInfoStore, senderSlot int, slotName *string, gameName *string, metrics *metrics) error {
	// Send to own slot clients first, before tag exclusion, if they match criteria
	err := cr.broadcastBounceToSlot(ctx, msg, senderSlot, slotName, gameName, metrics)
	if err != nil {
		return err
	}

	// If isolated, we can ignore everything else
	senderLimited := bounceInfo.IsLimitedToOwnSlot(senderSlot)
	if senderLimited {
		return nil
	}

	// Strip excluded tags
	if msg.Tags != nil {
		*msg.Tags = slices.DeleteFunc(*msg.Tags, func(tag string) bool {
			return bounceInfo.IsExcludedByTag(senderSlot, tag)
		})
	}

	err = cr.broadcastBounce(ctx, msg, bounceInfo, senderSlot, slotName, gameName, metrics)
	if err != nil {
		return err
	}
	return nil
}

// broadcastBounceToSlot sends to senderSlot clients that match the message criteria, before tag exclusion.
func (cr *connectionRegistry) broadcastBounceToSlot(ctx context.Context, msg BounceMessage, senderSlot int, slotName *string, gameName *string, metrics *metrics) error {
	targets := make([]*RegisteredClient, 0, 4)
	seen := make(map[*RegisteredClient]struct{}, 4)

	addTarget := func(c *RegisteredClient) {
		if c.Slot != senderSlot {
			return
		}
		if _, ok := seen[c]; ok {
			return
		}
		seen[c] = struct{}{}
		targets = append(targets, c)
	}

	cr.mu.RLock()
	if msg.Tags != nil {
		for _, tag := range *msg.Tags {
			for _, c := range cr.clientsByTag[tag] {
				addTarget(c)
			}
		}
	}
	if msg.Games != nil {
		for _, game := range *msg.Games {
			for _, c := range cr.clientsByGame[game] {
				addTarget(c)
			}
		}
	}
	if msg.Slots != nil {
		for _, slotId := range *msg.Slots {
			for _, c := range cr.clients[slotId] {
				addTarget(c)
			}
		}
	}
	cr.mu.RUnlock()

	out := msg
	out.Cmd = "Bounced"
	if metrics != nil && len(targets) > 0 && slotName != nil && gameName != nil {
		metrics.bounceResultPackets.WithLabelValues(*cr.lobbyRoomId, *slotName, *gameName).Add(float64(len(targets)))
	}

	// TODO: Only encode once
	numBytes, err := BroadcastJSON(ctx, targets, []any{out})
	if err != nil {
		return err
	}
	if metrics != nil {
		for _, client := range targets {
			metrics.bytesSent.WithLabelValues(*cr.lobbyRoomId, *client.slotName, *client.game).Add(float64(numBytes))
		}
	}
	return nil
}

func (cr *connectionRegistry) broadcastBounce(ctx context.Context, msg BounceMessage, bounceInfo *bounceInfoStore, senderSlot int, slotName *string, gameName *string, metrics *metrics) error {
	// Most will match 2 clients, but give a tiny bit of give
	targets := make([]*RegisteredClient, 0, 4)
	seen := make(map[*RegisteredClient]struct{}, 4)

	addTarget := func(c *RegisteredClient) {
		if c.Slot == senderSlot {
			return
		}
		if _, ok := seen[c]; ok {
			return
		}
		seen[c] = struct{}{}
		if bounceInfo.IsLimitedToOwnSlot(c.Slot) {
			return
		}
		targets = append(targets, c)
	}

	// Lock because client tags are mutable
	cr.mu.RLock()

	if msg.Tags != nil {
		for _, tag := range *msg.Tags {
			for _, c := range cr.clientsByTag[tag] {
				addTarget(c)
			}
		}
	}
	if msg.Games != nil {
		for _, game := range *msg.Games {
			for _, c := range cr.clientsByGame[game] {
				addTarget(c)
			}
		}
	}
	if msg.Slots != nil {
		for _, slotId := range *msg.Slots {
			for _, c := range cr.clients[slotId] {
				addTarget(c)
			}
		}
	}
	cr.mu.RUnlock()

	// Log how many we sent out
	if metrics != nil && len(targets) > 0 && slotName != nil && gameName != nil {
		metrics.bounceResultPackets.WithLabelValues(*cr.lobbyRoomId, *slotName, *gameName).Add(float64(len(targets)))
	}

	// Content is the same, just a different cmd sending out
	// TODO: Only encode once
	msg.Cmd = "Bounced"
	numBytes, err := BroadcastJSON(ctx, targets, []any{msg})
	if err != nil {
		return err
	}
	if metrics != nil {
		for _, client := range targets {
			metrics.bytesSent.WithLabelValues(*cr.lobbyRoomId, *client.slotName, *client.game).Add(float64(numBytes))
		}
	}
	return nil
}

func SendChatMessageToClient(ctx context.Context, clientConn *websocket.Conn, slotId int, msg string) {
	message := PrintJsonMessage{
		Data: []JsonMessagePart{
			{
				Type:  "text",
				Text:  msg,
				Color: "bold",
			},
		},
		Type:    "Chat",
		Team:    new(0),
		Slot:    new(slotId),
		Message: new(msg),
	}

	_ = wsjson.Write(ctx, clientConn, []any{message})
}

func (cr *connectionRegistry) SendChatMessageToSlot(ctx context.Context, slotId int, msg string, metrics *metrics) error {
	message := PrintJsonMessage{
		Data: []JsonMessagePart{
			{
				Type:  "text",
				Text:  msg,
				Color: "bold",
			},
		},
		Type:    "Chat",
		Team:    new(0),
		Slot:    new(slotId),
		Message: new(msg),
	}

	cr.mu.RLock()
	targets := cr.clients[slotId]
	cr.mu.RUnlock()

	wrappedMsg := []any{message}
	numBytes, err := BroadcastJSON(ctx, targets, wrappedMsg)
	if err != nil {
		return err
	}
	if metrics != nil {
		for _, client := range targets {
			metrics.bytesSent.WithLabelValues(*cr.lobbyRoomId, *client.slotName, *client.game).Add(float64(numBytes))
		}
	}
	return nil
}

type connectionState struct {
	authenticated           bool
	slotName                *string
	cancel                  context.CancelFunc
	clientConn              *websocket.Conn
	reduced                 bool
	pendingDatapackGames    []string
	registeredClient        *RegisteredClient
	authFailCount           int
	prevDatapackageGamesReq string
	remoteAddr              string
	// Retry storm may happen before auth, we can read this after connect for metrics
	isRetryStormClient bool
	largeDpRequested   int // Max that were requested at once before auth
}

func (cs *connectionState) SendMessage(ctx context.Context, msg []any) error {
	if cs.clientConn != nil {
		err := wsjson.Write(ctx, cs.clientConn, msg)
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("client conn not open")
}

type MessageType string

type Permission int

const (
	wsReadLimit = 1 << 24 // 16 MB
	dpReadLimit = 1 << 26 // 64 MB
)

type apxHandler struct {
	server  *ApxRoom
	reduced bool
	id      int
}

// Put options on the handler so we can run 2 servers with the same apxServer backing them
func (h apxHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Feels silly but works as a guard for testing. Should never be the case in prod.
	if h.server == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	h.server.serveConn(w, r, h.reduced)
}

func (s ApxRoom) serveConn(w http.ResponseWriter, r *http.Request, reduced bool) {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	// TODO: Simple whitelisting for our own stupid services. Dunno
	if ip != "38.246.56.120" {
		if s.ipLimiter != nil && !s.ipLimiter.Allow(ip) {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
	}

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Adds about 32mb memory usage per 1k connections, debatble CPU usage
		CompressionMode:    websocket.CompressionContextTakeover,
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.logf("client connection: %v", err)
		return
	}
	defer c.CloseNow()

	if s.metrics != nil {
		s.metrics.connectedClients.WithLabelValues(s.lobbyRoomId).Inc()
		defer s.metrics.connectedClients.WithLabelValues(s.lobbyRoomId).Dec()
	}

	c.SetReadLimit(wsReadLimit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Send RoomInfo as opening message
	if err := wsjson.Write(ctx, c, []any{s.roomInfo.Load()}); err != nil {
		s.logf("failed to send RoomInfo: %v", err)
		return
	}

	connState := &connectionState{
		authenticated:        false,
		cancel:               cancel,
		clientConn:           c,
		reduced:              reduced,
		pendingDatapackGames: []string{},
		authFailCount:        0,
		remoteAddr:           r.RemoteAddr,
	}

	// Start keepalive ping pong
	go s.keepalive(ctx, c, cancel)

	defer func() {
		if connState.registeredClient != nil {
			for _, key := range connState.registeredClient.notifyKeys {
				s.state.DataStorage.UnnotifyClient(key, connState.registeredClient)
			}
			s.connections.Unregister(connState.registeredClient)
		}
	}()

	for {
		_, raw, err := c.Read(ctx)
		if err != nil {
			status := websocket.CloseStatus(err)
			if status != websocket.StatusNormalClosure && !errors.Is(err, io.EOF) {
				if connState.slotName != nil {
					s.logf("client read inner [%s]: %v", *connState.slotName, err)
				} else {
					s.logf("client read inner: %v", err)
				}
			}
			return
		}

		if connState.authenticated && s.metrics != nil {
			s.metrics.bytesReceived.WithLabelValues(s.lobbyRoomId, *connState.slotName, *connState.registeredClient.game).Add(float64(len(raw)))
		}

		var messages []json.RawMessage
		if err := json.Unmarshal(raw, &messages); err != nil {
			s.logf("client unmarshal: %v", err)
			continue
		}

		for _, message := range messages {
			cmd, err := getPacketCmd(message)
			if err != nil {
				sendInvalidPacket(ctx, connState.clientConn, PacketProblemCmd, nil,
					fmt.Sprintf("message missing or invalid cmd field: %s", message), s.lokiLogger, connState.slotName)
				continue
			}

			if cmd == "" {
				sendInvalidPacket(ctx, connState.clientConn, PacketProblemCmd, nil, fmt.Sprintf("message missing or invalid cmd field: %v", message), s.lokiLogger, connState.slotName)
				continue
			}

			if s.lokiLogger != nil && connState.authenticated {
				s.lokiLogger.Log(connState.slotName, LogSourceClient, raw, MessageType(cmd))
			}

			if connState.authenticated && s.metrics != nil {
				s.metrics.incomingPackets.WithLabelValues(s.lobbyRoomId, *connState.slotName, *connState.registeredClient.game, cmd).Inc()
			}

			if err := s.handleMessage(ctx, connState, MessageType(cmd), message); err != nil {
				if !isNormalClose(err) && ctx.Err() == nil {
					s.logf("client read outer: %v", err)
				}
			}
		}
	}
}

func getPacketCmd(msg json.RawMessage) (string, error) {
	var header struct {
		Cmd string `json:"cmd"`
	}
	if err := json.Unmarshal(msg, &header); err != nil {
		return "", err
	}
	if header.Cmd == "" {
		return "", fmt.Errorf("message missing or invalid cmd field: %s", msg)
	}
	return header.Cmd, nil
}

// Multiserver defaults are 20 and 20, we're a little more generous I guess
const (
	pingInterval = 20 * time.Second
	pingTimeout  = 40 * time.Second
)

func (s ApxRoom) keepalive(ctx context.Context, c *websocket.Conn, cancel context.CancelFunc) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, pingTimeout)
			err := c.Ping(pingCtx)
			pingCancel()
			if err != nil {
				if ctx.Err() == nil {
					s.logf("client ping timeout, dropping connection: %v", err)
				}
				cancel()
				return
			}
		}
	}
}

func (s *ApxRoom) buildRoomInfoFromState() {
	games := make([]string, 0)
	seen := make(map[string]struct{})
	for _, info := range s.state.SlotInfo {
		if info.Type == SlotTypePlayer {
			if _, ok := seen[info.Game]; !ok {
				seen[info.Game] = struct{}{}
				games = append(games, info.Game)
			}
		}
	}

	so := s.state.ServerOptions
	so.mu.RLock()
	defer so.mu.RUnlock()

	s.roomInfo.Store(RoomInfoMessage{
		Version: NetworkVersion{
			Major: IntOrString(s.state.Version.Major),
			Minor: IntOrString(s.state.Version.Minor),
			Build: IntOrString(s.state.Version.Build),
		},
		GeneratorVersion: NetworkVersion{
			Major: IntOrString(s.state.Version.Major),
			Minor: IntOrString(s.state.Version.Minor),
			Build: IntOrString(s.state.Version.Build),
		},
		Tags:     s.state.Tags,
		Password: s.perSlotPasswords || so.password != "",
		Permissions: map[string]Permission{
			"release":   permissionFromString(so.releaseMode),
			"collect":   permissionFromString(so.collectMode),
			"remaining": permissionFromString(so.remainingMode),
		},
		HintCost:             so.hintCost,
		LocationCheckPoints:  so.locationCheckPoints,
		Games:                games,
		DatapackageChecksums: s.roomInfo.DatapackageChecksums,
		SeedName:             s.state.Seed,
	})
}

func (s ApxRoom) handleMessage(ctx context.Context, connState *connectionState, cmd MessageType, raw json.RawMessage) error {
	// Shovel logs to any debug listeners
	if connState.authenticated && s.debugTap != nil && s.debugTap.HasListeners(connState.registeredClient.Slot) {
		if raw, err := json.Marshal(raw); err == nil {
			s.debugTap.Send(connState.registeredClient.Slot, raw)
		}
	}

	// Match against each message type we want to intercept from client -> server
	switch cmd {
	case MessageTypeGetDataPackage:
		return s.handleGetDataPackage(ctx, connState, raw)
	}

	if connState.authenticated {
		switch cmd {
		case MessageTypeBounce:
			return s.handleBounce(ctx, connState, raw)
		case MessageTypeConnect:
			return s.handleAuthedConnect(ctx, connState, raw)
		case MessageTypeConnectUpdate:
			return s.handleConnectUpdate(ctx, connState, raw)
		case MessageTypeSay:
			return s.handleSay(ctx, connState, raw)
		case MessageTypeLocationChecks:
			return s.handleLocationChecks(ctx, connState, raw)
		case MessageTypeCreateHints:
			return s.handleCreateHints(ctx, connState, raw)
		case MessageTypeGet:
			return s.handleGet(ctx, connState, raw)
		case MessageTypeSet:
			return s.handleSet(ctx, connState, raw)
		default:
			// TODO: Handle message types

			return nil
		}
	} else {
		// Limit routes for unauthed clients so we don't need to check in every handler
		// Also lets us ignore duplicate connect messages
		switch cmd {
		case MessageTypeConnect:
			return s.handleConnect(ctx, connState, raw)
		default:
			s.logf("unknown command: %q", cmd)
			return nil
		}
	}
}

func (s ApxRoom) handleLocationChecks(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	var msg LocationChecksMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		cmd := MessageTypeLocationChecks
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd, fmt.Sprintf("invalid LocationChecks arguments: %v", err), s.lokiLogger, connState.slotName)
	}
	err := s.RegisterChecks(ctx, connState, TeamSlot{connState.registeredClient.Team, connState.registeredClient.Slot}, msg.Locations)
	return err
}

func (s *ApxRoom) handleCreateHints(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	var msg CreateHintsMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		cmd := MessageTypeCreateHints
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
			fmt.Sprintf("invalid CreateHints arguments: %v", err), s.lokiLogger, connState.slotName)
	}

	client := connState.registeredClient
	locationPlayer := client.Slot
	if msg.Player != nil {
		locationPlayer = *msg.Player
	}

	status := HintStatusUnspecified
	if msg.Status != nil {
		hintStatus := HintStatus(*msg.Status)
		if hintStatus == HintStatusFound {
			cmd := MessageTypeCreateHints
			return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
				"CreateHints: cannot set status to HINT_FOUND", s.lokiLogger, connState.slotName)
		}
		status = hintStatus
	}

	locationTS := TeamSlot{Team: client.Team, Slot: locationPlayer}

	var newHints []Hint

	for _, locID := range msg.Locations {
		loc, ok := s.state.Locations[locationTS][locID]
		if !ok {
			if locationPlayer != client.Slot {
				cmd := MessageTypeCreateHints
				return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
					"CreateHints: location does not exist for specified player", s.lokiLogger, connState.slotName)
			}
			continue
		}

		if locationPlayer != client.Slot && int(loc.Player) != client.Slot {
			cmd := MessageTypeCreateHints
			return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
				"CreateHints: location does not contain your item", s.lokiLogger, connState.slotName)
		}

		if s.state.Hints.HintExists(int32(locationPlayer), locID) {
			continue
		}

		found := s.state.Checks.IsChecked(locationTS, locID)
		hintStatus := status
		if found {
			hintStatus = HintStatusFound
		}
		newHints = append(newHints, Hint{
			ReceivingPlayer: int32(loc.Player),
			FindingPlayer:   int32(locationPlayer),
			Location:        locID,
			Item:            loc.Item,
			ItemFlags:       loc.Flags,
			Found:           found,
			Status:          hintStatus,
		})
	}

	for _, hint := range newHints {
		s.state.Hints.AddSlotHint(hint)
	}

	if len(newHints) > 0 {
		var msgs []PrintJsonMessage
		for _, hint := range newHints {
			msgs = append(msgs, formatHintMessage(hint))
		}
		s.broadcastPrintJson(ctx, msgs)
	}

	return nil
}

func formatHintMessage(hint Hint) PrintJsonMessage {
	found := hint.Found
	receiving := int(hint.ReceivingPlayer)
	item := NetworkItem{
		Item:     hint.Item,
		Location: hint.Location,
		Player:   int16(hint.FindingPlayer),
		Flags:    hint.ItemFlags,
	}

	parts := []JsonMessagePart{
		{Type: "text", Text: "[Hint]: "},
		{Type: "player_id", Text: fmt.Sprintf("%d", hint.ReceivingPlayer)},
		{Type: "text", Text: "'s "},
		{Type: "item_id", Text: fmt.Sprintf("%d", hint.Item), Player: int(hint.ReceivingPlayer), Flags: int(hint.ItemFlags)},
		{Type: "text", Text: " is at "},
		{Type: "location_id", Text: fmt.Sprintf("%d", hint.Location), Player: int(hint.FindingPlayer)},
		{Type: "text", Text: " in "},
		{Type: "player_id", Text: fmt.Sprintf("%d", hint.FindingPlayer)},
		{Type: "text", Text: "'s World"},
	}

	if hint.Entrance != "" {
		parts = append(parts,
			JsonMessagePart{Type: "text", Text: " at "},
			JsonMessagePart{Type: "entrance_name", Text: hint.Entrance},
		)
	}

	parts = append(parts, JsonMessagePart{
		Type:       "hint_status",
		Text:       hintStatusText(hint.Status),
		HintStatus: &hint.Status,
	})

	return PrintJsonMessage{
		Type:      "Hint",
		Data:      parts,
		Receiving: &receiving,
		Item:      &item,
		Found:     &found,
	}
}

func hintStatusText(s HintStatus) string {
	switch s {
	case HintStatusFound:
		return "(found)"
	case HintStatusUnspecified:
		return "(unspecified)"
	case HintStatusNoPriority:
		return "(no priority)"
	case HintStatusAvoid:
		return "(avoid)"
	case HintStatusPriority:
		return "(priority)"
	default:
		return "(unknown)"
	}
}

func (s *ApxRoom) handleGet(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	var msg GetMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		cmd := MessageTypeGet
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
			fmt.Sprintf("invalid Get arguments: %v", err), s.lokiLogger, connState.slotName)
	}

	if msg.Keys == nil {
		cmd := MessageTypeGet
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
			"Get: missing keys field", s.lokiLogger, connState.slotName)
	}

	keys := make(map[string]any, len(msg.Keys))
	for _, key := range msg.Keys {
		val, _ := s.state.DataStorage.Get(key)
		keys[key] = val
	}

	return wsjson.Write(ctx, connState.clientConn, []any{RetrievedMessage{
		Keys: keys,
	}})
}

func (s *ApxRoom) handleSet(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	var msg SetMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		cmd := MessageTypeSet
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
			fmt.Sprintf("invalid Set arguments: %v", err), s.lokiLogger, connState.slotName)
	}

	if msg.Key == "" {
		cmd := MessageTypeSet
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
			"Set: missing key field", s.lokiLogger, connState.slotName)
	}

	if strings.HasPrefix(msg.Key, "_read_") {
		cmd := MessageTypeSet
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
			fmt.Sprintf("Set: key %q is read-only", msg.Key), s.lokiLogger, connState.slotName)
	}

	client := connState.registeredClient
	owner := TeamSlot{Team: client.Team, Slot: client.Slot}

	var replyClient *RegisteredClient
	if msg.WantReply {
		replyClient = client
	}
	_, _, err := s.state.DataStorage.Set(owner, msg.Key, msg.Default, msg.Operations, client.Slot, replyClient, s.submitOrdered)
	if err != nil {
		cmd := MessageTypeSet
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd,
			fmt.Sprintf("Set: %v", err), s.lokiLogger, connState.slotName)
	}

	return nil
}

func (s *ApxRoom) broadcastPrintJson(ctx context.Context, msgs []PrintJsonMessage) {
	if len(msgs) == 0 {
		return
	}

	s.connections.mu.RLock()
	fullTargets := make([]*RegisteredClient, len(s.connections.fullClients))
	copy(fullTargets, s.connections.fullClients)
	selfOnlyTargets := make([]*RegisteredClient, len(s.connections.concernsSelfClients))
	copy(selfOnlyTargets, s.connections.concernsSelfClients)
	s.connections.mu.RUnlock()

	s.submitBroadcast(func() {
		if len(fullTargets) > 0 {
			BroadcastJSON(ctx, fullTargets, anySlice(msgs))
		}
		if len(selfOnlyTargets) > 0 {
			slotMsgs := make(map[int][]any)
			for _, msg := range msgs {
				if msg.Receiving != nil {
					slotMsgs[*msg.Receiving] = append(slotMsgs[*msg.Receiving], msg)
				}
				if msg.Item != nil && (msg.Receiving == nil || int(msg.Item.Player) != *msg.Receiving) {
					slotMsgs[int(msg.Item.Player)] = append(slotMsgs[int(msg.Item.Player)], msg)
				}
			}
			bySlot := make(map[int][]*RegisteredClient)
			for _, c := range selfOnlyTargets {
				bySlot[c.Slot] = append(bySlot[c.Slot], c)
			}
			for slot, clients := range bySlot {
				if relevant, ok := slotMsgs[slot]; ok {
					BroadcastJSON(ctx, clients, relevant)
				}
			}
		}
	})
}

func anySlice(msgs []PrintJsonMessage) []any {
	out := make([]any, len(msgs))
	for i, m := range msgs {
		out[i] = m
	}
	return out
}

func (dt *debugTap) HasListeners(slotId int) bool {
	if slotId <= 0 || slotId >= len(dt.slots) {
		return false
	}
	s := &dt.slots[slotId]
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.listeners) > 0
}

func (dt *debugTap) Subscribe(slotId int) (<-chan []byte, func(), bool) {
	if slotId <= 0 || slotId >= len(dt.slots) {
		return nil, nil, false
	}

	ch := make(chan []byte, 64)
	s := &dt.slots[slotId]

	s.mu.Lock()
	s.listeners = append(s.listeners, ch)
	s.mu.Unlock()

	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, l := range s.listeners {
			if l == ch {
				s.listeners = slices.Delete(s.listeners, i, i+1)
				close(ch)
				break
			}
		}
	}
	return ch, cancel, true
}

func (dt *debugTap) Send(slotId int, raw []byte) {
	if slotId <= 0 || slotId >= len(dt.slots) {
		return
	}
	s := &dt.slots[slotId]
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.listeners {
		select {
		case ch <- raw:
		default:
		}
	}
}

func (s ApxRoom) StatusString(client *RegisteredClient) string {
	connected := s.connections.ConnectedSlotCount()
	total := len(s.state.SlotData)
	probability := s.bounceInfo.GetProbability()
	deaths := s.bounceInfo.Get()

	totalDeaths := 0
	for _, count := range deaths {
		totalDeaths += count
	}

	lines := []string{
		fmt.Sprintf("=== APX Status: %s ===", s.lobbyRoomId),
		fmt.Sprintf("Slots connected: %d / %d", connected, total),
		fmt.Sprintf("Total deaths: %d", totalDeaths),
		fmt.Sprintf("Deathlink probability: %.0f%%", probability*100),
	}

	if client != nil {
		lines = append(lines, "--- Your Status ---")
		lines = append(lines, fmt.Sprintf("Deaths: %d", deaths[client.Slot]))
		if tags := s.bounceInfo.GetTagExclusionsForSlot(client.Slot); len(tags) > 0 {
			lines = append(lines, fmt.Sprintf("Blocked Bounce tags: %s", strings.Join(tags, ", ")))
		}
		if s.bounceInfo.IsLimitedToOwnSlot(client.Slot) {
			lines = append(lines, "You are isolated from other players bounces")
		}
	}

	return strings.Join(lines, "\n")
}

func (s ApxRoom) SyncReceivedItems(ctx context.Context, client *RegisteredClient, itemsHandling int) error {
	if itemsHandling == 0 {
		return nil
	}

	// Grab both arrays if appropriate first, to avoid extra allocations
	var startInv, recvItems []NetworkItem

	if itemsHandling&ItemsHandlingStartingInventory != 0 {
		startInv = s.state.SlotStartInventory[TeamSlot{client.Team, client.Slot}]
	}

	if itemsHandling&ItemsHandlingForeign != 0 {
		remoteItems := itemsHandling&ItemsHandlingOwn != 0
		recvItems = s.state.ReceivedItems.Get(ReceivedItemsKey{Team: client.Team, Slot: client.Slot, RemoteItems: remoteItems})
	}

	if len(startInv) == 0 && len(recvItems) == 0 {
		return nil
	}

	items := make([]NetworkItem, 0, len(startInv)+len(recvItems))
	items = append(items, startInv...)
	items = append(items, recvItems...)

	msg := ReceivedItemsMessage{
		Index: 0,
		Items: items,
	}
	client.sendIndex.Store(int32(len(startInv) + len(recvItems)))
	return wsjson.Write(ctx, client.clientConn, []any{msg})
}

func convertMultiDataToState(md *multidata.MultiData) (*ApState, error) {
	slotData := make(map[TeamSlot]map[string]any, len(md.SlotData))
	for slot, data := range md.SlotData {
		slotData[TeamSlot{Team: 0, Slot: slot}] = data
	}

	slotGames := make(map[TeamSlot]string, len(md.SlotInfo))
	for slot, info := range md.SlotInfo {
		slotGames[TeamSlot{Team: 0, Slot: slot}] = info.Game
	}

	slotMinVersions := make(map[TeamSlot]Version, len(md.MinimumVersions.Clients))
	for slot, v := range md.MinimumVersions.Clients {
		slotMinVersions[TeamSlot{Team: 0, Slot: slot}] = Version{
			Major: v[0],
			Minor: v[1],
			Build: v[2],
		}
	}

	locations := make(map[TeamSlot]map[int64]Location, len(md.Locations))
	for slot, locs := range md.Locations {
		inner := make(map[int64]Location, len(locs))
		for locID, t := range locs {
			inner[locID] = Location{
				Item:    int64(t[0]),
				Player:  int16(t[1]),
				Flags:   int16(t[2]),
				Checked: false,
			}
		}
		locations[TeamSlot{Team: 0, Slot: slot}] = inner
	}

	slotInfoMap := make(map[TeamSlot]SlotInfo, len(md.SlotInfo))
	for slot, info := range md.SlotInfo {
		members := make([]int, len(info.GroupMembers))
		copy(members, info.GroupMembers)
		slotInfoMap[TeamSlot{Team: 0, Slot: slot}] = SlotInfo{
			Team:         0,
			Slot:         slot,
			Name:         info.Name,
			Game:         info.Game,
			Type:         SlotType(info.Type),
			GroupMembers: members,
		}
	}

	nameToSlot := make(map[string]*SlotInfo, len(md.ConnectNames))
	for name, pair := range md.ConnectNames {
		ts := TeamSlot{Team: pair[0], Slot: pair[1]}
		if info, ok := slotInfoMap[TeamSlot{Team: 0, Slot: ts.Slot}]; ok {
			infoCopy := info
			nameToSlot[name] = &infoCopy
		}
	}

	spheres := make(Spheres, len(md.Spheres))
	sphereLocs := make(SphereLocs, len(md.Spheres))
	for i, s := range md.Spheres {
		sphere := make(Sphere, len(s))
		group := make(SphereLocGroup, len(s))
		for slot, locIDs := range s {
			ts := TeamSlot{Team: 0, Slot: slot}
			sphere[ts] = locIDs
			slotLocs := locations[ts]
			locs := make(map[int64]*Location, 0)
			for _, locID := range locIDs {
				if loc, ok := slotLocs[locID]; ok {
					locs[locID] = &loc
				}
			}
			group[ts] = locs
		}
		spheres[i] = sphere
		sphereLocs[i] = group
	}

	startInv := make(map[TeamSlot][]NetworkItem, len(md.PrecollectedItems))
	for slot, itemIDs := range md.PrecollectedItems {
		items := make([]NetworkItem, len(itemIDs))
		for i, id := range itemIDs {
			items[i] = NetworkItem{
				Item:     id,
				Location: -2, // sentinel: start inventory location
				Player:   0,
			}
		}
		startInv[TeamSlot{Team: 0, Slot: slot}] = items
	}

	// Change with teams support in future
	playersNetwork := make([]NetworkPlayer, 0, len(md.SlotInfo))
	slotInfoNetwork := make(map[int]NetworkSlot, len(md.SlotInfo))
	for slot, info := range md.SlotInfo {
		playersNetwork = append(playersNetwork, NetworkPlayer{
			Team:  0,
			Slot:  slot,
			Alias: info.Name,
			Name:  info.Name,
		})
		members := make([]int, len(info.GroupMembers))
		copy(members, info.GroupMembers)
		slotInfoNetwork[slot] = NetworkSlot{
			Name:         info.Name,
			Game:         info.Game,
			Type:         int(info.Type),
			GroupMembers: members,
		}
	}

	apState := ApState{
		Checks:             newChecksState(locations),
		ReceivedItems:      newReceivedItems(),
		Hints:              newHintsState(),
		DataStorage:        newDataStorage(),
		Locations:          locations,
		Spheres:            spheres,
		SphereLocs:         sphereLocs,
		NameToSlot:         nameToSlot,
		SlotInfo:           slotInfoMap,
		SlotData:           slotData,
		SlotMinVersions:    slotMinVersions,
		SlotStartInventory: startInv,
		Version: Version{
			Major: md.Version[0],
			Minor: md.Version[1],
			Build: md.Version[2],
		},
		ServerOptions: parseEmbeddedServerOptions(md.ServerOptions),
		Tags:          md.Tags,
		Seed:          md.SeedName,
		RaceMode:      md.RaceMode,
		PlayersNetwork: NetworkPlayers{
			players: playersNetwork,
		},
		SlotInfoNetwork: slotInfoNetwork,
	}
	populateReadData(&apState)
	return &apState, nil
}

func populateReadData(state *ApState) {
	ds := state.DataStorage

	// race_mode — static scalar from multidata
	raceMode := int64(state.RaceMode)
	ds.RegisterReadKey("race_mode", func() StorageValue {
		return raceMode
	})

	// Per-slot keys — all team 0 at load time
	for ts, slotData := range state.SlotData {
		ts := ts         // capture
		data := slotData // capture
		slot := ts.Slot

		// slot_data_{slot} — static, captured at load
		ds.RegisterReadKey(fmt.Sprintf("slot_data_%d", slot), func() StorageValue {
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

func defaultServerOptions() ServerOptions {
	return ServerOptions{
		hintCost:            10,
		locationCheckPoints: 1,
		releaseMode:         "auto",
		collectMode:         "auto",
		remainingMode:       "goal",
		countdownMode:       "auto",
		compatibility:       2,
	}
}

func parseEmbeddedServerOptions(raw map[string]any) *ServerOptions {
	opts := defaultServerOptions()
	if v, ok := raw["password"]; ok {
		if s, ok := v.(string); ok {
			opts.password = s
		}
	}
	if v, ok := raw["hint_cost"]; ok {
		if n, ok := toInt(v); ok {
			opts.hintCost = n
		}
	}
	if v, ok := raw["location_check_points"]; ok {
		if n, ok := toInt(v); ok {
			opts.locationCheckPoints = n
		}
	}
	if v, ok := raw["release_mode"]; ok {
		if s, ok := v.(string); ok {
			opts.releaseMode = s
		}
	}
	if v, ok := raw["collect_mode"]; ok {
		if s, ok := v.(string); ok {
			opts.collectMode = s
		}
	}
	if v, ok := raw["remaining_mode"]; ok {
		if s, ok := v.(string); ok {
			opts.remainingMode = s
		}
	}
	if v, ok := raw["countdown_mode"]; ok {
		if s, ok := v.(string); ok {
			opts.countdownMode = s
		}
	}
	if v, ok := raw["disable_item_cheat"]; ok {
		if b, ok := v.(bool); ok {
			opts.disableItemCheat = b
		}
	}
	if v, ok := raw["compatibility"]; ok {
		if n, ok := toInt(v); ok {
			opts.compatibility = n
		}
	}
	return &opts
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	}
	return 0, false
}

func startUnorderedWorkers(ctx context.Context, slotCount int) chan func() {
	workerCount := min(max(2, slotCount/8), 64)
	ch := make(chan func(), 256)
	for range workerCount {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case fn, ok := <-ch:
					if !ok {
						return
					}
					fn()
				}
			}
		}()
	}
	return ch
}

func startOrderedWorker(ctx context.Context) chan func() {
	ch := make(chan func(), 256)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case fn, ok := <-ch:
				if !ok {
					return
				}
				fn()
			}
		}
	}()
	return ch
}
