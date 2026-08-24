package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Spheres []SphereLocations

// { slot_id : [loc_id, loc_id, loc_id] }
type SphereLocations map[int32][]int64

type SphereResult struct {
	Locations []LocationEntry `json:"locations"`
	Total     int             `json:"total"`
	Checked   int             `json:"checked"`
}

type RoomManager struct {
	config        *Config
	registry      *RoomRegistry
	metrics       *metrics
	tlsCfg        *tls.Config
	reg           *prometheus.Registry
	largeDpLogger *log.Logger
}

type RoomRegistry struct {
	mu    sync.RWMutex
	rooms map[string]*HostedRoom
}

func (r *RoomRegistry) Get(roomId string) (*HostedRoom, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	room, ok := r.rooms[roomId]
	return room, ok
}

func (r *RoomRegistry) Add(hostedRoom *HostedRoom) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.rooms[hostedRoom.lobbyRoomId]
	if exists {
		return fmt.Errorf("room already exists: %s", hostedRoom.lobbyRoomId)
	}
	r.rooms[hostedRoom.lobbyRoomId] = hostedRoom
	return nil
}

func (r *RoomRegistry) Remove(roomId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	room, exists := r.rooms[roomId]
	if exists {
		room.cancel()
	}
	delete(r.rooms, roomId)
}

type CheckedLocations struct {
	checkedLocationsMu sync.RWMutex
	checkedLocations   map[int]map[int64]bool
}

func (cl *CheckedLocations) Get() map[int]map[int64]bool {
	cl.checkedLocationsMu.RLock()
	defer cl.checkedLocationsMu.RUnlock()
	return cl.checkedLocations
}

func (cl *CheckedLocations) GetSlot(slotId int) map[int64]bool {
	cl.checkedLocationsMu.RLock()
	defer cl.checkedLocationsMu.RUnlock()
	return cl.checkedLocations[slotId]
}

func (cl *CheckedLocations) Set(newCl map[int]map[int64]bool) {
	cl.checkedLocationsMu.Lock()
	defer cl.checkedLocationsMu.Unlock()
	cl.checkedLocations = newCl
}

func newCheckedLocations() CheckedLocations {
	return CheckedLocations{
		checkedLocations: make(map[int]map[int64]bool),
	}
}

type HostedRoom struct {
	lobbyRoomId string
	apx         *apxServer
	spheres     Spheres
	// e.g locationIdToName["Ocarina of Time"][3] == "Song from Saria"
	locationIdToName     map[string]map[int]string
	checkedLocations     *CheckedLocations
	sphereCache          *slotSphereCache
	completeSphere1Slots map[int]struct{}
	normalServer         *http.Server
	reducedServer        *http.Server
	apRoomId             string
	ctx                  context.Context
	cancel               context.CancelFunc
}

type Deathlink struct {
	Slot      string  `json:"slot"`
	Source    string  `json:"source"`
	Cause     *string `json:"cause"`
	CreatedAt string  `json:"created_at"`
}

type LocationEntry struct {
	ID      int64
	Name    string
	Checked bool
}

func (l LocationEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([3]any{l.ID, l.Name, l.Checked})
}

// Cache sphere responses to save compute
type slotSphereCache struct {
	mu    sync.RWMutex
	data  map[int][]byte // slotId -> pre-encoded JSON
	dirty map[int]bool   // slotId -> needs rebuild
}

func newSlotSphereCache() *slotSphereCache {
	return &slotSphereCache{
		data:  make(map[int][]byte),
		dirty: make(map[int]bool),
	}
}

func (c *slotSphereCache) Invalidate(slotId int) {
	c.mu.Lock()
	c.dirty[slotId] = true
	c.mu.Unlock()
}

func (c *slotSphereCache) Get(slotId int) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.dirty[slotId] {
		return nil, false
	}
	b, ok := c.data[slotId]
	return b, ok
}

func (c *slotSphereCache) Set(slotId int, b []byte) {
	c.mu.Lock()
	c.data[slotId] = b
	c.dirty[slotId] = false
	c.mu.Unlock()
}

func newRoomRegistry() *RoomRegistry {
	return &RoomRegistry{
		rooms: make(map[string]*HostedRoom),
	}
}

func startRoomManager(cfg *Config, reg *prometheus.Registry, metrics *metrics, tlsCfg *tls.Config) (*RoomManager, *mux.Router, error) {
	logFile, err := os.OpenFile("packets.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("opening log file: %w", err)
	}
	largeDpLogger := log.New(logFile, "", log.LstdFlags|log.LUTC)

	srv := &RoomManager{
		config:        cfg,
		registry:      newRoomRegistry(),
		reg:           reg,
		metrics:       metrics,
		tlsCfg:        tlsCfg,
		largeDpLogger: largeDpLogger,
	}

	r := mux.NewRouter()
	r.HandleFunc("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP)
	api := r.PathPrefix("/api/{roomId}").Subrouter()
	api.Use(srv.loggingMiddleware)
	api.Use(srv.authMiddleware)
	api.HandleFunc("/refresh_passwords", srv.handlePasswordRefresh).Methods(http.MethodPost)
	api.HandleFunc("/password/{slotId}", srv.handlePassword).Methods(http.MethodGet)
	api.HandleFunc("/deathlinks", srv.handleDeathlinks).Methods(http.MethodGet)
	api.HandleFunc("/full_feed", srv.handleFullFeed).Methods(http.MethodGet)
	api.HandleFunc("/full_feed/{slotId}", srv.handleFullFeedUser).Methods(http.MethodPost, http.MethodDelete)
	api.HandleFunc("/bounce_exclusions", srv.handleBounceExclusionsList).Methods(http.MethodGet)
	api.HandleFunc("/bounce_exclusions/{slotId}/{tag}", srv.handleBounceExclusions).Methods(http.MethodPost, http.MethodDelete)
	api.HandleFunc("/deathlink_probability", srv.handleProbability).Methods(http.MethodGet, http.MethodPost)
	api.HandleFunc("/spheres", srv.handleAllSpheres).Methods(http.MethodGet)
	api.HandleFunc("/incomplete_sphere1", srv.handleIncompleteSphere1).Methods(http.MethodGet)
	api.HandleFunc("/spheres/{slotId}", srv.handleSpheresForSlot).Methods(http.MethodGet)
	api.HandleFunc("/debug/slot/{slotId}", srv.handleDebugTap)

	return srv, r, nil
}

func (rm *RoomManager) startNewHostedRoom(apRoomId string, lobbyRoomId string, listenAddr, reducedListenAddr *string) error {
	_, exists := rm.registry.Get(lobbyRoomId)
	if exists {
		return fmt.Errorf("room already exists: %s", lobbyRoomId)
	}

	roomPlayers, err := fetchRoomPlayers(rm.config.ApApiRoot, apRoomId)
	if err != nil {
		return fmt.Errorf("failed to get %s/api/room/%s/players from AP server, aborting: %w", rm.config.ApApiRoot, apRoomId, err)
	}

	roomInfo, err := connectAndGetRoomInfo(rm.config.APHost, rm.config.APPort)
	if err != nil {
		return fmt.Errorf("failed to get RoomInfo from AP server, aborting: %w", err)
	}

	spheres, err := fetchRoomSpheres(rm.config.ApApiRoot, apRoomId)
	if err != nil {
		log.Fatalf("prefetching spheres: %v", err)
	}

	passwordStore := newPasswordStore()
	fullFeedStore := newFullFeedStore()
	connRegistry := newConnectionRegistry()
	datapackageCache := newDataPackageStore(true) // TODO: Add config flag
	bounceInfo := newBounceInfoStore()
	debugTap := newDebugTap(maxRoomPlayerId(roomPlayers.nameToID))
	if rm.config.Passwordless != true {
		slots, err := fetchSlotPasswords(rm.config, lobbyRoomId)
		if err != nil {
			return fmt.Errorf("failed to fetch slot passwords: %w", err)
		}
		loadPasswordsIntoStore(connRegistry, passwordStore, roomPlayers, slots)
	}

	apx := &apxServer{
		logf:         log.Printf,
		config:       rm.config,
		roomInfo:     *roomInfo,
		roomPlayers:  roomPlayers,
		passwords:    passwordStore,
		fullFeed:     fullFeedStore,
		connections:  connRegistry,
		bounceInfo:   bounceInfo,
		datapackages: datapackageCache,
		metrics:      rm.metrics,
		lobbyRoomId:  lobbyRoomId,
		debugTap:     debugTap,
		debugLogger:  rm.largeDpLogger,
	}

	if err := apx.prefetchDataPackages(context.Background()); err != nil {
		log.Fatalf("prefetching datapackages: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkedLocs := newCheckedLocations()
	room := &HostedRoom{
		lobbyRoomId:          lobbyRoomId,
		apx:                  apx,
		spheres:              spheres,
		sphereCache:          newSlotSphereCache(),
		completeSphere1Slots: map[int]struct{}{},
		checkedLocations:     &checkedLocs,
		ctx:                  ctx,
		cancel:               cancel,
	}

	if err := room.refreshCheckedLocations(rm.config.ApApiRoot, apRoomId); err != nil {
		log.Printf("initial checked locations fetch failed: %v", err)
	}
	room.startCheckedLocationPoller(rm.config.ApApiRoot, apRoomId, 30*time.Second)

	// Normal traffic
	normalServer := &http.Server{
		Handler:      apxHandler{server: apx, reduced: false},
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}

	// Reduced traffic (e.g less PrintJSON messages)
	reducedServer := &http.Server{
		Handler:      apxHandler{server: apx, reduced: true},
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}

	// Add room to registry
	err = rm.registry.Add(room)
	if err != nil {
		return fmt.Errorf("room already exists: %s", lobbyRoomId)
	}
	defer rm.registry.Remove(lobbyRoomId)

	// Setup both servers

	if listenAddr == nil {
		a := "0.0.0.0"
		listenAddr = &a
	}
	wsListener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listening on normal port: %w", err)
	}

	if reducedListenAddr == nil {
		a := "0.0.0.0"
		reducedListenAddr = &a
	}
	wsReducedListener, err := net.Listen("tcp", *reducedListenAddr)
	if err != nil {
		return fmt.Errorf("listening on reduced port: %w", err)
	}

	if rm.tlsCfg != nil {
		wsListener = &httpDropper{listener: wsListener, tlsCfg: rm.tlsCfg}
		wsReducedListener = &httpDropper{listener: wsReducedListener, tlsCfg: rm.tlsCfg}
		log.Printf("room %s: listening on wss://%v", lobbyRoomId, wsListener.Addr())
		log.Printf("room %s: reduced listening on wss://%v", lobbyRoomId, wsReducedListener.Addr())
	} else {
		log.Printf("room %s: listening on ws://%v", lobbyRoomId, wsListener.Addr())
		log.Printf("room %s: reduced listening on ws://%v", lobbyRoomId, wsReducedListener.Addr())
	}

	errc := make(chan error, 1)
	go func() { errc <- normalServer.Serve(wsListener) }()
	go func() { errc <- reducedServer.Serve(wsReducedListener) }()

	log.Printf("Opened room: %s", lobbyRoomId)

	// Wait until a server errors, or cancel is called
	select {
	case err := <-errc:
		return fmt.Errorf("ws server error: %w", err)
	case <-ctx.Done():
	}

	// Shutdown both servers immediately
	normalServer.Close()
	reducedServer.Close()

	return nil
}

func (rm *RoomManager) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		route := mux.CurrentRoute(r)
		path, _ := route.GetPathTemplate()
		log.Printf("%s %s (matched: %s) %s", r.Method, r.URL.Path, path, time.Since(start))
	})
}

func (rm *RoomManager) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if rm.config.ApiKey == "" || key != rm.config.ApiKey {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (rm *RoomManager) handlePasswordRefresh(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}
	if !rm.config.LobbyEnabled {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby not enabled"})
		return
	}

	slots, err := fetchSlotPasswords(rm.config, room.lobbyRoomId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to fetch passwords from lobby"})
		return
	}
	loadPasswordsIntoStore(room.apx.connections, room.apx.passwords, room.apx.roomPlayers, slots)
}

func (rm *RoomManager) handlePassword(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	if !rm.config.LobbyEnabled {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby not enabled"})
		return
	}

	vars := mux.Vars(r)
	slotId, err := strconv.Atoi(vars["slotId"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid slotId"})
		return
	}

	password, ok := room.apx.passwords.Get(slotId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "no password for slot"})
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"password": password})
}

func (rm *RoomManager) handleDeathlinks(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}
	deathlinks := room.apx.bounceInfo.Get()
	json.NewEncoder(w).Encode(deathlinks)
}

func (rm *RoomManager) handleFullFeed(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}
	fullFeed := room.apx.fullFeed.Get()
	json.NewEncoder(w).Encode(fullFeed)
}

func (rm *RoomManager) handleFullFeedUser(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	slotId, err := strconv.Atoi(vars["slotId"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid slotId"})
		return
	}

	switch r.Method {
	case http.MethodPost:
		room.apx.fullFeed.Set(slotId)
		json.NewEncoder(w).Encode(map[string]any{"full_feed": true})

	case http.MethodDelete:
		room.apx.fullFeed.Delete(slotId)
		json.NewEncoder(w).Encode(map[string]any{"full_feed": false})
	}
}

func (rm *RoomManager) handleBounceExclusionsList(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(room.apx.bounceInfo.GetExclusions())
}

func (rm *RoomManager) handleBounceExclusions(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	slotId, err := strconv.Atoi(vars["slotId"])
	if err != nil {
		http.Error(w, "invalid slotId", http.StatusBadRequest)
		return
	}
	tag := vars["tag"]
	if tag == "" {
		http.Error(w, "missing tag", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		room.apx.bounceInfo.Exclude(slotId, tag)
		json.NewEncoder(w).Encode(map[string]any{"excluded": true})

	case http.MethodDelete:
		room.apx.bounceInfo.Unexclude(slotId, tag)
		json.NewEncoder(w).Encode(map[string]any{"excluded": false})
	}
}

func (rm *RoomManager) handleProbability(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		json.NewEncoder(w).Encode(map[string]any{"probability": room.apx.bounceInfo.GetProbability()})

	case http.MethodPost:
		var body struct {
			Probability *float64 `json:"probability"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
			return
		}
		if body.Probability != nil && (*body.Probability < 0 || *body.Probability > 1) {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "probability must be between 0 and 1"})
			return
		}
		room.apx.bounceInfo.SetProbability(*body.Probability)
		json.NewEncoder(w).Encode(map[string]any{"probability": room.apx.bounceInfo.GetProbability()})
	}
}

func (rm *RoomManager) handleSpheresForSlot(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	slotId, err := strconv.Atoi(vars["slotId"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid slotId"})
		return
	}

	// Check for cache hit first
	if cached, ok := room.sphereCache.Get(slotId); ok {
		w.Write(cached)
		return
	}
	slot, ok := room.apx.roomPlayers.slots[slotId]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "slot not found"})
		return
	}

	locationIDToName, ok := room.apx.datapackages.LocationIDToName[slot.Game]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "datapackage not found"})
		return
	}
	checkedLocations := room.checkedLocations.GetSlot(slotId)

	result := make([]SphereResult, 0, len(room.spheres))
	for _, sphere := range room.spheres {
		locIDs, ok := sphere[int32(slotId)]
		if !ok {
			continue
		}
		entries := make([]LocationEntry, 0, len(locIDs))
		locs_checked := 0
		for _, locID := range locIDs {
			checked := checkedLocations[locID]
			if checked {
				locs_checked++
			}
			entries = append(entries, LocationEntry{
				ID:      locID,
				Name:    locationIDToName[locID],
				Checked: checked,
			})
		}
		result = append(result, SphereResult{
			Locations: entries,
			Total:     len(entries),
			Checked:   locs_checked,
		})
	}

	b, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	room.sphereCache.Set(slotId, b)
	w.Write(b)
}

func (rm *RoomManager) handleAllSpheres(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")

	result := make(map[string]json.RawMessage, len(room.apx.roomPlayers.slots))

	for slotId, slot := range room.apx.roomPlayers.slots {
		if cached, ok := room.sphereCache.Get(slotId); ok {
			result[slot.Name] = json.RawMessage(cached)
			continue
		}

		locationIDToName, ok := room.apx.datapackages.LocationIDToName[slot.Game]
		if !ok {
			continue
		}
		checkedLocations := room.checkedLocations.GetSlot(slotId)

		slotResult := make([]SphereResult, 0, len(room.spheres))
		for _, sphere := range room.spheres {
			locIDs, ok := sphere[int32(slotId)]
			if !ok {
				continue
			}
			entries := make([]LocationEntry, 0, len(locIDs))
			locsChecked := 0
			for _, locID := range locIDs {
				checked := checkedLocations[locID]
				if checked {
					locsChecked++
				}
				entries = append(entries, LocationEntry{
					ID:      locID,
					Name:    locationIDToName[locID],
					Checked: checked,
				})
			}
			slotResult = append(slotResult, SphereResult{
				Locations: entries,
				Total:     len(entries),
				Checked:   locsChecked,
			})
		}

		b, err := json.Marshal(slotResult)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		room.sphereCache.Set(slotId, b)
		result[slot.Name] = json.RawMessage(b)
	}

	if err := json.NewEncoder(w).Encode(result); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (rm *RoomManager) handleIncompleteSphere1(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if len(room.spheres) == 0 {
		json.NewEncoder(w).Encode([]int{})
		return
	}

	sphere1 := room.spheres[0]

	// Check if status of incomplete slots has changed
	checkedLocs := room.checkedLocations.Get()
	for slotId, locIDs := range sphere1 {
		if _, done := room.completeSphere1Slots[int(slotId)]; done {
			continue
		}
		if !isSphere1Incomplete(locIDs, checkedLocs[int(slotId)]) {
			room.completeSphere1Slots[int(slotId)] = struct{}{}
		}
	}

	// Build list of all incomplete slots
	result := make([]int, 0)
	for slotId := range room.apx.roomPlayers.slots {
		if _, done := room.completeSphere1Slots[slotId]; !done {
			result = append(result, slotId)
		}
	}
	json.NewEncoder(w).Encode(result)
}

func (rm *RoomManager) handleDebugTap(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	slotId, err := strconv.Atoi(vars["slotId"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid slotId"})
		return
	}

	// Validate slot exists
	if _, ok := room.apx.roomPlayers.slots[slotId]; !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "slot not found"})
		return
	}

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer c.CloseNow()

	ch, cancel, allowed := room.apx.debugTap.Subscribe(slotId)
	if !allowed {
		return
	}
	defer cancel()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if err := c.Write(ctx, websocket.MessageText, msg); err != nil {
				return
			}
		}
	}
}

func isSphere1Incomplete(locIDs []int64, checkedLocations map[int64]bool) bool {
	for _, locID := range locIDs {
		if !checkedLocations[locID] {
			return true
		}
	}
	return false
}

// TODO: Cancel on context cancel
func (rm *HostedRoom) startCheckedLocationPoller(apApiRoot, apRoomId string, interval time.Duration) {
	go func() {
		for {
			if err := rm.refreshCheckedLocations(apApiRoot, apRoomId); err != nil {
				log.Printf("refreshing checked locations: %v", err)
			}
			time.Sleep(interval)
		}
	}()
}

func (rm *HostedRoom) refreshCheckedLocations(apApiRoot, apRoomId string) error {
	url := fmt.Sprintf("%s/api/room/%s/checked_locations", apApiRoot, apRoomId)

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("fetching checked locations: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var raw map[string][]int64
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return fmt.Errorf("decoding checked locations: %w", err)
	}

	newChecked := make(map[int]map[int64]bool, len(raw))
	for slotStr, locIDs := range raw {
		slotId, err := strconv.Atoi(slotStr)
		if err != nil {
			return fmt.Errorf("invalid slot key %q: %w", slotStr, err)
		}
		locs := make(map[int64]bool, len(locIDs))
		for _, id := range locIDs {
			locs[id] = true
		}
		newChecked[slotId] = locs
	}

	// Invalidate cache for any slot whose checked locations changed
	for slotId, newLocs := range newChecked {
		oldLocs := rm.checkedLocations.GetSlot(slotId)
		if len(oldLocs) != len(newLocs) {
			rm.sphereCache.Invalidate(slotId)
		}
	}

	rm.checkedLocations.Set(newChecked)
	return nil
}

func fetchRoomSpheres(apApiRoot string, apRoomId string) (Spheres, error) {
	url := fmt.Sprintf("%s/api/room/%s/spheres", apApiRoot, apRoomId)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetching room spheres: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from /api/room/%s/spheres", resp.StatusCode, apRoomId)
	}

	var spheres Spheres
	if err := json.NewDecoder(resp.Body).Decode(&spheres); err != nil {
		return nil, fmt.Errorf("decoding room spheres: %w", err)
	}

	return spheres, nil
}

// Slot IDs are keys, so JSON turns them to strings, we want the numbers
func (s *SphereLocations) UnmarshalJSON(data []byte) error {
	var raw map[string][]int64
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*s = make(SphereLocations, len(raw))
	for k, v := range raw {
		id, err := strconv.ParseInt(k, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid slot_id key %q: %w", k, err)
		}
		(*s)[int32(id)] = v
	}
	return nil
}

func (rm *RoomManager) roomFromRequest(w http.ResponseWriter, r *http.Request) (*HostedRoom, bool) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]
	room, ok := rm.registry.Get(roomId)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return nil, false
	}
	return room, true
}

func maxRoomPlayerId(nameToId map[string]int) int {
	var maxNumber int
	for _, id := range nameToId {
		if id > maxNumber {
			maxNumber = id
		}
	}
	return maxNumber
}
