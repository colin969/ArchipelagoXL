package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
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

type RoomStore struct {
	db *sql.DB
}

type RoomRecord struct {
	LobbyRoomId  string
	ApRoomId     string
	NormalPort   int
	ReducedPort  int
	CreatedAt    time.Time
	Disabled     bool
	Passwordless bool
}

type RoomManager struct {
	config   *Config
	registry *RoomRegistry
	metrics  *metrics
	tlsCfg   *tls.Config
	reg      *prometheus.Registry
	store    *RoomStore
}

func NewRoomStore(path string) (*RoomStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS rooms (
				lobby_room_id  TEXT PRIMARY KEY,
				ap_room_id     TEXT NOT NULL,
				normal_port    INTEGER NOT NULL,
				reduced_port   INTEGER NOT NULL,
				created_at     INTEGER NOT NULL,
				disabled       INTEGER NOT NULL DEFAULT 0,
				passwordless   INTEGER NOT NULL DEFAULT 0
		);
	`); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}
	return &RoomStore{db: db}, nil
}

func (s *RoomStore) Disable(lobbyRoomId string) error {
	_, err := s.db.Exec(`UPDATE rooms SET disabled = 1 WHERE lobby_room_id = ?`, lobbyRoomId)
	return err
}

func (s *RoomStore) Save(r RoomRecord) error {
	_, err := s.db.Exec(`
			INSERT INTO rooms (lobby_room_id, ap_room_id, normal_port, reduced_port, created_at, passwordless)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(lobby_room_id) DO UPDATE SET
					ap_room_id   = excluded.ap_room_id,
					normal_port  = excluded.normal_port,
					reduced_port = excluded.reduced_port,
					passwordless = excluded.passwordless
	`, r.LobbyRoomId, r.ApRoomId, r.NormalPort, r.ReducedPort, r.CreatedAt.Unix(), r.Passwordless)
	return err
}

func (s *RoomStore) Delete(lobbyRoomId string) error {
	_, err := s.db.Exec(`DELETE FROM rooms WHERE lobby_room_id = ?`, lobbyRoomId)
	return err
}

func (s *RoomStore) FindByLobbyRoomId(lobbyRoomId string) (*RoomRecord, error) {
	var r RoomRecord
	var ts int64
	var passwordless int
	err := s.db.QueryRow(
		`SELECT lobby_room_id, ap_room_id, normal_port, reduced_port, created_at, disabled, passwordless FROM rooms WHERE lobby_room_id = ?`,
		lobbyRoomId,
	).Scan(&r.LobbyRoomId, &r.ApRoomId, &r.NormalPort, &r.ReducedPort, &ts, &r.Disabled, &passwordless)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.CreatedAt = time.Unix(ts, 0)
	r.Passwordless = passwordless == 1
	return &r, nil
}

func (s *RoomStore) LoadAll() ([]RoomRecord, error) {
	rows, err := s.db.Query(`SELECT lobby_room_id, ap_room_id, normal_port, reduced_port, created_at, passwordless FROM rooms WHERE disabled = 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []RoomRecord
	for rows.Next() {
		var r RoomRecord
		var ts int64
		var passwordless int
		if err := rows.Scan(&r.LobbyRoomId, &r.ApRoomId, &r.NormalPort, &r.ReducedPort, &ts, &passwordless); err != nil {
			return nil, err
		}
		r.CreatedAt = time.Unix(ts, 0)
		r.Passwordless = passwordless == 1
		records = append(records, r)
	}
	return records, rows.Err()
}

type RoomRegistry struct {
	mu          sync.RWMutex
	rooms       map[string]*HostedRoom
	idToHandler map[int]*apxHandler
}

func (r *RoomRegistry) Get(roomId string) (*HostedRoom, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	room, ok := r.rooms[roomId]
	return room, ok
}

func (r *RoomRegistry) List() []*HostedRoom {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rooms := make([]*HostedRoom, 0, len(r.rooms))
	for _, room := range r.rooms {
		rooms = append(rooms, room)
	}
	return rooms
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

func (r *RoomRegistry) GetHandlerById(id int) (*apxHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	room, ok := r.idToHandler[id]
	return room, ok
}

func (r *RoomRegistry) Remove(roomId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	room, exists := r.rooms[roomId]
	if exists {
		room.cancel()
		delete(r.idToHandler, room.normalHandler.id)
		delete(r.idToHandler, room.reducedHandler.id)
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

type ApxRoomInfo struct {
	LobbyRoomId string `json:"lobby_room_id"`
	ApRoomId    string `json:"ap_room_id"`
	NormalId    int    `json:"normal_id"`
	ReducedId   int    `json:"reduced_id"`
	Disabled    bool   `json:"disabled"`
}

// e.g locationIdToName["Ocarina of Time"][3] == "Song from Saria"
type HostedRoom struct {
	lastActivity         *atomic.Int64
	lobbyRoomId          string
	apx                  *apxServer
	spheres              Spheres
	locationIdToName     map[string]map[int]string
	checkedLocations     *CheckedLocations
	sphereCache          *slotSphereCache
	completeSphere1Slots map[int]struct{}
	apRoomId             string
	normalHandler        *apxHandler
	reducedHandler       *apxHandler
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
		rooms:       make(map[string]*HostedRoom),
		idToHandler: make(map[int]*apxHandler),
	}
}

func startRoomManager(cfg *Config, reg *prometheus.Registry, metrics *metrics, tlsCfg *tls.Config, store *RoomStore) (*RoomManager, *mux.Router, error) {
	srv := &RoomManager{
		config:   cfg,
		registry: newRoomRegistry(),
		reg:      reg,
		metrics:  metrics,
		tlsCfg:   tlsCfg,
		store:    store,
	}

	r := mux.NewRouter()
	r.HandleFunc("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP)
	api := r.PathPrefix("/api").Subrouter()
	api.Use(srv.loggingMiddleware)
	api.Use(srv.authMiddleware)
	api.HandleFunc("/rooms", srv.handleListRooms).Methods(http.MethodGet)
	api.HandleFunc("/room", srv.handleUploadRoom).Methods(http.MethodPost)
	api.HandleFunc("/room/{roomId}", srv.handleRoomStatus).Methods(http.MethodGet)
	api.HandleFunc("/room/{roomId}", srv.handleRoomDelete).Methods(http.MethodDelete)
	api.HandleFunc("/room/{roomId}/start", srv.handleRoomStart).Methods(http.MethodPost)
	api.HandleFunc("/room/{roomId}/stop", srv.handleRoomStop).Methods(http.MethodPost)
	room := api.PathPrefix("/{roomId}").Subrouter()
	room.HandleFunc("/release/{slotName}", srv.handleRelease).Methods(http.MethodGet)
	room.HandleFunc("/refresh_passwords", srv.handlePasswordRefresh).Methods(http.MethodPost)
	room.HandleFunc("/password/{slotId}", srv.handlePassword).Methods(http.MethodGet)
	room.HandleFunc("/deathlinks", srv.handleDeathlinks).Methods(http.MethodGet)
	room.HandleFunc("/full_feed", srv.handleFullFeed).Methods(http.MethodGet)
	room.HandleFunc("/full_feed/{slotId}", srv.handleFullFeedUser).Methods(http.MethodPost, http.MethodDelete)
	room.HandleFunc("/bounce_exclusions", srv.handleBounceExclusionsList).Methods(http.MethodGet)
	room.HandleFunc("/bounce_exclusions/{slotId}/{tag}", srv.handleBounceExclusions).Methods(http.MethodPost, http.MethodDelete)
	room.HandleFunc("/deathlink_probability", srv.handleProbability).Methods(http.MethodGet, http.MethodPost)
	room.HandleFunc("/spheres", srv.handleAllSpheres).Methods(http.MethodGet)
	room.HandleFunc("/incomplete_sphere1", srv.handleIncompleteSphere1).Methods(http.MethodGet)
	room.HandleFunc("/spheres/{slotId}", srv.handleSpheresForSlot).Methods(http.MethodGet)
	room.HandleFunc("/debug/slot/{slotId}", srv.handleDebugTap)

	// Check every 2m, kill after 2h
	srv.startRoomKiller(time.Duration(2)*time.Minute, time.Duration(2)*time.Hour)

	// Start up rooms
	records, err := srv.store.LoadAll()
	if err != nil {
		return nil, nil, err
	}
	for _, record := range records {
		_, err := srv.startNewHostedRoom(record.ApRoomId, record.LobbyRoomId, &record.NormalPort, &record.ReducedPort, record.Passwordless, true)
		if err != nil {
			log.Printf("failed to restart room %s: %v", record.LobbyRoomId, err)
		}
	}

	return srv, r, nil
}

func startWsRouter(cfg *Config, rm *RoomManager) error {
	router := newSubdomainRouter(rm.registry, rm.tlsCfg)

	addr := fmt.Sprintf(":%d", cfg.WsPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("ws router listen on %s: %w", addr, err)
	}

	var listener net.Listener = ln
	if rm.tlsCfg != nil {
		listener = &httpDropper{listener: ln, tlsCfg: rm.tlsCfg}
		log.Printf("WS router listening on wss://%s", addr)
	} else {
		log.Printf("WS router listening on ws://%s", addr)
	}

	go func() {
		if err := http.Serve(listener, router); err != nil {
			log.Printf("ws router error: %v", err)
		}
	}()

	return nil
}

func (rm *RoomManager) handleListRooms(w http.ResponseWriter, r *http.Request) {
	rooms := rm.registry.List()
	result := make([]ApxRoomInfo, 0, len(rooms))
	for _, room := range rooms {
		result = append(result, ApxRoomInfo{
			LobbyRoomId: room.lobbyRoomId,
			ApRoomId:    room.apRoomId,
			NormalId:    room.normalHandler.id,
			ReducedId:   room.reducedHandler.id,
			Disabled:    false,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (rm *RoomManager) handleRoomStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]

	w.Header().Set("Content-Type", "application/json")

	// Check if room exists in DB (regardless of running state)
	record, err := rm.store.FindByLobbyRoomId(roomId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to query store"})
		return
	}
	if record == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return
	}

	if room, ok := rm.registry.Get(roomId); ok {
		json.NewEncoder(w).Encode(ApxRoomInfo{
			LobbyRoomId: room.lobbyRoomId,
			ApRoomId:    room.apRoomId,
			NormalId:    room.normalHandler.id,
			ReducedId:   room.reducedHandler.id,
			Disabled:    false,
		})
		return
	}

	json.NewEncoder(w).Encode(ApxRoomInfo{
		LobbyRoomId: record.LobbyRoomId,
		ApRoomId:    record.ApRoomId,
		NormalId:    0,
		ReducedId:   0,
		Disabled:    true,
	})
}

func (rm *RoomManager) handleRoomStart(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]

	w.Header().Set("Content-Type", "application/json")

	// Already running
	if _, ok := rm.registry.Get(roomId); ok {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "room already running"})
		return
	}

	record, err := rm.store.FindByLobbyRoomId(roomId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to query store"})
		return
	}
	if record == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return
	}

	// Re-enable in DB if it was disabled
	if record.Disabled {
		if _, err := rm.store.db.Exec(`UPDATE rooms SET disabled = 0 WHERE lobby_room_id = ?`, roomId); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "failed to re-enable room"})
			return
		}
	} else {
		if room, ok := rm.registry.Get(roomId); ok {
			json.NewEncoder(w).Encode(ApxRoomInfo{
				LobbyRoomId: room.lobbyRoomId,
				ApRoomId:    room.apRoomId,
				NormalId:    room.normalHandler.id,
				ReducedId:   room.reducedHandler.id,
				Disabled:    false,
			})
			return
		}
	}

	// Poll for the backing AP room to come online (15s timeout, 3s interval)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	var apPort int
	for {
		apPort, err = ensureApRoomPort(rm.config.ApApiRoot, rm.config.ApApiKey, record.ApRoomId)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			w.WriteHeader(http.StatusGatewayTimeout)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("AP room did not come online in time: %v", err)})
			return
		case <-time.After(3 * time.Second):
		}
	}
	_ = apPort

	room, err := rm.startNewHostedRoom(record.ApRoomId, record.LobbyRoomId, &record.NormalPort, &record.ReducedPort, record.Passwordless, true)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("failed to start room: %v", err)})
		return
	}

	json.NewEncoder(w).Encode(ApxRoomInfo{
		LobbyRoomId: room.lobbyRoomId,
		ApRoomId:    room.apRoomId,
		NormalId:    room.normalHandler.id,
		ReducedId:   room.reducedHandler.id,
		Disabled:    false,
	})
}

func (rm *RoomManager) handleRoomStop(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]

	w.Header().Set("Content-Type", "application/json")

	room, ok := rm.registry.Get(roomId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not running"})
		return
	}

	if err := rm.store.Disable(roomId); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to disable room in store"})
		return
	}

	rm.registry.Remove(roomId)
	apRoomId := room.apRoomId
	go func() {
		rm.stopApRoom(apRoomId)
	}()

	json.NewEncoder(w).Encode(map[string]string{"status": "stopped", "room_id": roomId})
}

func (rm *RoomManager) handleRoomDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]

	w.Header().Set("Content-Type", "application/json")

	record, err := rm.store.FindByLobbyRoomId(roomId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to query store"})
		return
	}
	if record == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return
	}

	// Remove room record
	if err := rm.store.Delete(roomId); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to delete room from store"})
		return
	}

	// Remove from active rooms list
	rm.registry.Remove(roomId)
	// Stop backing webhost room (webhost can clean up database record left behind later)
	apRoomId := record.ApRoomId
	go func() {
		rm.stopApRoom(apRoomId)
	}()

	json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "room_id": roomId})
}

type uploadRoomRequest struct {
	LobbyRoomId  string
	Passwordless bool
}

func uploadRoomRequestFromForm(r *http.Request) uploadRoomRequest {
	return uploadRoomRequest{
		LobbyRoomId:  r.FormValue("lobby_room_id"),
		Passwordless: r.FormValue("passwordless") == "true",
	}
}

func (rm *RoomManager) handleUploadRoom(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	uploadReq := uploadRoomRequestFromForm(r)

	// Parse the incoming multipart form (100MB max)
	if err := r.ParseMultipartForm(100 << 20); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to parse multipart form"})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "no file provided"})
		return
	}
	defer file.Close()

	// Forward the file to the AP server's /api/upload_room
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	fw, err := mw.CreateFormFile("file", header.Filename)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to create form file"})
		return
	}
	if _, err := io.Copy(fw, file); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to buffer file"})
		return
	}
	mw.Close()

	uploadURL := fmt.Sprintf("%s/api/upload_room", rm.config.ApApiRoot)
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, uploadURL, &buf)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to create upstream request"})
		return
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-Api-Key", rm.config.ApApiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("uploading to AP: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("AP server error: %v", string(body))})
		return
	}

	var apResult struct {
		RoomID string `json:"room_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apResult); err != nil {
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to decode AP response"})
		return
	}
	if apResult.RoomID == "" {
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{"error": "AP server returned no room_id"})
		return
	}

	log.Printf("ap room id: %s", apResult.RoomID)
	// Create the hosted room using the returned AP room ID
	// Use the AP room ID as both the lobby and AP room ID if no lobby ID is provided,
	// or accept an optional lobby_room_id form field
	lobbyRoomId := uploadReq.LobbyRoomId
	if lobbyRoomId == "" {
		lobbyRoomId = apResult.RoomID
	}

	room, err := rm.startNewHostedRoom(apResult.RoomID, lobbyRoomId, nil, nil, uploadReq.Passwordless, true)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("failed to start room: %v", err)})
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(ApxRoomInfo{
		LobbyRoomId: room.lobbyRoomId,
		ApRoomId:    room.apRoomId,
		NormalId:    room.normalHandler.id,
		ReducedId:   room.reducedHandler.id,
	})
}

func (rm *RoomManager) startNewHostedRoom(apRoomId string, lobbyRoomId string, normalIdPtr, reducedIdPtr *int, passwordless bool, save bool) (*HostedRoom, error) {
	_, exists := rm.registry.Get(lobbyRoomId)
	if exists {
		return nil, fmt.Errorf("room already exists: %s", lobbyRoomId)
	}

	apPort, err := ensureApRoomPort(rm.config.ApApiRoot, rm.config.ApApiKey, apRoomId)
	if err != nil {
		return nil, err
	}

	roomPlayers, err := fetchRoomPlayers(rm.config.ApApiRoot, apRoomId)
	if err != nil {
		return nil, fmt.Errorf("failed to get %s/api/room/%s/players from AP server, aborting: %w", rm.config.ApApiRoot, apRoomId, err)
	}

	roomInfo, err := connectAndGetRoomInfo(rm.config.APHost, apPort)
	if err != nil {
		return nil, fmt.Errorf("failed to get RoomInfo from AP server, aborting: %w", err)
	}
	if passwordless != true {
		roomInfo.Password = true
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
	if passwordless != true {
		slots, err := fetchSlotPasswords(rm.config, lobbyRoomId)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch slot passwords: %w", err)
		}
		loadPasswordsIntoStore(connRegistry, passwordStore, roomPlayers, slots)
	}
	var lokiLogger *LokiLogger
	if rm.config.LokiEndpoint != "" {
		lokiLogger = NewLokiLogger(rm.config.LokiEndpoint, lobbyRoomId)
	}

	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().Unix())

	apx := &apxServer{
		lastActivity: &lastActivity,
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
		apPort:       apPort,
		debugTap:     debugTap,
		lokiLogger:   lokiLogger,
		passwordless: passwordless,
	}

	if err := apx.prefetchDataPackages(context.Background()); err != nil {
		log.Fatalf("prefetching datapackages: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	checkedLocs := newCheckedLocations()
	room := &HostedRoom{
		lastActivity:         &lastActivity,
		lobbyRoomId:          lobbyRoomId,
		apRoomId:             apRoomId,
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
	room.startCheckedLocationPoller(ctx, rm.config.ApApiRoot, apRoomId, 30*time.Second)

	var normalId, reducedId int
	if normalIdPtr != nil {
		normalId = *normalIdPtr
	}
	if reducedIdPtr != nil {
		reducedId = *reducedIdPtr
	}
	normalHandler := apxHandler{server: apx, reduced: false, id: normalId}
	reducedHandler := apxHandler{server: apx, reduced: true, id: reducedId}

	err = rm.registry.AllocateAndRegisterHandlerPair(&normalHandler, &reducedHandler)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("allocating handler IDs: %w", err)
	}

	room.normalHandler = &normalHandler
	room.reducedHandler = &reducedHandler

	log.Printf("room %s: listening on id %d", lobbyRoomId, room.normalHandler.id)
	log.Printf("room %s: reduced listening on id %d", lobbyRoomId, room.reducedHandler.id)
	log.Printf("room %s: multiserver port %d", lobbyRoomId, apPort)

	// Add room to registry
	err = rm.registry.Add(room)
	if err != nil {
		return nil, fmt.Errorf("room already exists: %s", lobbyRoomId)
	}

	// We can run APX temporarily without a lobby using the env vars, so ignore those
	if save {
		if err := rm.store.Save(RoomRecord{
			LobbyRoomId: lobbyRoomId,
			ApRoomId:    apRoomId,
			NormalPort:  room.normalHandler.id,
			ReducedPort: room.reducedHandler.id,
			CreatedAt:   time.Now(),
		}); err != nil {
			log.Printf("failed to persist room %s: %v", lobbyRoomId, err)
		}
	}

	return room, nil
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
func (rm *HostedRoom) startCheckedLocationPoller(ctx context.Context, apApiRoot, apRoomId string, interval time.Duration) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := rm.refreshCheckedLocations(apApiRoot, apRoomId); err != nil {
				log.Printf("refreshing checked locations: %v", err)
			}
			// Interval before polling again
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
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

func (rm *RoomManager) handleRelease(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	slotName := vars["slotName"]

	// Find slot by name
	var targetSlot *NetworkSlotArray // adjust to your actual slot type
	for _, slot := range room.apx.roomPlayers.slots {
		if slot.Name == slotName {
			s := slot
			targetSlot = &s
			break
		}
	}
	if targetSlot == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "slot not found"})
		return
	}

	wsURL := fmt.Sprintf("ws://%s:%d", rm.config.APHost, room.apx.apPort)

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("connecting to AP: %v", err)})
		return
	}
	defer c.CloseNow()

	msg := fmt.Sprintf(
		`[{"cmd":"Connect","version":{"major":9000,"minor":0,"build":1,"class":"Version"},"items_handling":7,"uuid":"","tags":["Admin"],"password":null,"game":%q,"name":%q},{"cmd":"StatusUpdate","status":30}]`,
		targetSlot.Game, slotName,
	)

	if err := c.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("sending release command: %v", err)})
		return
	}

	// Give the server a moment to process, then close
	c.Close(websocket.StatusNormalClosure, "")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "released", "slot": slotName})
}

func (rm *RoomManager) startRoomKiller(interval, timeout time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			deadline := time.Now().Add(-timeout).Unix()
			for _, room := range rm.registry.List() {
				if room.lastActivity.Load() < deadline {
					log.Printf("killing idle room %s", room.lobbyRoomId)
					apRoomId := room.apRoomId
					if rm.store != nil {
						if err := rm.store.Disable(room.lobbyRoomId); err != nil {
							log.Printf("failed to disable room %s in store: %v", room.lobbyRoomId, err)
						}
					}
					rm.registry.Remove(room.lobbyRoomId)
					go func() {
						rm.stopApRoom(apRoomId)
					}()
				}
			}
		}
	}()
}

func (r *RoomRegistry) AllocateAndRegisterHandlerPair(normal, reduced *apxHandler) error {
	const minID = 1000
	const maxID = 50000
	const rangeSize = maxID - minID + 1

	r.mu.Lock()
	defer r.mu.Unlock()

	tryAllocate := func(id int) bool {
		if id < minID || id > maxID {
			return false
		}
		_, inUse := r.idToHandler[id]
		return !inUse
	}

	// Storage for found ids (useful to let filler know about preallocate ids later)
	ids := [2]int{}
	found := 0

	// Try to set to old ids first
	if normal.id != 0 && tryAllocate(normal.id) {
		ids[0] = normal.id
		found++
	}
	if reduced.id != 0 && reduced.id != ids[0] && tryAllocate(reduced.id) {
		ids[1] = reduced.id
		found++
	}

	// Fill any
	if found < 2 {
		start := minID + rand.Intn(rangeSize)
		for i := 0; i < rangeSize && found < 2; i++ {
			id := minID + (start-minID+i)%rangeSize
			if id == ids[0] || id == ids[1] {
				continue
			}
			if _, inUse := r.idToHandler[id]; !inUse {
				// Slot 0 = normal, Slot 1 = reduced. Probably a better way of making this readable
				if found == 0 || (found == 1 && ids[0] != 0) {
					if ids[0] == 0 {
						ids[0] = id
					} else {
						ids[1] = id
					}
				} else {
					ids[found] = id
				}
				found++
			}
		}
	}

	if ids[0] == 0 || ids[1] == 0 {
		return fmt.Errorf("no available Id pair in range [%d, %d]", minID, maxID)
	}

	normal.id = ids[0]
	reduced.id = ids[1]
	r.idToHandler[ids[0]] = normal
	r.idToHandler[ids[1]] = reduced

	return nil
}

func (rm *RoomManager) stopApRoom(apRoomId string) {
	url := fmt.Sprintf("%s/room/%s/stop", rm.config.ApApiRoot, apRoomId)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		log.Printf("stop AP room %s: creating request: %v", apRoomId, err)
		return
	}
	req.Header.Set("X-Api-Key", rm.config.ApApiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("stop AP room %s: %v", apRoomId, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("stop AP room %s: unexpected status %d: %s", apRoomId, resp.StatusCode, body)
	}
}

type ApRoomStatus struct {
	Alive bool `json:"alive"`
	Port  int  `json:"port"`
}

func ensureApRoomPort(apApiRoot string, apApiKey string, apRoomId string) (int, error) {
	url := fmt.Sprintf("%s/room/%s/status", apApiRoot, apRoomId)

	const timeout = 30 * time.Second
	const interval = 3 * time.Second
	deadline := time.Now().Add(timeout)

	for {
		port, err := tryGetApRoomPort(url, apApiKey, apRoomId)
		if err == nil {
			return port, nil
		}

		if time.Now().After(deadline) {
			return 0, fmt.Errorf("AP room %s did not come online within %s: %w", apRoomId, timeout, err)
		}

		log.Printf("AP room %s not ready, retrying in %s: %v", apRoomId, interval, err)
		time.Sleep(interval)
	}
}

func tryGetApRoomPort(url, apApiKey, apRoomId string) (int, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Api-Key", apApiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetching room status: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// good, pass through
	case http.StatusForbidden:
		return 0, fmt.Errorf("access denied to room %s status (check API key)", apRoomId)
	case http.StatusServiceUnavailable:
		return 0, fmt.Errorf("room %s not yet available", apRoomId)
	default:
		return 0, fmt.Errorf("unexpected status %d from /room/%s/status", resp.StatusCode, apRoomId)
	}

	var status ApRoomStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return 0, fmt.Errorf("decoding room status: %w", err)
	}

	if !status.Alive {
		return 0, fmt.Errorf("room %s is not alive", apRoomId)
	}

	return status.Port, nil
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
