package main

import (
	"apx/multidata"
	"archive/zip"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/gorilla/mux"
	"github.com/klauspost/compress/zstd"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type TrackerSpheres []TrackerSphereLocations

// { slot_id : [loc_id, loc_id, loc_id] }
type TrackerSphereLocations map[int][]int64

type SphereResult struct {
	Locations []LocationEntry `json:"locations"`
	Total     int             `json:"total"`
	Checked   int             `json:"checked"`
}

type RoomManager struct {
	config           *Config
	registry         *RoomRegistry
	metrics          *metrics
	tlsCfg           *tls.Config
	reg              *prometheus.Registry
	store            *RoomStore
	diskDataPackages *DiskDataPackageStore
}

type RoomRegistry struct {
	rooms        map[string]*HostedRoom
	idToHandler  map[int]*apxHandler
	datapackages *GlobalDataPackageCache
	mu           sync.RWMutex
}

const (
	RoomTimeout = time.Duration(2) * time.Hour
)

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

func (r *RoomRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.rooms)
}

func (r *RoomRegistry) CountConnections() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var total int
	for _, room := range r.rooms {
		total += room.apx.connections.ConnectedClientCount()
	}
	return total
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
		room.apx.datapackages.Release()
		delete(r.idToHandler, room.normalHandler.id)
		delete(r.idToHandler, room.reducedHandler.id)
	}

	delete(r.rooms, roomId)
}

type CheckedLocations struct {
	checkedLocations   map[int]map[int64]bool
	checkedLocationsMu sync.RWMutex
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
	LobbyRoomId       string `json:"lobby_room_id"`
	NormalId          int    `json:"normal_id"`
	ReducedId         int    `json:"reduced_id"`
	Disabled          bool   `json:"disabled"`
	PerSlotPasswords  bool   `json:"per_slot_passwords"`
	DeathlinkDisabled bool   `json:"deathlink_disabled"`
	ReducedAccess     bool   `json:"reduced_access"`
	CreatedAt         string `json:"created_at"`
	LastActivity      string `json:"last_activity"`
}

// e.g locationIdToName["Ocarina of Time"][3] == "Song from Saria"
type HostedRoom struct {
	ctx                  context.Context
	reducedHandler       *apxHandler
	apx                  *ApxRoom
	locationIdToName     map[string]map[int]string
	checkedLocations     *CheckedLocations
	sphereCache          *slotSphereCache
	completeSphere1Slots map[int]struct{}
	normalHandler        *apxHandler
	lastActivity         *atomic.Int64
	cancel               context.CancelFunc
	lobbyRoomId          string
	completeSphere1Mu    sync.Mutex
	deathlinkDisabled    bool
	reducedAccess        bool
}

type RoomState struct {
	BounceTagExclusions  map[int][]string `json:"bounce_tag_exclusions"`
	Deaths               map[int]int      `json:"deaths"`
	SlotBounceExclusions []int            `json:"slot_bounce_exclusions"`
	FullFeedSlots        []int            `json:"full_feed_slots"`
}

type Deathlink struct {
	Slot      string  `json:"slot"`
	Source    string  `json:"source"`
	Cause     *string `json:"cause"`
	CreatedAt string  `json:"created_at"`
}

type LocationEntry struct {
	Name    string
	ID      int64
	Checked bool
}

func (l LocationEntry) MarshalJSON() ([]byte, error) {
	return json.Marshal([3]any{l.ID, l.Name, l.Checked})
}

// Cache sphere responses to save compute
type slotSphereCache struct {
	data  map[int][]byte
	dirty map[int]bool
	mu    sync.RWMutex
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

func newRoomRegistry(metrics *metrics) *RoomRegistry {
	return &RoomRegistry{
		rooms:        make(map[string]*HostedRoom),
		idToHandler:  make(map[int]*apxHandler),
		datapackages: newGlobalDataPackageCache(metrics),
	}
}

func startRoomManager(cfg *Config, reg *prometheus.Registry, metrics *metrics, tlsCfg *tls.Config, store *RoomStore) (*RoomManager, *mux.Router, error) {
	dpDiskStorage, err := NewDiskDataPackageStore(cfg.DataPackageStoragePath, cfg.StandaloneMode, metrics)
	if err != nil {
		return nil, nil, err
	}

	if metrics != nil {
		savedCount := dpDiskStorage.SavedCount()
		metrics.totalDatapackages.Set(float64(savedCount))
	}

	if !cfg.StandaloneMode {
		if err := os.MkdirAll(cfg.SlotDataStoragePath, 0755); err != nil {
			return nil, nil, fmt.Errorf("creating slotdata dir")
		}
	}

	srv := &RoomManager{
		config:           cfg,
		registry:         newRoomRegistry(metrics),
		reg:              reg,
		metrics:          metrics,
		tlsCfg:           tlsCfg,
		store:            store,
		diskDataPackages: dpDiskStorage,
	}

	// If store disabled, no DB to read
	if store.enabled {
		err = srv.cleanOrphanedDataPackagesOnStartup()
		if err != nil {
			log.Panicf("Failed to clean up orphaned datapackages: %v", err)
		}
	}

	roomLoggedHandler := func(h http.HandlerFunc) http.HandlerFunc {
		return srv.logRequestMiddleware(h).ServeHTTP
	}

	r := mux.NewRouter()
	r.HandleFunc("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP)
	if !(cfg.DisableAdminDashboard || cfg.StandaloneMode) {
		srv.registerAdminRoutes(r)
	}
	api := r.PathPrefix("/api").Subrouter()
	api.Use(srv.loggingMiddleware)
	api.HandleFunc("/room/{roomId}", srv.handleRoomStatus).Methods(http.MethodGet)
	api.HandleFunc("/room/{roomId}/players", srv.handleRoomPlayers).Methods(http.MethodGet)
	// Webhost compatible APIs
	api.HandleFunc("/room_status/{roomId}", srv.handleRoomWebhostApiStatus).Methods(http.MethodGet)
	api.HandleFunc("/tracker/{roomId}", srv.handleRoomWebhostApiTracker).Methods(http.MethodGet)
	// /tracker/.../overview
	// /tracker/.../slot/...
	// /tracker/.../slot/.../overview
	// /tracker/.../slot/.../items
	// /tracker/.../slot/.../checks
	// /tracker/.../slot/.../hints
	// /tracker/.../slot/.../datastorage
	// /tracker/.../checks
	// /tracker/.../hints
	// /tracker/.../datastorage
	api.HandleFunc("/static_tracker/{roomId}", srv.handleRoomWebhostApiStaticTracker).Methods(http.MethodGet)
	api.HandleFunc("/slot_data_tracker/{roomId}", srv.handleRoomWebhostApiTrackerSlotData).Methods(http.MethodGet)
	api.HandleFunc("/datapackage/{checksum}", srv.handleDatapackageByChecksum).Methods(http.MethodGet)
	if !cfg.StandaloneMode {
		mgmt := api.PathPrefix("").Subrouter()
		mgmt.Use(srv.authMiddleware)
		mgmt.HandleFunc("/rooms", srv.handleListRooms).Methods(http.MethodGet)
		mgmt.HandleFunc("/room", srv.handleUploadRoom).Methods(http.MethodPost)
	}
	roomAdmin := api.PathPrefix("/room/{roomId}").Subrouter()
	roomAdmin.Use(srv.authMiddleware)
	roomAdmin.Use(srv.validateRoomIdMiddleware)
	if !cfg.StandaloneMode {
		roomAdmin.HandleFunc("/", srv.handleRoomDelete).Methods(http.MethodDelete)
		roomAdmin.HandleFunc("/start", srv.handleRoomStart).Methods(http.MethodPost)
		roomAdmin.HandleFunc("/stop", srv.handleRoomStop).Methods(http.MethodPost)
	}
	roomAdmin.HandleFunc("/state", srv.handleRoomState).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/names/{typeName}/{gameName}", srv.handleItemNames).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/release/{slotName}", roomLoggedHandler(srv.handleRelease)).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/refresh_slot_passwords", roomLoggedHandler(srv.handlePasswordRefresh)).Methods(http.MethodPost)
	roomAdmin.HandleFunc("/server_password", srv.handleAdminPassword).Methods(http.MethodGet, http.MethodDelete, http.MethodPost)
	roomAdmin.HandleFunc("/slot_password/{slotId}", srv.handlePassword).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/deathlinks", srv.handleDeathlinks).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/full_feed", srv.handleFullFeed).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/full_feed/{slotId}", roomLoggedHandler(srv.handleFullFeedUser)).Methods(http.MethodPost, http.MethodDelete)
	roomAdmin.HandleFunc("/bounce_exclusions", srv.handleBounceExclusionsList).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/bounce_exclusions/{slotId}/{tag}", roomLoggedHandler(srv.handleBounceExclusions)).Methods(http.MethodPost, http.MethodDelete)
	roomAdmin.HandleFunc("/slot_bounce_exclusions", srv.handleSlotBounceExclusionsList).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/slot_bounce_exclusions/{slotId}", roomLoggedHandler(srv.handleSlotBounceExclusions)).Methods(http.MethodPost, http.MethodDelete)
	roomAdmin.HandleFunc("/deathlink_probability", srv.handleDeathlinkProbability).Methods(http.MethodGet, http.MethodPost)
	roomAdmin.HandleFunc("/spheres", srv.handleAllSpheres).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/incomplete_sphere1", srv.handleIncompleteSphere1).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/spheres/{slotId}", srv.handleSpheresForSlot).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/debug/slot/{slotId}", roomLoggedHandler(srv.handleDebugTap))
	// Alt names need more work to be listable and deletable, will reset on restart for now
	roomAdmin.HandleFunc("/alt_connect_name/slot/{slotName}", roomLoggedHandler(srv.handleAltConnectNamesBySlot)).Methods(http.MethodGet)
	roomAdmin.HandleFunc("/alt_connect_name/{altName}", roomLoggedHandler(srv.handleAltConnectName)).Methods(http.MethodPost, http.MethodDelete)

	// Check every 2m, kill after 2h
	srv.startRoomKiller(time.Duration(2)*time.Minute, RoomTimeout)

	// Start up rooms
	records, err := srv.store.LoadAllActive()
	if err != nil {
		return nil, nil, err
	}
	totalRooms, err := srv.store.CountRooms()
	if err != nil {
		return nil, nil, err
	}
	if metrics != nil {
		metrics.totalRooms.Set(float64(totalRooms))
	}
	started := 0
	for _, record := range records {
		if record.Definition == nil {
			log.Printf("skipping room %s: no definition stored", record.LobbyRoomId)
			continue
		}

		room, err := srv.startNewHostedRoom(record.LobbyRoomId, record.Definition, &record.NormalId, &record.ReducedId,
			record.PerSlotPasswords, record.DeathlinkDisabled, record.ReducedAccess, record.DeathlinkProbability, true, nil)
		if err != nil {
			log.Printf("failed to restart room %s: %v", record.LobbyRoomId, err)
			continue
		}
		started += 1
		// Restore persistent data
		srv.restoreBounceTagExclusions(room)
		srv.restoreSlotBounceExclusions(room)
		srv.restoreFullFeedSlots(room)
		srv.restoreSlotDeaths(room)
		srv.restoreAltConnectNames(room)
	}
	if metrics != nil {
		metrics.activeRooms.Set(float64(started))
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

	server := &http.Server{
		Handler:  router,
		ErrorLog: log.New(io.Discard, "", 0), // Stop a bunch of ws:// clients flooding logs
	}

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("ws router error: %v", err)
		}
	}()

	return nil
}

func startStandaloneWsRouter(rm *RoomManager, normalPort int, reducedPort int) error {
	makeHandler := func(targetId int) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h, ok := rm.registry.GetHandlerById(targetId)
			if !ok {
				http.Error(w, "room not found", http.StatusNotFound)
				return
			}
			h.ServeHTTP(w, r)
		})
	}

	startOne := func(port int, reduced bool) error {
		addr := fmt.Sprintf(":%d", port)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("standalone ws listen on %s: %w", addr, err)
		}
		var listener net.Listener = ln
		if rm.tlsCfg != nil {
			listener = &httpDropper{listener: ln, tlsCfg: rm.tlsCfg}
		}
		label := "normal"
		if reduced {
			label = "reduced"
		}
		log.Printf("Standalone WS (%s) listening on :%d", label, port)
		server := &http.Server{
			Handler:  makeHandler(port),
			ErrorLog: log.New(io.Discard, "", 0),
		}
		go func() {
			if err := server.Serve(listener); err != nil {
				log.Printf("standalone ws (%s) error: %v", label, err)
			}
		}()
		return nil
	}

	if err := startOne(normalPort, false); err != nil {
		return err
	}
	return startOne(reducedPort, true)
}

// Only works on room routes
func (rm *RoomManager) logRequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		roomId := vars["roomId"]

		room, ok := rm.registry.Get(roomId)
		if ok && room.apx.lokiLogger != nil {
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
			if err == nil {
				r.Body = io.NopCloser(bytes.NewReader(body))
				msg, _ := json.Marshal(map[string]string{
					"method": r.Method,
					"path":   r.URL.Path,
					"body":   string(body),
				})
				room.apx.lokiLogger.LogApi(LogLevelDebug, msg)
			}
		}

		next.ServeHTTP(w, r)
	})
}

func (rm *RoomManager) handleListRooms(w http.ResponseWriter, r *http.Request) {
	rooms := rm.registry.List()
	result := make([]ApxRoomInfo, 0, len(rooms))
	for _, room := range rooms {
		result = append(result, ApxRoomInfo{
			LobbyRoomId:       room.lobbyRoomId,
			NormalId:          room.normalHandler.id,
			ReducedId:         room.reducedHandler.id,
			Disabled:          false,
			PerSlotPasswords:  room.apx.perSlotPasswords,
			DeathlinkDisabled: room.deathlinkDisabled,
			ReducedAccess:     room.reducedAccess,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (rm *RoomManager) saveDataPackagesToDisk(md *multidata.MultiData) {
	encoder, err := newZstdEncoder()
	if err != nil {
		log.Printf("Failed to open zstd encoder, skipping saving of datapackage to disk")
	}
	defer encoder.Close()
	for game, dp := range md.DataPackage {
		data, err := json.Marshal(dp)
		if err != nil {
			log.Printf("failed to marshal datapackage for %q, skipping: %v", game, err)
			continue
		}
		if _, err := rm.diskDataPackages.WriteIfAbsent(dp.Checksum, data, encoder); err != nil {
			log.Printf("failed to write datapackage %q (%s) to disk, skipping: %v", game, dp.Checksum, err)
		}
	}
}

func (rm *RoomManager) saveSlotDataToDisk(lobbyRoomId string, md *multidata.MultiData) (map[int][2]int, error) {
	encoder, err := newZstdEncoder()
	if err != nil {
		return nil, err
	}
	defer encoder.Close()

	path, err := safePathFromJoin(lobbyRoomId, rm.config.SlotDataStoragePath)
	if err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	index := make(map[int][2]int, len(md.SlotData))
	var offset int
	for slotId, slotData := range md.SlotData {
		raw, err := json.Marshal(slotData)
		if err != nil {
			return nil, fmt.Errorf("marshal slot %d: %w", slotId, err)
		}
		if len(raw) <= 2 {
			continue
		}
		compressed := encoder.EncodeAll(raw, nil)
		if _, err := f.Write(compressed); err != nil {
			return nil, err
		}
		index[slotId] = [2]int{offset, len(compressed)}
		offset += len(compressed)
	}

	return index, nil
}

func (rm *RoomManager) buildSlotDataBlob(md *multidata.MultiData) ([]byte, map[int][2]int, error) {
	encoder, err := newZstdEncoder()
	if err != nil {
		return nil, nil, err
	}
	defer encoder.Close()

	var buf bytes.Buffer
	index := make(map[int][2]int, len(md.SlotData))
	var offset int
	for slotId, slotData := range md.SlotData {
		raw, err := json.Marshal(slotData)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal slot %d: %w", slotId, err)
		}
		if len(raw) <= 2 {
			continue
		}
		compressed := encoder.EncodeAll(raw, nil)
		buf.Write(compressed)
		index[slotId] = [2]int{offset, len(compressed)}
		offset += len(compressed)
	}

	return buf.Bytes(), index, nil
}

func (rm *RoomManager) handleUploadRoom(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	uploadReq := uploadRoomRequestFromForm(r)

	if uploadReq.LobbyRoomId == "" {
		uploadReq.LobbyRoomId = randomId(24)
	}

	if !isValidLobbyRoomId(uploadReq.LobbyRoomId) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
		return
	}

	exists, err := rm.store.LobbyRoomIdExists(uploadReq.LobbyRoomId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to check room id"})
		return
	}
	if exists {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby_room_id already in use"})
		return
	}

	// 100mb, kinda big, needed for the stupidly large rooms. Maybe add extra auth on a frontend if you want to contain it.
	if err := r.ParseMultipartForm(maxUploadLimit); err != nil {
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

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to read file"})
		return
	}

	if strings.HasSuffix(strings.ToLower(header.Filename), ".zip") {
		zr, err := zip.NewReader(bytes.NewReader(fileBytes), int64(len(fileBytes)))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("invalid zip: %v", err)})
			return
		}
		fileBytes, err = multidata.ReadArchipelagoFromZip(zr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("reading zip: %v", err)})
			return
		}

	}

	md, err := multidata.ParseMultidata(fileBytes)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("invalid multidata: %v", err)})
		return
	}

	err = validateDataPackageChecksums(md)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("invalid multidata: %v", err)})
		return
	}

	lobbyRoomId := uploadReq.LobbyRoomId

	room, err := rm.startRoomFromMultiData(lobbyRoomId, md, nil, nil,
		uploadReq.PerSlotPasswords, uploadReq.DeathlinkDisabled, uploadReq.ReducedAccess, 1, true)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("failed to start room: %v", err)})
		return
	}

	if rm.metrics != nil {
		rm.metrics.totalRooms.Inc()
		rm.metrics.activeRooms.Inc()
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(ApxRoomInfo{
		LobbyRoomId: room.lobbyRoomId,
		NormalId:    room.normalHandler.id,
		ReducedId:   room.reducedHandler.id,
		Disabled:    false,
	})
}

func randomId(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func (rm *RoomManager) handleRoomStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]
	w.Header().Set("Content-Type", "application/json")

	if !isValidLobbyRoomId(roomId) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
		return
	}

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
		info := ApxRoomInfo{
			LobbyRoomId:       room.lobbyRoomId,
			NormalId:          room.normalHandler.id,
			ReducedId:         room.reducedHandler.id,
			Disabled:          false,
			PerSlotPasswords:  room.apx.perSlotPasswords,
			DeathlinkDisabled: room.deathlinkDisabled,
			ReducedAccess:     room.reducedAccess,
		}
		json.NewEncoder(w).Encode(info)
		return
	}

	json.NewEncoder(w).Encode(ApxRoomInfo{
		LobbyRoomId:       record.LobbyRoomId,
		NormalId:          0,
		ReducedId:         0,
		Disabled:          true,
		PerSlotPasswords:  record.PerSlotPasswords,
		DeathlinkDisabled: record.DeathlinkDisabled,
		ReducedAccess:     record.ReducedAccess,
	})
}

type roomStartRequest struct {
	PerSlotPasswords  *bool `json:"per_slot_passwords"`
	DeathlinkDisabled *bool `json:"deathlink_disabled"`
	ReducedAccess     *bool `json:"reduced_access"`
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

	record, err := rm.store.FindFullByLobbyRoomId(roomId)
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

	var startReq roomStartRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&startReq); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
			return
		}
	}

	// Update room's apx config from json body
	if startReq.PerSlotPasswords != nil {
		record.PerSlotPasswords = *startReq.PerSlotPasswords
	}
	if startReq.DeathlinkDisabled != nil {
		if *startReq.DeathlinkDisabled != record.DeathlinkDisabled {
			if err := rm.store.DeleteBounceTagExclusionsForTag(record.LobbyRoomId, "DeathLink"); err != nil {
				log.Printf("failed to delete stale DeathLink tag exclusions for %s: %v", record.LobbyRoomId, err)
			}
		}
		record.DeathlinkDisabled = *startReq.DeathlinkDisabled
	}
	if startReq.ReducedAccess != nil {
		if *startReq.ReducedAccess != record.ReducedAccess {
			if err := rm.store.DeleteFullFeedSlots(record.LobbyRoomId); err != nil {
				log.Printf("failed to delete stale full feed slots for %s: %v", record.LobbyRoomId, err)
			}
		}
		record.ReducedAccess = *startReq.ReducedAccess
	}

	// Re-enable in DB if it was disabled
	if record.Disabled {
		if err := rm.store.Enable(roomId); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "failed to re-enable room"})
			return
		}
	} else {
		if room, ok := rm.registry.Get(roomId); ok {
			json.NewEncoder(w).Encode(ApxRoomInfo{
				LobbyRoomId: room.lobbyRoomId,
				NormalId:    room.normalHandler.id,
				ReducedId:   room.reducedHandler.id,
				Disabled:    false,
			})
			return
		}
	}

	if record.Definition == nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "room has no definition stored? Broken"})
		return
	}

	room, err := rm.startNewHostedRoom(record.LobbyRoomId, record.Definition, &record.NormalId, &record.ReducedId,
		record.PerSlotPasswords, record.DeathlinkDisabled, record.ReducedAccess, record.DeathlinkProbability, true, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("failed to start room: %v", err)})
		return
	}

	// Restore persistent data
	rm.restoreBounceTagExclusions(room)
	rm.restoreSlotBounceExclusions(room)
	rm.restoreFullFeedSlots(room)
	rm.restoreSlotDeaths(room)
	rm.restoreAltConnectNames(room)

	if rm.metrics != nil {
		rm.metrics.activeRooms.Inc()
	}

	json.NewEncoder(w).Encode(ApxRoomInfo{
		LobbyRoomId: room.lobbyRoomId,
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

	saved := room.Close()
	if saved {
		log.Printf("Closed room %s (saved)", roomId)
	} else {
		log.Printf("Closed room %s (not saved)", roomId)
	}

	if err := rm.store.Disable(roomId); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to disable room in store"})
		return
	}

	rm.registry.Remove(roomId)

	json.NewEncoder(w).Encode(map[string]string{"status": "stopped", "room_id": roomId})
}

type ApiPlayer [2]int
type ApiPlayersResponse map[string]ApiPlayer

func (rm *RoomManager) handleRoomPlayers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]
	w.Header().Set("Content-Type", "application/json")

	if !isValidLobbyRoomId(roomId) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
		return
	}

	room, ok := rm.registry.Get(roomId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return
	}

	players := make(ApiPlayersResponse, len(room.apx.state.NameToSlot))
	for name, slotInfo := range room.apx.state.NameToSlot {
		players[name] = [2]int{slotInfo.Team, slotInfo.Slot}
	}

	json.NewEncoder(w).Encode(players)
}

type WebhostApiRoomStatusResponse struct {
	LastActivity string      `json:"last_activity"`
	Tracker      string      `json:"tracker"`
	Downloads    []any       `json:"downloads"`
	Players      [][2]string `json:"players"`
	LastPort     int         `json:"last_port"`
	Timeout      int         `json:"timeout"`
}

func (rm *RoomManager) handleRoomWebhostApiStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]
	w.Header().Set("Content-Type", "application/json")

	if !isValidLobbyRoomId(roomId) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
		return
	}

	room, ok := rm.registry.Get(roomId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return
	}

	lastActivity := room.lastActivity.Load()
	players := make([][2]string, len(room.apx.state.SlotInfo))
	for _, player := range room.apx.state.SlotInfo {
		players = append(players, [2]string{player.Name, player.Game})
	}

	json.NewEncoder(w).Encode(WebhostApiRoomStatusResponse{
		Downloads:    []any{},
		Players:      players,
		LastActivity: time.Unix(lastActivity, 0).UTC().Format(http.TimeFormat),
		LastPort:     rm.config.WsPort,
		Timeout:      int(RoomTimeout.Seconds()),
		Tracker:      room.lobbyRoomId, // Just use the same
	})
}

type TrackerAlias struct {
	Alias  *string `json:"alias"`
	Team   int     `json:"team"`
	Player int     `json:"player"`
}

type TrackerPlayerItems struct {
	Items  []NetworkItem `json:"items"`
	Team   int           `json:"team"`
	Player int           `json:"player"`
}

func (tp TrackerPlayerItems) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, 40+len(tp.Items)*40)
	b = append(b, `{"team":`...)
	b = strconv.AppendInt(b, int64(tp.Team), 10)
	b = append(b, `,"player":`...)
	b = strconv.AppendInt(b, int64(tp.Player), 10)
	b = append(b, `,"items":[`...)
	for i, item := range tp.Items {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '[')
		b = strconv.AppendInt(b, item.Item, 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, item.Location, 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(item.Player), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(item.Flags), 10)
		b = append(b, ']')
	}
	b = append(b, `]}`...)
	return b, nil
}

type TrackerPlayerChecks struct {
	Locations []int64 `json:"locations"`
	Team      int     `json:"team"`
	Player    int     `json:"player"`
}

type TrackerTotalChecksDone struct {
	Team       int `json:"team"`
	ChecksDone int `json:"checks_done"`
}

type TrackerHint struct {
	Hints  []Hint `json:"hints"`
	Team   int    `json:"team"`
	Player int    `json:"player"`
}

func (th TrackerHint) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, 40+len(th.Hints)*80)
	b = append(b, `{"team":`...)
	b = strconv.AppendInt(b, int64(th.Team), 10)
	b = append(b, `,"player":`...)
	b = strconv.AppendInt(b, int64(th.Player), 10)
	b = append(b, `,"hints":[`...)
	for i, h := range th.Hints {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '[')
		b = strconv.AppendInt(b, int64(h.ReceivingPlayer), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(h.FindingPlayer), 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, h.Location, 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, h.Item, 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(h.ItemFlags), 10)
		b = append(b, ',')
		if h.Found {
			b = append(b, "true"...)
		} else {
			b = append(b, "false"...)
		}
		b = append(b, ',')
		b = strconv.AppendQuote(b, h.Entrance)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(h.Status), 10)
		b = append(b, ']')
	}
	b = append(b, `]}`...)
	return b, nil
}

// TODO: Find out what a 'no activity' timer responds with
type TrackerSlotTimer struct {
	Time   *string `json:"time"`
	Team   int     `json:"team"`
	Player int     `json:"player"`
}

type TrackerPlayerStatus struct {
	Team   int `json:"team"`
	Player int `json:"player"`
	Status int `json:"status"`
}

type WebhostApiTrackerData struct {
	Aliases             []TrackerAlias           `json:"aliases"`
	PlayerItemsReceived []TrackerPlayerItems     `json:"player_items_received"`
	PlayerChecksDone    []TrackerPlayerChecks    `json:"player_checks_done"`
	TotalChecksDone     []TrackerTotalChecksDone `json:"total_checks_done"`
	Hints               []TrackerHint            `json:"hints"`
	ActivityTimers      []TrackerSlotTimer       `json:"activity_timers"`
	ConnectionTimers    []TrackerSlotTimer       `json:"connection_timers"`
	PlayerStatus        []TrackerPlayerStatus    `json:"player_status"`
}

func (rm *RoomManager) handleRoomWebhostApiTracker(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]
	w.Header().Set("Content-Type", "application/json")

	if !isValidLobbyRoomId(roomId) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
		return
	}

	room, ok := rm.registry.Get(roomId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return
	}

	state := room.apx.state

	// Aliases
	players := state.PlayersNetwork.Get()
	aliases := make([]TrackerAlias, 0, len(players))
	for _, p := range players {
		alias := p.Alias
		aliases = append(aliases, TrackerAlias{
			Team:   p.Team,
			Player: p.Slot,
			Alias:  &alias,
		})
	}

	// Player items received
	// Group by team+slot, emit [item, location, player, flags]
	allItems := state.ReceivedItems.GetAll()
	playerItemsReceived := make([]TrackerPlayerItems, 0, len(state.SlotInfo))
	for ts := range state.SlotInfo {
		playerItemsReceived = append(playerItemsReceived, TrackerPlayerItems{
			Team:   ts.Team,
			Player: ts.Slot,
			Items:  allItems[ReceivedItemsKey{Team: ts.Team, Slot: ts.Slot, RemoteItems: true}],
		})
	}

	// Player checks done — [locId, flags]
	// Group checked locations by team+slot
	teamCheckTotals := make(map[int]int) // team -> total checks
	playerChecksDone := make([]TrackerPlayerChecks, 0, len(state.SlotInfo))
	for ts := range state.SlotInfo {
		checkedIds := state.Checks.GetChecked(ts)
		playerChecksDone = append(playerChecksDone, TrackerPlayerChecks{
			Team:      ts.Team,
			Player:    ts.Slot,
			Locations: checkedIds,
		})
		teamCheckTotals[ts.Team] += len(checkedIds)
	}

	// Total checks done per team
	totalChecksDone := make([]TrackerTotalChecksDone, 0, len(state.SlotInfo))
	for team, count := range teamCheckTotals {
		totalChecksDone = append(totalChecksDone, TrackerTotalChecksDone{
			Team:       team,
			ChecksDone: count,
		})
	}

	// Hints — group by receiving player's team+slot
	// [receiving_player, finding_player, location, item, item_flags, found, entrance, status]
	hintsMap := state.Hints.GetAllReceiverHints()
	hints := make([]TrackerHint, 0, len(state.SlotInfo))
	for ts := range state.SlotInfo {
		hints = append(hints, TrackerHint{
			Team:   ts.Team,
			Player: ts.Slot,
			Hints:  hintsMap[ts],
		})
	}

	// Activity timers
	activityTimersMap := state.SlotActivityTimers.GetAll()
	activityTimers := make([]TrackerSlotTimer, 0, len(state.SlotInfo))
	for ts := range state.SlotInfo {
		var t *string
		if v, ok := activityTimersMap[ts]; ok {
			s := time.Unix(v, 0).UTC().Format(time.RFC3339)
			t = &s
		}
		activityTimers = append(activityTimers, TrackerSlotTimer{
			Team:   ts.Team,
			Player: ts.Slot,
			Time:   t,
		})
	}

	// Connection timers
	connectionTimersMap := state.SlotConnectionTimers.GetAll()
	connectionTimers := make([]TrackerSlotTimer, 0, len(state.SlotInfo))
	for ts := range state.SlotInfo {
		var t *string
		if v, ok := connectionTimersMap[ts]; ok {
			s := time.Unix(v, 0).UTC().Format(time.RFC3339)
			t = &s
		}
		connectionTimers = append(connectionTimers, TrackerSlotTimer{
			Team:   ts.Team,
			Player: ts.Slot,
			Time:   t,
		})
	}

	// Player statuses
	clientStatusMap := state.ClientStatuses.GetAll()
	playerStatus := make([]TrackerPlayerStatus, 0, len(state.SlotInfo))
	for ts := range state.SlotInfo {
		playerStatus = append(playerStatus, TrackerPlayerStatus{
			Team:   ts.Team,
			Player: ts.Slot,
			Status: int(clientStatusMap[ts]),
		})
	}

	// TODO Finish

	json.NewEncoder(w).Encode(WebhostApiTrackerData{
		Aliases:             aliases,
		PlayerItemsReceived: playerItemsReceived,
		PlayerChecksDone:    playerChecksDone,
		TotalChecksDone:     totalChecksDone,
		Hints:               hints,
		ActivityTimers:      activityTimers,
		ConnectionTimers:    connectionTimers,
		PlayerStatus:        playerStatus,
	})
}

type TrackerItemLinkGroup struct {
	Slot    int    `json:"slot"`
	Name    string `json:"name"`
	Members []int  `json:"members"`
}

type TrackerDataPackage struct {
	Checksum string `json:"checksum"`
}

type TrackerPlayerLocationTotal struct {
	Player         int `json:"player"`
	Team           int `json:"team"`
	TotalLocations int `json:"total_locations"`
}

type TrackerPlayerGame struct {
	Player int    `json:"player"`
	Team   int    `json:"team"`
	Game   string `json:"game"`
}

type WebhostApiTrackerStaticData struct {
	ItemLinkGroups       []TrackerItemLinkGroup        `json:"groups"`
	DataPackages         map[string]TrackerDataPackage `json:"datapackage"`
	PlayerLocationTotals []TrackerPlayerLocationTotal  `json:"player_location_total"`
	PlayerGames          []TrackerPlayerGame           `json:"player_game"`
}

func (rm *RoomManager) handleRoomWebhostApiStaticTracker(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]
	w.Header().Set("Content-Type", "application/json")

	if !isValidLobbyRoomId(roomId) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
		return
	}

	room, ok := rm.registry.Get(roomId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return
	}

	itemLinkGroups := make([]TrackerItemLinkGroup, 0)
	playerGames := make([]TrackerPlayerGame, 0, len(room.apx.state.SlotInfo))
	playerLocationTotals := make([]TrackerPlayerLocationTotal, 0, len(room.apx.state.SlotInfo))
	for ts, player := range room.apx.state.SlotInfo {
		playerGames = append(playerGames, TrackerPlayerGame{
			Player: ts.Slot,
			Team:   ts.Team,
			Game:   player.Game,
		})
		playerLocationTotals = append(playerLocationTotals, TrackerPlayerLocationTotal{
			Player:         ts.Slot,
			Team:           ts.Team,
			TotalLocations: len(room.apx.state.Locations[ts]),
		})
		if player.Type == SlotTypeGroup && len(player.GroupMembers) > 0 {
			members := make([]int, len(player.GroupMembers))
			copy(members, player.GroupMembers)
			itemLinkGroups = append(itemLinkGroups, TrackerItemLinkGroup{
				Slot:    ts.Slot,
				Name:    player.Name,
				Members: members,
			})
		}
	}

	json.NewEncoder(w).Encode(WebhostApiTrackerStaticData{
		ItemLinkGroups:       itemLinkGroups,
		PlayerLocationTotals: playerLocationTotals,
		PlayerGames:          playerGames,
		DataPackages:         room.apx.roomInfo.GetTrackerDataPackages(),
	})
}

type TrackerSlotData struct {
	Player   int32           `json:"player"`
	SlotData json.RawMessage `json:"slot_data"`
}

func (rm *RoomManager) handleRoomWebhostApiTrackerSlotData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]
	w.Header().Set("Content-Type", "application/json")

	if !isValidLobbyRoomId(roomId) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
		return
	}

	room, ok := rm.registry.Get(roomId)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "room not found"})
		return
	}

	response := make([]TrackerSlotData, 0, len(room.apx.state.SlotDataIndex))
	players := make([]int, 0, len(room.apx.state.SlotDataIndex))
	for player := range room.apx.state.SlotDataIndex {
		players = append(players, player)
	}
	sort.Ints(players)

	for _, player := range players {
		slotData, err := room.apx.GetSlotData(TeamSlot{0, player})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("failed reading slot data for player %d", player)})
			return
		}
		response = append(response, TrackerSlotData{
			Player:   int32(player),
			SlotData: slotData,
		})
	}

	sort.Slice(response, func(i, j int) bool {
		return response[i].Player < response[j].Player
	})
	json.NewEncoder(w).Encode(response)
}

func (rm *RoomManager) restoreBounceTagExclusions(room *HostedRoom) {
	exclusions, err := rm.store.LoadBounceTagExclusions(room.lobbyRoomId)
	if err != nil {
		log.Printf("failed to load bounce tag exclusions for %s: %v", room.lobbyRoomId, err)
		return
	}
	for slotId, tags := range exclusions {
		for _, tag := range tags {
			if tag == "DeathLink" && room.deathlinkDisabled {
				// Flip back value saved if deathlinkDisabled is set, since the value was flipped when saved
				room.apx.bounceInfo.UnexcludeByTag(slotId, tag)
			} else {
				room.apx.bounceInfo.ExcludeByTag(slotId, tag)
			}
		}
	}
}

func (rm *RoomManager) restoreSlotBounceExclusions(room *HostedRoom) {
	slotExclusions, err := rm.store.LoadSlotBounceExclusions(room.lobbyRoomId)
	if err != nil {
		log.Printf("failed to load slot bounce exclusions for %s: %v", room.lobbyRoomId, err)
		return
	} else {
		for _, slotId := range slotExclusions {
			room.apx.bounceInfo.LimitToOwnSlot(slotId)
		}
	}
}

func (rm *RoomManager) restoreFullFeedSlots(room *HostedRoom) {
	slots, err := rm.store.LoadFullFeedSlots(room.lobbyRoomId)
	if err != nil {
		log.Printf("failed to load full feed slots for %s: %v", room.lobbyRoomId, err)
		return
	}
	for _, slotId := range slots {
		if !room.reducedAccess {
			// Flip back value saved if reducedAccess is set, since the value was flipped when saved
			room.apx.fullFeed.Delete(slotId)
		} else {
			room.apx.fullFeed.Set(slotId)
		}
	}
}

func (rm *RoomManager) restoreSlotDeaths(room *HostedRoom) {
	deaths, err := rm.store.LoadSlotDeaths(room.lobbyRoomId)
	if err != nil {
		log.Printf("failed to load slot deaths for %s: %v", room.lobbyRoomId, err)
		return
	}
	room.apx.bounceInfo.mu.Lock()
	defer room.apx.bounceInfo.mu.Unlock()
	maps.Copy(room.apx.bounceInfo.counts, deaths)
}

func (rm *RoomManager) restoreAltConnectNames(room *HostedRoom) {
	names, err := rm.store.LoadAltConnectNames(room.lobbyRoomId)
	if err != nil {
		log.Printf("failed to load alt connect names for %s: %v", room.lobbyRoomId, err)
		return
	}
	for altName, slotName := range names {
		room.apx.altConnectNames.SetAltName(slotName, altName)
	}
}

func (rm *RoomManager) handleRoomDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	roomId := vars["roomId"]

	log.Printf("trying delete for %s", roomId)

	w.Header().Set("Content-Type", "application/json")

	room, ok := rm.registry.Get(roomId)
	if ok {
		checksums := uniqueChecksums(room.apx.roomInfo.DatapackageChecksums)
		if len(checksums) > 0 {
			orphaned, err := rm.store.RemoveDataPackageRefs(roomId, checksums)
			if err != nil {
				log.Printf("failed to decrement datapackage refs for room %s: %v", roomId, err)
			} else {
				for _, checksum := range orphaned {
					if err := rm.diskDataPackages.Delete(checksum); err != nil && !os.IsNotExist(err) {
						log.Printf("failed to delete orphaned datapackage %s: %v", checksum, err)
					}
				}
			}
		}

		room.Close()
	}

	// TODO: Slot data deletion. Probably a really rare race cond that could prevent deletion?

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

	// Remove persistant data entries
	if err := rm.store.DeleteFullFeedSlots(roomId); err != nil {
		log.Printf("failed to delete full feed slots for room %s: %v", roomId, err)
	}
	if err := rm.store.DeleteSlotBounceExclusions(roomId); err != nil {
		log.Printf("failed to delete slot bounce exclusions for room %s: %v", roomId, err)
	}
	if err := rm.store.DeleteBounceTagExclusions(roomId); err != nil {
		log.Printf("failed to delete bounce tag exclusions for room %s: %v", roomId, err)
	}
	if err := rm.store.DeleteAltConnectNames(roomId); err != nil {
		log.Printf("failed to delete alt connect names for room %s: %v", roomId, err)
	}
	if err := rm.store.DeleteSlotDeaths(roomId); err != nil {
		log.Printf("failed to delete deaths for room %s: %v", roomId, err)
	}

	// Remove room record
	if err := rm.store.Delete(roomId); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "failed to delete room from store"})
		return
	}

	if rm.metrics != nil {
		rm.metrics.totalRooms.Dec()
	}

	// Remove from active rooms list
	rm.registry.Remove(roomId)

	json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "room_id": roomId})
}

type uploadRoomRequest struct {
	LobbyRoomId       string
	PerSlotPasswords  bool
	DeathlinkDisabled bool
	ReducedAccess     bool
}

func uploadRoomRequestFromForm(r *http.Request) uploadRoomRequest {
	return uploadRoomRequest{
		LobbyRoomId:       r.FormValue("lobby_room_id"),
		PerSlotPasswords:  r.FormValue("per_slot_passwords") == "true",
		DeathlinkDisabled: r.FormValue("deathlink_disabled") == "true",
		ReducedAccess:     r.FormValue("reduced_access") == "true",
	}
}

func (rm *RoomManager) startRoomFromMultiData(
	lobbyRoomId string,
	md *multidata.MultiData,
	normalIdPtr, reducedIdPtr *int,
	perSlotPasswords, deathlinkDisabled, reducedAccess bool,
	deathlinkProbability float64,
	save bool,
) (*HostedRoom, error) {
	def := roomDefinitionFromMultidata(md)
	var standaloneSlotData []byte
	if rm.config.StandaloneMode {
		blob, index, err := rm.buildSlotDataBlob(md)
		if err != nil {
			return nil, err
		}
		def.SlotDataIndex = index
		standaloneSlotData = blob
	} else {
		rm.saveDataPackagesToDisk(md)
		index, err := rm.saveSlotDataToDisk(lobbyRoomId, md)
		if err != nil {
			return nil, err
		}
		def.SlotDataIndex = index
	}
	return rm.startNewHostedRoom(lobbyRoomId, def, normalIdPtr, reducedIdPtr,
		perSlotPasswords, deathlinkDisabled, reducedAccess, deathlinkProbability, save, standaloneSlotData)
}

func (rm *RoomManager) startNewHostedRoom(lobbyRoomId string, roomDef *RoomDefinition, normalIdPtr, reducedIdPtr *int,
	perSlotPasswords bool, deathlinkDisabled bool, reducedAccess bool, deathlinkProbability float64, save bool, standaloneSlotData []byte) (*HostedRoom, error) {

	_, exists := rm.registry.Get(lobbyRoomId)
	if exists {
		return nil, fmt.Errorf("room already exists: %s", lobbyRoomId)
	}

	// Construct the Ap State from the Room definition
	apState, err := apStateFromDefinition(roomDef)
	if err != nil {
		return nil, fmt.Errorf("failed to parse multidata: %v", err)
	}

	altConnectNames := newAltConnectNames()
	passwordStore := newPasswordStore()
	fullFeedStore := newFullFeedStore()
	connRegistry := newConnectionRegistry(&lobbyRoomId, rm.metrics)

	// Only use memory costly optimizations when at least 75 datapackages in the room
	var useDatapackageOptimization = len(roomDef.DatapackChecksums) >= 5000
	datapackageCache := newDataPackageStore(useDatapackageOptimization, rm.registry.datapackages) // TODO: Add config flag
	bounceInfo := newBounceInfoStore()
	bounceInfo.deathlinkProbability = deathlinkProbability
	debugTap := newDebugTap(len(apState.SlotInfo))
	if perSlotPasswords == true {
		slots, err := fetchSlotPasswords(rm.config, lobbyRoomId)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch slot passwords: %w", err)
		}
		loadPasswordsIntoStore(connRegistry, passwordStore, apState.NameToSlot, slots)
	}
	var lokiLogger *LokiLogger
	// if rm.config.LokiEndpoint != "" {
	// 	lokiLogger = NewLokiLogger(rm.config.LokiEndpoint, lobbyRoomId, rm.metrics)
	// }

	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().Unix())

	// Room config options
	if deathlinkDisabled {
		for slotInfo := range apState.SlotInfo {
			bounceInfo.ExcludeByTag(slotInfo.Slot, "DeathLink")
		}
	}

	if !reducedAccess {
		for slotInfo := range apState.SlotInfo {
			fullFeedStore.Set(slotInfo.Slot)
		}
	}

	roomInfoStore := newRoomInfoStore(roomDef.DatapackChecksums)

	ctx, cancel := context.WithCancel(context.Background())

	// Create APX room
	apx := &ApxRoom{
		lastActivity:     &lastActivity,
		logf:             log.Printf,
		config:           rm.config,
		roomInfo:         roomInfoStore,
		altConnectNames:  altConnectNames,
		passwords:        passwordStore,
		fullFeed:         fullFeedStore,
		connections:      connRegistry,
		bounceInfo:       bounceInfo,
		datapackages:     datapackageCache,
		metrics:          rm.metrics,
		lobbyRoomId:      lobbyRoomId,
		debugTap:         debugTap,
		lokiLogger:       lokiLogger,
		perSlotPasswords: perSlotPasswords,
		ipLimiter:        newIPRateLimiter(),
		state:            apState,
		logDeath: func(slotId int) {
			if err := rm.store.IncrementSlotDeathCount(lobbyRoomId, slotId); err != nil {
				log.Printf("failed to persist death count for room %s slot %d: %v", lobbyRoomId, slotId, err)
			}
		},
		standaloneSlotData: standaloneSlotData,
	}
	apx.chatCommandRouter = newCommandRouter(apx)
	apx.buildRoomInfoFromState()
	apx.populateReadData()

	if save != false {
		if rm.config.DatabaseURL != "" {
			apx.saveHandler = newSafeSaveHandler(newSaveDBHandler(rm.store.Pool()))
		} else {
			apx.saveHandler = newSafeSaveHandler(newSaveFileHandler(rm.config.SavesPath))
		}
		ls, err := apx.saveHandler.Load(context.Background(), apx.lobbyRoomId, apx.state.Seed)
		if err != nil {
			log.Printf("Failed to load save state for room %s: %v", apx.lobbyRoomId, err)
		} else {
			log.Printf("Loaded save for %s with seed %s", apx.lobbyRoomId, apx.state.Seed)
			if ls != nil {
				err = applySnapshot(apx.state, ls)
			}
			if err != nil {
				apx.saveHandler = nil
				cancel()
				return nil, fmt.Errorf("Failed to apply save snapshot for room %s, possible bad state: %v", apx.lobbyRoomId, err)
			}
		}
	}

	if err := apx.loadDataPackages(rm.diskDataPackages); err != nil {
		log.Fatalf("loading datapackages: %v", err)
	}

	checkedLocs := newCheckedLocations()
	room := &HostedRoom{
		lastActivity:         &lastActivity,
		lobbyRoomId:          lobbyRoomId,
		apx:                  apx,
		sphereCache:          newSlotSphereCache(),
		completeSphere1Slots: map[int]struct{}{},
		checkedLocations:     &checkedLocs,
		ctx:                  ctx,
		cancel:               cancel,
		deathlinkDisabled:    deathlinkDisabled,
		reducedAccess:        reducedAccess,
	}

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

	// Add room to registry
	err = rm.registry.Add(room)
	if err != nil {
		return nil, fmt.Errorf("room already exists: %s", lobbyRoomId)
	}

	// We can run APX temporarily without a lobby using the env vars, so ignore if save not specified
	if save {
		if err := rm.store.Save(RoomRecord{
			LobbyRoomId:       lobbyRoomId,
			NormalId:          room.normalHandler.id,
			ReducedId:         room.reducedHandler.id,
			CreatedAt:         time.Now(),
			PerSlotPasswords:  perSlotPasswords,
			DeathlinkDisabled: deathlinkDisabled,
			ReducedAccess:     reducedAccess,
			Definition:        roomDef,
		}); err != nil {
			log.Printf("failed to persist room %s: %v", lobbyRoomId, err)
		}
		checksums := uniqueChecksums(roomDef.DatapackChecksums)
		if err := rm.store.AddDataPackageRefs(lobbyRoomId, checksums); err != nil {
			log.Printf("failed to increment datapackage refs for room %s: %v", lobbyRoomId, err)
		}
	}

	return room, nil
}

func (rm *RoomManager) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
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

func (rm *RoomManager) validateRoomIdMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		roomId := mux.Vars(r)["roomId"]
		if !isValidLobbyRoomId(roomId) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "lobby room id must be alphanumeric"})
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
	loadPasswordsIntoStore(room.apx.connections, room.apx.passwords, room.apx.state.NameToSlot, slots)
}

type PasswordRequest struct {
	Password string `json:"password"`
}

func (rm *RoomManager) handleAdminPassword(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	switch r.Method {
	case http.MethodPost:
		var req PasswordRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		room.apx.state.ServerOptions.SetAdminPassword(req.Password)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		room.apx.state.ServerOptions.SetAdminPassword("")
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		password := room.apx.state.ServerOptions.GetAdminPassword()
		json.NewEncoder(w).Encode(PasswordRequest{password})
	}
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

	// When reducedAccess, Full feed is disabled for all slots by default.
	// Value saved to db is flipped, so it can be restored flipped if room restarts
	// Kinda weird but it works
	isDefaultEnabled := !room.reducedAccess

	switch r.Method {
	case http.MethodPost:
		room.apx.fullFeed.Set(slotId)
		if isDefaultEnabled {
			if err := rm.store.DeleteFullFeedSlot(room.lobbyRoomId, slotId); err != nil {
				log.Printf("failed to remove full feed override for room %s slot %d: %v", room.lobbyRoomId, slotId, err)
			}
		} else {
			if err := rm.store.SetFullFeedSlot(room.lobbyRoomId, slotId); err != nil {
				log.Printf("failed to save full feed slot for room %s slot %d: %v", room.lobbyRoomId, slotId, err)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"full_feed": true})

	case http.MethodDelete:
		room.apx.fullFeed.Delete(slotId)
		if isDefaultEnabled {
			if err := rm.store.SetFullFeedSlot(room.lobbyRoomId, slotId); err != nil {
				log.Printf("failed to save full feed removal override for room %s slot %d: %v", room.lobbyRoomId, slotId, err)
			}
		} else {
			if err := rm.store.DeleteFullFeedSlot(room.lobbyRoomId, slotId); err != nil {
				log.Printf("failed to remove full feed slot for room %s slot %d: %v", room.lobbyRoomId, slotId, err)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"full_feed": false})
	}
}

func (rm *RoomManager) handleBounceExclusionsList(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(room.apx.bounceInfo.GetTagExclusions())
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

	// When deathlinkDisabled, DeathLink is excluded for all slots by default.
	// Value saved to db is flipped, so it can be restored flipped if room restarts
	// Kinda weird but it works
	isDefaultExcluded := tag == "DeathLink" && room.deathlinkDisabled

	switch r.Method {
	case http.MethodPost:
		room.apx.bounceInfo.ExcludeByTag(slotId, tag)
		if isDefaultExcluded {
			if err := rm.store.DeleteBounceTagExclusion(room.lobbyRoomId, slotId, tag); err != nil {
				log.Printf("failed to remove tag bounce exclusion override for slot %d tag %s: %v", slotId, tag, err)
			}
		} else {
			if err := rm.store.AddBounceTagExclusion(room.lobbyRoomId, slotId, tag); err != nil {
				log.Printf("failed to save tag bounce exclusion for slot %d tag %s: %v", slotId, tag, err)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"excluded": true})

	case http.MethodDelete:
		room.apx.bounceInfo.UnexcludeByTag(slotId, tag)
		if isDefaultExcluded {
			if err := rm.store.AddBounceTagExclusion(room.lobbyRoomId, slotId, tag); err != nil {
				log.Printf("failed to save tag bounce exclusion override for slot %d tag %s: %v", slotId, tag, err)
			}
		} else {
			if err := rm.store.DeleteBounceTagExclusion(room.lobbyRoomId, slotId, tag); err != nil {
				log.Printf("failed to remove tag bounce exclusion for slot %d tag %s: %v", slotId, tag, err)
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"excluded": false})
	}
}

func (rm *RoomManager) handleSlotBounceExclusionsList(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(room.apx.bounceInfo.GetSlotExclusions())
}

func (rm *RoomManager) handleSlotBounceExclusions(w http.ResponseWriter, r *http.Request) {
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

	switch r.Method {
	case http.MethodPost:
		room.apx.bounceInfo.LimitToOwnSlot(slotId)
		if err = rm.store.AddSlotBounceExclusion(room.lobbyRoomId, slotId); err != nil {
			log.Printf("failed to save slot bounce exclusion for room %s slot %d: %v", room.lobbyRoomId, slotId, err)
		}
		json.NewEncoder(w).Encode(map[string]any{"limited": true})

	case http.MethodDelete:
		room.apx.bounceInfo.RemoveLimitToOwnSlot(slotId)
		if err = rm.store.DeleteSlotBounceExclusion(room.lobbyRoomId, slotId); err != nil {
			log.Printf("failed to remove slot bounce exclusion for room %s slot %d: %v", room.lobbyRoomId, slotId, err)
		}
		json.NewEncoder(w).Encode(map[string]any{"limited": false})
	}
}

func (rm *RoomManager) handleDeathlinkProbability(w http.ResponseWriter, r *http.Request) {
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
			Probability float64 `json:"probability"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
			return
		}
		if body.Probability < 0 || body.Probability > 1 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "probability must be between 0 and 1"})
			return
		}
		room.apx.bounceInfo.SetProbability(body.Probability)
		if err := rm.store.SaveDeathlinkProbability(room.lobbyRoomId, body.Probability); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "db: error setting deathlink probability"})
			return
		}
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
	slotInfo, ok := room.apx.state.SlotInfo[TeamSlot{0, slotId}]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "slot not found"})
		return
	}

	locationIDToName, ok := room.apx.datapackages.LocationIDToName[slotInfo.Game]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "datapackage not found"})
		return
	}
	checkedLocations := room.checkedLocations.GetSlot(slotId)

	result := make([]SphereResult, 0, len(room.apx.state.Spheres))
	for _, sphere := range room.apx.state.Spheres {
		locIDs, ok := sphere[TeamSlot{0, slotId}]
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

	result := make(map[string]json.RawMessage, len(room.apx.state.NameToSlot))

	for teamSlot, slotInfo := range room.apx.state.SlotInfo {
		if cached, ok := room.sphereCache.Get(teamSlot.Slot); ok {
			result[slotInfo.Name] = json.RawMessage(cached)
			continue
		}

		locationIDToName, ok := room.apx.datapackages.LocationIDToName[slotInfo.Game]
		if !ok {
			continue
		}
		checkedLocations := room.checkedLocations.GetSlot(slotInfo.Slot)

		slotResult := make([]SphereResult, 0, len(room.apx.state.Spheres))
		for _, sphere := range room.apx.state.Spheres {
			locIDs, ok := sphere[teamSlot]
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
		room.sphereCache.Set(teamSlot.Slot, b)
		result[slotInfo.Name] = json.RawMessage(b)
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

	if len(room.apx.state.Spheres) == 0 {
		json.NewEncoder(w).Encode([]int{})
		return
	}

	sphere1 := room.apx.state.Spheres[0]

	// Check if status of incomplete slots has changed
	checkedLocs := room.checkedLocations.Get()
	room.completeSphere1Mu.Lock()
	for teamSlot, locIDs := range sphere1 {
		if _, done := room.completeSphere1Slots[teamSlot.Slot]; done {
			continue
		}
		if !isSphere1Incomplete(locIDs, checkedLocs[teamSlot.Slot]) {
			room.completeSphere1Slots[teamSlot.Slot] = struct{}{}
		}
	}

	// Build list of all incomplete slots
	result := make([]int, 0)
	for teamSlot := range room.apx.state.SlotInfo {
		if _, done := room.completeSphere1Slots[teamSlot.Slot]; !done {
			result = append(result, teamSlot.Slot)
		}
	}
	room.completeSphere1Mu.Unlock()

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
	if _, ok := room.apx.state.SlotInfo[TeamSlot{0, slotId}]; !ok {
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

type AltConnectNameRequest struct {
	SlotName string `json:"slot_name"`
}

func (rm *RoomManager) handleAltConnectName(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	altName := vars["altName"]

	switch r.Method {
	case http.MethodPost:
		var req AltConnectNameRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.SlotName == "" {
			http.Error(w, "slot_name is required", http.StatusBadRequest)
			return
		}

		_, exists := room.apx.state.NameToSlot[req.SlotName]
		if !exists {
			http.Error(w, "slot not found", http.StatusNotFound)
			return
		}
		_, exists = room.apx.state.NameToSlot[altName]
		if exists {
			http.Error(w, "alt name is already taken by real slot name", http.StatusConflict)
			return
		}
		existingAlt := room.apx.altConnectNames.GetAltName(altName)
		if existingAlt != nil {
			if *existingAlt == req.SlotName {
				http.Error(w, "alt name already mapped to this slot", http.StatusConflict)
				return
			}
			http.Error(w, "alt name is already taken by another slot", http.StatusConflict)
			return
		}

		room.apx.altConnectNames.SetAltName(req.SlotName, altName)
		if err := rm.store.AddAltConnectName(room.lobbyRoomId, altName, req.SlotName); err != nil {
			log.Printf("failed to persist alt connect name for room %s: %v", room.lobbyRoomId, err)
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		room.apx.altConnectNames.RemoveAltName(altName)
		if err := rm.store.DeleteAltConnectName(room.lobbyRoomId, altName); err != nil {
			log.Printf("failed to delete alt connect name for room %s: %v", room.lobbyRoomId, err)
		}
		w.WriteHeader(http.StatusOK)
	}
}

func (rm *RoomManager) handleAltConnectNamesBySlot(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	slotName := vars["slotName"]

	_, exists := room.apx.state.NameToSlot[slotName]
	if !exists {
		http.Error(w, "slot not found", http.StatusNotFound)
		return
	}

	altNames := room.apx.altConnectNames.GetAltNamesBySlot(slotName)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(altNames)
}

func isSphere1Incomplete(locIDs []int64, checkedLocations map[int64]bool) bool {
	for _, locID := range locIDs {
		if !checkedLocations[locID] {
			return true
		}
	}
	return false
}

// Slot IDs are keys, so JSON turns them to strings, we want the numbers
func (s *TrackerSphereLocations) UnmarshalJSON(data []byte) error {
	var raw map[string][]int64
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*s = make(TrackerSphereLocations, len(raw))
	for k, v := range raw {
		id, err := strconv.ParseInt(k, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid slot_id key %q: %w", k, err)
		}
		(*s)[int(id)] = v
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

func (rm *RoomManager) handleRoomState(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	fullFeedMap := room.apx.fullFeed.Get()
	fullFeedSlots := make([]int, 0, len(fullFeedMap))
	for slotId := range fullFeedMap {
		fullFeedSlots = append(fullFeedSlots, slotId)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(RoomState{
		BounceTagExclusions:  room.apx.bounceInfo.GetTagExclusions(),
		SlotBounceExclusions: room.apx.bounceInfo.GetSlotExclusions(),
		Deaths:               room.apx.bounceInfo.Get(),
		FullFeedSlots:        fullFeedSlots,
	})
}

func (rm *RoomManager) handleItemNames(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	gameName := vars["gameName"]
	typeName := vars["typeName"]
	log.Printf("[handleItemNames] requested game=%q type=%q", gameName, typeName)

	var names map[int64]string
	switch typeName {
	case "item":
		names, ok = room.apx.datapackages.ItemIDToName[gameName]
	case "location":
		names, ok = room.apx.datapackages.LocationIDToName[gameName]
	default:
		http.Error(w, "invalid type, must be 'item' or 'location'", http.StatusBadRequest)
		return
	}

	if !ok {
		http.Error(w, fmt.Sprintf("no data found for game %q", gameName), http.StatusNotFound)
		return
	}

	values := make([]string, 0, len(names))
	for _, v := range names {
		values = append(values, v)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(values); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

func (rm *RoomManager) handleRelease(w http.ResponseWriter, r *http.Request) {
	room, ok := rm.roomFromRequest(w, r)
	if !ok {
		return
	}

	vars := mux.Vars(r)
	slotName := vars["slotName"]

	_, ok = room.apx.state.NameToSlot[slotName]
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "slot not found"})
		return
	}

	// TODO: Release

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "released", "slot": slotName})
}

func (rm *RoomManager) handleDatapackageByChecksum(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	checksum := vars["checksum"]
	data, ok := rm.registry.datapackages.GetByChecksum(checksum)
	if ok {
		// Hit cache
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	} else {
		// Try disk
		data, err := rm.diskDataPackages.Read(checksum)
		if err != nil || data == nil {
			// Not on disk
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "datapackage not found"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

func (rm *RoomManager) startRoomKiller(interval, timeout time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			deadline := time.Now().Add(-timeout).Unix()
			for _, room := range rm.registry.List() {
				if room.lastActivity.Load() >= deadline {
					continue
				}

				lobbyId := room.lobbyRoomId
				saved := room.Close()
				if saved {
					log.Printf("Closed room %s (idle timeout, saved)", lobbyId)
				} else {
					log.Printf("Closed room %s (idle timeout, not saved)", lobbyId)
				}
				if rm.store != nil {
					if err := rm.store.Disable(lobbyId); err != nil {
						log.Printf("failed to disable room %s in store: %v", lobbyId, err)
					}
				}
				rm.registry.Remove(lobbyId)
			}
		}
	}()
}

const (
	roomMinID = 10000
	roomMaxID = 50000
)

func (r *RoomRegistry) AllocateAndRegisterHandlerPair(normal, reduced *apxHandler) error {
	const rangeSize = roomMaxID - roomMinID + 1

	r.mu.Lock()
	defer r.mu.Unlock()

	tryAllocate := func(id int) bool {
		if id < roomMinID || id > roomMaxID {
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

	// Fill any ports missing
	if found < 2 {
		start := roomMinID + rand.Intn(rangeSize)
		for i := 0; i < rangeSize && found < 2; i++ {
			id := roomMinID + (start-roomMinID+i)%rangeSize
			if id == ids[0] || id == ids[1] {
				continue
			}
			if _, inUse := r.idToHandler[id]; !inUse {
				if ids[0] == 0 {
					ids[0] = id
				} else {
					ids[1] = id
				}
				found++
			}
		}
	}

	if ids[0] == 0 || ids[1] == 0 {
		return fmt.Errorf("no available Id pair in range [%d, %d]", roomMinID, roomMaxID)
	}

	normal.id = ids[0]
	reduced.id = ids[1]
	r.idToHandler[ids[0]] = normal
	r.idToHandler[ids[1]] = reduced

	return nil
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

func (rm *RoomManager) CloseAll() {
	rooms := rm.registry.List()
	var (
		wg     sync.WaitGroup
		closed atomic.Int64
		saved  atomic.Int64
	)
	for _, room := range rooms {
		wg.Add(1)
		go func(r *HostedRoom) {
			defer wg.Done()
			if r.Close() {
				saved.Add(1)
			}
			closed.Add(1)
		}(room)
	}
	wg.Wait()
	log.Printf("closed %d rooms, %d saved", closed.Load(), saved.Load())
}

func (rm *RoomManager) cleanOrphanedDataPackagesOnStartup() error {
	// Any file not referenced by an existing room record can be deleted
	var knownChecksums []string
	rows, err := rm.store.pool.Query(context.Background(), `SELECT DISTINCT checksum FROM room_datapackage_refs`)
	if err != nil {
		return fmt.Errorf("querying datapackage refs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return err
		}
		knownChecksums = append(knownChecksums, c)
	}

	known := make(map[string]struct{}, len(knownChecksums))
	for _, c := range knownChecksums {
		known[c] = struct{}{}
	}

	entries, err := os.ReadDir(rm.diskDataPackages.dir)
	if err != nil {
		return fmt.Errorf("reading datapackage dir: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		checksum := strings.TrimSuffix(name, ".zst")
		if strings.HasSuffix(name, ".tmp") || checksum == name {
			continue // skip tmp files and anything not .zst
		}
		if _, ok := known[checksum]; !ok {
			if err := rm.diskDataPackages.Delete(checksum); err != nil && !os.IsNotExist(err) {
				log.Printf("failed to delete orphaned datapackage %s: %v", checksum, err)
			}
		}
	}
	return nil
}

func (hs *HostedRoom) Close() bool {
	if hs.apx.metrics != nil {
		hs.apx.metrics.activeRooms.Dec()
	}
	hs.cancel()

	if hs.apx != nil && hs.apx.saveHandler != nil {
		ss := snapshotState(hs.apx.state)
		err := hs.apx.saveHandler.Save(context.Background(), hs.apx.lobbyRoomId, ss)
		if err != nil {
			log.Printf("Failed to save room %s: %v", hs.apx.lobbyRoomId, err)
		}
		return err == nil
	}

	return false
}

func uniqueChecksums(m map[string]string) []string {
	seen := make(map[string]struct{}, len(m))
	for _, checksum := range m {
		seen[checksum] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for checksum := range seen {
		result = append(result, checksum)
	}
	return result
}

func newZstdEncoder() (*zstd.Encoder, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, err
	}
	return enc, nil
}

func isValidLobbyRoomId(id string) bool {
	if id == "" {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

func safePathFromJoin(lobbyRoomId string, storagePath string) (string, error) {
	if !isValidLobbyRoomId(lobbyRoomId) {
		return "", fmt.Errorf("invalid lobby_room_id: %q", lobbyRoomId)
	}
	resolved, err := filepath.Abs(filepath.Join(storagePath, lobbyRoomId+".zst"))
	if err != nil {
		return "", err
	}
	base, err := filepath.Abs(storagePath)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(resolved, base+string(os.PathSeparator)) {
		return "", fmt.Errorf("path traversal detected")
	}
	return resolved, nil
}

func validateDataPackageChecksums(md *multidata.MultiData) error {
	for game, dp := range md.DataPackage {
		sum, err := dp.ComputeChecksum()
		if err != nil {
			return fmt.Errorf("game %q: failed to compute checksum: %w", game, err)
		}
		if sum != dp.Checksum {
			return fmt.Errorf("game %q: checksum mismatch: got %s, expected %s", game, sum, dp.Checksum)
		}
	}
	return nil
}
