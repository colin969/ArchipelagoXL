package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
)

type DiskDataPackageStore struct {
	dir     string
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func NewDiskDataPackageStore(dir string) (*DiskDataPackageStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating datapackage dir: %w", err)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &DiskDataPackageStore{dir: dir, encoder: enc, decoder: dec}, nil
}

func (d *DiskDataPackageStore) path(checksum string) string {
	return filepath.Join(d.dir, checksum+".zst")
}

func (d *DiskDataPackageStore) Has(checksum string) bool {
	_, err := os.Stat(d.path(checksum))
	return err == nil
}

func (d *DiskDataPackageStore) Write(checksum string, data []byte) error {
	compressed := d.encoder.EncodeAll(data, nil)
	tmp := d.path(checksum) + ".tmp"
	if err := os.WriteFile(tmp, compressed, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, d.path(checksum))
}

func (d *DiskDataPackageStore) Read(checksum string) ([]byte, error) {
	compressed, err := os.ReadFile(d.path(checksum))
	if err != nil {
		return nil, err
	}
	return d.decoder.DecodeAll(compressed, nil)
}

type cachedDataPackage struct {
	encoded          json.RawMessage
	itemIDToName     map[int]string
	locationIDToName map[int]string
	// Number of cachedGameDataPackage wrappers holding this as a reference
	refCount int
}

// Wrapper for a datapackage, keyed by game name
type cachedGameDataPackage struct {
	datapackage     *cachedDataPackage
	checksum        string
	encodedGameName []byte
	singleResponse  json.RawMessage
	// Number of rooms holding this as a reference
	refCount int
}

type GlobalDataPackageCache struct {
	mu         sync.RWMutex
	byChecksum map[string]*cachedDataPackage
	byGameKey  map[string]*cachedGameDataPackage
}

func newGlobalDataPackageCache() *GlobalDataPackageCache {
	return &GlobalDataPackageCache{
		byChecksum: make(map[string]*cachedDataPackage),
		byGameKey:  make(map[string]*cachedGameDataPackage),
	}
}

type GameData struct {
	ItemNameToID     map[string]int `json:"item_name_to_id"`
	LocationNameToID map[string]int `json:"location_name_to_id"`
	Checksum         string         `json:"checksum"`
}

type GetDataPackageMessage struct {
	Cmd   MessageType `json:"cmd"`
	Games []string    `json:"games"`
}

type EncodedDataPackageMessage struct {
	Cmd  MessageType              `json:"cmd"`
	Data EncodedDataPackageObject `json:"data"`
}

type DataPackageMessage struct {
	Cmd  MessageType       `json:"cmd"`
	Data DataPackageObject `json:"data"`
}

type DataPackageObject struct {
	Games map[string]GameData `json:"games"`
}

type EncodedDataPackageObject struct {
	Games map[string]json.RawMessage `json:"games"`
}

// Immutable
type DataPackageStore struct {
	fullGameResponseOptimization bool // Whether to duplicate allocations for full-game responses as a cpu optimization
	globalCache                  *GlobalDataPackageCache
	packages                     map[string]json.RawMessage // Raw encoded datapackages keyed by game name
	singleResponses              map[string]json.RawMessage // Pre-built response for single-game requests
	fullGameResponse             json.RawMessage            // Response for all games at once to avoid allocations
	encodedGameNameKeys          map[string][]byte          // pre-encoded JSON keys for game names
	ItemIDToName                 map[string]map[int]string  // game -> id -> name
	LocationIDToName             map[string]map[int]string  // game -> id -> name
	gameKeys                     []string
}

func newDataPackageStore(fullGameResponseOptimization bool, globalCache *GlobalDataPackageCache) *DataPackageStore {
	return &DataPackageStore{
		fullGameResponseOptimization: fullGameResponseOptimization,
		globalCache:                  globalCache,
		packages:                     make(map[string]json.RawMessage),
		singleResponses:              make(map[string]json.RawMessage),
		encodedGameNameKeys:          make(map[string][]byte),
		ItemIDToName:                 make(map[string]map[int]string),
		LocationIDToName:             make(map[string]map[int]string),
		gameKeys:                     make([]string, 0),
	}
}

func (c *GlobalDataPackageCache) GetOrAdd(checksum, game string, diskStorage *DiskDataPackageStore) (*cachedGameDataPackage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	gameKey := checksum + ":" + game

	// Cache hit for wrapper, increase ref on cache and return it
	if wrapper, ok := c.byGameKey[gameKey]; ok {
		wrapper.refCount++
		return wrapper, nil
	}

	// No cache hit for wrapper, try and find cache hit on inner datapackage
	datapackage, ok := c.byChecksum[checksum]
	if !ok {
		// No cache hit for inner datapackage, load from disk
		raw, err := diskStorage.Read(checksum)
		if err != nil {
			return nil, fmt.Errorf("reading datapackage %q from disk: %w", checksum, err)
		}
		var gd GameData
		if err := json.Unmarshal(raw, &gd); err != nil {
			return nil, fmt.Errorf("unmarshaling datapackage %q: %w", checksum, err)
		}
		itemIDToName := make(map[int]string, len(gd.ItemNameToID))
		for name, id := range gd.ItemNameToID {
			itemIDToName[id] = name
		}
		locationIDToName := make(map[int]string, len(gd.LocationNameToID))
		for name, id := range gd.LocationNameToID {
			locationIDToName[id] = name
		}
		datapackage = &cachedDataPackage{
			encoded:          raw,
			itemIDToName:     itemIDToName,
			locationIDToName: locationIDToName,
		}
		c.byChecksum[checksum] = datapackage
	}
	datapackage.refCount++

	encodedGameName, _ := json.Marshal(game)
	singleResponse := []byte(`[{"cmd":"DataPackage","data":{"games":{`)
	singleResponse = append(singleResponse, encodedGameName...)
	singleResponse = append(singleResponse, ':')
	singleResponse = append(singleResponse, datapackage.encoded...)
	singleResponse = append(singleResponse, `}}}]`...)

	// Return wrapper, stick in cache first
	wrapper := &cachedGameDataPackage{
		datapackage:     datapackage,
		checksum:        checksum,
		encodedGameName: encodedGameName,
		singleResponse:  singleResponse,
		refCount:        1,
	}
	c.byGameKey[gameKey] = wrapper
	return wrapper, nil
}

func (c *GlobalDataPackageCache) Get(checksum, game string) (*cachedGameDataPackage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gameKey := checksum + ":" + game
	if wrapper, ok := c.byGameKey[gameKey]; ok {
		wrapper.refCount++
		return wrapper, true
	}
	return nil, false
}

func (c *GlobalDataPackageCache) Release(gameKeys []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, gameKey := range gameKeys {
		wrapper, ok := c.byGameKey[gameKey]
		if !ok {
			continue
		}
		wrapper.refCount--
		if wrapper.refCount <= 0 {
			delete(c.byGameKey, gameKey)
			wrapper.datapackage.refCount--
			if wrapper.datapackage.refCount <= 0 {
				delete(c.byChecksum, wrapper.checksum)
			}
		}
	}
}

func (ds *DataPackageStore) AddDataPackage(game string, gd GameData) error {
	encodedData, err := json.Marshal(gd)
	if err != nil {
		return err
	}

	itemIDToName := make(map[int]string, len(gd.ItemNameToID))
	for name, id := range gd.ItemNameToID {
		itemIDToName[id] = name
	}
	locationIDToName := make(map[int]string, len(gd.LocationNameToID))
	for name, id := range gd.LocationNameToID {
		locationIDToName[id] = name
	}

	wrapper := ds.globalCache.GetOrAdd(gd.Checksum, game, encodedData, itemIDToName, locationIDToName)
	ds.attachWrapper(game, wrapper)
	return nil
}

func (ds *DataPackageStore) Release() {
	ds.globalCache.Release(ds.gameKeys)
}

// Store the references from the game data wrapper into our local store
func (ds *DataPackageStore) attachWrapper(game string, wrapper *cachedGameDataPackage) {
	ds.packages[game] = wrapper.datapackage.encoded
	ds.encodedGameNameKeys[game] = wrapper.encodedGameName
	ds.ItemIDToName[game] = wrapper.datapackage.itemIDToName
	ds.LocationIDToName[game] = wrapper.datapackage.locationIDToName
	ds.gameKeys = append(ds.gameKeys, wrapper.checksum+":"+game)
	ds.singleResponses[game] = wrapper.singleResponse
}

// MUST be called before server is live to other users. CANNOT be called safely after.
// TODO: This should really be optimized to not open a conn for each
func (s ApxRoom) loadDataPackages(diskStorage *DiskDataPackageStore) error {
	checksums := s.roomInfo.GetDataPackageChecksums()
	log.Printf("loading %d datapackages", len(checksums))

	for game, checksum := range checksums {
		// Get a wrapper ref for this game + checksum combo
		wrapper, err := s.datapackages.globalCache.GetOrAdd(checksum, game, diskStorage)
		if err != nil {
			// Per Berserker, games should work without them anyway?
			continue
		}
		s.datapackages.attachWrapper(game, wrapper)
	}

	// All DataPackages in memory, if we want to do a full game optimization, do it now
	if s.datapackages.fullGameResponseOptimization {
		games := make([]string, 0, len(s.datapackages.packages))
		for game := range s.datapackages.packages {
			games = append(games, game)
		}
		// We know this is a valid set of games, ignore err
		s.datapackages.fullGameResponse, _ = s.datapackages.buildResponse(games)
	}

	return nil
}

func (s ApxRoom) handleGetDataPackage(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	// We only need 1 field, no point re and unmarshaling the whole message just for the struct
	var requestedGames []string
	var packet struct {
		Games []string `json:"games"`
	}
	if err := json.Unmarshal(raw, &packet); err == nil {
		requestedGames = packet.Games
	}

	// Nothing requested = all requested. Not great clients :(
	if len(requestedGames) == 0 {
		for game := range s.roomInfo.DatapackageChecksums {
			requestedGames = append(requestedGames, game)
		}
	}

	games := make([]string, 0)
	for _, game := range requestedGames {
		if _, ok := s.roomInfo.DatapackageChecksums[game]; ok {
			games = append(games, game)
		}
	}
	return s.sendDataPackages(ctx, connState.clientConn, games)
}

// Stitch together to avoid doing any json ops on the already encoded datapackage
func (s ApxRoom) sendDataPackages(ctx context.Context, client *websocket.Conn, games []string) error {
	if len(games) == 1 {
		raw, ok := s.datapackages.singleResponses[games[0]]
		if !ok {
			return fmt.Errorf("unknown datapackage for %q", games[0])
		}
		if s.metrics != nil {
			s.metrics.datapackageBytesSent.WithLabelValues(s.lobbyRoomId).Add(float64(len(raw)))
			s.metrics.bytesSent.WithLabelValues(s.lobbyRoomId, "__datapackage__", "__datapackage__").Add(float64(len(raw)))
		}
		return client.Write(ctx, websocket.MessageText, raw)
	}

	if s.datapackages.fullGameResponseOptimization {
		if len(games) == len(s.datapackages.packages) && s.datapackages.fullGameResponse != nil {
			if s.metrics != nil {
				s.metrics.datapackageBytesSent.WithLabelValues(s.lobbyRoomId).Add(float64(len(s.datapackages.fullGameResponse)))
			}
			return client.Write(ctx, websocket.MessageText, s.datapackages.fullGameResponse)
		}
	}

	// Optimization off for full game respons, or we've got a weird batched request
	msg, err := s.datapackages.buildResponse(games)
	if err != nil {
		return err
	}

	if s.metrics != nil {
		s.metrics.datapackageBytesSent.WithLabelValues(s.lobbyRoomId).Add(float64(len(msg)))
	}
	return client.Write(ctx, websocket.MessageText, msg)
}

func (ds *DataPackageStore) buildResponse(games []string) (json.RawMessage, error) {
	const header = `[{"cmd":"DataPackage","data":{"games":{`
	const footer = `}}}]`

	// Estimate size of data first, so we avoid extra allocations when building message later
	// Probably not perfect, but much better
	size := len(header) + len(footer) + len(games) - 1
	for _, game := range games {
		raw, ok := ds.packages[game]
		if !ok {
			return nil, fmt.Errorf("unknown datapackage for %q", game)
		}
		// account for "<game_name>": key
		size += len(ds.encodedGameNameKeys[game]) + 1 + len(raw)
	}

	msg := make([]byte, 0, size)
	msg = append(msg, header...)
	for i, game := range games {
		raw := ds.packages[game]
		if i > 0 {
			msg = append(msg, ',')
		}
		msg = append(msg, ds.encodedGameNameKeys[game]...)
		msg = append(msg, ':')
		msg = append(msg, raw...)
	}
	msg = append(msg, footer...)

	return msg, nil
}
