package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type cachedDataPackage struct {
	encoded          json.RawMessage
	itemIDToName     map[int64]string
	locationIDToName map[int64]string
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
	ItemNameToID     map[string]int64 `json:"item_name_to_id"`
	LocationNameToID map[string]int64 `json:"location_name_to_id"`
	Checksum         string           `json:"checksum"`
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
	packages                     map[string]json.RawMessage  // Raw encoded datapackages keyed by game name
	singleResponses              map[string]json.RawMessage  // Pre-built response for single-game requests
	fullGameResponse             json.RawMessage             // Response for all games at once to avoid allocations
	encodedGameNameKeys          map[string][]byte           // pre-encoded JSON keys for game names
	ItemIDToName                 map[string]map[int64]string // game -> id -> name
	LocationIDToName             map[string]map[int64]string // game -> id -> name
	gameKeys                     []string
}

func newDataPackageStore(fullGameResponseOptimization bool, globalCache *GlobalDataPackageCache) *DataPackageStore {
	return &DataPackageStore{
		fullGameResponseOptimization: fullGameResponseOptimization,
		globalCache:                  globalCache,
		packages:                     make(map[string]json.RawMessage),
		singleResponses:              make(map[string]json.RawMessage),
		encodedGameNameKeys:          make(map[string][]byte),
		ItemIDToName:                 make(map[string]map[int64]string),
		LocationIDToName:             make(map[string]map[int64]string),
		gameKeys:                     make([]string, 0),
	}
}

func (c *GlobalDataPackageCache) GetOrAdd(checksum, game string, encoded json.RawMessage, itemIDToName, locationIDToName map[int64]string) *cachedGameDataPackage {
	c.mu.Lock()
	defer c.mu.Unlock()

	gameKey := checksum + ":" + game
	// Cache hit for wrapper, increase ref on cache and return it
	if wrapper, ok := c.byGameKey[gameKey]; ok {
		wrapper.refCount++
		return wrapper
	}

	// No cache hit for wrapper, try and find cache hit on inner datapackage
	datapackage, ok := c.byChecksum[checksum]
	if !ok {
		// No cache hit, make our own and put it into the cache
		datapackage = &cachedDataPackage{
			encoded:          encoded,
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
	return wrapper
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

	itemIDToName := make(map[int64]string, len(gd.ItemNameToID))
	for name, id := range gd.ItemNameToID {
		itemIDToName[id] = name
	}
	locationIDToName := make(map[int64]string, len(gd.LocationNameToID))
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
func (s ApxRoom) prefetchDataPackages(ctx context.Context) error {
	var missing []string
	// Only get ones we haven't already gotten locally
	for game := range s.roomInfo.DatapackageChecksums {
		if _, ok := s.datapackages.packages[game]; !ok {
			missing = append(missing, game)
		}
	}

	if len(missing) > 0 {
		gameData, err := s.fetchDataPackagesFromAPServer(ctx, missing)
		if err != nil {
			return fmt.Errorf("prefetching datapackage for %d games: %w", len(missing), err)
		}
		// This will add to global store if missing, then we get the ref
		for game, gd := range gameData {
			if err := s.datapackages.AddDataPackage(game, gd); err != nil {
				return fmt.Errorf("adding datapackage for %q: %w", game, err)
			}
		}
	}

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

	// TODO: Verify this works before allowing to go live
	// If the client immediately requests an identical message, bad client!
	// Probably an application level timeout
	// slices.Sort(requestedGames)
	// gamesKey := strings.Join(requestedGames, ",")
	// if connState.prevDatapackageGamesReq == gamesKey {
	// 	// We'll log it and also metric it here if already authed, otherwise mark
	// 	if connState.authenticated {
	// 		log.Printf("retry storm client: %s, %s, %s", s.lobbyRoomId, *connState.slotName, *connState.registeredClient.game)
	// 		s.metrics.retryStormClients.WithLabelValues(s.lobbyRoomId, *connState.slotName, *connState.registeredClient.game).Inc()
	// 	} else {
	// 		connState.isRetryStormClient = true
	// 	}
	// 	return nil
	// }
	// connState.prevDatapackageGamesReq = gamesKey

	games := make([]string, 0)
	for _, game := range requestedGames {
		if _, ok := s.roomInfo.DatapackageChecksums[game]; ok {
			games = append(games, game)
		}
	}
	return s.sendDataPackages(ctx, connState.clientConn, games)

	// We can uncomment this if we want to delay datapackages again later. Need to do before the branches above, extra changes still.

	// if !connState.authenticated {
	// 	// Don't send datapackages until after authed
	// 	connState.pendingDatapackGames = append(connState.pendingDatapackGames, games...)
	// 	return nil
	// }
}

// Grab datapackage from AP server and cache locally so we can provide it to clients ourselves
func (s ApxRoom) fetchDataPackagesFromAPServer(ctx context.Context, games []string) (map[string]GameData, error) {
	apConn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://%s:%d", s.config.APHost, s.apPort), nil)
	if err != nil {
		return nil, fmt.Errorf("dialing upstream: %w", err)
	}
	defer apConn.CloseNow()

	var roomInfo []map[string]any
	if err := wsjson.Read(ctx, apConn, &roomInfo); err != nil {
		return nil, fmt.Errorf("reading RoomInfo: %w", err)
	}

	req := GetDataPackageMessage{Cmd: MessageTypeGetDataPackage, Games: games}
	if err := wsjson.Write(ctx, apConn, []any{req}); err != nil {
		return nil, fmt.Errorf("sending GetDataPackage: %w", err)
	}

	apConn.SetReadLimit(wsReadLimit)

	var responses []map[string]any
	if err := wsjson.Read(ctx, apConn, &responses); err != nil {
		return nil, fmt.Errorf("reading DataPackage response from fetch: %w", err)
	}

	for _, resp := range responses {
		if cmd, _ := resp["cmd"].(string); cmd != string(MessageTypeDataPackage) {
			continue
		}
		raw, err := json.Marshal(resp)
		if err != nil {
			return nil, fmt.Errorf("marshalling DataPackage: %w", err)
		}
		var pkg DataPackageMessage
		if err := json.Unmarshal(raw, &pkg); err != nil {
			return nil, fmt.Errorf("unmarshalling DataPackage: %w", err)
		}
		return pkg.Data.Games, nil
	}

	return nil, fmt.Errorf("DataPackage fetch Response did not contain DataPackages?")
}

// Stitch together to avoid doing any json ops on the already encoded datapackage
func (s ApxRoom) sendDataPackages(ctx context.Context, client *websocket.Conn, games []string) error {
	if len(games) == 1 {
		raw, ok := s.datapackages.singleResponses[games[0]]
		if !ok {
			return fmt.Errorf("unknown datapackage for %q", games[0])
		}
		return client.Write(ctx, websocket.MessageText, raw)
	}

	if s.datapackages.fullGameResponseOptimization {
		if len(games) == len(s.datapackages.packages) && s.datapackages.fullGameResponse != nil {
			return client.Write(ctx, websocket.MessageText, s.datapackages.fullGameResponse)
		}
	}

	// Optimization off for full game respons, or we've got a weird batched request
	msg, err := s.datapackages.buildResponse(games)
	if err != nil {
		return err
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
