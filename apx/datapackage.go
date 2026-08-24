package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

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
	singleRepOptimization bool                        // Allow single game requests caching, triples memory usage
	packages              map[string]json.RawMessage  // Raw encoded datapackages keyed by game name
	singleResponses       map[string]json.RawMessage  // Pre-built response for single-game requests
	fullGameResponse      json.RawMessage             // Response for all games at once to avoid allocations
	encodedGameNameKeys   map[string][]byte           // pre-encoded JSON keys for game names
	ItemIDToName          map[string]map[int64]string // game -> id -> name
	LocationIDToName      map[string]map[int64]string // game -> id -> name
}

func newDataPackageStore(singleRepOptimization bool) *DataPackageStore {
	return &DataPackageStore{
		singleRepOptimization: singleRepOptimization,
		packages:              make(map[string]json.RawMessage),
		singleResponses:       make(map[string]json.RawMessage),
		encodedGameNameKeys:   make(map[string][]byte),
		ItemIDToName:          make(map[string]map[int64]string),
		LocationIDToName:      make(map[string]map[int64]string),
	}
}

func (ds *DataPackageStore) AddDataPackage(game string, gd GameData) error {
	encodedData, err := json.Marshal(gd)
	if err != nil {
		return err
	}
	ds.packages[game] = encodedData

	encodedGameName, _ := json.Marshal(game)
	ds.encodedGameNameKeys[game] = encodedGameName

	if ds.singleRepOptimization {
		msg := []byte(`[{"cmd":"DataPackage","data":{"games":{`)
		msg = append(msg, encodedGameName...)
		msg = append(msg, ':')
		msg = append(msg, encodedData...)
		msg = append(msg, `}}}]`...)
		ds.singleResponses[game] = msg
	}

	// Populate global item id to name
	itemIDToName := make(map[int64]string, len(gd.ItemNameToID))
	for name, id := range gd.ItemNameToID {
		itemIDToName[id] = name
	}
	ds.ItemIDToName[game] = itemIDToName

	// Populate global location id to name
	locationIDToName := make(map[int64]string, len(gd.LocationNameToID))
	for name, id := range gd.LocationNameToID {
		locationIDToName[id] = name
	}
	ds.LocationIDToName[game] = locationIDToName

	return nil
}

// MUST be called before server is live to other users. CANNOT be called safely after.
// TODO: This should really be optimized to not open a conn for each
func (s apxServer) prefetchDataPackages(ctx context.Context) error {
	for game := range s.roomInfo.DatapackageChecksums {
		if _, ok := s.datapackages.packages[game]; ok {
			continue // already cached
		}
		gd, err := s.fetchDataPackageFromAPServer(ctx, game)
		if err != nil {
			return fmt.Errorf("prefetching datapackage for %q: %w", game, err)
		}
		s.datapackages.AddDataPackage(game, gd)
	}

	if s.datapackages.singleRepOptimization {
		games := make([]string, 0, len(s.datapackages.packages))
		for game := range s.datapackages.packages {
			games = append(games, game)
		}
		// We know this is a valid set of games, ignore err
		s.datapackages.fullGameResponse, _ = s.datapackages.buildResponse(games)
	}

	return nil
}

func (s apxServer) handleGetDataPackage(ctx context.Context, connState *connectionState, raw map[string]any) error {
	// We only need 1 field, no point re and unmarshaling just for the struct
	var requestedGames []string
	if gamesList, ok := raw["games"].([]any); ok {
		for _, g := range gamesList {
			if gs, ok := g.(string); ok {
				requestedGames = append(requestedGames, gs)
			}
		}
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
func (s apxServer) fetchDataPackageFromAPServer(ctx context.Context, game string) (GameData, error) {
	apConn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://%s:%d", s.config.APHost, s.config.APPort), nil)
	if err != nil {
		return GameData{}, fmt.Errorf("dialing upstream: %w", err)
	}
	defer apConn.CloseNow()
	apConn.SetReadLimit(1 << 24)

	var roomInfo []map[string]any
	if err := wsjson.Read(ctx, apConn, &roomInfo); err != nil {
		return GameData{}, fmt.Errorf("reading RoomInfo: %w", err)
	}

	req := GetDataPackageMessage{Cmd: MessageTypeGetDataPackage, Games: []string{game}}
	if err := wsjson.Write(ctx, apConn, []any{req}); err != nil {
		return GameData{}, fmt.Errorf("sending GetDataPackage: %w", err)
	}

	apConn.SetReadLimit(wsReadLimit)

	var responses []map[string]any
	if err := wsjson.Read(ctx, apConn, &responses); err != nil {
		return GameData{}, fmt.Errorf("reading DataPackage response for %q (may be too large): %w", game, err)
	}

	for _, resp := range responses {
		if cmd, _ := resp["cmd"].(string); cmd != string(MessageTypeDataPackage) {
			continue
		}
		raw, err := json.Marshal(resp)
		if err != nil {
			return GameData{}, fmt.Errorf("marshalling DataPackage: %w", err)
		}
		var pkg DataPackageMessage
		if err := json.Unmarshal(raw, &pkg); err != nil {
			return GameData{}, fmt.Errorf("unmarshalling DataPackage: %w", err)
		}
		if gd, ok := pkg.Data.Games[game]; ok {
			return gd, nil
		}
	}

	return GameData{}, fmt.Errorf("DataPackage response did not contain game %q", game)
}

// Stitch together to avoid doing any json ops on the already encoded datapackage
func (s apxServer) sendDataPackages(ctx context.Context, client *websocket.Conn, games []string) error {
	if s.datapackages.singleRepOptimization {
		if len(games) == 1 {
			raw, ok := s.datapackages.singleResponses[games[0]]
			if !ok {
				return fmt.Errorf("unknown datapackage for %q", games[0])
			}
			return client.Write(ctx, websocket.MessageText, raw)
		}
		if len(games) == len(s.datapackages.packages) && s.datapackages.fullGameResponse != nil {
			return client.Write(ctx, websocket.MessageText, s.datapackages.fullGameResponse)
		}
	}

	// Optimization off, or we've got a weird batched request
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
