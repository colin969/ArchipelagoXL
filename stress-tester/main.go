package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akamensky/argparse"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

type Stats struct {
	Connected            atomic.Int64
	TrackersConnected    atomic.Int64
	Completed            atomic.Int64
	TrackersCompleted    atomic.Int64
	ChecksSent           atomic.Int64
	ChecksReceived       atomic.Int64
	MsgsReceived         atomic.Int64
	Errors               atomic.Int64
	TrackersErrors       atomic.Int64
	BadAuth              atomic.Int64
	DataPackagesReceived atomic.Int64
	UnexpectedItemResets atomic.Int64
}

func startStatsPrinter(ctx context.Context, stats *Stats, total int) {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		defer ticker.Stop()
		var lastChecksSent int64
		for {
			select {
			case <-ticker.C:
				currentChecksSent := stats.ChecksSent.Load()
				rate := (currentChecksSent - lastChecksSent) / 5
				lastChecksSent = currentChecksSent
				totalConnected := stats.Connected.Load()
				toatlTrackersConnected := stats.TrackersConnected.Load()
				trackersCompleted := stats.TrackersCompleted.Load()
				completed := stats.Completed.Load()
				errors := stats.Errors.Load()
				trackersErrors := stats.TrackersErrors.Load()
				clientsConnected := totalConnected - (completed + errors)
				trackersConnected := toatlTrackersConnected - (trackersCompleted + trackersErrors)
				log.Printf("[Progress] (clients=%d trackers=%d)  completed=%d/%d  checks_sent=%d  checks_processed=%d  send_rate=%d/s  msgs_recv=%d dpr=%d errors=%d (auth: %d) unexpected_item_resets=%d",
					clientsConnected,
					trackersConnected,
					completed,
					total,
					currentChecksSent,
					stats.ChecksReceived.Load(),
					rate,
					stats.MsgsReceived.Load(),
					stats.DataPackagesReceived.Load(),
					errors+trackersErrors,
					stats.BadAuth.Load(),
					stats.UnexpectedItemResets.Load(),
				)
			case <-ctx.Done():
				return
			}
		}
	}()
}

type Config struct {
	ServerURL           string
	DataFilepath        string
	Concurrency         int
	CheckRate           int
	Passwords           string
	DisableCompression  bool
	ReducedTraffic      bool
	RequestDataPackages bool
}

type PlayerSlot struct {
	PlayerName string
	Game       string
	SlotNumber int
}

type ClientStats struct {
	PlayerName string
	Game       string
	Connected  bool
	Refused    bool
	ChecksSent int64
	Error      string
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := getConfig()
	if err != nil {
		return err
	}

	log.Printf("Starting stress tester with settings:")
	log.Printf("  Server URL:           %s", cfg.ServerURL)
	log.Printf("  Data filepath:        %s", cfg.DataFilepath)
	log.Printf("  Concurrency:          %d", cfg.Concurrency)
	log.Printf("  Check rate:           %d/s", cfg.CheckRate)
	log.Printf("  Passwords file:       %v", cfg.Passwords != "")
	log.Printf("  Disable compression:  %v", cfg.DisableCompression)
	log.Printf("  Reduced traffic:      %v", cfg.ReducedTraffic)
	log.Printf("  Request datapackages: %v", cfg.RequestDataPackages)

	slots, err := loadSlotData(cfg.DataFilepath)
	if err != nil {
		return fmt.Errorf("failed to load slot data: %w", err)
	}

	limiter := rate.NewLimiter(rate.Limit(cfg.CheckRate), 10)

	if cfg.Passwords != "" {
		entries, err := loadPasswords(cfg.Passwords)
		if err != nil {
			return fmt.Errorf("failed to load passwords: %w", err)
		}
		for _, e := range entries {
			for i := range slots {
				if slots[i].PlayerName == e.PlayerName {
					slots[i].Password = &e.Password
					break
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stats Stats
	startStatsPrinter(ctx, &stats, len(slots))

	// Try and ramp up clients slowly over 3 minutes
	connLimiter := rate.NewLimiter(rate.Limit(cfg.Concurrency/30), 1)
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup

	for _, slotEntry := range slots {
		// Wait for a free client spot for concurrency limit
		sem <- struct{}{}
		wg.Go(func() {
			if err := connLimiter.Wait(ctx); err != nil {
				stats.TrackersErrors.Add(1)
				return
			}
			err := runTrackerClient(ctx, cfg, slotEntry, &stats)
			if err != nil {
				stats.TrackersErrors.Add(1)
				if err.Error() != "auth denied" {
					log.Printf("client error for player %s: %v", slotEntry.PlayerName, err)
				}
			} else {
				stats.TrackersCompleted.Add(1)
			}
		})
		wg.Go(func() {
			defer func() { <-sem }()
			if err := connLimiter.Wait(ctx); err != nil {
				stats.Errors.Add(1)
				return
			}
			err := runClient(ctx, cfg, limiter, slotEntry, &stats)
			if err != nil {
				stats.Errors.Add(1)
				if err.Error() != "auth denied" {
					log.Printf("client error for player %s: %v", slotEntry.PlayerName, err)
				}
			} else {
				stats.Completed.Add(1)
			}
		})
	}

	wg.Wait()

	// Final summary
	currentChecksSent := stats.ChecksSent.Load()
	completed := stats.Completed.Load()
	errors := stats.Errors.Load()
	trackersErrors := stats.TrackersErrors.Load()
	log.Printf("  [Results] completed=%d/%d  checks_sent=%d  checks_processed=%d msgs_recv=%d  errors=%d (auth: %d) unexpected_item_resets=%d",
		completed,
		len(slots),
		currentChecksSent,
		stats.ChecksReceived.Load(),
		stats.MsgsReceived.Load(),
		errors+trackersErrors,
		stats.BadAuth.Load(),
		stats.UnexpectedItemResets.Load(),
	)

	return nil
}

type MessageType string

type ConnectMessage struct {
	Cmd            MessageType    `json:"cmd"`
	Password       *string        `json:"password"`
	Game           string         `json:"game"`
	Name           string         `json:"name"`
	UUID           string         `json:"uuid"`
	Version        NetworkVersion `json:"version"`
	ItemsHandling  int            `json:"items_handling"`
	Tags           []string       `json:"tags"`
	SlotData       bool           `json:"slot_data"`
	ReducedTraffic bool           `json:"reduced"`
}

type NetworkVersion struct {
	Class string `json:"class"`
	Major int    `json:"major"`
	Minor int    `json:"minor"`
	Build int    `json:"build"`
}

type NetworkSlot struct {
	Name         string `json:"name"`
	Game         string `json:"game"`
	Type         int    `json:"type"`
	GroupMembers []int  `json:"group_members"`
}

type NetworkPlayer struct {
	Team  int    `json:"team"`
	Slot  int    `json:"slot"`
	Alias string `json:"alias"`
	Name  string `json:"name"`
}

type ConnectedMessage struct {
	Cmd              string              `json:"cmd"`
	Team             int                 `json:"team"`
	Slot             int                 `json:"slot"`
	Players          []NetworkPlayer     `json:"players"`
	MissingLocations []int64             `json:"missing_locations"`
	CheckedLocations []int64             `json:"checked_locations"`
	SlotData         map[string]any      `json:"slot_data,omitempty"`
	SlotInfo         map[int]NetworkSlot `json:"slot_info"`
	HintPoints       int                 `json:"hint_points"`
}

type LocationChecksMessage struct {
	Cmd       MessageType `json:"cmd"`
	Locations []int64     `json:"locations"`
}

type RoomUpdateMessage struct {
	CheckedLocations []int64 `json:"checked_locations"`
}

func runClient(ctx context.Context, cfg *Config, limiter *rate.Limiter, slotEntry SlotEntry, stats *Stats) error {
	compressionMode := websocket.CompressionContextTakeover
	if cfg.DisableCompression {
		compressionMode = websocket.CompressionDisabled
	}

	conn, _, err := websocket.Dial(ctx, cfg.ServerURL, &websocket.DialOptions{
		CompressionMode: compressionMode,
	})
	if err != nil {
		return fmt.Errorf("dialing AP Tracker: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(1 << 24)

	// Read room info
	var roomInfo []map[string]any
	if err := wsjson.Read(ctx, conn, &roomInfo); err != nil {
		return fmt.Errorf("reading first message from AP: %w", err)
	}

	connectMsg := ConnectMessage{
		Cmd:      "Connect",
		Password: slotEntry.Password,
		Game:     slotEntry.Game,
		Name:     slotEntry.PlayerName,
		UUID:     uuid.New().String(),
		Version: NetworkVersion{
			Class: "Version",
			Build: 0,
			Major: 6,
			Minor: 7,
		},
		ItemsHandling:  7,
		Tags:           []string{"AP"},
		SlotData:       false,
		ReducedTraffic: cfg.ReducedTraffic,
	}
	if err := wsjson.Write(ctx, conn, []any{connectMsg}); err != nil {
		return fmt.Errorf("sending Connect from AP: %w", err)
	}

	var response []map[string]any
	if err := wsjson.Read(ctx, conn, &response); err != nil {
		return fmt.Errorf("reading Connected from AP: %w", err)
	}
	if len(response) < 1 {
		return fmt.Errorf("No response from AP for Connected")
	}

	connectedData, err := json.Marshal(response[0])
	if err != nil {
		return fmt.Errorf("marshalling connected: %w", err)
	}

	var msg ConnectedMessage
	if err := json.Unmarshal(connectedData, &msg); err != nil {
		return fmt.Errorf("unmarshalling connected message: %w", err)
	}
	stats.Connected.Add(1)
	if msg.Cmd != "Connected" {
		stats.BadAuth.Add(1)
		return fmt.Errorf("auth denied")
	}

	// Now we're connected, we can start sending checks and receiving updates

	missingLocations := msg.MissingLocations
	checkedLocations := make(map[int64]struct{}, len(msg.CheckedLocations))
	for _, id := range msg.CheckedLocations {
		checkedLocations[id] = struct{}{}
	}
	totalLocations := len(missingLocations) + len(checkedLocations)

	if len(missingLocations) == 0 {
		return nil
	}

	readErr := make(chan error, 1)
	allChecked := make(chan struct{})

	// Wait for responses to know how many the server has checked
	go func() {
		for {
			var msgs []map[string]any
			if err := wsjson.Read(ctx, conn, &msgs); err != nil {
				readErr <- err
				return
			}
			for _, m := range msgs {
				cmd, _ := m["cmd"].(string)
				stats.MsgsReceived.Add(1)
				if cmd != "RoomUpdate" {
					continue
				}
				raw, err := json.Marshal(m)
				if err != nil {
					readErr <- fmt.Errorf("marshalling RoomUpdate: %w", err)
					return
				}
				var update RoomUpdateMessage
				if err := json.Unmarshal(raw, &update); err != nil {
					readErr <- fmt.Errorf("unmarshalling RoomUpdate: %w", err)
					return
				}
				for _, id := range update.CheckedLocations {
					checkedLocations[id] = struct{}{}
				}
				stats.ChecksReceived.Add(int64(len(update.CheckedLocations)))
				if len(checkedLocations) >= totalLocations {
					close(allChecked)
					return
				}
			}
		}
	}()

	// Send messages when possible
	sendErr := make(chan error, 1)
	go func() {
		for _, loc := range missingLocations {
			if err := limiter.Wait(ctx); err != nil {
				return // context cancelled
			}
			if err := wsjson.Write(ctx, conn, []any{LocationChecksMessage{
				Cmd:       "LocationChecks",
				Locations: []int64{loc},
			}}); err != nil {
				sendErr <- fmt.Errorf("sending LocationChecks: %w", err)
				return
			}
			stats.ChecksSent.Add(1)
		}
	}()

	select {
	case <-allChecked:
		return nil
	case err := <-readErr:
		return err
	case err := <-sendErr:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Try and do some 'vague' representation of a tracker / poptracker client
func runTrackerClient(ctx context.Context, cfg *Config, slotEntry SlotEntry, stats *Stats) error {
	compressionMode := websocket.CompressionContextTakeover
	if cfg.DisableCompression {
		compressionMode = websocket.CompressionDisabled
	}

	conn, _, err := websocket.Dial(ctx, cfg.ServerURL, &websocket.DialOptions{
		CompressionMode: compressionMode,
	})
	if err != nil {
		return fmt.Errorf("dialing AP: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(1 << 24)

	// Read room info
	var roomInfo []map[string]any
	if err := wsjson.Read(ctx, conn, &roomInfo); err != nil {
		return fmt.Errorf("reading first message from AP: %w", err)
	}

	if cfg.RequestDataPackages && len(roomInfo) > 0 {
		checksums, _ := roomInfo[0]["datapackage_checksums"].(map[string]any)
		for game := range checksums {
			if err := wsjson.Write(ctx, conn, []any{map[string]any{
				"cmd":   "GetDataPackage",
				"games": []string{game},
			}}); err != nil {
				return fmt.Errorf("sending GetDataPackage for %s: %w", game, err)
			}
			var dp []map[string]any
			if err := wsjson.Read(ctx, conn, &dp); err != nil {
				return fmt.Errorf("reading DataPackage for %s: %w", game, err)
			}
			if len(dp) > 0 {
				if cmd, _ := dp[0]["cmd"].(string); cmd == "DataPackage" {
					stats.DataPackagesReceived.Add(1)
				} else {
					log.Printf("unexpected response to GetDataPackage for %s: cmd=%q", game, cmd)
				}
			} else {
				log.Printf("empty response to GetDataPackage for %s", game)
			}
		}
	}

	connectMsg := ConnectMessage{
		Cmd:      "Connect",
		Password: slotEntry.Password,
		Game:     slotEntry.Game,
		Name:     slotEntry.PlayerName,
		UUID:     uuid.New().String(),
		Version: NetworkVersion{
			Class: "Version",
			Build: 0,
			Major: 6,
			Minor: 7,
		},
		ItemsHandling:  7,
		Tags:           []string{"AP", "NoText"},
		SlotData:       false,
		ReducedTraffic: cfg.ReducedTraffic,
	}
	if err := wsjson.Write(ctx, conn, []any{connectMsg}); err != nil {
		return fmt.Errorf("sending Connect from AP: %w", err)
	}

	var response []map[string]any
	if err := wsjson.Read(ctx, conn, &response); err != nil {
		return fmt.Errorf("reading Connected from AP: %w", err)
	}
	if len(response) < 1 {
		return fmt.Errorf("No response from AP for Connected")
	}

	connectedData, err := json.Marshal(response[0])
	if err != nil {
		return fmt.Errorf("marshalling connected: %w", err)
	}

	var msg ConnectedMessage
	if err := json.Unmarshal(connectedData, &msg); err != nil {
		return fmt.Errorf("unmarshalling connected message: %w", err)
	}
	stats.TrackersConnected.Add(1)
	if msg.Cmd != "Connected" {
		stats.BadAuth.Add(1)
		return fmt.Errorf("auth denied")
	}

	slotId := msg.Slot

	// Storage simulation goroutine

	storageErr := make(chan error, 1)
	usesStorage := rand.Float64() < 0.2 // 20% odds to be doing it
	if usesStorage {
		go func() {
			// Spread out start times
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(rand.Int63n(int64(5 * time.Second)))):
			}

			// Generate up to 200 keys, up to 5KB total
			const maxKeys = 200
			const maxTotalBytes = 5 * 1024

			type keyType int
			const (
				keyTypeString keyType = iota
				keyTypeInt
			)

			type keyMeta struct {
				key     string
				value   string // used for string keys
				keyType keyType
			}

			keys := make([]keyMeta, 0, maxKeys)
			totalBytes := 0
			for i := range maxKeys {
				valSize := 10 + rand.Intn(41)
				if totalBytes+valSize > maxTotalBytes {
					break
				}

				var km keyMeta
				if rand.Float64() < 0.5 {
					// String key
					val := make([]byte, valSize)
					if _, err := crand.Read(val); err != nil {
						storageErr <- fmt.Errorf("generating random bytes: %w", err)
						return
					}
					km = keyMeta{
						key:     fmt.Sprintf("tracker_0_%d_%d", slotId, i),
						value:   fmt.Sprintf("%x", val),
						keyType: keyTypeString,
					}
				} else {
					// Int key
					km = keyMeta{
						key:     fmt.Sprintf("tracker_0_%d_%d", slotId, i),
						keyType: keyTypeInt,
					}
				}
				keys = append(keys, km)
				totalBytes += valSize
			}

			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					km := keys[rand.Intn(len(keys))]

					var ops []map[string]any
					if km.keyType == keyTypeString {
						// String keys: always replace with another string
						val := make([]byte, 10+rand.Intn(41))
						if _, err := crand.Read(val); err != nil {
							storageErr <- fmt.Errorf("generating random bytes: %w", err)
							return
						}
						ops = []map[string]any{
							{"operation": "replace", "value": fmt.Sprintf("%x", val)},
						}
					} else {
						// Int keys: 2% pow, 50% replace, 24% add, 24% sub
						r := rand.Float64()
						switch {
						case r < 0.02:
							ops = []map[string]any{
								{"operation": "pow", "value": 2},
							}
						case r < 0.52:
							ops = []map[string]any{
								{"operation": "replace", "value": rand.Intn(1000)},
							}
						case r < 0.76:
							ops = []map[string]any{
								{"operation": "add", "value": 1},
							}
						default:
							ops = []map[string]any{
								{"operation": "add", "value": -1},
							}
						}
					}

					msg := map[string]any{
						"cmd":        "Set",
						"key":        km.key,
						"default":    0,
						"want_reply": false,
						"operations": ops,
					}
					if err := wsjson.Write(ctx, conn, []any{msg}); err != nil {
						storageErr <- fmt.Errorf("sending Set: %w", err)
						return
					}
				}
			}
		}()
	}

	// Now we're connected, we can start sending checks and receiving updates

	missingLocations := msg.MissingLocations
	checkedLocations := make(map[int64]struct{}, len(msg.CheckedLocations))
	for _, id := range msg.CheckedLocations {
		checkedLocations[id] = struct{}{}
	}
	totalLocations := len(missingLocations) + len(checkedLocations)

	if len(missingLocations) == 0 {
		return nil
	}

	readErr := make(chan error, 1)
	allChecked := make(chan struct{})

	// Wait for responses to know how many the server has checked
	go func() {
		firstReceivedItems := true
		for {
			var msgs []map[string]any
			if err := wsjson.Read(ctx, conn, &msgs); err != nil {
				readErr <- err
				return
			}
			for _, m := range msgs {
				cmd, _ := m["cmd"].(string)
				stats.MsgsReceived.Add(1)

				switch cmd {
				case "ReceivedItems":
					index, _ := m["index"].(float64) // JSON numbers decode as float64
					if index == 0 && !firstReceivedItems {
						stats.UnexpectedItemResets.Add(1)
						log.Printf("[WARN] tracker %s (slot %d) received ReceivedItems with index=0 after connect", slotEntry.PlayerName, slotId)
					}
					firstReceivedItems = false

				case "RoomUpdate":
					raw, err := json.Marshal(m)
					if err != nil {
						readErr <- fmt.Errorf("marshalling RoomUpdate: %w", err)
						return
					}
					var update RoomUpdateMessage
					if err := json.Unmarshal(raw, &update); err != nil {
						readErr <- fmt.Errorf("unmarshalling RoomUpdate: %w", err)
						return
					}
					for _, id := range update.CheckedLocations {
						checkedLocations[id] = struct{}{}
					}
					if len(checkedLocations) >= totalLocations {
						close(allChecked)
						return
					}
				}
			}
		}
	}()

	// Poptracker
	// Aggressive, 1 slot specific bounce message every 20 seconds
	sendErr := make(chan error, 1)
	go func() {
		// Try and spread them out a bit, each tracker starts really close together and bursts otherwise
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(rand.Int63n(int64(20 * time.Second)))):
		}

		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := wsjson.Write(ctx, conn, []any{map[string]any{
					"cmd":   "Bounce",
					"games": []string{},
					"tags":  []string{},
					"slots": []int{slotId},
					"data":  map[string]any{"stress-test": true},
				}}); err != nil {
					sendErr <- fmt.Errorf("sending Bounce: %w", err)
					return
				}
			}
		}
	}()

	select {
	case <-allChecked:
		return nil
	case err := <-readErr:
		return err
	case err := <-sendErr:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func getConfig() (*Config, error) {
	parser := argparse.NewParser("stress-tester", "Archipelago server stress tester")

	serverURL := parser.StringPositional(&argparse.Options{Help: "WebSocket URL of the AP server"})
	dataFilepath := parser.StringPositional(&argparse.Options{Help: "Path to slot data JSON file"})
	concurrency := parser.Int("", "concurrency", &argparse.Options{Default: 150, Help: "Max simultaneous WebSocket connections"})
	checkRate := parser.Int("", "check-rate", &argparse.Options{Default: 50, Help: "Average checks per second to target across all clients (usually optimistic, will be lower)"})
	passwords := parser.String("", "passwords", &argparse.Options{Default: "", Help: "Path to JSON file with per-slot passwords"})
	disableCompression := parser.Flag("", "disable-compression", &argparse.Options{Help: "Do not use compression when connecting to the AP server"})
	reducedTraffic := parser.Flag("", "reduced-traffic", &argparse.Options{Help: "Ask for reduced traffic for client connections"})
	requestDataPackages := parser.Flag("", "request-datapackages", &argparse.Options{Help: "Trackers request each game's datapackage individually to stress test datapackage byte metrics"})

	if err := parser.Parse(os.Args); err != nil {
		fmt.Fprint(os.Stderr, parser.Usage(err))
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}

	return &Config{
		ServerURL:           *serverURL,
		DataFilepath:        *dataFilepath,
		Concurrency:         *concurrency,
		CheckRate:           *checkRate,
		Passwords:           *passwords,
		DisableCompression:  *disableCompression,
		ReducedTraffic:      *reducedTraffic,
		RequestDataPackages: *requestDataPackages,
	}, nil
}

type SlotData struct {
	Yamls []SlotEntry `json:"yamls"`
}

type SlotEntry struct {
	PlayerName string `json:"player_name"`
	Game       string `json:"game"`
	Password   *string
}

type SlotPassword struct {
	PlayerName string `json:"player_name"`
	Password   string `json:"password"`
}

func loadSlotData(filePath string) ([]SlotEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var slotData SlotData
	err = json.Unmarshal(data, &slotData)
	if err != nil {
		return nil, err
	}

	return slotData.Yamls, nil
}

func loadPasswords(filePath string) ([]SlotPassword, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var slotPasswords []SlotPassword
	err = json.Unmarshal(data, &slotPasswords)
	if err != nil {
		return nil, err
	}

	return slotPasswords, nil
}
