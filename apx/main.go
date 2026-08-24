package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type Config struct {
	ListenAddr           string `json:"apx_ws_listenaddr"`
	ReducedListenAddr    string `json:"apx_ws_reduced_listenaddr"`
	TlsListenAddr        string `json:"apx_ws_tls_listenaddr"`
	TlsReducedListenAddr string `json:"apx_ws_tls_reduced_listenaddr"`
	APHost               string `json:"ap_room_host"`
	APPort               int    `json:"ap_room_port"`
	APPassword           string `json:"ap_room_password"`
	LobbyEnabled         bool   `json:"lobby_enabled"`
	LobbyRootUrl         string `json:"lobby_root_url"`
	LobbyRoomId          string `json:"lobby_room_id"`
	LobbyApiKey          string `json:"lobby_api_key"`
	ApiListenAddr        string `json:"apx_api_listen"`
	ApiKey               string `json:"apx_api_key"`
	ApRoomId             string `json:"ap_room_id"`
	ApApiRoot            string `json:"ap_api_root"`
	TLSCertFile          string `json:"tls_cert_file"`
	TLSKeyFile           string `json:"tls_key_file"`
	Passwordless         bool   `json:"passwordless"`
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

// run starts a http.Server for the passed in address
// with all requests handled by echoServer.
func run() error {
	cfg, err := getConfig()
	if err != nil {
		return err
	}

	var tlsCfg *tls.Config
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("loading TLS keypair: %w", err)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	reg, metrics := initMetrics()
	rm, router, err := startRoomManager(cfg, reg, metrics, tlsCfg)
	if err != nil {
		log.Panicf("starting room manager: %v", err)
	}

	s := &http.Server{
		Addr:         cfg.ApiListenAddr,
		Handler:      router,
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}

	go func() {
		err := rm.startNewHostedRoom(cfg.ApRoomId, cfg.LobbyRoomId, &cfg.ListenAddr, &cfg.ReducedListenAddr)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}()

	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("API server error: %v", err)
		return err
	}
	log.Printf("API server listening on http://%s", cfg.ApiListenAddr)

	return nil
}

func loadConfigFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return &cfg, nil
}

func getConfig() (*Config, error) {
	cfg := &Config{}

	// Try config file first
	configPath := os.Getenv("CONFIG_FILE")
	if configPath == "" {
		configPath = "config.json"
	}
	if fileCfg, err := loadConfigFile(configPath); err == nil {
		cfg = fileCfg
		log.Printf("loaded config from %s", configPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// Env vars override config file
	if v := os.Getenv("APX_WS_LISTENADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := os.Getenv("APX_WS_REDUCED_LISTENADDR"); v != "" {
		cfg.ReducedListenAddr = v
	}
	if v := os.Getenv("APX_WS_TLS_LISTENADDR"); v != "" {
		cfg.TlsListenAddr = v
	}
	if v := os.Getenv("APX_WS_TLS_REDUCED_LISTENADDR"); v != "" {
		cfg.TlsReducedListenAddr = v
	}
	if v := os.Getenv("AP_ROOM_HOST"); v != "" {
		cfg.APHost = v
	}
	if v := os.Getenv("AP_ROOM_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid AP_ROOM_PORT %q: %w", v, err)
		}
		cfg.APPort = port
	}
	if v := os.Getenv("AP_ROOM_PASSWORD"); v != "" {
		cfg.APPassword = v
	}
	if v := os.Getenv("LOBBY_ENABLED"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid LOBBY_ENABLED %q: %w", v, err)
		}
		cfg.LobbyEnabled = enabled
	}
	if v := os.Getenv("LOBBY_ROOT_URL"); v != "" {
		cfg.LobbyRootUrl = v
	}
	if v := os.Getenv("LOBBY_ROOM_ID"); v != "" {
		cfg.LobbyRoomId = v
	}
	if v := os.Getenv("LOBBY_API_KEY"); v != "" {
		cfg.LobbyApiKey = v
	}
	if v := os.Getenv("APX_API_LISTENADDR"); v != "" {
		cfg.ApiListenAddr = v
	}
	if v := os.Getenv("APX_API_KEY"); v != "" {
		cfg.ApiKey = v
	}
	if v := os.Getenv("AP_ROOM_ID"); v != "" {
		cfg.ApRoomId = v
	}
	if v := os.Getenv("AP_API_ROOT"); v != "" {
		cfg.ApApiRoot = v
	}
	if v := os.Getenv("TLS_CERT_FILE"); v != "" {
		cfg.TLSCertFile = v
	}
	if v := os.Getenv("TLS_KEY_FILE"); v != "" {
		cfg.TLSKeyFile = v
	}
	if v := os.Getenv("PASSWORDLESS"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid PASSWORDLESS %q: %w", v, err)
		}
		cfg.Passwordless = enabled
	}

	if cfg.APPort < 1 || cfg.APPort > 65535 {
		return nil, fmt.Errorf("port %d out of range (1-65535)", cfg.APPort)
	}

	return cfg, nil
}

// Get the room info from the AP multiserver so we can cache it
func connectAndGetRoomInfo(apHost string, apPort int) (*RoomInfoMessage, error) {
	url := fmt.Sprintf("ws://%s:%d", apHost, apPort)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Archipelago server at %s: %w", url, err)
	}
	defer c.CloseNow()

	var raw []map[string]any
	if err := wsjson.Read(ctx, c, &raw); err != nil {
		return nil, fmt.Errorf("failed to read initial message: %w", err)
	}

	for _, msg := range raw {
		cmd, _ := msg["cmd"].(string)
		if cmd != "RoomInfo" {
			continue
		}

		data, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal RoomInfo: %w", err)
		}

		var roomInfo RoomInfoMessage
		if err := json.Unmarshal(data, &roomInfo); err != nil {
			return nil, fmt.Errorf("failed to parse RoomInfo: %w", err)
		}

		roomInfo.Password = true

		log.Printf("connected to AP server: seed=%q", roomInfo.SeedName)
		return &roomInfo, nil
	}

	return nil, errors.New("no RoomInfo packet received in initial message")
}

type RoomPlayers struct {
	slots    map[int]NetworkSlotArray
	auth     map[string][2]int
	nameToID map[string]int
}

func fetchRoomPlayers(apApiRoot string, apRoomId string) (*RoomPlayers, error) {
	url := fmt.Sprintf("%s/api/room/%s/players", apApiRoot, apRoomId)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetching room players: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from /api/room/%s/players", resp.StatusCode, apRoomId)
	}

	var roomPlayers RoomPlayers
	if err := json.NewDecoder(resp.Body).Decode(&roomPlayers); err != nil {
		return nil, fmt.Errorf("decoding room players: %w", err)
	}

	roomPlayers.nameToID = make(map[string]int, len(roomPlayers.slots))
	for slotId, slotInfo := range roomPlayers.slots {
		roomPlayers.nameToID[slotInfo.Name] = slotId
	}

	return &roomPlayers, nil
}

func (rp *RoomPlayers) UnmarshalJSON(data []byte) error {
	var raw struct {
		Slots map[string]NetworkSlotArray `json:"slots"`
		Auth  map[string][2]int           `json:"auth"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	rp.slots = make(map[int]NetworkSlotArray, len(raw.Slots))
	for k, v := range raw.Slots {
		id, err := strconv.Atoi(k)
		if err != nil {
			return fmt.Errorf("invalid slot id %q: %w", k, err)
		}
		rp.slots[id] = v
	}

	rp.auth = raw.Auth
	return nil
}
