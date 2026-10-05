package main

import (
	"apx/multidata"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Config struct {
	WsPort                 int    `json:"ws_port"`
	NormalPort             int    `json:"normal_port"`
	ReducedPort            int    `json:"reduced_port"`
	APHost                 string `json:"ap_room_host"`
	APPassword             string `json:"ap_room_password"`
	LobbyEnabled           bool   `json:"lobby_enabled"`
	LobbyRootUrl           string `json:"lobby_root_url"`
	LobbyRoomId            string `json:"lobby_room_id"`
	LobbyApiKey            string `json:"lobby_api_key"`
	ApiListenAddr          string `json:"apx_api_listen"`
	ApiKey                 string `json:"apx_api_key"`
	ApRoomId               string `json:"ap_room_id"`
	ApApiRoot              string `json:"ap_api_root"`
	ApApiKey               string `json:"ap_admin_api_key"`
	TLSCertFile            string `json:"tls_cert_file"`
	TLSKeyFile             string `json:"tls_key_file"`
	PerSlotPasswords       bool   `json:"per_slot_passwords"`
	LokiEndpoint           string `json:"loki_endpoint"`
	DataPackageStoragePath string `json:"datapackage_storage_path"`
	DebugMultidata         string `json:"debug_multidata"`
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

	runProfiler()
	reg, metrics := initMetrics()
	roomStore, err := NewRoomStore("./data.sqlite")
	if err != nil {
		log.Panicf("creating room store: %v", err)
	}
	rm, router, err := startRoomManager(cfg, reg, metrics, tlsCfg, roomStore)
	if err != nil {
		log.Panicf("starting room manager: %v", err)
	}

	err = startWsRouter(cfg, rm)
	if err != nil {
		log.Panicf("starting ws router: %v", err)
	}

	s := &http.Server{
		Addr:         cfg.ApiListenAddr,
		Handler:      router,
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}

	if cfg.LobbyRoomId != "" && cfg.DebugMultidata != "" {
		md, err := multidata.LoadMultiData(cfg.DebugMultidata)
		if err != nil {
			log.Fatal(err)
		}
		_, err = rm.startRoomFromMultiData(cfg.LobbyRoomId, md, &cfg.NormalPort, &cfg.ReducedPort,
			false, false, false, 1, false)
		if err != nil {
			log.Fatalf("starting env defined room: %v", err)
		}
	}

	log.Printf("API server listening on http://%s", cfg.ApiListenAddr)
	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("API server error: %v", err)
		return err
	}

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
	if v := os.Getenv("WS_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid WS_PORT %q: %w", v, err)
		}
		cfg.WsPort = port
	}
	if v := os.Getenv("NORMAL_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid NORMAL_PORT %q: %w", v, err)
		}
		cfg.NormalPort = port
	}
	if v := os.Getenv("REDUCED_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid REDUCED_PORT %q: %w", v, err)
		}
		cfg.ReducedPort = port
	}
	if v := os.Getenv("AP_ROOM_HOST"); v != "" {
		cfg.APHost = v
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
	if v := os.Getenv("AP_ADMIN_API_KEY"); v != "" {
		cfg.ApApiKey = v
	}
	if v := os.Getenv("TLS_CERT_FILE"); v != "" {
		cfg.TLSCertFile = v
	}
	if v := os.Getenv("TLS_KEY_FILE"); v != "" {
		cfg.TLSKeyFile = v
	}
	dpStoragePath := os.Getenv("DATAPACKAGE_STORAGE_PATH")
	if dpStoragePath == "" {
		dpStoragePath = "./data/datapackages/"
	}
	cfg.DataPackageStoragePath = dpStoragePath
	if v := os.Getenv("DEBUG_MULTIDATA"); v != "" {
		cfg.DebugMultidata = v
	}
	cfg.PerSlotPasswords = true
	if v := os.Getenv("PER_SLOT_PASSWORDS"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid PER_SLOT_PASSWORDS %q: %w", v, err)
		}
		cfg.PerSlotPasswords = enabled
	}
	if v := os.Getenv("LOKI_ENDPOINT"); v != "" {
		cfg.LokiEndpoint = v
	}

	return cfg, nil
}
