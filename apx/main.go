package main

import (
	"apx/multidata"
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ncruces/zenity"
)

type Config struct {
	TLSKeyFile             string `json:"tls_key_file"`
	ApRoomId               string `json:"ap_room_id"`
	DatabaseURL            string `json:"database_url"`
	APHost                 string `json:"ap_room_host"`
	APPassword             string `json:"ap_room_password"`
	SavesPath              string `json:"saves_path"`
	LobbyRootUrl           string `json:"lobby_root_url"`
	LobbyRoomId            string `json:"lobby_room_id"`
	LobbyApiKey            string `json:"lobby_api_key"`
	ApiListenAddr          string `json:"apx_api_listen"`
	SlotDataStoragePath    string `json:"slot_data_storage_path"`
	DataPackageStoragePath string `json:"datapackage_storage_path"`
	ApiKey                 string `json:"apx_api_key"`
	TLSCertFile            string `json:"tls_cert_file"`
	LokiEndpoint           string `json:"loki_endpoint"`
	WsPort                 int    `json:"ws_port"`
	NormalPort             int    `json:"normal_port"`
	ReducedPort            int    `json:"reduced_port"`
	PerSlotPasswords       bool   `json:"per_slot_passwords"`
	LobbyEnabled           bool   `json:"lobby_enabled"`
	DisableAdminDashboard  bool   `json:"disable_frontend"`
	StandaloneMode         bool
}

type ApxArgs struct {
	configFile    string
	multidataFile string // positional, optional
	apiAddr       string // optional
	normalPort    int    // optional
	reducedPort   int    // optional
	initConfig    bool   // optional
	webhost       bool   // optional
	profiler      bool   // optional
	noSave        bool   // optional
}

func parseArgs() ApxArgs {
	a := ApxArgs{}
	flag.StringVar(&a.configFile, "config", "config.json", "path to config file")
	flag.StringVar(&a.apiAddr, "api-addr", "", "API listen address (default 127.0.0.1:9001)")
	flag.IntVar(&a.normalPort, "port", 0, "normal WS port (default 38281)")
	flag.IntVar(&a.reducedPort, "reduced-port", 0, "reduced WS port (default normal-port + 1)")
	flag.BoolVar(&a.initConfig, "init", false, "create a default config file and exit")
	flag.BoolVar(&a.webhost, "webhost", false, "run in webhost mode")
	flag.BoolVar(&a.profiler, "profiler", false, "expose pprof on local port 6060")
	flag.BoolVar(&a.noSave, "no-save", false, "disable saving")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: apx [options] [multidata-file]\n\n")
		fmt.Fprintf(os.Stderr, "  multidata-file  optional path to multidata file for running standalone rooms without a db\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() > 0 {
		a.multidataFile = flag.Arg(0)
	}
	return a
}

func main() {
	err := run()
	if err != nil {
		log.Printf("Error: %v", err)
		waitForKeypress()
		os.Exit(1)
	}
}

func waitForKeypress() {
	fmt.Fprintln(os.Stderr, "\nPress Enter to exit...")
	bufio.NewReader(os.Stdin).ReadString('\n')
}

func initConfig(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config file already exists at %s, not overwriting", path)
	}

	// Empty struct — just shows all available keys with zero values.
	// Runtime defaults are applied by getConfig() regardless.
	template := Config{}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating config file: %w", err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(template); err != nil {
		return fmt.Errorf("writing config file: %w", err)
	}

	log.Printf("created config template at %s — edit before starting", path)
	return nil
}

// run starts a http.Server for the passed in address
// with all requests handled by echoServer.
func run() error {
	args := parseArgs()

	if args.initConfig {
		return initConfig(args.configFile)
	}

	cfg, err := getConfig()
	if err != nil {
		return err
	}

	// Arg overrides
	if args.apiAddr != "" {
		cfg.ApiListenAddr = args.apiAddr
	}
	if args.normalPort != 0 {
		cfg.NormalPort = args.normalPort
	}
	if args.reducedPort != 0 {
		cfg.ReducedPort = args.reducedPort
	}

	var tlsCfg *tls.Config
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return fmt.Errorf("loading TLS keypair: %w", err)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	standalone := !args.webhost
	cfg.StandaloneMode = standalone

	if standalone {
		// Disable DB in standalone
		cfg.DatabaseURL = ""

		// Use positional arg or prompt
		if args.multidataFile == "" {
			args.multidataFile, err = promptMultidataFile()
			if err != nil {
				return err
			}
		}

		if cfg.NormalPort == 0 {
			cfg.NormalPort = 38281
		}
		if cfg.ReducedPort == 0 {
			cfg.ReducedPort = cfg.NormalPort + 1
		}
	}

	if args.profiler {
		runProfiler()
	}
	reg, metrics := initMetrics()
	roomStore, err := NewRoomStore(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("creating room store: %w", err)
	}
	rm, router, err := startRoomManager(cfg, reg, metrics, tlsCfg, roomStore)
	if err != nil {
		return fmt.Errorf("starting room manager: %w", err)
	}

	if standalone {
		err = startStandaloneWsRouter(rm, cfg.NormalPort, cfg.ReducedPort)
	} else {
		err = startWsRouter(cfg, rm)
		if err != nil {
			return fmt.Errorf("starting ws router: %w", err)
		}
	}

	s := &http.Server{
		Addr:         cfg.ApiListenAddr,
		Handler:      router,
		ReadTimeout:  time.Second * 300,
		WriteTimeout: time.Second * 300,
	}

	if standalone {
		base := filepath.Base(args.multidataFile)
		roomId := strings.TrimSuffix(base, filepath.Ext(base))

		log.Printf("Opening multidata file: %s", args.multidataFile)

		md, err := multidata.LoadMultiData(args.multidataFile)
		if err != nil {
			return err
		}
		room, err := rm.startRoomFromMultiData(roomId, md, &cfg.NormalPort, &cfg.ReducedPort,
			false, false, false, 1, !args.noSave)
		if err != nil {
			return fmt.Errorf("starting env defined room: %w", err)
		}
		log.Printf("Loaded room with %d players", len(room.apx.state.SlotInfo))
	}

	log.Printf("API server listening on http://%s", cfg.ApiListenAddr)
	serverErr := make(chan error, 1)
	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	if !(cfg.DisableAdminDashboard || standalone) {
		log.Printf("Admin Dashboard available http://%s/admin/rooms", cfg.ApiListenAddr)
	} else {
		if standalone {
			log.Printf("Admin Dashboard unavailable in standalone mode")
		} else {
			log.Printf("Admin Dashboard disabled by config")
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	select {
	case err := <-serverErr:
		return fmt.Errorf("API server error: %w", err)
	case <-ctx.Done():
	}

	log.Printf("APX shutting down...")
	rm.CloseAll()

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
	if v := os.Getenv("DISABLE_FRONTEND"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid DISABLE_FRONTEND %q: %w", v, err)
		}
		cfg.DisableAdminDashboard = enabled
	}
	if v := os.Getenv("SAVES_PATH"); v != "" {
		cfg.SavesPath = v
	}
	if cfg.SavesPath == "" {
		cfg.SavesPath = "./saves"
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
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
	if cfg.ApiListenAddr == "" {
		cfg.ApiListenAddr = "127.0.0.1:9001"
	}
	if v := os.Getenv("APX_API_KEY"); v != "" {
		cfg.ApiKey = v
	}
	if v := os.Getenv("AP_ROOM_ID"); v != "" {
		cfg.ApRoomId = v
	}
	if v := os.Getenv("TLS_CERT_FILE"); v != "" {
		cfg.TLSCertFile = v
	}
	if v := os.Getenv("TLS_KEY_FILE"); v != "" {
		cfg.TLSKeyFile = v
	}
	sdStoragePath := os.Getenv("SLOT_DATA_STORAGE_PATH")
	if sdStoragePath == "" {
		sdStoragePath = "./data/slot_data/"
	}
	cfg.SlotDataStoragePath = sdStoragePath
	dpStoragePath := os.Getenv("DATAPACKAGE_STORAGE_PATH")
	if dpStoragePath == "" {
		dpStoragePath = "./data/datapackages/"
	}
	cfg.DataPackageStoragePath = dpStoragePath
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

func promptMultidataFile() (string, error) {
	path, err := zenity.SelectFile(
		zenity.Title("Select multidata file"),
		zenity.FileFilters{
			{Name: "Multidata files", Patterns: []string{"*.archipelago", "*.zip"}},
		},
	)
	if err != nil {
		return "", fmt.Errorf("cancelled or unavailable, use file path in command instead")
	}
	return path, nil
}
