//go:build integration
// +build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"golang.org/x/time/rate"
)

type RoomBenchConfig struct {
	SmallIdle        int
	SmallActive      int
	MediumIdle       int
	MediumActive     int
	LargeIdle        int
	LargeActive      int
	ExtraLargeIdle   int
	ExtraLargeActive int
	DatapackageMulti float64
	ConnMultiplier   float64 // 1.0 = one conn per player, 2.0 = two per player, etc.
	CompletionFrac   float64 // 0.0-1.0: fraction of locations checked
}

var defaultBenchConfig = RoomBenchConfig{
	SmallIdle:        30,
	SmallActive:      150,
	MediumIdle:       20,
	MediumActive:     60,
	LargeIdle:        4,
	LargeActive:      20,
	ExtraLargeIdle:   0,
	ExtraLargeActive: 2,
	DatapackageMulti: 0.6,
	ConnMultiplier:   2,
	CompletionFrac:   0.5,
}

func BenchmarkRoomMemory(b *testing.B) {
	configs := []struct {
		name string
		cfg  RoomBenchConfig
	}{
		{"default", defaultBenchConfig},
		{"all_small_idle", RoomBenchConfig{SmallIdle: 20}},
		{"all_small_active_full", RoomBenchConfig{SmallActive: 20, ConnMultiplier: 1.0, CompletionFrac: 1.0}},
		{"mixed_heavy", RoomBenchConfig{
			SmallIdle: 2, SmallActive: 18,
			MediumIdle: 1, MediumActive: 5,
			LargeIdle: 1, LargeActive: 9,
			ExtraLargeIdle: 0, ExtraLargeActive: 5,
			ConnMultiplier: 0.5, CompletionFrac: 0.75,
		}},
		{"high_conn_multiplier", RoomBenchConfig{SmallActive: 10, ConnMultiplier: 3.0, CompletionFrac: 0.5}},
	}

	for _, tc := range configs {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkRoomMemory(b, tc.cfg)
		})
	}
}

type roomKind int

const (
	kindSmallIdle roomKind = iota
	kindSmallActive
	kindMediumIdle
	kindMediumActive
	kindLargeIdle
	kindLargeActive
	kindXLIdle
	kindXLActive
)

func (k roomKind) String() string {
	switch k {
	case kindSmallIdle:
		return "small/idle"
	case kindSmallActive:
		return "small/active"
	case kindMediumIdle:
		return "medium/idle"
	case kindMediumActive:
		return "medium/active"
	case kindLargeIdle:
		return "large/idle"
	case kindLargeActive:
		return "large/active"
	case kindXLIdle:
		return "xl/idle"
	case kindXLActive:
		return "xl/active"
	}
	return "unknown"
}

type roomResult struct {
	kind        roomKind
	heapBefore  uint64
	heapAfter   uint64
	connections int
	slots       int
}

func benchmarkRoomMemory(b *testing.B, cfg RoomBenchConfig) {
	b.Helper()
	b.ReportAllocs()

	concernsSelfTags := []string{"TextConcernsSelf"}

	rm := newTestRoomManager(b)

	type roomSpec struct {
		file string
		idle bool
		kind roomKind
	}

	specs := make([]roomSpec, 0)
	addSpecs := func(file string, idleKind, activeKind roomKind, idleCount, activeCount int) {
		for range idleCount {
			specs = append(specs, roomSpec{file: file, idle: true, kind: idleKind})
		}
		for range activeCount {
			specs = append(specs, roomSpec{file: file, idle: false, kind: activeKind})
		}
	}
	addSpecs("./testdata/small.archipelago", kindSmallIdle, kindSmallActive, cfg.SmallIdle, cfg.SmallActive)
	addSpecs("./testdata/medium.archipelago", kindMediumIdle, kindMediumActive, cfg.MediumIdle, cfg.MediumActive)
	addSpecs("./testdata/large.archipelago", kindLargeIdle, kindLargeActive, cfg.LargeIdle, cfg.LargeActive)
	addSpecs("./testdata/extralarge.archipelago", kindXLIdle, kindXLActive, cfg.ExtraLargeIdle, cfg.ExtraLargeActive)

	totalRooms := len(specs)
	if totalRooms == 0 {
		b.Skip("no rooms configured")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Force GC baseline
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	results := make([]roomResult, 0, totalRooms)
	totalConnections := 0
	totalSlotsConnected := 0
	totalDSHeap := int64(0)
	activeRoomCount := 0

	rng := rand.New(rand.NewSource(12345))

	for i, spec := range specs {
		b.Logf("[%d/%d] starting %s room: %s", i+1, totalRooms, func() string {
			if spec.idle {
				return "idle"
			}
			return "active"
		}(), spec.file)
		var heapBefore runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&heapBefore)

		room := startRoomFromFileWithBadDatapackages(b, rm, spec.file, cfg.DatapackageMulti)
		room.apx.ipLimiter = nil
		srv := httptest.NewServer(room.normalHandler)
		b.Cleanup(srv.Close)

		res := roomResult{kind: spec.kind}

		if spec.idle {
			time.Sleep(50 * time.Millisecond)
			runtime.GC()
			var heapAfter runtime.MemStats
			runtime.ReadMemStats(&heapAfter)
			res.heapBefore = heapBefore.HeapInuse
			res.heapAfter = heapAfter.HeapInuse
			results = append(results, res)
			continue
		}

		activeRoomCount++

		// Collect player slots
		slotsToConnect := make([]*SlotInfo, 0, len(room.apx.state.SlotInfo))
		for _, info := range room.apx.state.SlotInfo {
			if info.Type == SlotTypePlayer {
				slotsToConnect = append(slotsToConnect, info)
			}
		}

		wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
		connTarget := 0
		if len(slotsToConnect) > 0 {
			connTarget = int(float64(len(slotsToConnect)) * cfg.ConnMultiplier)
		}

		log.Printf("connecting %d slots", connTarget)
		slotsConnectedThisRoom := make(map[int]struct{})
		dialLimiter := rate.NewLimiter(rate.Limit(400), 25) // 400/s, burst of 25
		for j := range connTarget {
			info := slotsToConnect[j%len(slotsToConnect)]

			if err := dialLimiter.Wait(ctx); err != nil {
				break
			}

			conn, _, err := websocket.Dial(ctx, wsURL, nil)
			if err != nil {
				log.Panicf("%v", err)
				continue
			}
			conn.SetReadLimit(wsReadLimit)
			startConnDrainer(ctx, conn)
			b.Cleanup(func() { conn.CloseNow() })

			itemsHandling := 7
			tags := []string{}
			if spec.kind == kindXLActive {
				tags = concernsSelfTags
			}
			_ = wsjson.Write(ctx, conn, []any{ConnectMessage{
				Game:          info.Game,
				Name:          info.Name,
				UUID:          StringOrBigInt(fmt.Sprintf("bench-%d-%d-%d", i, info.Slot, j)),
				Version:       NetworkVersion{0, 7, 0},
				Tags:          tags,
				ItemsHandling: &itemsHandling,
			}})

			slotsConnectedThisRoom[info.Slot] = struct{}{}
			totalConnections++
		}

		totalSlotsConnected += len(slotsConnectedThisRoom)
		res.connections = connTarget
		res.slots = len(slotsConnectedThisRoom)

		time.Sleep(100 * time.Millisecond)

		// Register checks for completion fraction
		log.Println("doing checks")
		if cfg.CompletionFrac > 0 {
			for ts := range room.apx.state.SlotInfo {
				missing := room.apx.state.Checks.GetMissing(ts)
				checkCount := int(float64(len(missing)) * cfg.CompletionFrac)
				if checkCount == 0 {
					continue
				}
				_ = room.apx.RegisterChecks(context.Background(), ts, missing[:checkCount])
			}
		}

		// Measure datastorage cost separately
		runtime.GC()
		var dsHeapBefore runtime.MemStats
		runtime.ReadMemStats(&dsHeapBefore)
		testPopulateDataStorage(room, rng)
		runtime.GC()
		var dsHeapAfter runtime.MemStats
		runtime.ReadMemStats(&dsHeapAfter)
		totalDSHeap += int64(dsHeapAfter.HeapInuse) - int64(dsHeapBefore.HeapInuse)

		// Small sleep to let connections register before snapshotting
		time.Sleep(20 * time.Millisecond)

		runtime.GC()
		var heapAfter runtime.MemStats
		runtime.ReadMemStats(&heapAfter)
		res.heapBefore = heapBefore.HeapInuse
		res.heapAfter = heapAfter.HeapInuse
		results = append(results, res)
	}

	// Allow everything to settle, especially the XL room flush buffers for 300k+ releases
	time.Sleep(3000 * time.Millisecond)

	runtime.GC()
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	allocDelta := int64(memAfter.TotalAlloc) - int64(memBefore.TotalAlloc)

	// Per-kind averages
	type kindStats struct {
		totalHeap int64
		count     int
	}
	byKind := make(map[roomKind]*kindStats)
	for _, res := range results {
		delta := int64(res.heapAfter) - int64(res.heapBefore)
		if byKind[res.kind] == nil {
			byKind[res.kind] = &kindStats{}
		}
		byKind[res.kind].totalHeap += delta
		byKind[res.kind].count++
	}

	dpCount := rm.registry.datapackages.Count()

	b.ReportMetric(float64(allocDelta), "total_alloc_bytes")
	b.ReportMetric(float64(totalRooms), "rooms")
	b.ReportMetric(float64(totalConnections), "total_connections")
	b.ReportMetric(float64(totalSlotsConnected), "unique_slots_connected")
	b.ReportMetric(float64(dpCount), "cached_datapackages")
	b.ReportMetric(float64(totalDSHeap), "datastorage_heap_bytes")

	b.Logf("=== Memory Summary ===")
	runtime.GC()
	runtime.GC()

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	b.Logf("Heap in use:          %s", formatBytes(int64(stats.HeapInuse)))
	b.Logf("Heap alloc:           %s", formatBytes(int64(stats.HeapAlloc)))
	b.Logf("Rooms:                %d (small=%d/%d medium=%d/%d large=%d/%d xl=%d/%d idle/active)",
		totalRooms,
		cfg.SmallIdle, cfg.SmallActive,
		cfg.MediumIdle, cfg.MediumActive,
		cfg.LargeIdle, cfg.LargeActive,
		cfg.ExtraLargeIdle, cfg.ExtraLargeActive,
	)
	b.Logf("Total connections:    %d", totalConnections)
	b.Logf("Unique slots conn'd:  %d", totalSlotsConnected)
	b.Logf("Cached datapackages:  %d", dpCount)
	b.Logf("Goroutines:           %d", runtime.NumGoroutine())
	runtime.GC()
	b.Logf("Total alloc:          %s", formatBytes(allocDelta))
	b.Logf("--- Per room kind ---")
	for _, k := range []roomKind{
		kindSmallIdle, kindSmallActive,
		kindMediumIdle, kindMediumActive,
		kindLargeIdle, kindLargeActive,
		kindXLIdle, kindXLActive,
	} {
		s, ok := byKind[k]
		if !ok || s.count == 0 {
			continue
		}
		avg := s.totalHeap / int64(s.count)
		b.Logf("  %-14s count=%-3d avg=%s", k.String(), s.count, formatBytes(avg))
	}
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

type dsValueKind int

const (
	dsKindInt64 dsValueKind = iota
	dsKindShortString
	dsKindLongString
)

type dsValueSpec struct {
	kind   dsValueKind
	weight int
}

var dsValueSpecs = []dsValueSpec{
	{dsKindInt64, 90},
	{dsKindShortString, 8},
	{dsKindLongString, 2},
}

func pickDsKind(rng *rand.Rand) dsValueKind {
	total := 0
	for _, s := range dsValueSpecs {
		total += s.weight
	}
	roll := rng.Intn(total)
	for _, s := range dsValueSpecs {
		if roll < s.weight {
			return s.kind
		}
		roll -= s.weight
	}
	return dsValueSpecs[0].kind
}

func testPopulateDataStorage(room *HostedRoom, rng *rand.Rand) {
	const keysPerPlayer = 5
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	randString := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = chars[rng.Intn(len(chars))]
		}
		return string(b)
	}

	state := room.apx.state
	keyIdx := 0
	for ts, info := range state.SlotInfo {
		if info.Type != SlotTypePlayer {
			continue
		}
		owner := TeamSlot{Team: ts.Team, Slot: ts.Slot}
		for range keysPerPlayer {
			key := fmt.Sprintf("_bench_%d_%d_%d", ts.Slot, ts.Team, keyIdx)
			keyIdx++

			var val any
			switch pickDsKind(rng) {
			case dsKindInt64:
				val = rng.Int63()
			case dsKindShortString:
				val = randString(20)
			case dsKindLongString:
				val = randString(200)
			}

			defaultVal, _ := json.Marshal(val)
			state.DataStorage.Set(owner, key, defaultVal, nil, ts.Slot, nil)
		}
	}
}

type TestLocation struct {
	Item    int64
	Player  int32
	Flags   int32
	Checked bool
}

func TestLocationSize(t *testing.T) {
	var l TestLocation
	t.Logf("TestLocation size: %d bytes", unsafe.Sizeof(l))
}
