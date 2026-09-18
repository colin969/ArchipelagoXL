package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// TestGlobalCacheGetOrAdd verifies basic add and cache hit behaviour
func TestGlobalCacheGetOrAdd(t *testing.T) {
	cache := newGlobalDataPackageCache()

	encoded := json.RawMessage(`{"item_name_to_id":{},"location_name_to_id":{},"checksum":"abc"}`)
	itemIDToName := map[int64]string{1: "Sword"}
	locationIDToName := map[int64]string{100: "Chest"}

	// First call — miss, should create wrapper and datapackage
	w1 := cache.GetOrAdd("abc", "TestGame", encoded, itemIDToName, locationIDToName)
	if w1 == nil {
		t.Fatal("expected wrapper, got nil")
	}
	if w1.refCount != 1 {
		t.Fatalf("expected wrapper refCount 1, got %d", w1.refCount)
	}
	if w1.datapackage.refCount != 1 {
		t.Fatalf("expected datapackage refCount 1, got %d", w1.datapackage.refCount)
	}

	// Second call — hit, should increment wrapper refCount only
	w2 := cache.GetOrAdd("abc", "TestGame", encoded, itemIDToName, locationIDToName)
	if w1 != w2 {
		t.Fatal("expected same wrapper on cache hit")
	}
	if w2.refCount != 2 {
		t.Fatalf("expected wrapper refCount 2, got %d", w2.refCount)
	}
	// datapackage refCount must NOT change on wrapper hit
	if w2.datapackage.refCount != 1 {
		t.Fatalf("expected datapackage refCount still 1 on wrapper hit, got %d", w2.datapackage.refCount)
	}
}

// TestGlobalCacheSharedDataPackage verifies two games with the same checksum share a datapackage
func TestGlobalCacheSharedDataPackage(t *testing.T) {
	cache := newGlobalDataPackageCache()

	encoded := json.RawMessage(`{"item_name_to_id":{},"location_name_to_id":{},"checksum":"shared"}`)

	w1 := cache.GetOrAdd("shared", "GameA", encoded, nil, nil)
	w2 := cache.GetOrAdd("shared", "GameB", encoded, nil, nil)

	if w1 == w2 {
		t.Fatal("expected different wrappers for different games")
	}
	if w1.datapackage != w2.datapackage {
		t.Fatal("expected shared datapackage for same checksum")
	}
	if w1.datapackage.refCount != 2 {
		t.Fatalf("expected datapackage refCount 2, got %d", w1.datapackage.refCount)
	}
}

// TestReleaseEvictsWrapper verifies wrapper and datapackage are evicted when refCount hits zero
func TestReleaseEvictsWrapper(t *testing.T) {
	cache := newGlobalDataPackageCache()
	encoded := json.RawMessage(`{}`)

	cache.GetOrAdd("abc", "TestGame", encoded, nil, nil)
	cache.Release([]string{"abc:TestGame"})

	cache.mu.RLock()
	_, wrapperExists := cache.byGameKey["abc:TestGame"]
	_, datapackageExists := cache.byChecksum["abc"]
	cache.mu.RUnlock()

	if wrapperExists {
		t.Fatal("expected wrapper to be evicted after release")
	}
	if datapackageExists {
		t.Fatal("expected datapackage to be evicted after release")
	}
}

// TestReleaseSharedDataPackageNotEvictedEarly verifies datapackage survives while another wrapper holds it
func TestReleaseSharedDataPackageNotEvictedEarly(t *testing.T) {
	cache := newGlobalDataPackageCache()
	encoded := json.RawMessage(`{}`)

	cache.GetOrAdd("shared", "GameA", encoded, nil, nil)
	cache.GetOrAdd("shared", "GameB", encoded, nil, nil)

	// Release only GameA's wrapper
	cache.Release([]string{"shared:GameA"})

	cache.mu.RLock()
	_, wrapperA := cache.byGameKey["shared:GameA"]
	_, wrapperB := cache.byGameKey["shared:GameB"]
	_, datapackageExists := cache.byChecksum["shared"]
	cache.mu.RUnlock()

	if wrapperA {
		t.Fatal("expected GameA wrapper to be evicted")
	}
	if !wrapperB {
		t.Fatal("expected GameB wrapper to still exist")
	}
	if !datapackageExists {
		t.Fatal("expected datapackage to survive while GameB wrapper still holds it")
	}

	// Now release GameB — datapackage should go too
	cache.Release([]string{"shared:GameB"})

	cache.mu.RLock()
	_, datapackageExists = cache.byChecksum["shared"]
	cache.mu.RUnlock()

	if datapackageExists {
		t.Fatal("expected datapackage to be evicted after all wrappers released")
	}
}

// TestReleaseMultipleRoomsHoldingWrapper verifies wrapper survives until all rooms release
func TestReleaseMultipleRoomsHoldingWrapper(t *testing.T) {
	cache := newGlobalDataPackageCache()
	encoded := json.RawMessage(`{}`)

	// Two rooms take the same wrapper
	cache.GetOrAdd("abc", "TestGame", encoded, nil, nil)
	cache.GetOrAdd("abc", "TestGame", encoded, nil, nil)

	// First room releases
	cache.Release([]string{"abc:TestGame"})

	cache.mu.RLock()
	wrapper, exists := cache.byGameKey["abc:TestGame"]
	cache.mu.RUnlock()

	if !exists {
		t.Fatal("expected wrapper to survive after first release")
	}
	if wrapper.refCount != 1 {
		t.Fatalf("expected wrapper refCount 1, got %d", wrapper.refCount)
	}

	// Second room releases
	cache.Release([]string{"abc:TestGame"})

	cache.mu.RLock()
	_, exists = cache.byGameKey["abc:TestGame"]
	cache.mu.RUnlock()

	if exists {
		t.Fatal("expected wrapper to be evicted after all rooms released")
	}
}

// TestDataPackageStoreAttachWrapper verifies per-room store is populated correctly from wrapper
func TestDataPackageStoreAttachWrapper(t *testing.T) {
	cache := newGlobalDataPackageCache()
	ds := newDataPackageStore(true, cache)

	gd := GameData{
		Checksum:         "abc",
		ItemNameToID:     map[string]int64{"Sword": 1},
		LocationNameToID: map[string]int64{"Chest": 100},
	}

	if err := ds.AddDataPackage("TestGame", gd); err != nil {
		t.Fatalf("AddDataPackage failed: %v", err)
	}

	if _, ok := ds.packages["TestGame"]; !ok {
		t.Fatal("expected packages to contain TestGame")
	}
	if _, ok := ds.encodedGameNameKeys["TestGame"]; !ok {
		t.Fatal("expected encodedGameNameKeys to contain TestGame")
	}
	if _, ok := ds.singleResponses["TestGame"]; !ok {
		t.Fatal("expected singleResponses to contain TestGame")
	}
	if ds.ItemIDToName["TestGame"][1] != "Sword" {
		t.Fatal("expected ItemIDToName to contain Sword")
	}
	if ds.LocationIDToName["TestGame"][100] != "Chest" {
		t.Fatal("expected LocationIDToName to contain Chest")
	}
	if len(ds.gameKeys) != 1 || ds.gameKeys[0] != "abc:TestGame" {
		t.Fatalf("expected gameKeys to contain abc:TestGame, got %v", ds.gameKeys)
	}
}

// TestDataPackageStoreRelease verifies Release() decrements global cache refcounts
func TestDataPackageStoreRelease(t *testing.T) {
	cache := newGlobalDataPackageCache()
	ds := newDataPackageStore(true, cache)

	gd := GameData{Checksum: "abc"}
	ds.AddDataPackage("TestGame", gd)
	ds.Release()

	cache.mu.RLock()
	_, wrapperExists := cache.byGameKey["abc:TestGame"]
	_, datapackageExists := cache.byChecksum["abc"]
	cache.mu.RUnlock()

	if wrapperExists {
		t.Fatal("expected wrapper evicted after store Release()")
	}
	if datapackageExists {
		t.Fatal("expected datapackage evicted after store Release()")
	}
}

// TestDataPackageStoreTwoRoomsSharedRelease verifies two stores sharing a wrapper release correctly
func TestDataPackageStoreTwoRoomsSharedRelease(t *testing.T) {
	cache := newGlobalDataPackageCache()

	gd := GameData{
		Checksum:         "abc",
		ItemNameToID:     map[string]int64{"Sword": 1},
		LocationNameToID: map[string]int64{"Chest": 100},
	}

	ds1 := newDataPackageStore(true, cache)
	ds1.AddDataPackage("TestGame", gd)

	ds2 := newDataPackageStore(true, cache)
	ds2.AddDataPackage("TestGame", gd)

	// Verify shared wrapper
	cache.mu.RLock()
	wrapper := cache.byGameKey["abc:TestGame"]
	cache.mu.RUnlock()
	if wrapper.refCount != 2 {
		t.Fatalf("expected wrapper refCount 2, got %d", wrapper.refCount)
	}

	// Release first store
	ds1.Release()
	cache.mu.RLock()
	wrapper = cache.byGameKey["abc:TestGame"]
	cache.mu.RUnlock()
	if wrapper == nil {
		t.Fatal("expected wrapper to survive after first store released")
	}
	if wrapper.refCount != 1 {
		t.Fatalf("expected wrapper refCount 1, got %d", wrapper.refCount)
	}

	// Release second store — everything should be evicted
	ds2.Release()
	cache.mu.RLock()
	_, wrapperExists := cache.byGameKey["abc:TestGame"]
	_, datapackageExists := cache.byChecksum["abc"]
	cache.mu.RUnlock()

	if wrapperExists {
		t.Fatal("expected wrapper evicted after both stores released")
	}
	if datapackageExists {
		t.Fatal("expected datapackage evicted after both stores released")
	}
}

// BenchmarkBuildResponse benchmarks the multi-game stitching
func BenchmarkBuildResponse(b *testing.B) {
	testDataPackageJSON, err := os.ReadFile("testdata/datapackage.json")
	if err != nil {
		panic(err)
	}

	globalCache := newGlobalDataPackageCache()
	ds := newDataPackageStore(false, globalCache)
	games := make([]string, 20)
	for i := range 20 {
		name := fmt.Sprintf("TestGame%d", i)
		ds.packages[name] = json.RawMessage(testDataPackageJSON)
		encodedKey, _ := json.Marshal(name)
		ds.encodedGameNameKeys[name] = encodedKey
		games[i] = name
	}

	b.ResetTimer()
	for b.Loop() {
		msg, err := ds.buildResponse(games)
		if err != nil {
			b.Fatal(err)
		}
		_ = msg
	}
}
