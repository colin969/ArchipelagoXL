package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// Utils

func makeTestDiskStore(t *testing.T) *DiskDataPackageStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewDiskDataPackageStore(dir)
	if err != nil {
		t.Fatalf("NewDiskDataPackageStore: %v", err)
	}
	return store
}

func writeToDisk(t *testing.T, store *DiskDataPackageStore, checksum string, gd GameData) {
	t.Helper()
	data, err := json.Marshal(gd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.Write(checksum, data); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

// Tests

func TestDatapackage(t *testing.T) {
	t.Run("GlobalCacheGetOrAdd", func(t *testing.T) {
		cache := newGlobalDataPackageCache()
		disk := makeTestDiskStore(t)

		gd := GameData{
			Checksum:         "abc",
			ItemNameToID:     map[string]int64{"Sword": 1},
			LocationNameToID: map[string]int64{"Chest": 100},
		}
		writeToDisk(t, disk, "abc", gd)

		w1, err := cache.GetOrAdd("abc", "TestGame", disk)
		if err != nil {
			t.Fatalf("GetOrAdd: %v", err)
		}
		if w1 == nil {
			t.Fatal("expected wrapper, got nil")
		}
		if w1.refCount != 1 {
			t.Fatalf("expected wrapper refCount 1, got %d", w1.refCount)
		}
		if w1.datapackage.refCount != 1 {
			t.Fatalf("expected datapackage refCount 1, got %d", w1.datapackage.refCount)
		}

		w2, err := cache.GetOrAdd("abc", "TestGame", disk)
		if err != nil {
			t.Fatalf("GetOrAdd: %v", err)
		}
		if w1 != w2 {
			t.Fatal("expected same wrapper on cache hit")
		}
		if w2.refCount != 2 {
			t.Fatalf("expected wrapper refCount 2, got %d", w2.refCount)
		}
		if w2.datapackage.refCount != 1 {
			t.Fatalf("expected datapackage refCount still 1 on wrapper hit, got %d", w2.datapackage.refCount)
		}
	})

	t.Run("GlobalCacheSharedDataPackage", func(t *testing.T) {
		cache := newGlobalDataPackageCache()
		disk := makeTestDiskStore(t)

		gd := GameData{Checksum: "shared"}
		writeToDisk(t, disk, "shared", gd)

		w1, _ := cache.GetOrAdd("shared", "GameA", disk)
		w2, _ := cache.GetOrAdd("shared", "GameB", disk)

		if w1 == w2 {
			t.Fatal("expected different wrappers for different games")
		}
		if w1.datapackage != w2.datapackage {
			t.Fatal("expected shared datapackage for same checksum")
		}
		if w1.datapackage.refCount != 2 {
			t.Fatalf("expected datapackage refCount 2, got %d", w1.datapackage.refCount)
		}
	})

	t.Run("GlobalCacheMissingFromDisk", func(t *testing.T) {
		cache := newGlobalDataPackageCache()
		disk := makeTestDiskStore(t)

		_, err := cache.GetOrAdd("nonexistent", "TestGame", disk)
		if err == nil {
			t.Fatal("expected error for missing checksum on disk")
		}
	})

	t.Run("ReleaseEvictsWrapper", func(t *testing.T) {
		cache := newGlobalDataPackageCache()
		disk := makeTestDiskStore(t)
		writeToDisk(t, disk, "abc", GameData{Checksum: "abc"})

		cache.GetOrAdd("abc", "TestGame", disk)
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
	})

	t.Run("ReleaseSharedDataPackageNotEvictedEarly", func(t *testing.T) {
		cache := newGlobalDataPackageCache()
		disk := makeTestDiskStore(t)
		writeToDisk(t, disk, "shared", GameData{Checksum: "shared"})

		cache.GetOrAdd("shared", "GameA", disk)
		cache.GetOrAdd("shared", "GameB", disk)
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

		cache.Release([]string{"shared:GameB"})

		cache.mu.RLock()
		_, datapackageExists = cache.byChecksum["shared"]
		cache.mu.RUnlock()

		if datapackageExists {
			t.Fatal("expected datapackage to be evicted after all wrappers released")
		}
	})

	t.Run("ReleaseMultipleRoomsHoldingWrapper", func(t *testing.T) {
		cache := newGlobalDataPackageCache()
		disk := makeTestDiskStore(t)
		writeToDisk(t, disk, "abc", GameData{Checksum: "abc"})

		cache.GetOrAdd("abc", "TestGame", disk)
		cache.GetOrAdd("abc", "TestGame", disk)
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

		cache.Release([]string{"abc:TestGame"})

		cache.mu.RLock()
		_, exists = cache.byGameKey["abc:TestGame"]
		cache.mu.RUnlock()

		if exists {
			t.Fatal("expected wrapper to be evicted after all rooms released")
		}
	})

	t.Run("StoreAttachWrapper", func(t *testing.T) {
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
	})

	t.Run("StoreRelease", func(t *testing.T) {
		cache := newGlobalDataPackageCache()
		ds := newDataPackageStore(true, cache)

		ds.AddDataPackage("TestGame", GameData{Checksum: "abc"})
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
	})

	t.Run("StoreTwoRoomsSharedRelease", func(t *testing.T) {
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

		cache.mu.RLock()
		wrapper := cache.byGameKey["abc:TestGame"]
		cache.mu.RUnlock()
		if wrapper.refCount != 2 {
			t.Fatalf("expected wrapper refCount 2, got %d", wrapper.refCount)
		}

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
	})

	t.Run("DiskNotReadTwiceForSameChecksum", func(t *testing.T) {
		cache := newGlobalDataPackageCache()
		disk := makeTestDiskStore(t)
		writeToDisk(t, disk, "abc", GameData{Checksum: "abc"})

		// First call reads from disk
		cache.GetOrAdd("abc", "GameA", disk)

		// Remove from disk — second call should use in-memory cache
		os.Remove(disk.path("abc"))

		_, err := cache.GetOrAdd("abc", "GameB", disk)
		if err != nil {
			t.Fatalf("expected cache hit without disk read, got error: %v", err)
		}
	})
}

// Benchmarks the multi-game stitching
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
