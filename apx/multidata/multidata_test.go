package multidata

import (
	"testing"
)

func TestLoadMultiDataSmall(t *testing.T) {
	md, err := LoadMultiData("../testdata/small.archipelago")
	if err != nil {
		t.Fatalf("failed to parse multidata: %v", err)
	}

	if len(md.SlotData) == 0 {
		t.Error("SlotData is empty")
	}
	if len(md.SlotInfo) == 0 {
		t.Error("SlotInfo is empty")
	}
	if len(md.ConnectNames) == 0 {
		t.Error("ConnectNames is empty")
	}
	if len(md.Locations) == 0 {
		t.Error("Locations is empty")
	}
	if len(md.ServerOptions) == 0 {
		t.Error("ServerOptions is empty")
	}
	if len(md.PrecollectedItems) == 0 {
		t.Error("PrecollectedItems is empty")
	}
	if md.Version == ([3]int{}) {
		t.Error("Version is zero")
	}
	if md.SeedName == "" {
		t.Error("SeedName is empty")
	}
	if len(md.Tags) == 0 {
		t.Error("Tags is empty")
	}
	if md.MinimumVersions.Server == ([3]int{}) {
		t.Error("MinimumVersions.Server is zero")
	}

	for id, slot := range md.SlotInfo {
		if slot.Name == "" {
			t.Errorf("SlotInfo[%d].Name is empty", id)
		}
		if slot.Game == "" {
			t.Errorf("SlotInfo[%d].Game is empty", id)
		}
	}

	for name, pair := range md.ConnectNames {
		if pair == ([2]int{}) {
			t.Errorf("ConnectNames[%q] is zero", name)
		}
	}
}
