package main

import (
	"apx/multidata"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestRoomManager(t *testing.T) *RoomManager {
	t.Helper()
	store := newTestStore(t) // in-memory SQLite
	rm := &RoomManager{
		config:           &Config{ApiKey: "test"},
		registry:         newRoomRegistry(),
		metrics:          nil,
		store:            store,
		diskDataPackages: newTestDiskDataPackages(t),
	}
	return rm
}

func startRoomFromFile(t *testing.T, rm *RoomManager, path string) *HostedRoom {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	md, err := multidata.ParseMultidata(data)
	require.NoError(t, err)

	room, err := rm.startRoomFromMultiData(
		"test-room-"+t.Name(),
		md,
		nil, nil,
		false, // perSlotPasswords
		false, // deathlinkDisabled
		false, // reducedAccess
		1.0,   // deathlinkProbability
		false, // save — skip DB persistence
	)
	require.NoError(t, err)
	t.Cleanup(func() { room.Close() })
	return room
}

func newTestStore(t *testing.T) *RoomStore {
	t.Helper()
	store, err := NewRoomStore(":memory:")
	require.NoError(t, err)
	return store
}

func newTestDiskDataPackages(t *testing.T) *DiskDataPackageStore {
	t.Helper()
	store, err := NewDiskDataPackageStore(t.TempDir())
	require.NoError(t, err)
	return store
}
