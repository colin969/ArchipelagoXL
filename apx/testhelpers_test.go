package main

import (
	"apx/multidata"
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/assert"
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

type ExpectedMessageReader struct {
	ctx   context.Context
	conn  *websocket.Conn
	queue []json.RawMessage
}

func newExpectedMessageReader(ctx context.Context, conn *websocket.Conn) *ExpectedMessageReader {
	return &ExpectedMessageReader{
		ctx:  ctx,
		conn: conn,
	}
}

func (emr *ExpectedMessageReader) readExpectedMessage(expectedCmd string, t *testing.T) json.RawMessage {
	t.Helper()

	if len(emr.queue) == 0 {
		var batch []json.RawMessage
		err := wsjson.Read(emr.ctx, emr.conn, &batch)
		require.NoError(t, err)
		emr.queue = batch
	}

	next := emr.queue[0]
	emr.queue = emr.queue[1:]

	cmd, err := getPacketCmd(next)
	require.NoError(t, err)
	assert.Equal(t, expectedCmd, cmd)
	return next
}
