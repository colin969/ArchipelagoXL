package main

import (
	"apx/multidata"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/require"
)

func newTestRoomManager(t testing.TB) *RoomManager {
	t.Helper()
	store := newTestStore(t) // in-memory SQLite
	rm := &RoomManager{
		config: &Config{
			ApiKey:              "test",
			SlotDataStoragePath: t.TempDir(),
		},
		registry:         newRoomRegistry(),
		metrics:          nil,
		store:            store,
		diskDataPackages: newTestDiskDataPackages(t),
	}
	return rm
}

var testRoomCounter atomic.Int64

func startRoomFromFile(t testing.TB, rm *RoomManager, path string) *HostedRoom {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	md, err := multidata.ParseMultidata(data)
	require.NoError(t, err)

	room, err := rm.startRoomFromMultiData(
		fmt.Sprintf("test-room-%d", testRoomCounter.Add(1)),
		md, nil,
		nil,
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

func startRoomFromFileWithBadDatapackages(t testing.TB, rm *RoomManager, path string, badPackagePercent float64) *HostedRoom {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	md, err := multidata.ParseMultidata(data)
	require.NoError(t, err)

	// Corrupt a percentage of datapackage checksums before the room is created
	// so clients see unknown checksums and can't use their cache
	if badPackagePercent > 0 && badPackagePercent <= 1.0 {
		games := make([]string, 0, len(md.DataPackage))
		for game := range md.DataPackage {
			games = append(games, game)
		}
		rand.Shuffle(len(games), func(i, j int) {
			games[i], games[j] = games[j], games[i]
		})
		numBad := max(1, int(float64(len(games))*badPackagePercent))
		for _, game := range games[:numBad] {
			pkg := md.DataPackage[game]
			pkg.Checksum = fmt.Sprintf("bench-bad-%s-%d", game, rand.Int64())
			md.DataPackage[game] = pkg
		}
	}
	room, err := rm.startRoomFromMultiData(
		fmt.Sprintf("test-room-%d", testRoomCounter.Add(1)),
		md, nil,
		nil,
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

func newTestStore(t testing.TB) *RoomStore {
	t.Helper()
	store, err := NewRoomStore("")
	require.NoError(t, err)
	return store
}

func newTestDiskDataPackages(t testing.TB) *DiskDataPackageStore {
	t.Helper()
	store, err := NewDiskDataPackageStore(t.TempDir())
	require.NoError(t, err)
	return store
}

type ExpectedMessageReader struct {
	t    *testing.T
	msgs chan json.RawMessage
	done chan struct{}
}

func newExpectedMessageReader(ctx context.Context, conn *websocket.Conn, t *testing.T) *ExpectedMessageReader {
	t.Helper()
	emr := &ExpectedMessageReader{
		t:    t,
		msgs: make(chan json.RawMessage, 64),
		done: make(chan struct{}),
	}
	go emr.readLoop(ctx, conn)
	return emr
}

func (emr *ExpectedMessageReader) readLoop(ctx context.Context, conn *websocket.Conn) {
	defer close(emr.msgs)
	for {
		var batch []json.RawMessage
		if err := wsjson.Read(ctx, conn, &batch); err != nil {
			return
		}
		for _, msg := range batch {
			select {
			case emr.msgs <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (emr *ExpectedMessageReader) next(ctx context.Context) (string, json.RawMessage, error) {
	select {
	case msg, ok := <-emr.msgs:
		if !ok {
			return "", nil, fmt.Errorf("message channel closed")
		}
		cmd, err := getPacketCmd(msg)
		return cmd, msg, err
	case <-ctx.Done():
		return "", nil, ctx.Err()
	}
}

func (emr *ExpectedMessageReader) readMessage(t *testing.T) (string, json.RawMessage) {
	t.Helper()
	cmd, msg, err := emr.next(context.Background())
	require.NoError(t, err)
	return cmd, msg
}

func (emr *ExpectedMessageReader) readExpectedMessage(expectedCmd string, t *testing.T) json.RawMessage {
	t.Helper()
	cmd, msg := emr.readMessage(t)
	require.Equal(t, expectedCmd, cmd)
	return msg
}

func (emr *ExpectedMessageReader) readUntil(expectedCmd string, t *testing.T) json.RawMessage {
	t.Helper()
	for {
		cmd, msg := emr.readMessage(t)
		if cmd == expectedCmd {
			return msg
		}
	}
}

// discardFor drains all messages for duration d, then returns.
func (emr *ExpectedMessageReader) discardFor(d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		_, _, err := emr.next(ctx)
		if err != nil {
			return
		}
	}
}

// assertNoCmdWithin fails if the given cmd is received within duration d.
func (emr *ExpectedMessageReader) assertNoCmdWithin(cmd string, d time.Duration, t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		got, msg, err := emr.next(ctx)
		if err != nil {
			return // timeout or closed — no forbidden cmd received
		}
		if got == cmd {
			t.Errorf("expected no %q message within %s, but received one: %s", cmd, d, msg)
			return
		}
	}
}

func startConnDrainer(ctx context.Context, conn *websocket.Conn) {
	go func() {
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}()
}
