//go:build integration
// +build integration

package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// integration_test.go

func TestClient_RoomInfo(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.CloseNow()

	msgReader := newExpectedMessageReader(ctx, conn, t)

	rawMsg := msgReader.readExpectedMessage("RoomInfo", t)

	var roomInfo RoomInfoMessage
	err = json.Unmarshal(rawMsg, &roomInfo)
	require.NoError(t, err)

	assert.Equal(t, room.apx.state.Seed, roomInfo.SeedName)
	assert.Len(t, roomInfo.Games, 1)
	assert.Contains(t, roomInfo.Tags, "AP")
	assert.NotNil(t, roomInfo.Permissions)
	assert.Len(t, roomInfo.DatapackageChecksums, 2)
	assert.Greater(t, roomInfo.Time, float64(0))
}

func TestClient_DataPackages(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	conn.SetReadLimit(wsReadLimit)
	require.NoError(t, err)
	defer conn.CloseNow()

	msgReader := newExpectedMessageReader(ctx, conn, t)

	rawMsg := msgReader.readExpectedMessage("RoomInfo", t)

	var roomInfo RoomInfoMessage
	err = json.Unmarshal(rawMsg, &roomInfo)
	require.NoError(t, err)

	games := make([]string, len(roomInfo.DatapackageChecksums))
	for gameName := range roomInfo.DatapackageChecksums {
		games = append(games, gameName)
	}
	dpReqMsg := GetDataPackageMessage{
		Games: games,
	}
	err = wsjson.Write(ctx, conn, []any{dpReqMsg})
	require.NoError(t, err)

	rawMsg = msgReader.readExpectedMessage("DataPackage", t)

	var dpMsg DataPackageMessage
	err = json.Unmarshal(rawMsg, &dpMsg)
	require.NoError(t, err)

	assert.Len(t, dpMsg.Data.Games, 2)
}

func TestClient_Connected_WithItems(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.CloseNow()

	msgReader := newExpectedMessageReader(ctx, conn, t)

	_ = msgReader.readExpectedMessage("RoomInfo", t)

	connectMsg := ConnectMessage{
		Game:           "Hollow Knight",
		Name:           "Player",
		UUID:           StringOrBigInt("1234"),
		Version:        NetworkVersion{0, 7, 0},
		ItemsHandling:  new(7),
		ReducedTraffic: false,
	}

	err = wsjson.Write(ctx, conn, []any{connectMsg})
	require.NoError(t, err)

	rawMsg := msgReader.readExpectedMessage("Connected", t)

	var connected ConnectedMessage
	err = json.Unmarshal(rawMsg, &connected)
	require.NoError(t, err)

	assert.Equal(t, 10, connected.HintPoints)
	assert.Len(t, connected.Players, 2)
	assert.Len(t, connected.SlotInfo, 2)
	assert.Equal(t, 1, connected.Slot)

	rawMsg = msgReader.readExpectedMessage("ReceivedItems", t)

	// Starting inventory
	var recvItemsMsg ReceivedItemsMessage
	err = json.Unmarshal(rawMsg, &recvItemsMsg)
	require.NoError(t, err)

	assert.Equal(t, 0, recvItemsMsg.Index)
	assert.Len(t, recvItemsMsg.Items, 7)
	assert.Equal(t, int32(-2), recvItemsMsg.Items[0].Location)
	assert.Equal(t, int16(0), recvItemsMsg.Items[0].Player)
}

func TestClient_LocationChecks(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.CloseNow()

	msgReader := newExpectedMessageReader(ctx, conn, t)
	_ = msgReader.readExpectedMessage("RoomInfo", t)

	connectMsg := ConnectMessage{
		Game:          "Hollow Knight",
		Name:          "Player",
		UUID:          StringOrBigInt("1234"),
		Version:       NetworkVersion{0, 7, 0},
		ItemsHandling: new(7),
	}
	err = wsjson.Write(ctx, conn, []any{connectMsg})
	require.NoError(t, err)

	_ = msgReader.readExpectedMessage("Connected", t)
	_ = msgReader.readExpectedMessage("ReceivedItems", t)

	// Check 3 missing locations
	missingChecks := room.apx.state.Checks.GetMissing(TeamSlot{0, 1})
	require.GreaterOrEqual(t, len(missingChecks), 3)
	locations := missingChecks[:3]

	for i, locID := range locations {
		err = wsjson.Write(ctx, conn, []any{LocationChecksMessage{
			Locations: []int{locID},
		}})
		require.NoError(t, err)

		rawMsg := msgReader.readUntil("RoomUpdate", t)
		var roomUpdate RoomUpdateMessage
		err = json.Unmarshal(rawMsg, &roomUpdate)
		require.NoError(t, err)
		assert.Contains(t, roomUpdate.CheckedLocations, locID, "check %d: location should be in RoomUpdate", i)
	}
}

func TestClient_ReceivesItems(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	// ConnectSlot 1 (receives items)
	conn1, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn1.CloseNow()

	reader1 := newExpectedMessageReader(ctx, conn1, t)
	_ = reader1.readExpectedMessage("RoomInfo", t)

	err = wsjson.Write(ctx, conn1, []any{ConnectMessage{
		Game:          "Hollow Knight",
		Name:          "Player",
		UUID:          StringOrBigInt("1234"),
		Version:       NetworkVersion{0, 7, 0},
		ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader1.readExpectedMessage("Connected", t)
	// Clear starting inv
	_ = reader1.readExpectedMessage("ReceivedItems", t)

	// Connect Slot 2 (send checks)
	conn2, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn2.CloseNow()

	reader2 := newExpectedMessageReader(ctx, conn2, t)
	_ = reader2.readExpectedMessage("RoomInfo", t)

	err = wsjson.Write(ctx, conn2, []any{ConnectMessage{
		Game:          "Hollow Knight",
		Name:          "Player2",
		UUID:          StringOrBigInt("5678"),
		Version:       NetworkVersion{0, 7, 0},
		ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader2.readExpectedMessage("Connected", t)

	// We don't care about the rest for slot 2, just throw them away to stop clogging up
	startConnDrainer(ctx, conn2)

	// Find 3 locations in slot 2 that hold items for slot 1
	missing2 := room.apx.state.Checks.GetMissing(TeamSlot{0, 2})
	var targetLocs []int
	for _, locID := range missing2 {
		loc := room.apx.state.Locations[TeamSlot{0, 2}][locID]
		if int(loc.Player) == 1 {
			targetLocs = append(targetLocs, locID)
			if len(targetLocs) == 3 {
				break
			}
		}
	}
	require.Len(t, targetLocs, 3, "need at least 3 locations in slot 2 holding items for slot 1")

	// Slot 2 sends locationchecks for all 3 items at once
	err = wsjson.Write(ctx, conn2, []any{LocationChecksMessage{
		Locations: targetLocs,
	}})
	require.NoError(t, err)

	// Slot 1 should receive all 3 items at once
	rawMsg := reader1.readUntil("ReceivedItems", t)
	var recvItems ReceivedItemsMessage
	err = json.Unmarshal(rawMsg, &recvItems)
	require.NoError(t, err)

	assert.NotEmpty(t, recvItems.Items)
	assert.Len(t, recvItems.Items, 3, "slot 1 should have received at least 3 new items")
}

func TestClient_Say_NoText(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	// Connect client with full feed
	conn1, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn1.CloseNow()
	reader1 := newExpectedMessageReader(ctx, conn1, t)
	_ = reader1.readExpectedMessage("RoomInfo", t)
	err = wsjson.Write(ctx, conn1, []any{ConnectMessage{
		Game: "Hollow Knight", Name: "Player", UUID: StringOrBigInt("1"),
		Version: NetworkVersion{0, 7, 0}, ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader1.readExpectedMessage("Connected", t)

	// Connect client with NoText
	conn2, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn2.CloseNow()
	reader2 := newExpectedMessageReader(ctx, conn2, t)
	_ = reader2.readExpectedMessage("RoomInfo", t)
	err = wsjson.Write(ctx, conn2, []any{ConnectMessage{
		Game: "Hollow Knight", Name: "Player2", UUID: StringOrBigInt("2"),
		Version: NetworkVersion{0, 7, 0}, ItemsHandling: new(7),
		Tags: []string{"NoText"},
	}})
	require.NoError(t, err)
	_ = reader2.readExpectedMessage("Connected", t)

	// Drain join-related messages before sending Say
	reader1.discardFor(100 * time.Millisecond)
	reader2.discardFor(100 * time.Millisecond)

	// Send Say message
	sayMsg := SayMessage{
		Text: "hello world",
	}
	err = wsjson.Write(ctx, conn1, []any{sayMsg})
	require.NoError(t, err)

	// Expect read on full feed client
	raw := reader1.readUntil("PrintJSON", t)
	assert.Contains(t, string(raw), "hello world")

	// Expect no read on NoText client
	reader2.assertNoCmdWithin("PrintJSON", 300*time.Millisecond, t)
}

func TestClient_Say_TextConcernsSelf(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	conn1, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn1.CloseNow()
	reader1 := newExpectedMessageReader(ctx, conn1, t)
	_ = reader1.readExpectedMessage("RoomInfo", t)
	err = wsjson.Write(ctx, conn1, []any{ConnectMessage{
		Game: "Hollow Knight", Name: "Player", UUID: StringOrBigInt("1"),
		Version: NetworkVersion{0, 7, 0}, ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader1.readExpectedMessage("Connected", t)
	_ = reader1.readExpectedMessage("ReceivedItems", t)

	conn2, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn2.CloseNow()
	reader2 := newExpectedMessageReader(ctx, conn2, t)
	_ = reader2.readExpectedMessage("RoomInfo", t)
	err = wsjson.Write(ctx, conn2, []any{ConnectMessage{
		Game: "Hollow Knight", Name: "Player2", UUID: StringOrBigInt("2"),
		Version: NetworkVersion{0, 7, 0}, ItemsHandling: new(7),
		Tags: []string{"TextConcernsSelf"},
	}})
	require.NoError(t, err)
	_ = reader2.readExpectedMessage("Connected", t)

	// Drain join-related messages before sending Say
	reader1.discardFor(250 * time.Millisecond)
	reader2.discardFor(250 * time.Millisecond)

	err = wsjson.Write(ctx, conn1, []any{map[string]any{"cmd": "Say", "text": "hello from slot 1"}})
	require.NoError(t, err)

	// Slot 1 (full client) receives the chat
	raw := reader1.readUntil("PrintJSON", t)
	assert.Contains(t, string(raw), "hello from slot 1")

	// Slot 2 (TextConcernsSelf) should not receive a chat from slot 1
	// since Chat has no Receiving or Item.Player matching slot 2
	reader2.assertNoCmdWithin("PrintJSON", 300*time.Millisecond, t)
}
