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
		Game:          "Hollow Knight",
		Name:          "Player",
		UUID:          StringOrBigInt("1234"),
		Version:       NetworkVersion{0, 7, 0},
		ItemsHandling: new(7),
	}

	err = wsjson.Write(ctx, conn, []any{connectMsg})
	require.NoError(t, err)

	rawMsg := msgReader.readExpectedMessage("Connected", t)

	var connected ConnectedMessage
	err = json.Unmarshal(rawMsg, &connected)
	require.NoError(t, err)

	assert.Equal(t, 10, connected.HintPoints)

	var players []NetworkPlayer
	err = json.Unmarshal(connected.Players, &players)
	require.NoError(t, err)
	assert.Len(t, players, 2)
	assert.Equal(t, 10, connected.HintPoints)

	var slotInfo map[int]NetworkSlot
	err = json.Unmarshal(connected.SlotInfo, &slotInfo)
	require.NoError(t, err)
	assert.Len(t, slotInfo, 2)
	assert.Equal(t, 1, connected.Slot)

	rawMsg = msgReader.readExpectedMessage("ReceivedItems", t)

	// Starting inventory
	var recvItemsMsg ReceivedItemsMessage
	err = json.Unmarshal(rawMsg, &recvItemsMsg)
	require.NoError(t, err)

	assert.Equal(t, 0, recvItemsMsg.Index)
	assert.Len(t, recvItemsMsg.Items, 7)
	assert.Equal(t, int64(-2), recvItemsMsg.Items[0].Location)
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
			Locations: []int64{locID},
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
	var targetLocs []int64
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

func TestClient_CreateHints_OwnLocation(t *testing.T) {
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

	reader := newExpectedMessageReader(ctx, conn, t)
	_ = reader.readExpectedMessage("RoomInfo", t)

	err = wsjson.Write(ctx, conn, []any{ConnectMessage{
		Game:          "Hollow Knight",
		Name:          "Player",
		UUID:          StringOrBigInt("1234"),
		Version:       NetworkVersion{0, 7, 0},
		ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader.readExpectedMessage("Connected", t)
	_ = reader.readExpectedMessage("ReceivedItems", t)

	// Pick any location belonging to slot 1
	missing := room.apx.state.Checks.GetMissing(TeamSlot{0, 1})
	require.NotEmpty(t, missing, "need at least one unchecked location")
	locID := missing[0]

	reader.discardFor(100 * time.Millisecond)

	// TODO: Fix
	err = wsjson.Write(ctx, conn, []any{map[string]any{
		"cmd":       "CreateHints",
		"locations": []int64{locID},
	}})
	require.NoError(t, err)

	// Make sure we got a notification message back
	raw := reader.readUntil("PrintJSON", t)

	var msg PrintJsonMessage
	err = json.Unmarshal(raw, &msg)
	require.NoError(t, err)

	assert.Equal(t, "Hint", msg.Type)
	require.NotNil(t, msg.Item)
	assert.Equal(t, int64(locID), msg.Item.Location)
	require.NotNil(t, msg.Found)
	assert.False(t, *msg.Found)

	// Verify stored in state
	hints := room.apx.state.Hints.GetSlotHints(TeamSlot{0, 1})
	require.NotEmpty(t, hints)
	found := false
	for _, h := range hints {
		if h.Location == locID {
			found = true
			break
		}
	}
	assert.True(t, found, "hint should be stored in state for slot 1")
}

func TestClient_Cmd_HintLocation(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.CloseNow()

	reader := newExpectedMessageReader(ctx, conn, t)
	_ = reader.readExpectedMessage("RoomInfo", t)

	err = wsjson.Write(ctx, conn, []any{ConnectMessage{
		Game:          "Hollow Knight",
		Name:          "Player",
		UUID:          StringOrBigInt("1234"),
		Version:       NetworkVersion{0, 7, 0},
		ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader.readExpectedMessage("Connected", t)
	reader.discardFor(100 * time.Millisecond)

	ts := TeamSlot{0, 1}
	game := room.apx.state.SlotInfo[ts].Game
	missing := room.apx.state.Checks.GetMissing(ts)

	cost := room.apx.state.GetSlotHintCost(ts)
	locationCheckPoints := room.apx.state.ServerOptions.locationCheckPoints
	checksNeeded := (cost + locationCheckPoints - 1) / locationCheckPoints

	toCheck := missing[:checksNeeded]
	unfoundLocName, ok := room.apx.datapackages.LocationIDToName[game][toCheck[0]]
	require.True(t, ok)

	// Hint with 0 points fails
	err = wsjson.Write(ctx, conn, []any{SayMessage{Text: "!hint_location " + unfoundLocName}})
	require.NoError(t, err)

	raw := reader.readUntil("PrintJSON", t)
	assert.Contains(t, string(raw), "A hint costs")

	// Check enough to earn points to hint
	for _, checkLocID := range toCheck {
		err = wsjson.Write(ctx, conn, []any{LocationChecksMessage{Locations: []int64{checkLocID}}})
		require.NoError(t, err)
		_ = reader.readUntil("RoomUpdate", t)
	}
	reader.discardFor(100 * time.Millisecond)

	pointsBeforeUnfound := room.apx.state.GetSlotRemainingPoints(ts)

	// Hint missing loc, spends points
	missing = room.apx.state.Checks.GetMissing(ts)
	unfoundLocName, ok = room.apx.datapackages.LocationIDToName[game][missing[0]]
	unfoundLocId := missing[0]
	require.True(t, ok)
	err = wsjson.Write(ctx, conn, []any{SayMessage{Text: "!hint_location " + unfoundLocName}})
	require.NoError(t, err)

	raw = reader.readUntil("PrintJSON", t)
	var unfoundMsg PrintJsonMessage
	err = json.Unmarshal(raw, &unfoundMsg)
	require.NoError(t, err)

	assert.Equal(t, "Hint", unfoundMsg.Type)
	require.NotNil(t, unfoundMsg.Item)
	assert.Equal(t, int64(unfoundLocId), unfoundMsg.Item.Location)
	require.NotNil(t, unfoundMsg.Found)
	assert.False(t, *unfoundMsg.Found)

	pointsAfterUnfound := room.apx.state.GetSlotRemainingPoints(ts)
	assert.Equal(t, pointsBeforeUnfound-cost, pointsAfterUnfound, "unfound hint should deduct points")

	// Verify stored
	hints := room.apx.state.Hints.GetSlotHints(ts)
	unfoundStored := false
	for _, h := range hints {
		if h.Location == unfoundLocId {
			unfoundStored = true
			break
		}
	}
	assert.True(t, unfoundStored, "unfound hint should be stored")

	// --- Check the found location, then hint it — should not cost points ---
	found := room.apx.state.Checks.GetChecked(ts)
	foundLocName, ok := room.apx.datapackages.LocationIDToName[game][found[0]]
	foundLocID := found[0]

	pointsBeforeFound := room.apx.state.GetSlotRemainingPoints(ts)

	err = wsjson.Write(ctx, conn, []any{SayMessage{Text: "!hint_location " + foundLocName}})
	require.NoError(t, err)

	raw = reader.readUntil("PrintJSON", t)
	var foundMsg PrintJsonMessage
	err = json.Unmarshal(raw, &foundMsg)
	require.NoError(t, err)

	assert.Equal(t, "Hint", foundMsg.Type)
	require.NotNil(t, foundMsg.Item)
	assert.Equal(t, int64(foundLocID), foundMsg.Item.Location)
	require.NotNil(t, foundMsg.Found)
	assert.True(t, *foundMsg.Found)

	pointsAfterFound := room.apx.state.GetSlotRemainingPoints(ts)
	assert.Equal(t, pointsBeforeFound, pointsAfterFound, "found hint should not deduct points")

	// Verify stored
	hints = room.apx.state.Hints.GetSlotHints(ts)
	foundStored := false
	for _, h := range hints {
		if h.Location == foundLocID {
			foundStored = true
			break
		}
	}
	assert.True(t, foundStored, "found hint should be stored")
}

func TestClient_Cmd_Hint(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.CloseNow()

	reader := newExpectedMessageReader(ctx, conn, t)
	_ = reader.readExpectedMessage("RoomInfo", t)

	err = wsjson.Write(ctx, conn, []any{ConnectMessage{
		Game:          "Hollow Knight",
		Name:          "Player",
		UUID:          StringOrBigInt("1234"),
		Version:       NetworkVersion{0, 7, 0},
		ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader.readExpectedMessage("Connected", t)
	reader.discardFor(100 * time.Millisecond)

	ts := TeamSlot{0, 1}
	game := room.apx.state.SlotInfo[ts].Game

	// --- Free hint test: cost=0, hint "Simply_Key", expect exactly 4 hints back ---
	room.apx.state.ServerOptions.SetHintCost(0)

	simpleKeyID, ok := room.apx.datapackages.ItemNameToID[game]["Simple_Key"]
	require.True(t, ok, "Simple_Key must exist in datapackage")

	expectedHints := 0
	for _, locs := range room.apx.state.Locations {
		for _, loc := range locs {
			if loc.Item == simpleKeyID && loc.Player == 1 {
				expectedHints++
			}
		}
	}
	require.Equal(t, 4, expectedHints, "test requires exactly 4 Simple_Key locations")

	err = wsjson.Write(ctx, conn, []any{SayMessage{Text: "!hint Simple_Key"}})
	require.NoError(t, err)

	readNextHint := func() PrintJsonMessage {
		t.Helper()
		for {
			raw := reader.readUntil("PrintJSON", t)
			var msg PrintJsonMessage
			err = json.Unmarshal(raw, &msg)
			require.NoError(t, err)
			if msg.Type == "Hint" {
				return msg
			}
		}
	}

	for range expectedHints {
		msg := readNextHint()
		assert.Equal(t, "Hint", msg.Type)
	}

	// --- Part 2: cost=1, earn points for 2 hints, hint Rancid_Egg twice ---
	room.apx.state.ServerOptions.SetHintCost(1)

	cost := room.apx.state.GetSlotHintCost(ts)
	locationCheckPoints := room.apx.state.ServerOptions.locationCheckPoints
	// Need points for 2 hints
	checksNeeded := (cost*2 + locationCheckPoints - 1) / locationCheckPoints

	missing := room.apx.state.Checks.GetMissing(ts)
	require.GreaterOrEqual(t, len(missing), checksNeeded, "need enough locations to earn points for 2 hints")

	toCheck := missing[:checksNeeded]
	for _, locID := range toCheck {
		err = wsjson.Write(ctx, conn, []any{LocationChecksMessage{Locations: []int64{locID}}})
		require.NoError(t, err)
		_ = reader.readUntil("RoomUpdate", t)
	}
	reader.discardFor(100 * time.Millisecond)

	points := room.apx.state.GetSlotRemainingPoints(ts)
	require.GreaterOrEqual(t, points, cost*2, "should have enough points for 2 hints")

	rancidEggID, ok := room.apx.datapackages.ItemNameToID[game]["Rancid_Egg"]
	require.True(t, ok, "Rancid_Egg must exist in datapackage")

	rancidEggLocations := 0
	for _, locs := range room.apx.state.Locations {
		for _, loc := range locs {
			if loc.Item == rancidEggID {
				rancidEggLocations++
			}
		}
	}
	require.GreaterOrEqual(t, rancidEggLocations, 2, "test requires at least 2 Rancid_Egg locations")

	// First hint - expect exactly 1 new hint, points deducted
	pointsBefore := room.apx.state.GetSlotRemainingPoints(ts)

	err = wsjson.Write(ctx, conn, []any{SayMessage{Text: "!hint Rancid_Egg"}})
	require.NoError(t, err)

	firstHint := readNextHint()
	assert.Equal(t, "Hint", firstHint.Type)

	pointsAfterFirst := room.apx.state.GetSlotRemainingPoints(ts)
	assert.Equal(t, pointsBefore-cost, pointsAfterFirst, "first Rancid_Egg hint should deduct points")

	storedAfterFirst := room.apx.state.Hints.GetSlotHints(ts)
	rancidAfterFirst := 0
	for _, h := range storedAfterFirst {
		if h.Item == rancidEggID {
			rancidAfterFirst++
		}
	}
	assert.Equal(t, 1, rancidAfterFirst, "should have exactly 1 unfound Rancid_Egg hint stored after first call")

	// Second hint — expect 2 hints back (existing re-broadcast), no additional points deducted
	pointsBeforeSecond := room.apx.state.GetSlotRemainingPoints(ts)

	err = wsjson.Write(ctx, conn, []any{SayMessage{Text: "!hint Rancid_Egg"}})
	require.NoError(t, err)

	// First message is the existing hint re-broadcast, second is the new one
	received := 0
	for received < 2 {
		msg := readNextHint()
		assert.Equal(t, "Hint", msg.Type)
		if msg.Found != nil && *msg.Found {
			continue
		}
		received++
	}
	pointsAfterSecond := room.apx.state.GetSlotRemainingPoints(ts)
	assert.Equal(t, pointsBeforeSecond-cost, pointsAfterSecond, "second Rancid_Egg hint should deduct points for the new one only")

	storedAfterSecond := room.apx.state.Hints.GetSlotHints(ts)
	rancidAfterSecond := 0
	for _, h := range storedAfterSecond {
		if h.Item == rancidEggID {
			rancidAfterSecond++
		}
	}
	assert.Equal(t, 2, rancidAfterSecond, "should have exactly 2 unfound Rancid_Egg hints stored after second call")
}

func TestClient_CreateHints_NotifiesSetNotify(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	// Slot 1 — will create the hint
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

	// Slot 2 — subscribes to slot 1's hints via SetNotify
	conn2, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn2.CloseNow()
	reader2 := newExpectedMessageReader(ctx, conn2, t)
	_ = reader2.readExpectedMessage("RoomInfo", t)
	err = wsjson.Write(ctx, conn2, []any{ConnectMessage{
		Game: "Hollow Knight", Name: "Player2", UUID: StringOrBigInt("2"),
		Version: NetworkVersion{0, 7, 0}, ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader2.readExpectedMessage("Connected", t)

	err = wsjson.Write(ctx, conn2, []any{map[string]any{
		"cmd":  "SetNotify",
		"keys": []string{"_read_hints_0_1"},
	}})
	require.NoError(t, err)

	reader1.discardFor(100 * time.Millisecond)
	reader2.discardFor(100 * time.Millisecond)

	missing := room.apx.state.Checks.GetMissing(TeamSlot{0, 1})
	require.NotEmpty(t, missing)
	locID := missing[0]

	err = wsjson.Write(ctx, conn1, []any{map[string]any{
		"cmd":       "CreateHints",
		"locations": []int64{locID},
	}})
	require.NoError(t, err)

	// Slot 2 should receive a SetReply for _read_hints_0_1
	raw := reader2.readUntil("SetReply", t)
	var reply SetReplyMessage
	err = json.Unmarshal(raw, &reply)
	require.NoError(t, err)
	assert.Equal(t, "_read_hints_0_1", reply.Key)
	require.NotNil(t, reply.Value)
}

func TestClient_UpdateHint_NotifiesSetNotify(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	// Slot 1 — receiving player, will update the hint
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

	// Slot 2 — finding player
	conn2, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn2.CloseNow()
	reader2 := newExpectedMessageReader(ctx, conn2, t)
	_ = reader2.readExpectedMessage("RoomInfo", t)
	err = wsjson.Write(ctx, conn2, []any{ConnectMessage{
		Game: "Hollow Knight", Name: "Player2", UUID: StringOrBigInt("2"),
		Version: NetworkVersion{0, 7, 0}, ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader2.readExpectedMessage("Connected", t)

	// Both subscribe to their hint keys
	err = wsjson.Write(ctx, conn1, []any{map[string]any{
		"cmd":  "SetNotify",
		"keys": []string{"_read_hints_0_1"},
	}})
	require.NoError(t, err)
	err = wsjson.Write(ctx, conn2, []any{map[string]any{
		"cmd":  "SetNotify",
		"keys": []string{"_read_hints_0_2"},
	}})
	require.NoError(t, err)

	// Find a location in slot 2 holding an item for slot 1
	missing2 := room.apx.state.Checks.GetMissing(TeamSlot{0, 2})
	var targetLocID int64
	foundLoc := false
	for _, locID := range missing2 {
		loc := room.apx.state.Locations[TeamSlot{0, 2}][locID]
		if int(loc.Player) == 1 {
			targetLocID = locID
			foundLoc = true
			break
		}
	}
	require.True(t, foundLoc, "need a location in slot 2 holding an item for slot 1")

	// Slot 2 creates the hint (finder=slot2, receiver=slot1) — no cost
	err = wsjson.Write(ctx, conn2, []any{map[string]any{
		"cmd":       "CreateHints",
		"locations": []int64{targetLocID},
	}})
	require.NoError(t, err)

	// Drain the PrintJSON broadcast before testing UpdateHint notifies
	_ = reader1.readUntil("PrintJSON", t)
	_ = reader2.readUntil("PrintJSON", t)
	reader1.discardFor(100 * time.Millisecond)
	reader2.discardFor(100 * time.Millisecond)

	// Slot 1 (receiver) updates the hint status
	err = wsjson.Write(ctx, conn1, []any{map[string]any{
		"cmd":      "UpdateHint",
		"player":   2,
		"location": targetLocID,
		"status":   HintStatusNoPriority,
	}})
	require.NoError(t, err)

	// Both slots should receive a SetReply for their hint keys
	raw1 := reader1.readUntil("SetReply", t)
	var reply1 SetReplyMessage
	err = json.Unmarshal(raw1, &reply1)
	require.NoError(t, err)
	assert.Equal(t, "_read_hints_0_1", reply1.Key)

	raw2 := reader2.readUntil("SetReply", t)
	var reply2 SetReplyMessage
	err = json.Unmarshal(raw2, &reply2)
	require.NoError(t, err)
	assert.Equal(t, "_read_hints_0_2", reply2.Key)

	// Verify status updated in state
	updatedHint, ok := room.apx.state.Hints.GetHint(2, targetLocID)
	require.True(t, ok)
	assert.Equal(t, HintStatusNoPriority, updatedHint.Status)
}

func TestClient_StatusUpdate_Goal_Release(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/small.archipelago")
	room.apx.state.ServerOptions.releaseMode = PermissionGoal

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.CloseNow()

	reader := newExpectedMessageReader(ctx, conn, t)
	_ = reader.readExpectedMessage("RoomInfo", t)

	err = wsjson.Write(ctx, conn, []any{ConnectMessage{
		Game:          "Hollow Knight",
		Name:          "Player",
		UUID:          StringOrBigInt("1234"),
		Version:       NetworkVersion{0, 7, 0},
		ItemsHandling: new(7),
	}})
	require.NoError(t, err)
	_ = reader.readExpectedMessage("Connected", t)
	_ = reader.readExpectedMessage("ReceivedItems", t)
	reader.discardFor(100 * time.Millisecond)

	ts := TeamSlot{0, 1}

	// Verify not yet at goal
	assert.NotEqual(t, ClientStatusGoal, room.apx.state.ClientStatuses.Get(ts.Team, ts.Slot))

	// Send StatusUpdate: Goal
	err = wsjson.Write(ctx, conn, []any{map[string]any{
		"cmd":    "StatusUpdate",
		"status": ClientStatusGoal,
	}})
	require.NoError(t, err)

	// Give server time to process
	reader.discardFor(100 * time.Millisecond)

	// Verify goal status stored
	assert.Equal(t, ClientStatusGoal, room.apx.state.ClientStatuses.Get(ts.Team, ts.Slot))

	// !release should now work — collect all unchecked locations belonging to slot 1
	missingBefore := room.apx.state.Checks.GetMissing(ts)
	require.NotEmpty(t, missingBefore, "need unchecked locations to release")

	err = wsjson.Write(ctx, conn, []any{SayMessage{Text: "!release"}})
	require.NoError(t, err)

	// Should receive ReceivedItems with the released items
	raw := reader.readUntil("ReceivedItems", t)
	var recvItems ReceivedItemsMessage
	err = json.Unmarshal(raw, &recvItems)
	require.NoError(t, err)
	assert.NotEmpty(t, recvItems.Items, "release should send items to receiving players")

	// All locations for slot 1 should now be checked
	reader.discardFor(200 * time.Millisecond)
	missingAfter := room.apx.state.Checks.GetMissing(ts)
	assert.Empty(t, missingAfter, "all locations should be checked after release")
}
