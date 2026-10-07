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

	msgReader := newExpectedMessageReader(ctx, conn)

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

	msgReader := newExpectedMessageReader(ctx, conn)

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

	msgReader := newExpectedMessageReader(ctx, conn)

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

	msgReader := newExpectedMessageReader(ctx, conn)
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
