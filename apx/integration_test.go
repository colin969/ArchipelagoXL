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

func TestClient_Connect_ReceivesRoomInfo(t *testing.T) {
	rm := newTestRoomManager(t)
	room := startRoomFromFile(t, rm, "./testdata/0_6_7.archipelago")

	srv := httptest.NewServer(room.normalHandler)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.CloseNow()

	var msgs []json.RawMessage
	err = wsjson.Read(ctx, conn, &msgs)
	require.NoError(t, err)
	require.Len(t, msgs, 1, "expected exactly one opening message")

	cmd, err := getPacketCmd(msgs[0])
	require.NoError(t, err)
	assert.Equal(t, "RoomInfo", cmd)

	var roomInfo RoomInfoMessage
	err = json.Unmarshal(msgs[0], &roomInfo)
	require.NoError(t, err)

	assert.Equal(t, room.apx.state.Seed, roomInfo.SeedName)
	assert.Len(t, roomInfo.Games, 1)
	assert.NotNil(t, roomInfo.Permissions)
	assert.Len(t, roomInfo.DatapackageChecksums, 2)
	assert.Greater(t, roomInfo.Time, float64(0))
}
