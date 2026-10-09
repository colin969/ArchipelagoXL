package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/assert"
)

// Tests

func TestBroadcastBounce(t *testing.T) {
	game := "Celeste"
	game2 := "Hollow Knight"

	cases := []struct {
		name          string
		clientTags    []string
		clientSlot    int
		clientGame    string
		msgTags       []string
		msgSlots      []int
		msgGames      []string
		senderSlot    int
		clientLimited bool
		senderLimited bool
		expectReceive bool
	}{
		// Tag matching
		{"tag match", []string{"DeathLink", "AP"}, 1, game, []string{"DeathLink"}, nil, nil, 0, false, false, true},
		{"tag no match", []string{"AP"}, 1, game, []string{"DeathLink"}, nil, nil, 0, false, false, false},

		// Slot matching
		{"slot match", nil, 3, game, nil, []int{3}, nil, 0, false, false, true},
		{"slot no match", nil, 3, game, nil, []int{1, 2}, nil, 0, false, false, false},

		// Game matching
		{"game match", nil, 1, game, nil, nil, []string{game}, 0, false, false, true},
		{"game no match", nil, 1, game, nil, nil, []string{game2}, 0, false, false, false},

		// Weird packet just for the sake of it
		{"all empty", []string{"AP"}, 3, game, nil, nil, nil, 0, false, false, false},

		// Client limited to own slot
		{"client limited, client is same slot as sender", []string{"DeathLink"}, 1, game, []string{"DeathLink"}, nil, nil, 1, true, false, true},
		{"client limited, client is not sender", []string{"DeathLink"}, 2, game, []string{"DeathLink"}, nil, nil, 1, true, false, false},

		// Sender limited to own slot
		{"client limited, client is same slot as sender", []string{"DeathLink"}, 1, game, []string{"DeathLink"}, nil, nil, 1, false, true, true},
		{"sender limited, client matches", []string{"DeathLink"}, 2, game, []string{"DeathLink"}, nil, nil, 1, false, true, false},
		{"sender limited, client does not match", []string{"DeathLink"}, 2, game, []string{"AP"}, nil, nil, 1, false, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, received := newTestWSClient(t)

			rc := &RegisteredClient{
				Slot:             tc.clientSlot,
				game:             &tc.clientGame,
				clientConn:       conn,
				cancel:           func() {},
				textConcernsSelf: false,
			}

			bounceInfo := newBounceInfoStore()
			if tc.clientLimited {
				bounceInfo.LimitToOwnSlot(tc.clientSlot)
			}
			if tc.senderLimited {
				bounceInfo.LimitToOwnSlot(tc.senderSlot)
			}

			reg := newConnectionRegistry(nil, nil)
			reg.Register(tc.clientSlot, rc, tc.clientGame, tc.clientTags)

			reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
				Tags:  &tc.msgTags,
				Slots: &tc.msgSlots,
				Games: &tc.msgGames,
				Data:  &map[string]any{"test": true},
			}, bounceInfo, tc.senderSlot, nil, nil, nil)

			select {
			case <-received:
				if !tc.expectReceive {
					t.Error("client received message but should not have")
				}
			case <-time.After(200 * time.Millisecond):
				if tc.expectReceive {
					t.Error("client did not receive message but should have")
				}
			}
		})
	}
}

func TestBroadcastBounceNilFields(t *testing.T) {
	game := "Celeste"

	t.Run("all nil, no delivery", func(t *testing.T) {
		conn, received := newTestWSClient(t)
		rc := &RegisteredClient{Slot: 1, game: &game, clientConn: conn, cancel: func() {}}
		reg := newConnectionRegistry(nil, nil)
		reg.Register(1, rc, game, []string{"DeathLink"})

		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  nil,
			Slots: nil,
			Games: nil,
			Data:  &map[string]any{"test": true},
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case <-received:
			t.Error("client received message but should not have with all nil fields")
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("nil slots and games, tags set, tag match", func(t *testing.T) {
		conn, received := newTestWSClient(t)
		rc := &RegisteredClient{Slot: 1, game: &game, clientConn: conn, cancel: func() {}}
		reg := newConnectionRegistry(nil, nil)
		reg.Register(1, rc, game, []string{"DeathLink"})

		tags := []string{"DeathLink"}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  &tags,
			Slots: nil,
			Games: nil,
			Data:  &map[string]any{"test": true},
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case <-received:
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received message via tag match with nil slots and games")
		}
	})

	t.Run("nil tags and games, slots set, slot match", func(t *testing.T) {
		conn, received := newTestWSClient(t)
		rc := &RegisteredClient{Slot: 3, game: &game, clientConn: conn, cancel: func() {}}
		reg := newConnectionRegistry(nil, nil)
		reg.Register(3, rc, game, nil)

		slots := []int{3}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  nil,
			Slots: &slots,
			Games: nil,
			Data:  &map[string]any{"test": true},
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case <-received:
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received message via slot match with nil tags and games")
		}
	})

	t.Run("nil tags and slots, games set, game match", func(t *testing.T) {
		conn, received := newTestWSClient(t)
		rc := &RegisteredClient{Slot: 1, game: &game, clientConn: conn, cancel: func() {}}
		reg := newConnectionRegistry(nil, nil)
		reg.Register(1, rc, game, nil)

		games := []string{game}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  nil,
			Slots: nil,
			Games: &games,
			Data:  &map[string]any{"test": true},
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case <-received:
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received message via game match with nil tags and slots")
		}
	})
}

func newTestWSClient(t *testing.T) (*websocket.Conn, <-chan struct{}) {
	t.Helper()
	received := make(chan struct{}, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var msg []any
		if wsjson.Read(r.Context(), conn, &msg) == nil {
			received <- struct{}{}
		}
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial test ws client: %v", err)
	}

	return conn, received
}

func TestBroadcastBounceNilFieldsPassthrough(t *testing.T) {
	game := "Celeste"

	newReceiverAndReg := func(t *testing.T) (*connectionRegistry, <-chan []byte) {
		t.Helper()
		received := make(chan []byte, 1)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			var raw []byte
			if _, raw, err = conn.Read(r.Context()); err == nil {
				received <- raw
			}
		}))
		t.Cleanup(server.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		t.Cleanup(cancel)

		url := "ws" + strings.TrimPrefix(server.URL, "http")
		conn, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		rc := &RegisteredClient{Slot: 1, game: &game, clientConn: conn, cancel: func() {}}
		reg := newConnectionRegistry(nil, nil)
		reg.Register(1, rc, game, []string{"DeathLink"})

		return reg, received
	}

	assertAbsent := func(t *testing.T, raw []byte, field string) {
		t.Helper()
		s := string(raw)
		if strings.Contains(s, `"`+field+`":`) {
			t.Errorf("field %q should be absent in forwarded packet, got: %s", field, s)
		}
	}

	assertPresent := func(t *testing.T, raw []byte, field string) {
		t.Helper()
		s := string(raw)
		if !strings.Contains(s, `"`+field+`":`) {
			t.Errorf("field %q should be present in forwarded packet, got: %s", field, s)
		}
	}

	t.Run("nil tags absent in forwarded packet", func(t *testing.T) {
		reg, received := newReceiverAndReg(t)
		slots := []int{1}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  nil,
			Slots: &slots,
			Games: nil,
			Data:  &map[string]any{"test": true},
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case raw := <-received:
			assertAbsent(t, raw, "tags")
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received message")
		}
	})

	t.Run("nil slots absent in forwarded packet", func(t *testing.T) {
		reg, received := newReceiverAndReg(t)
		tags := []string{"DeathLink"}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  &tags,
			Slots: nil,
			Games: nil,
			Data:  &map[string]any{"test": true},
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case raw := <-received:
			assertAbsent(t, raw, "slots")
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received message")
		}
	})

	t.Run("nil games absent in forwarded packet", func(t *testing.T) {
		reg, received := newReceiverAndReg(t)
		tags := []string{"DeathLink"}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  &tags,
			Slots: nil,
			Games: nil,
			Data:  &map[string]any{"test": true},
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case raw := <-received:
			assertAbsent(t, raw, "games")
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received message")
		}
	})

	t.Run("nil data absent in forwarded packet", func(t *testing.T) {
		reg, received := newReceiverAndReg(t)
		tags := []string{"DeathLink"}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  &tags,
			Slots: nil,
			Games: nil,
			Data:  nil,
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case raw := <-received:
			assertAbsent(t, raw, "data")
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received message")
		}
	})

	t.Run("non-nil fields present in forwarded packet", func(t *testing.T) {
		reg, received := newReceiverAndReg(t)
		tags := []string{"DeathLink"}
		slots := []int{1}
		games := []string{game}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags:  &tags,
			Slots: &slots,
			Games: &games,
			Data:  &map[string]any{"test": true},
		}, newBounceInfoStore(), 0, nil, nil, nil)

		select {
		case raw := <-received:
			assertPresent(t, raw, "tags")
			assertPresent(t, raw, "slots")
			assertPresent(t, raw, "games")
			assertPresent(t, raw, "data")
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received message")
		}
	})
}

func TestBroadcastBounceSenderTagExclusion(t *testing.T) {
	game := "Celeste"

	newRawReceiver := func(t *testing.T, slotId int, tags []string) (*connectionRegistry, <-chan []byte) {
		t.Helper()
		received := make(chan []byte, 1)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			var raw []byte
			if _, raw, err = conn.Read(r.Context()); err == nil {
				received <- raw
			}
		}))
		t.Cleanup(server.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		t.Cleanup(cancel)

		url := "ws" + strings.TrimPrefix(server.URL, "http")
		conn, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		rc := &RegisteredClient{Slot: slotId, game: &game, clientConn: conn, cancel: func() {}}
		reg := newConnectionRegistry(nil, nil)
		reg.Register(slotId, rc, game, tags)

		return reg, received
	}

	t.Run("sender slot client receives excluded tags intact", func(t *testing.T) {
		// Slot 1 is both the sender and a registered client.
		// "DeathLink" is excluded for slot 1, but slot 1 should still receive
		// the message with "DeathLink" present (pre-exclusion delivery).
		reg, received := newRawReceiver(t, 1, []string{"DeathLink"})

		bounceInfo := newBounceInfoStore()
		bounceInfo.ExcludeByTag(1, "DeathLink")

		msgTags := []string{"DeathLink", "AP"}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags: &msgTags,
			Data: &map[string]any{"test": true},
		}, bounceInfo, 1, nil, nil, nil)

		select {
		case raw := <-received:
			s := string(raw)
			if !strings.Contains(s, `"DeathLink"`) {
				t.Errorf("sender slot client should see DeathLink in Bounced, got: %s", s)
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("sender slot client should have received the Bounced message")
		}
	})

	t.Run("non-sender client has excluded tags stripped", func(t *testing.T) {
		// Two clients: slot 1 (sender, excludes "DeathLink"), slot 2 (non-sender, subscribed to "DeathLink").
		// Slot 2 should receive the message but with "DeathLink" stripped.
		receivedCh := make(chan []byte, 1)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			var raw []byte
			if _, raw, err = conn.Read(r.Context()); err == nil {
				receivedCh <- raw
			}
		}))
		t.Cleanup(server.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		t.Cleanup(cancel)

		url := "ws" + strings.TrimPrefix(server.URL, "http")
		conn2, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatalf("dial slot2 client: %v", err)
		}

		rc2 := &RegisteredClient{Slot: 2, game: &game, clientConn: conn2, cancel: func() {}}
		reg := newConnectionRegistry(nil, nil)
		reg.Register(2, rc2, game, []string{"DeathLink", "AP"})

		bounceInfo := newBounceInfoStore()
		bounceInfo.ExcludeByTag(1, "DeathLink") // slot 1 (sender) excludes DeathLink

		msgTags := []string{"DeathLink", "AP"}
		reg.BroadcastBounceFromSlot(context.Background(), BounceMessage{
			Tags: &msgTags,
			Data: &map[string]any{"test": true},
		}, bounceInfo, 1, nil, nil, nil)

		select {
		case raw := <-receivedCh:
			s := string(raw)
			if strings.Contains(s, `"DeathLink"`) {
				t.Errorf("non-sender client should NOT see DeathLink after tag exclusion, got: %s", s)
			}
			if !strings.Contains(s, `"AP"`) {
				t.Errorf("non-sender client should still see AP tag, got: %s", s)
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("non-sender client should have received the Bounced message via AP tag")
		}
	})
}

func TestHandleDeathLink(t *testing.T) {
	game := "Celeste"
	slotName := "TestSlot"

	makeConnState := func(t *testing.T, reg *connectionRegistry, slotId int) *connectionState {
		t.Helper()
		conn, _ := newTestWSClient(t)
		rc := &RegisteredClient{Slot: slotId, game: &game, clientConn: conn, cancel: func() {}}
		reg.Register(slotId, rc, game, []string{"DeathLink"})
		return &connectionState{
			registeredClient: rc,
			slotName:         &slotName,
		}
	}

	t.Run("nil tags returns error", func(t *testing.T) {
		reg := newConnectionRegistry(nil, nil)
		room := ApxRoom{connections: reg, bounceInfo: newBounceInfoStore()}
		cs := makeConnState(t, reg, 1)

		err := room.handleDeathLink(context.Background(), cs, BounceMessage{
			Tags: nil,
			Data: &map[string]any{"time": 1.0, "source": "TestSlot"},
		})
		if err == nil {
			t.Error("expected error for nil tags, got nil")
		}
	})

	t.Run("nil data returns error", func(t *testing.T) {
		reg := newConnectionRegistry(nil, nil)
		room := ApxRoom{connections: reg, bounceInfo: newBounceInfoStore()}
		cs := makeConnState(t, reg, 1)

		tags := []string{"DeathLink"}
		err := room.handleDeathLink(context.Background(), cs, BounceMessage{
			Tags: &tags,
			Data: nil,
		})
		if err == nil {
			t.Error("expected error for nil data, got nil")
		}
	})

	t.Run("source defaults to slot name when empty", func(t *testing.T) {
		receivedCh := make(chan []byte, 1)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			var raw []byte
			if _, raw, err = conn.Read(r.Context()); err == nil {
				receivedCh <- raw
			}
		}))
		t.Cleanup(server.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		t.Cleanup(cancel)

		url := "ws" + strings.TrimPrefix(server.URL, "http")
		conn, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		reg := newConnectionRegistry(nil, nil)
		rc := &RegisteredClient{Slot: 1, game: &game, clientConn: conn, cancel: func() {}}
		reg.Register(1, rc, game, []string{"DeathLink"})

		cs := &connectionState{registeredClient: rc, slotName: &slotName}
		room := ApxRoom{connections: reg, bounceInfo: newBounceInfoStore()}

		tags := []string{"DeathLink"}
		err = room.handleDeathLink(context.Background(), cs, BounceMessage{
			Tags: &tags,
			Data: &map[string]any{"time": 1234567890.0, "source": ""},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case raw := <-receivedCh:
			s := string(raw)
			if !strings.Contains(s, slotName) {
				t.Errorf("expected source to default to slot name %q, got: %s", slotName, s)
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("client should have received the deathlink broadcast")
		}
	})

	t.Run("missing source field returns error", func(t *testing.T) {
		reg := newConnectionRegistry(nil, nil)
		conn, _ := newTestWSClient(t)
		rc := &RegisteredClient{Slot: 1, game: &game, clientConn: conn, cancel: func() {}}
		reg.Register(1, rc, game, []string{"DeathLink"})

		cs := &connectionState{registeredClient: rc, slotName: &slotName}
		room := ApxRoom{connections: reg, bounceInfo: newBounceInfoStore()}

		tags := []string{"DeathLink"}
		err := room.handleDeathLink(context.Background(), cs, BounceMessage{
			Tags: &tags,
			Data: &map[string]any{"time": 1234567890.0},
			// no "source" key at all
		})
		if err == nil {
			t.Error("expected error when source field is absent, got nil")
		}
	})

	t.Run("valid deathlink broadcasts to all DeathLink subscribers", func(t *testing.T) {
		receivedCh := make(chan []byte, 2)

		makeRawConn := func(t *testing.T) *websocket.Conn {
			t.Helper()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				var raw []byte
				if _, raw, err = conn.Read(r.Context()); err == nil {
					receivedCh <- raw
				}
			}))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.Cleanup(cancel)
			url := "ws" + strings.TrimPrefix(server.URL, "http")
			conn, _, err := websocket.Dial(ctx, url, nil)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			return conn
		}

		reg := newConnectionRegistry(nil, nil)
		conn1 := makeRawConn(t)
		conn2 := makeRawConn(t)

		rc1 := &RegisteredClient{Slot: 1, game: &game, clientConn: conn1, cancel: func() {}}
		rc2 := &RegisteredClient{Slot: 2, game: &game, clientConn: conn2, cancel: func() {}}
		reg.Register(1, rc1, game, []string{"DeathLink"})
		reg.Register(2, rc2, game, []string{"DeathLink"})

		cs := &connectionState{registeredClient: rc1, slotName: &slotName}
		room := ApxRoom{connections: reg, bounceInfo: newBounceInfoStore()}

		tags := []string{"DeathLink"}
		err := room.handleDeathLink(context.Background(), cs, BounceMessage{
			Tags: &tags,
			Data: &map[string]any{"time": 1234567890.0, "source": "TestSlot"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		received := 0
		deadline := time.After(300 * time.Millisecond)
		for received < 2 {
			select {
			case <-receivedCh:
				received++
			case <-deadline:
				t.Errorf("expected 2 clients to receive deathlink, got %d", received)
				return
			}
		}
	})
}

func TestConnectionRegistry(t *testing.T) {
	t.Run("SuccessfulConnect", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		game := "TestGame"
		client := &RegisteredClient{
			Slot: 1,
			game: &game,
		}

		cr.Register(1, client, game, []string{"DeathLink"})

		t.Run("client in clients map", func(t *testing.T) {
			if len(cr.clients[1]) != 1 {
				t.Errorf("expected 1 client for slot 1, got %d", len(cr.clients[1]))
			}
		})

		t.Run("client in clientsByGame", func(t *testing.T) {
			if len(cr.clientsByGame[game]) != 1 {
				t.Errorf("expected 1 client for game %q, got %d", game, len(cr.clientsByGame[game]))
			}
		})

		t.Run("client in clientsByTag", func(t *testing.T) {
			if len(cr.clientsByTag["DeathLink"]) != 1 {
				t.Errorf("expected 1 client for tag DeathLink, got %d", len(cr.clientsByTag["DeathLink"]))
			}
		})
	})

	t.Run("UnregisterAfterConnect", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		game := "TestGame"
		client := &RegisteredClient{
			Slot: 1,
			game: &game,
		}

		cr.Register(1, client, game, []string{"DeathLink"})
		cr.Unregister(client)

		t.Run("client removed from clients map", func(t *testing.T) {
			if len(cr.clients[1]) != 0 {
				t.Errorf("expected 0 clients for slot 1, got %d", len(cr.clients[1]))
			}
		})

		t.Run("client removed from clientsByGame", func(t *testing.T) {
			if len(cr.clientsByGame[game]) != 0 {
				t.Errorf("expected 0 clients for game %q", game)
			}
		})

		t.Run("client removed from clientsByTag", func(t *testing.T) {
			if len(cr.clientsByTag["DeathLink"]) != 0 {
				t.Errorf("expected 0 clients for tag DeathLink")
			}
		})
	})

	t.Run("MultipleConnectsSameSlot", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		game := "TestGame"
		c1 := &RegisteredClient{Slot: 1, game: &game}
		c2 := &RegisteredClient{Slot: 1, game: &game}

		cr.Register(1, c1, game, []string{})
		cr.Register(1, c2, game, []string{})

		t.Run("both clients registered", func(t *testing.T) {
			if len(cr.clients[1]) != 2 {
				t.Errorf("expected 2 clients for slot 1, got %d", len(cr.clients[1]))
			}
		})

		cr.Unregister(c1)

		t.Run("one client remains after unregister", func(t *testing.T) {
			if len(cr.clients[1]) != 1 {
				t.Errorf("expected 1 client remaining, got %d", len(cr.clients[1]))
			}
			if cr.clients[1][0] != c2 {
				t.Error("expected c2 to remain")
			}
		})
	})
}

func TestConnectionRegistryTextIndices(t *testing.T) {
	game := "TestGame"

	newClient := func(slot int) *RegisteredClient {
		return &RegisteredClient{Slot: slot, game: &game}
	}

	t.Run("plain client lands in fullClients", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		cr.Register(1, c, game, []string{})

		assert.Contains(t, cr.fullClients, c)
		assert.NotContains(t, cr.concernsSelfClients, c)
	})

	t.Run("NoText client absent from both indices", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		cr.Register(1, c, game, []string{"NoText"})

		assert.NotContains(t, cr.fullClients, c)
		assert.NotContains(t, cr.concernsSelfClients, c)
		assert.True(t, c.noText)
	})

	t.Run("TextConcernsSelf client lands in concernsSelfClients", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		cr.Register(1, c, game, []string{"TextConcernsSelf"})

		assert.Contains(t, cr.concernsSelfClients, c)
		assert.NotContains(t, cr.fullClients, c)
	})

	t.Run("UpdateTags plain -> NoText removes from fullClients", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		cr.Register(1, c, game, []string{})
		assert.Contains(t, cr.fullClients, c)

		cr.UpdateTags(c, []string{"NoText"})

		assert.NotContains(t, cr.fullClients, c)
		assert.NotContains(t, cr.concernsSelfClients, c)
		assert.True(t, c.noText)
	})

	t.Run("UpdateTags NoText -> plain restores to fullClients", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		cr.Register(1, c, game, []string{"NoText"})
		assert.NotContains(t, cr.fullClients, c)

		cr.UpdateTags(c, []string{})

		assert.Contains(t, cr.fullClients, c)
		assert.False(t, c.noText)
	})

	t.Run("UpdateTags plain -> TextConcernsSelf moves between indices", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		cr.Register(1, c, game, []string{})
		assert.Contains(t, cr.fullClients, c)

		cr.UpdateTags(c, []string{"TextConcernsSelf"})

		assert.NotContains(t, cr.fullClients, c)
		assert.Contains(t, cr.concernsSelfClients, c)
	})

	t.Run("Unregister removes from fullClients", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		cr.Register(1, c, game, []string{})
		cr.Unregister(c)

		assert.NotContains(t, cr.fullClients, c)
	})

	t.Run("Unregister removes from concernsSelfClients", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		cr.Register(1, c, game, []string{"TextConcernsSelf"})
		cr.Unregister(c)

		assert.NotContains(t, cr.concernsSelfClients, c)
	})

	t.Run("Kick removes from fullClients", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		c.cancel = func() {}
		cr.Register(1, c, game, []string{})
		cr.Kick(1)

		assert.NotContains(t, cr.fullClients, c)
	})

	t.Run("multiple clients same slot, indices stay consistent", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c1 := newClient(1)
		c2 := newClient(1)

		cr.Register(1, c1, game, []string{})
		cr.Register(1, c2, game, []string{"TextConcernsSelf"})

		assert.Contains(t, cr.fullClients, c1)
		assert.Contains(t, cr.concernsSelfClients, c2)

		cr.Unregister(c1)

		assert.NotContains(t, cr.fullClients, c1)
		assert.Contains(t, cr.concernsSelfClients, c2)
	})

	t.Run("tagless forcedTextConcernsSelf client lands in concernsSelfClients", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		c.forcedTextConcernsSelf = true
		cr.Register(1, c, game, []string{})

		assert.True(t, c.textConcernsSelf)
		assert.Contains(t, cr.concernsSelfClients, c)
		assert.NotContains(t, cr.fullClients, c)
	})

	t.Run("NoText forcedTextConcernsSelf client absent from both indices", func(t *testing.T) {
		cr := newConnectionRegistry(nil, nil)
		c := newClient(1)
		c.forcedTextConcernsSelf = true
		cr.Register(1, c, game, []string{"NoText"})

		assert.True(t, c.textConcernsSelf)
		assert.True(t, c.noText)
		assert.NotContains(t, cr.fullClients, c)
		assert.NotContains(t, cr.concernsSelfClients, c)
	})
}

func TestConnectNames(t *testing.T) {
	t.Run("set and get", func(t *testing.T) {
		cn := newAltConnectNames()
		cn.SetAltName("Alice", "ali")
		got := cn.GetAltName("ali")
		if got == nil || *got != "Alice" {
			t.Errorf("expected Alice, got %v", got)
		}
	})

	t.Run("get unknown returns nil", func(t *testing.T) {
		cn := newAltConnectNames()
		if cn.GetAltName("unknown") != nil {
			t.Error("expected nil for unknown alt name")
		}
	})

	t.Run("remove", func(t *testing.T) {
		cn := newAltConnectNames()
		cn.SetAltName("Alice", "ali")
		cn.RemoveAltName("ali")
		if cn.GetAltName("ali") != nil {
			t.Error("expected nil after removal")
		}
	})

	// Only subtest really doing the hard work here ngl
	t.Run("get by slot", func(t *testing.T) {
		cn := newAltConnectNames()
		cn.SetAltName("Alice", "ali")
		cn.SetAltName("Alice", "alice2")
		cn.SetAltName("Bob", "bobby")

		alts := cn.GetAltNamesBySlot("Alice")
		if len(alts) != 2 {
			t.Errorf("expected 2 alt names for Alice, got %d", len(alts))
		}
	})
}

func TestIPRateLimiter(t *testing.T) {
	rl := newIPRateLimiter()

	// Burst of 5 should all pass
	for i := range 5 {
		if !rl.Allow("1.2.3.4") {
			t.Fatalf("expected Allow to return true on attempt %d", i+1)
		}
	}

	// 6th should be denied
	if rl.Allow("1.2.3.4") {
		t.Fatal("expected Allow to return false after burst exhausted")
	}

	// Different IP should still have full burst
	if !rl.Allow("5.6.7.8") {
		t.Fatal("expected Allow to return true for different IP")
	}
}

func makeTestLocations() map[TeamSlot]map[int64]Location {
	return map[TeamSlot]map[int64]Location{
		{0, 1}: {
			100: {Item: 999, Player: 2, Flags: 0},
			101: {Item: 1000, Player: 1, Flags: 0},
			102: {Item: 1001, Player: 2, Flags: 0},
		},
		{0, 2}: {
			200: {Item: 500, Player: 1, Flags: 0},
		},
	}
}

func TestChecks(t *testing.T) {
	t.Run("InitiallyAllMissing", func(t *testing.T) {
		ts := TeamSlot{0, 1}
		checks := newChecksState(makeTestLocations())

		assert.ElementsMatch(t, []int64{100, 101, 102}, checks.GetMissing(ts))
	})

	t.Run("InitiallyNoneChecked", func(t *testing.T) {
		ts := TeamSlot{0, 1}
		checks := newChecksState(makeTestLocations())

		assert.Empty(t, checks.GetChecked(ts))
	})

	t.Run("IsChecked_FalseInitially", func(t *testing.T) {
		ts := TeamSlot{0, 1}
		checks := newChecksState(makeTestLocations())

		assert.False(t, checks.IsChecked(ts, 100))
		assert.False(t, checks.IsChecked(ts, 101))
	})

	t.Run("MarkChecked", func(t *testing.T) {
		ts := TeamSlot{0, 1}
		checks := newChecksState(makeTestLocations())

		checks.mu.Lock()
		checks.checked[ts][100] = struct{}{}
		checks.mu.Unlock()

		assert.True(t, checks.IsChecked(ts, 100))
		assert.False(t, checks.IsChecked(ts, 101))
		assert.ElementsMatch(t, []int64{100}, checks.GetChecked(ts))
		assert.ElementsMatch(t, []int64{101, 102}, checks.GetMissing(ts))
	})

	t.Run("HalfChecked_HalfMissing", func(t *testing.T) {
		ts := TeamSlot{0, 1}
		checks := newChecksState(makeTestLocations())

		checks.mu.Lock()
		checks.checked[ts][100] = struct{}{}
		checks.checked[ts][101] = struct{}{}
		checks.mu.Unlock()

		assert.ElementsMatch(t, []int64{100, 101}, checks.GetChecked(ts))
		assert.ElementsMatch(t, []int64{102}, checks.GetMissing(ts))
	})

	t.Run("SlotsAreIndependent", func(t *testing.T) {
		ts1 := TeamSlot{0, 1}
		ts2 := TeamSlot{0, 2}
		checks := newChecksState(makeTestLocations())

		checks.mu.Lock()
		checks.checked[ts1][100] = struct{}{}
		checks.mu.Unlock()

		assert.False(t, checks.IsChecked(ts2, 200))
		assert.Empty(t, checks.GetChecked(ts2))
		assert.ElementsMatch(t, []int64{200}, checks.GetMissing(ts2))
	})

	t.Run("UnknownSlot_ReturnsEmpty", func(t *testing.T) {
		checks := newChecksState(makeTestLocations())
		unknown := TeamSlot{0, 99}

		assert.Empty(t, checks.GetChecked(unknown))
		assert.Empty(t, checks.GetMissing(unknown))
		assert.False(t, checks.IsChecked(unknown, 100))
	})

	t.Run("AllChecked_MissingEmpty", func(t *testing.T) {
		ts := TeamSlot{0, 1}
		checks := newChecksState(makeTestLocations())

		checks.mu.Lock()
		checks.checked[ts][100] = struct{}{}
		checks.checked[ts][101] = struct{}{}
		checks.checked[ts][102] = struct{}{}
		checks.mu.Unlock()

		assert.Empty(t, checks.GetMissing(ts))
		assert.Len(t, checks.GetChecked(ts), 3)
	})
}

func TestBroadcastPrintJson(t *testing.T) {
	game := "Celeste"

	makeRoom := func(t *testing.T) (*ApxRoom, func(slot int, tags []string) (<-chan []byte, *RegisteredClient)) {
		t.Helper()
		reg := newConnectionRegistry(nil, nil)
		room := &ApxRoom{connections: reg}

		newReceiver := func(slot int, tags []string) (<-chan []byte, *RegisteredClient) {
			received := make(chan []byte, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				for {
					_, raw, err := conn.Read(r.Context())
					if err != nil {
						return
					}
					received <- raw
				}
			}))
			t.Cleanup(server.Close)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.Cleanup(cancel)

			url := "ws" + strings.TrimPrefix(server.URL, "http")
			conn, _, err := websocket.Dial(ctx, url, nil)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}

			rc := &RegisteredClient{Slot: slot, game: &game, clientConn: conn, cancel: func() {}}
			reg.Register(slot, rc, game, tags)
			return received, rc
		}

		return room, newReceiver
	}

	t.Run("full client receives all messages", func(t *testing.T) {
		room, newReceiver := makeRoom(t)
		received, _ := newReceiver(1, []string{})

		receiving := 2
		room.broadcastPrintJson(context.Background(), []PrintJsonMessage{
			{Type: "Chat", Receiving: &receiving},
		})

		select {
		case <-received:
		case <-time.After(200 * time.Millisecond):
			t.Error("full client should have received message")
		}
	})

	t.Run("NoText client receives nothing", func(t *testing.T) {
		room, newReceiver := makeRoom(t)
		received, _ := newReceiver(1, []string{"NoText"})

		receiving := 1
		room.broadcastPrintJson(context.Background(), []PrintJsonMessage{
			{Type: "Chat", Receiving: &receiving},
		})

		select {
		case <-received:
			t.Error("NoText client should not have received message")
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("TextConcernsSelf client only receives messages concerning their slot", func(t *testing.T) {
		room, newReceiver := makeRoom(t)
		received, _ := newReceiver(1, []string{"TextConcernsSelf"})

		slotOne := 1
		slotTwo := 2
		room.broadcastPrintJson(context.Background(), []PrintJsonMessage{
			{Type: "ItemSend", Receiving: &slotOne}, // relevant
			{Type: "ItemSend", Receiving: &slotTwo}, // not relevant
		})

		select {
		case raw := <-received:
			// Should only contain the message for slot 1
			if strings.Contains(string(raw), `"receiving":2`) {
				t.Error("TextConcernsSelf client received message for another slot")
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("TextConcernsSelf client should have received relevant message")
		}

		// Ensure no second message arrives
		select {
		case <-received:
			t.Error("TextConcernsSelf client should not have received second message")
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("TextConcernsSelf client receives message when they are the item sender", func(t *testing.T) {
		room, newReceiver := makeRoom(t)
		received, _ := newReceiver(1, []string{"TextConcernsSelf"})

		otherSlot := 2
		item := NetworkItem{Item: 100, Location: 50, Player: 1, Flags: 0} // Player 1 is the sender
		room.broadcastPrintJson(context.Background(), []PrintJsonMessage{
			{Type: "ItemSend", Receiving: &otherSlot, Item: &item},
		})

		select {
		case <-received:
		case <-time.After(200 * time.Millisecond):
			t.Error("TextConcernsSelf client should have received message where they are the item sender")
		}
	})

	t.Run("full and TextConcernsSelf clients both receive relevant messages", func(t *testing.T) {
		room, newReceiver := makeRoom(t)
		fullReceived, _ := newReceiver(1, []string{})
		selfReceived, _ := newReceiver(2, []string{"TextConcernsSelf"})

		slotTwo := 2
		room.broadcastPrintJson(context.Background(), []PrintJsonMessage{
			{Type: "ItemSend", Receiving: &slotTwo},
		})

		select {
		case <-fullReceived:
		case <-time.After(200 * time.Millisecond):
			t.Error("full client should have received message")
		}
		select {
		case <-selfReceived:
		case <-time.After(200 * time.Millisecond):
			t.Error("TextConcernsSelf client should have received message concerning their slot")
		}
	})

	t.Run("empty message list does nothing", func(t *testing.T) {
		room, newReceiver := makeRoom(t)
		received, _ := newReceiver(1, []string{})

		room.broadcastPrintJson(context.Background(), []PrintJsonMessage{})

		select {
		case <-received:
			t.Error("no message should be sent for empty list")
		case <-time.After(100 * time.Millisecond):
		}
	})
}

func TestHintsState(t *testing.T) {
	hint1 := Hint{FindingPlayer: 1, Location: 100, ReceivingPlayer: 2, Item: 50}
	ts := TeamSlot{Team: 0, Slot: 1}

	t.Run("AddSlotHint", func(t *testing.T) {
		t.Run("adds to empty slot", func(t *testing.T) {
			h := newHintsState()
			h.AddSlotHint(hint1)
			if hints := h.GetSlotHints(ts); len(hints) != 1 || hints[0] != hint1 {
				t.Errorf("expected [%+v], got %+v", hint1, hints)
			}
		})

		t.Run("ignores duplicate", func(t *testing.T) {
			h := newHintsState()
			h.AddSlotHint(hint1)
			h.AddSlotHint(hint1)
			if hints := h.GetSlotHints(ts); len(hints) != 1 {
				t.Errorf("expected 1 hint, got %d", len(hints))
			}
		})

		t.Run("same location different finder is not duplicate", func(t *testing.T) {
			h := newHintsState()
			h.AddSlotHint(hint1)
			h.AddSlotHint(Hint{FindingPlayer: 2, Location: 100, ReceivingPlayer: 1})
			// slot 1 sees hint1 (as finder) + second hint (as receiver)
			if hints := h.GetSlotHints(ts); len(hints) != 2 {
				t.Errorf("expected 2 hints for slot 1, got %d", len(hints))
			}
		})

		t.Run("different finders produce independent hints", func(t *testing.T) {
			h := newHintsState()
			ts2 := TeamSlot{Team: 0, Slot: 2}
			ts3 := TeamSlot{Team: 0, Slot: 3}
			h.AddSlotHint(hint1) // finder=1, receiver=2
			h.AddSlotHint(Hint{FindingPlayer: 2, Location: 200, ReceivingPlayer: 3})

			assert.Len(t, h.GetSlotHints(ts), 1)  // slot 1: finder of hint1
			assert.Len(t, h.GetSlotHints(ts2), 2) // slot 2: receiver of hint1, finder of hint2
			assert.Len(t, h.GetSlotHints(ts3), 1) // slot 3: receiver of hint2
		})
	})

	t.Run("UpdateSlotHint", func(t *testing.T) {
		t.Run("updates existing hint", func(t *testing.T) {
			h := newHintsState()
			h.AddSlotHint(hint1)
			updated := hint1
			updated.Found = true
			updated.Status = HintStatusFound
			h.UpdateSlotHint(updated)
			hints := h.GetSlotHints(ts)
			if len(hints) != 1 || !hints[0].Found || hints[0].Status != HintStatusFound {
				t.Errorf("hint not updated correctly: %+v", hints)
			}
		})

		t.Run("no-op when hint does not exist", func(t *testing.T) {
			h := newHintsState()
			h.UpdateSlotHint(hint1)
			if hints := h.GetSlotHints(ts); len(hints) != 0 {
				t.Errorf("expected 0 hints, got %d", len(hints))
			}
		})
	})

	t.Run("GetSlotHints returns copy", func(t *testing.T) {
		h := newHintsState()
		h.AddSlotHint(hint1)
		got := h.GetSlotHints(ts)
		got[0].Found = true
		if h.GetSlotHints(ts)[0].Found {
			t.Error("GetSlotHints should return a copy")
		}
	})

	t.Run("GetHintsUsed returns zero for unknown slot", func(t *testing.T) {
		h := newHintsState()
		if used := h.GetHintsUsed(TeamSlot{Team: 0, Slot: 99}); used != 0 {
			t.Errorf("expected 0, got %d", used)
		}
	})
}

func BenchmarkBroadcastPrintJson(b *testing.B) {
	const (
		slotCount          = 2500
		selfClientsPerSlot = 4
		msgsPerBroadcast   = 5
	)

	ctx := context.Background()

	// Build a minimal ApxRoom with fake connections
	room := &ApxRoom{
		connections:   newConnectionRegistry(nil, nil),
		bcUnorderedCh: make(chan func(), 256),
	}

	// Drain the broadcast channel in background
	go func() {
		for fn := range room.bcUnorderedCh {
			fn()
		}
	}()

	// Register fake self-only clients across slots
	for slot := 1; slot <= slotCount; slot++ {
		for range selfClientsPerSlot {
			slotName := fmt.Sprintf("slot%d", slot)
			game := "TestGame"
			client := &RegisteredClient{
				Slot:                   slot,
				slotName:               &slotName,
				game:                   &game,
				forcedTextConcernsSelf: true,
				clientConn:             nil, // writes will be no-ops
			}
			room.connections.Register(slot, client, game, []string{"TextConcernsSelf"})
		}
	}

	// Build representative msgs — 5 messages each targeting 2 different slots
	makeMsgs := func() []PrintJsonMessage {
		msgs := make([]PrintJsonMessage, msgsPerBroadcast)
		for i := range msgs {
			receiving := (i % slotCount) + 1
			findingPlayer := int16((i+1)%slotCount + 1)
			item := NetworkItem{Item: int64(i + 1), Location: int64(i + 100), Player: findingPlayer}
			msgs[i] = PrintJsonMessage{
				Type:      "ItemSend",
				Receiving: &receiving,
				Item:      &item,
			}
		}
		return msgs
	}

	b.ReportAllocs()

	for b.Loop() {
		room.broadcastPrintJson(ctx, makeMsgs())
	}
}
