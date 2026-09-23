package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
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

			rc := &registeredClient{
				slotId:     tc.clientSlot,
				game:       &tc.clientGame,
				clientConn: conn,
				cancel:     func() {},
				reduced:    false,
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
		rc := &registeredClient{slotId: 1, game: &game, clientConn: conn, cancel: func() {}}
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
		rc := &registeredClient{slotId: 1, game: &game, clientConn: conn, cancel: func() {}}
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
		rc := &registeredClient{slotId: 3, game: &game, clientConn: conn, cancel: func() {}}
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
		rc := &registeredClient{slotId: 1, game: &game, clientConn: conn, cancel: func() {}}
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

		rc := &registeredClient{slotId: 1, game: &game, clientConn: conn, cancel: func() {}}
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

		rc := &registeredClient{slotId: slotId, game: &game, clientConn: conn, cancel: func() {}}
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

		rc2 := &registeredClient{slotId: 2, game: &game, clientConn: conn2, cancel: func() {}}
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
		rc := &registeredClient{slotId: slotId, game: &game, clientConn: conn, cancel: func() {}}
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
		rc := &registeredClient{slotId: 1, game: &game, clientConn: conn, cancel: func() {}}
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
		rc := &registeredClient{slotId: 1, game: &game, clientConn: conn, cancel: func() {}}
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

		rc1 := &registeredClient{slotId: 1, game: &game, clientConn: conn1, cancel: func() {}}
		rc2 := &registeredClient{slotId: 2, game: &game, clientConn: conn2, cancel: func() {}}
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
		client := &registeredClient{
			slotId: 1,
			game:   &game,
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
		client := &registeredClient{
			slotId: 1,
			game:   &game,
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
		c1 := &registeredClient{slotId: 1, game: &game}
		c2 := &registeredClient{slotId: 1, game: &game}

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
