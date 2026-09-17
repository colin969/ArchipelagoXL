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

// Make sure only the right bounce messages reach the right clients
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

			reg := newConnectionRegistry()
			reg.Register(tc.clientSlot, rc, tc.clientGame, tc.clientTags)

			reg.BroadcastBounce(context.Background(), BounceMessage{
				Tags:  tc.msgTags,
				Slots: tc.msgSlots,
				Games: tc.msgGames,
				Data:  map[string]any{"test": true},
			}, bounceInfo, tc.senderSlot)

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

func TestConnectionRegistry_SuccessfulConnect(t *testing.T) {
	cr := newConnectionRegistry()
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
}

func TestConnectionRegistry_UnregisterAfterConnect(t *testing.T) {
	cr := newConnectionRegistry()
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
}

func TestConnectionRegistry_MultipleConnectsSameSlot(t *testing.T) {
	cr := newConnectionRegistry()
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
}
