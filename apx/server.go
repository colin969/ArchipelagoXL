package main

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type fullFeedStore struct {
	mu            sync.RWMutex
	fullFeedSlots map[int]struct{}
}

func newFullFeedStore() *fullFeedStore {
	return &fullFeedStore{fullFeedSlots: make(map[int]struct{})}
}

func (ffs *fullFeedStore) Get() map[int]struct{} {
	ffs.mu.RLock()
	defer ffs.mu.RUnlock()
	result := make(map[int]struct{}, len(ffs.fullFeedSlots))
	maps.Copy(result, ffs.fullFeedSlots)
	return result
}

func (ffs *fullFeedStore) Allowed(slotId int) bool {
	ffs.mu.RLock()
	defer ffs.mu.RUnlock()
	_, ok := ffs.fullFeedSlots[slotId]
	return ok
}

func (ffs *fullFeedStore) Set(slotId int) {
	ffs.mu.Lock()
	defer ffs.mu.Unlock()
	ffs.fullFeedSlots[slotId] = struct{}{}
}

func (ffs *fullFeedStore) Delete(slotId int) {
	ffs.mu.Lock()
	defer ffs.mu.Unlock()
	delete(ffs.fullFeedSlots, slotId)
}

type passwordStore struct {
	mu        sync.RWMutex
	passwords map[int]string
}

func newPasswordStore() *passwordStore {
	return &passwordStore{passwords: make(map[int]string)}
}

func (ps *passwordStore) Get(slotId int) (string, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	p, ok := ps.passwords[slotId]
	return p, ok
}

func (ps *passwordStore) Set(slotId int, password string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.passwords[slotId] = password
}

func (ps *passwordStore) Delete(slotId int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.passwords, slotId)
}

type debugTap struct {
	slots []debugTapSlot
}

type debugTapSlot struct {
	mu        sync.RWMutex
	listeners []chan []byte
}

func newDebugTap(slotCount int) *debugTap {
	// + 1 since slots start at 1.
	return &debugTap{slots: make([]debugTapSlot, slotCount+1)}
}

type apxServer struct {
	passwordless bool
	lastActivity *atomic.Int64
	logf         func(f string, v ...any)
	config       *Config
	roomInfo     RoomInfoMessage
	roomPlayers  *RoomPlayers // Immutable
	passwords    *passwordStore
	fullFeed     *fullFeedStore
	bounceInfo   *bounceInfoStore
	connections  *connectionRegistry
	datapackages *DataPackageStore
	metrics      *metrics
	lobbyRoomId  string
	apPort       int
	debugTap     *debugTap
	lokiLogger   *LokiLogger
}

// No strict lock, but this MUST be immutable to be safe
type registeredClient struct {
	slotId     int
	game       *string
	cancel     context.CancelFunc
	clientConn *websocket.Conn
	reduced    bool
}

// Stores data from all connected clients which is needed globally
type connectionRegistry struct {
	mu      sync.RWMutex
	clients map[int][]*registeredClient
	// Tags being covered here means registeredClient can stay immutable
	tags map[*registeredClient][]string
}

func newConnectionRegistry() *connectionRegistry {
	return &connectionRegistry{
		clients: make(map[int][]*registeredClient),
		tags:    make(map[*registeredClient][]string),
	}
}

func (cr *connectionRegistry) Register(slotId int, client *registeredClient, tags []string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.clients[slotId] = append(cr.clients[slotId], client)
	cr.tags[client] = tags
}

// There HAS to be a safer way of doing this surely
func (cr *connectionRegistry) UpdateTags(client *registeredClient, tags []string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.tags[client] = tags
}

func (cr *connectionRegistry) Kick(slotId int) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Disconnect all clients to this slot
	clients := cr.clients[slotId]
	for _, client := range clients {
		client.cancel()
		delete(cr.tags, client)
	}
	delete(cr.clients, slotId)
}

func (cr *connectionRegistry) Unregister(client *registeredClient) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Remove client from slot names arrays
	clients := cr.clients[client.slotId]
	i := slices.Index(clients, client)
	if i < 0 {
		return
	}
	cr.clients[client.slotId] = slices.Delete(clients, i, i+1)
	if len(cr.clients[client.slotId]) == 0 {
		delete(cr.clients, client.slotId)
	}

	delete(cr.tags, client)
	if len(cr.clients[client.slotId]) == 0 {
		delete(cr.clients, client.slotId)
	}
}

func (cr *connectionRegistry) BroadcastBounceFromSlot(ctx context.Context, bounceInfo *bounceInfoStore, slotId int, msg BounceMessage) {
	// Strip excluded tags
	msg.Tags = slices.DeleteFunc(msg.Tags, func(tag string) bool {
		return bounceInfo.IsExcluded(slotId, tag)
	})
	cr.BroadcastBounce(ctx, msg)
}

func (cr *connectionRegistry) BroadcastBounce(ctx context.Context, msg BounceMessage) {
	msgTagSet := buildTagSet(msg.Tags)

	// Lock because client tags are mutable
	cr.mu.RLock()
	var targets []*registeredClient
	for _, clients := range cr.clients {
		for _, c := range clients {
			if hasTagOverlap(cr.tags[c], msgTagSet) || hasSlotOverlap(c.slotId, msg.Slots) || hasGameOverlap(*c.game, msg.Games) {
				targets = append(targets, c)
			}
		}
	}
	cr.mu.RUnlock()

	if msg.Tags == nil {
		msg.Tags = []string{}
	}

	if msg.Games == nil {
		msg.Games = []string{}
	}

	if msg.Slots == nil {
		msg.Slots = []int{}
	}
	// Content is the same, just a different cmd sending out
	msg.Cmd = "Bounced"
	for _, c := range targets {
		_ = wsjson.Write(ctx, c.clientConn, []any{msg})
	}
}

func SendChatMessageToClient(ctx context.Context, clientConn *websocket.Conn, slotId int, msg string) {
	message := PrintJsonChatMessage{
		Cmd: "PrintJSON",
		Data: []JsonMessagePart{
			{
				Type:  "text",
				Text:  msg,
				Color: "bold",
			},
		},
		Type:    "Chat",
		Team:    0,
		Slot:    slotId,
		Message: msg,
	}

	_ = wsjson.Write(ctx, clientConn, []any{message})
}

func (cr *connectionRegistry) SendChatMessageToSlot(ctx context.Context, slotId int, msg string) {
	message := PrintJsonChatMessage{
		Cmd: "PrintJSON",
		Data: []JsonMessagePart{
			{
				Type:  "text",
				Text:  msg,
				Color: "bold",
			},
		},
		Type:    "Chat",
		Team:    0,
		Slot:    slotId,
		Message: msg,
	}

	cr.mu.RLock()
	var targets []*registeredClient
	for _, clients := range cr.clients {
		for _, c := range clients {
			if c.slotId == slotId {
				targets = append(targets, c)
			}
		}
	}
	cr.mu.RUnlock()

	for _, c := range targets {
		_ = wsjson.Write(ctx, c.clientConn, []any{message})
	}
}

type connectionState struct {
	authenticated           bool
	slotName                *string
	cancel                  context.CancelFunc
	clientConn              *websocket.Conn
	apConn                  *websocket.Conn
	reduced                 bool
	pendingDatapackGames    []string
	registeredClient        *registeredClient
	authFailCount           int
	prevDatapackageGamesReq string
	// Retry storm may happen before auth, we can read this after connect for metrics
	isRetryStormClient bool
	largeDpRequested   int // Max that were requested at once before auth
}

type MessageType string
type Permission int

const (
	wsReadLimit = 1 << 24 // 16 MB
)

type apxHandler struct {
	server  *apxServer
	reduced bool
}

// Put options on the handler so we can run 2 servers with the same apxServer backing them
func (h apxHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.server.serveConn(w, r, h.reduced)
}

func (s apxServer) serveConn(w http.ResponseWriter, r *http.Request, reduced bool) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Adds about 32mb memory usage per 1k connections, debatble CPU usage
		CompressionMode:    websocket.CompressionContextTakeover,
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.logf("client connection: %v", err)
		return
	}
	defer c.CloseNow()

	s.metrics.connectedClients.WithLabelValues(s.lobbyRoomId).Inc()
	defer s.metrics.connectedClients.WithLabelValues(s.lobbyRoomId).Dec()

	c.SetReadLimit(wsReadLimit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Send RoomInfo as opening message
	if err := wsjson.Write(ctx, c, []any{s.roomInfo}); err != nil {
		s.logf("failed to send RoomInfo: %v", err)
		return
	}

	connState := &connectionState{
		authenticated:        false,
		cancel:               cancel,
		clientConn:           c,
		reduced:              reduced,
		pendingDatapackGames: []string{},
		authFailCount:        0,
	}
	defer func() {
		if connState.registeredClient != nil {
			s.connections.Unregister(connState.registeredClient)
		}
		if connState.apConn != nil {
			connState.apConn.CloseNow()
		}
	}()

	for {
		var messages []map[string]any
		err = wsjson.Read(ctx, c, &messages)
		if err != nil {
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				s.logf("client read inner: %v", err)
			}
			return
		}

		for _, message := range messages {
			// Uncomment to add raw printing
			raw, err := json.Marshal(message)
			if err != nil {
				s.logf("client unmarshal: %v", err)
				return
			}

			cmd, ok := message["cmd"].(string)
			if !ok {
				s.logf("message missing or invalid cmd field: %v", message)
				continue
			}

			if s.lokiLogger != nil && connState.authenticated {
				s.lokiLogger.Log(*connState.slotName, LogSourceClient, raw)
			}

			if connState.authenticated {
				s.metrics.incomingPackets.WithLabelValues(s.lobbyRoomId, *connState.slotName, *connState.registeredClient.game, cmd).Inc()
			}

			if err := s.handleMessage(ctx, connState, MessageType(cmd), message); err != nil {
				if !isNormalClose(err) && ctx.Err() == nil {
					s.logf("client read outer: %v", err)
				}
			}
		}
	}
}

func (s apxServer) handleMessage(ctx context.Context, connState *connectionState, cmd MessageType, raw map[string]any) error {
	// Shovel logs to any debug listeners
	if connState.authenticated && s.debugTap != nil && s.debugTap.HasListeners(connState.registeredClient.slotId) {
		if raw, err := json.Marshal(raw); err == nil {
			s.debugTap.Send(connState.registeredClient.slotId, raw)
		}
	}

	// Match against each message type we want to intercept from client -> server
	switch cmd {
	case MessageTypeGetDataPackage:
		return s.handleGetDataPackage(ctx, connState, raw)
	}

	if connState.authenticated {
		s.lastActivity.Store(time.Now().Unix())
		switch cmd {
		case MessageTypeBounce:
			return s.handleBounce(ctx, connState, raw)
		case MessageTypeConnect:
			return s.handleAuthedConnect(ctx, connState, raw)
		case MessageTypeConnectUpdate:
			return s.handleConnectUpdate(ctx, connState, raw)
		case MessageTypeSay:
			return s.handleSay(ctx, connState, raw)
		default:
			// We're authed, it's a message we don't care about, pass it on
			if connState.apConn != nil {
				return wsjson.Write(ctx, connState.apConn, []any{raw})
			} else {
				connState.cancel()
			}
			return nil
		}
	} else {
		// Limit routes for unauthed clients so we don't need to check in every handler
		// Also lets us ignore duplicate connect messages
		switch cmd {
		case MessageTypeConnect:
			return s.handleConnect(ctx, connState, raw)
		default:
			s.logf("unknown command: %q", cmd)
			return nil
		}
	}
}

// Debatable whether this way of using tags as a set/map is even saving time, but eh, what's the harm
func hasTagOverlap(clientTags []string, msgTagSet map[string]struct{}) bool {
	for _, t := range clientTags {
		if _, ok := msgTagSet[t]; ok {
			return true
		}
	}
	return false
}

func hasSlotOverlap(slotID int, slots []int) bool {
	return slices.Contains(slots, slotID)
}

func hasGameOverlap(game string, games []string) bool {
	return slices.Contains(games, game)
}

// Turns an array into an O(1) map
func buildTagSet(tags []string) map[string]struct{} {
	set := make(map[string]struct{}, len(tags))
	for _, t := range tags {
		set[t] = struct{}{}
	}
	return set
}

func (dt *debugTap) HasListeners(slotId int) bool {
	if slotId <= 0 || slotId >= len(dt.slots) {
		return false
	}
	s := &dt.slots[slotId]
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.listeners) > 0
}

func (dt *debugTap) Subscribe(slotId int) (<-chan []byte, func(), bool) {
	if slotId <= 0 || slotId >= len(dt.slots) {
		return nil, nil, false
	}

	ch := make(chan []byte, 64)
	s := &dt.slots[slotId]

	s.mu.Lock()
	s.listeners = append(s.listeners, ch)
	s.mu.Unlock()

	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, l := range s.listeners {
			if l == ch {
				s.listeners = slices.Delete(s.listeners, i, i+1)
				close(ch)
				break
			}
		}
	}
	return ch, cancel, true
}

func (dt *debugTap) Send(slotId int, raw []byte) {
	if slotId <= 0 || slotId >= len(dt.slots) {
		return
	}
	s := &dt.slots[slotId]
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ch := range s.listeners {
		select {
		case ch <- raw:
		default:
		}
	}
}
