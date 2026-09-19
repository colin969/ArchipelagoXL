package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
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

type ConnectNames struct {
	mu    sync.RWMutex
	names map[string]string
}

func newAltConnectNames() *ConnectNames {
	return &ConnectNames{
		names: make(map[string]string),
	}
}

func (cn *ConnectNames) SetAltName(realName string, connectName string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.names[connectName] = realName
}

func (cn *ConnectNames) RemoveAltName(connectName string) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	delete(cn.names, connectName)
}

func (cn *ConnectNames) GetAltName(connectName string) *string {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	realName, ok := cn.names[connectName]
	if ok {
		return &realName
	}
	return nil
}

func (cn *ConnectNames) GetAltNamesBySlot(realName string) []string {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	altNames := make([]string, 0)
	for connectName, slot := range cn.names {
		if slot == realName {
			altNames = append(altNames, connectName)
		}
	}
	return altNames
}

type RoomInfoStore struct {
	mu  sync.RWMutex
	msg RoomInfoMessage
	// immutable
	DatapackageChecksums map[string]string
}

func newRoomInfoStore(msg RoomInfoMessage) *RoomInfoStore {
	checksums := make(map[string]string, len(msg.DatapackageChecksums))
	maps.Copy(checksums, msg.DatapackageChecksums)
	return &RoomInfoStore{
		msg:                  msg,
		DatapackageChecksums: checksums,
	}
}

func (ris *RoomInfoStore) Load() RoomInfoMessage {
	ris.mu.RLock()
	defer ris.mu.RUnlock()
	msg := ris.msg
	msg.Time = float64(time.Now().UnixNano()) / 1e9
	return msg
}

func (ris *RoomInfoStore) Store(msg RoomInfoMessage) {
	ris.mu.Lock()
	defer ris.mu.Unlock()
	ris.msg = msg
}

func (ris *RoomInfoStore) HasPassword() bool {
	ris.mu.RLock()
	defer ris.mu.RUnlock()
	return ris.msg.Password
}

type ApxRoom struct {
	perSlotPasswords bool
	lastActivity     *atomic.Int64
	logf             func(f string, v ...any)
	config           *Config
	roomInfo         *RoomInfoStore
	roomPlayers      *RoomPlayers // Immutable
	altConnectNames  *ConnectNames
	passwords        *passwordStore
	fullFeed         *fullFeedStore
	bounceInfo       *bounceInfoStore
	connections      *connectionRegistry
	datapackages     *DataPackageStore
	metrics          *metrics
	lobbyRoomId      string
	apPort           int
	debugTap         *debugTap
	lokiLogger       *LokiLogger
	logDeath         func(slotId int)
}

// No strict lock, but this MUST be immutable to be safe
type registeredClient struct {
	slotId     int
	game       *string
	cancel     context.CancelFunc
	clientConn *websocket.Conn
	reduced    bool
}

func removeClient(clients []*registeredClient, target *registeredClient) []*registeredClient {
	i := slices.Index(clients, target)
	if i < 0 {
		return clients
	}
	return slices.Delete(clients, i, i+1)
}

// Stores data from all connected clients which is needed globally
type connectionRegistry struct {
	mu      sync.RWMutex
	clients map[int][]*registeredClient
	// Tags being covered here means registeredClient can stay immutable
	tags          map[*registeredClient][]string
	clientsByGame map[string][]*registeredClient
	clientsByTag  map[string][]*registeredClient
	// Just a copy of the static data so we can use it during register / unregister etc
	lobbyRoomId *string
	metrics     *metrics
}

func newConnectionRegistry(lobbyRoomId *string, metrics *metrics) *connectionRegistry {
	return &connectionRegistry{
		clients:       make(map[int][]*registeredClient),
		tags:          make(map[*registeredClient][]string),
		clientsByGame: make(map[string][]*registeredClient),
		clientsByTag:  make(map[string][]*registeredClient),
		lobbyRoomId:   lobbyRoomId,
		metrics:       metrics,
	}
}

func (cr *connectionRegistry) Register(slotId int, client *registeredClient, game string, tags []string) {
	cr.mu.Lock()
	cr.clients[slotId] = append(cr.clients[slotId], client)
	cr.tags[client] = tags
	cr.clientsByGame[game] = append(cr.clientsByGame[game], client)
	for _, tag := range tags {
		cr.clientsByTag[tag] = append(cr.clientsByTag[tag], client)
	}

	slotsConnected := len(cr.clients)

	cr.mu.Unlock()

	if cr.metrics != nil && cr.lobbyRoomId != nil {
		cr.metrics.connectedSlots.WithLabelValues(*cr.lobbyRoomId).Set(float64(slotsConnected))
	}
}

// There HAS to be a safer way of doing this surely
func (cr *connectionRegistry) UpdateTags(client *registeredClient, tags []string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	// Remove from old tag indices
	for _, tag := range cr.tags[client] {
		cr.clientsByTag[tag] = removeClient(cr.clientsByTag[tag], client)
		if len(cr.clientsByTag[tag]) == 0 {
			delete(cr.clientsByTag, tag)
		}
	}
	// Add to new tag indices
	cr.tags[client] = tags
	for _, tag := range tags {
		cr.clientsByTag[tag] = append(cr.clientsByTag[tag], client)
	}
}

func (cr *connectionRegistry) ConnectedSlotCount() int {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return len(cr.clients)
}

func (cr *connectionRegistry) Kick(slotId int) {
	cr.mu.Lock()

	// Disconnect all clients to this slot
	for _, client := range cr.clients[slotId] {
		client.cancel()
		cr.clientsByGame[*client.game] = removeClient(cr.clientsByGame[*client.game], client)
		for _, tag := range cr.tags[client] {
			cr.clientsByTag[tag] = removeClient(cr.clientsByTag[tag], client)
		}
		delete(cr.tags, client)
	}
	delete(cr.clients, slotId)

	slotsConnected := len(cr.clients)

	cr.mu.Unlock()

	if cr.metrics != nil && cr.lobbyRoomId != nil {
		cr.metrics.connectedSlots.WithLabelValues(*cr.lobbyRoomId).Set(float64(slotsConnected))
	}
}

func (cr *connectionRegistry) Unregister(client *registeredClient) {
	cr.mu.Lock()

	// Remove client from slot names arrays
	clients := cr.clients[client.slotId]
	i := slices.Index(clients, client)
	if i < 0 {
		cr.mu.Unlock()
		return
	}
	cr.clients[client.slotId] = slices.Delete(clients, i, i+1)
	if len(cr.clients[client.slotId]) == 0 {
		delete(cr.clients, client.slotId)
	}

	game := *client.game
	cr.clientsByGame[game] = removeClient(cr.clientsByGame[game], client)
	if len(cr.clientsByGame[game]) == 0 {
		delete(cr.clientsByGame, game)
	}
	for _, tag := range cr.tags[client] {
		cr.clientsByTag[tag] = removeClient(cr.clientsByTag[tag], client)
		if len(cr.clientsByTag[tag]) == 0 {
			delete(cr.clientsByTag, tag)
		}
	}
	delete(cr.tags, client)

	slotsConnected := len(cr.clients)

	cr.mu.Unlock()

	if cr.metrics != nil && cr.lobbyRoomId != nil {
		cr.metrics.connectedSlots.WithLabelValues(*cr.lobbyRoomId).Set(float64(slotsConnected))
	}
}

func (cr *connectionRegistry) BroadcastBounceFromSlot(ctx context.Context, bounceInfo *bounceInfoStore, slotId int, msg BounceMessage) {
	// Strip excluded tags
	msg.Tags = slices.DeleteFunc(msg.Tags, func(tag string) bool {
		return bounceInfo.IsExcludedByTag(slotId, tag)
	})
	cr.BroadcastBounce(ctx, msg, bounceInfo, slotId)
}

func (cr *connectionRegistry) BroadcastBounce(ctx context.Context, msg BounceMessage, bounceInfo *bounceInfoStore, senderSlot int) {
	// Most will match 2 clients, but give a tiny bit of give
	targets := make([]*registeredClient, 0, 4)
	seen := make(map[*registeredClient]struct{}, 4)
	senderLimited := bounceInfo.IsLimitedToOwnSlot(senderSlot)

	addTarget := func(c *registeredClient) {
		if _, ok := seen[c]; ok {
			return
		}
		seen[c] = struct{}{}
		if senderLimited && c.slotId != senderSlot {
			return
		} else if c.slotId != senderSlot && bounceInfo.IsLimitedToOwnSlot(c.slotId) {
			return
		}
		targets = append(targets, c)
	}

	// Lock because client tags are mutable
	cr.mu.RLock()

	for _, tag := range msg.Tags {
		for _, c := range cr.clientsByTag[tag] {
			addTarget(c)
		}
	}
	for _, game := range msg.Games {
		for _, c := range cr.clientsByGame[game] {
			addTarget(c)
		}
	}
	for _, slotId := range msg.Slots {
		for _, c := range cr.clients[slotId] {
			addTarget(c)
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
	targets := cr.clients[slotId]
	cr.mu.RUnlock()

	wrappedMsg := []any{message}
	for _, c := range targets {
		_ = wsjson.Write(ctx, c.clientConn, wrappedMsg)
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

func (cs *connectionState) SendMessage(ctx context.Context, msg []any) error {
	if cs.clientConn != nil {
		err := wsjson.Write(ctx, cs.clientConn, msg)
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("client conn not open")
}

type MessageType string
type Permission int

const (
	wsReadLimit = 1 << 24 // 16 MB
)

type apxHandler struct {
	server  *ApxRoom
	reduced bool
	id      int
}

// Put options on the handler so we can run 2 servers with the same apxServer backing them
func (h apxHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Feels silly but works as a guard for testing. Should never be the case in prod.
	if h.server == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	h.server.serveConn(w, r, h.reduced)
}

func (s ApxRoom) serveConn(w http.ResponseWriter, r *http.Request, reduced bool) {
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
	if err := wsjson.Write(ctx, c, []any{s.roomInfo.Load()}); err != nil {
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

	// Start keepalive ping pong
	go s.keepalive(ctx, c, cancel)

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
				sendInvalidPacket(ctx, connState.clientConn, PacketProblemCmd, nil, fmt.Sprintf("message missing or invalid cmd field: %v", message), s.lokiLogger, connState.slotName)
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

// Multiserver defaults are 20 and 20, we're a little more generous I guess
const (
	pingInterval = 20 * time.Second
	pingTimeout  = 40 * time.Second
)

func (s ApxRoom) keepalive(ctx context.Context, c *websocket.Conn, cancel context.CancelFunc) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, pingTimeout)
			err := c.Ping(pingCtx)
			pingCancel()
			if err != nil {
				if ctx.Err() == nil {
					s.logf("client ping timeout, dropping connection: %v", err)
				}
				cancel()
				return
			}
		}
	}
}

func (s ApxRoom) handleMessage(ctx context.Context, connState *connectionState, cmd MessageType, raw map[string]any) error {
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

func (s ApxRoom) StatusString(client *registeredClient) string {
	connected := s.connections.ConnectedSlotCount()
	total := len(s.roomPlayers.slots)
	probability := s.bounceInfo.GetProbability()
	deaths := s.bounceInfo.Get()

	totalDeaths := 0
	for _, count := range deaths {
		totalDeaths += count
	}

	lines := []string{
		fmt.Sprintf("=== APX Status: %s ===", s.lobbyRoomId),
		fmt.Sprintf("Slots connected: %d / %d", connected, total),
		fmt.Sprintf("Total deaths: %d", totalDeaths),
		fmt.Sprintf("Deathlink probability: %.0f%%", probability*100),
	}

	if client != nil {
		lines = append(lines, "--- Your Status ---")
		lines = append(lines, fmt.Sprintf("Deaths: %d", deaths[client.slotId]))
		if tags := s.bounceInfo.GetTagExclusionsForSlot(client.slotId); len(tags) > 0 {
			lines = append(lines, fmt.Sprintf("Blocked Bounce tags: %s", strings.Join(tags, ", ")))
		}
		if s.bounceInfo.IsLimitedToOwnSlot(client.slotId) {
			lines = append(lines, "You are isolated from other players bounces")
		}
	}

	return strings.Join(lines, "\n")
}
