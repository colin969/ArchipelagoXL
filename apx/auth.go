package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type CmdPeek struct {
	Cmd string `json:"cmd"`
}

type PrintJSONPeek struct {
	Receiving *int   `json:"receiving"`
	Slot      *int   `json:"slot"`
	Type      string `json:"type"`
}

func (s ApxRoom) handleAuthedConnect(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	var msg ConnectMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		cmd := MessageTypeConnect
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd, fmt.Sprintf("invalid Connect arguments: %v", err), s.lokiLogger, connState.slotName)
	}

	// bullshit multiserver behaviour
	if msg.SlotData == nil {
		slotDataDefault := true
		msg.SlotData = &slotDataDefault
	}

	// Update msg from alt connect name if it matches
	msg.Name = s.resolveAltName(msg.Name)

	log.Printf("[WS] reconnect (authed): game=%q name=%q uuid=%q version=%+v tags=%v slotData=%v",
		msg.Game, msg.Name, msg.UUID, msg.Version, msg.Tags, *msg.SlotData)

	// If they're trying to switch slots, drop the connection. Shouldn't break anything important.
	if connState.slotName != nil && *connState.slotName != msg.Name {
		s.logf("slot switch attempt from %q to %q, dropping", *connState.slotName, msg.Name)
		return connState.clientConn.Close(websocket.StatusNormalClosure, "SlotSwitch")
	}

	// TODO: Implement Connect for authed clients

	// Update tags on existing registration in case they changed
	s.connections.UpdateTags(connState.registeredClient, msg.Tags)

	return nil
}

func (s ApxRoom) handleConnect(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	var msg ConnectMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		cmd := MessageTypeConnect
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd, fmt.Sprintf("invalid Connect arguments: %v", err), s.lokiLogger, connState.slotName)
	}

	// bullshit multiserver behaviour
	if msg.SlotData == nil {
		slotDataDefault := true
		msg.SlotData = &slotDataDefault
	}

	// Update msg from alt connect name
	realName := s.altConnectNames.GetAltName(msg.Name)
	if realName != nil {
		msg.Name = *realName
	}

	log.Printf("[WS] connect: game=%q name=%q uuid=%q version=%+v tags=%v slotData=%v addr=%s",
		msg.Game, msg.Name, msg.UUID, msg.Version, msg.Tags, *msg.SlotData, connState.remoteAddr)

	connState.slotName = &msg.Name

	slotInfo, ok := s.state.NameToSlot[msg.Name]
	if !ok {
		return s.sendConnectionRefused(ctx, connState, "InvalidSlot", msg.Name)
	}

	// We've got a connect message, we should log it to the slot even if not authed yet, for debugging
	if s.debugTap != nil {
		slot, ok := s.state.NameToSlot[msg.Name]
		if ok && s.debugTap.HasListeners(slot.Slot) {
			if raw, err := json.Marshal(raw); err == nil {
				s.debugTap.Send(slot.Slot, raw)
			}
		}
	}

	if s.lokiLogger != nil {
		if raw, err := json.Marshal(raw); err == nil {
			s.lokiLogger.Log(&msg.Name, LogSourceClient, raw, MessageTypeConnect)
		}
	}

	if s.perSlotPasswords == true {
		if !s.validatePassword(slotInfo.Slot, msg.Password) {
			return s.sendConnectionRefused(ctx, connState, "InvalidPassword", msg.Name)
		}
	}

	// Restrict full feed client access
	if !connState.fromReducedPort && !s.isFullFeedAllowed(slotInfo.Slot) {
		return s.sendConnectionRefused(ctx, connState, "FullFeedDenial", msg.Name)
	}

	if len(connState.pendingDatapackGames) > 0 {
		if err := s.sendDataPackages(ctx, connState.clientConn, connState.pendingDatapackGames); err != nil {
			s.logf("error sending pending datapackages: %v", err)
		}
		connState.pendingDatapackGames = nil
	}

	// TODO: Send connected message back
	teamSlot := TeamSlot{slotInfo.Team, slotInfo.Slot}
	connectedMsg := ConnectedMessage{
		Team:             slotInfo.Team,
		Slot:             slotInfo.Slot,
		MissingLocations: s.state.Checks.GetMissing(teamSlot),
		CheckedLocations: s.state.Checks.GetChecked(teamSlot),
		Players:          s.state.PlayersNetwork.GetJson(),
		SlotInfo:         s.state.SlotInfoNetworkJson,
		HintPoints:       s.state.ServerOptions.GetHintCost(),
	}
	if msg.SlotData != nil && *msg.SlotData != false {
		connectedMsg.SlotData = s.state.SlotData[teamSlot]
	}

	err := wsjson.Write(ctx, connState.clientConn, connectedMsg)
	if err != nil {
		return err
	}

	client := RegisteredClient{
		Team:                   slotInfo.Team,
		Slot:                   slotInfo.Slot,
		slotName:               &slotInfo.Name,
		game:                   &slotInfo.Game,
		cancel:                 connState.cancel,
		clientConn:             connState.clientConn,
		forcedTextConcernsSelf: connState.fromReducedPort,
		notifyKeys:             make([]string, 0),
	}
	s.connections.Register(slotInfo.Slot, &client, slotInfo.Game, msg.Tags)
	connState.authenticated = true
	connState.registeredClient = &client

	// Send items (if they asked)
	if msg.ItemsHandling == nil {
		itemsHandling := 7
		msg.ItemsHandling = &itemsHandling
	}
	client.itemsHandling.Store(int32(*msg.ItemsHandling))
	err = s.SyncReceivedItems(ctx, &client, *msg.ItemsHandling)
	if err != nil {
		return err
	}

	// Send initial connection messages
	printMsg := simplePrintJsonMessage("Tutorial", "You are connected :)")
	err = wsjson.Write(ctx, client.clientConn, []any{printMsg})
	if err != nil {
		return err
	}

	log.Printf("[WS] Connected to %s", msg.Name)

	return nil
}

func (s ApxRoom) handleSay(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	var packet struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &packet); err != nil || packet.Text == "" {
		cmd := MessageTypeSay
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd, "Say packet missing or invalid 'text' field", s.lokiLogger, connState.slotName)
	}
	text := packet.Text

	trimmed := strings.TrimSpace(text)
	if strings.HasPrefix(trimmed, "!countdown") {
		SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.Slot, "You're not allowed to do this")
		return nil
	}
	if strings.HasPrefix(trimmed, "!players") {
		SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.Slot, "You're not allowed to do this")
		return nil
	}
	if handled, err := s.chatCommandRouter.Handle(ctx, connState, trimmed); handled {
		return err
	}

	slot := connState.registeredClient.Slot
	team := connState.registeredClient.Team
	text = fmt.Sprintf("%s: %s", *connState.slotName, text)
	s.broadcastPrintJson(ctx, []PrintJsonMessage{{
		Type:    "Chat",
		Data:    []JsonMessagePart{{Type: "text", Text: text}},
		Team:    &team,
		Slot:    &slot,
		Message: &text,
	}})

	return nil
}

func (s ApxRoom) handleConnectUpdate(ctx context.Context, connState *connectionState, raw json.RawMessage) error {
	var msg ConnectUpdateMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		cmd := MessageTypeConnectUpdate
		return sendInvalidPacket(ctx, connState.clientConn, PacketProblemArguments, &cmd, fmt.Sprintf("invalid ConnectUpdate arguments: %v", err), s.lokiLogger, connState.slotName)
	}

	s.connections.UpdateTags(connState.registeredClient, msg.Tags)

	// TODO: Implement Connect Update properly

	return nil
}

func (s ApxRoom) validatePassword(slotKey int, provided *string) bool {
	password, ok := s.passwords.Get(slotKey)
	return ok && provided != nil && password == *provided
}

func (s ApxRoom) resolveAltName(name string) string {
	if real := s.altConnectNames.GetAltName(name); real != nil {
		return *real
	}
	return name
}

func (s ApxRoom) isFullFeedAllowed(slotKey int) bool {
	return s.fullFeed.Allowed(slotKey)
}

func (s ApxRoom) sendConnectionRefused(ctx context.Context, connState *connectionState, reason string, name string) error {
	s.logf("%s for %s", reason, name)
	msg := ConnectionRefusedMessage{Errors: []string{reason}}

	if s.lokiLogger != nil {
		if raw, err := json.Marshal(msg); err == nil {
			s.lokiLogger.Log(&name, LogSourceApx, raw, "ConnectionRefused")
		}
	}

	if err := wsjson.Write(ctx, connState.clientConn, []any{msg}); err != nil {
		connState.cancel()
		_ = connState.clientConn.CloseNow()
		return err
	}
	connState.authFailCount++
	if connState.authFailCount > 10 {
		_ = connState.clientConn.Close(websocket.StatusNormalClosure, reason)
		connState.cancel()
		return nil
	}
	return nil
}

func isNormalClose(err error) bool {
	if err == nil {
		return true
	}
	status := websocket.CloseStatus(err)
	if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "use of closed network connection") || strings.Contains(msg, "context canceled")
}
