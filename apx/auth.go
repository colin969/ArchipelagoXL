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

func (s apxServer) handleAuthedConnect(ctx context.Context, connState *connectionState, raw map[string]any) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshalling connect message: %w", err)
	}

	var msg ConnectMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshalling connect message: %w", err)
	}

	// bullshit multiserver behaviour
	if msg.SlotData == nil {
		slotDataDefault := true
		msg.SlotData = &slotDataDefault
	}

	log.Printf("reconnect (authed): game=%q name=%q uuid=%q version=%+v tags=%v slotData=%v",
		msg.Game, msg.Name, msg.UUID, msg.Version, msg.Tags, msg.SlotData)

	// If they're trying to switch slots, drop the connection. Shouldn't break anything important.
	if connState.slotName != nil && *connState.slotName != msg.Name {
		s.logf("slot switch attempt from %q to %q, dropping", *connState.slotName, msg.Name)
		return connState.clientConn.Close(websocket.StatusNormalClosure, "SlotSwitch")
	}

	// Don't allow to change from reduced to full feed or vice versa
	msg.ReducedTraffic = connState.reduced

	// Send on to AP, they'll still expect a Connected message back
	if err := wsjson.Write(ctx, connState.apConn, []any{msg}); err != nil {
		return fmt.Errorf("forwarding reconnect to AP: %w", err)
	}

	// Update tags on existing registration in case they changed
	s.connections.UpdateTags(connState.registeredClient, msg.Tags)

	return nil
}

func (s apxServer) handleConnect(ctx context.Context, connState *connectionState, raw map[string]any) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshalling connect message: %w", err)
	}

	var msg ConnectMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshalling connect message: %w", err)
	}

	// bullshit multiserver behaviour
	if msg.SlotData == nil {
		slotDataDefault := true
		msg.SlotData = &slotDataDefault
	}

	log.Printf("connect: game=%q name=%q uuid=%q version=%+v tags=%v slotData=%v",
		msg.Game, msg.Name, msg.UUID, msg.Version, msg.Tags, msg.SlotData)

	connState.slotName = &msg.Name

	slotEntry, ok := s.roomPlayers.auth[msg.Name]
	if !ok {
		errorMsg := ConnectionRefusedMessage{
			Cmd:    "ConnectionRefused",
			Errors: []string{"InvalidSlot"},
		}
		s.logf("InvalidSlot for %s", msg.Name)
		if err := wsjson.Write(ctx, connState.clientConn, []any{errorMsg}); err != nil {
			_ = connState.clientConn.CloseNow()
			return err
		}
		connState.authFailCount += 1
		if connState.authFailCount > 10 {
			return connState.clientConn.Close(websocket.StatusNormalClosure, "InvalidSlot")
		}
		return nil
	}

	// We've got a connect message, we should log it to the slot even if not authed yet, for debugging
	if s.debugTap != nil {
		slotId, ok := s.roomPlayers.nameToID[msg.Name]
		if ok && s.debugTap.HasListeners(slotId) {
			if raw, err := json.Marshal(raw); err == nil {
				s.debugTap.Send(slotId, raw)
			}
		}
	}

	if s.lokiLogger != nil {
		if raw, err := json.Marshal(raw); err == nil {
			s.lokiLogger.Log(msg.Name, LogSourceClient, raw)
		}
	}

	if s.passwordless != true {
		password, ok := s.passwords.Get(slotEntry[1])
		if !(ok && msg.Password != nil && password == *msg.Password) {
			errorMsg := ConnectionRefusedMessage{
				Cmd:    "ConnectionRefused",
				Errors: []string{"InvalidPassword"},
			}
			s.logf("InvalidPassword for %s", msg.Name)
			if err := wsjson.Write(ctx, connState.clientConn, []any{errorMsg}); err != nil {
				_ = connState.clientConn.CloseNow()
				return err
			}
			connState.authFailCount += 1
			if connState.authFailCount > 10 {
				return connState.clientConn.Close(websocket.StatusNormalClosure, "InvalidPassword")
			}
			return nil
		}
	}

	// Restrict full feed client access
	if !connState.reduced {
		allowed := s.fullFeed.Allowed(slotEntry[1])
		if !allowed {
			SendChatMessageToClient(ctx, connState.clientConn, slotEntry[1], "Restricted access to this port")
			errorMsg := ConnectionRefusedMessage{
				Cmd:    "ConnectionRefused",
				Errors: []string{"InvalidSlot"},
			}
			s.logf("Full feed denial for %s", msg.Name)
			if err := wsjson.Write(ctx, connState.clientConn, []any{errorMsg}); err != nil {
				_ = connState.clientConn.CloseNow()
				return err
			}
			connState.authFailCount += 1
			if connState.authFailCount > 10 {
				return connState.clientConn.Close(websocket.StatusNormalClosure, "FullFeedDenial")
			}
			return nil
		}
	}

	connState.authenticated = true

	if len(connState.pendingDatapackGames) > 0 {
		if err := s.sendDataPackages(ctx, connState.clientConn, connState.pendingDatapackGames); err != nil {
			s.logf("error sending pending datapackages: %v", err)
		}
		connState.pendingDatapackGames = nil
	}

	apConn, slotId, game, err := s.connectAP(ctx, connState, connState.reduced, msg, s.apPort)
	if err != nil {
		return fmt.Errorf("connecting to AP: %w", err)
	}
	connState.apConn = apConn
	client := registeredClient{
		slotId:     slotId,
		game:       game,
		cancel:     connState.cancel,
		clientConn: connState.clientConn,
		reduced:    connState.reduced,
	}
	s.connections.Register(slotId, &client, msg.Tags)
	connState.registeredClient = &client

	log.Printf("Connected to %s", msg.Name)

	return nil
}

func (s apxServer) handleSay(ctx context.Context, connState *connectionState, raw map[string]any) error {
	// Only need 1 field, don't bother re and unmarshaling for struct
	if text, ok := raw["text"].(string); ok && text != "" {
		trimmed := strings.ToLower(strings.TrimSpace(text))
		if strings.HasPrefix(trimmed, "!countdown") {
			SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.slotId, "You're not allowed to do this")
			return nil
		}
		if strings.HasPrefix(trimmed, "!players") {
			SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.slotId, "You're not allowed to do this")
			return nil
		}
	}

	if connState.apConn != nil {
		return wsjson.Write(ctx, connState.apConn, []any{raw})
	}

	return nil
}

func (s apxServer) handleConnectUpdate(ctx context.Context, connState *connectionState, raw map[string]any) error {
	data, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshalling connectupdate message: %w", err)
	}

	var msg ConnectUpdateMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("unmarshalling connectupdate message: %w", err)
	}

	s.connections.UpdateTags(connState.registeredClient, msg.Tags)

	if connState.apConn != nil {
		return wsjson.Write(ctx, connState.apConn, []any{raw})
	}
	return nil
}

func (s apxServer) connectAP(ctx context.Context, connState *connectionState, reduced bool, connectMsg ConnectMessage, apPort int) (*websocket.Conn, int, *string, error) {
	// Fix password when talking to ap server (only needed when using per-slot passwords)
	if s.config.LobbyEnabled {
		if s.roomInfo.Password {
			connectMsg.Password = &s.config.APPassword
		} else {
			connectMsg.Password = nil
		}
	}

	log.Printf("Connecting to AP server at %s", fmt.Sprintf("ws://%s:%d", s.config.APHost, apPort))

	apConn, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://%s:%d", s.config.APHost, apPort), nil)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("dialing AP server: %w", err)
	}

	apConn.SetReadLimit(wsReadLimit)

	// Wait for RoomInfo from AP server (just so we know it's ready to accept the Connect message)
	var roomInfo []map[string]any
	if err := wsjson.Read(ctx, apConn, &roomInfo); err != nil {
		apConn.CloseNow()
		return nil, 0, nil, fmt.Errorf("reading RoomInfo from AP: %w", err)
	}

	// Send connect message
	connectMsg.ReducedTraffic = reduced
	if err := wsjson.Write(ctx, apConn, []any{connectMsg}); err != nil {
		apConn.CloseNow()
		return nil, 0, nil, fmt.Errorf("forwarding Connect to AP: %w", err)
	}

	// Preserves key order. Thanks AHIT.
	// Read Connected (or ConnectionRefused) response from AP
	var response []json.RawMessage
	if err := wsjson.Read(ctx, apConn, &response); err != nil {
		apConn.CloseNow()
		return nil, 0, nil, fmt.Errorf("reading Connected from AP: %w", err)
	}

	// Forward the Connected message to the client
	if s.lokiLogger != nil {
		for _, raw := range response {
			s.lokiLogger.Log(connectMsg.Name, LogSourceServer, raw)
		}
	}
	if err := wsjson.Write(ctx, connState.clientConn, response); err != nil {
		apConn.CloseNow()
		return nil, 0, nil, fmt.Errorf("forwarding Connected to client: %w", err)
	}

	slotId := 0
	game := ""
	for _, raw := range response {
		var msg struct {
			Cmd      string `json:"cmd"`
			Slot     int    `json:"slot"`
			SlotInfo map[string]struct {
				Game string `json:"game"`
			} `json:"slot_info"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil || msg.Cmd != "Connected" {
			continue
		}
		slotId = msg.Slot
		if slotData, ok := msg.SlotInfo[fmt.Sprintf("%d", slotId)]; ok {
			game = slotData.Game
		}
	}

	// Proxy AP -> client

	// Avoid any processing on these packets where possible

	go func() {
		defer apConn.CloseNow()
		defer connState.cancel()
		for {
			msgType, data, err := apConn.Read(ctx)
			if err != nil {
				if !isNormalClose(err) && ctx.Err() == nil {
					s.logf("AP read error: %v", err)
				}
				return
			}

			if s.lokiLogger != nil {
				var decodedMsgs []map[string]any
				if err := json.Unmarshal(data, &decodedMsgs); err == nil {
					for _, decodedMsg := range decodedMsgs {
						raw, err := json.Marshal(decodedMsg)
						if err != nil {
							continue
						}
						s.lokiLogger.Log(*connState.slotName, LogSourceServer, raw)
					}
				} else {
					s.logf("failed to unmarshal server message: %v", err)
				}
			}

			if err := connState.clientConn.Write(ctx, msgType, data); err != nil {
				if ctx.Err() == nil {
					s.logf("client write error: %v", err)
				}
				return
			}
		}
	}()

	return apConn, slotId, &game, nil
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
