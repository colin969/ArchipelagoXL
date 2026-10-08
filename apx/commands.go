package main

import (
	"context"
	"fmt"
	"strings"
)

type chatCommand struct {
	name        string
	description string
	handler     func(ctx context.Context, connState *connectionState, args string) error
}

type chatCommandGroup struct {
	prefix      string
	description string
	commands    []chatCommand
}

func (g *chatCommandGroup) helpText() string {
	lines := make([]string, 0, len(g.commands)+1)
	lines = append(lines, fmt.Sprintf("=== %s Commands ===", strings.ToUpper(g.prefix)))
	for _, cmd := range g.commands {
		lines = append(lines, fmt.Sprintf("!%s %s - %s", g.prefix, cmd.name, cmd.description))
	}
	return strings.Join(lines, "\n")
}

type chatCommandRouter struct {
	topLevel []chatCommand
	groups   []*chatCommandGroup
}

func (r *chatCommandRouter) AddTopLevel(cmds ...chatCommand) *chatCommandRouter {
	r.topLevel = append(r.topLevel, cmds...)
	return r
}

func (r *chatCommandRouter) AddGroup(group *chatCommandGroup) *chatCommandRouter {
	r.groups = append(r.groups, group)
	return r
}

func (r *chatCommandRouter) HelpText() string {
	lines := []string{"=== Available Commands ==="}
	for _, cmd := range r.topLevel {
		lines = append(lines, fmt.Sprintf("!%s - %s", cmd.name, cmd.description))
	}
	for _, group := range r.groups {
		lines = append(lines, fmt.Sprintf("!%s - %s", group.prefix, group.description))
	}
	return strings.Join(lines, "\n")
}

func (r *chatCommandRouter) Handle(ctx context.Context, connState *connectionState, text string) (bool, error) {
	if !strings.HasPrefix(text, "!") {
		return false, nil
	}

	rest := strings.TrimSpace(strings.TrimPrefix(text, "!"))

	// Top-level commands
	for _, cmd := range r.topLevel {
		if rest == cmd.name || strings.HasPrefix(rest, cmd.name+" ") {
			args := strings.TrimSpace(strings.TrimPrefix(rest, cmd.name))
			return true, cmd.handler(ctx, connState, args)
		}
	}

	// Group sub-commands
	for _, group := range r.groups {
		if rest == group.prefix || strings.HasPrefix(rest, group.prefix+" ") {
			sub := strings.TrimSpace(strings.TrimPrefix(rest, group.prefix))
			for _, cmd := range group.commands {
				if sub == cmd.name || strings.HasPrefix(sub, cmd.name+" ") {
					args := strings.TrimSpace(strings.TrimPrefix(sub, cmd.name))
					return true, cmd.handler(ctx, connState, args)
				}
			}
			// Matched group prefix but no sub-command — show group help
			SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.Slot, group.helpText())
			return true, nil
		}
	}

	return false, nil
}

func newCommandRouter(s *ApxRoom) *chatCommandRouter {
	r := &chatCommandRouter{}

	r.AddTopLevel(
		chatCommand{
			name:        "help",
			description: "Show available commands",
			handler: func(ctx context.Context, connState *connectionState, args string) error {
				SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.Slot, r.HelpText())
				return nil
			},
		},
	)
	r.AddTopLevel(
		chatCommand{
			name:        "hint_location",
			description: "Spend hint points for a location in your game",
			handler:     s.handleHintLocationCommand,
		},
	)
	r.AddTopLevel(
		chatCommand{
			name:        "hint",
			description: "Spend hint points for an item in your game",
			handler:     s.handleHintItemCommand,
		},
	)

	r.AddGroup(&chatCommandGroup{
		prefix:      "apx",
		description: "APX specific commands",
		commands: []chatCommand{
			{
				name:        "status",
				description: "Show room status",
				handler: func(ctx context.Context, connState *connectionState, args string) error {
					SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.Slot,
						s.StatusString(connState.registeredClient))
					return nil
				},
			},
		},
	})

	return r
}

func (s *ApxRoom) handleHintLocationCommand(ctx context.Context, connState *connectionState, args string) error {
	client := connState.registeredClient
	locationName := strings.TrimSpace(args)

	if locationName == "" {
		SendChatMessageToClient(ctx, connState.clientConn, client.Slot,
			"Usage: !hint_location <location name>")
		return nil
	}

	teamSlot := TeamSlot{Team: client.Team, Slot: client.Slot}

	// Resolve location name -> ID from datapackage
	game := *client.game
	locID, err := s.datapackages.GetLocationID(game, locationName)
	if err != nil {
		SendChatMessageToClient(ctx, connState.clientConn, client.Slot,
			fmt.Sprintf("Unknown location: %q", locationName))
		return nil
	}

	// Check location belongs to this slot
	if _, exists := s.state.Locations[teamSlot][locID]; !exists {
		SendChatMessageToClient(ctx, connState.clientConn, client.Slot,
			fmt.Sprintf("Location %q does not exist in your world", locationName))
		return nil
	}

	// Skip if hint already exists — no cost
	existing := s.state.Hints.GetSlotHints(teamSlot)
	for _, h := range existing {
		if int(h.FindingPlayer) == client.Slot && int(h.Location) == locID {
			hint := formatHintMessage(h)
			s.broadcastPrintJson(ctx, []PrintJsonMessage{hint})
			return nil
		}
	}

	loc := s.state.Locations[teamSlot][locID]
	found := s.state.Checks.IsChecked(teamSlot, locID)

	status := HintStatusUnspecified
	if found {
		status = HintStatusFound
	}

	hint := Hint{
		ReceivingPlayer: int32(loc.Player),
		FindingPlayer:   int32(client.Slot),
		Location:        int32(locID),
		Item:            loc.Item,
		ItemFlags:       loc.Flags,
		Found:           found,
		Status:          status,
	}

	cost := s.state.GetSlotHintCost(teamSlot)
	points := s.state.GetSlotRemainingPoints(teamSlot)

	// Store bidirectionally
	hint, allowed := s.state.Hints.AddPaidSlotHint(ctx, teamSlot, hint, client, cost, points)
	if allowed {
		s.broadcastPrintJson(ctx, []PrintJsonMessage{formatHintMessage(hint)})
	}
	return nil
}

func (s *ApxRoom) handleHintItemCommand(ctx context.Context, connState *connectionState, args string) error {
	client := connState.registeredClient
	itemName := strings.TrimSpace(args)

	if itemName == "" {
		SendChatMessageToClient(ctx, connState.clientConn, client.Slot,
			"Usage: !hint <item name>")
		return nil
	}

	teamSlot := TeamSlot{Team: client.Team, Slot: client.Slot}
	game := *client.game

	itemID, err := s.datapackages.GetItemID(game, itemName)
	if err != nil {
		SendChatMessageToClient(ctx, connState.clientConn, client.Slot,
			fmt.Sprintf("Unknown item: %q", itemName))
		return nil
	}

	cost := s.state.GetSlotHintCost(teamSlot)
	points := s.state.GetSlotRemainingPoints(teamSlot)
	hints := s.state.Hints.CollectItemHint(teamSlot, int16(client.Slot), int32(*itemID), &s.state.SphereLocs, cost, points)

	if len(hints) == 0 {
		SendChatMessageToClient(ctx, connState.clientConn, client.Slot,
			fmt.Sprintf("No locations found containing %q", itemName))
		return nil
	}

	if len(hints) == 0 {
		SendChatMessageToClient(ctx, connState.clientConn, client.Slot,
			fmt.Sprintf("No locations found containing %q", itemName))
		return nil
	}

	var msgs []PrintJsonMessage
	for _, hint := range hints {
		if s.state.Hints.HintExists(hint.FindingPlayer, hint.Location) {
			msgs = append(msgs, formatHintMessage(hint))
			continue
		}
		stored, allowed := s.state.Hints.AddPaidSlotHint(ctx, teamSlot, hint, client, cost, points)
		if allowed {
			msgs = append(msgs, formatHintMessage(stored))
		}
	}

	if len(msgs) > 0 {
		s.broadcastPrintJson(ctx, msgs)
	}
	return nil
}
