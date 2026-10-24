package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ServerRunner struct{}

type CommandContext struct {
	Write func(text string)
	// Only set for client commands
	connState *connectionState
	// Only set for server commands
	serverRunner *ServerRunner
}

func (c *CommandContext) IsClient() bool {
	return c.connState != nil
}
func (c *CommandContext) IsAdmin() bool {
	if c.IsClient() {
		return c.connState.adminAuthed.Load()
	} else {
		return true
	}
}
func (c *CommandContext) IsServer() bool {
	return c.serverRunner != nil
}

type commandHandler func(ctx context.Context, cmdCtx *CommandContext, args string) error

type chatCommand struct {
	handler     commandHandler
	name        string
	description string
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

type CommandRouter struct {
	client *chatCommandRouter
	server *chatCommandRouter
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

func (r *chatCommandRouter) Handle(ctx context.Context, commandCtx *CommandContext, rest string) (bool, error) {
	// Top-level commands
	for _, cmd := range r.topLevel {
		if rest == cmd.name || strings.HasPrefix(rest, cmd.name+" ") {
			args := strings.TrimSpace(strings.TrimPrefix(rest, cmd.name))
			return true, cmd.handler(ctx, commandCtx, args)
		}
	}

	// Group sub-commands
	for _, group := range r.groups {
		if rest == group.prefix || strings.HasPrefix(rest, group.prefix+" ") {
			sub := strings.TrimSpace(strings.TrimPrefix(rest, group.prefix))
			sub = strings.TrimPrefix(sub, "/") // Mostly for !admin compat with python multiserver, e.g !admin release and !admin /release
			for _, cmd := range group.commands {
				if sub == cmd.name || strings.HasPrefix(sub, cmd.name+" ") {
					args := strings.TrimSpace(strings.TrimPrefix(sub, cmd.name))
					return true, cmd.handler(ctx, commandCtx, args)
				}
			}
			// Matched group prefix but no sub-command — show group help
			return true, errors.New(group.helpText())
		}
	}

	return false, nil
}

func newCommandRouter(s *ApxRoom) *CommandRouter {
	clientRouter := &chatCommandRouter{}
	serverRouter := &chatCommandRouter{}

	clientRouter.AddTopLevel(
		chatCommand{
			name:        "help",
			description: "Show available commands",
			handler: func(ctx context.Context, commandCtx *CommandContext, args string) error {
				commandCtx.Write(clientRouter.HelpText())
				return nil
			},
		},
	)
	clientRouter.AddTopLevel(
		chatCommand{
			name:        "hint_location",
			description: "Spend hint points for a location in your game",
			handler:     s.handleHintLocationCommand,
		},
		chatCommand{
			name:        "hint",
			description: "Spend hint points for an item in your game",
			handler:     s.handleHintItemCommand,
		},
	)
	clientRouter.AddTopLevel(
		chatCommand{
			name:        "countdown",
			description: "Start a countdown (default: 10 seconds)",
			handler:     s.handleCountdownCommand,
		},
		chatCommand{
			name:        "getitem",
			description: "Cheat in an item for your game",
			handler:     s.handleGetItemCommand,
		},
		chatCommand{
			name:        "collect",
			description: "Collect all remaining items for your game",
			handler:     s.handleCollectCommand,
		},
		chatCommand{
			name:        "release",
			description: "Release all remaining items from your game",
			handler:     s.handleReleaseCommand,
		},
	)
	clientRouter.AddTopLevel(
		chatCommand{
			name:        "players",
			description: "List all players in the room",
			handler:     s.handlePlayersCommand,
		},
	)

	clientRouter.AddGroup(&chatCommandGroup{
		prefix:      "admin",
		description: "Admin commands (must be !admin login <password> first)",
		commands: []chatCommand{
			{
				name:        "login",
				description: "Login with password",
				handler:     s.handleAdminLoginCommand,
			},
			{
				name:        "logout",
				description: "Logout",
				handler:     handleAuthedCommand(s.handleAdminLogoutCommand),
			},
			{
				name:        "release",
				description: "Release a players items",
				handler:     handleAuthedCommand(s.handleAdminReleaseCommand),
			},
			{
				name:        "send",
				description: "Send an item to a player",
				handler:     handleAuthedCommand(s.handleAdminSendCommand),
			},
			{
				name:        "send_location",
				description: "Send a check for a player",
				handler:     handleAuthedCommand(s.handleAdminSendLocationCommand),
			},
			{
				name:        "hint",
				description: "Hint an item for a player",
				handler:     handleAuthedCommand(s.handleAdminHintCommand),
			},
			{
				name:        "hint_location",
				description: "Hint a location for a player",
				handler:     handleAuthedCommand(s.handleAdminHintLocationCommand),
			},
		},
	})

	clientRouter.AddGroup(&chatCommandGroup{
		prefix:      "apx",
		description: "APX specific commands",
		commands: []chatCommand{
			{
				name:        "status",
				description: "Show room status",
				handler:     s.handleRoomStatusCommand,
			},
		},
	})

	serverRouter.AddTopLevel(
		chatCommand{
			name:        "help",
			description: "Show available commands",
			handler: func(ctx context.Context, commandCtx *CommandContext, args string) error {
				commandCtx.Write(serverRouter.HelpText())
				return nil
			},
		},
		chatCommand{
			name:        "countdown",
			description: "Start a countdown (default: 10 seconds)",
			handler:     s.handleCountdownCommand,
		},
		chatCommand{
			name:        "release",
			description: "Release a players items",
			handler:     handleAuthedCommand(s.handleAdminReleaseCommand),
		},
		chatCommand{
			name:        "send",
			description: "Send an item to a player",
			handler:     handleAuthedCommand(s.handleAdminSendCommand),
		},
		chatCommand{
			name:        "send_location",
			description: "Send a check for a player",
			handler:     handleAuthedCommand(s.handleAdminSendLocationCommand),
		},
		chatCommand{
			name:        "hint",
			description: "Hint an item for a player",
			handler:     handleAuthedCommand(s.handleAdminHintCommand),
		},
		chatCommand{
			name:        "hint_location",
			description: "Hint a location for a player",
			handler:     handleAuthedCommand(s.handleAdminHintLocationCommand),
		},
	)

	serverRouter.AddGroup(&chatCommandGroup{
		prefix:      "apx",
		description: "APX specific commands",
		commands: []chatCommand{
			{
				name:        "status",
				description: "Show room status",
				handler:     s.handleRoomStatusCommand,
			},
		},
	})

	router := CommandRouter{
		client: clientRouter,
		server: serverRouter,
	}
	return &router
}

func (s *ApxRoom) handleRoomStatusCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	if commandCtx.IsClient() {
		commandCtx.Write(s.StatusString(commandCtx.connState.registeredClient))
	} else {
		commandCtx.Write(s.StatusString(nil))
	}
	return nil
}

func (s *ApxRoom) handleHintLocationCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	if !commandCtx.IsClient() {
		return fmt.Errorf("Command supported on clients only")
	}

	client := commandCtx.connState.registeredClient
	locationName := strings.TrimSpace(args)

	if locationName == "" {
		return fmt.Errorf("Usage: !hint_location <location name>")
	}

	teamSlot := TeamSlot{Team: client.Team, Slot: client.Slot}

	// Resolve location name -> ID from datapackage
	game := *client.game
	locID, err := s.datapackages.GetLocationID(game, locationName)
	if err != nil {
		return fmt.Errorf("Unknown location: %q", locationName)
	}

	// Check location belongs to this slot
	if _, exists := s.state.Locations[teamSlot][locID]; !exists {
		return fmt.Errorf("Location %q does not exist in your world", locationName)
	}

	// Skip if hint already exists — no cost
	existing := s.state.Hints.GetSlotHints(teamSlot)
	for _, h := range existing {
		if int(h.FindingPlayer) == client.Slot && h.Location == locID {
			hint := formatHintMessage(h)
			s.broadcastPrintJson([]PrintJsonMessage{hint})
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
		Location:        locID,
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
		// Send own clients hint points update
		newPoints := s.state.GetSlotRemainingPoints(TeamSlot{client.Team, client.Slot})
		clients := s.connections.GetSlotClients(teamSlot)

		err = enqueueMessage([]any{RoomUpdateMessage{
			HintPoints: &newPoints,
		}}, clients...)
		if err != nil {
			return err
		}

		s.broadcastPrintJson([]PrintJsonMessage{formatHintMessage(hint)})
	}

	return nil
}

func (s *ApxRoom) handleHintItemCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	if !commandCtx.IsClient() {
		return fmt.Errorf("Command supported on clients only")
	}

	client := commandCtx.connState.registeredClient
	itemName := strings.TrimSpace(args)

	if itemName == "" {
		return fmt.Errorf("Usage: !hint <item name>")
	}

	teamSlot := TeamSlot{Team: client.Team, Slot: client.Slot}
	game := *client.game

	itemID, ok := s.datapackages.GetItemID(game, itemName)
	if !ok {
		return fmt.Errorf("Unknown item: %q", itemName)
	}

	cost := s.state.GetSlotHintCost(teamSlot)
	points := s.state.GetSlotRemainingPoints(teamSlot)
	hints, newHints := s.state.Hints.CollectItemHint(teamSlot, int32(client.Slot), itemID, &s.state.SphereLocs, &s.state.Checks, cost, points)

	// Had points to spend, never found a hint
	if (cost <= 0 || points >= cost) && len(hints) == 0 {
		commandCtx.Write(fmt.Sprintf("No locations found containing %q", itemName))
		return nil
	}

	if len(newHints) > 0 {
		// Send own clients hint points update
		newPoints := s.state.GetSlotRemainingPoints(TeamSlot{client.Team, client.Slot})
		clients := s.connections.GetSlotClients(teamSlot)

		err := enqueueMessage([]any{RoomUpdateMessage{
			HintPoints: &newPoints,
		}}, clients...)
		if err != nil {
			return err
		}

		// Notify listeners of change
		affectedSlots := make(map[TeamSlot]struct{})
		for _, hint := range newHints {
			affectedSlots[TeamSlot{0, int(hint.FindingPlayer)}] = struct{}{}
			if hint.ReceivingPlayer != hint.FindingPlayer {
				affectedSlots[TeamSlot{0, int(hint.ReceivingPlayer)}] = struct{}{}
			}
		}
		s.notifyHintsChanged(affectedSlots)
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

	if cost > 0 && points < cost {
		text := fmt.Sprintf("A hint costs %d points. You have %d points.", cost, points)
		msgs = append(msgs, formatGenericMessage(&client.Team, &client.Slot, "CommandResult", text))
	} else if len(newHints) == 0 {
		text := fmt.Sprintf("A hint costs %d points. You have %d points.", cost, points)
		msgs = append(msgs, formatGenericMessage(&client.Team, &client.Slot, "CommandResult", text))
	}

	if len(msgs) > 0 {
		s.broadcastPrintJson(msgs)
	}

	if cost > 0 && points < cost {
		text := fmt.Sprintf("A hint costs %d points. You have %d points.", cost, points)
		commandCtx.Write(text)
	} else if len(newHints) == 0 {
		text := "No more copies of this item left to find."
		commandCtx.Write(text)
	}
	return nil
}

func (s *ApxRoom) handleCountdownCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	allowed := s.state.ServerOptions.CountdownAllowed(len(s.state.SlotInfo))

	if !allowed && !commandCtx.IsAdmin() {
		return fmt.Errorf("Not allowed.")
	}

	seconds := 10
	if args != "" {
		var err error
		seconds, err = strconv.Atoi(args)
		if err != nil {
			return fmt.Errorf("Invalid value for seconds")
		}
	}

	allTargets := s.connections.GetAllClients()

	startingText := fmt.Sprintf("[Server]: Starting countdown of %ds", seconds)
	startingMsg := []PrintJsonMessage{
		{
			Type: "Countdown",
			Data: []JsonMessagePart{
				{
					Text: startingText,
				},
			},
			Countdown: &seconds,
		},
	}
	enqueueMessage(startingMsg, allTargets...)

	go func() {
		for second := seconds; second > 0; second-- {
			time.Sleep(1 * time.Second)
			s := second
			secondText := fmt.Sprintf("[Server]: %d", s)
			secondMsg := []PrintJsonMessage{
				{
					Type:      "Countdown",
					Data:      []JsonMessagePart{{Text: secondText}},
					Countdown: &s,
				},
			}
			enqueueMessage(secondMsg, allTargets...)
		}

		time.Sleep(1 * time.Second)
		finalText := "[Server]: GO"
		finalSecond := 0
		finalMsg := []PrintJsonMessage{
			{
				Type:      "Countdown",
				Data:      []JsonMessagePart{{Text: finalText}},
				Countdown: &finalSecond,
			},
		}
		enqueueMessage(finalMsg, allTargets...)
	}()

	return nil
}

func (s *ApxRoom) handleGetItemCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	if !commandCtx.IsClient() {
		return fmt.Errorf("Command supported on clients only")
	}

	client := commandCtx.connState.registeredClient

	allowed := s.state.ServerOptions.ItemCheatAllowed()

	if !allowed {
		return fmt.Errorf("Sorry, item cheating has been disabled on this server. You can ask the server admin to enable item cheating or use /send")
	}

	itemID, ok := s.datapackages.GetItemID(*client.game, args)
	if !ok {
		return fmt.Errorf("Item not found for '%s'", args)
	}

	newItem := NetworkItem{itemID, -1, int32(client.Slot), 0}
	s.state.ReceivedItems.Add(client.Team, client.Slot, []NetworkItem{newItem})

	text := fmt.Sprintf("Cheat console: sending %q to %s", args, *client.slotName)

	msg := formatGenericMessageWithItem(
		&client.Team,
		&client.Slot,
		"ItemCheat",
		text,
		&newItem,
	)

	s.broadcastPrintJson([]PrintJsonMessage{msg})

	return nil
}

func (s *ApxRoom) handleCollectCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	if !commandCtx.IsClient() {
		return fmt.Errorf("Command supported on clients only")
	}

	client := commandCtx.connState.registeredClient

	cm := s.state.ServerOptions.GetCollectMode()
	status := s.state.ClientStatuses.Get(client.Team, client.Slot)

	if cm == PermissionEnabled || cm == PermissionAutoEnabled {
		s.CollectPlayer(client.Team, client.Slot)
		return nil
	}
	if cm == PermissionDisabled {
		return fmt.Errorf("Sorry, client item collecting has been disabled on this server. You can ask the server admin for a /collect")
	}
	if cm == PermissionGoal && status == ClientStatusGoal {
		s.CollectPlayer(client.Team, client.Slot)
		return nil
	}

	return nil
}

func (s *ApxRoom) handleReleaseCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	if !commandCtx.IsClient() {
		return fmt.Errorf("Command supported on clients only")
	}

	client := commandCtx.connState.registeredClient

	rm := s.state.ServerOptions.GetReleaseMode()
	status := s.state.ClientStatuses.Get(client.Team, client.Slot)

	if rm == PermissionEnabled || rm == PermissionAutoEnabled {
		s.ReleasePlayer(client.Team, client.Slot)
		return nil
	}
	if rm == PermissionDisabled {
		return fmt.Errorf("Sorry, client item releasing has been disabled on this server. You can ask the server admin for a /release")
	}
	if rm == PermissionGoal && status == ClientStatusGoal {
		s.ReleasePlayer(client.Team, client.Slot)
		return nil
	}

	return nil
}

func (s *ApxRoom) handlePlayersCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	s.connections.mu.RLock()
	numSlotsConnected := len(s.connections.clients)
	s.connections.mu.RUnlock()

	playerNames := make([]string, 0, len(s.state.SlotInfo))
	for _, ts := range s.state.SlotInfo {
		if ts.Type == SlotTypePlayer {
			playerNames = append(playerNames, ts.Name)
		}
	}

	text := fmt.Sprintf("%d players of %d connected: %s", numSlotsConnected, len(playerNames), strings.Join(playerNames, ", "))
	commandCtx.Write(text)
	return nil
}

func (s *ApxRoom) handleAdminLoginCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	if !commandCtx.IsClient() {
		return fmt.Errorf("Command supported on clients only")
	}

	authed := commandCtx.connState.adminAuthed.Load()
	if authed {
		text := "You are already authenticated as admin"
		commandCtx.Write(text)
		return nil
	}

	allowed := s.state.ServerOptions.AdminPasswordCheck(&args)
	if !allowed {
		return fmt.Errorf("Entered password was incorrect, or there is none set")
	}

	commandCtx.connState.adminAuthed.Store(true)
	text := "Login successful. You can now issue server side commands."
	commandCtx.Write(text)
	return nil
}

func (s *ApxRoom) handleAdminLogoutCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	if !commandCtx.IsClient() {
		return fmt.Errorf("Command supported on clients only")
	}

	commandCtx.connState.adminAuthed.Store(false)

	text := "Logout successful. You can no longer issue server side commands."
	commandCtx.Write(text)

	return nil
}

func (s *ApxRoom) handleAdminOptionCommand(_ context.Context, commandCtx *CommandContext, args string) error {
	argsList := splitArgs(args)
	if len(argsList) < 2 {
		return fmt.Errorf("Usage: !admin option <option> <value>\nOptions list: password, server_password, hint_cost")
	}

	option := argsList[0]
	value := strings.Join(argsList[1:], " ")

	switch option {
	case "password":
		newPassword := processNewPassword(value)
		s.state.ServerOptions.SetPassword(newPassword)
		s.buildRoomInfoFromState()
	case "server_password", "admin_password":
		newPassword := processNewPassword(value)
		s.state.ServerOptions.SetAdminPassword(newPassword)
	case "hint_cost":
		hintCost, err := strconv.Atoi(value)
		if err != nil || hintCost < 0 || hintCost > 100 {
			return fmt.Errorf("Usage: !admin option hint_cost <0-100>")
		}
		s.state.ServerOptions.SetHintCost(hintCost)
		s.buildRoomInfoFromState()
		s.BroadcastRoomUpdateHintCost()
	case "location_check_points":
		locationCheckPoints, err := strconv.Atoi(value)
		if err != nil || locationCheckPoints < 0 || locationCheckPoints > 100 {
			return fmt.Errorf("Usage: !admin option location_check_points <0-100>")
		}
		s.state.ServerOptions.SetLocationCheckPoints(locationCheckPoints)
		s.buildRoomInfoFromState()
		s.BroadcastRoomUpdateLocationCheckPoints()
	case "release_mode":
		mode, valid := permissionFromString(value)
		if !valid {
			return fmt.Errorf("Usage: !admin option release_mode <enabled|goal|auto|auto-enabled|disabled>")
		}
		s.state.ServerOptions.SetReleaseMode(mode)
		s.buildRoomInfoFromState()
		s.BroadcastRoomUpdatePermissions()
	case "collect_mode":
		mode, valid := permissionFromString(value)
		if !valid {
			return fmt.Errorf("Usage: !admin option collect_mode <enabled|goal|auto|auto-enabled|disabled>")
		}
		s.state.ServerOptions.SetReleaseMode(mode)
		s.buildRoomInfoFromState()
		s.BroadcastRoomUpdatePermissions()
	case "remaining_mode":
		mode, valid := permissionFromString(value)
		if !valid || mode == PermissionAuto || mode == PermissionAutoEnabled {
			return fmt.Errorf("Usage: !admin option remaining_mode <enabled|goal|disabled>")
		}
		s.state.ServerOptions.SetRemainingMode(mode)
		s.buildRoomInfoFromState()
		s.BroadcastRoomUpdatePermissions()
	case "countdown_mode":
		mode, valid := permissionFromString(value)
		if !valid || (mode != PermissionEnabled && mode != PermissionAuto && mode != PermissionDisabled) {
			return fmt.Errorf("Usage: !admin option countdown_mode <enabled|auto|disabled>")
		}
		s.state.ServerOptions.SetCountdownMode(mode)
		// Weird this isn't in room info
	case "item_cheat":
		mode, valid := permissionFromString(value)
		if !valid || (mode != PermissionEnabled && mode != PermissionDisabled) {
			return fmt.Errorf("Usage: !admin option item_cheat <enabled|disabled>")
		}
		s.state.ServerOptions.SetItemCheatAllowed(mode == PermissionEnabled)
		// Weird this isn't in room info
	case "disable_item_cheat":
		// Mostly for compat
		mode, valid := permissionFromString(value)
		if !valid || (mode != PermissionEnabled && mode != PermissionDisabled) {
			return fmt.Errorf("Usage: !admin option disable_item_cheat <enabled|disabled>")
		}
		s.state.ServerOptions.SetItemCheatAllowed(mode == PermissionDisabled)
		// Weird this isn't in room info
	default:
		return fmt.Errorf("Available options: password, server_password, hint_cost, location_check_points, release_mode, collect_mode, remaining_mode, countdown_mode, item_cheat, disable_item_cheat")
	}

	commandCtx.Write(fmt.Sprintf("Changed '%s' to '%s'", option, value))

	return nil
}

func (s *ApxRoom) handleAdminReleaseCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	slotInfo, ok := s.state.NameToSlot[args]
	if !ok {
		return fmt.Errorf("No player found for name '%s'", args)
	}

	text := fmt.Sprintf("Releasing player '%s' (team %d - slot %d)", args, slotInfo.Team, slotInfo.Slot)
	commandCtx.Write(text)

	locIds := make([]int64, 0, len(s.state.Locations[TeamSlot{slotInfo.Team, slotInfo.Slot}]))
	for locId := range s.state.Locations[TeamSlot{slotInfo.Team, slotInfo.Slot}] {
		locIds = append(locIds, locId)
	}
	s.RegisterChecks(context.Background(), TeamSlot{slotInfo.Team, slotInfo.Slot}, locIds)
	return nil
}

func (s *ApxRoom) handleAdminSendCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	argsList := splitArgs(args)
	if len(argsList) < 2 {
		return fmt.Errorf("Usage: !admin send <player> <item>")
	}

	player := argsList[0]
	item := strings.Join(argsList[1:], " ")

	slotInfo, ok := s.state.NameToSlot[player]
	if !ok {
		return fmt.Errorf("Player '%s' not found", player)
	}
	itemId, ok := s.datapackages.ItemNameToID[slotInfo.Game][item]
	if !ok {
		return fmt.Errorf("Item '%s' not found for game %s", item, slotInfo.Game)
	}

	networkItem := NetworkItem{
		Item:     itemId,
		Location: -1,
		Player:   0,
		Flags:    0,
	}

	s.state.ReceivedItems.Add(slotInfo.Team, slotInfo.Slot, []NetworkItem{networkItem})

	text := fmt.Sprintf("Cheat console: sending %q to %s", item, player)

	msg := formatGenericMessageWithItem(
		&slotInfo.Team,
		&slotInfo.Slot,
		"ItemCheat",
		text,
		&networkItem,
	)

	s.broadcastPrintJson([]PrintJsonMessage{msg})

	// Make sure requester always gets the feedback too
	commandCtx.Write(text)

	return nil
}

func (s *ApxRoom) handleAdminSendLocationCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	argsList := splitArgs(args)
	if len(argsList) < 2 {
		return fmt.Errorf("Usage: !admin send_location <player> <location>")
	}

	player := argsList[0]
	location := strings.Join(argsList[1:], " ")

	slotInfo, ok := s.state.NameToSlot[player]
	if !ok {
		return fmt.Errorf("Player '%s' not found", player)
	}
	locId, ok := s.datapackages.LocationNameToID[slotInfo.Game][location]
	if !ok {
		return fmt.Errorf("Location '%s' not found for game %s", location, slotInfo.Game)
	}

	s.RegisterChecks(context.Background(), TeamSlot{slotInfo.Team, slotInfo.Slot}, []int64{locId})

	return nil
}

func (s *ApxRoom) handleAdminHintCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	argsList := splitArgs(args)
	if len(argsList) < 2 {
		return fmt.Errorf("Usage: !admin hint <player> <item>")
	}

	player := argsList[0]
	item := strings.Join(argsList[1:], " ")

	slotInfo, ok := s.state.NameToSlot[player]
	if !ok {
		return fmt.Errorf("Player '%s' not found", player)
	}
	itemId, ok := s.datapackages.ItemNameToID[slotInfo.Game][item]
	if !ok {
		return fmt.Errorf("Item '%s' not found for game %s", item, slotInfo.Game)
	}

	teamSlot := TeamSlot{Team: slotInfo.Team, Slot: slotInfo.Slot}
	_, newHints := s.state.Hints.CollectItemHint(teamSlot, int32(slotInfo.Slot), itemId, &s.state.SphereLocs, &s.state.Checks, 0, 0)

	if len(newHints) > 0 {
		// Notify listeners of change
		affectedSlots := make(map[TeamSlot]struct{})
		for _, hint := range newHints {
			affectedSlots[TeamSlot{0, int(hint.FindingPlayer)}] = struct{}{}
			if hint.ReceivingPlayer != hint.FindingPlayer {
				affectedSlots[TeamSlot{0, int(hint.ReceivingPlayer)}] = struct{}{}
			}
		}
		s.notifyHintsChanged(affectedSlots)
	}

	return nil
}

func (s *ApxRoom) handleAdminHintLocationCommand(ctx context.Context, commandCtx *CommandContext, args string) error {
	argsList := splitArgs(args)
	if len(argsList) < 2 {
		return fmt.Errorf("Usage: !admin hint_location <player> <location>")
	}

	player := argsList[0]
	location := strings.Join(argsList[1:], " ")

	slotInfo, ok := s.state.NameToSlot[player]
	if !ok {
		return fmt.Errorf("Player '%s' not found", player)
	}
	locId, ok := s.datapackages.LocationNameToID[slotInfo.Game][location]
	if !ok {
		return fmt.Errorf("Location '%s' not found for game %s", location, slotInfo.Game)
	}

	teamSlot := TeamSlot{slotInfo.Team, slotInfo.Slot}
	existing := s.state.Hints.GetSlotHints(teamSlot)
	for _, h := range existing {
		if int(h.FindingPlayer) == slotInfo.Slot && h.Location == locId {
			return nil
		}
	}

	loc := s.state.Locations[teamSlot][locId]
	found := s.state.Checks.IsChecked(teamSlot, locId)

	status := HintStatusUnspecified
	if found {
		status = HintStatusFound
	}

	hint := Hint{
		ReceivingPlayer: int32(loc.Player),
		FindingPlayer:   int32(slotInfo.Slot),
		Location:        locId,
		Item:            loc.Item,
		ItemFlags:       loc.Flags,
		Found:           found,
		Status:          status,
	}

	s.state.Hints.AddPaidSlotHint(ctx, teamSlot, hint, nil, 0, 0)

	return nil
}

func handleAuthedCommand(handler commandHandler) commandHandler {
	return func(ctx context.Context, commandCtx *CommandContext, args string) error {
		if commandCtx.IsClient() {
			authed := commandCtx.connState.adminAuthed.Load()
			if !authed {
				return fmt.Errorf("You must first login using !admin login <password>")
			}
		}

		return handler(ctx, commandCtx, args)
	}
}

// Return list of args with respect to quotes around args with spaces
func splitArgs(args string) []string {
	quotesOpen := false
	argsList := make([]string, 0)

	// Find next space or quote
	runningArg := ""
	for _, arg := range strings.Split(args, " ") {
		if len(arg) > 0 {
			if !quotesOpen && strings.HasPrefix(arg, "\"") {
				quotesOpen = true
				runningArg = arg[1:] + " "
			} else if quotesOpen {
				runningArg += arg + " "
			} else {
				argsList = append(argsList, arg)
			}
			if quotesOpen && strings.HasSuffix(arg, "\"") {
				quotesOpen = false
				cleanArg := strings.TrimSpace(runningArg)
				cleanArg = cleanArg[:len(cleanArg)-1]
				argsList = append(argsList, cleanArg)
				runningArg = ""
			}
		}
	}

	// Unclosed quote :(
	if len(runningArg) > 0 {
		cleanArg := strings.TrimSpace(runningArg)
		argsList = append(argsList, cleanArg)
	}

	return argsList
}

func valueToBool(s string) bool {
	switch strings.ToLower(s) {
	case "off", "0", "false", "none", "null", "no":
		return false
	default:
		return true
	}
}

func processNewPassword(s string) string {
	switch s {
	case "null", "none", "", "''", `""`:
		return ""
	default:
		return s
	}
}
