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
	prefix   string
	commands []chatCommand
}

func newCommandGroup(prefix string, commands []chatCommand) *chatCommandGroup {
	return &chatCommandGroup{prefix: prefix, commands: commands}
}

func (g *chatCommandGroup) Handle(ctx context.Context, connState *connectionState, text string) (bool, error) {
	if !strings.HasPrefix(text, "!"+g.prefix) {
		return false, nil
	}

	rest := strings.TrimSpace(strings.TrimPrefix(text, "!"+g.prefix))

	for _, cmd := range g.commands {
		if rest == cmd.name || strings.HasPrefix(rest, cmd.name+" ") {
			args := strings.TrimSpace(strings.TrimPrefix(rest, cmd.name))
			return true, cmd.handler(ctx, connState, args)
		}
	}

	// No match — show help
	lines := make([]string, 0, len(g.commands)+1)
	lines = append(lines, fmt.Sprintf("=== %s Commands ===", strings.ToUpper(g.prefix)))
	for _, cmd := range g.commands {
		lines = append(lines, fmt.Sprintf("!%s %s - %s", g.prefix, cmd.name, cmd.description))
	}
	SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.slotId, strings.Join(lines, "\n"))
	return true, nil
}

func (s ApxRoom) apxCommandGroup() *chatCommandGroup {
	return newCommandGroup("apx", []chatCommand{
		{
			name:        "status",
			description: "Show room status",
			handler: func(ctx context.Context, connState *connectionState, args string) error {
				SendChatMessageToClient(ctx, connState.clientConn, connState.registeredClient.slotId, s.StatusString(connState.registeredClient))
				return nil
			},
		},
	})
}
