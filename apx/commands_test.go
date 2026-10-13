package main

import (
	"testing"
)

func TestCommands(t *testing.T) {
	t.Run("SplitArgs", func(t *testing.T) {
		tests := []struct {
			name     string
			input    string
			expected []string
		}{
			{
				name:     "quoted player with multi-word item",
				input:    `"Player One" Item Name`,
				expected: []string{"Player One", "Item", "Name"},
			},
			{
				name:     "simple args no quotes",
				input:    "player item",
				expected: []string{"player", "item"},
			},
			{
				name:     "quoted arg only",
				input:    `"Player One"`,
				expected: []string{"Player One"},
			},
			{
				name:     "multiple quoted args",
				input:    `"Player One" "Some Item"`,
				expected: []string{"Player One", "Some Item"},
			},
			{
				name:     "unclosed quote",
				input:    `"Player One item`,
				expected: []string{"Player One item"},
			},
			{
				name:     "single word args",
				input:    "player item extra",
				expected: []string{"player", "item", "extra"},
			},
			{
				name:     "empty string",
				input:    "",
				expected: []string{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := splitArgs(tt.input)
				if len(result) != len(tt.expected) {
					t.Fatalf("got %d args %v, want %d args %v", len(result), result, len(tt.expected), tt.expected)
				}
				for i, v := range result {
					if v != tt.expected[i] {
						t.Errorf("arg[%d]: got %q, want %q", i, v, tt.expected[i])
					}
				}
			})
		}
	})
}
