// apxroom_auth_test.go
package main

import (
	"fmt"
	"testing"
)

// Helpers

func roomWithAuth(auth map[string]*SlotInfo) ApxRoom {
	return ApxRoom{
		state: &ApState{
			NameToSlot: auth,
		},
	}
}

func strPtr(s string) *string { return &s }

// Tests

func TestAuth(t *testing.T) {
	t.Run("ValidateSlot", func(t *testing.T) {
		s := roomWithAuth(map[string]*SlotInfo{
			"Alice": {Team: 1, Slot: 42},
		})

		tests := []struct {
			name      string
			slot      string
			wantOK    bool
			wantEntry TeamSlot
		}{
			{"known slot", "Alice", true, TeamSlot{1, 42}},
			{"unknown slot", "Bob", false, TeamSlot{}},
			{"empty name", "", false, TeamSlot{}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				slotInfo, ok := s.state.NameToSlot[tt.slot]
				if ok != tt.wantOK {
					t.Errorf("ok = %v, want %v", ok, tt.wantOK)
				}
				if ok {
					teamSlot := TeamSlot{slotInfo.Team, slotInfo.Slot}
					if ok && teamSlot != tt.wantEntry {
						t.Errorf("entry = %v, want %v", teamSlot, tt.wantEntry)
					}
				}
			})
		}
	})

	t.Run("ValidatePassword", func(t *testing.T) {
		store := newPasswordStore()
		store.Set(1, "secret")

		s := ApxRoom{passwords: store}

		tests := []struct {
			name     string
			slotKey  int
			provided *string
			want     bool
		}{
			{"correct password", 1, strPtr("secret"), true},
			{"wrong password", 1, strPtr("wrong"), false},
			{"nil password", 1, nil, false},
			{"unknown slot key", 99, strPtr("secret"), false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := s.validatePassword(tt.slotKey, tt.provided); got != tt.want {
					t.Errorf("validatePassword() = %v, want %v", got, tt.want)
				}
			})
		}
	})

	t.Run("ResolveAltName", func(t *testing.T) {
		cn := newAltConnectNames()
		cn.SetAltName("NewName", "OldName")

		s := ApxRoom{altConnectNames: cn}

		tests := []struct {
			name string
			in   string
			want string
		}{
			{"mapped name", "OldName", "NewName"},
			{"unmapped name", "Alice", "Alice"},
			{"empty name", "", ""},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := s.resolveAltName(tt.in); got != tt.want {
					t.Errorf("resolveAltName(%q) = %q, want %q", tt.in, got, tt.want)
				}
			})
		}
	})

	t.Run("IsFullFeedAllowed", func(t *testing.T) {
		ffs := newFullFeedStore()
		ffs.Set(1)

		s := ApxRoom{fullFeed: ffs}

		tests := []struct {
			name    string
			slotKey int
			want    bool
		}{
			{"allowed slot", 1, true},
			{"denied slot", 2, false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := s.isFullFeedAllowed(tt.slotKey); got != tt.want {
					t.Errorf("isFullFeedAllowed(%d) = %v, want %v", tt.slotKey, got, tt.want)
				}
			})
		}
	})
}

func TestIsNormalClose(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, true},
		{"context canceled", fmt.Errorf("context canceled"), true},
		{"closed network", fmt.Errorf("use of closed network connection"), true},
		{"unexpected error", fmt.Errorf("something exploded"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNormalClose(tt.err); got != tt.want {
				t.Errorf("isNormalClose() = %v, want %v", got, tt.want)
			}
		})
	}
}
