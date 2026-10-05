package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Anything relevant to accessing the Ionium lobby should be in here

type SlotPasswordInfo struct {
	SlotNumber int     `json:"slot_number"`
	PlayerName string  `json:"player_name"`
	Password   *string `json:"password"`
}

func fetchSlotPasswords(cfg *Config, roomId string) ([]SlotPasswordInfo, error) {
	url := fmt.Sprintf("%s/api/room/%s/slots_passwords", cfg.LobbyRootUrl, roomId)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-Api-Key", cfg.LobbyApiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch slot passwords: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from lobby API", resp.StatusCode)
	}

	var slots []SlotPasswordInfo
	if err := json.NewDecoder(resp.Body).Decode(&slots); err != nil {
		return nil, fmt.Errorf("failed to decode slot passwords: %w", err)
	}

	return slots, nil
}

func loadPasswordsIntoStore(connections *connectionRegistry, store *passwordStore, connectNames map[string]*SlotInfo, slots []SlotPasswordInfo) {
	for _, slot := range slots {
		slotEntry, ok := connectNames[slot.PlayerName]
		if ok && slot.Password != nil && *slot.Password != "" {
			current, ok := store.Get(slotEntry.Slot)
			store.Set(slotEntry.Slot, *slot.Password)
			// If the password has changed, boot any connected clients from that slot
			if !ok || current != *slot.Password {
				connections.Kick(slotEntry.Slot)
			}
		} else if ok {
			store.Delete(slotEntry.Slot)
		}
	}
}
