package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	MessageTypeConnect        MessageType = "Connect"
	MessageTypeConnectUpdate  MessageType = "ConnectUpdate"
	MessageTypeBounce         MessageType = "Bounce"
	MessageTypeGetDataPackage MessageType = "GetDataPackage"
	MessageTypeDataPackage    MessageType = "DataPackage"
	MessageTypeSay            MessageType = "Say"
	MessageTypeInvalidPacket  MessageType = "InvalidPacket"
)

const (
	PermissionDisabled    Permission = 0b000
	PermissionEnabled     Permission = 0b001
	PermissionGoal        Permission = 0b010
	PermissionAuto        Permission = 0b110
	PermissionAutoEnabled Permission = 0b111
)

type RoomInfoMessage struct {
	Cmd                  MessageType           `json:"cmd"`
	Version              NetworkVersion        `json:"version"`
	GeneratorVersion     NetworkVersion        `json:"generator_version"`
	Tags                 []string              `json:"tags"`
	Password             bool                  `json:"password"`
	Permissions          map[string]Permission `json:"permissions"`
	HintCost             int                   `json:"hint_cost"`
	LocationCheckPoints  int                   `json:"location_check_points"`
	Games                []string              `json:"games"`
	DatapackageChecksums map[string]string     `json:"datapackage_checksums"`
	SeedName             string                `json:"seed_name"`
	Time                 float64               `json:"time"`
}

// Follow Multiserver out of spec behaviour
type StringOrBigInt string

func (f *StringOrBigInt) UnmarshalJSON(data []byte) error {
	// Try string first
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*f = StringOrBigInt(s)
		return nil
	}
	// Fall back to int, convert to string
	var i int64
	if err := json.Unmarshal(data, &i); err == nil {
		*f = StringOrBigInt(strconv.FormatInt(i, 10))
		return nil
	}
	// Fall back to big.Int for numbers exceeding int64 range
	var b big.Int
	if err := json.Unmarshal(data, &b); err == nil {
		*f = StringOrBigInt(b.String())
		return nil
	}
	return fmt.Errorf("uuid: cannot unmarshal %s into string or int", data)
}

// Follow Multiserver out of spec behaviour
type IntOrString int

func (f *IntOrString) UnmarshalJSON(data []byte) error {
	// Try int first
	var i int64
	if err := json.Unmarshal(data, &i); err == nil {
		*f = IntOrString(i)
		return nil
	}
	// Fall back to string, convert to int
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("version number: cannot parse string %q as int: %w", s, err)
		}
		*f = IntOrString(parsed)
		return nil
	}
	return fmt.Errorf("version number: cannot unmarshal %s into int or string", data)
}

type ConnectMessage struct {
	Cmd            MessageType    `json:"cmd"`
	Password       *string        `json:"password"`
	Game           string         `json:"game"`
	Name           string         `json:"name"`
	UUID           StringOrBigInt `json:"uuid"`
	Version        NetworkVersion `json:"version"`
	ItemsHandling  *int           `json:"items_handling"`
	Tags           []string       `json:"tags"`
	SlotData       *bool          `json:"slot_data"`
	ReducedTraffic bool           `json:"reduced"`
}

type ConnectionRefusedMessage struct {
	Cmd    MessageType `json:"cmd"`
	Errors []string    `json:"errors"`
}

type ConnectUpdateMessage struct {
	Cmd           MessageType `json:"cmd"`
	ItemsHandling *int        `json:"items_handling"`
	Tags          []string    `json:"tags"`
}

type ConnectedMessage struct {
	Team             int                 `json:"team"`
	Slot             int                 `json:"slot"`
	Players          []NetworkPlayer     `json:"players"`
	MissingLocations []int64             `json:"missing_locations"`
	CheckedLocations []int64             `json:"checked_locations"`
	SlotData         map[string]any      `json:"slot_data,omitempty"`
	SlotInfo         map[int]NetworkSlot `json:"slot_info"`
	HintPoints       int                 `json:"hint_points"`
}

type SayMessage struct {
	Cmd  MessageType `json:"cmd"`
	Text string      `json:"text"`
}

type PrintJsonChatMessage struct {
	Cmd     MessageType       `json:"cmd"`
	Data    []JsonMessagePart `json:"data"`
	Type    string            `json:"type"`
	Team    int               `json:"team"`
	Slot    int               `json:"slot"`
	Message string            `json:"message"`
}

type HintStatus int

const (
	HintUnspecified HintStatus = 0
	HintNoPriority  HintStatus = 10
	HintAvoid       HintStatus = 20
	HintPriority    HintStatus = 30
	HintFound       HintStatus = 40
)

type JsonMessagePart struct {
	Type       string     `json:"type,omitempty"`
	Text       string     `json:"text,omitempty"`
	Color      string     `json:"color,omitempty"`
	Flags      int        `json:"flags,omitempty"`
	Player     int        `json:"player,omitempty"`
	HintStatus HintStatus `json:"hint_status,omitempty"`
}

type BounceMessage struct {
	Cmd   MessageType    `json:"cmd"`
	Games []string       `json:"games"`
	Slots []int          `json:"slots"`
	Tags  []string       `json:"tags"`
	Data  map[string]any `json:"data"`
}

type BounceDataDeathlink struct {
	Time   float64 `json:"time"`
	Source string  `json:"source"`
	Cause  *string `json:"cause,omitempty"`
}

type NetworkPlayer struct {
	Team  int    `json:"team"`
	Slot  int    `json:"slot"`
	Alias string `json:"alias"`
	Name  string `json:"name"`
}

type NetworkSlot struct {
	Name         string `json:"name"`
	Game         string `json:"game"`
	Type         int    `json:"type"`
	GroupMembers []int  `json:"group_members"`
}

type NetworkSlotArray struct {
	Name         string `json:"name"`
	Game         string `json:"game"`
	Type         int    `json:"type"`
	GroupMembers []int  `json:"group_members"`
}

func (ns *NetworkSlotArray) UnmarshalJSON(data []byte) error {
	var raw [4]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	name, ok := raw[0].(string)
	if !ok {
		return fmt.Errorf("NetworkSlot[0] name: expected string, got %T", raw[0])
	}

	game, ok := raw[1].(string)
	if !ok {
		return fmt.Errorf("NetworkSlot[1] game: expected string, got %T", raw[1])
	}

	slotType, ok := raw[2].(float64)
	if !ok {
		return fmt.Errorf("NetworkSlot[2] type: expected number, got %T", raw[2])
	}

	var groupMembers []int
	if raw[3] != nil {
		members, ok := raw[3].([]any)
		if !ok {
			return fmt.Errorf("NetworkSlot[3] group_members: expected array, got %T", raw[3])
		}
		groupMembers = make([]int, len(members))
		for i, m := range members {
			f, ok := m.(float64)
			if !ok {
				return fmt.Errorf("NetworkSlot[3][%d]: expected number, got %T", i, m)
			}
			groupMembers[i] = int(f)
		}
	}

	ns.Name = name
	ns.Game = game
	ns.Type = int(slotType)
	ns.GroupMembers = groupMembers
	return nil
}

type NetworkVersion struct {
	Class string      `json:"class"`
	Major IntOrString `json:"major"`
	Minor IntOrString `json:"minor"`
	Build IntOrString `json:"build"`
}

type PacketProblemType string

const (
	PacketProblemCmd       PacketProblemType = "cmd"
	PacketProblemArguments PacketProblemType = "arguments"
)

type InvalidPacketMessage struct {
	Cmd         MessageType       `json:"cmd"`
	Type        PacketProblemType `json:"type"`
	OriginalCmd *MessageType      `json:"original_cmd"`
	Text        string            `json:"text"`
}

func sendInvalidPacket(ctx context.Context, conn *websocket.Conn, problemType PacketProblemType, originalCmd *MessageType, text string, lokiLogger *LokiLogger, slotName *string) error {
	msg := InvalidPacketMessage{
		Cmd:         "InvalidPacket",
		Type:        problemType,
		OriginalCmd: originalCmd,
		Text:        text,
	}
	if lokiLogger != nil && slotName != nil {
		if raw, err := json.Marshal(msg); err == nil {
			lokiLogger.Log(*slotName, LogSourceServer, raw)
		}
	}
	return wsjson.Write(ctx, conn, []any{msg})
}
