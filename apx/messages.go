package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	MessageTypeConnect           MessageType = "Connect"
	MessageTypeConnectUpdate     MessageType = "ConnectUpdate"
	MessageTypeBounce            MessageType = "Bounce"
	MessageTypeBounced           MessageType = "Bounced"
	MessageTypeGetDataPackage    MessageType = "GetDataPackage"
	MessageTypeDataPackage       MessageType = "DataPackage"
	MessageTypeSay               MessageType = "Say"
	MessageTypeInvalidPacket     MessageType = "InvalidPacket"
	MessageTypeRoomInfo          MessageType = "RoomInfo"
	MessageTypeConnectionRefused MessageType = "ConnectionRefused"
	MessageTypeConnected         MessageType = "Connected"
	MessageTypeReceivedItems     MessageType = "ReceivedItems"
	MessageTypeLocationInfo      MessageType = "LocationInfo"
	MessageTypeRoomUpdate        MessageType = "RoomUpdate"
	MessageTypePrintJSON         MessageType = "PrintJSON"
	MessageTypeRetrieved         MessageType = "Retrieved"
	MessageTypeSetReply          MessageType = "SetReply"
	MessageTypeSync              MessageType = "Sync"
	MessageTypeLocationChecks    MessageType = "LocationChecks"
	MessageTypeLocationScouts    MessageType = "LocationScouts"
	MessageTypeCreateHints       MessageType = "CreateHints"
	MessageTypeUpdateHint        MessageType = "UpdateHint"
	MessageTypeStatusUpdate      MessageType = "StatusUpdate"
	MessageTypeGet               MessageType = "Get"
	MessageTypeSet               MessageType = "Set"
	MessageTypeSetNotify         MessageType = "SetNotify"
)

var messageTypeUnknownBytes = []byte(`"Unknown"`)
var messageTypeBytes = map[MessageType][]byte{
	MessageTypeConnect:           []byte(`"Connect"`),
	MessageTypeConnectUpdate:     []byte(`"ConnectUpdate"`),
	MessageTypeBounce:            []byte(`"Bounce"`),
	MessageTypeBounced:           []byte(`"Bounced"`),
	MessageTypeGetDataPackage:    []byte(`"GetDataPackage"`),
	MessageTypeDataPackage:       []byte(`"DataPackage"`),
	MessageTypeSay:               []byte(`"Say"`),
	MessageTypeInvalidPacket:     []byte(`"InvalidPacket"`),
	MessageTypeRoomInfo:          []byte(`"RoomInfo"`),
	MessageTypeConnectionRefused: []byte(`"ConnectionRefused"`),
	MessageTypeConnected:         []byte(`"Connected"`),
	MessageTypeReceivedItems:     []byte(`"ReceivedItems"`),
	MessageTypeLocationInfo:      []byte(`"LocationInfo"`),
	MessageTypeRoomUpdate:        []byte(`"RoomUpdate"`),
	MessageTypePrintJSON:         []byte(`"PrintJSON"`),
	MessageTypeRetrieved:         []byte(`"Retrieved"`),
	MessageTypeSetReply:          []byte(`"SetReply"`),
	MessageTypeSync:              []byte(`"Sync"`),
	MessageTypeLocationChecks:    []byte(`"LocationChecks"`),
	MessageTypeLocationScouts:    []byte(`"LocationScouts"`),
	MessageTypeCreateHints:       []byte(`"CreateHints"`),
	MessageTypeUpdateHint:        []byte(`"UpdateHint"`),
	MessageTypeStatusUpdate:      []byte(`"StatusUpdate"`),
	MessageTypeGet:               []byte(`"Get"`),
	MessageTypeSet:               []byte(`"Set"`),
	MessageTypeSetNotify:         []byte(`"SetNotify"`),
}

const (
	PermissionDisabled    Permission = 0b000
	PermissionEnabled     Permission = 0b001
	PermissionGoal        Permission = 0b010
	PermissionAuto        Permission = 0b110
	PermissionAutoEnabled Permission = 0b111
)

func permissionFromString(mode string) Permission {
	switch mode {
	case "enabled":
		return PermissionEnabled
	case "goal":
		return PermissionGoal
	case "auto":
		return PermissionAuto
	case "auto-enabled":
		return PermissionAutoEnabled
	default:
		return PermissionDisabled
	}
}

type RoomInfoMessage struct {
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

func (m RoomInfoMessage) MarshalJSON() ([]byte, error) {
	type Alias RoomInfoMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeRoomInfo, Alias: Alias(m)})
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

func (m ConnectMessage) MarshalJSON() ([]byte, error) {
	type Alias ConnectMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeConnect, Alias: Alias(m)})
}

const (
	ItemsHandlingNone              int = 0b000
	ItemsHandlingForeign           int = 0b001
	ItemsHandlingOwn               int = 0b010
	ItemsHandlingStartingInventory int = 0b100
)

type ConnectionRefusedMessage struct {
	Errors []string `json:"errors"`
}

func (m ConnectionRefusedMessage) MarshalJSON() ([]byte, error) {
	type Alias ConnectionRefusedMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeConnectionRefused, Alias: Alias(m)})
}

type ConnectUpdateMessage struct {
	ItemsHandling *int     `json:"items_handling"`
	Tags          []string `json:"tags"`
}

type ConnectedMessage struct {
	Team             int                 `json:"team"`
	Slot             int                 `json:"slot"`
	Players          []NetworkPlayer     `json:"players"`
	MissingLocations []int               `json:"missing_locations"`
	CheckedLocations []int               `json:"checked_locations"`
	SlotData         map[string]any      `json:"slot_data,omitempty"`
	SlotInfo         map[int]NetworkSlot `json:"slot_info"`
	HintPoints       int                 `json:"hint_points"`
}

func (m ConnectedMessage) MarshalJSON() ([]byte, error) {
	type Alias ConnectedMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeConnected, Alias: Alias(m)})
}

type RoomUpdateMessage struct {
	Players          []NetworkPlayer `json:"players,omitempty"`
	CheckedLocations []int           `json:"checked_locations"`
	HintPoints       *int            `json:"hint_points,omitempty"`
}

func (m RoomUpdateMessage) MarshalJSON() ([]byte, error) {
	type Alias RoomUpdateMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeRoomUpdate, Alias: Alias(m)})
}

type SayMessage struct {
	Text string `json:"text"`
}

func (m SayMessage) MarshalJSON() ([]byte, error) {
	type Alias SayMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeSay, Alias: Alias(m)})
}

type PrintJsonMessage struct {
	Data      []JsonMessagePart `json:"data"`
	Type      string            `json:"type"`
	Receiving *int              `json:"receiving,omitempty"`
	Item      *NetworkItem      `json:"item,omitempty"`
	Found     *bool             `json:"found,omitempty"`
	Team      *int              `json:"team,omitempty"`
	Slot      *int              `json:"slot,omitempty"`
	Tags      []string          `json:"tags,omitempty"`
	Message   *string           `json:"message,omitempty"`
}

func (m PrintJsonMessage) MarshalJSON() ([]byte, error) {
	type Alias PrintJsonMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypePrintJSON, Alias: Alias(m)})
}

type HintStatus int

const (
	HintStatusUnspecified HintStatus = 0
	HintStatusNoPriority  HintStatus = 10
	HintStatusAvoid       HintStatus = 20
	HintStatusPriority    HintStatus = 30
	HintStatusFound       HintStatus = 40
)

type JsonMessagePart struct {
	Type       string      `json:"type,omitempty"`
	Text       string      `json:"text,omitempty"`
	Color      string      `json:"color,omitempty"`
	Flags      int         `json:"flags,omitempty"`
	Player     int         `json:"player,omitempty"`
	HintStatus *HintStatus `json:"hint_status,omitempty"`
}

type BounceMessage struct {
	Cmd   MessageType     `json:"cmd"`
	Games *[]string       `json:"games,omitempty"`
	Slots *[]int          `json:"slots,omitempty"`
	Tags  *[]string       `json:"tags,omitempty"`
	Data  *map[string]any `json:"data,omitempty"`
}

type BounceDataDeathlink struct {
	Time   float64 `json:"time"`
	Source *string `json:"source"`
	Cause  *string `json:"cause,omitempty"`
}

type NetworkPlayer struct {
	Team  int    `json:"team"`
	Slot  int    `json:"slot"`
	Alias string `json:"alias"`
	Name  string `json:"name"`
}

func (np NetworkPlayer) MarshalJSON() ([]byte, error) {
	type Alias NetworkPlayer
	return json.Marshal(struct {
		Alias
		Class string `json:"class"`
	}{
		Alias: Alias(np),
		Class: "NetworkPlayer",
	})
}

type NetworkSlot struct {
	Name         string `json:"name"`
	Game         string `json:"game"`
	Type         int    `json:"type"`
	GroupMembers []int  `json:"group_members"`
}

func (ns NetworkSlot) MarshalJSON() ([]byte, error) {
	type Alias NetworkSlot
	return json.Marshal(struct {
		Alias
		Class string `json:"class"`
	}{
		Alias: Alias(ns),
		Class: "NetworkSlot",
	})
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
	Major IntOrString `json:"major"`
	Minor IntOrString `json:"minor"`
	Build IntOrString `json:"build"`
}

func (np NetworkVersion) MarshalJSON() ([]byte, error) {
	type Alias NetworkVersion
	return json.Marshal(struct {
		Alias
		Class string `json:"class"`
	}{
		Alias: Alias(np),
		Class: "Version",
	})
}

type LocationChecksMessage struct {
	Locations []int `json:"locations"`
}

func (m LocationChecksMessage) MarshalJSON() ([]byte, error) {
	type Alias LocationChecksMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeLocationChecks, Alias: Alias(m)})
}

type ReceivedItemsMessage struct {
	Index int           `json:"index"`
	Items []NetworkItem `json:"items"`
}

func (m ReceivedItemsMessage) MarshalJSON() ([]byte, error) {
	type Alias ReceivedItemsMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeReceivedItems, Alias: Alias(m)})
}

type CreateHintsMessage struct {
	Locations []int `json:"locations"`
	Player    *int  `json:"player"`
	Status    *int  `json:"status"`
}

func (m CreateHintsMessage) MarshalJSON() ([]byte, error) {
	type Alias CreateHintsMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeCreateHints, Alias: Alias(m)})
}

type UpdateHintMessage struct {
	Player   int  `json:"player"`
	Location int  `json:"location"`
	Status   *int `json:"status"`
}

func (m UpdateHintMessage) MarshalJSON() ([]byte, error) {
	type Alias UpdateHintMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeUpdateHint, Alias: Alias(m)})
}

type PacketProblemType string

const (
	PacketProblemCmd       PacketProblemType = "cmd"
	PacketProblemArguments PacketProblemType = "arguments"
)

type InvalidPacketMessage struct {
	Type        PacketProblemType `json:"type"`
	OriginalCmd *MessageType      `json:"original_cmd"`
	Text        string            `json:"text"`
}

func (m InvalidPacketMessage) MarshalJSON() ([]byte, error) {
	type Alias InvalidPacketMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeInvalidPacket, Alias: Alias(m)})
}

func sendInvalidPacket(ctx context.Context, conn *websocket.Conn, problemType PacketProblemType, originalCmd *MessageType, text string, lokiLogger *LokiLogger, slotName *string) error {
	msg := InvalidPacketMessage{
		Type:        problemType,
		OriginalCmd: originalCmd,
		Text:        text,
	}
	if lokiLogger != nil && slotName != nil {
		if raw, err := json.Marshal(msg); err == nil {
			lokiLogger.Log(slotName, LogSourceApx, raw, "InvalidPacket")
		}
	}
	return wsjson.Write(ctx, conn, []any{msg})
}

var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func BroadcastJSON(ctx context.Context, clients []*RegisteredClient, v any) (int, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return 0, fmt.Errorf("failed to marshal broadcast message: %w", err)
	}

	data := buf.Bytes()
	n := len(data)

	for _, c := range clients {
		_ = c.clientConn.Write(ctx, websocket.MessageText, data)
	}

	return n, nil
}

func simplePrintJsonMessage(msgType string, msg string) *PrintJsonMessage {
	return &PrintJsonMessage{
		Data: []JsonMessagePart{
			{
				Type:  "text",
				Text:  msg,
				Color: "bold",
			},
		},
		Type:    msgType,
		Message: &msg,
	}
}
