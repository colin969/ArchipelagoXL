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

func (perm Permission) Valid() bool {
	switch perm {
	case PermissionDisabled, PermissionEnabled, PermissionGoal, PermissionAuto, PermissionAutoEnabled:
		return true
	}
	return false
}

func permissionFromString(mode string) (Permission, bool) {
	switch mode {
	case "enabled":
		return PermissionEnabled, true
	case "goal":
		return PermissionGoal, true
	case "auto":
		return PermissionAuto, true
	case "auto-enabled":
		return PermissionAutoEnabled, true
	case "disabled":
		return PermissionDisabled, true
	default:
		return PermissionDisabled, false
	}
}

type RoomInfoMessage struct {
	Permissions          map[string]Permission `json:"permissions"`
	DatapackageChecksums map[string]string     `json:"datapackage_checksums"`
	SeedName             string                `json:"seed_name"`
	Tags                 []string              `json:"tags"`
	Games                []string              `json:"games"`
	Version              NetworkVersion        `json:"version"`
	GeneratorVersion     NetworkVersion        `json:"generator_version"`
	HintCost             int                   `json:"hint_cost"`
	LocationCheckPoints  int                   `json:"location_check_points"`
	Time                 float64               `json:"time"`
	Password             bool                  `json:"password"`
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
	Password      *string        `json:"password"`
	ItemsHandling *int           `json:"items_handling"`
	SlotData      *bool          `json:"slot_data"`
	Game          string         `json:"game"`
	Name          string         `json:"name"`
	UUID          StringOrBigInt `json:"uuid"`
	Tags          []string       `json:"tags"`
	Version       NetworkVersion `json:"version"`
}

func (m ConnectMessage) MarshalJSON() ([]byte, error) {
	type Alias ConnectMessage
	return json.Marshal(struct {
		Alias
		Cmd MessageType `json:"cmd"`
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
	ItemsHandling *int      `json:"items_handling"`
	Tags          *[]string `json:"tags"`
}

type ConnectedMessage struct {
	SlotData         map[string]any  `json:"slot_data,omitempty"`
	Players          json.RawMessage `json:"players"`
	MissingLocations []int64         `json:"missing_locations"`
	CheckedLocations []int64         `json:"checked_locations"`
	SlotInfo         json.RawMessage `json:"slot_info"`
	Team             int             `json:"team"`
	Slot             int             `json:"slot"`
	HintPoints       int             `json:"hint_points"`
}

// Special case, must not be wrapped in []any
func (m ConnectedMessage) MarshalJSON() ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	buf.WriteString(`[{"cmd":"Connected","team":`)
	buf.WriteString(strconv.Itoa(m.Team))
	buf.WriteString(`,"slot":`)
	buf.WriteString(strconv.Itoa(m.Slot))
	buf.WriteString(`,"players":`)
	buf.Write(m.Players)
	buf.WriteString(`,"missing_locations":`)
	missingJSON, _ := json.Marshal(m.MissingLocations)
	buf.Write(missingJSON)
	buf.WriteString(`,"checked_locations":`)
	checkedJSON, _ := json.Marshal(m.CheckedLocations)
	buf.Write(checkedJSON)
	if m.SlotData != nil {
		buf.WriteString(`,"slot_data":`)
		slotDataJSON, _ := json.Marshal(m.SlotData)
		buf.Write(slotDataJSON)
	}
	buf.WriteString(`,"slot_info":`)
	buf.Write(m.SlotInfo)
	buf.WriteString(`,"hint_points":`)
	buf.WriteString(strconv.Itoa(m.HintPoints))
	buf.WriteString(`}]`)

	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

type RoomUpdateMessage struct {
	HintPoints          *int                  `json:"hint_points,omitempty"`
	HintCost            *int                  `json:"hint_cost,omitempty"`
	LocationCheckPoints *int                  `json:"location_check_points,omitempty"`
	Permissions         map[string]Permission `json:"permissions,omitempty"`
	Players             []NetworkPlayer       `json:"players,omitempty"`
	CheckedLocations    []int64               `json:"checked_locations,omitempty"`
}

func (m RoomUpdateMessage) MarshalJSON() ([]byte, error) {
	type Alias RoomUpdateMessage
	return json.Marshal(struct {
		Alias
		Cmd MessageType `json:"cmd"`
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
	Receiving *int              `json:"receiving,omitempty"`
	Item      *NetworkItem      `json:"item,omitempty"`
	Found     *bool             `json:"found,omitempty"`
	Team      *int              `json:"team,omitempty"`
	Slot      *int              `json:"slot,omitempty"`
	Message   *string           `json:"message,omitempty"`
	Type      string            `json:"type"`
	Data      []JsonMessagePart `json:"data"`
	Tags      []string          `json:"tags,omitempty"`
}

func (m PrintJsonMessage) MarshalJSON() ([]byte, error) {
	type Alias PrintJsonMessage
	return json.Marshal(struct {
		Alias
		Cmd MessageType `json:"cmd"`
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
	HintStatus *HintStatus `json:"hint_status,omitempty"`
	Type       string      `json:"type,omitempty"`
	Text       string      `json:"text,omitempty"`
	Color      string      `json:"color,omitempty"`
	Flags      int         `json:"flags,omitempty"`
	Player     int         `json:"player,omitempty"`
}

type BounceMessage struct {
	Games *[]string       `json:"games,omitempty"`
	Slots *[]int          `json:"slots,omitempty"`
	Tags  *[]string       `json:"tags,omitempty"`
	Data  *map[string]any `json:"data,omitempty"`
	Cmd   MessageType     `json:"cmd"`
}

type BounceDataDeathlink struct {
	Source *string `json:"source"`
	Cause  *string `json:"cause,omitempty"`
	Time   float64 `json:"time"`
}

type NetworkPlayer struct {
	Alias string `json:"alias"`
	Name  string `json:"name"`
	Team  int    `json:"team"`
	Slot  int    `json:"slot"`
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
	GroupMembers []int  `json:"group_members"`
	Type         int    `json:"type"`
}

func (ns NetworkSlot) MarshalJSON() ([]byte, error) {
	type Alias NetworkSlot
	return json.Marshal(struct {
		Class string `json:"class"`
		Alias
	}{
		Alias: Alias(ns),
		Class: "NetworkSlot",
	})
}

type NetworkSlotArray struct {
	Name         string `json:"name"`
	Game         string `json:"game"`
	GroupMembers []int  `json:"group_members"`
	Type         int    `json:"type"`
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
		Class string `json:"class"`
		Alias
	}{
		Alias: Alias(np),
		Class: "Version",
	})
}

type ClientStatus int

const (
	ClientStatusUnknown   ClientStatus = 0
	ClientStatusConnected ClientStatus = 5
	ClientStatusReady     ClientStatus = 10
	ClientStatusPlaying   ClientStatus = 20
	ClientStatusGoal      ClientStatus = 30
)

func (cs ClientStatus) Valid() bool {
	switch cs {
	case ClientStatusUnknown, ClientStatusConnected, ClientStatusReady, ClientStatusPlaying, ClientStatusGoal:
		return true
	}
	return false
}

// Multiserver behaviour
func (cs *ClientStatus) UnmarshalJSON(data []byte) error {
	var n json.Number
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	vals, err := parseIntConvFloatList([]json.Number{n})
	if err != nil {
		return err
	}
	*cs = ClientStatus(vals[0])
	return nil
}

type StatusUpdateMessage struct {
	Status ClientStatus `json:"status"`
}

type LocationScoutsMessage struct {
	Locations    []int64 `json:"locations"`
	CreateAsHint int     `json:"create_as_hint"`
}

// Multiserver behaviour
func (m *LocationScoutsMessage) UnmarshalJSON(data []byte) error {
	var raw struct {
		Locations    []json.Number `json:"locations"`
		CreateAsHint int           `json:"create_as_hint"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	m.CreateAsHint = raw.CreateAsHint
	var err error
	m.Locations, err = parseIntConvFloatList(raw.Locations)
	return err
}

type LocationInfoMessage struct {
	Locations []*NetworkItem `json:"locations"`
}

func (m LocationInfoMessage) MarshalJSON() ([]byte, error) {
	type Alias LocationInfoMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeLocationInfo, Alias: Alias(m)})
}

type LocationChecksMessage struct {
	// Special decoding since the multiserver doesn't type check it, floats are acceptable :(
	Locations []int64 `json:"locations"`
}

// Multiserver behaviour
func (m *LocationChecksMessage) UnmarshalJSON(data []byte) error {
	var raw struct {
		Locations []json.Number `json:"locations"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var err error
	m.Locations, err = parseIntConvFloatList(raw.Locations)
	return err
}

func (m LocationChecksMessage) MarshalJSON() ([]byte, error) {
	type Alias LocationChecksMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeLocationChecks, Alias: Alias(m)})
}

type ReceivedItemsMessage struct {
	Items []NetworkItem `json:"items"`
	Index int           `json:"index"`
}

func (m ReceivedItemsMessage) MarshalJSON() ([]byte, error) {
	type Alias ReceivedItemsMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeReceivedItems, Alias: Alias(m)})
}

type CreateHintsMessage struct {
	Player    *int    `json:"player"`
	Status    *int    `json:"status"`
	Locations []int64 `json:"locations"`
}

// Multiserver behaviour
func (m *CreateHintsMessage) UnmarshalJSON(data []byte) error {
	var raw struct {
		Player    *int          `json:"player"`
		Status    *int          `json:"status"`
		Locations []json.Number `json:"locations"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	m.Player = raw.Player
	m.Status = raw.Status
	var err error
	m.Locations, err = parseIntConvFloatList(raw.Locations)
	return err
}

func (m CreateHintsMessage) MarshalJSON() ([]byte, error) {
	type Alias CreateHintsMessage
	return json.Marshal(struct {
		Alias
		Cmd MessageType `json:"cmd"`
	}{Cmd: MessageTypeCreateHints, Alias: Alias(m)})
}

type UpdateHintMessage struct {
	Status   *HintStatus `json:"status"`
	Player   int         `json:"player"`
	Location int64       `json:"location"`
}

// Multiserver behaviours
func (m *UpdateHintMessage) UnmarshalJSON(data []byte) error {
	var raw struct {
		Status   *HintStatus `json:"status"`
		Player   json.Number `json:"player"`
		Location json.Number `json:"location"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	m.Status = raw.Status
	vals, err := parseIntConvFloatList([]json.Number{raw.Player, raw.Location})
	if err != nil {
		return err
	}
	m.Player = int(vals[0])
	m.Location = vals[1]
	return nil
}

func (m UpdateHintMessage) MarshalJSON() ([]byte, error) {
	type Alias UpdateHintMessage
	return json.Marshal(struct {
		Alias
		Cmd MessageType `json:"cmd"`
	}{Cmd: MessageTypeUpdateHint, Alias: Alias(m)})
}

type GetMessage struct {
	Keys []string `json:"keys"`
}

type SetMessage struct {
	Default    any                    `json:"default"`
	Key        string                 `json:"key"`
	Operations []DataStorageOperation `json:"operations"`
	WantReply  bool                   `json:"want_reply"`
}

type SetNotifyMessage struct {
	Keys []string `json:"keys"`
}

type SetReplyMessage struct {
	Value         any    `json:"value"`
	OriginalValue any    `json:"original_value,omitempty"`
	Key           string `json:"key"`
	Slot          int    `json:"slot"`
}

func (m SetReplyMessage) MarshalJSON() ([]byte, error) {
	type Alias SetReplyMessage
	return json.Marshal(struct {
		Cmd MessageType `json:"cmd"`
		Alias
	}{Cmd: MessageTypeSetReply, Alias: Alias(m)})
}

type RetrievedMessage struct {
	Keys map[string]any `json:"keys"`
}

func (m RetrievedMessage) MarshalJSON() ([]byte, error) {
	type Alias RetrievedMessage
	return json.Marshal(struct {
		Alias
		Cmd MessageType `json:"cmd"`
	}{Cmd: MessageTypeRetrieved, Alias: Alias(m)})
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
		if c.clientConn == nil {
			continue
		}
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

func parseIntConvFloatList(nums []json.Number) ([]int64, error) {
	out := make([]int64, len(nums))
	for i, n := range nums {
		i64, err := strconv.ParseInt(string(n), 10, 64)
		if err == nil {
			out[i] = i64
			continue
		}
		f, err := strconv.ParseFloat(string(n), 64)
		if err != nil {
			return nil, fmt.Errorf("[%d]: cannot parse %q as number", i, n)
		}
		if f != float64(int64(f)) {
			return nil, fmt.Errorf("[%d]: %v is not a whole number", i, f)
		}
		out[i] = int64(f)
	}
	return out, nil
}

func enqueueMessage(msg []any, clients ...*RegisteredClient) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	for _, client := range clients {
		client.messageQueue.Enqueue(b)
	}
	return nil
}
