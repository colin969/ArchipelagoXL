package multidata

import (
	"archive/zip"
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/nlpodyssey/gopickle/pickle"
	"github.com/nlpodyssey/gopickle/types"
)

type SlotType int

const (
	SlotTypeSpectator SlotType = 0b00
	SlotTypePlayer    SlotType = 0b01
	SlotTypeGroup     SlotType = 0b10
)

type HintStatus int

const (
	HintStatusUnspecified HintStatus = 0
	HintStatusNoPriority  HintStatus = 10
	HintStatusAvoid       HintStatus = 20
	HintStatusPriority    HintStatus = 30
	HintStatusFound       HintStatus = 40
)

type NetworkSlot struct {
	Name         string
	Game         string
	Type         SlotType
	GroupMembers []int
}

type Hint struct {
	ReceivingPlayer int
	FindingPlayer   int
	Location        int
	Item            int
	Found           bool
	Entrance        string
	ItemFlags       int
	Status          HintStatus
}

// toInt converts int or int64 to int.
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	}
	return 0, false
}

// pickle class handlers

type SlotTypeClass struct{}

func (s *SlotTypeClass) Call(args ...any) (any, error) {
	if len(args) == 1 {
		if v, ok := toInt(args[0]); ok {
			return SlotType(v), nil
		}
	}
	return SlotType(0), nil
}

func (s *SlotTypeClass) PyNew(args ...any) (any, error) { return s.Call(args...) }

type HintStatusClass struct{}

func (h *HintStatusClass) Call(args ...any) (any, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("HintStatus: expected 1 arg, got %d", len(args))
	}
	v, ok := toInt(args[0])
	if !ok {
		return 0, fmt.Errorf("HintStatus: value must be int, got %T", args[0])
	}
	return HintStatus(v), nil
}

func (h *HintStatusClass) PyNew(args ...any) (any, error) { return h.Call(args...) }

type NetworkSlotClass struct{}

func (n *NetworkSlotClass) Call(args ...any) (any, error)  { return buildNetworkSlot(args...) }
func (n *NetworkSlotClass) PyNew(args ...any) (any, error) { return buildNetworkSlot(args...) }

func buildNetworkSlot(args ...any) (*NetworkSlot, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("NetworkSlot: expected at least 3 args, got %d", len(args))
	}
	name, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("NetworkSlot: name must be string, got %T", args[0])
	}
	game, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("NetworkSlot: game must be string, got %T", args[1])
	}
	slotType, ok := args[2].(SlotType)
	if !ok {
		return nil, fmt.Errorf("NetworkSlot: type must be SlotType, got %T", args[2])
	}

	slot := &NetworkSlot{Name: name, Game: game, Type: slotType}

	if len(args) >= 4 {
		members, ok := args[3].(*types.Tuple)
		if !ok {
			return nil, fmt.Errorf("NetworkSlot: group_members must be tuple, got %T", args[3])
		}
		for _, m := range *members {
			id, ok := toInt(m)
			if !ok {
				return nil, fmt.Errorf("NetworkSlot: group_members entry must be int, got %T", m)
			}
			slot.GroupMembers = append(slot.GroupMembers, id)
		}
	}

	return slot, nil
}

type HintClass struct{}

func (h *HintClass) Call(args ...any) (any, error)  { return buildHint(args...) }
func (h *HintClass) PyNew(args ...any) (any, error) { return buildHint(args...) }

func buildHint(args ...any) (*Hint, error) {
	if len(args) < 5 {
		return nil, fmt.Errorf("Hint: expected at least 5 args, got %d", len(args))
	}
	receivingPlayer, ok := toInt(args[0])
	if !ok {
		return nil, fmt.Errorf("Hint: receiving_player must be int, got %T", args[0])
	}
	findingPlayer, ok := toInt(args[1])
	if !ok {
		return nil, fmt.Errorf("Hint: finding_player must be int, got %T", args[1])
	}
	location, ok := toInt(args[2])
	if !ok {
		return nil, fmt.Errorf("Hint: location must be int, got %T", args[2])
	}
	item, ok := toInt(args[3])
	if !ok {
		return nil, fmt.Errorf("Hint: item must be int, got %T", args[3])
	}
	found, ok := args[4].(bool)
	if !ok {
		return nil, fmt.Errorf("Hint: found must be bool, got %T", args[4])
	}

	hint := &Hint{
		ReceivingPlayer: receivingPlayer,
		FindingPlayer:   findingPlayer,
		Location:        location,
		Item:            item,
		Found:           found,
	}

	if len(args) >= 6 {
		entrance, ok := args[5].(string)
		if !ok {
			return nil, fmt.Errorf("Hint: entrance must be string, got %T", args[5])
		}
		hint.Entrance = entrance
	}
	if len(args) >= 7 {
		itemFlags, ok := toInt(args[6])
		if !ok {
			return nil, fmt.Errorf("Hint: item_flags must be int, got %T", args[6])
		}
		hint.ItemFlags = itemFlags
	}
	if len(args) >= 8 {
		status, ok := args[7].(HintStatus)
		if !ok {
			return nil, fmt.Errorf("Hint: status must be HintStatus, got %T", args[7])
		}
		hint.Status = status
	}

	return hint, nil
}

type MinimumVersions struct {
	Server  [3]int
	Clients map[int][3]int
}

type GamesPackage struct {
	ItemNameGroups     map[string][]string
	ItemNameToID       map[string]int
	LocationNameGroups map[string][]string
	LocationNameToID   map[string]int
	Checksum           string
}

type MultiData struct {
	SlotData          map[int]map[string]any
	SlotInfo          map[int]NetworkSlot
	ConnectNames      map[string][2]int
	Locations         map[int]map[int][3]int
	ServerOptions     map[string]any
	ErHintData        map[int]map[int]string
	PrecollectedItems map[int][]int
	PrecollectedHints map[int][]Hint // set -> slice
	Version           [3]int
	Tags              []string
	MinimumVersions   MinimumVersions
	SeedName          string
	Spheres           []map[int][]int // set -> slice
	DataPackage       map[string]GamesPackage
	RaceMode          int
}

func findMultidataClass(module, name string) (any, error) {
	switch module {
	case "NetUtils":
		switch name {
		case "SlotType":
			return &SlotTypeClass{}, nil
		case "NetworkSlot":
			return &NetworkSlotClass{}, nil
		case "HintStatus":
			return &HintStatusClass{}, nil
		case "Hint":
			return &HintClass{}, nil
		}
	}
	return nil, fmt.Errorf("unknown class: %s.%s", module, name)
}

func dictGet(d *types.Dict, key string) (any, bool) {
	for _, kv := range *d {
		if s, ok := kv.Key.(string); ok && s == key {
			return kv.Value, true
		}
	}
	return nil, false
}

func toIntDict[V any](d *types.Dict, valFn func(any) (V, error)) (map[int]V, error) {
	out := make(map[int]V, len(*d))
	for _, kv := range *d {
		k, ok := toInt(kv.Key)
		if !ok {
			return nil, fmt.Errorf("expected int key, got %T", kv.Key)
		}
		v, err := valFn(kv.Value)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

func toStringDict[V any](d *types.Dict, valFn func(any) (V, error)) (map[string]V, error) {
	out := make(map[string]V, len(*d))
	for _, kv := range *d {
		k, ok := kv.Key.(string)
		if !ok {
			return nil, fmt.Errorf("expected string key, got %T", kv.Key)
		}
		v, err := valFn(kv.Value)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

func toIntSlice(v any) ([]int, error) {
	switch s := v.(type) {
	case *types.List:
		out := make([]int, len(*s))
		for i, e := range *s {
			n, ok := toInt(e)
			if !ok {
				return nil, fmt.Errorf("expected int, got %T", e)
			}
			out[i] = n
		}
		return out, nil
	case *types.FrozenSet:
		out := make([]int, 0, len(*s))
		for _, e := range *s {
			n, ok := toInt(e)
			if !ok {
				return nil, fmt.Errorf("expected int, got %T", e)
			}
			out = append(out, n)
		}
		return out, nil
	case *types.Set:
		out := make([]int, 0, len(*s))
		for e := range *s {
			n, ok := toInt(e)
			if !ok {
				return nil, fmt.Errorf("expected int, got %T", e)
			}
			out = append(out, n)
		}
		return out, nil
	}
	return nil, fmt.Errorf("expected list/frozenset/set, got %T", v)
}

func toVersion(v any) ([3]int, error) {
	t, ok := v.(*types.Tuple)
	if !ok || len(*t) != 3 {
		return [3]int{}, fmt.Errorf("version must be 3-tuple, got %T", v)
	}
	var out [3]int
	for i, e := range *t {
		n, ok := toInt(e)
		if !ok {
			return [3]int{}, fmt.Errorf("version element must be int, got %T", e)
		}
		out[i] = n
	}
	return out, nil
}

func mapToMultiData(d *types.Dict) (*MultiData, error) {
	md := &MultiData{}
	var err error

	get := func(key string) (any, bool) { return dictGet(d, key) }
	mustDict := func(key string) (*types.Dict, error) {
		v, ok := get(key)
		if !ok {
			return &types.Dict{}, nil
		}
		d, ok := v.(*types.Dict)
		if !ok {
			return nil, fmt.Errorf("%s: expected dict, got %T", key, v)
		}
		return d, nil
	}

	// slot_info: dict[int -> *NetworkSlot]
	raw, err := mustDict("slot_info")
	if err != nil {
		return nil, fmt.Errorf("slot_info: %w", err)
	}
	md.SlotInfo, err = toIntDict(raw, func(v any) (NetworkSlot, error) {
		s, ok := v.(*NetworkSlot)
		if !ok {
			return NetworkSlot{}, fmt.Errorf("expected *NetworkSlot, got %T", v)
		}
		return *s, nil
	})
	if err != nil {
		return nil, fmt.Errorf("slot_info: %w", err)
	}

	// connect_names: dict[string -> [2]int]
	raw, err = mustDict("connect_names")
	if err != nil {
		return nil, fmt.Errorf("connect_names: %w", err)
	}
	md.ConnectNames, err = toStringDict(raw, func(v any) ([2]int, error) {
		t, ok := v.(*types.Tuple)
		if !ok || len(*t) != 2 {
			return [2]int{}, fmt.Errorf("expected 2-tuple, got %T", v)
		}
		a, ok1 := toInt((*t)[0])
		b, ok2 := toInt((*t)[1])
		if !ok1 || !ok2 {
			return [2]int{}, fmt.Errorf("tuple elements must be int")
		}
		return [2]int{a, b}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("connect_names: %w", err)
	}

	// locations: dict[int -> dict[int -> [3]int]]
	raw, err = mustDict("locations")
	if err != nil {
		return nil, fmt.Errorf("locations: %w", err)
	}
	md.Locations, err = toIntDict(raw, func(v any) (map[int][3]int, error) {
		inner, ok := v.(*types.Dict)
		if !ok {
			return nil, fmt.Errorf("expected dict, got %T", v)
		}
		return toIntDict(inner, func(v any) ([3]int, error) {
			t, ok := v.(*types.Tuple)
			if !ok || len(*t) != 3 {
				return [3]int{}, fmt.Errorf("expected 3-tuple, got %T", v)
			}
			var out [3]int
			for i, e := range *t {
				n, ok := toInt(e)
				if !ok {
					return [3]int{}, fmt.Errorf("tuple element must be int, got %T", e)
				}
				out[i] = n
			}
			return out, nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("locations: %w", err)
	}

	// datapackage: dict[string -> GamesPackage]
	raw, err = mustDict("datapackage")
	if err != nil {
		return nil, fmt.Errorf("datapackage: %w", err)
	}
	md.DataPackage, err = toStringDict(raw, func(v any) (GamesPackage, error) {
		inner, ok := v.(*types.Dict)
		if !ok {
			return GamesPackage{}, fmt.Errorf("expected dict, got %T", v)
		}

		gp := GamesPackage{}

		if v, ok := dictGet(inner, "checksum"); ok {
			gp.Checksum, _ = v.(string)
		}

		if v, ok := dictGet(inner, "item_name_to_id"); ok {
			d, ok := v.(*types.Dict)
			if !ok {
				return GamesPackage{}, fmt.Errorf("item_name_to_id: expected dict, got %T", v)
			}
			gp.ItemNameToID, err = toStringDict(d, func(v any) (int, error) {
				n, ok := toInt(v)
				if !ok {
					return 0, fmt.Errorf("expected int, got %T", v)
				}
				return n, nil
			})
			if err != nil {
				return GamesPackage{}, fmt.Errorf("item_name_to_id: %w", err)
			}
		}

		if v, ok := dictGet(inner, "location_name_to_id"); ok {
			d, ok := v.(*types.Dict)
			if !ok {
				return GamesPackage{}, fmt.Errorf("location_name_to_id: expected dict, got %T", v)
			}
			gp.LocationNameToID, err = toStringDict(d, func(v any) (int, error) {
				n, ok := toInt(v)
				if !ok {
					return 0, fmt.Errorf("expected int, got %T", v)
				}
				return n, nil
			})
			if err != nil {
				return GamesPackage{}, fmt.Errorf("location_name_to_id: %w", err)
			}
		}

		return gp, nil
	})
	if err != nil {
		return nil, fmt.Errorf("datapackage: %w", err)
	}

	// precollected_items: dict[int -> []int]
	raw, err = mustDict("precollected_items")
	if err != nil {
		return nil, fmt.Errorf("precollected_items: %w", err)
	}
	md.PrecollectedItems, err = toIntDict(raw, func(v any) ([]int, error) {
		return toIntSlice(v)
	})
	if err != nil {
		return nil, fmt.Errorf("precollected_items: %w", err)
	}

	// precollected_hints: dict[int -> []Hint]
	raw, err = mustDict("precollected_hints")
	if err != nil {
		return nil, fmt.Errorf("precollected_hints: %w", err)
	}
	md.PrecollectedHints, err = toIntDict(raw, func(v any) ([]Hint, error) {
		set, ok := v.(*types.Set)
		if !ok {
			return nil, fmt.Errorf("expected frozenset, got %T", v)
		}
		hints := make([]Hint, 0, len(*set))
		for e := range *set {
			h, ok := e.(*Hint)
			if !ok {
				return nil, fmt.Errorf("expected *Hint, got %T", e)
			}
			hints = append(hints, *h)
		}
		return hints, nil
	})
	if err != nil {
		return nil, fmt.Errorf("precollected_hints: %w", err)
	}

	// spheres: list[dict[int -> list[int]]]
	if v, ok := get("spheres"); ok {
		list, ok := v.(*types.List)
		if !ok {
			return nil, fmt.Errorf("spheres: expected list, got %T", v)
		}
		md.Spheres = make([]map[int][]int, len(*list))
		for i, e := range *list {
			d, ok := e.(*types.Dict)
			if !ok {
				return nil, fmt.Errorf("spheres[%d]: expected dict, got %T", i, e)
			}
			sphere, err := toIntDict(d, func(v any) ([]int, error) {
				return toIntSlice(v)
			})
			if err != nil {
				return nil, fmt.Errorf("spheres[%d]: %w", i, err)
			}
			md.Spheres[i] = sphere
		}
	}

	// version
	if v, ok := get("version"); ok {
		md.Version, err = toVersion(v)
		if err != nil {
			return nil, fmt.Errorf("version: %w", err)
		}
	}

	// seed_name
	if v, ok := get("seed_name"); ok {
		md.SeedName, _ = v.(string)
	}

	// race_mode
	if v, ok := get("race_mode"); ok {
		md.RaceMode, _ = toInt(v)
	}

	// tags: list[string]
	if v, ok := get("tags"); ok {
		if list, ok := v.(*types.List); ok {
			for _, e := range *list {
				s, _ := e.(string)
				md.Tags = append(md.Tags, s)
			}
		}
	}

	if raw, err := mustDict("server_options"); err != nil {
		return nil, fmt.Errorf("server_options: %w", err)
	} else {
		m := make(map[string]any, len(*raw))
		for _, kv := range *raw {
			m[fmt.Sprint(kv.Key)] = kv.Value
		}
		md.ServerOptions = m
	}

	raw, err = mustDict("slot_data")
	if err != nil {
		return nil, fmt.Errorf("slot_data: %w", err)
	}
	md.SlotData, err = toIntDict(raw, func(v any) (map[string]any, error) {
		inner, ok := v.(*types.Dict)
		if !ok {
			return nil, fmt.Errorf("expected dict, got %T", v)
		}
		m := make(map[string]any, len(*inner))
		for _, kv := range *inner {
			m[fmt.Sprint(kv.Key)] = kv.Value
		}
		return m, nil
	})
	if err != nil {
		return nil, fmt.Errorf("slot_data: %w", err)
	}

	if v, ok := get("minimum_versions"); ok {
		mvDict, ok := v.(*types.Dict)
		if !ok {
			return nil, fmt.Errorf("minimum_versions: expected dict, got %T", v)
		}
		if sv, ok := dictGet(mvDict, "server"); ok {
			md.MinimumVersions.Server, err = toVersion(sv)
			if err != nil {
				return nil, fmt.Errorf("minimum_versions.server: %w", err)
			}
		}
		if cv, ok := dictGet(mvDict, "clients"); ok {
			clientsDict, ok := cv.(*types.Dict)
			if !ok {
				return nil, fmt.Errorf("minimum_versions.clients: expected dict, got %T", cv)
			}
			md.MinimumVersions.Clients, err = toIntDict(clientsDict, toVersion)
			if err != nil {
				return nil, fmt.Errorf("minimum_versions.clients: %w", err)
			}
		}
	}

	return md, nil
}

func ParseMultidata(data []byte) (*MultiData, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("multidata too short")
	}

	zlibReader, err := zlib.NewReader(bytes.NewReader(data[1:]))
	if err != nil {
		return nil, fmt.Errorf("multidata: zlib: %w", err)
	}
	defer zlibReader.Close()

	decompressed, err := io.ReadAll(zlibReader)
	if err != nil {
		return nil, fmt.Errorf("multidata: decompress: %w", err)
	}

	unpickler := pickle.NewUnpickler(bytes.NewReader(decompressed))
	unpickler.FindClass = findMultidataClass

	raw, err := unpickler.Load()
	if err != nil {
		return nil, fmt.Errorf("multidata: unpickle: %w", err)
	}
	d, ok := raw.(*types.Dict)
	if !ok {
		return nil, fmt.Errorf("multidata: expected dict at top level, got %T", raw)
	}
	return mapToMultiData(d)
}

func LoadMultiData(path string) (*MultiData, error) {
	var data []byte
	var err error

	if strings.HasSuffix(strings.ToLower(path), ".zip") {
		data, err = readArchipelagoFromZip(path)
		if err != nil {
			return nil, fmt.Errorf("load: %w", err)
		}
	} else {
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("load: read file: %w", err)
		}
	}

	return ParseMultidata(data)
}

func readArchipelagoFromZip(path string) ([]byte, error) {
	r, err := zip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	defer r.Close()

	for _, f := range r.File {
		if strings.HasSuffix(f.Name, ".archipelago") {
			rc, err := f.Open()
			if err != nil {
				return nil, fmt.Errorf("open %s in zip: %w", f.Name, err)
			}
			defer rc.Close()
			data, err := io.ReadAll(rc)
			if err != nil {
				return nil, fmt.Errorf("read %s in zip: %w", f.Name, err)
			}
			return data, nil
		}
	}

	return nil, fmt.Errorf("no .archipelago file found in archive")
}
