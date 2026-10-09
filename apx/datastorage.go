package main

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"slices"
	"strings"
	"sync"

	"github.com/coder/websocket/wsjson"
)

const (
	maxIntBits     = 64
	maxStringLen   = 1 * 1024 * 1024
	maxListLen     = 1 * 1024 * 1024
	maxKeysPerSlot = 5050
	maxSlotSize    = 2 * 1024 * 1024
)

type DataStorage struct {
	mu              sync.RWMutex
	data            map[string]*StorageEntry
	keyCount        map[TeamSlot]int
	readData        map[string]func() StorageValue // for _read_ keys
	notifiedClients map[string][]*RegisteredClient
}

func newDataStorage() *DataStorage {
	return &DataStorage{
		data:     make(map[string]*StorageEntry),
		keyCount: make(map[TeamSlot]int),
		readData: make(map[string]func() StorageValue),
	}
}

type StorageValue = any
type StorageEntry struct {
	value       StorageValue
	slotWriters map[TeamSlot]struct{}
}

type DataStorageOperation struct {
	Operation string       `json:"operation"`
	Value     StorageValue `json:"value"`
}

func (ds *DataStorage) RegisterReadKey(key string, fn func() StorageValue) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.readData[key] = fn
}

func (ds *DataStorage) NotifyClient(key string, client *RegisteredClient) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if slices.Contains(ds.notifiedClients[key], client) {
		return // already subscribed
	}
	client.notifyKeys = append(client.notifyKeys, key)
	ds.notifiedClients[key] = append(ds.notifiedClients[key], client)
}

// No way for a client to unnotify except for disconnecting, so we can ignore the client list for now. I'm sure it'll screw us later.
func (ds *DataStorage) UnnotifyClient(key string, client *RegisteredClient) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	clients := ds.notifiedClients[key]
	for i, c := range clients {
		if c == client {
			ds.notifiedClients[key] = append(clients[:i], clients[i+1:]...)
			if len(ds.notifiedClients[key]) == 0 {
				delete(ds.notifiedClients, key)
			}
			return
		}
	}
}

func (ds *DataStorage) GetNotifyClients(key string) []*RegisteredClient {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	clients := ds.notifiedClients[key]
	if len(clients) == 0 {
		return nil
	}
	clientsCopy := make([]*RegisteredClient, len(clients))
	copy(clientsCopy, clients)
	return clientsCopy
}

func (ds *DataStorage) OnHintsChanged(teamSlot TeamSlot) {

}

func (ds *DataStorage) Get(key string) (StorageValue, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	// Client sends "_read_race_mode", we strip prefix and look up "race_mode"
	if strings.HasPrefix(key, "_read_") {
		bare := key[len("_read_"):]
		if fn, ok := ds.readData[bare]; ok {
			return fn(), true
		}
		return nil, false
	}

	entry, exists := ds.data[key]
	if !exists {
		return nil, false
	}
	return entry.value, true
}

func (ds *DataStorage) Set(owner TeamSlot, key string, defaultVal StorageValue, ops []DataStorageOperation, senderSlot int, replyClient *RegisteredClient, submitOrdered func(func())) (original StorageValue, result StorageValue, err error) {
	if strings.HasPrefix(key, "_read_") {
		return nil, nil, fmt.Errorf("key %q is read-only", key)
	}

	ds.mu.Lock()

	entry, exists := ds.data[key]
	if !exists {
		if ds.keyCount[owner] >= maxKeysPerSlot {
			ds.mu.Unlock()
			return nil, nil, fmt.Errorf("slot key quota exceeded")
		}
		if defaultVal == nil {
			defaultVal = int64(0)
		}
		if ds.slotSize(owner)+storageSize(defaultVal) > maxSlotSize {
			ds.mu.Unlock()
			return nil, nil, fmt.Errorf("slot storage size quota exceeded")
		}
		entry = &StorageEntry{
			slotWriters: map[TeamSlot]struct{}{owner: {}},
			value:       defaultVal,
		}
		ds.data[key] = entry
		ds.keyCount[owner]++
	} else {
		if _, alreadyWriter := entry.slotWriters[owner]; !alreadyWriter {
			// Slot is newly touching this key — charge them for its current size
			if ds.slotSize(owner)+storageSize(entry.value) > maxSlotSize {
				ds.mu.Unlock()
				return nil, nil, fmt.Errorf("slot storage size quota exceeded")
			}
			entry.slotWriters[owner] = struct{}{}
		}
	}

	original = shallowCopy(entry.value)

	value := entry.value
	for _, op := range ops {
		value, err = applyOp(value, op.Operation, op.Value)
		if err != nil {
			ds.mu.Unlock()
			return nil, nil, fmt.Errorf("operation %q failed: %w", op.Operation, err)
		}
	}
	entry.value = value

	if submitOrdered != nil {
		// Get list of clients that want to be notified on change
		// Add sender if wantReply is true
		var notify []*RegisteredClient
		if clients := ds.notifiedClients[key]; len(clients) > 0 {
			notify = make([]*RegisteredClient, len(clients))
			copy(notify, clients)
		}
		if replyClient != nil && !slices.Contains(notify, replyClient) {
			notify = append(notify, replyClient)
		}
		if len(notify) > 0 {
			submitOrdered(func() {
				reply := SetReplyMessage{
					Key:           key,
					OriginalValue: original,
					Value:         value,
					Slot:          senderSlot,
				}

				for _, c := range notify {
					wsjson.Write(context.Background(), c.clientConn, []any{reply})
				}
			})
		}
	}

	ds.mu.Unlock()

	return original, value, nil
}

func (ds *DataStorage) slotSize(slot TeamSlot) int {
	total := 0
	for key, entry := range ds.data {
		if _, ok := entry.slotWriters[slot]; ok {
			total += len(key) + storageSize(entry.value)
		}
	}
	return total
}

func storageSize(v StorageValue) int {
	switch val := v.(type) {
	case int64:
		return 8
	case float64:
		return 8
	case bool:
		return 1
	case string:
		return len(val)
	case []any:
		size := 0
		for _, item := range val {
			size += storageSize(item)
		}
		return size
	case map[string]any:
		size := 0
		for k, item := range val {
			size += len(k) + storageSize(item)
		}
		return size
	case nil:
		return 0
	default:
		return 8 // conservative fallback
	}
}

func shallowCopy(v StorageValue) StorageValue {
	switch val := v.(type) {
	case []any:
		c := make([]any, len(val))
		copy(c, val)
		return c
	case map[string]any:
		c := make(map[string]any, len(val))
		maps.Copy(c, val)
		return c
	default:
		return v
	}
}

func applyOp(current StorageValue, op string, operand StorageValue) (StorageValue, error) {
	switch op {
	case "replace":
		return operand, nil
	case "default":
		// Default is set before we actually get here, no-op
		return current, nil
	case "mul":
		return dsOperatorMul(current, operand)
	case "pow":
		return dsOperatorPow(current, operand)
	case "mod":
		return dsOperatorMod(current, operand)
	case "floor":
		return dsOperatorFloor(current, operand)
	case "ceil":
		return dsOperatorCeil(current, operand)
	case "max":
		return dsOperatorMax(current, operand)
	case "min":
		return dsOperatorMin(current, operand)
	case "add":
		return dsOperatorAdd(current, operand)
	case "and":
		return dsOperatorAnd(current, operand)
	case "or":
		return dsOperatorOr(current, operand)
	case "xor":
		return dsOperatorXor(current, operand)
	case "left_shift":
		return dsOperatorLeftShift(current, operand)
	case "right_shift":
		return dsOperatorRightShift(current, operand)
	case "remove":
		return dsOperatorRemove(current, operand)
	case "pop":
		return dsOperatorPop(current, operand)
	case "update":
		return dsOperatorUpdate(current, operand)

	default:
		return nil, fmt.Errorf("unknown operation: %q", op)
	}
}

func dsOperatorAdd(lhs, rhs StorageValue) (StorageValue, error) {
	switch l := lhs.(type) {
	case int64:
		r, ok := rhs.(int64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for add: expected int64, got %T", rhs)
		}
		result := l + r
		if bits.Len64(uint64(result)) > maxIntBits {
			return nil, fmt.Errorf("result would exceed int bit limit")
		}
		return result, nil
	case float64:
		r, ok := rhs.(float64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for add: expected float64, got %T", rhs)
		}
		return l + r, nil
	case string:
		r, ok := rhs.(string)
		if !ok {
			return nil, fmt.Errorf("type mismatch for add: expected string, got %T", rhs)
		}
		if len(l)+len(r) > maxStringLen {
			return nil, fmt.Errorf("result would exceed string size limit")
		}
		return l + r, nil
	case []any:
		r, ok := rhs.([]any)
		if !ok {
			return nil, fmt.Errorf("type mismatch for add: expected []any, got %T", rhs)
		}
		if len(l)+len(r) > maxListLen {
			return nil, fmt.Errorf("result would exceed list size limit")
		}
		return append(l, r...), nil
	}
	return nil, fmt.Errorf("unsupported type for add: %T", lhs)
}

func dsOperatorMul(lhs, rhs StorageValue) (StorageValue, error) {
	switch l := lhs.(type) {
	case int64:
		r, ok := rhs.(int64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for mul: expected int64, got %T", rhs)
		}
		if l != 0 && r != 0 {
			result := l * r
			if result/l != r {
				return nil, fmt.Errorf("result would exceed int bit limit")
			}
			return result, nil
		}
		return int64(0), nil
	case float64:
		r, ok := rhs.(float64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for mul: expected float64, got %T", rhs)
		}
		return l * r, nil
	}
	return nil, fmt.Errorf("unsupported type for mul: %T", lhs)
}

func dsOperatorPow(lhs, rhs StorageValue) (StorageValue, error) {
	switch l := lhs.(type) {
	case int64:
		r, ok := rhs.(int64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for pow: expected int64, got %T", rhs)
		}
		if r < 0 {
			return nil, fmt.Errorf("pow: negative exponent not supported for integers")
		}
		result := int64(1)
		base := l
		exp := r
		for exp > 0 {
			if exp&1 == 1 {
				prev := result
				result *= base
				if base != 0 && result/base != prev {
					return nil, fmt.Errorf("result would exceed int bit limit")
				}
			}
			exp >>= 1
			if exp > 0 {
				prev := base
				base *= base
				if prev != 0 && base/prev != prev {
					return nil, fmt.Errorf("result would exceed int bit limit")
				}
			}
		}
		return result, nil
	case float64:
		r, ok := rhs.(float64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for pow: expected float64, got %T", rhs)
		}
		return math.Pow(l, r), nil
	}
	return nil, fmt.Errorf("unsupported type for pow: %T", lhs)
}

func dsOperatorMod(lhs, rhs StorageValue) (StorageValue, error) {
	switch l := lhs.(type) {
	case int64:
		r, ok := rhs.(int64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for mod: expected int64, got %T", rhs)
		}
		if r == 0 {
			return nil, fmt.Errorf("mod by zero")
		}
		return l % r, nil
	case float64:
		r, ok := rhs.(float64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for mod: expected float64, got %T", rhs)
		}
		return math.Mod(l, r), nil
	}
	return nil, fmt.Errorf("unsupported type for mod: %T", lhs)
}

func dsOperatorFloor(lhs, _ StorageValue) (StorageValue, error) {
	switch l := lhs.(type) {
	case float64:
		return math.Floor(l), nil
	case int64:
		return l, nil
	}
	return nil, fmt.Errorf("unsupported type for floor: %T", lhs)
}

func dsOperatorCeil(lhs, _ StorageValue) (StorageValue, error) {
	switch l := lhs.(type) {
	case float64:
		return math.Ceil(l), nil
	case int64:
		return l, nil
	}
	return nil, fmt.Errorf("unsupported type for ceil: %T", lhs)
}

func dsOperatorMax(lhs, rhs StorageValue) (StorageValue, error) {
	switch l := lhs.(type) {
	case int64:
		r, ok := rhs.(int64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for max: expected int64, got %T", rhs)
		}
		if r > l {
			return r, nil
		}
		return l, nil
	case float64:
		r, ok := rhs.(float64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for max: expected float64, got %T", rhs)
		}
		return math.Max(l, r), nil
	}
	return nil, fmt.Errorf("unsupported type for max: %T", lhs)
}

func dsOperatorMin(lhs, rhs StorageValue) (StorageValue, error) {
	switch l := lhs.(type) {
	case int64:
		r, ok := rhs.(int64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for min: expected int64, got %T", rhs)
		}
		if r < l {
			return r, nil
		}
		return l, nil
	case float64:
		r, ok := rhs.(float64)
		if !ok {
			return nil, fmt.Errorf("type mismatch for min: expected float64, got %T", rhs)
		}
		return math.Min(l, r), nil
	}
	return nil, fmt.Errorf("unsupported type for min: %T", lhs)
}

func dsOperatorAnd(lhs, rhs StorageValue) (StorageValue, error) {
	l, ok1 := lhs.(int64)
	r, ok2 := rhs.(int64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("and requires int64 operands, got %T and %T", lhs, rhs)
	}
	return l & r, nil
}

func dsOperatorOr(lhs, rhs StorageValue) (StorageValue, error) {
	l, ok1 := lhs.(int64)
	r, ok2 := rhs.(int64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("or requires int64 operands, got %T and %T", lhs, rhs)
	}
	return l | r, nil
}

func dsOperatorXor(lhs, rhs StorageValue) (StorageValue, error) {
	l, ok1 := lhs.(int64)
	r, ok2 := rhs.(int64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("xor requires int64 operands, got %T and %T", lhs, rhs)
	}
	return l ^ r, nil
}

func dsOperatorLeftShift(lhs, rhs StorageValue) (StorageValue, error) {
	l, ok1 := lhs.(int64)
	r, ok2 := rhs.(int64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("left_shift requires int64 operands, got %T and %T", lhs, rhs)
	}
	if r < 0 {
		return nil, fmt.Errorf("left_shift: negative shift amount")
	}
	if r >= maxIntBits {
		return nil, fmt.Errorf("left_shift: shift amount %d exceeds int bit limit", r)
	}
	if l != 0 {
		if bits.Len64(uint64(l))+int(r) >= maxIntBits {
			return nil, fmt.Errorf("result would exceed int bit limit")
		}
	}
	return l << uint(r), nil
}

func dsOperatorRightShift(lhs, rhs StorageValue) (StorageValue, error) {
	l, ok1 := lhs.(int64)
	r, ok2 := rhs.(int64)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("right_shift requires int64 operands, got %T and %T", lhs, rhs)
	}
	if r < 0 {
		return nil, fmt.Errorf("right_shift: negative shift amount")
	}
	if r >= maxIntBits {
		return int64(0), nil // shifting out all bits
	}
	return l >> uint(r), nil
}

func dsOperatorRemove(lhs, rhs StorageValue) (StorageValue, error) {
	list, ok := lhs.([]any)
	if !ok {
		return nil, fmt.Errorf("remove requires a list, got %T", lhs)
	}
	for i, v := range list {
		if v == rhs {
			return append(list[:i], list[i+1:]...), nil
		}
	}
	return list, nil // not found, return unchanged — matches Python's silent pass
}

func dsOperatorPop(lhs, rhs StorageValue) (StorageValue, error) {
	switch container := lhs.(type) {
	case []any:
		idx, ok := rhs.(int64)
		if !ok {
			return nil, fmt.Errorf("pop on list requires int64 index, got %T", rhs)
		}
		if idx < 0 || int(idx) >= len(container) {
			return container, nil // out of bounds, return unchanged
		}
		return append(container[:idx], container[idx+1:]...), nil
	case map[string]any:
		key, ok := rhs.(string)
		if !ok {
			return nil, fmt.Errorf("pop on dict requires string key, got %T", rhs)
		}
		if _, exists := container[key]; !exists {
			return container, nil // key not found, return unchanged
		}
		delete(container, key)
		return container, nil
	}
	return nil, fmt.Errorf("pop requires list or dict, got %T", lhs)
}

func dsOperatorUpdate(lhs, rhs StorageValue) (StorageValue, error) {
	switch container := lhs.(type) {
	case []any:
		entries, ok := rhs.([]any)
		if !ok {
			return nil, fmt.Errorf("update on list requires []any operand, got %T", rhs)
		}
		existing := make(map[any]struct{}, len(container))
		for _, v := range container {
			existing[v] = struct{}{}
		}
		for _, v := range entries {
			if _, found := existing[v]; !found {
				container = append(container, v)
				existing[v] = struct{}{} // prevent dupes within entries itself
			}
		}
		return container, nil
	case map[string]any:
		entries, ok := rhs.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("update on dict requires map[string]any operand, got %T", rhs)
		}
		for k, v := range entries {
			container[k] = v
		}
		return container, nil
	}
	return nil, fmt.Errorf("update requires list or dict, got %T", lhs)
}
