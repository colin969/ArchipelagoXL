package main

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStorage() *DataStorage {
	return &DataStorage{
		data:            make(map[string]*StorageEntry),
		keyCount:        make(map[TeamSlot]int),
		readData:        make(map[string]func() StorageValue),
		notifiedClients: make(map[string][]*RegisteredClient),
	}
}

var testSlot = TeamSlot{Team: 0, Slot: 1}

func TestDataStorage(t *testing.T) {
	t.Run("ReadData", func(t *testing.T) {
		t.Run("Get_ReadKey_CallsRegisteredFunc", func(t *testing.T) {
			ds := newDataStorage()
			ds.RegisterReadKey("race_mode", func() StorageValue {
				return int64(1)
			})
			val, ok := ds.Get("_read_race_mode")
			assert.True(t, ok)
			assert.Equal(t, int64(1), val)
		})

		t.Run("Get_ReadKey_ReflectsLiveState", func(t *testing.T) {
			ds := newDataStorage()
			counter := int64(0)
			ds.RegisterReadKey("live_val", func() StorageValue {
				return counter
			})
			val, _ := ds.Get("_read_live_val")
			assert.Equal(t, int64(0), val)

			counter = int64(42)
			val, _ = ds.Get("_read_live_val")
			assert.Equal(t, int64(42), val)
		})

		t.Run("Get_ReadKey_Missing_ReturnsFalse", func(t *testing.T) {
			ds := newDataStorage()
			_, ok := ds.Get("_read_nonexistent")
			assert.False(t, ok)
		})

		t.Run("Get_ReadPrefix_DoesNotFallThroughToStoredData", func(t *testing.T) {
			// Even if stored data has a key that looks like a read key, _read_ prefix
			// must only resolve via readData, not stored data.
			ds := newDataStorage()
			// Manually insert a stored entry with a _read_-like bare key
			ds.mu.Lock()
			ds.data["race_mode"] = &StorageEntry{
				value:       int64(99),
				slotWriters: map[TeamSlot]struct{}{testSlot: {}},
			}
			ds.mu.Unlock()

			// No readData registered — should return false, not the stored value
			_, ok := ds.Get("_read_race_mode")
			assert.False(t, ok)
		})

		t.Run("Set_ReadKeyPrefix_Rejected", func(t *testing.T) {
			ds := newDataStorage()
			_, _, err := ds.Set(testSlot, "_read_race_mode", int64(0), nil, 0, nil)
			assert.ErrorContains(t, err, "read-only")
		})

		t.Run("Set_ReadKeyPrefix_DoesNotCreateEntry", func(t *testing.T) {
			ds := newDataStorage()
			ds.Set(testSlot, "_read_anything", int64(0), nil, 0, nil) //nolint: errcheck
			ds.mu.RLock()
			_, exists := ds.data["_read_anything"]
			ds.mu.RUnlock()
			assert.False(t, exists)
		})

		t.Run("Set_ReadKeyPrefix_DoesNotIncrementKeyCount", func(t *testing.T) {
			ds := newDataStorage()
			ds.Set(testSlot, "_read_anything", int64(0), nil, 0, nil) //nolint: errcheck
			ds.mu.RLock()
			count := ds.keyCount[testSlot]
			ds.mu.RUnlock()
			assert.Equal(t, 0, count)
		})
	})

	t.Run("Set", func(t *testing.T) {
		t.Run("NewKey_DefaultsToZero", func(t *testing.T) {
			ds := newTestStorage()
			_, result, err := ds.Set(testSlot, "key", nil, nil, 0, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(0), result)
		})

		t.Run("NewKey_UsesProvidedDefault", func(t *testing.T) {
			ds := newTestStorage()
			_, result, err := ds.Set(testSlot, "key", int64(42), nil, 0, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(42), result)
		})

		t.Run("ReturnsOriginalBeforeOps", func(t *testing.T) {
			ds := newTestStorage()
			ds.Set(testSlot, "key", int64(10), nil, 0, nil)
			original, result, err := ds.Set(testSlot, "key", nil, []DataStorageOperation{
				{Operation: "add", Value: int64(5)},
			}, 0, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(10), original)
			assert.Equal(t, int64(15), result)
		})

		t.Run("QuotaExceeded", func(t *testing.T) {
			ds := newTestStorage()
			for i := range maxKeysPerSlot {
				_, _, err := ds.Set(testSlot, fmt.Sprintf("key%d", i), int64(0), nil, 0, nil)
				require.NoError(t, err)
			}
			_, _, err := ds.Set(testSlot, "overflow", int64(0), nil, 0, nil)
			assert.ErrorContains(t, err, "quota exceeded")
		})

		t.Run("QuotaNotSharedBetweenSlots", func(t *testing.T) {
			ds := newTestStorage()
			slot2 := TeamSlot{Team: 0, Slot: 2}
			for i := range maxKeysPerSlot {
				_, _, err := ds.Set(testSlot, fmt.Sprintf("key%d", i), int64(0), nil, 0, nil)
				require.NoError(t, err)
			}
			_, _, err := ds.Set(slot2, "slot2key", int64(0), nil, 0, nil)
			assert.NoError(t, err)
		})

		t.Run("SlotWritersTracked", func(t *testing.T) {
			ds := newTestStorage()
			slot2 := TeamSlot{Team: 0, Slot: 2}
			ds.Set(testSlot, "key", int64(0), nil, 0, nil)
			ds.Set(slot2, "key", nil, nil, 0, nil)

			ds.mu.RLock()
			entry := ds.data["key"]
			ds.mu.RUnlock()

			assert.Contains(t, entry.slotWriters, testSlot)
			assert.Contains(t, entry.slotWriters, slot2)
		})

		t.Run("MultipleOpsAppliedInOrder", func(t *testing.T) {
			ds := newTestStorage()
			_, result, err := ds.Set(testSlot, "key", int64(0), []DataStorageOperation{
				{Operation: "add", Value: int64(10)},
				{Operation: "mul", Value: int64(3)},
				{Operation: "add", Value: int64(2)},
			}, 0, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(32), result) // (0+10)*3+2
		})

		t.Run("FailedOpDoesNotMutateEntry", func(t *testing.T) {
			ds := newTestStorage()
			ds.Set(testSlot, "key", int64(5), nil, 0, nil)
			_, _, err := ds.Set(testSlot, "key", nil, []DataStorageOperation{
				{Operation: "add", Value: "wrong type"},
			}, 0, nil)
			assert.Error(t, err)
			val, _ := ds.Get("key")
			assert.Equal(t, int64(5), val)
		})
	})

	// t.Run("SlotSize", func(t *testing.T) {
	// 	t.Run("NewKey_DeniedWhenSizeExceeded", func(t *testing.T) {
	// 		ds := newTestStorage()
	// 		// Fill up to just under the limit with a large string value
	// 		bigVal := strings.Repeat("x", maxSlotSize-10)
	// 		_, _, err := ds.Set(testSlot, "bigkey", bigVal, nil, 0, nil, nil)
	// 		require.NoError(t, err)

	// 		// Next key should be denied — key name + value would exceed limit
	// 		_, _, err = ds.Set(testSlot, "overflow", "hello", nil, 0, nil, nil)
	// 		assert.ErrorContains(t, err, "storage size quota exceeded")
	// 	})

	// 	t.Run("NewWriter_DeniedWhenSizeExceeded", func(t *testing.T) {
	// 		ds := newTestStorage()
	// 		slot2 := TeamSlot{Team: 0, Slot: 2}

	// 		// slot2 fills up their own quota
	// 		bigVal := strings.Repeat("x", maxSlotSize-10)
	// 		_, _, err := ds.Set(slot2, "slot2key", bigVal, nil, 0, nil, nil)
	// 		require.NoError(t, err)

	// 		// testSlot creates a large key
	// 		_, _, err = ds.Set(testSlot, "sharedkey", strings.Repeat("y", 100), nil, 0, nil, nil)
	// 		require.NoError(t, err)

	// 		// slot2 tries to touch testSlot's key — would push them over limit
	// 		_, _, err = ds.Set(slot2, "sharedkey", nil, nil, 0, nil, nil)
	// 		assert.ErrorContains(t, err, "storage size quota exceeded")
	// 	})

	// 	t.Run("SizeNotSharedBetweenSlots", func(t *testing.T) {
	// 		ds := newTestStorage()
	// 		slot2 := TeamSlot{Team: 0, Slot: 2}

	// 		bigVal := strings.Repeat("x", maxSlotSize-10)
	// 		_, _, err := ds.Set(testSlot, "bigkey", bigVal, nil, 0, nil, nil)
	// 		require.NoError(t, err)

	// 		// slot2 should be unaffected by testSlot's usage
	// 		_, _, err = ds.Set(slot2, "slot2key", "small", nil, 0, nil, nil)
	// 		assert.NoError(t, err)
	// 	})

	// 	t.Run("KeyNameCountsTowardSize", func(t *testing.T) {
	// 		ds := newTestStorage()
	// 		// value is tiny but key name pushes it over
	// 		bigKey := strings.Repeat("k", maxSlotSize-3)
	// 		_, _, err := ds.Set(testSlot, bigKey, int64(0), nil, 0, nil, nil)
	// 		require.NoError(t, err)

	// 		_, _, err = ds.Set(testSlot, "overflow", int64(0), nil, 0, nil, nil)
	// 		assert.ErrorContains(t, err, "storage size quota exceeded")
	// 	})
	// })

	t.Run("Get", func(t *testing.T) {
		t.Run("MissingKey", func(t *testing.T) {
			ds := newTestStorage()
			_, ok := ds.Get("missing")
			assert.False(t, ok)
		})

		t.Run("ExistingKey", func(t *testing.T) {
			ds := newTestStorage()
			ds.Set(testSlot, "key", int64(99), nil, 0, nil)
			val, ok := ds.Get("key")
			assert.True(t, ok)
			assert.Equal(t, int64(99), val)
		})
	})

	t.Run("Ops", func(t *testing.T) {
		t.Run("replace", func(t *testing.T) {
			ds := newTestStorage()
			ds.Set(testSlot, "key", int64(1), nil, 0, nil)
			_, result, err := ds.Set(testSlot, "key", nil, []DataStorageOperation{
				{Operation: "replace", Value: int64(99)},
			}, 0, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(99), result)
		})

		t.Run("default_KeyExists", func(t *testing.T) {
			ds := newTestStorage()
			ds.Set(testSlot, "key", int64(5), nil, 0, nil)
			_, result, err := ds.Set(testSlot, "key", nil, []DataStorageOperation{
				{Operation: "default", Value: int64(99)},
			}, 0, nil)
			require.NoError(t, err)
			assert.Equal(t, int64(5), result)
		})

		t.Run("add", func(t *testing.T) {
			t.Run("Int", func(t *testing.T) {
				result, err := dsOperatorAdd(int64(3), int64(4))
				require.NoError(t, err)
				assert.Equal(t, int64(7), result)
			})
			t.Run("NegativeInt", func(t *testing.T) {
				result, err := dsOperatorAdd(int64(10), int64(-3))
				require.NoError(t, err)
				assert.Equal(t, int64(7), result)
			})
			t.Run("OverflowPromotesToBigInt", func(t *testing.T) {
				result, err := dsOperatorAdd(int64(math.MaxInt64), int64(1))
				require.NoError(t, err)
				expected := new(big.Int).Add(big.NewInt(math.MaxInt64), big.NewInt(1))
				assert.Equal(t, expected, result)
			})
			t.Run("BigInt_Plus_Int64", func(t *testing.T) {
				large := bigIntShift(1, 100)
				result, err := dsOperatorAdd(large, int64(1))
				require.NoError(t, err)
				expected := new(big.Int).Add(large, big.NewInt(1))
				assert.Equal(t, expected, result)
			})
			t.Run("BigInt_DemotesWhenFits", func(t *testing.T) {
				// large - large = small: normInt should demote back to int64
				large := bigIntShift(1, 100)
				neg := new(big.Int).Neg(large)
				// large + (-large) + 42 via two ops
				result, err := dsOperatorAdd(large, neg)
				require.NoError(t, err)
				assert.Equal(t, int64(0), result)
			})
			t.Run("BigInt_ExceedsLimit", func(t *testing.T) {
				huge := bigIntShift(1, maxIntBits-1)
				_, err := dsOperatorAdd(huge, huge)
				assert.ErrorContains(t, err, "int limit")
			})
			t.Run("Float", func(t *testing.T) {
				result, err := dsOperatorAdd(float64(1.5), float64(2.5))
				require.NoError(t, err)
				assert.Equal(t, float64(4.0), result)
			})
			t.Run("String", func(t *testing.T) {
				result, err := dsOperatorAdd("hello", " world")
				require.NoError(t, err)
				assert.Equal(t, "hello world", result)
			})
			t.Run("StringExceedsLimit", func(t *testing.T) {
				big := make([]byte, maxStringLen)
				_, err := dsOperatorAdd(string(big), "x")
				assert.ErrorContains(t, err, "string size limit")
			})
			t.Run("List", func(t *testing.T) {
				result, err := dsOperatorAdd([]any{int64(1), int64(2)}, []any{int64(3)})
				require.NoError(t, err)
				assert.Equal(t, []any{int64(1), int64(2), int64(3)}, result)
			})
			t.Run("ListExceedsLimit", func(t *testing.T) {
				big := make([]any, maxListLen)
				_, err := dsOperatorAdd(big, []any{"x"})
				assert.ErrorContains(t, err, "list size limit")
			})
			t.Run("TypeMismatch", func(t *testing.T) {
				_, err := dsOperatorAdd(int64(1), "string")
				assert.Error(t, err)
			})
		})

		t.Run("mul", func(t *testing.T) {
			t.Run("Int", func(t *testing.T) {
				result, err := dsOperatorMul(int64(6), int64(7))
				require.NoError(t, err)
				assert.Equal(t, int64(42), result)
			})
			t.Run("IntByZero", func(t *testing.T) {
				result, err := dsOperatorMul(int64(999), int64(0))
				require.NoError(t, err)
				assert.Equal(t, int64(0), result)
			})
			t.Run("OverflowPromotesToBigInt", func(t *testing.T) {
				result, err := dsOperatorMul(int64(math.MaxInt64), int64(2))
				require.NoError(t, err)
				expected := new(big.Int).Mul(big.NewInt(math.MaxInt64), big.NewInt(2))
				assert.Equal(t, expected, result)
			})
			t.Run("BigInt_Times_Int64", func(t *testing.T) {
				large := bigIntShift(1, 100)
				result, err := dsOperatorMul(large, int64(2))
				require.NoError(t, err)
				expected := new(big.Int).Mul(large, big.NewInt(2))
				assert.Equal(t, expected, result)
			})
			t.Run("BigInt_Times_BigInt", func(t *testing.T) {
				a := bigIntShift(1, 50)
				b := bigIntShift(1, 50)
				result, err := dsOperatorMul(a, b)
				require.NoError(t, err)
				expected := bigIntShift(1, 100)
				assert.Equal(t, expected, result)
			})
			t.Run("BigInt_ExceedsLimit", func(t *testing.T) {
				large := bigIntShift(1, 200)
				_, err := dsOperatorMul(large, bigIntShift(1, 100))
				assert.ErrorContains(t, err, "int limit")
			})
			t.Run("BigInt_DemotesWhenFits", func(t *testing.T) {
				large := bigIntShift(1, 100)
				// large * 0 = 0, should demote
				result, err := dsOperatorMul(large, int64(0))
				require.NoError(t, err)
				assert.Equal(t, int64(0), result)
			})
			t.Run("Float", func(t *testing.T) {
				result, err := dsOperatorMul(float64(2.5), float64(4.0))
				require.NoError(t, err)
				assert.Equal(t, float64(10.0), result)
			})
			t.Run("IntByString", func(t *testing.T) {
				result, err := dsOperatorMul(int64(3), "ab")
				require.NoError(t, err)
				assert.Equal(t, "ababab", result)
			})
			t.Run("StringByInt", func(t *testing.T) {
				result, err := dsOperatorMul("ab", int64(3))
				require.NoError(t, err)
				assert.Equal(t, "ababab", result)
			})
			t.Run("StringByZero", func(t *testing.T) {
				result, err := dsOperatorMul("ab", int64(0))
				require.NoError(t, err)
				assert.Equal(t, "", result)
			})
			t.Run("StringByNegative", func(t *testing.T) {
				result, err := dsOperatorMul("ab", int64(-1))
				require.NoError(t, err)
				assert.Equal(t, "", result)
			})
			t.Run("StringByBigInt", func(t *testing.T) {
				result, err := dsOperatorMul("ab", big.NewInt(3))
				require.NoError(t, err)
				assert.Equal(t, "ababab", result)
			})
			t.Run("StringByBigInt_Negative", func(t *testing.T) {
				result, err := dsOperatorMul("ab", big.NewInt(-1))
				require.NoError(t, err)
				assert.Equal(t, "", result)
			})
			t.Run("StringExceedsLimit", func(t *testing.T) {
				s := strings.Repeat("x", maxStringLen/2+1)
				_, err := dsOperatorMul(s, int64(2))
				assert.ErrorContains(t, err, "string size limit")
			})
			t.Run("TypeMismatch", func(t *testing.T) {
				_, err := dsOperatorMul(int64(1), float64(1))
				assert.Error(t, err)
			})
		})

		t.Run("pow", func(t *testing.T) {
			t.Run("Int", func(t *testing.T) {
				result, err := dsOperatorPow(int64(2), int64(10))
				require.NoError(t, err)
				assert.Equal(t, int64(1024), result)
			})
			t.Run("ZeroExp", func(t *testing.T) {
				result, err := dsOperatorPow(int64(5), int64(0))
				require.NoError(t, err)
				assert.Equal(t, int64(1), result)
			})
			t.Run("NegativeExp", func(t *testing.T) {
				_, err := dsOperatorPow(int64(2), int64(-1))
				assert.Error(t, err)
			})
			t.Run("Overflow", func(t *testing.T) {
				_, err := dsOperatorPow(int64(2), int64(256))
				assert.ErrorContains(t, err, "int limit")
			})
			t.Run("PromotesToBigInt", func(t *testing.T) {
				// 2^64 exceeds int64 but fits in 256 bits
				result, err := dsOperatorPow(int64(2), int64(64))
				require.NoError(t, err)
				expected := bigIntShift(1, 64)
				assert.Equal(t, expected, result)
			})
			t.Run("DemotesWhenFits", func(t *testing.T) {
				// 2^62 fits in int64
				result, err := dsOperatorPow(int64(2), int64(62))
				require.NoError(t, err)
				assert.Equal(t, int64(1<<62), result)
			})
			t.Run("BigIntBase_Rejected", func(t *testing.T) {
				large := bigIntShift(1, 100)
				_, err := dsOperatorPow(large, int64(2))
				assert.ErrorContains(t, err, "too large")
			})
			t.Run("Float", func(t *testing.T) {
				result, err := dsOperatorPow(float64(2.0), float64(0.5))
				require.NoError(t, err)
				assert.InDelta(t, math.Sqrt2, result, 1e-9)
			})
		})

		t.Run("mod", func(t *testing.T) {
			t.Run("Int", func(t *testing.T) {
				result, err := dsOperatorMod(int64(10), int64(3))
				require.NoError(t, err)
				assert.Equal(t, int64(1), result)
			})
			t.Run("ByZero", func(t *testing.T) {
				_, err := dsOperatorMod(int64(10), int64(0))
				assert.ErrorContains(t, err, "mod by zero")
			})
			t.Run("BigInt_Mod_Int64", func(t *testing.T) {
				large := bigIntShift(1, 100)
				result, err := dsOperatorMod(large, int64(3))
				require.NoError(t, err)
				expected := new(big.Int).Mod(large, big.NewInt(3))
				// result fits in int64 so should be demoted
				assert.Equal(t, expected.Int64(), result)
			})
			t.Run("BigInt_Mod_BigInt", func(t *testing.T) {
				a := bigIntShift(1, 100)
				b := bigIntShift(1, 50)
				result, err := dsOperatorMod(a, b)
				require.NoError(t, err)
				assert.Equal(t, int64(0), result) // 2^100 % 2^50 == 0
			})
			t.Run("BigInt_ByZero", func(t *testing.T) {
				large := bigIntShift(1, 100)
				_, err := dsOperatorMod(large, big.NewInt(0))
				assert.ErrorContains(t, err, "mod by zero")
			})
			t.Run("Float", func(t *testing.T) {
				result, err := dsOperatorMod(float64(10.5), float64(3.0))
				require.NoError(t, err)
				assert.InDelta(t, 1.5, result, 1e-9)
			})
		})

		t.Run("floor", func(t *testing.T) {
			t.Run("Float", func(t *testing.T) {
				result, err := dsOperatorFloor(float64(3.9), nil)
				require.NoError(t, err)
				assert.Equal(t, float64(3), result)
			})
			t.Run("Int", func(t *testing.T) {
				result, err := dsOperatorFloor(int64(3), nil)
				require.NoError(t, err)
				assert.Equal(t, int64(3), result)
			})
			t.Run("BigInt", func(t *testing.T) {
				large := bigIntShift(1, 100)
				result, err := dsOperatorFloor(large, nil)
				require.NoError(t, err)
				assert.Equal(t, large, result)
			})
		})

		t.Run("ceil", func(t *testing.T) {
			t.Run("Float", func(t *testing.T) {
				result, err := dsOperatorCeil(float64(3.1), nil)
				require.NoError(t, err)
				assert.Equal(t, float64(4), result)
			})
			t.Run("Int", func(t *testing.T) {
				result, err := dsOperatorCeil(int64(3), nil)
				require.NoError(t, err)
				assert.Equal(t, int64(3), result)
			})
			t.Run("BigInt", func(t *testing.T) {
				large := bigIntShift(1, 100)
				result, err := dsOperatorCeil(large, nil)
				require.NoError(t, err)
				assert.Equal(t, large, result)
			})
		})

		t.Run("max", func(t *testing.T) {
			t.Run("OperandWins", func(t *testing.T) {
				result, err := dsOperatorMax(int64(3), int64(7))
				require.NoError(t, err)
				assert.Equal(t, int64(7), result)
			})
			t.Run("CurrentWins", func(t *testing.T) {
				result, err := dsOperatorMax(int64(10), int64(3))
				require.NoError(t, err)
				assert.Equal(t, int64(10), result)
			})
			t.Run("BigInt_Wins", func(t *testing.T) {
				large := bigIntShift(1, 100)
				result, err := dsOperatorMax(int64(1), large)
				require.NoError(t, err)
				assert.Equal(t, large, result)
			})
			t.Run("Int64_Wins_Over_BigInt", func(t *testing.T) {
				small := big.NewInt(1)
				result, err := dsOperatorMax(int64(100), small)
				require.NoError(t, err)
				assert.Equal(t, int64(100), result)
			})
			t.Run("BigInt_vs_BigInt", func(t *testing.T) {
				a := bigIntShift(1, 100)
				b := bigIntShift(1, 200)
				result, err := dsOperatorMax(a, b)
				require.NoError(t, err)
				assert.Equal(t, b, result)
			})
		})

		t.Run("min", func(t *testing.T) {
			t.Run("Int", func(t *testing.T) {
				result, err := dsOperatorMin(int64(3), int64(7))
				require.NoError(t, err)
				assert.Equal(t, int64(3), result)
			})
			t.Run("Float", func(t *testing.T) {
				result, err := dsOperatorMin(float64(1.5), float64(2.5))
				require.NoError(t, err)
				assert.Equal(t, float64(1.5), result)
			})
			t.Run("Int64_Wins_Over_BigInt", func(t *testing.T) {
				large := bigIntShift(1, 100)
				result, err := dsOperatorMin(int64(1), large)
				require.NoError(t, err)
				assert.Equal(t, int64(1), result)
			})
			t.Run("BigInt_vs_BigInt", func(t *testing.T) {
				a := bigIntShift(1, 100)
				b := bigIntShift(1, 200)
				result, err := dsOperatorMin(a, b)
				require.NoError(t, err)
				assert.Equal(t, a, result)
			})
		})

		t.Run("bitwise", func(t *testing.T) {
			t.Run("and", func(t *testing.T) {
				t.Run("Basic", func(t *testing.T) {
					result, err := dsOperatorAnd(int64(0b1100), int64(0b1010))
					require.NoError(t, err)
					assert.Equal(t, int64(0b1000), result)
				})
				t.Run("BigInt", func(t *testing.T) {
					a := bigIntShift(0b1100, 64)
					b := bigIntShift(0b1010, 64)
					result, err := dsOperatorAnd(a, b)
					require.NoError(t, err)
					assert.Equal(t, bigIntShift(0b1000, 64), result)
				})
				t.Run("Mixed_Int64_BigInt", func(t *testing.T) {
					result, err := dsOperatorAnd(int64(0xFF), big.NewInt(0x0F))
					require.NoError(t, err)
					assert.Equal(t, int64(0x0F), result)
				})
				t.Run("TypeMismatch", func(t *testing.T) {
					_, err := dsOperatorAnd(int64(1), float64(1))
					assert.Error(t, err)
				})
			})

			t.Run("or", func(t *testing.T) {
				t.Run("Basic", func(t *testing.T) {
					result, err := dsOperatorOr(int64(0b1100), int64(0b1010))
					require.NoError(t, err)
					assert.Equal(t, int64(0b1110), result)
				})
				t.Run("BigInt", func(t *testing.T) {
					a := bigIntShift(0b1100, 64)
					b := bigIntShift(0b1010, 64)
					result, err := dsOperatorOr(a, b)
					require.NoError(t, err)
					assert.Equal(t, bigIntShift(0b1110, 64), result)
				})
				t.Run("Mixed_Int64_BigInt", func(t *testing.T) {
					result, err := dsOperatorOr(int64(0xF0), big.NewInt(0x0F))
					require.NoError(t, err)
					assert.Equal(t, int64(0xFF), result)
				})
				t.Run("TypeMismatch", func(t *testing.T) {
					_, err := dsOperatorOr(int64(1), float64(1))
					assert.Error(t, err)
				})
			})

			t.Run("xor", func(t *testing.T) {
				t.Run("Basic", func(t *testing.T) {
					result, err := dsOperatorXor(int64(0b1100), int64(0b1010))
					require.NoError(t, err)
					assert.Equal(t, int64(0b0110), result)
				})
				t.Run("SelfCancels", func(t *testing.T) {
					result, err := dsOperatorXor(int64(0b1111), int64(0b1111))
					require.NoError(t, err)
					assert.Equal(t, int64(0), result)
				})
				t.Run("BigInt_SelfCancels", func(t *testing.T) {
					a := bigIntShift(0b1111, 64)
					result, err := dsOperatorXor(a, a)
					require.NoError(t, err)
					assert.Equal(t, int64(0), result)
				})
				t.Run("BigInt", func(t *testing.T) {
					a := bigIntShift(0b1100, 64)
					b := bigIntShift(0b1010, 64)
					result, err := dsOperatorXor(a, b)
					require.NoError(t, err)
					assert.Equal(t, bigIntShift(0b0110, 64), result)
				})
				t.Run("Mixed_Int64_BigInt", func(t *testing.T) {
					result, err := dsOperatorXor(int64(0xFF), big.NewInt(0x0F))
					require.NoError(t, err)
					assert.Equal(t, int64(0xF0), result)
				})
				t.Run("TypeMismatch", func(t *testing.T) {
					_, err := dsOperatorXor(int64(1), float64(1))
					assert.Error(t, err)
				})
			})

			t.Run("left_shift", func(t *testing.T) {
				t.Run("Basic", func(t *testing.T) {
					result, err := dsOperatorLeftShift(int64(1), int64(4))
					require.NoError(t, err)
					assert.Equal(t, int64(16), result)
				})
				t.Run("ByZero", func(t *testing.T) {
					result, err := dsOperatorLeftShift(int64(5), int64(0))
					require.NoError(t, err)
					assert.Equal(t, int64(5), result)
				})
				t.Run("PromotesToBigInt", func(t *testing.T) {
					// 1 << 63 exceeds int64, should promote
					result, err := dsOperatorLeftShift(int64(1), int64(63))
					require.NoError(t, err)
					assert.Equal(t, bigIntShift(1, 63), result)
				})
				t.Run("BigInt_Lhs", func(t *testing.T) {
					large := bigIntShift(1, 64)
					result, err := dsOperatorLeftShift(large, int64(10))
					require.NoError(t, err)
					assert.Equal(t, bigIntShift(1, 74), result)
				})
				t.Run("NegativeShift", func(t *testing.T) {
					_, err := dsOperatorLeftShift(int64(1), int64(-1))
					assert.Error(t, err)
				})
				t.Run("ShiftExceedsLimit", func(t *testing.T) {
					_, err := dsOperatorLeftShift(int64(1), int64(maxIntBits))
					assert.ErrorContains(t, err, "int limit")
				})
				t.Run("ResultExceedsLimit", func(t *testing.T) {
					large := bigIntShift(1, 200)
					_, err := dsOperatorLeftShift(large, int64(100))
					assert.ErrorContains(t, err, "int limit")
				})
				t.Run("TypeMismatch", func(t *testing.T) {
					_, err := dsOperatorLeftShift(int64(1), float64(1))
					assert.Error(t, err)
				})
			})

			t.Run("right_shift", func(t *testing.T) {
				t.Run("Basic", func(t *testing.T) {
					result, err := dsOperatorRightShift(int64(16), int64(4))
					require.NoError(t, err)
					assert.Equal(t, int64(1), result)
				})
				t.Run("ByZero", func(t *testing.T) {
					result, err := dsOperatorRightShift(int64(5), int64(0))
					require.NoError(t, err)
					assert.Equal(t, int64(5), result)
				})
				t.Run("ShiftBeyondBits", func(t *testing.T) {
					result, err := dsOperatorRightShift(int64(1), int64(maxIntBits))
					require.NoError(t, err)
					assert.Equal(t, int64(0), result)
				})
				t.Run("BigInt_Rhs_ShiftsDown", func(t *testing.T) {
					large := bigIntShift(1, 100)
					result, err := dsOperatorRightShift(large, int64(100))
					require.NoError(t, err)
					assert.Equal(t, int64(1), result) // demotes back to int64
				})
				t.Run("BigInt_Rhs_Partial", func(t *testing.T) {
					large := bigIntShift(1, 100)
					result, err := dsOperatorRightShift(large, int64(50))
					require.NoError(t, err)
					assert.Equal(t, int64(1<<50), result)
				})
				t.Run("NegativeShift", func(t *testing.T) {
					_, err := dsOperatorRightShift(int64(1), int64(-1))
					assert.Error(t, err)
				})
				t.Run("TypeMismatch", func(t *testing.T) {
					_, err := dsOperatorRightShift(int64(1), float64(1))
					assert.Error(t, err)
				})
			})
		})

		t.Run("remove", func(t *testing.T) {
			t.Run("RemovesFirstOccurrence", func(t *testing.T) {
				result, err := dsOperatorRemove([]any{int64(1), int64(2), int64(1), int64(3)}, int64(1))
				require.NoError(t, err)
				assert.Equal(t, []any{int64(2), int64(1), int64(3)}, result)
			})
			t.Run("NotFound_Unchanged", func(t *testing.T) {
				result, err := dsOperatorRemove([]any{int64(1), int64(2)}, int64(99))
				require.NoError(t, err)
				assert.Equal(t, []any{int64(1), int64(2)}, result)
			})
			t.Run("BigInt_Match", func(t *testing.T) {
				large := bigIntShift(1, 100)
				result, err := dsOperatorRemove([]any{int64(1), large, int64(2)}, large)
				require.NoError(t, err)
				assert.Equal(t, []any{int64(1), int64(2)}, result)
			})
			t.Run("BigInt_CrossType_Match", func(t *testing.T) {
				// big.NewInt(1) should match int64(1) via storageValEqual
				result, err := dsOperatorRemove([]any{int64(1), int64(2)}, big.NewInt(1))
				require.NoError(t, err)
				assert.Equal(t, []any{int64(2)}, result)
			})
			t.Run("RequiresList", func(t *testing.T) {
				_, err := dsOperatorRemove(map[string]any{"a": int64(1)}, "a")
				assert.Error(t, err)
			})
		})

		t.Run("pop", func(t *testing.T) {
			t.Run("List_ByIndex", func(t *testing.T) {
				result, err := dsOperatorPop([]any{int64(1), int64(2), int64(3)}, int64(1))
				require.NoError(t, err)
				assert.Equal(t, []any{int64(1), int64(3)}, result)
			})
			t.Run("List_OutOfBounds_Unchanged", func(t *testing.T) {
				result, err := dsOperatorPop([]any{int64(1), int64(2)}, int64(99))
				require.NoError(t, err)
				assert.Equal(t, []any{int64(1), int64(2)}, result)
			})
			t.Run("Dict_ByKey", func(t *testing.T) {
				result, err := dsOperatorPop(map[string]any{"a": int64(1), "b": int64(2)}, "a")
				require.NoError(t, err)
				assert.NotContains(t, result.(map[string]any), "a")
				assert.Contains(t, result.(map[string]any), "b")
			})
			t.Run("Dict_MissingKey_Unchanged", func(t *testing.T) {
				result, err := dsOperatorPop(map[string]any{"a": int64(1)}, "missing")
				require.NoError(t, err)
				assert.Contains(t, result.(map[string]any), "a")
			})
			t.Run("RequiresListOrDict", func(t *testing.T) {
				_, err := dsOperatorPop(int64(1), int64(0))
				assert.Error(t, err)
			})
		})

		t.Run("update", func(t *testing.T) {
			t.Run("List_AddsNewOnly", func(t *testing.T) {
				result, err := dsOperatorUpdate([]any{int64(1), int64(2)}, []any{int64(2), int64(3)})
				require.NoError(t, err)
				assert.Equal(t, []any{int64(1), int64(2), int64(3)}, result)
			})
			t.Run("List_NoDuplicatesWithinEntries", func(t *testing.T) {
				result, err := dsOperatorUpdate([]any{}, []any{int64(1), int64(1), int64(2)})
				require.NoError(t, err)
				assert.Equal(t, []any{int64(1), int64(2)}, result)
			})
			t.Run("Dict_UpdatesExisting", func(t *testing.T) {
				result, err := dsOperatorUpdate(
					map[string]any{"a": int64(1), "b": int64(2)},
					map[string]any{"b": int64(99), "c": int64(3)},
				)
				require.NoError(t, err)
				m := result.(map[string]any)
				assert.Equal(t, int64(1), m["a"])
				assert.Equal(t, int64(99), m["b"])
				assert.Equal(t, int64(3), m["c"])
			})
			t.Run("RequiresListOrDict", func(t *testing.T) {
				_, err := dsOperatorUpdate(int64(1), int64(2))
				assert.Error(t, err)
			})
		})

		t.Run("unknown", func(t *testing.T) {
			_, err := applyOp(int64(0), "explode", int64(1))
			assert.ErrorContains(t, err, "unknown operation")
		})
	})

	t.Run("ShallowCopy", func(t *testing.T) {
		t.Run("List", func(t *testing.T) {
			original := []any{int64(1), int64(2), int64(3)}
			copied := shallowCopy(original).([]any)
			copied[0] = int64(99)
			assert.Equal(t, int64(1), original[0])
		})
		t.Run("Dict", func(t *testing.T) {
			original := map[string]any{"a": int64(1)}
			copied := shallowCopy(original).(map[string]any)
			copied["a"] = int64(99)
			assert.Equal(t, int64(1), original["a"])
		})
		t.Run("Scalar", func(t *testing.T) {
			assert.Equal(t, int64(42), shallowCopy(int64(42)))
			assert.Equal(t, "hello", shallowCopy("hello"))
			assert.Equal(t, float64(3.14), shallowCopy(float64(3.14)))
		})
		t.Run("BigInt", func(t *testing.T) {
			large := bigIntShift(1, 100)
			copied := shallowCopy(large).(*big.Int)
			copied.SetInt64(0)
			assert.Equal(t, bigIntShift(1, 100), large) // original unaffected
		})
	})

	t.Run("StorageSize", func(t *testing.T) {
		t.Run("Int64", func(t *testing.T) {
			assert.Equal(t, 8, storageSize(int64(42)))
		})
		t.Run("BigInt", func(t *testing.T) {
			assert.Equal(t, 13, storageSize(bigIntShift(1, 100)))
		})
		t.Run("Float64", func(t *testing.T) {
			assert.Equal(t, 8, storageSize(float64(3.14)))
		})
		t.Run("Bool", func(t *testing.T) {
			assert.Equal(t, 1, storageSize(true))
		})
		t.Run("String", func(t *testing.T) {
			assert.Equal(t, 5, storageSize("hello"))
		})
		t.Run("Nil", func(t *testing.T) {
			assert.Equal(t, 0, storageSize(nil))
		})
		t.Run("List", func(t *testing.T) {
			// 3 int64s = 24 bytes
			assert.Equal(t, 24, storageSize([]any{int64(1), int64(2), int64(3)}))
		})
		t.Run("Dict", func(t *testing.T) {
			// key "a" (1) + int64 (8) = 9
			assert.Equal(t, 9, storageSize(map[string]any{"a": int64(1)}))
		})
		t.Run("Nested", func(t *testing.T) {
			// key "x" (1) + list of 2 int64s (16) = 17
			assert.Equal(t, 17, storageSize(map[string]any{"x": []any{int64(1), int64(2)}}))
		})
	})

	t.Run("SetNotify", func(t *testing.T) {
		t.Run("NotifiesSubscribedClient", func(t *testing.T) {
			ds := newTestStorage()
			client := newTestClient(1)
			ds.NotifyClient("key", client)

			ds.Set(testSlot, "key", int64(0), nil, 1, nil)
			ds.Set(testSlot, "key", nil, []DataStorageOperation{
				{Operation: "add", Value: int64(5)},
			}, 1, nil)

			assert.Equal(t, 2, clientQueueLen(client))
		})

		t.Run("DoesNotNotifyUnsubscribedClient", func(t *testing.T) {
			ds := newTestStorage()
			client := newTestClient(1)

			ds.Set(testSlot, "key", int64(0), nil, 1, nil)

			assert.Equal(t, 0, clientQueueLen(client))
		})

		t.Run("ReplyClientAddedIfNotSubscribed", func(t *testing.T) {
			ds := newTestStorage()
			client := newTestClient(1)

			ds.Set(testSlot, "key", int64(0), nil, 1, client)

			assert.Equal(t, 1, clientQueueLen(client))
		})

		t.Run("ReplyClientNotDuplicatedIfAlreadySubscribed", func(t *testing.T) {
			ds := newTestStorage()
			client := newTestClient(1)
			ds.NotifyClient("key", client)

			ds.Set(testSlot, "key", int64(0), nil, 1, client)

			assert.Equal(t, 1, clientQueueLen(client))
		})

		t.Run("UnnotifyRemovesClient", func(t *testing.T) {
			ds := newTestStorage()
			client := newTestClient(1)
			ds.NotifyClient("key", client)
			ds.UnnotifyClient("key", client)

			ds.Set(testSlot, "key", int64(0), nil, 1, nil)

			assert.Equal(t, 0, clientQueueLen(client))
		})

		t.Run("MultipleSubscribersAllNotified", func(t *testing.T) {
			ds := newTestStorage()
			c1 := newTestClient(1)
			c2 := newTestClient(2)
			ds.NotifyClient("key", c1)
			ds.NotifyClient("key", c2)

			ds.Set(testSlot, "key", int64(0), nil, 1, nil)

			assert.Equal(t, 1, clientQueueLen(c1))
			assert.Equal(t, 1, clientQueueLen(c2))
		})
	})
}

func newTestClient(slot int) *RegisteredClient {
	return &RegisteredClient{
		Slot:         slot,
		messageQueue: newMessageQueue(),
	}
}

func clientQueueLen(c *RegisteredClient) int {
	c.messageQueue.mu.Lock()
	defer c.messageQueue.mu.Unlock()
	return len(c.messageQueue.sendQueue)
}
func bigIntShift(base int64, shift uint) *big.Int {
	return new(big.Int).Lsh(big.NewInt(base), shift)
}
