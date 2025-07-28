package kv_log

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

// TestFuzzyRandomOperations performs multiple runs of randomized functional tests
func TestFuzzyRandomOperations(t *testing.T) {
	// Check if SEED is set for reproducing a specific failure
	if seedStr := os.Getenv("SEED"); seedStr != "" {
		seed, err := strconv.ParseInt(seedStr, 10, 64)
		if err != nil {
			t.Fatalf("Invalid SEED value: %v", err)
		}
		t.Logf("Reproducing with seed: %d", seed)
		testFuzzyRandomOperations(t, seed)
		return
	}

	// Normal mode: run 10 times with different seeds
	for i := 0; i < 10; i++ {
		seed := time.Now().UnixNano() + int64(i) // Add iteration to ensure unique seeds
		t.Run(fmt.Sprintf("Run_%d_Seed_%d", i+1, seed), func(t *testing.T) {
			testFuzzyRandomOperations(t, seed)
		})
	}
}

// testFuzzyRandomOperations performs a randomized functional test of
// database operations with transactions, similar to a fuzzy test
func testFuzzyRandomOperations(t *testing.T, seed int64) {
	rand.Seed(seed)
	fmt.Printf("Seed: %d\n", seed)


	// Create a test database
	dbPath := "test_fuzzy.db"

	// Clean up any existing test database
	cleanupTestFiles(dbPath)

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		cleanupTestFiles(dbPath)
	}()

	// Memory database to track expected key-value pairs
	mainDB := make(map[string][]byte) // Main database state
	txnDB := make(map[string][]byte)  // Transaction state
	var currentTxn *Transaction       // Current transaction

	// iterateMemoryDB iterates over all key-value pairs in the memory database,
	// respecting transaction isolation. For each key-value pair, it calls the
	// provided callback function. Keys are sorted for deterministic iteration.
	iterateMemoryDB := func(callback func(key string, value []byte)) {
		usedKeys := make(map[string]bool)

		// First, collect and sort all keys from main state
		var mainKeys []string
		for key := range mainDB {
			mainKeys = append(mainKeys, key)
		}
		sort.Strings(mainKeys)

		// Iterate through sorted main keys
		for _, key := range mainKeys {
			value := mainDB[key]
			// If there's a transaction and it has a newer version, use that instead
			if currentTxn != nil {
				if txnValue, exists := txnDB[key]; exists {
					// Skip deleted entries (nil values)
					if txnValue != nil {
						callback(key, txnValue)
					}
					usedKeys[key] = true
					continue
				}
			}
			// Use the main database value
			callback(key, value)
			usedKeys[key] = true
		}

		// Then collect and sort all transaction-only keys
		if currentTxn != nil {
			var txnOnlyKeys []string
			for key := range txnDB {
				if !usedKeys[key] {
					txnOnlyKeys = append(txnOnlyKeys, key)
				}
			}
			sort.Strings(txnOnlyKeys)

			// Iterate through sorted transaction-only keys
			for _, key := range txnOnlyKeys {
				value := txnDB[key]
				if value != nil {
					callback(key, value)
				}
			}
		}
	}

	// getRandomKeyFromMemoryDB returns a random key from the visible memory database state.
	// Returns empty string if no keys are available.
	// Keys are collected in sorted order for deterministic behavior.
	getRandomKeyFromMemoryDB := func() string {
		var availableKeys []string

		iterateMemoryDB(func(key string, value []byte) {
			availableKeys = append(availableKeys, key)
		})

		if len(availableKeys) == 0 {
			return ""
		}

		// Since iterateMemoryDB already provides sorted keys, this is deterministic
		return availableKeys[rand.Intn(len(availableKeys))]
	}

	// Operations:
	// 0: begin transaction
	// 1: insert new pair
	// 2: update existing pair
	// 3: delete pair
	// 4: commit transaction
	// 5: rollback transaction
	// 6: reopen database
	// 7: get (check all pairs)

	const numOperations = 1000
	for i := 0; i < numOperations; i++ {
		var operation int

		for {
			// Pick an operation
			operation = rand.Intn(8)
			// Decrease probability of rollback, reopen and delete operations
			if operation == 5 || operation == 6 || operation == 3 {
				// Pick another operation
				newOperation := rand.Intn(8)
				// only execute if it matches the previous random value
				if newOperation == operation {
					break
				}
			} else {
				break
			}
		}

		// Log the operation (we'll add more specific details in each case)
		operationNames := []string{"begin", "insert", "update", "delete", "commit", "rollback", "reopen", "get"}
		fmt.Printf("%d: %s", i, operationNames[operation])

		switch operation {
		case 0: // Begin transaction
			if currentTxn == nil {
				fmt.Println()
				currentTxn, err = db.Begin()
				if err != nil {
					t.Fatalf("Failed to begin transaction at operation %d: %v", i, err)
				}
				// Prepare the transaction map
				txnDB = make(map[string][]byte)
			} else {
				fmt.Println(" (already in txn)")
			}

		case 1: // Insert new pair
			fmt.Println()
			// Generate key size (1-255, keys cannot be empty)
			keySize := rand.Intn(255) + 1
			key := make([]byte, keySize)
			rand.Read(key)

			// Generate value size (0-255)
			valueSize := rand.Intn(256) + 1
			value := make([]byte, valueSize)
			rand.Read(value)

			// Set in the appropriate database
			if currentTxn != nil {
				err = currentTxn.Set(key, value)
				if err != nil {
					t.Fatalf("Failed to set key-value in transaction at operation %d: %v", i, err)
				}
				txnDB[string(key)] = value
			} else {
				err = db.Set(key, value)
				if err != nil {
					t.Fatalf("Failed to set key-value at operation %d: %v", i, err)
				}
				mainDB[string(key)] = value
			}

		case 2: // Update existing pair
			// Get a random key from the visible memory database state
			selectedKey := getRandomKeyFromMemoryDB()

			if selectedKey != "" {
				// Check if this key exists in mainDB or only in txnDB
				_, inMainDB := mainDB[selectedKey]
				_, inTxnDB := txnDB[selectedKey]
				if inMainDB && inTxnDB {
					fmt.Printf(" (existing key, in both main+txn) %v\n", currentTxn != nil)
				} else if inMainDB {
					fmt.Printf(" (existing key, in main) %v\n", currentTxn != nil)
				} else if inTxnDB {
					fmt.Printf(" (existing key, in txn only) %v\n", currentTxn != nil)
				} else {
					fmt.Printf(" (existing key, unknown state) %v\n", currentTxn != nil)
				}
				// Generate new value
				valueSize := rand.Intn(256) + 1
				newValue := make([]byte, valueSize)
				rand.Read(newValue)

				// Update in the appropriate database
				if currentTxn != nil {
					err = currentTxn.Set([]byte(selectedKey), newValue)
					if err != nil {
						t.Fatalf("Failed to update key-value in transaction at operation %d: %v", i, err)
					}
					txnDB[selectedKey] = newValue
				} else {
					err = db.Set([]byte(selectedKey), newValue)
					if err != nil {
						t.Fatalf("Failed to update key-value at operation %d: %v", i, err)
					}
					mainDB[selectedKey] = newValue
				}
			} else {
				fmt.Printf(" (no keys to update)\n")
			}

		case 3: // Delete pair
			// 90% chance to delete existing key, 10% chance to delete non-existing key
			shouldDeleteExisting := rand.Intn(10) != 0

			if shouldDeleteExisting {
				// Get a random key from the visible memory database state
				selectedKey := getRandomKeyFromMemoryDB()

				if selectedKey != "" {
					fmt.Printf(" (existing key)\n")
					// Delete from the appropriate database
					if currentTxn != nil {
						err = currentTxn.Delete([]byte(selectedKey))
						if err != nil {
							t.Fatalf("Failed to delete key in transaction at operation %d: %v", i, err)
						}
						txnDB[selectedKey] = nil
					} else {
						err = db.Delete([]byte(selectedKey))
						if err != nil {
							t.Fatalf("Failed to delete key at operation %d: %v", i, err)
						}
						delete(mainDB, selectedKey)
					}
				} else {
					fmt.Printf(" (no keys to delete)\n")
				}
			} else {
				fmt.Printf(" (non-existing key)\n")
				// Delete a non-existing key - generate keys until we find one that doesn't exist
				var nonExistingKey []byte
				for attempts := 0; attempts < 100; attempts++ {
					keySize := rand.Intn(255) + 1
					candidateKey := make([]byte, keySize)
					rand.Read(candidateKey)

					// Check if this key exists in the visible memory database state
					keyStr := string(candidateKey)
					exists := false

					// Check main database
					if _, found := mainDB[keyStr]; found {
						exists = true
					}

					// Check transaction database if in transaction
					if !exists && currentTxn != nil {
						if txnValue, found := txnDB[keyStr]; found && txnValue != nil {
							exists = true
						}
					}

					if !exists {
						nonExistingKey = candidateKey
						break
					}
				}

				// If we couldn't find a non-existing key after 100 attempts, generate a very long unique key
				if nonExistingKey == nil {
					uniqueKey := fmt.Sprintf("non_existing_key_%d_%d", i, rand.Int63())
					nonExistingKey = []byte(uniqueKey)
				}

				// Delete from the appropriate database
				if currentTxn != nil {
					err = currentTxn.Delete(nonExistingKey)
					if err != nil {
						t.Fatalf("Failed to delete non-existing key in transaction at operation %d: %v", i, err)
					}
				} else {
					err = db.Delete(nonExistingKey)
					if err != nil {
						t.Fatalf("Failed to delete non-existing key at operation %d: %v", i, err)
					}
				}
			}

		case 4: // Commit transaction
			if currentTxn != nil {
				fmt.Println()
				err = currentTxn.Commit()
				if err != nil {
					t.Fatalf("Failed to commit transaction at operation %d: %v", i, err)
				}
				// Transfer all pairs from txn map to main map
				for key, value := range txnDB {
					if value == nil {
						delete(mainDB, key)
					} else {
						mainDB[key] = value
					}
				}
				// Clear the transaction map
				txnDB = make(map[string][]byte)
				currentTxn = nil
			} else {
				fmt.Println(" (no active txn)")
			}

		case 5: // Rollback transaction
			if currentTxn != nil {
				fmt.Println()
				err = currentTxn.Rollback()
				if err != nil {
					t.Fatalf("Failed to rollback transaction at operation %d: %v", i, err)
				}
				// Clear the transaction map (discard all changes)
				txnDB = make(map[string][]byte)
				currentTxn = nil
			} else {
				fmt.Println(" (no active txn)")
			}

		case 6: // Reopen database
			// Only allow reopen if not in a transaction
			if currentTxn == nil {
				fmt.Println()
				// Close the database
				err = db.Close()
				if err != nil {
					t.Fatalf("Failed to close database at operation %d: %v", i, err)
				}

				// Reopen the database
				db, err = Open(dbPath)
				if err != nil {
					t.Fatalf("Failed to reopen database at operation %d: %v", i, err)
				}
			} else {
				fmt.Println(" (in txn, skipped)")
			}

		case 7: // Get (check all pairs)
			fmt.Println()
			// Check all key-value pairs using the memory database iterator
			iterateMemoryDB(func(key string, expectedValue []byte) {
				var actualValue []byte
				if currentTxn != nil {
					actualValue, err = currentTxn.Get([]byte(key))
				} else {
					actualValue, err = db.Get([]byte(key))
				}

				if err != nil {
					t.Fatalf("Failed to get key %v (len: %d) at operation %d: %v", []byte(key), len(key), i, err)
				}

				if !bytes.Equal(actualValue, expectedValue) {
					t.Fatalf("Value mismatch for key %v (len: %d) at operation %d: expected %v, got %v",
						[]byte(key), len(key), i, expectedValue, actualValue)
				}
			})

			// Only verify count when not in a transaction (iterator only sees committed data)
			if currentTxn == nil {
				actualCount := 0
				it := db.NewIterator(nil, nil, false)
				for it.Valid() {
					actualCount++
					it.Next()
				}
				it.Close()

				expectedCount := len(mainDB)
				if actualCount != expectedCount {
					t.Fatalf("Entry count mismatch at operation %d: expected %d, got %d",
						i, expectedCount, actualCount)
				}
			}
		}
	}

	// If we're in a transaction at the end, commit it
	if currentTxn != nil {
		err = currentTxn.Commit()
		if err != nil {
			t.Fatalf("Failed to commit final transaction: %v", err)
		}
		// Transfer all pairs from txn map to main map
		for key, value := range txnDB {
			if value == nil {
				delete(mainDB, key)
			} else {
				mainDB[key] = value
			}
		}
	}

	// Final verification
	finalExpectedCount := 0
	iterateMemoryDB(func(key string, expectedValue []byte) {
		finalExpectedCount++

		actualValue, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %v (len: %d) in final verification: %v", []byte(key), len(key), err)
		}

		if !bytes.Equal(actualValue, expectedValue) {
			t.Fatalf("Value mismatch for key %v (len: %d) in final verification: expected %v, got %v",
				[]byte(key), len(key), expectedValue, actualValue)
		}
	})

	// Verify count
	finalActualCount := 0
	it := db.NewIterator(nil, nil, false)
	for it.Valid() {
		finalActualCount++
		it.Next()
	}
	it.Close()

	if finalActualCount != finalExpectedCount {
		t.Fatalf("Final entry count mismatch: expected %d, got %d", finalExpectedCount, finalActualCount)
	}
}
