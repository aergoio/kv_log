package kv_log

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestConcurrentAccess(t *testing.T) {
	// Create a test database
	dbPath := "test_concurrent.db"

	// Clean up any existing test database
	os.Remove(dbPath)

	// Open a new database with default settings (no lock)
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
	}()

	// Insert some initial data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		value := fmt.Sprintf("init-value-%d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Number of concurrent operations
	numOps := 100
	// Wait group to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(numOps * 3) // For readers, writers, and deleters

	// Track errors
	var errMutex sync.Mutex
	var errors []string

	// Concurrent readers
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()

			// Read some keys
			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("init-key-%d", j)
				value, err := db.Get([]byte(key))
				if err != nil {
					errMutex.Lock()
					errors = append(errors, fmt.Sprintf("Reader %d failed to get key %s: %v", id, key, err))
					errMutex.Unlock()
				} else {
					expectedPrefix := "init-value-"
					if !bytes.HasPrefix(value, []byte(expectedPrefix)) {
						errMutex.Lock()
						errors = append(errors, fmt.Sprintf("Reader %d got unexpected value for key %s: %s", id, key, string(value)))
						errMutex.Unlock()
					}
				}

				// Brief pause to allow interleaving with other operations
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Concurrent writers
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()

			// Write some keys
			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("writer-%d-key-%d", id, j)
				value := fmt.Sprintf("writer-%d-value-%d", id, j)
				if err := db.Set([]byte(key), []byte(value)); err != nil {
					errMutex.Lock()
					errors = append(errors, fmt.Sprintf("Writer %d failed to set key %s: %v", id, key, err))
					errMutex.Unlock()
				}

				// Brief pause to allow interleaving with other operations
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Concurrent deleters
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()

			// Delete some keys (both existing and non-existing)
			for j := 0; j < 5; j++ {
				var key string
				if j % 2 == 0 && id % 5 == 0 {
					// Occasionally try to delete an initial key
					key = fmt.Sprintf("init-key-%d", (id+j) % 10)
				} else {
					// Try to delete a key that might have been written by a writer
					key = fmt.Sprintf("writer-%d-key-%d", (id+j) % numOps, j)
				}

				if err := db.Delete([]byte(key)); err != nil {
					errMutex.Lock()
					errors = append(errors, fmt.Sprintf("Deleter %d failed to delete key %s: %v", id, key, err))
					errMutex.Unlock()
				}

				// Brief pause to allow interleaving with other operations
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Check if there were any errors
	if len(errors) > 0 {
		for _, err := range errors {
			t.Errorf("%s", err)
		}
		t.Fatalf("Encountered %d errors during concurrent operations", len(errors))
	}

	// Verify the database is still functional
	// Try to read some keys that should still exist
	for i := 0; i < 10; i++ {
		if i % 5 == 0 {
			// These keys might have been deleted
			continue
		}

		key := fmt.Sprintf("init-key-%d", i)
		_, err := db.Get([]byte(key))
		if err != nil {
			t.Logf("Note: Key %s was deleted during concurrent operations", key)
		}
	}

	// Try to write and read a new key
	testKey := "final-test-key"
	testValue := "final-test-value"
	if err := db.Set([]byte(testKey), []byte(testValue)); err != nil {
		t.Fatalf("Failed to set test key after concurrent operations: %v", err)
	}

	value, err := db.Get([]byte(testKey))
	if err != nil {
		t.Fatalf("Failed to get test key after concurrent operations: %v", err)
	}
	if !bytes.Equal(value, []byte(testValue)) {
		t.Fatalf("Value mismatch for test key after concurrent operations: got %s, want %s", string(value), testValue)
	}
}

func TestConcurrentReadersWithExclusiveWriter(t *testing.T) {
	// Create a test database
	dbPath := "test_concurrent_lock.db"

	// Clean up any existing test database
	os.Remove(dbPath)

	// Insert initial data with a writer that has an exclusive lock
	writerDB, err := Open(dbPath, Options{"LockType": LockExclusive})
	if err != nil {
		t.Fatalf("Failed to open database with exclusive lock: %v", err)
	}

	// Insert some initial data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		value := fmt.Sprintf("init-value-%d", i)
		if err := writerDB.Set([]byte(key), []byte(value)); err != nil {
			writerDB.Close()
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Keep the writer open to maintain the exclusive lock
	defer func() {
		writerDB.Close()
		os.Remove(dbPath)
	}()

	// Try to open the same database with a shared lock (should fail)
	_, err = Open(dbPath, Options{"LockType": LockShared})
	if err == nil {
		t.Fatalf("Expected error when opening database with shared lock while exclusive lock is held")
	} else {
		t.Logf("Correctly got error when trying to open with shared lock: %v", err)
	}

	// Try to open the same database with no lock (should succeed for reading)
	readerDB, err := Open(dbPath, Options{"LockType": LockNone})
	if err != nil {
		t.Fatalf("Failed to open database with no lock: %v", err)
	}
	defer readerDB.Close()

	// Try to read from the database
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		value, err := readerDB.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to read key %s with no lock: %v", key, err)
		}
		expectedValue := fmt.Sprintf("init-value-%d", i)
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s: got %s, want %s", key, string(value), expectedValue)
		}
	}

	// Try to write to the database with no lock (should succeed but might corrupt data)
	err = readerDB.Set([]byte("test-key"), []byte("test-value"))
	if err != nil {
		t.Logf("Got error when trying to write with no lock: %v", err)
	} else {
		t.Logf("Warning: Write with no lock succeeded, but this might lead to data corruption")
	}
}

func TestReadOnlyMode(t *testing.T) {
	// Create a test database
	dbPath := "test_readonly.db"

	// Clean up any existing test database
	os.Remove(dbPath)

	// Create and populate the database
	writeDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database for writing: %v", err)
	}

	// Insert some data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := writeDB.Set([]byte(key), []byte(value)); err != nil {
			writeDB.Close()
			t.Fatalf("Failed to set data: %v", err)
		}
	}

	// Close the write database
	if err := writeDB.Close(); err != nil {
		t.Fatalf("Failed to close write database: %v", err)
	}

	// Open the database in read-only mode
	readDB, err := Open(dbPath, Options{"ReadOnly": true})
	if err != nil {
		t.Fatalf("Failed to open database in read-only mode: %v", err)
	}
	defer func() {
		readDB.Close()
		os.Remove(dbPath)
	}()

	// Verify we can read data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value, err := readDB.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to read key %s in read-only mode: %v", key, err)
		}
		expectedValue := fmt.Sprintf("value-%d", i)
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s in read-only mode: got %s, want %s", key, string(value), expectedValue)
		}
	}

	// Try to write data (should fail)
	err = readDB.Set([]byte("new-key"), []byte("new-value"))
	if err == nil {
		t.Fatalf("Expected error when writing in read-only mode, but got nil")
	} else {
		t.Logf("Correctly got error when trying to write in read-only mode: %v", err)
	}

	// Try to delete data (should fail)
	err = readDB.Delete([]byte("key-0"))
	if err == nil {
		t.Fatalf("Expected error when deleting in read-only mode, but got nil")
	} else {
		t.Logf("Correctly got error when trying to delete in read-only mode: %v", err)
	}

	// Test multiple read-only connections
	readDB2, err := Open(dbPath, Options{"ReadOnly": true})
	if err != nil {
		t.Fatalf("Failed to open second read-only connection: %v", err)
	}
	defer readDB2.Close()

	// Verify second connection can also read
	value, err := readDB2.Get([]byte("key-5"))
	if err != nil {
		t.Fatalf("Failed to read from second read-only connection: %v", err)
	}
	if !bytes.Equal(value, []byte("value-5")) {
		t.Fatalf("Value mismatch in second read-only connection: got %s, want %s", string(value), "value-5")
	}
}