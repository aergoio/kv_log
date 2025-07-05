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
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
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
					// "key not found" is acceptable due to concurrent deletion
					if err.Error() != "key not found" {
						errMutex.Lock()
						errors = append(errors, fmt.Sprintf("Reader %d failed to get key %s: %v", id, key, err))
						errMutex.Unlock()
					}
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
	// Try to read initial keys - some might have been deleted by concurrent deleters
	keysFound := 0
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("init-key-%d", i)
		value, err := db.Get([]byte(key))
		if err != nil {
			if err.Error() != "key not found" {
				t.Errorf("Unexpected error reading initial key %s after concurrent operations: %v", key, err)
			}
			// "key not found" is expected for some keys due to concurrent deletion
			continue
		}
		keysFound++
		expectedValue := fmt.Sprintf("init-value-%d", i)
		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Errorf("Value mismatch for initial key %s: got %s, want %s", key, string(value), expectedValue)
		}
	}

	// We should find at least some initial keys (not all should be deleted)
	if keysFound == 0 {
		t.Errorf("All initial keys were deleted - this is unexpected")
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

func TestExclusiveAccess(t *testing.T) {
	// Create a test database
	dbPath := "test_exclusive.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database connection
	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open first database connection: %v", err)
	}
	defer func() {
		db1.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Insert some initial data
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := db1.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Try to open a second connection to the same database
	// This should fail because the database only allows one connection at a time
	db2, err := Open(dbPath)

	// The second connection should fail
	if err == nil {
		db2.Close() // Make sure to close it if it somehow succeeded
		t.Fatalf("Expected second database connection to fail, but it succeeded")
	}

	// Verify the first connection still works
	value, err := db1.Get([]byte("key-0"))
	if err != nil {
		t.Fatalf("Failed to read from first connection after attempting second connection: %v", err)
	}
	if !bytes.Equal(value, []byte("value-0")) {
		t.Fatalf("Value mismatch: got %s, want %s", string(value), "value-0")
	}
}

func TestReadOnlyMode(t *testing.T) {
	// Create a test database
	dbPath := "test_readonly.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

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
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
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
	}

	// Try to delete data (should fail)
	err = readDB.Delete([]byte("key-0"))
	if err == nil {
		t.Fatalf("Expected error when deleting in read-only mode, but got nil")
	}

	// Try to open a second read-only connection (should fail due to exclusive access)
	readDB2, err := Open(dbPath, Options{"ReadOnly": true})
	if err == nil {
		readDB2.Close()
		t.Fatalf("Expected second read-only connection to fail, but it succeeded")
	}
}

func TestTransactionWaitsForPreviousToFinish(t *testing.T) {
	dbPath := "test_txn_wait.db"
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Start first transaction
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}

	// Channel to signal when second transaction starts
	secondStarted := make(chan struct{})
	secondAcquired := make(chan struct{})
	secondDone := make(chan struct{})

	// Start second transaction in another goroutine
	go func() {
		close(secondStarted)
		tx2, err := db.Begin()
		if err != nil {
			t.Errorf("Second transaction failed to begin: %v", err)
			return
		}
		close(secondAcquired)
		// Do something in tx2
		err = tx2.Set([]byte("key2"), []byte("val2"))
		if err != nil {
			t.Errorf("Second transaction failed to set: %v", err)
		}
		err = tx2.Commit()
		if err != nil {
			t.Errorf("Second transaction failed to commit: %v", err)
		}
		close(secondDone)
	}()

	// Wait for goroutine to start and attempt Begin
	<-secondStarted
	// Sleep briefly to ensure goroutine is blocked on Begin
	time.Sleep(100 * time.Millisecond)

	select {
	case <-secondAcquired:
		t.Fatalf("Second transaction acquired lock before first committed!")
	default:
		// Expected: second transaction is blocked
	}

	// Commit first transaction
	err = tx1.Set([]byte("key1"), []byte("val1"))
	if err != nil {
		t.Fatalf("First transaction failed to set: %v", err)
	}
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("First transaction failed to commit: %v", err)
	}

	// Now second transaction should proceed
	select {
	case <-secondAcquired:
		// Good: second transaction acquired lock after first committed
	case <-time.After(1 * time.Second):
		t.Fatalf("Second transaction did not acquire lock after first committed")
	}

	<-secondDone

	// Check both keys are present
	val, err := db.Get([]byte("key1"))
	if err != nil || string(val) != "val1" {
		t.Fatalf("key1 not found or value mismatch: %v, %s", err, val)
	}
	val, err = db.Get([]byte("key2"))
	if err != nil || string(val) != "val2" {
		t.Fatalf("key2 not found or value mismatch: %v, %s", err, val)
	}
}
