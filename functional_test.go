package kv_log

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestDatabaseBasicOperations(t *testing.T) {
	// Create a test database
	dbPath := "test_db.db"

	// Clean up any existing test database
	os.Remove(dbPath)

	// Open a new database
	db, err := Open(dbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		//os.Remove(dbPath) // Clean up after test
	}()

	// Test setting multiple key-value pairs similar to main.go
	err = db.Set([]byte("name"), []byte("hash-table-tree"))
	if err != nil {
		t.Fatalf("Failed to set 'name': %v", err)
	}

	err = db.Set([]byte("author"), []byte("Bernardo"))
	if err != nil {
		t.Fatalf("Failed to set 'author': %v", err)
	}

	err = db.Set([]byte("type"), []byte("key-value database"))
	if err != nil {
		t.Fatalf("Failed to set 'type': %v", err)
	}

	// Test getting the values back
	nameVal, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get 'name': %v", err)
	}
	if !bytes.Equal(nameVal, []byte("hash-table-tree")) {
		t.Fatalf("Value mismatch for 'name': got %s, want %s", string(nameVal), "hash-table-tree")
	}

	authorVal, err := db.Get([]byte("author"))
	if err != nil {
		t.Fatalf("Failed to get 'author': %v", err)
	}
	if !bytes.Equal(authorVal, []byte("Bernardo")) {
		t.Fatalf("Value mismatch for 'author': got %s, want %s", string(authorVal), "Bernardo")
	}

	typeVal, err := db.Get([]byte("type"))
	if err != nil {
		t.Fatalf("Failed to get 'type': %v", err)
	}
	if !bytes.Equal(typeVal, []byte("key-value database")) {
		t.Fatalf("Value mismatch for 'type': got %s, want %s", string(typeVal), "key-value database")
	}

	// Test getting a non-existent key
	_, err = db.Get([]byte("unknown"))
	if err == nil {
		t.Fatalf("Expected error when getting non-existent key, got nil")
	}

	// Test updating an existing key
	err = db.Set([]byte("name"), []byte("hash-table-tree DB"))
	if err != nil {
		t.Fatalf("Failed to update 'name': %v", err)
	}

	// Get updated value
	updatedNameVal, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get updated 'name': %v", err)
	}
	if !bytes.Equal(updatedNameVal, []byte("hash-table-tree DB")) {
		t.Fatalf("Updated value mismatch for 'name': got %s, want %s", string(updatedNameVal), "hash-table-tree DB")
	}

	// Test deleting a key
	err = db.Delete([]byte("author"))
	if err != nil {
		t.Fatalf("Failed to delete 'author': %v", err)
	}

	// Verify the key was deleted
	_, err = db.Get([]byte("author"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'author', got nil")
	}

	// Verify other keys still exist
	nameVal, err = db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get 'name' after deletion: %v", err)
	}
	if !bytes.Equal(nameVal, []byte("hash-table-tree DB")) {
		t.Fatalf("Value mismatch for 'name' after deletion: got %s, want %s", string(nameVal), "hash-table-tree DB")
	}

	// Test deleting a non-existent key (should not error)
	err = db.Delete([]byte("unknown"))
	if err != nil {
		t.Fatalf("Failed to delete non-existent key: %v", err)
	}
}

func TestMultipleKeyValues(t *testing.T) {
	// Create a test database
	dbPath := "test_multi.db"

	// Clean up any existing test database
	os.Remove(dbPath)

	// Open a new database
	db, err := Open(dbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		//os.Remove(dbPath) // Clean up after test
	}()

	// Insert multiple key-value pairs
	numPairs := 1000
	keys := make([][]byte, numPairs)
	values := make([][]byte, numPairs)

	for i := 0; i < numPairs; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("value-%d", i))

		if err := db.Set(keys[i], values[i]); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Verify all keys can be retrieved
	for i := 0; i < numPairs; i++ {
		result, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if !bytes.Equal(result, values[i]) {
			t.Fatalf("Value mismatch for key %d: got %s, want %s", i, string(result), string(values[i]))
		}
	}

	// Update some values
	for i := 0; i < numPairs; i += 50 {
		values[i] = []byte(fmt.Sprintf("updated-value-%d", i))
		if err := db.Set(keys[i], values[i]); err != nil {
			t.Fatalf("Failed to update key %d: %v", i, err)
		}
	}

	// Verify updated keys
	for i := 0; i < numPairs; i += 50 {
		result, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Failed to get updated key %d: %v", i, err)
		}
		if !bytes.Equal(result, values[i]) {
			t.Fatalf("Updated value mismatch for key %d: got %s, want %s", i, string(result), string(values[i]))
		}
	}

	// Delete every third key
	for i := 0; i < numPairs; i += 3 {
		if err := db.Delete(keys[i]); err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < numPairs; i += 3 {
		_, err := db.Get(keys[i])
		if err == nil {
			t.Fatalf("Expected error when getting deleted key %d, got nil", i)
		}
	}

	// Verify non-deleted keys still exist
	for i := 1; i < numPairs; i += 3 {
		result, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Failed to get key %d after deletions: %v", i, err)
		}
		if !bytes.Equal(result, values[i]) {
			t.Fatalf("Value mismatch for key %d after deletions: got %s, want %s", i, string(result), string(values[i]))
		}
	}

	// Test deleting already deleted keys (should not error)
	for i := 0; i < numPairs; i += 6 {
		if err := db.Delete(keys[i]); err != nil {
			t.Fatalf("Failed to delete already deleted key %d: %v", i, err)
		}
	}
}

func TestDeleteOperations(t *testing.T) {
	// Create a test database
	dbPath := "test_delete.db"

	// Clean up any existing test database
	os.Remove(dbPath)

	// Open a new database
	db, err := Open(dbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath) // Clean up after test
	}()

	// Set up some test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	// Insert all test data
	for k, v := range testData {
		err := db.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Verify all data was inserted correctly
	for k, v := range testData {
		result, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", k, err)
		}
		if !bytes.Equal(result, []byte(v)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", k, string(result), v)
		}
	}

	// Test 1: Delete a key and verify it's gone
	err = db.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to delete 'key1': %v", err)
	}

	_, err = db.Get([]byte("key1"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'key1', got nil")
	}

	// Test 2: Delete a key, then try to set it again
	err = db.Delete([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to delete 'key2': %v", err)
	}

	// Verify key2 is deleted
	_, err = db.Get([]byte("key2"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'key2', got nil")
	}

	// Set key2 again with a new value
	err = db.Set([]byte("key2"), []byte("new-value2"))
	if err != nil {
		t.Fatalf("Failed to set 'key2' after deletion: %v", err)
	}

	// Verify key2 has the new value
	result, err := db.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get 'key2' after re-setting: %v", err)
	}
	if !bytes.Equal(result, []byte("new-value2")) {
		t.Fatalf("Value mismatch for 'key2' after re-setting: got %s, want %s", string(result), "new-value2")
	}

	// Test 3: Delete multiple keys
	keysToDelete := []string{"key3", "key4"}
	for _, k := range keysToDelete {
		err := db.Delete([]byte(k))
		if err != nil {
			t.Fatalf("Failed to delete '%s': %v", k, err)
		}
	}

	// Verify deleted keys are gone
	for _, k := range keysToDelete {
		_, err := db.Get([]byte(k))
		if err == nil {
			t.Fatalf("Expected error when getting deleted key '%s', got nil", k)
		}
	}

	// Verify key5 still exists
	result, err = db.Get([]byte("key5"))
	if err != nil {
		t.Fatalf("Failed to get 'key5' after other deletions: %v", err)
	}
	if !bytes.Equal(result, []byte("value5")) {
		t.Fatalf("Value mismatch for 'key5' after other deletions: got %s, want %s", string(result), "value5")
	}

	// Test 4: Delete a non-existent key (should not error)
	err = db.Delete([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Failed to delete non-existent key: %v", err)
	}

	// Test 5: Delete an already deleted key (should not error)
	err = db.Delete([]byte("key3"))
	if err != nil {
		t.Fatalf("Failed to delete already deleted key: %v", err)
	}
}

func TestMainIndexPagesPersistence(t *testing.T) {
	// Create a test database with custom mainIndexPages value
	dbPath := "test_main_index_pages.db"
	customPages := 4 // Use a non-default value

	// Clean up any existing test database
	os.Remove(dbPath)

	// Open a new database with custom mainIndexPages
	db, err := Open(dbPath, Options{"MainIndexPages": customPages})
	if err != nil {
		t.Fatalf("Failed to open database with custom mainIndexPages: %v", err)
	}

	// Add some data to ensure file is written
	err = db.Set([]byte("test_key"), []byte("test_value"))
	if err != nil {
		t.Fatalf("Failed to set test key-value: %v", err)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database without specifying mainIndexPages
	reopenedDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func() {
		reopenedDB.Close()
		os.Remove(dbPath) // Clean up after test
	}()

	// Verify the mainIndexPages value was preserved
	if reopenedDB.mainIndexPages != customPages {
		t.Fatalf("mainIndexPages not preserved: got %d, want %d",
			reopenedDB.mainIndexPages, customPages)
	}

	// Verify data can still be read
	value, err := reopenedDB.Get([]byte("test_key"))
	if err != nil {
		t.Fatalf("Failed to get test key after reopen: %v", err)
	}
	if !bytes.Equal(value, []byte("test_value")) {
		t.Fatalf("Value mismatch after reopen: got %s, want %s",
			string(value), "test_value")
	}
}
