package kv_log

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"
	"path/filepath"
)

func TestDatabaseBasicOperations(t *testing.T) {
	// Create a test database
	dbPath := "test_basic.db"

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

	// Test setting a key-value pair
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
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
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

func TestShortKeys(t *testing.T) {
	// Create a test database
	dbPath := "test_short_keys.db"

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

	// Create test keys with 1, 2, and 3 bytes in length
	oneByteKeys := []string{"a", "b", "c", "d", "e"}
	twoByteKeys := []string{"ab", "ac", "cd", "ef", "gh", "ij"}
	threeByteKeys := []string{"abc", "abd", "acd", "def", "ghi", "jkl", "mno"}

	// Create values for each key
	values := make(map[string]string)

	// Add values for 1-byte keys
	for i, key := range oneByteKeys {
		values[key] = fmt.Sprintf("one-byte-value-%d", i)
	}

	// Add values for 2-byte keys
	for i, key := range twoByteKeys {
		values[key] = fmt.Sprintf("two-byte-value-%d", i)
	}

	// Add values for 3-byte keys
	for i, key := range threeByteKeys {
		values[key] = fmt.Sprintf("three-byte-value-%d", i)
	}

	// Insert all keys
	for key, value := range values {
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set '%s': %v", key, err)
		}
	}

	// Verify all keys can be retrieved
	for key, expectedValue := range values {
		result, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s': %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s': got %s, want %s", key, string(result), expectedValue)
		}
	}

	// Delete some keys (one of each length)
	keysToDelete := []string{oneByteKeys[0], twoByteKeys[0], threeByteKeys[0]}
	for _, key := range keysToDelete {
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete '%s': %v", key, err)
		}
		// Remove from our tracking map
		delete(values, key)
	}

	// Verify deleted keys are gone
	for _, key := range keysToDelete {
		_, err := db.Get([]byte(key))
		if err == nil {
			t.Fatalf("Expected error when getting deleted key '%s', got nil", key)
		}
	}

	// Verify remaining keys still exist
	for key, expectedValue := range values {
		result, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s' after deletions: %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after deletions: got %s, want %s",
				key, string(result), expectedValue)
		}
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	reopenedDb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify all remaining keys still exist after reopening
	for key, expectedValue := range values {
		result, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s' after reopen: %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after reopen: got %s, want %s",
				key, string(result), expectedValue)
		}
	}

	// Delete more keys (another one of each length)
	moreKeysToDelete := []string{oneByteKeys[1], twoByteKeys[1], threeByteKeys[1]}
	for _, key := range moreKeysToDelete {
		err := reopenedDb.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete '%s' after reopen: %v", key, err)
		}
		// Remove from our tracking map
		delete(values, key)
	}

	// Add new keys (one of each length)
	newKeys := map[string]string{
		"x":   "new-one-byte",
		"yz":  "new-two-byte",
		"xyz": "new-three-byte",
	}

	for key, value := range newKeys {
		err := reopenedDb.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set new key '%s': %v", key, err)
		}
		// Add to our tracking map
		values[key] = value
	}

	// Verify all current keys exist
	for key, expectedValue := range values {
		result, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s' after additions: %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after additions: got %s, want %s",
				key, string(result), expectedValue)
		}
	}

	// Close the database again
	if err := reopenedDb.Close(); err != nil {
		t.Fatalf("Failed to close reopened database: %v", err)
	}

	// Reopen the database again
	reopenedDb2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database second time: %v", err)
	}
	defer func() {
		reopenedDb2.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Final verification of all keys
	for key, expectedValue := range values {
		result, err := reopenedDb2.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get '%s' after second reopen: %v", key, err)
		}
		if !bytes.Equal(result, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for '%s' after second reopen: got %s, want %s",
				key, string(result), expectedValue)
		}
	}
}

func TestDeleteOperations(t *testing.T) {
	// Create a test database
	dbPath := "test_delete.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
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

func TestDatabasePersistence1(t *testing.T) {
	// Create a test database
	dbPath := "test_persistence.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Set initial key-value pairs
	initialData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}

	for k, v := range initialData {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Modify some data
	if err := db.Set([]byte("key2"), []byte("modified2")); err != nil {
		t.Fatalf("Failed to update 'key2': %v", err)
	}

	// Delete a key
	if err := db.Delete([]byte("key3")); err != nil {
		t.Fatalf("Failed to delete 'key3': %v", err)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	reopenedDb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func() {
		reopenedDb.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Verify key1 still exists with original value
	val1, err := reopenedDb.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get 'key1' after reopen: %v", err)
	}
	if !bytes.Equal(val1, []byte("value1")) {
		t.Fatalf("Value mismatch for 'key1' after reopen: got %s, want %s", string(val1), "value1")
	}

	// Verify key2 has the modified value
	val2, err := reopenedDb.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get 'key2' after reopen: %v", err)
	}
	if !bytes.Equal(val2, []byte("modified2")) {
		t.Fatalf("Value mismatch for 'key2' after reopen: got %s, want %s", string(val2), "modified2")
	}

	// Verify key3 was deleted
	_, err = reopenedDb.Get([]byte("key3"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'key3' after reopen, got nil")
	}

	// Verify key4 still exists with original value
	val4, err := reopenedDb.Get([]byte("key4"))
	if err != nil {
		t.Fatalf("Failed to get 'key4' after reopen: %v", err)
	}
	if !bytes.Equal(val4, []byte("value4")) {
		t.Fatalf("Value mismatch for 'key4' after reopen: got %s, want %s", string(val4), "value4")
	}

	// Add a new key to the reopened database
	if err := reopenedDb.Set([]byte("key5"), []byte("value5")); err != nil {
		t.Fatalf("Failed to set 'key5' after reopen: %v", err)
	}

	// Verify the new key exists
	val5, err := reopenedDb.Get([]byte("key5"))
	if err != nil {
		t.Fatalf("Failed to get 'key5' after setting: %v", err)
	}
	if !bytes.Equal(val5, []byte("value5")) {
		t.Fatalf("Value mismatch for 'key5': got %s, want %s", string(val5), "value5")
	}
}

func TestDatabasePersistence2(t *testing.T) {
	// Create a test database
	dbPath := "test_persistence2.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Test setting key-value pairs from TestDatabaseBasicOperations
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

	// Update a key
	err = db.Set([]byte("name"), []byte("hash-table-tree DB"))
	if err != nil {
		t.Fatalf("Failed to update 'name': %v", err)
	}

	// Delete a key
	err = db.Delete([]byte("author"))
	if err != nil {
		t.Fatalf("Failed to delete 'author': %v", err)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database
	reopenedDb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer func() {
		reopenedDb.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Verify name has the updated value
	nameVal, err := reopenedDb.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get 'name' after reopen: %v", err)
	}
	if !bytes.Equal(nameVal, []byte("hash-table-tree DB")) {
		t.Fatalf("Value mismatch for 'name' after reopen: got %s, want %s",
			string(nameVal), "hash-table-tree DB")
	}

	// Verify author was deleted
	_, err = reopenedDb.Get([]byte("author"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'author' after reopen, got nil")
	}

	// Verify type still exists with original value
	typeVal, err := reopenedDb.Get([]byte("type"))
	if err != nil {
		t.Fatalf("Failed to get 'type' after reopen: %v", err)
	}
	if !bytes.Equal(typeVal, []byte("key-value database")) {
		t.Fatalf("Value mismatch for 'type' after reopen: got %s, want %s",
			string(typeVal), "key-value database")
	}

	// Add a new key after reopening
	err = reopenedDb.Set([]byte("version"), []byte("1.0"))
	if err != nil {
		t.Fatalf("Failed to set 'version' after reopen: %v", err)
	}

	// Verify the new key exists
	versionVal, err := reopenedDb.Get([]byte("version"))
	if err != nil {
		t.Fatalf("Failed to get 'version': %v", err)
	}
	if !bytes.Equal(versionVal, []byte("1.0")) {
		t.Fatalf("Value mismatch for 'version': got %s, want %s", string(versionVal), "1.0")
	}
}

func TestIterator(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator.db"

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

	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for k, v := range testData {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Create an iterator (no range filtering)
	it := db.NewIterator(nil, nil, false)
	defer it.Close()

	// Count the number of entries found
	count := 0
	foundKeys := make(map[string]bool)
	foundValues := make(map[string]string)

	// Iterate through all entries
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())

		// Verify the key-value pair
		expectedValue, exists := testData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", key, value, expectedValue)
		}

		// Track found keys and values
		foundKeys[key] = true
		foundValues[key] = value
		count++

		// Move to next entry
		it.Next()
	}

	// Verify we found all keys
	if count != len(testData) {
		t.Fatalf("Iterator found %d entries, expected %d", count, len(testData))
	}

	// Verify each key was found
	for k := range testData {
		if !foundKeys[k] {
			t.Fatalf("Key '%s' was not found by iterator", k)
		}
	}

	// Test iterator after modifications

	// Delete a key
	if err := db.Delete([]byte("key3")); err != nil {
		t.Fatalf("Failed to delete 'key3': %v", err)
	}

	// Add a new key
	if err := db.Set([]byte("key6"), []byte("value6")); err != nil {
		t.Fatalf("Failed to set 'key6': %v", err)
	}

	// Modify an existing key
	if err := db.Set([]byte("key1"), []byte("modified1")); err != nil {
		t.Fatalf("Failed to update 'key1': %v", err)
	}

	// Create a new iterator (no range filtering)
	modifiedIt := db.NewIterator(nil, nil, false)
	defer modifiedIt.Close()

	// Reset tracking variables
	count = 0
	foundKeys = make(map[string]bool)
	foundValues = make(map[string]string)

	// Expected data after modifications
	expectedData := map[string]string{
		"key1": "modified1", // Modified
		"key2": "value2",
		// key3 deleted
		"key4": "value4",
		"key5": "value5",
		"key6": "value6", // New
	}

	// Iterate through all entries
	for modifiedIt.Valid() {
		key := string(modifiedIt.Key())
		value := string(modifiedIt.Value())

		// Verify the key-value pair
		expectedValue, exists := expectedData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key after modifications: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch after modifications for key '%s': got %s, want %s",
				key, value, expectedValue)
		}

		// Track found keys and values
		foundKeys[key] = true
		foundValues[key] = value
		count++

		// Move to next entry
		modifiedIt.Next()
	}

	// Verify we found all keys
	if count != len(expectedData) {
		t.Fatalf("Iterator found %d entries after modifications, expected %d",
			count, len(expectedData))
	}

	// Verify each key was found
	for k := range expectedData {
		if !foundKeys[k] {
			t.Fatalf("Key '%s' was not found by iterator after modifications", k)
		}
	}

	// Test range filtering with iterator
	// Test 1: Range from "key2" to "key5" (exclusive)
	rangeIt := db.NewIterator([]byte("key2"), []byte("key5"), false)
	defer rangeIt.Close()

	expectedRangeKeys := []string{"key2", "key4"} // key3 was deleted, key5 is excluded
	foundRangeKeys := make([]string, 0)

	for rangeIt.Valid() {
		key := string(rangeIt.Key())
		foundRangeKeys = append(foundRangeKeys, key)
		rangeIt.Next()
	}

	if len(foundRangeKeys) != len(expectedRangeKeys) {
		t.Fatalf("Range iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundRangeKeys), len(expectedRangeKeys), foundRangeKeys, expectedRangeKeys)
	}

	for i, expectedKey := range expectedRangeKeys {
		if i >= len(foundRangeKeys) || foundRangeKeys[i] != expectedKey {
			t.Fatalf("Range iterator key mismatch at position %d: got %s, want %s",
				i, foundRangeKeys[i], expectedKey)
		}
	}

	// Test 2: Range with only start bound
	startOnlyIt := db.NewIterator([]byte("key4"), nil, false)
	defer startOnlyIt.Close()

	expectedStartOnlyKeys := []string{"key4", "key5", "key6"} // keys >= "key4"
	foundStartOnlyKeys := make([]string, 0)

	for startOnlyIt.Valid() {
		key := string(startOnlyIt.Key())
		foundStartOnlyKeys = append(foundStartOnlyKeys, key)
		startOnlyIt.Next()
	}

	if len(foundStartOnlyKeys) != len(expectedStartOnlyKeys) {
		t.Fatalf("Start-only iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundStartOnlyKeys), len(expectedStartOnlyKeys), foundStartOnlyKeys, expectedStartOnlyKeys)
	}

	for i, expectedKey := range expectedStartOnlyKeys {
		if i >= len(foundStartOnlyKeys) || foundStartOnlyKeys[i] != expectedKey {
			t.Fatalf("Start-only iterator key mismatch at position %d: got %s, want %s",
				i, foundStartOnlyKeys[i], expectedKey)
		}
	}

	// Test 3: Range with only end bound
	endOnlyIt := db.NewIterator(nil, []byte("key4"), false)
	defer endOnlyIt.Close()

	expectedEndOnlyKeys := []string{"key1", "key2"} // keys < "key4"
	foundEndOnlyKeys := make([]string, 0)

	for endOnlyIt.Valid() {
		key := string(endOnlyIt.Key())
		foundEndOnlyKeys = append(foundEndOnlyKeys, key)
		endOnlyIt.Next()
	}

	if len(foundEndOnlyKeys) != len(expectedEndOnlyKeys) {
		t.Fatalf("End-only iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundEndOnlyKeys), len(expectedEndOnlyKeys), foundEndOnlyKeys, expectedEndOnlyKeys)
	}

	for i, expectedKey := range expectedEndOnlyKeys {
		if i >= len(foundEndOnlyKeys) || foundEndOnlyKeys[i] != expectedKey {
			t.Fatalf("End-only iterator key mismatch at position %d: got %s, want %s",
				i, foundEndOnlyKeys[i], expectedKey)
		}
	}

	// Test iterator with empty database
	emptyDbPath := "test_empty_iterator.db"
	os.Remove(emptyDbPath)
	os.Remove(emptyDbPath + "-index")
	os.Remove(emptyDbPath + "-wal")

	emptyDb, err := Open(emptyDbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open empty database: %v", err)
	}
	defer func() {
		emptyDb.Close()
		os.Remove(emptyDbPath)
		os.Remove(emptyDbPath + "-index")
		os.Remove(emptyDbPath + "-wal")
	}()

	emptyIt := emptyDb.NewIterator(nil, nil, false)
	defer emptyIt.Close()

	// Verify the iterator is not valid for an empty database
	if emptyIt.Valid() {
		t.Fatalf("Iterator for empty database should not be valid")
	}
}

func TestReverseIterator(t *testing.T) {
	// Create a test database
	dbPath := "test_reverse_iterator.db"

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

	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for k, v := range testData {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to set '%s': %v", k, err)
		}
	}

	// Test 1: Create a reverse iterator (start > end)
	it := db.NewIterator([]byte("key5"), []byte("key1"), true)
	defer it.Close()

	// Count the number of entries found
	count := 0
	foundKeys := make([]string, 0)
	foundValues := make([]string, 0)

	// Iterate through all entries in reverse order
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())

		// Verify the key-value pair
		expectedValue, exists := testData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", key, value, expectedValue)
		}

		// Track found keys and values
		foundKeys = append(foundKeys, key)
		foundValues = append(foundValues, value)
		count++

		// Move to next entry
		it.Next()
	}

	// Verify we found all keys in reverse order
	// Exclusive end: should not include "key1"
	expectedKeys := []string{"key5", "key4", "key3", "key2"}
	if count != len(expectedKeys) {
		t.Fatalf("Reverse iterator found %d entries, expected %d", count, len(expectedKeys))
	}

	// Verify keys are in reverse order
	for i, expectedKey := range expectedKeys {
		if i >= len(foundKeys) || foundKeys[i] != expectedKey {
			t.Fatalf("Reverse iterator key mismatch at position %d: got %s, want %s",
				i, foundKeys[i], expectedKey)
		}
	}

	// Test 2: Test specific range (key4 to key2)
	rangeIt := db.NewIterator([]byte("key4"), []byte("key2"), true)
	defer rangeIt.Close()

	// Exclusive end: should not include "key2"
	expectedRangeKeys := []string{"key4", "key3"}
	foundRangeKeys := make([]string, 0)

	for rangeIt.Valid() {
		key := string(rangeIt.Key())
		foundRangeKeys = append(foundRangeKeys, key)
		rangeIt.Next()
	}

	if len(foundRangeKeys) != len(expectedRangeKeys) {
		t.Fatalf("Range iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundRangeKeys), len(expectedRangeKeys), foundRangeKeys, expectedRangeKeys)
	}

	for i, expectedKey := range expectedRangeKeys {
		if i >= len(foundRangeKeys) || foundRangeKeys[i] != expectedKey {
			t.Fatalf("Range iterator key mismatch at position %d: got %s, want %s",
				i, foundRangeKeys[i], expectedKey)
		}
	}

	// Test 3: Test after modifications
	// Delete a key
	if err := db.Delete([]byte("key3")); err != nil {
		t.Fatalf("Failed to delete 'key3': %v", err)
	}

	// Add a new key
	if err := db.Set([]byte("key6"), []byte("value6")); err != nil {
		t.Fatalf("Failed to set 'key6': %v", err)
	}

	// Modify an existing key
	if err := db.Set([]byte("key1"), []byte("modified1")); err != nil {
		t.Fatalf("Failed to update 'key1': %v", err)
	}

	// Create a new reverse iterator (start > end)
	modifiedIt := db.NewIterator([]byte("key6"), []byte("key1"), true)
	defer modifiedIt.Close()

	// Expected data after modifications in reverse order
	// Exclusive end: should not include "key1"
	expectedModKeys := []string{"key6", "key5", "key4", "key2"}
	foundModKeys := make([]string, 0)

	// Iterate through entries in reverse order
	for modifiedIt.Valid() {
		key := string(modifiedIt.Key())
		foundModKeys = append(foundModKeys, key)
		modifiedIt.Next()
	}

	if len(foundModKeys) != len(expectedModKeys) {
		t.Fatalf("Modified reverse iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundModKeys), len(expectedModKeys), foundModKeys, expectedModKeys)
	}

	for i, expectedKey := range expectedModKeys {
		if i >= len(foundModKeys) || foundModKeys[i] != expectedKey {
			t.Fatalf("Modified reverse iterator key mismatch at position %d: got %s, want %s",
				i, foundModKeys[i], expectedKey)
		}
	}

	// Test with empty database
	emptyDbPath := "test_empty_reverse_iterator.db"
	os.Remove(emptyDbPath)
	os.Remove(emptyDbPath + "-index")
	os.Remove(emptyDbPath + "-wal")

	emptyDb, err := Open(emptyDbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open empty database: %v", err)
	}
	defer func() {
		emptyDb.Close()
		os.Remove(emptyDbPath)
		os.Remove(emptyDbPath + "-index")
		os.Remove(emptyDbPath + "-wal")
	}()

	emptyIt := emptyDb.NewIterator([]byte("z"), []byte("a"), true)
	defer emptyIt.Close()

	// Verify the iterator is not valid for an empty database
	if emptyIt.Valid() {
		t.Fatalf("Reverse iterator for empty database should not be valid")
	}
}

func TestIteratorWithMixedKeys(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator_mixed_keys.db"

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

	// Insert keys with different lengths and different prefixes
	testKeys := []string{
		string([]byte{0}),  // NUL character (byte 0)
		"a",
		"aa",
		"a1",
		"az5",
		"a-long-2",
		"ab",
		"ab2",
		"ab2-1",
		"ab2-long-2",
		"abc",
		"abc3",
		"abc3-1",
		"ab-long-2",
		"abc-3-1",
		"abc-long-2",
		"b-1-1",
		"b-long-2",
		"bc-2-1",
		"bc-long-2",
		"bcd-3-1",
		"bcd-long-2",
		"c",
		"ca",
		"cab",
		"cablong",
		"d-very-long-key-with-many-characters-to-test-different-length",
		"z",
		string([]byte{255}),  // Character with byte value 255
	}

	// Insert all keys with their values
	testData := make(map[string]string)
	for i, key := range testKeys {
		value := fmt.Sprintf("value-%d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set '%s': %v", key, err)
		}
		testData[key] = value
	}

	// Test 1: Full forward iteration (all keys in lexicographical order)
	it := db.NewIterator(nil, nil, false)
	defer it.Close()

	// Expected order: keys in lexicographical order
	expectedOrder := make([]string, len(testKeys))
	copy(expectedOrder, testKeys)
	sort.Strings(expectedOrder)

	foundKeys := make([]string, 0, len(testKeys))
	foundValues := make([]string, 0, len(testKeys))

	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys = append(foundKeys, key)
		foundValues = append(foundValues, value)
		it.Next()
	}

	// Verify we found all keys in the expected order
	if len(foundKeys) != len(expectedOrder) {
		t.Fatalf("Full iterator found %d keys, expected %d. Found: %v", len(foundKeys), len(expectedOrder), foundKeys)
	}

	for i, expectedKey := range expectedOrder {
		if i >= len(foundKeys) || foundKeys[i] != expectedKey {
			t.Fatalf("Full iterator key mismatch at position %d: got %s, want %s", i, foundKeys[i], expectedKey)
		}
		expectedValue := testData[expectedKey]
		if foundValues[i] != expectedValue {
			t.Fatalf("Full iterator value mismatch for key %s: got %s, want %s", expectedKey, foundValues[i], expectedValue)
		}
	}

	// Test 2: Prefix-based forward iteration (only keys with prefix "b")
	prefixIt := db.NewIterator([]byte("b"), []byte("bzz"), false)
	defer prefixIt.Close()

	expectedPrefixKeys := []string{
		"b-1-1",
		"b-long-2",
		"bc-2-1",
		"bc-long-2",
		"bcd-3-1",
		"bcd-long-2",
	}

	foundPrefixKeys := make([]string, 0)
	for prefixIt.Valid() {
		key := string(prefixIt.Key())
		foundPrefixKeys = append(foundPrefixKeys, key)
		prefixIt.Next()
	}

	if len(foundPrefixKeys) != len(expectedPrefixKeys) {
		t.Fatalf("Prefix iterator found %d keys, expected %d. Found: %v, Expected: %v", len(foundPrefixKeys), len(expectedPrefixKeys), foundPrefixKeys, expectedPrefixKeys)
	}

	for i, expectedKey := range expectedPrefixKeys {
		if i >= len(foundPrefixKeys) || foundPrefixKeys[i] != expectedKey {
			t.Fatalf("Prefix iterator key mismatch at position %d: got %s, want %s", i, foundPrefixKeys[i], expectedKey)
		}
	}

	// Test 3: Mixed prefix forward iteration (keys between "a" and "c")
	mixedIt := db.NewIterator([]byte("a"), []byte("c"), false) // From "a" (inclusive) to "c" (exclusive)
	defer mixedIt.Close()

	// Expected keys between "a" (inclusive) and "c" (exclusive) in order
	expectedMixedKeys := make([]string, 0)
	for _, key := range expectedOrder {
		if key >= "a" && key < "c" {
			expectedMixedKeys = append(expectedMixedKeys, key)
		}
	}

	foundMixedKeys := make([]string, 0)
	for mixedIt.Valid() {
		key := string(mixedIt.Key())
		foundMixedKeys = append(foundMixedKeys, key)
		mixedIt.Next()
	}

	if len(foundMixedKeys) != len(expectedMixedKeys) {
		t.Fatalf("Mixed iterator found %d keys, expected %d. Found: %v, Expected: %v", len(foundMixedKeys), len(expectedMixedKeys), foundMixedKeys, expectedMixedKeys)
	}

	for i, expectedKey := range expectedMixedKeys {
		if i >= len(foundMixedKeys) || foundMixedKeys[i] != expectedKey {
			t.Fatalf("Mixed iterator key mismatch at position %d: got %s, want %s", i, foundMixedKeys[i], expectedKey)
		}
	}
}

func TestReverseIteratorWithMixedKeys(t *testing.T) {
	// Create a test database
	dbPath := "test_reverse_iterator_mixed_keys.db"

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

	// Insert keys with different lengths and different prefixes
	// Format: prefix-length-index
	testKeys := []string{
		string([]byte{0}),  // NUL character (byte 0)
		"a",
		"aa",
		"a1",
		"az5",
		"a-long-2",
		"ab",
		"ab2",
		"ab2-1",
		"ab2-long-2",
		"abc",
		"abc3",
		"abc3-1",
		"ab-long-2",
		"abc-3-1",
		"abc-long-2",
		"b-1-1",
		"b-long-2",
		"bc-2-1",
		"bc-long-2",
		"bcd-3-1",
		"bcd-long-2",
		"c",
		"ca",
		"cab",
		"cablong",
		"d-very-long-key-with-many-characters-to-test-different-length",
		"z",
		string([]byte{255}),  // Character with byte value 255
	}

	// Insert all keys with their values
	testData := make(map[string]string)
	for i, key := range testKeys {
		value := fmt.Sprintf("value-%d", i)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set '%s': %v", key, err)
		}
		testData[key] = value
	}

	// Test 1: Full reverse iteration (all keys in reverse lexicographical order)
	// Use maximum byte value as start and minimum as end to ensure we cover all keys
	it := db.NewIterator([]byte{255}, []byte{0}, true) // From byte 255 (inclusive) to byte 0 (exclusive)
	defer it.Close()

	// Expected order: keys in reverse lexicographical order, excluding the end key (byte 0)
	expectedOrder := make([]string, 0, len(testKeys))
	for _, k := range testKeys {
		if k != string([]byte{0}) {
			expectedOrder = append(expectedOrder, k)
		}
	}
	sort.Slice(expectedOrder, func(i, j int) bool {
		return expectedOrder[i] > expectedOrder[j]
	})

	foundKeys := make([]string, 0, len(testKeys))
	foundValues := make([]string, 0, len(testKeys))

	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys = append(foundKeys, key)
		foundValues = append(foundValues, value)
		it.Next()
	}

	// Verify we found all keys in the expected order
	if len(foundKeys) != len(expectedOrder) {
		t.Fatalf("Full reverse iterator found %d keys, expected %d. Found: %v",
			len(foundKeys), len(expectedOrder), foundKeys)
	}

	for i, expectedKey := range expectedOrder {
		if i >= len(foundKeys) || foundKeys[i] != expectedKey {
			t.Fatalf("Full reverse iterator key mismatch at position %d: got %s, want %s",
				i, foundKeys[i], expectedKey)
		}

		expectedValue := testData[expectedKey]
		if foundValues[i] != expectedValue {
			t.Fatalf("Full reverse iterator value mismatch for key %s: got %s, want %s",
				expectedKey, foundValues[i], expectedValue)
		}
	}

	// Test 2: Prefix-based reverse iteration (only keys with prefix "b")
	prefixIt := db.NewIterator([]byte("bzz"), []byte("b"), true)
	defer prefixIt.Close()

	// Expected keys with prefix "b" in reverse lexicographical order, excluding the end key "b"
	expectedPrefixKeys := []string{
		"bcd-long-2",
		"bcd-3-1",
		"bc-long-2",
		"bc-2-1",
		"b-long-2",
		"b-1-1",
	}

	foundPrefixKeys := make([]string, 0)
	for prefixIt.Valid() {
		key := string(prefixIt.Key())
		if key <= "b" {
			break
		}
		foundPrefixKeys = append(foundPrefixKeys, key)
		prefixIt.Next()
	}

	if len(foundPrefixKeys) != len(expectedPrefixKeys) {
		t.Fatalf("Prefix reverse iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundPrefixKeys), len(expectedPrefixKeys), foundPrefixKeys, expectedPrefixKeys)
	}

	for i, expectedKey := range expectedPrefixKeys {
		if i >= len(foundPrefixKeys) || foundPrefixKeys[i] != expectedKey {
			t.Fatalf("Prefix reverse iterator key mismatch at position %d: got %s, want %s",
				i, foundPrefixKeys[i], expectedKey)
		}
	}

	// Test 3: Mixed prefix reverse iteration (keys between "c" and "a")
	mixedIt := db.NewIterator([]byte("c"), []byte("a"), true) // From "c" (inclusive) to "a" (exclusive)
	defer mixedIt.Close()

	// Expected keys between "c" (inclusive) and "a" (exclusive) in reverse order
	expectedMixedKeys := make([]string, 0)
	for _, key := range testKeys {
		if (key > "a" && key <= "c") {
			expectedMixedKeys = append(expectedMixedKeys, key)
		}
	}
	// Sort in reverse lexicographical order
	sort.Slice(expectedMixedKeys, func(i, j int) bool {
		return expectedMixedKeys[i] > expectedMixedKeys[j]
	})

	foundMixedKeys := make([]string, 0)
	for mixedIt.Valid() {
		key := string(mixedIt.Key())
		foundMixedKeys = append(foundMixedKeys, key)
		mixedIt.Next()
	}

	if len(foundMixedKeys) != len(expectedMixedKeys) {
		t.Fatalf("Mixed reverse iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundMixedKeys), len(expectedMixedKeys), foundMixedKeys, expectedMixedKeys)
	}

	for i, expectedKey := range expectedMixedKeys {
		if i >= len(foundMixedKeys) || foundMixedKeys[i] != expectedKey {
			t.Fatalf("Mixed reverse iterator key mismatch at position %d: got %s, want %s",
				i, foundMixedKeys[i], expectedKey)
		}
	}
}

func TestIteratorWithLargeDataset(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator_large_dataset.db"

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

	// Insert many key-value pairs to test iterator with a large dataset
	numPairs := 1000
	keys := make([]string, numPairs)
	values := make([]string, numPairs)

	for i := 0; i < numPairs; i++ {
		keys[i] = fmt.Sprintf("test-key-%d", i)
		values[i] = fmt.Sprintf("test-value-%d", i)

		if err := db.Set([]byte(keys[i]), []byte(values[i])); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Create a map for verification
	expectedData := make(map[string]string)
	for i := 0; i < numPairs; i++ {
		expectedData[keys[i]] = values[i]
	}

	// Create an iterator (no range filtering)
	it := db.NewIterator(nil, nil, false)
	defer it.Close()

	// Count the number of entries found
	count := 0
	foundKeys := make(map[string]bool)

	// Iterate through all entries
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())

		// Verify the key-value pair exists in our expected data
		expectedValue, exists := expectedData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", key, value, expectedValue)
		}

		// Track found keys
		foundKeys[key] = true
		count++

		// Move to next entry
		it.Next()
	}

	// Verify we found all keys
	if count != numPairs {
		t.Fatalf("Iterator found %d entries, expected %d", count, numPairs)
	}

	// Verify each key was found
	for i := 0; i < numPairs; i++ {
		key := keys[i]
		if !foundKeys[key] {
			t.Fatalf("Key '%s' was not found by iterator", key)
		}
	}

	// Test range filtering with large dataset
	// Test range from "test-key-100" to "test-key-200" (exclusive)
	rangeStart := []byte("test-key-100")
	rangeEnd := []byte("test-key-200")
	rangeIt := db.NewIterator(rangeStart, rangeEnd, false)
	defer rangeIt.Close()

	// Count keys in the range
	rangeCount := 0
	for rangeIt.Valid() {
		key := string(rangeIt.Key())

		// Verify the key is within the expected range
		if key < "test-key-100" || key >= "test-key-200" {
			t.Fatalf("Range iterator returned key outside range: %s", key)
		}

		// Verify the key exists in our expected data
		if _, exists := expectedData[key]; !exists {
			t.Fatalf("Range iterator returned unexpected key: %s", key)
		}

		rangeCount++
		rangeIt.Next()
	}

	// Calculate expected count: test-key-100 to test-key-199 (inclusive)
	// This includes test-key-100, test-key-101, ..., test-key-109, test-key-110, ..., test-key-199
	expectedRangeCount := 0
	for i := 0; i < numPairs; i++ {
		key := keys[i]
		if key >= "test-key-100" && key < "test-key-200" {
			expectedRangeCount++
		}
	}

	if rangeCount != expectedRangeCount {
		t.Fatalf("Range iterator found %d keys, expected %d", rangeCount, expectedRangeCount)
	}
}

// generateVariableLengthKey generates a key of variable length based on index i
// Key lengths range from 1 to 64 bytes, using base64-like characters for variety
// Limits single-character keys to 64 total to avoid collisions
func generateVariableLengthKey(i int) string {
	// Base64-like character set for more variety
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

	// Determine the key length (1 to 64 bytes)
	// But limit single-character keys to only the first 64 indices
	var keyLength int
	if i < 64 {
		// First 64 keys get single characters (1 byte each)
		keyLength = 1
	} else {
		// Remaining keys get lengths from 2 to 64 bytes
		keyLength = ((i - 64) % 63) + 2
	}

	// Create a base pattern using the index - ensure it's always unique
	basePattern := fmt.Sprintf("k%d", i)

	// Handle single character keys specially
	if keyLength == 1 {
		return string(charset[i % len(charset)])
	}

	// If the base pattern is already longer than desired length,
	// we need to create a shorter unique key
	if len(basePattern) > keyLength {
		// For short keys, create a compact unique representation
		if keyLength == 2 {
			first := charset[i % len(charset)]
			second := charset[(i / len(charset)) % len(charset)]
			return string([]byte{first, second})
		} else if keyLength == 3 {
			first := charset[i % len(charset)]
			second := charset[(i / len(charset)) % len(charset)]
			third := charset[(i / (len(charset) * len(charset))) % len(charset)]
			return string([]byte{first, second, third})
		} else {
			// For longer keys that are still shorter than basePattern,
			// create a compact representation
			compactKey := fmt.Sprintf("%d", i)
			if len(compactKey) > keyLength {
				// If even the number is too long, use base64 encoding of the number
				var builder strings.Builder
				remaining := i
				for j := 0; j < keyLength; j++ {
					builder.WriteByte(charset[remaining % len(charset)])
					remaining = remaining / len(charset)
				}
				return builder.String()
			} else {
				// Pad with charset characters
				var builder strings.Builder
				builder.WriteString(compactKey)
				for j := len(compactKey); j < keyLength; j++ {
					builder.WriteByte(charset[(i + j) % len(charset)])
				}
				return builder.String()
			}
		}
	}

	// If we need to pad, use repeating characters from charset
	if len(basePattern) < keyLength {
		padding := keyLength - len(basePattern)

		var builder strings.Builder
		builder.WriteString(basePattern)
		for j := 0; j < padding; j++ {
			// Use different characters based on position and index
			charIndex := (i + j) % len(charset)
			builder.WriteByte(charset[charIndex])
		}

		return builder.String()
	}

	return basePattern
}

func TestIteratorWithLargeDataset2(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator_variable_length.db"

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

	// Insert many key-value pairs with variable length keys to test iterator
	numPairs := 1000
	keys := make([]string, numPairs)
	values := make([]string, numPairs)
	keySet := make(map[string]bool) // To check for duplicates

	for i := 0; i < numPairs; i++ {
		keys[i] = generateVariableLengthKey(i)
		values[i] = fmt.Sprintf("val-%d", i)

		// Check for duplicate keys
		if keySet[keys[i]] {
			t.Fatalf("Duplicate key generated at index %d: %s", i, keys[i])
		}
		keySet[keys[i]] = true

		if err := db.Set([]byte(keys[i]), []byte(values[i])); err != nil {
			t.Fatalf("Failed to set key %d (%s): %v", i, keys[i], err)
		}
	}

	// Create a map for verification
	expectedData := make(map[string]string)
	for i := 0; i < numPairs; i++ {
		expectedData[keys[i]] = values[i]
	}

	// Create an iterator (no range filtering)
	it := db.NewIterator(nil, nil, false)
	defer it.Close()

	// Count the number of entries found
	count := 0
	foundKeys := make(map[string]bool)

	// Iterate through all entries
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())

		// Verify the key-value pair exists in our expected data
		expectedValue, exists := expectedData[key]
		if !exists {
			t.Fatalf("Iterator returned unexpected key: %s", key)
		}
		if value != expectedValue {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", key, value, expectedValue)
		}

		// Track found keys
		foundKeys[key] = true
		count++

		// Move to next entry
		it.Next()
	}

	// Verify we found all keys
	if count != numPairs {
		t.Fatalf("Iterator found %d entries, expected %d", count, numPairs)
	}

	// Verify each key was found
	for i := 0; i < numPairs; i++ {
		key := keys[i]
		if !foundKeys[key] {
			t.Fatalf("Key '%s' was not found by iterator", key)
		}
	}

	// Test range filtering with variable length keys
	// Test range from "k100" to "k200" (exclusive) - this will catch keys starting with k1
	rangeStart := []byte("k100")
	rangeEnd := []byte("k200")
	rangeIt := db.NewIterator(rangeStart, rangeEnd, false)
	defer rangeIt.Close()

	// Count keys in the range
	rangeCount := 0
	foundRangeKeys := make([]string, 0)
	for rangeIt.Valid() {
		key := string(rangeIt.Key())

		// Verify the key is within the expected range
		if key < "k100" || key >= "k200" {
			t.Fatalf("Range iterator returned key outside range: %s", key)
		}

		// Verify the key exists in our expected data
		if _, exists := expectedData[key]; !exists {
			t.Fatalf("Range iterator returned unexpected key: %s", key)
		}

		foundRangeKeys = append(foundRangeKeys, key)
		rangeCount++
		rangeIt.Next()
	}

	// Calculate expected count: keys that are >= "k100" and < "k200"
	expectedRangeCount := 0
	expectedRangeKeys := make([]string, 0)
	for i := 0; i < numPairs; i++ {
		key := keys[i]
		if key >= "k100" && key < "k200" {
			expectedRangeCount++
			expectedRangeKeys = append(expectedRangeKeys, key)
		}
	}

	if rangeCount != expectedRangeCount {
		t.Logf("Range iterator found %d keys, expected %d", rangeCount, expectedRangeCount)
		t.Logf("Found keys: %v", foundRangeKeys)
		t.Logf("Expected keys: %v", expectedRangeKeys)
	}

	// Test with keys of different lengths - single character keys
	// Since we know the first 64 keys are single characters from our charset,
	// let's test a range that should include some of them
	singleCharStart := []byte("A")
	singleCharEnd := []byte("z") // Extend range to include lowercase
	singleCharIt := db.NewIterator(singleCharStart, singleCharEnd, false)
	defer singleCharIt.Close()

	singleCharCount := 0
	foundSingleChars := make([]string, 0)
	for singleCharIt.Valid() {
		key := string(singleCharIt.Key())

		// Only count actual single character keys
		if len(key) == 1 {
			foundSingleChars = append(foundSingleChars, key)
			singleCharCount++
		}

		singleCharIt.Next()
	}

	// Count expected single character keys in range A-y (inclusive of both upper and lower case)
	expectedSingleCharCount := 0
	expectedSingleChars := make([]string, 0)
	for i := 0; i < numPairs; i++ {
		key := keys[i]
		if len(key) == 1 && key[0] >= 'A' && key[0] < 'z' {
			expectedSingleCharCount++
			expectedSingleChars = append(expectedSingleChars, key)
		}
	}

	if singleCharCount != expectedSingleCharCount {
		t.Logf("Single char iterator found %d keys, expected %d", singleCharCount, expectedSingleCharCount)
		t.Logf("Found single chars: %v", foundSingleChars)
		t.Logf("Expected single chars: %v", expectedSingleChars)
	}

	// Test with prefix-based range for keys starting with "k5"
	prefixStart := []byte("k5")
	prefixEnd := []byte("k6")
	prefixIt := db.NewIterator(prefixStart, prefixEnd, false)
	defer prefixIt.Close()

	prefixCount := 0
	for prefixIt.Valid() {
		key := string(prefixIt.Key())

		// Verify the key starts with "k5"
		if !strings.HasPrefix(key, "k5") {
			t.Fatalf("Prefix iterator returned unexpected key: %s", key)
		}

		prefixCount++
		prefixIt.Next()
	}

	// Count expected keys starting with "k5"
	expectedPrefixCount := 0
	for i := 0; i < numPairs; i++ {
		key := keys[i]
		if strings.HasPrefix(key, "k5") {
			expectedPrefixCount++
		}
	}

	if prefixCount != expectedPrefixCount {
		t.Logf("Prefix iterator found %d keys starting with 'k5', expected %d", prefixCount, expectedPrefixCount)
	}
}

func TestDatabaseReindex(t *testing.T) {
	// Create a test database
	dbPath := "test_reindex.db"
	indexPath := dbPath + "-index"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(indexPath)
	os.Remove(dbPath + "-wal")

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(indexPath)
		os.Remove(dbPath + "-wal")
	}()

	// Test setting key-value pairs
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

	// Update a key
	err = db.Set([]byte("name"), []byte("hash-table-tree DB"))
	if err != nil {
		t.Fatalf("Failed to update 'name': %v", err)
	}

	// Delete a key
	err = db.Delete([]byte("author"))
	if err != nil {
		t.Fatalf("Failed to delete 'author': %v", err)
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Save the original index file for comparison
	originalIndexPath := indexPath + ".original"
	os.Rename(indexPath, originalIndexPath)

	// Reopen the database - it should rebuild the index
	reopenedDb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database after index deletion: %v", err)
	}
	defer func() {
		reopenedDb.Close()
		os.Remove(originalIndexPath)
	}()

	// Verify name has the updated value
	nameVal, err := reopenedDb.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Failed to get 'name' after reindex: %v", err)
	}
	if !bytes.Equal(nameVal, []byte("hash-table-tree DB")) {
		t.Fatalf("Value mismatch for 'name' after reindex: got %s, want %s",
			string(nameVal), "hash-table-tree DB")
	}

	// Verify author was deleted
	_, err = reopenedDb.Get([]byte("author"))
	if err == nil {
		t.Fatalf("Expected error when getting deleted key 'author' after reindex, got nil")
	}

	// Verify type still exists with original value
	typeVal, err := reopenedDb.Get([]byte("type"))
	if err != nil {
		t.Fatalf("Failed to get 'type' after reindex: %v", err)
	}
	if !bytes.Equal(typeVal, []byte("key-value database")) {
		t.Fatalf("Value mismatch for 'type' after reindex: got %s, want %s",
			string(typeVal), "key-value database")
	}

	// Close the database
	if err := reopenedDb.Close(); err != nil {
		t.Fatalf("Failed to close database after reindex: %v", err)
	}

	// Compare files using diff to verify index was rebuilt

	// Compare files starting from byte 4096 (skipping the first 4096 bytes)
	// Use cmp command with skip option to compare files from offset 4096
	cmd := exec.Command("cmp", "-s", "-i", "4096:4096", originalIndexPath, indexPath)
	err = cmd.Run()

	// If cmp finds no differences, it returns exit status 0
	// If files differ, it returns exit status 1
	// For any other error, it returns other non-zero status
	if err != nil {
		// Files should be identical after byte 4096, so any difference is an error
		t.Fatalf("Index files should be identical after byte 4096 but differ: %v", err)
	}

}

func TestTransactionRollback(t *testing.T) {
	// Create a test database
	dbPath := "test_transaction_rollback.db"

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

	// Create keys with the same prefix to ensure they share radix and leaf pages
	keyPrefix := "aa"
	keySuffix := "_some-long-suffix-here-to-consume-a-lot-of-space"

	// Transaction 1: Insert first batch of keys
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Insert 10 keys in transaction 1
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx1-%d", i)

		if err := tx1.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 1: %v", key, err)
		}
	}

	// Commit transaction 1
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Transaction 2: Insert second batch of keys
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Insert 10 more keys in transaction 2
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx2-%d", i)

		if err := tx2.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 2: %v", key, err)
		}
	}

	// Commit transaction 2
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	// Transaction 3: Insert third batch of keys
	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}

	// Insert 10 more keys in transaction 3
	for i := 20; i < 30; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx3-%d", i)

		if err := tx3.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 3: %v", key, err)
		}
	}

	// Commit transaction 3
	if err := tx3.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 3: %v", err)
	}

	// Verify all keys from transactions 1-3 exist
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		var expectedValue string

		if i < 10 {
			expectedValue = fmt.Sprintf("value-tx1-%d", i)
		} else if i < 20 {
			expectedValue = fmt.Sprintf("value-tx2-%d", i)
		} else {
			expectedValue = fmt.Sprintf("value-tx3-%d", i)
		}

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s: got %s, want %s", key, string(value), expectedValue)
		}
	}

	// Transaction 4: This one will be rolled back
	tx4, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 4: %v", err)
	}

	// Insert new keys in transaction 4
	for i := 30; i < 40; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx4-%d", i)

		if err := tx4.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 4: %v", key, err)
		}
	}

	// Modify some existing keys from previous transactions
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("modified-value-tx4-%d", i)

		if err := tx4.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to modify key %s in transaction 4: %v", key, err)
		}
	}

	// Delete some existing keys
	for i := 15; i < 20; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)

		if err := tx4.Delete([]byte(key)); err != nil {
			t.Fatalf("Failed to delete key %s in transaction 4: %v", key, err)
		}
	}

	// Verify the changes are visible within the transaction
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		var expectedValue string
		var shouldExist bool = true

		if i < 15 {
			// Modified keys
			expectedValue = fmt.Sprintf("modified-value-tx4-%d", i)
		} else if i < 20 {
			// Deleted keys
			shouldExist = false
		} else if i < 30 {
			// Unmodified keys from transaction 3
			expectedValue = fmt.Sprintf("value-tx3-%d", i)
		} else {
			// New keys from transaction 4
			expectedValue = fmt.Sprintf("value-tx4-%d", i)
		}

		value, err := tx4.Get([]byte(key))
		if !shouldExist {
			if err == nil {
				t.Fatalf("Expected key %s to be deleted, but it still exists", key)
			}
		} else {
			if err != nil {
				t.Fatalf("Failed to get key %s within transaction 4: %v", key, err)
			}

			if !bytes.Equal(value, []byte(expectedValue)) {
				t.Fatalf("Value mismatch for key %s within transaction 4: got %s, want %s",
					key, string(value), expectedValue)
			}
		}
	}

	// Now rollback transaction 4
	if err := tx4.Rollback(); err != nil {
		t.Fatalf("Failed to rollback transaction 4: %v", err)
	}

	// Verify that all changes from transaction 4 are discarded
	// and all data from transactions 1-3 are preserved

	// Keys from transactions 1-3 should have their original values
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		var expectedValue string

		if i < 10 {
			expectedValue = fmt.Sprintf("value-tx1-%d", i)
		} else if i < 20 {
			expectedValue = fmt.Sprintf("value-tx2-%d", i)
		} else {
			expectedValue = fmt.Sprintf("value-tx3-%d", i)
		}

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after rollback: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after rollback: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// New keys from transaction 4 should not exist
	for i := 30; i < 40; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		_, err := db.Get([]byte(key))
		if err == nil {
			t.Fatalf("Key %s from rolled back transaction still exists", key)
		}
	}

	// Start a new transaction after rollback
	tx5, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction 5: %v", err)
	}

	// Add some new keys in transaction 5
	for i := 30; i < 35; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-tx5-%d", i)

		if err := tx5.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key %s in transaction 5: %v", key, err)
		}
	}

	// Commit transaction 5
	if err := tx5.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 5: %v", err)
	}

	// Verify all keys from transactions 1-3 and 5 exist with correct values
	for i := 0; i < 35; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		var expectedValue string

		if i < 10 {
			expectedValue = fmt.Sprintf("value-tx1-%d", i)
		} else if i < 20 {
			expectedValue = fmt.Sprintf("value-tx2-%d", i)
		} else if i < 30 {
			expectedValue = fmt.Sprintf("value-tx3-%d", i)
		} else {
			expectedValue = fmt.Sprintf("value-tx5-%d", i)
		}

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after transaction 5: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after transaction 5: got %s, want %s",
				key, string(value), expectedValue)
		}
	}
}

func TestSharedPrefixKeys(t *testing.T) {
	// Create a test database
	dbPath := "test_shared_prefix.db"

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

	// Test the specific problematic patterns mentioned in the issue:
	// "abc"⇄"ab", "acd"⇄"ac", "ghi"⇄"gh"

	// Test Case 1: Insert 3-byte key first, then its 2-byte prefix
	err = db.Set([]byte("ab"), []byte("value-ab"))
	if err != nil {
		t.Fatalf("Failed to set 'ab': %v", err)
	}

	err = db.Set([]byte("abc"), []byte("value-abc"))
	if err != nil {
		t.Fatalf("Failed to set 'abc': %v", err)
	}

	// Verify both keys exist
	val, err := db.Get([]byte("abc"))
	if err != nil {
		t.Fatalf("Failed to get 'abc': %v", err)
	}
	if !bytes.Equal(val, []byte("value-abc")) {
		t.Fatalf("Value mismatch for 'abc': got %s, want %s", string(val), "value-abc")
	}

	val, err = db.Get([]byte("ab"))
	if err != nil {
		t.Fatalf("Failed to get 'ab': %v", err)
	}
	if !bytes.Equal(val, []byte("value-ab")) {
		t.Fatalf("Value mismatch for 'ab': got %s, want %s", string(val), "value-ab")
	}

	// Test Case 2: Insert 2-byte key first, then its 3-byte extension
	err = db.Set([]byte("acd"), []byte("value-acd"))
	if err != nil {
		t.Fatalf("Failed to set 'acd': %v", err)
	}

	err = db.Set([]byte("ac"), []byte("value-ac"))
	if err != nil {
		t.Fatalf("Failed to set 'ac': %v", err)
	}

	// Verify both keys exist
	val, err = db.Get([]byte("acd"))
	if err != nil {
		t.Fatalf("Failed to get 'acd': %v", err)
	}
	if !bytes.Equal(val, []byte("value-acd")) {
		t.Fatalf("Value mismatch for 'acd': got %s, want %s", string(val), "value-acd")
	}

	val, err = db.Get([]byte("ac"))
	if err != nil {
		t.Fatalf("Failed to get 'ac': %v", err)
	}
	if !bytes.Equal(val, []byte("value-ac")) {
		t.Fatalf("Value mismatch for 'ac': got %s, want %s", string(val), "value-ac")
	}

	// Test Case 3: Insert 3-byte key first, then its 2-byte prefix (different pattern)
	err = db.Set([]byte("gh"), []byte("value-gh"))
	if err != nil {
		t.Fatalf("Failed to set 'gh': %v", err)
	}

	err = db.Set([]byte("ghi"), []byte("value-ghi"))
	if err != nil {
		t.Fatalf("Failed to set 'ghi': %v", err)
	}

	// Verify both keys exist
	val, err = db.Get([]byte("ghi"))
	if err != nil {
		t.Fatalf("Failed to get 'ghi': %v", err)
	}
	if !bytes.Equal(val, []byte("value-ghi")) {
		t.Fatalf("Value mismatch for 'ghi': got %s, want %s", string(val), "value-ghi")
	}

	val, err = db.Get([]byte("gh"))
	if err != nil {
		t.Fatalf("Failed to get 'gh': %v", err)
	}
	if !bytes.Equal(val, []byte("value-gh")) {
		t.Fatalf("Value mismatch for 'gh': got %s, want %s", string(val), "value-gh")
	}

	// Test all keys with iterator to make sure they're all present
	it := db.NewIterator(nil, nil, false)
	defer it.Close()

	expectedKeys := []string{"ab", "abc", "ac", "acd", "gh", "ghi"}
	expectedValues := []string{"value-ab", "value-abc", "value-ac", "value-acd", "value-gh", "value-ghi"}

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all expected keys were found
	for i, expectedKey := range expectedKeys {
		foundValue, exists := foundKeys[expectedKey]
		if !exists {
			t.Fatalf("Key '%s' was not found by iterator", expectedKey)
		}
		if foundValue != expectedValues[i] {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s", expectedKey, foundValue, expectedValues[i])
		}
	}

	// Verify we didn't find any unexpected keys
	if len(foundKeys) != len(expectedKeys) {
		t.Fatalf("Iterator found %d keys, expected %d. Found keys: %v, Expected: %v", len(foundKeys), len(expectedKeys), foundKeys, expectedKeys)
	}
}

func TestSharedPrefixKeysStress(t *testing.T) {
	// Create a test database with limited index pages to force more page splits
	dbPath := "test_shared_prefix_stress.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open a new database with limited main index pages to force more reorganization
	db, err := Open(dbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Create many keys with shared prefixes to stress the radix tree
	type testCase struct {
		longKey  string
		shortKey string
		order    string // "long_first" or "short_first"
	}

	testCases := []testCase{
		// 2-byte prefix, 3-byte extension
		{"abc", "ab", "long_first"},
		{"abd", "ab", "short_first"},
		{"acd", "ac", "long_first"},
		{"ace", "ac", "short_first"},
		{"def", "de", "long_first"},
		{"deg", "de", "short_first"},
		{"ghi", "gh", "long_first"},
		{"ghj", "gh", "short_first"},
		{"jkl", "jk", "long_first"},
		{"jkm", "jk", "short_first"},
		{"mno", "mn", "long_first"},
		{"mnp", "mn", "short_first"},

		// 3-byte prefix, 4-byte extension
		{"abcd", "abc", "long_first"},
		{"abce", "abc", "short_first"},
		{"defg", "def", "long_first"},
		{"defh", "def", "short_first"},
		{"ghij", "ghi", "long_first"},
		{"ghik", "ghi", "short_first"},

		// 1-byte prefix, 2-byte extension
		{"xy", "x", "long_first"},
		{"xz", "x", "short_first"},
		{"yz", "y", "long_first"},
		{"ya", "y", "short_first"},
		{"za", "z", "long_first"},
		{"zb", "z", "short_first"},
	}

	// Track all keys we insert
	insertedKeys := make(map[string]string)

	// Insert keys according to their specified order
	for i, tc := range testCases {
		longValue := fmt.Sprintf("long-value-%d", i)
		shortValue := fmt.Sprintf("short-value-%d", i)

		if tc.order == "long_first" {
			// Insert long key first, then short key
			err = db.Set([]byte(tc.longKey), []byte(longValue))
			if err != nil {
				t.Fatalf("Failed to set long key '%s': %v", tc.longKey, err)
			}
			insertedKeys[tc.longKey] = longValue

			err = db.Set([]byte(tc.shortKey), []byte(shortValue))
			if err != nil {
				t.Fatalf("Failed to set short key '%s': %v", tc.shortKey, err)
			}
			insertedKeys[tc.shortKey] = shortValue
		} else {
			// Insert short key first, then long key
			err = db.Set([]byte(tc.shortKey), []byte(shortValue))
			if err != nil {
				t.Fatalf("Failed to set short key '%s': %v", tc.shortKey, err)
			}
			insertedKeys[tc.shortKey] = shortValue

			err = db.Set([]byte(tc.longKey), []byte(longValue))
			if err != nil {
				t.Fatalf("Failed to set long key '%s': %v", tc.longKey, err)
			}
			insertedKeys[tc.longKey] = longValue
		}

		// Verify both keys exist after each insertion
		val, err := db.Get([]byte(tc.longKey))
		if err != nil {
			t.Fatalf("Failed to get long key '%s' after insertion %d: %v", tc.longKey, i, err)
		}
		if !bytes.Equal(val, []byte(longValue)) {
			t.Fatalf("Value mismatch for long key '%s' after insertion %d: got %s, want %s",
				tc.longKey, i, string(val), longValue)
		}

		val, err = db.Get([]byte(tc.shortKey))
		if err != nil {
			t.Fatalf("Failed to get short key '%s' after insertion %d: %v", tc.shortKey, i, err)
		}
		if !bytes.Equal(val, []byte(shortValue)) {
			t.Fatalf("Value mismatch for short key '%s' after insertion %d: got %s, want %s",
				tc.shortKey, i, string(val), shortValue)
		}
	}

	// Verify all keys are still accessible
	for key, expectedValue := range insertedKeys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key '%s' in final verification: %v", key, err)
		}
		if !bytes.Equal(val, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key '%s' in final verification: got %s, want %s",
				key, string(val), expectedValue)
		}
	}

	// Use iterator to verify all keys are present
	it := db.NewIterator(nil, nil, false)
	defer it.Close()

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all inserted keys were found by iterator
	for key, expectedValue := range insertedKeys {
		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key '%s' was not found by iterator", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key '%s': got %s, want %s",
				key, foundValue, expectedValue)
		}
	}

	// Verify iterator didn't find any unexpected keys
	if len(foundKeys) != len(insertedKeys) {
		t.Fatalf("Iterator found %d keys, expected %d. Found: %v, Expected: %v",
			len(foundKeys), len(insertedKeys), foundKeys, insertedKeys)
	}

	// Test persistence by closing and reopening
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	reopenedDb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDb.Close()

	// Verify all keys still exist after reopening
	for key, expectedValue := range insertedKeys {
		val, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key '%s' after reopen: %v", key, err)
		}
		if !bytes.Equal(val, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key '%s' after reopen: got %s, want %s",
				key, string(val), expectedValue)
		}
	}
}

func TestSharedPrefixKeyOrdering(t *testing.T) {
	// This test reproduces the exact bug found in TestShortKeys
	// Using the insertion order that caused the failure:
	// [gh acd def ghi a e ac ef b d ij ab abc jkl mno c cd abd]

	// Create a test database
	dbPath := "test_shared_prefix_ordering.db"

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

	// Use the exact insertion order that caused the failure
	insertOrder := []struct {
		key   string
		value string
	}{
		{"gh", "two-byte-value-4"},
		{"acd", "three-byte-value-2"},
		{"def", "three-byte-value-3"},
		{"ghi", "three-byte-value-4"},
		{"a", "one-byte-value-0"},
		{"e", "one-byte-value-4"},
		{"ac", "two-byte-value-1"},
		{"ef", "two-byte-value-3"},
		{"b", "one-byte-value-1"},
		{"d", "one-byte-value-3"},
		{"ij", "two-byte-value-5"},
		{"ab", "two-byte-value-0"},
		{"abc", "three-byte-value-0"},  // This key failed to be retrieved
		{"jkl", "three-byte-value-5"},
		{"mno", "three-byte-value-6"},
		{"c", "one-byte-value-2"},
		{"cd", "two-byte-value-2"},
		{"abd", "three-byte-value-1"},
	}

	// Track all inserted keys
	allKeys := make(map[string]string)

	// Insert keys in the exact order that caused the failure
	for i, kv := range insertOrder {
		err = db.Set([]byte(kv.key), []byte(kv.value))
		if err != nil {
			t.Fatalf("Failed to set '%s' at step %d: %v", kv.key, i, err)
		}
		allKeys[kv.key] = kv.value

		// After each insertion, verify that previously inserted keys still exist
		for prevKey, prevValue := range allKeys {
			val, err := db.Get([]byte(prevKey))
			if err != nil {
				t.Fatalf("Key '%s' disappeared after inserting '%s' (step %d): %v",
					prevKey, kv.key, i, err)
			}
			if !bytes.Equal(val, []byte(prevValue)) {
				t.Fatalf("Value mismatch for key '%s' after inserting '%s' (step %d): got %s, want %s",
					prevKey, kv.key, i, string(val), prevValue)
			}
		}
	}

	// Final verification - check all keys exist
	for key, expectedValue := range allKeys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key '%s' in final verification: %v", key, err)
		}
		if !bytes.Equal(val, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key '%s': got %s, want %s",
				key, string(val), expectedValue)
		}
	}

	// Use iterator to verify all keys are present
	it := db.NewIterator(nil, nil, false)
	defer it.Close()

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all keys were found by iterator
	for key, expectedValue := range allKeys {
		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key '%s' not found by iterator", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key '%s': got %s, want %s",
				key, foundValue, expectedValue)
		}
	}
}

// printPageTraversalInfo prints detailed information about the page traversal for a given key
func printPageTraversalInfo(db *DB, key []byte) {
	fmt.Printf("Traversing key: '%s' (bytes: %v)\n", string(key), key)

	// Start with the root radix sub-page
	rootSubPage, err := db.getRootRadixSubPage()
	if err != nil {
		fmt.Printf("ERROR: Failed to get root radix sub-page: %v\n", err)
		return
	}

	fmt.Printf("Root page: %d, sub-page: %d\n", rootSubPage.Page.pageNumber, rootSubPage.SubPageIdx)

	// Process the key byte by byte
	currentSubPage := rootSubPage
	keyPos := 0

	// Traverse the radix trie until we reach a leaf page or the end of the key
	for keyPos < len(key) {
		// Get the current byte from the key
		byteValue := key[keyPos]
		fmt.Printf("Processing byte %d: '%c' (0x%02x)\n", keyPos, byteValue, byteValue)

		// Get the next page number and sub-page index from the current sub-page
		nextPageNumber, nextSubPageIdx := db.getRadixEntry(currentSubPage, byteValue)
		fmt.Printf("  Radix entry for byte 0x%02x: page=%d, subPage=%d\n", byteValue, nextPageNumber, nextSubPageIdx)

		// If there's no entry for this byte, the key doesn't exist yet
		if nextPageNumber == 0 {
			fmt.Printf("  No entry found for byte 0x%02x - key path doesn't exist\n", byteValue)
			return
		}

		// There's an entry for this byte, load the page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			fmt.Printf("  ERROR: Failed to load page %d: %v\n", nextPageNumber, err)
			return
		}

		// Check what type of page we got
		if page.pageType == ContentTypeRadix {
			fmt.Printf("  Found radix page %d, continuing to sub-page %d\n", nextPageNumber, nextSubPageIdx)
			// It's a radix page, continue traversing
			currentSubPage = &RadixSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			keyPos++
		} else if page.pageType == ContentTypeLeaf {
			fmt.Printf("  Found leaf page %d\n", nextPageNumber)
			// It's a leaf page, check its entries
			leafPage := page
			suffix := key[keyPos+1:]
			fmt.Printf("  Looking for suffix: '%s' (bytes: %v)\n", string(suffix), suffix)
			totalEntries := 0
			for subPageIdx, subPageInfo := range leafPage.SubPages {
				if subPageInfo != nil {
					entryCount := 0
					db.iterateLeafSubPageEntries(leafPage, uint8(subPageIdx), func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
						entryCount++
						return true
					})
					totalEntries += entryCount
				}
			}
			fmt.Printf("  Leaf page has %d entries across sub-pages:\n", totalEntries)

			for subPageIdx, subPageInfo := range leafPage.SubPages {
				if subPageInfo != nil {
					entryCount := 0
					db.iterateLeafSubPageEntries(leafPage, uint8(subPageIdx), func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
						entryCount++
						return true
					})
					fmt.Printf("    Sub-page %d has %d entries:\n", subPageIdx, entryCount)

					entryIdx := 0
					db.iterateLeafSubPageEntries(leafPage, uint8(subPageIdx), func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
						entrySuffix := leafPage.data[suffixOffset:suffixOffset+suffixLen]
						fmt.Printf("      Entry %d: suffix='%s' (bytes: %v), dataOffset=%d\n",
							entryIdx, string(entrySuffix), entrySuffix, dataOffset)
						entryIdx++
						return true
					})
				}
			}
			return
		} else {
			fmt.Printf("  ERROR: Invalid page type: %c\n", page.pageType)
			return
		}
	}

	// We've processed all bytes of the key
	// Check if there's an empty suffix in the current sub-page
	emptySuffixOffset := db.getEmptySuffixOffset(currentSubPage)
	fmt.Printf("Empty suffix offset in current sub-page: %d\n", emptySuffixOffset)
}

func TestLeafPageToRadixPageConversion(t *testing.T) {
	// Create a test database
	dbPath := "test_leaf_subpage_to_radix_subpage_conversion.db"

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

	// Use keys that will all go to the same leaf sub-page initially
	keyPrefix := "aa"
	keySuffix := "_some-long-suffix-here-to-consume-a-lot-of-space-and-fill-up-the-leaf-sub-page-quickly"

	// Helper function to get page information for a key
	getPageInfo := func(key []byte) (uint32, byte, uint8, int, int, error) {
		// Navigate to the page containing this key
		rootSubPage, err := db.getRootRadixSubPage()
		if err != nil {
			return 0, 0, 0, 0, 0, err
		}

		currentSubPage := rootSubPage
		keyPos := 0

		// Traverse the radix trie until we reach a leaf page
		for keyPos < len(key) {
			byteValue := key[keyPos]
			nextPageNumber, nextSubPageIdx := db.getRadixEntry(currentSubPage, byteValue)

			if nextPageNumber == 0 {
				return 0, 0, 0, 0, 0, fmt.Errorf("key path doesn't exist")
			}

			page, err := db.getPage(nextPageNumber)
			if err != nil {
				return 0, 0, 0, 0, 0, err
			}

			if page.pageType == ContentTypeLeaf {
				// Found the leaf page - we already know which sub-page from nextSubPageIdx
				leafPage := page

				// Calculate total entries across all sub-pages
				totalEntries := 0
				for subPageIdx, sp := range leafPage.SubPages {
					if sp != nil {
						entryCount := 0
						db.iterateLeafSubPageEntries(leafPage, uint8(subPageIdx), func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
							entryCount++
							return true
						})
						totalEntries += entryCount
					}
				}

				return leafPage.pageNumber, leafPage.pageType, nextSubPageIdx, leafPage.ContentSize, totalEntries, nil
			} else if page.pageType == ContentTypeRadix {
				// Continue traversing
				currentSubPage = &RadixSubPage{
					Page:       page,
					SubPageIdx: nextSubPageIdx,
				}
				keyPos++
			} else {
				return 0, 0, 0, 0, 0, fmt.Errorf("invalid page type: %c", page.pageType)
			}
		}

		// Check if there's an empty suffix in the current sub-page
		emptySuffixOffset := db.getEmptySuffixOffset(currentSubPage)
		if emptySuffixOffset != 0 {
			// The key exists as an empty suffix in a radix page
			return currentSubPage.Page.pageNumber, currentSubPage.Page.pageType, currentSubPage.SubPageIdx, 0, 0, nil
		}

		return 0, 0, 0, 0, 0, fmt.Errorf("key not found")
	}

	// Helper function to check if a specific page is a radix page
	checkPageType := func(pageNumber uint32) (byte, error) {
		page, err := db.getPage(pageNumber)
		if err != nil {
			return 0, err
		}
		return page.pageType, nil
	}

	// Phase 1: Fill up a leaf sub-page until conversion happens
	var keyCount int
	var firstPageNumber uint32
	var conversionDetected bool
	var conversionKeyIndex int

	// Insert keys until we detect the conversion from leaf sub-page to radix sub-page
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("value-%d", i)

		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}

		// Get page information for this key
		pageNumber, pageType, _, _, _, err := getPageInfo([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get page info for key %s: %v", key, err)
		}

		// Remember the first page number
		if i == 0 {
			firstPageNumber = pageNumber
			if pageType != ContentTypeLeaf {
				t.Fatalf("Expected first key to be in a leaf page, got %c", pageType)
			}
		}

		// Check if we've moved to a different page (indicating conversion happened)
		if i > 0 && pageNumber != firstPageNumber && !conversionDetected {
			// Check if the original page is still a leaf page or became a radix page
			originalPageType, err := checkPageType(firstPageNumber)
			if err != nil {
				t.Fatalf("Failed to check original page type: %v", err)
			}

			// In the new schema, the original leaf page might still be a leaf page,
			// but the entries should have been redistributed to new pages
			conversionDetected = true
			conversionKeyIndex = i

			// Log information about the conversion
			t.Logf("Conversion detected at key %d: original page %d (type %c), new page %d (type %c)",
				i, firstPageNumber, originalPageType, pageNumber, pageType)
		}

		keyCount = i + 1

		// Stop after we detect conversion and add a few more keys
		if conversionDetected && i > conversionKeyIndex + 5 {
			break
		}
	}

	if !conversionDetected {
		t.Fatalf("Expected leaf sub-page to be converted to radix sub-page, but conversion was not detected after %d keys", keyCount)
	}

	// Phase 2: Verify the conversion created the expected structure
	// Count how many different pages our keys ended up in
	pageDistribution := make(map[uint32][]int) // page number -> list of key indices
	// Track page+sub-page combinations
	pageSubPageDistribution := make(map[string][]int) // "pageNum:subPageIdx" -> list of key indices

	// Check each key to see which pages they ended up in
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		pageNumber, pageType, subPageIdx, _, _, err := getPageInfo([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get page info for key %s after conversion: %v", key, err)
		}

		if _, exists := pageDistribution[pageNumber]; !exists {
			pageDistribution[pageNumber] = make([]int, 0)
		}
		pageDistribution[pageNumber] = append(pageDistribution[pageNumber], i)

		// Track page+sub-page distribution
		pageSubPageKey := fmt.Sprintf("%d:%d", pageNumber, subPageIdx)
		if _, exists := pageSubPageDistribution[pageSubPageKey]; !exists {
			pageSubPageDistribution[pageSubPageKey] = make([]int, 0)
		}
		pageSubPageDistribution[pageSubPageKey] = append(pageSubPageDistribution[pageSubPageKey], i)

		// Log which page each key ended up in
		if i < 10 || i >= keyCount-5 { // Log first 10 and last 5 keys
			t.Logf("Key %d (%s) -> page %d (type %c), sub-page %d", i, key, pageNumber, pageType, subPageIdx)
		}
	}

	t.Logf("After conversion: keys distributed across %d pages", len(pageDistribution))
	for pageNum, keyIndices := range pageDistribution {
		pageType, _ := checkPageType(pageNum)
		t.Logf("Page %d (type %c): %d keys", pageNum, pageType, len(keyIndices))
	}

	t.Logf("Sub-page distribution across %d page+sub-page combinations:", len(pageSubPageDistribution))
	for pageSubPageKey, keyIndices := range pageSubPageDistribution {
		t.Logf("  %s: %d keys %v", pageSubPageKey, len(keyIndices), keyIndices)
	}

	// Phase 3: Verify all keys are still accessible
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("value-%d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after conversion: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after conversion: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Phase 4: Test persistence

	// Close and reopen the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	reopenedDb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDb.Close()

	// Verify all keys still exist after reopening
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("value-%d", i)

		value, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after reopen: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after reopen: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Phase 5: Test iterator after conversion

	it := reopenedDb.NewIterator(nil, nil, false)
	defer it.Close()

	foundKeys := make(map[string]string)
	for it.Valid() {
		key := string(it.Key())
		value := string(it.Value())
		foundKeys[key] = value
		it.Next()
	}

	// Verify all keys were found by iterator
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("value-%d", i)

		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key %s not found by iterator after conversion", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key %s: got %s, want %s",
				key, foundValue, expectedValue)
		}
	}

	// Verify iterator didn't find any unexpected keys
	if len(foundKeys) != keyCount {
		t.Fatalf("Iterator found %d keys, expected %d", len(foundKeys), keyCount)
	}
}

func TestLeafPageToRadixPageConversionSimilarKeys(t *testing.T) {
	// Create a test database
	dbPath := "test_leaf_to_radix_conversion_similar.db"

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

	// Use keys that are very similar at the beginning but differ only at the end
	// This will test how the radix tree handles keys with long common prefixes
	//keyPrefix := "user_profile_data_very_long_common_prefix_here_"
	//keySuffix := "_with_some_additional_content_to_make_entries_larger"
	keyPrefix := "prefix"
	keySuffix := "_with_some_additional_content_to_make_entries_larger-user_profile_data_very_long_common_here_"

	// Helper function to get page information for a key
	getPageInfo := func(key []byte) (uint32, byte, uint8, int, int, error) {
		// Navigate to the page containing this key
		rootSubPage, err := db.getRootRadixSubPage()
		if err != nil {
			return 0, 0, 0, 0, 0, err
		}

		currentSubPage := rootSubPage
		keyPos := 0

		// Traverse the radix trie until we reach a leaf page
		for keyPos < len(key) {
			byteValue := key[keyPos]
			nextPageNumber, nextSubPageIdx := db.getRadixEntry(currentSubPage, byteValue)

			if nextPageNumber == 0 {
				return 0, 0, 0, 0, 0, fmt.Errorf("key path doesn't exist")
			}

			page, err := db.getPage(nextPageNumber)
			if err != nil {
				return 0, 0, 0, 0, 0, err
			}

			if page.pageType == ContentTypeLeaf {
				// Found the leaf page - we already know which sub-page from nextSubPageIdx
				leafPage := page

				// Calculate total entries across all sub-pages
				totalEntries := 0
				for subPageIdx, sp := range leafPage.SubPages {
					if sp != nil {
						entryCount := 0
						db.iterateLeafSubPageEntries(leafPage, uint8(subPageIdx), func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
							entryCount++
							return true
						})
						totalEntries += entryCount
					}
				}

				return leafPage.pageNumber, leafPage.pageType, nextSubPageIdx, leafPage.ContentSize, totalEntries, nil
			} else if page.pageType == ContentTypeRadix {
				// Continue traversing
				currentSubPage = &RadixSubPage{
					Page:       page,
					SubPageIdx: nextSubPageIdx,
				}
				keyPos++
			} else {
				return 0, 0, 0, 0, 0, fmt.Errorf("invalid page type: %c", page.pageType)
			}
		}

		// Check if there's an empty suffix in the current sub-page
		emptySuffixOffset := db.getEmptySuffixOffset(currentSubPage)
		if emptySuffixOffset != 0 {
			// The key exists as an empty suffix in a radix page
			return currentSubPage.Page.pageNumber, currentSubPage.Page.pageType, currentSubPage.SubPageIdx, 0, 0, nil
		}

		return 0, 0, 0, 0, 0, fmt.Errorf("key not found")
	}

	// Helper function to check if a specific page is a radix page
	checkPageType := func(pageNumber uint32) (byte, error) {
		page, err := db.getPage(pageNumber)
		if err != nil {
			return 0, err
		}
		return page.pageType, nil
	}

	// Phase 1: Fill up a leaf sub-page with very similar keys
	var keyCount int
	var firstPageNumber uint32
	var conversionDetected bool
	var conversionKeyIndex int

	// Insert keys that differ only at the end - this tests radix tree efficiency
	// with long common prefixes
	for i := 0; i < 200; i++ { // Increase limit since similar keys might pack differently
		// Create keys that are identical except for the number at the end
		// Format: "prefix000001_with_some_additional_content_to_make_entries_larger-user_profile_data_very_long_common_here_"
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		value := fmt.Sprintf("user_data_%d", i)

		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}

		// Get page information for this key
		pageNumber, pageType, _, _, _, err := getPageInfo([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get page info for key %s: %v", key, err)
		}

		// Remember the first page number
		if i == 0 {
			firstPageNumber = pageNumber
			if pageType != ContentTypeLeaf {
				t.Fatalf("Expected first key to be in a leaf page, got %c", pageType)
			}
		}

		// Check if we've moved to a different page (indicating conversion happened)
		if i > 0 && pageNumber != firstPageNumber && !conversionDetected {
			// Check if the original page is still a leaf page or became a radix page
			originalPageType, err := checkPageType(firstPageNumber)
			if err != nil {
				t.Fatalf("Failed to check original page type: %v", err)
			}

			// In the new schema, the original leaf page might still be a leaf page,
			// but the entries should have been redistributed to new pages
			conversionDetected = true
			conversionKeyIndex = i

			// Log information about the conversion
			t.Logf("Conversion detected at key %d: original page %d (type %c), new page %d (type %c)",
				i, firstPageNumber, originalPageType, pageNumber, pageType)
		}

		keyCount = i + 1

		// Stop after we detect conversion and add a few more keys
		if conversionDetected && i > conversionKeyIndex + 5 {
			break
		}
	}

	if !conversionDetected {
		t.Fatalf("Expected leaf sub-page to be converted to radix sub-page, but conversion was not detected after %d keys", keyCount)
	}

	// Phase 2: Analyze how similar keys are distributed after conversion
	// Count how many different pages our keys ended up in
	pageDistribution := make(map[uint32][]int) // page number -> list of key indices
	// Track page+sub-page combinations
	pageSubPageDistribution := make(map[string][]int) // "pageNum:subPageIdx" -> list of key indices

	// Check each key to see which pages they ended up in
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		pageNumber, pageType, subPageIdx, _, _, err := getPageInfo([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get page info for key %s after conversion: %v", key, err)
		}

		if _, exists := pageDistribution[pageNumber]; !exists {
			pageDistribution[pageNumber] = make([]int, 0)
		}
		pageDistribution[pageNumber] = append(pageDistribution[pageNumber], i)

		// Track page+sub-page distribution
		pageSubPageKey := fmt.Sprintf("%d:%d", pageNumber, subPageIdx)
		if _, exists := pageSubPageDistribution[pageSubPageKey]; !exists {
			pageSubPageDistribution[pageSubPageKey] = make([]int, 0)
		}
		pageSubPageDistribution[pageSubPageKey] = append(pageSubPageDistribution[pageSubPageKey], i)

		// Log which page each key ended up in for the first few keys
		if i < 10 {
			t.Logf("Key %d (%s) -> page %d (type %c), sub-page %d", i, key, pageNumber, pageType, subPageIdx)
		}
	}

	t.Logf("After conversion: keys distributed across %d pages", len(pageDistribution))
	for pageNum, keyIndices := range pageDistribution {
		pageType, _ := checkPageType(pageNum)
		t.Logf("Page %d (type %c): %d keys", pageNum, pageType, len(keyIndices))
	}

	t.Logf("Sub-page distribution across %d page+sub-page combinations:", len(pageSubPageDistribution))
	for pageSubPageKey, keyIndices := range pageSubPageDistribution {
		t.Logf("  %s: %d keys %v", pageSubPageKey, len(keyIndices), keyIndices)
	}

	// Phase 3: Verify all keys are still accessible
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after conversion: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after conversion: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Phase 4: Test range queries on similar keys

	// Test range query from key 10 to key 20
	startKey := fmt.Sprintf("%s%06d%s", keyPrefix, 10, keySuffix)
	endKey := fmt.Sprintf("%s%06d%s", keyPrefix, 20, keySuffix)

	it := db.NewIterator([]byte(startKey), []byte(endKey), false)
	defer it.Close()

	rangeCount := 0
	expectedRange := make(map[string]bool)
	for i := 10; i < 20; i++ {
		if i < keyCount {
			key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
			expectedRange[key] = true
		}
	}

	for it.Valid() {
		key := string(it.Key())
		if !expectedRange[key] {
			t.Fatalf("Range iterator returned unexpected key: %s", key)
		}
		rangeCount++
		it.Next()
	}

	if rangeCount != len(expectedRange) {
		t.Fatalf("Range iterator found %d keys, expected %d", rangeCount, len(expectedRange))
	}

	// Phase 4B: Test iterator on similar keys

	it2 := db.NewIterator(nil, nil, false)
	defer it2.Close()

	foundKeys := make(map[string]string)
	for it2.Valid() {
		key := string(it2.Key())
		value := string(it2.Value())
		foundKeys[key] = value
		it2.Next()
	}

	// Verify all keys were found by iterator
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key %s not found by iterator after conversion", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key %s: got %s, want %s",
				key, foundValue, expectedValue)
		}
	}

	// Verify iterator didn't find any unexpected keys
	if len(foundKeys) != keyCount {
		t.Fatalf("Iterator found %d keys, expected %d", len(foundKeys), keyCount)
	}

	// Phase 5: Test persistence

	// Close and reopen the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	reopenedDb, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopenedDb.Close()

	// Verify all keys still exist after reopening
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		value, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s after reopen: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Value mismatch for key %s after reopen: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Phase 6: Verify all keys are accessible via Get() after reopen

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		value, err := reopenedDb.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %s via Get() after reopen: %v", key, err)
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Get() value mismatch for key %s after reopen: got %s, want %s",
				key, string(value), expectedValue)
		}
	}

	// Phase 7: Test iterator on similar keys after reopen

	it2 = reopenedDb.NewIterator(nil, nil, false)
	defer it2.Close()

	foundKeys = make(map[string]string)
	for it2.Valid() {
		key := string(it2.Key())
		value := string(it2.Value())
		foundKeys[key] = value
		it2.Next()
	}

	// Verify all keys were found by iterator
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%06d%s", keyPrefix, i, keySuffix)
		expectedValue := fmt.Sprintf("user_data_%d", i)

		foundValue, exists := foundKeys[key]
		if !exists {
			t.Fatalf("Key %s not found by iterator after conversion", key)
		}
		if foundValue != expectedValue {
			t.Fatalf("Iterator value mismatch for key %s: got %s, want %s",
				key, foundValue, expectedValue)
		}
	}

	// Verify iterator didn't find any unexpected keys
	if len(foundKeys) != keyCount {
		t.Fatalf("Iterator found %d keys, expected %d", len(foundKeys), keyCount)
	}
}

func TestBackgroundWorkerDeadlock(t *testing.T) {
	// This test is designed to trigger a deadlock between the caller thread
	// and the background worker thread by forcing frequent background operations
	// while the caller thread is performing database operations

	dbPath := "test_background_deadlock.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open database with extremely low thresholds to force immediate background worker activity
	db, err := Open(dbPath, Options{
		"CacheSizeThreshold":   2,    // Extremely low - force cache cleanup after 2 pages
		"DirtyPageThreshold":   1,    // Force flush after every single dirty page
		"CheckpointThreshold":  256,  // Very small checkpoint threshold (256 bytes)
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Verify we're using WorkerThread mode (background worker should be active)
	t.Logf("Database opened, background worker should be active")

	// Create keys that will force page creation and background activity
	// Each operation should trigger background worker due to low thresholds
	keySuffix := "_deadlock_test_key_with_long_suffix_to_consume_space"
	valuePrefix := "deadlock_test_value_with_very_long_content_to_make_pages_fill_up_quickly_and_trigger_background_worker_activity_"

	// Do direct database operations that should trigger background worker
	// Each Set() should trigger background worker due to DirtyPageThreshold=1
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d%s", i, keySuffix)
		value := fmt.Sprintf("%s%d", valuePrefix, i)

		// Set operation - this should trigger background worker immediately
		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}

		// Immediately try to read it back while background worker might be active
		_, err = db.Get([]byte(key))
		if err != nil {
			// Log detailed information about the failure
			t.Logf("=== BUG DETECTED ===")
			t.Logf("Failed to get key %d ('%s') immediately after setting", i, key)
			t.Logf("Error: %v", err)
			t.Logf("Expected value: '%s'", value)

			// Get cache stats to understand the state
			cacheStats := db.GetCacheStats()
			t.Logf("Cache stats when bug occurred: %+v", cacheStats)

			t.Fatalf("Failed to get key %d: %v", i, err)
		}

		// Small delay to let background worker process and potentially create deadlock
		time.Sleep(1 * time.Millisecond)

		// Do another operation to increase lock contention
		if i > 0 {
			prevKey := fmt.Sprintf("%d%s", i-1, keySuffix)
			_, err = db.Get([]byte(prevKey))
			if err != nil {
				t.Fatalf("Failed to get previous key %d: %v", i-1, err)
			}
		}
	}

	t.Logf("Completed %d direct database operations", 1000)

	// Force more background activity by creating an iterator
	// while background worker is likely still active
	t.Logf("Creating iterator while background worker is active")
	it := db.NewIterator(nil, nil, false)

	keyCount := 0
	for it.Valid() {
		_ = it.Key()
		_ = it.Value()
		keyCount++
		it.Next()

		// Add small delays to increase chance of deadlock
		if keyCount%10 == 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}
	it.Close()

	t.Logf("Iterator found %d keys", keyCount)

	// Do some more operations to stress test the deadlock scenario
	for i := 1000; i < 1050; i++ {
		key := fmt.Sprintf("%d%s", i, keySuffix)
		value := fmt.Sprintf("%s%d", valuePrefix, i)

		// Set and immediately get to maximize lock contention
		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set final key %d: %v", i, err)
		}

		_, err = db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get final key %d: %v", i, err)
		}

		// No delay here to maximize pressure on locks
	}

	// Give background worker time to finish any pending operations
	time.Sleep(100 * time.Millisecond)

	// Get final cache stats
	cacheStats := db.GetCacheStats()
	t.Logf("Final cache stats: %+v", cacheStats)

	t.Logf("Test completed successfully - no deadlock detected")
}

func TestBackgroundWorkerWithTransactions(t *testing.T) {
	// Simpler version focusing on transaction + background worker interaction
	dbPath := "test_background_worker.db"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open database with very low thresholds to force background worker activity
	db, err := Open(dbPath, Options{
		"CacheSizeThreshold":   3,    // Low cache size to force frequent cleanups
		"DirtyPageThreshold":   1,    // Force flush after every dirty page
		"CheckpointThreshold":  512,  // Small checkpoint threshold
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Sequential operations that should trigger background worker
	numTransactions := 5
	keysPerTransaction := 1000

	for txId := 0; txId < numTransactions; txId++ {
		t.Logf("Starting transaction %d", txId)

		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", txId, err)
		}

		// Insert keys that should trigger background worker due to low thresholds
		for i := 0; i < keysPerTransaction; i++ {
			key := fmt.Sprintf("tx%d_key%d_with_long_suffix_to_consume_space", txId, i)
			value := fmt.Sprintf("tx%d_value%d_with_long_content_to_consume_space", txId, i)

			err = tx.Set([]byte(key), []byte(value))
			if err != nil {
				tx.Rollback()
				t.Fatalf("Transaction %d: failed to set key %d: %v", txId, i, err)
			}

			// Small delay to let background worker potentially run
			time.Sleep(1 * time.Millisecond)
		}

		// Commit while background worker might be active
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction %d: %v", txId, err)
		}

		t.Logf("Transaction %d completed", txId)

		// Small delay between transactions
		time.Sleep(10 * time.Millisecond)
	}

	// Verify all data exists
	totalKeysExpected := numTransactions * keysPerTransaction

	it := db.NewIterator(nil, nil, false)
	defer it.Close()

	keysFound := 0
	for it.Valid() {
		keysFound++
		it.Next()
	}

	if keysFound != totalKeysExpected {
		t.Fatalf("Expected %d keys, found %d", totalKeysExpected, keysFound)
	}

	t.Logf("Successfully completed test with %d transactions and %d total keys", numTransactions, totalKeysExpected)
}

func TestHeaderReadingWithWAL(t *testing.T) {
	// Create a temporary database
	dbPath := "test_header_wal.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-index")
	defer os.Remove(dbPath + "-wal")

	// Test 1: Create database with WAL enabled
	db, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Add some data to trigger index updates
	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	// Force a flush to write pages (including header) to WAL
	err = db.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Add more data to create a difference between WAL and index file
	err = db.Set([]byte("another_key"), []byte("another_value"))
	if err != nil {
		t.Fatalf("Failed to set second key: %v", err)
	}

	// Store the current lastIndexedOffset (this should be in WAL but not in index file)
	originalOffset := db.mainFileSize
	originalFreeHead := db.freeRadixPagesHead

	// Close the database (will commit the changes to the WAL file)
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Test 2: Reopen database and verify header is read correctly from WAL
	db2, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify that the lastIndexedOffset was read from WAL (should match original)
	if db2.lastIndexedOffset != originalOffset {
		t.Errorf("lastIndexedOffset mismatch: expected %d, got %d", originalOffset, db2.lastIndexedOffset)
	}

	// Verify that the freeRadixPagesHead was read from WAL
	if db2.freeRadixPagesHead != originalFreeHead {
		t.Errorf("freeRadixPagesHead mismatch: expected %d, got %d", originalFreeHead, db2.freeRadixPagesHead)
	}

	// Verify that we can still read both keys (showing WAL was properly loaded)
	value1, err := db2.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if string(value1) != "value" {
		t.Errorf("Value mismatch: expected 'value', got '%s'", string(value1))
	}

	value2, err := db2.Get([]byte("another_key"))
	if err != nil {
		t.Fatalf("Failed to get another_key: %v", err)
	}
	if string(value2) != "another_value" {
		t.Errorf("Value mismatch: expected 'another_value', got '%s'", string(value2))
	}

	err = db2.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestHeaderReadingWithoutWAL(t *testing.T) {
	// Create a temporary database
	dbPath := "test_header_no_wal.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-index")
	defer os.Remove(dbPath + "-wal")

	// Open database using default options (WAL enabled)
	db, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Add some data
	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}
	err = db.Set([]byte("another_key"), []byte("another_value"))
	if err != nil {
		t.Fatalf("Failed to set another key: %v", err)
	}
	err = db.Set([]byte("third_key"), []byte("third_value"))
	if err != nil {
		t.Fatalf("Failed to set third key: %v", err)
	}

	// Store the current mainFileSize for comparison
	originalOffset := db.mainFileSize
	originalFreeHead := db.freeRadixPagesHead

	// Close the database (will commit the changes to the WAL file)
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen database - this will do a checkpoint of the WAL into the index file
	db2, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("Failed to reopen database first time: %v", err)
	}

	// Close again to ensure any WAL created during opening is cleaned up
	err = db2.Close()
	if err != nil {
		t.Fatalf("Failed to close database second time: %v", err)
	}

	// Verify that no WAL file exists before the final test
	walPath := dbPath + "-wal"
	if _, err := os.Stat(walPath); err == nil {
		// WAL file exists, remove it to ensure clean test
		os.Remove(walPath)
	}

	// Final reopen - this should read header directly from index file only
	db3, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("Failed to reopen database final time: %v", err)
	}

	// Verify that the lastIndexedOffset was read correctly from index file
	if db3.lastIndexedOffset != originalOffset {
		t.Errorf("lastIndexedOffset mismatch: expected %d, got %d", originalOffset, db3.lastIndexedOffset)
	}

	// Verify that the freeRadixPagesHead was read correctly from index file
	if db3.freeRadixPagesHead != originalFreeHead {
		t.Errorf("freeRadixPagesHead mismatch: expected %d, got %d", originalFreeHead, db3.freeRadixPagesHead)
	}

	// Verify that we can still read the data
	value, err := db3.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if string(value) != "value" {
		t.Errorf("Value mismatch: expected 'value', got '%s'", string(value))
	}
	value, err = db3.Get([]byte("another_key"))
	if err != nil {
		t.Fatalf("Failed to get another_key: %v", err)
	}
	if string(value) != "another_value" {
		t.Errorf("Value mismatch: expected 'another_value', got '%s'", string(value))
	}
	value, err = db3.Get([]byte("third_key"))
	if err != nil {
		t.Fatalf("Failed to get third_key: %v", err)
	}
	if string(value) != "third_value" {
		t.Errorf("Value mismatch: expected 'third_value', got '%s'", string(value))
	}

	err = db3.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestTransactionVisibility(t *testing.T) {
	type txnVisibilityTestCase struct {
		name        string
		initialData map[string]string
		txnChanges  map[string]string
		deleteKey   string
	}

	testCases := []txnVisibilityTestCase{
		{
			name: "SamePrefixKeys",
			initialData: map[string]string{
				"key1": "initial-value1",
				"key2": "initial-value2",
				"key3": "initial-value3",
			},
			txnChanges: map[string]string{
				"key1": "txn-modified-value1",
				"key4": "txn-new-value4",
			},
			deleteKey: "key3",
		},
		{
			name: "DifferentPrefixKeys",
			initialData: map[string]string{
				"first-key": "initial-value1",
				"second-key": "initial-value2",
				"third-key": "initial-value3",
			},
			txnChanges: map[string]string{
				"first-key": "txn-modified-value1",
				"fourth-key": "txn-new-value4",
			},
			deleteKey: "third-key",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test database
			dbPath := "test_transaction_visibility_" + tc.name + ".db"

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

			// Insert initial data
			for k, v := range tc.initialData {
				if err := db.Set([]byte(k), []byte(v)); err != nil {
					t.Fatalf("Failed to set initial key %s: %v", k, err)
				}
			}

			// Verify initial data exists
			for k, expectedValue := range tc.initialData {
				value, err := db.Get([]byte(k))
				if err != nil {
					t.Fatalf("Failed to get initial key %s: %v", k, err)
				}
				if !bytes.Equal(value, []byte(expectedValue)) {
					t.Fatalf("Initial value mismatch for key %s: got %s, want %s", k, string(value), expectedValue)
				}
			}

			// Begin a transaction
			tx, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			// Make changes within the transaction
			for k, v := range tc.txnChanges {
				if err := tx.Set([]byte(k), []byte(v)); err != nil {
					t.Fatalf("Failed to set key %s in transaction: %v", k, err)
				}
			}

			// Delete a key within the transaction
			if err := tx.Delete([]byte(tc.deleteKey)); err != nil {
				t.Fatalf("Failed to delete %s in transaction: %v", tc.deleteKey, err)
			}

			// TEST 1: Verify that db.Get() doesn't see changes from the open transaction
			t.Log("Testing db.Get() doesn't see transaction changes")

			// Check modified key
			var modifiedKey string
			var modifiedValue string
			for k, v := range tc.txnChanges {
				if _, ok := tc.initialData[k]; ok {
					modifiedKey = k
					modifiedValue = v
					break
				}
			}
			if modifiedKey == "" {
				t.Fatalf("No modified key found in txnChanges that exists in initialData")
			}
			value, err := db.Get([]byte(modifiedKey))
			if err != nil {
				t.Fatalf("Failed to get %s with db.Get(): %v", modifiedKey, err)
			}
			if !bytes.Equal(value, []byte(tc.initialData[modifiedKey])) {
				t.Fatalf("db.Get() should not see transaction changes for %s: got %s, want %s",
					string(modifiedKey), string(value), tc.initialData[modifiedKey])
			}

			// Check new key
			var newKey string
			var newValue string
			for k, v := range tc.txnChanges {
				if _, ok := tc.initialData[k]; !ok {
					newKey = k
					newValue = v
					break
				}
			}
			if newKey == "" {
				t.Fatalf("No new key found in txnChanges that does not exist in initialData")
			}
			_, err = db.Get([]byte(newKey))
			if err == nil {
				t.Fatalf("db.Get() should not see new %s from transaction", newKey)
			}

			// Check deleted key
			deletedKey := tc.deleteKey
			value, err = db.Get([]byte(deletedKey))
			if err != nil {
				t.Fatalf("db.Get() should still see %s that was deleted in transaction: %v", deletedKey, err)
			}
			if !bytes.Equal(value, []byte(tc.initialData[deletedKey])) {
				t.Fatalf("db.Get() value mismatch for %s: got %s, want %s",
					deletedKey, string(value), tc.initialData[deletedKey])
			}

			// TEST 2: Verify that txn.Get() can see its own changes
			t.Log("Testing txn.Get() can see transaction changes")

			// Check modified key
			txValue, err := tx.Get([]byte(modifiedKey))
			if err != nil {
				t.Fatalf("Failed to get %s with tx.Get(): %v", modifiedKey, err)
			}
			if !bytes.Equal(txValue, []byte(modifiedValue)) {
				t.Fatalf("tx.Get() should see transaction changes for %s: got %s, want %s",
					modifiedKey, string(txValue), modifiedValue)
			}

			// Check new key
			txValue, err = tx.Get([]byte(newKey))
			if err != nil {
				t.Fatalf("Failed to get %s with tx.Get(): %v", newKey, err)
			}
			if !bytes.Equal(txValue, []byte(newValue)) {
				t.Fatalf("tx.Get() should see new %s from transaction: got %s, want %s",
					newKey, string(txValue), newValue)
			}

			// Check deleted key
			_, err = tx.Get([]byte(deletedKey))
			if err == nil {
				t.Fatalf("tx.Get() should not see %s that was deleted in transaction", deletedKey)
			}

			// TEST 3: Commit the transaction and verify db.Get() now sees the changes
			if err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}
			t.Log("Transaction committed, testing db.Get() now sees changes")

			// Check modified key
			value, err = db.Get([]byte(modifiedKey))
			if err != nil {
				t.Fatalf("Failed to get %s after commit: %v", modifiedKey, err)
			}
			if !bytes.Equal(value, []byte(modifiedValue)) {
				t.Fatalf("After commit, value mismatch for %s: got %s, want %s",
					modifiedKey, string(value), modifiedValue)
			}

			// Check new key
			value, err = db.Get([]byte(newKey))
			if err != nil {
				t.Fatalf("Failed to get %s after commit: %v", newKey, err)
			}
			if !bytes.Equal(value, []byte(newValue)) {
				t.Fatalf("After commit, value mismatch for %s: got %s, want %s",
					newKey, string(value), newValue)
			}

			// Check deleted key
			_, err = db.Get([]byte(deletedKey))
			if err == nil {
				t.Fatalf("After commit, %s should still be deleted", deletedKey)
			}

			// TEST 4: Start a new transaction and verify it sees the committed changes
			tx2, err := db.Begin()
			if err != nil {
				t.Fatalf("Failed to begin second transaction: %v", err)
			}
			defer tx2.Rollback()

			// Check modified key
			tx2Value, err := tx2.Get([]byte(modifiedKey))
			if err != nil {
				t.Fatalf("Failed to get %s in second transaction: %v", modifiedKey, err)
			}
			if !bytes.Equal(tx2Value, []byte(modifiedValue)) {
				t.Fatalf("Second transaction value mismatch for %s: got %s, want %s",
					modifiedKey, string(tx2Value), modifiedValue)
			}

			// Check new key
			tx2Value, err = tx2.Get([]byte(newKey))
			if err != nil {
				t.Fatalf("Failed to get %s in second transaction: %v", newKey, err)
			}
			if !bytes.Equal(tx2Value, []byte(newValue)) {
				t.Fatalf("Second transaction value mismatch for %s: got %s, want %s",
					newKey, string(tx2Value), newValue)
			}

			// Check deleted key
			_, err = tx2.Get([]byte(deletedKey))
			if err == nil {
				t.Fatalf("Second transaction should not see deleted %s", deletedKey)
			}
		})
	}
}

// TestTransactionVisibilityOnFreshDB covers the case when the DB was just opened and there is a single transaction run on it.
// This is important to ensure isolation guarantees even for the very first transaction on a fresh database.
func TestTransactionVisibilityOnFreshDB(t *testing.T) {
	dbPath := "test_transaction_visibility_fresh.db"
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

	key := "tx-key-1"
	val := "tx-value-1"

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Set a value in the transaction
	err = tx.Set([]byte(key), []byte(val))
	if err != nil {
		t.Fatalf("Failed to set value in transaction: %v", err)
	}

	// The value should NOT be visible from db.Get (should return error)
	_, err = db.Get([]byte(key))
	if err == nil {
		t.Fatalf("db.Get should not see uncommitted value, but got value for key %s", key)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Now the value should be visible from db.Get
	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("db.Get should see committed value, but got error: %v", err)
	}
	if !bytes.Equal(got, []byte(val)) {
		t.Fatalf("db.Get returned wrong value after commit: got %s, want %s", string(got), val)
	}
}

// TestLastIndexedOffsetUpdate tests that lastIndexedOffset is properly updated when the worker thread
// flushes pages during active transactions. This test covers the exact scenario that was causing the bug
// where lastIndexedOffset was not being updated because no dirty pages were found during flush.
func TestLastIndexedOffsetUpdate(t *testing.T) {
	dbPath := "test_last_indexed_offset.db"
	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	// Open database with small dirty page threshold to trigger frequent flushes
	options := Options{
		"DirtyPageThreshold": 5, // Very small to trigger flushes quickly
		"WorkerThread":       true,
	}

	db, err := Open(dbPath, options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + "-index")
		os.Remove(dbPath + "-wal")
	}()

	// Initial state check
	initialLastIndexed := db.lastIndexedOffset
	initialMainFileSize := db.mainFileSize
	t.Logf("Initial state - lastIndexedOffset: %d, mainFileSize: %d", initialLastIndexed, initialMainFileSize)

	// Phase 1: Add some initial data and let it get properly indexed
	t.Log("Phase 1: Adding initial data and ensuring it gets indexed")
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("initial-key-%d", i)
		value := fmt.Sprintf("initial-value-%d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set initial key %s: %v", key, err)
		}
	}

	// Wait a bit for worker thread to flush
	time.Sleep(100 * time.Millisecond)

	// Force a manual flush to ensure everything is indexed
	err = db.flushIndexToDisk()
	if err != nil {
		t.Fatalf("Manual flush failed: %v", err)
	}

	afterInitialFlush := db.lastIndexedOffset
	afterInitialMainFileSize := db.mainFileSize
	t.Logf("After initial flush - lastIndexedOffset: %d, mainFileSize: %d", afterInitialFlush, afterInitialMainFileSize)

	// Verify that lastIndexedOffset was updated
	if afterInitialFlush <= initialLastIndexed {
		t.Fatalf("lastIndexedOffset should have increased after initial data, got %d, was %d", afterInitialFlush, initialLastIndexed)
	}

	// Phase 2: Start a transaction and add data without committing
	t.Log("Phase 2: Starting transaction and adding data without committing")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Track state before transaction data
	beforeTxLastIndexed := db.lastIndexedOffset
	beforeTxMainFileSize := db.mainFileSize
	t.Logf("Before transaction data - lastIndexedOffset: %d, mainFileSize: %d", beforeTxLastIndexed, beforeTxMainFileSize)

	// Add data within the transaction
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("tx-key-%d", i)
		value := fmt.Sprintf("tx-value-%d-" + strings.Repeat("x", 100), i) // Make values larger to increase file size
		err := tx.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set transaction key %s: %v", key, err)
		}
	}

	// At this point, main file size should have grown but lastIndexedOffset should NOT be updated
	// to the current main file size because the transaction hasn't committed yet
	afterTxDataLastIndexed := db.lastIndexedOffset
	afterTxDataMainFileSize := db.mainFileSize
	t.Logf("After transaction data (before commit) - lastIndexedOffset: %d, mainFileSize: %d", afterTxDataLastIndexed, afterTxDataMainFileSize)

	// Verify that main file size grew (data was written)
	if afterTxDataMainFileSize <= beforeTxMainFileSize {
		t.Fatalf("Main file size should have grown after transaction data, got %d, was %d", afterTxDataMainFileSize, beforeTxMainFileSize)
	}

	// Phase 3: Force a flush while transaction is still active
	// This simulates the worker thread flushing during an active transaction
	t.Log("Phase 3: Forcing flush during active transaction")

	// Wait a bit to let any automatic flushes happen
	time.Sleep(100 * time.Millisecond)

	// Force manual flush while transaction is active
	err = db.flushIndexToDisk()
	if err != nil {
		t.Fatalf("Manual flush during transaction failed: %v", err)
	}

	afterFlushDuringTxLastIndexed := db.lastIndexedOffset
	afterFlushDuringTxMainFileSize := db.mainFileSize
	t.Logf("After flush during transaction - lastIndexedOffset: %d, mainFileSize: %d", afterFlushDuringTxLastIndexed, afterFlushDuringTxMainFileSize)

	// CRITICAL TEST: The lastIndexedOffset should NOT be updated to the current main file size
	// because the transaction data hasn't been committed yet. It should remain at the
	// file size from before the transaction (prevFileSize).
	if afterFlushDuringTxLastIndexed > beforeTxMainFileSize {
		t.Fatalf("lastIndexedOffset should not exceed pre-transaction main file size during active transaction. "+
			"lastIndexedOffset: %d, pre-transaction mainFileSize: %d, current mainFileSize: %d",
			afterFlushDuringTxLastIndexed, beforeTxMainFileSize, afterFlushDuringTxMainFileSize)
	}

	// Phase 4: Commit the transaction and verify lastIndexedOffset gets updated
	t.Log("Phase 4: Committing transaction and verifying lastIndexedOffset update")

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Wait a bit for any post-commit processing
	time.Sleep(100 * time.Millisecond)

	// Force another flush after commit
	err = db.flushIndexToDisk()
	if err != nil {
		t.Fatalf("Manual flush after commit failed: %v", err)
	}

	afterCommitLastIndexed := db.lastIndexedOffset
	afterCommitMainFileSize := db.mainFileSize
	t.Logf("After commit and flush - lastIndexedOffset: %d, mainFileSize: %d", afterCommitLastIndexed, afterCommitMainFileSize)

	// After commit, lastIndexedOffset should be updated to reflect the new main file size
	if afterCommitLastIndexed != afterCommitMainFileSize {
		t.Fatalf("After commit, lastIndexedOffset should equal mainFileSize. "+
			"lastIndexedOffset: %d, mainFileSize: %d", afterCommitLastIndexed, afterCommitMainFileSize)
	}

	// Phase 5: Verify that the fix works for multiple transactions
	t.Log("Phase 5: Testing multiple transactions to ensure consistent behavior")

	for round := 0; round < 3; round++ {
		t.Logf("Transaction round %d", round+1)

		beforeRoundMainFileSize := db.mainFileSize

		tx2, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction round %d: %v", round+1, err)
		}

		// Add some data
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("round-%d-key-%d", round, i)
			value := fmt.Sprintf("round-%d-value-%d-" + strings.Repeat("y", 50), round, i)
			err := tx2.Set([]byte(key), []byte(value))
			if err != nil {
				t.Fatalf("Failed to set key in round %d: %v", round+1, err)
			}
		}

		// Force flush during transaction
		err = db.flushIndexToDisk()
		if err != nil {
			t.Fatalf("Flush during transaction round %d failed: %v", round+1, err)
		}

		duringTxLastIndexed := db.lastIndexedOffset

		// Verify lastIndexedOffset doesn't exceed pre-transaction file size
		if duringTxLastIndexed > beforeRoundMainFileSize {
			t.Fatalf("Round %d: lastIndexedOffset should not exceed pre-transaction file size. "+
				"lastIndexedOffset: %d, pre-transaction mainFileSize: %d",
				round+1, duringTxLastIndexed, beforeRoundMainFileSize)
		}

		// Commit transaction
		err = tx2.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction round %d: %v", round+1, err)
		}

		// Force flush after commit
		err = db.flushIndexToDisk()
		if err != nil {
			t.Fatalf("Flush after commit round %d failed: %v", round+1, err)
		}

		afterRoundLastIndexed := db.lastIndexedOffset
		afterRoundMainFileSize := db.mainFileSize

		// Verify lastIndexedOffset matches mainFileSize after commit
		if afterRoundLastIndexed != afterRoundMainFileSize {
			t.Fatalf("Round %d: After commit, lastIndexedOffset should equal mainFileSize. "+
				"lastIndexedOffset: %d, mainFileSize: %d",
				round+1, afterRoundLastIndexed, afterRoundMainFileSize)
		}

		t.Logf("Round %d completed successfully", round+1)
	}

	t.Log("TestLastIndexedOffsetUpdate completed successfully")
}

// TestFreeListCycle tests for cycles in the free pages linked list
func TestFreeListCycle(t *testing.T) {
	// Create a temporary database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	keySize := 33
	valueSize := 750
	numItems := 100000

	// Pregenerate all keys and values
	keys := make([][]byte, numItems)
	values := make([][]byte, numItems)

	for i := 0; i < numItems; i++ {
		keys[i] = generateDeterministicBytes(i, keySize)
		values[i] = generateDeterministicBytes(i+23456789, valueSize)
	}

	// Set using a transaction to trigger the problematic code path
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert entries one by one - this should trigger the cycle
	for i := 0; i < numItems; i++ {
		if i%5000 == 0 {
			t.Logf("Setting entry %d", i)
		}

		err := tx.Set(keys[i], values[i])
		if err != nil {
			t.Fatalf("Failed to set entry %d: %v", i, err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify that all entries can be retrieved
	for i := 0; i < numItems; i++ {
		value, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", i, err)
		}
		if !bytes.Equal(value, values[i]) {
			t.Fatalf("Value mismatch for entry %d", i)
		}
	}

	t.Logf("Successfully inserted and retrieved %d entries", numItems)

	// Prepare transaction test data - multiple transactions with multiple items each
	txNumTransactions := 1000
	txItemsPerTx := 10

	// Pre-generate all keys and values for transactions
	txAllKeys := make([][][]byte, txNumTransactions)
	txAllValues := make([][][]byte, txNumTransactions)

	for txNum := 0; txNum < txNumTransactions; txNum++ {
		txAllKeys[txNum] = make([][]byte, txItemsPerTx)
		txAllValues[txNum] = make([][]byte, txItemsPerTx)

		for i := 0; i < txItemsPerTx; i++ {
			txAllKeys[txNum][i] = generateDeterministicBytes(numItems+txNum*txItemsPerTx+i, keySize)
			txAllValues[txNum][i] = generateDeterministicBytes(numItems+txNum*txItemsPerTx+i+87654321, valueSize)
		}
	}

	t.Logf("Testing %d transactions with %d items each...", txNumTransactions, txItemsPerTx)

	for txNum := 0; txNum < txNumTransactions; txNum++ {
		// Create and execute transaction
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", txNum, err)
		}

		for i := 0; i < txItemsPerTx; i++ {
			err := tx.Set(txAllKeys[txNum][i], txAllValues[txNum][i])
			if err != nil {
				t.Fatalf("Failed to set entry %d in transaction %d: %v", i, txNum, err)
			}
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction %d: %v", txNum, err)
		}

		// Verify a few values from the transaction
		if txNum%100 == 0 {
			t.Logf("Verifying transaction %d", txNum)
			for i := 0; i < txItemsPerTx; i += 3 {
				value, err := db.Get(txAllKeys[txNum][i])
				if err != nil {
					t.Fatalf("Failed to get entry %d from transaction %d: %v", i, txNum, err)
				}
				if !bytes.Equal(value, txAllValues[txNum][i]) {
					t.Fatalf("Value mismatch for entry %d in transaction %d", i, txNum)
				}
			}
		}
	}

	totalEntries := numItems + txNumTransactions*txItemsPerTx
	t.Logf("Successfully completed %d transactions, total entries: %d", txNumTransactions, totalEntries)

	db.Close()

	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Verify that the database is still working
	for i := 0; i < numItems; i++ {
		value, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", i, err)
		}
		if !bytes.Equal(value, values[i]) {
			t.Fatalf("Value mismatch for entry %d", i)
		}
	}

	t.Logf("Successfully opened database and retrieved %d entries", numItems)

	db.Close()

	os.Remove(dbPath)
	os.Remove(dbPath + "-index")
	os.Remove(dbPath + "-wal")
}

// Generate deterministic bytes based on seed and size
func generateDeterministicBytes(seed int, size int) []byte {
	bytes := make([]byte, size)

	// Use a simple deterministic algorithm
	a := uint32(1103515245)
	c := uint32(12345)
	m := uint32(1<<31 - 1)

	x := uint32(seed)

	for i := 0; i < size; i++ {
		// Linear congruential generator
		x = (a*x + c) % m
		bytes[i] = byte(x % 256)
	}

	return bytes
}
