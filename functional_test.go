package kv_log

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestDatabaseBasicOperations(t *testing.T) {
	// Create a test database
	dbPath := "test_basic.db"

	// Clean up any existing test database
	os.Remove(dbPath)

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		//os.Remove(dbPath) // Clean up after test
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

func TestDatabasePersistence1(t *testing.T) {
	// Create a test database
	dbPath := "test_persistence.db"

	// Clean up any existing test database
	os.Remove(dbPath)

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
		os.Remove(dbPath) // Clean up after test
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
		os.Remove(dbPath) // Clean up after test
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

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath) // Clean up after test
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

	// Create an iterator
	it := db.NewIterator()
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

	// Create a new iterator
	modifiedIt := db.NewIterator()
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

	// Test iterator with empty database
	emptyDbPath := "test_empty_iterator.db"
	os.Remove(emptyDbPath)

	emptyDb, err := Open(emptyDbPath, Options{"MainIndexPages": 1})
	if err != nil {
		t.Fatalf("Failed to open empty database: %v", err)
	}
	defer func() {
		emptyDb.Close()
		os.Remove(emptyDbPath)
	}()

	emptyIt := emptyDb.NewIterator()
	defer emptyIt.Close()

	// Verify the iterator is not valid for an empty database
	if emptyIt.Valid() {
		t.Fatalf("Iterator for empty database should not be valid")
	}
}

func TestIteratorWithLargeDataset(t *testing.T) {
	// Create a test database
	dbPath := "test_iterator_large_dataset.db"

	// Clean up any existing test database
	os.Remove(dbPath)

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove(dbPath) // Clean up after test
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

	// Create an iterator
	it := db.NewIterator()
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
}

func TestDatabaseReindex(t *testing.T) {
	// Create a test database
	dbPath := "test_reindex.db"
	indexPath := dbPath + "-index"

	// Clean up any existing test database
	os.Remove(dbPath)
	os.Remove(indexPath)

	// Open a new database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

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

	// Run diff command to compare the index files
	cmd := exec.Command("diff", "-q", originalIndexPath, indexPath)
	output, err := cmd.CombinedOutput()

	// If diff finds no differences, it returns exit status 0
	// If files differ, it returns exit status 1
	// For any other error, it returns other non-zero status
	if err != nil {
		// Files should be identical, so any difference is an error
		t.Fatalf("Index files should be identical but differ: %v, output: %s", err, string(output))
	}

	// Clean up
	os.Remove(dbPath)
	os.Remove(indexPath)
	os.Remove(originalIndexPath)
}
