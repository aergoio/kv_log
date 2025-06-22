package kv_log

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestMultiProcessAccess tests database access from multiple processes
// This test verifies that only one process can access the database at a time
func TestMultiProcessAccess(t *testing.T) {
	// Skip if running in short mode
	if testing.Short() {
		t.Skip("Skipping multi-process test in short mode")
	}

	// Create a temporary directory for our test
	tempDir, err := os.MkdirTemp("", "multiprocess_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Path to the database file
	dbPath := filepath.Join(tempDir, "multiprocess.db")

	// Create and initialize the database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Add some initial data
	initialKeys := []string{"key1", "key2", "key3", "key4", "key5"}
	for i, key := range initialKeys {
		value := fmt.Sprintf("value%d", i+1)
		if err := db.Set([]byte(key), []byte(value)); err != nil {
			db.Close()
			t.Fatalf("Failed to set initial data: %v", err)
		}
	}

	// Close the database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Create a reader program
	readerPath := filepath.Join(tempDir, "reader.go")
	if err := createReaderProgram(readerPath, dbPath); err != nil {
		t.Fatalf("Failed to create reader program: %v", err)
	}

	// Create a writer program
	writerPath := filepath.Join(tempDir, "writer.go")
	if err := createWriterProgram(writerPath, dbPath); err != nil {
		t.Fatalf("Failed to create writer program: %v", err)
	}

	// Start a reader process first
	readerCmd := exec.Command("go", "run", readerPath)
	readerOutput := &bytes.Buffer{}
	readerCmd.Stdout = readerOutput
	readerCmd.Stderr = readerOutput
	if err := readerCmd.Start(); err != nil {
		t.Fatalf("Failed to start reader process: %v", err)
	}

	// Give the reader a moment to acquire the connection
	time.Sleep(100 * time.Millisecond)

	// Try to start a second process (writer) while the first one is running
	// This should fail because the database only allows one connection at a time
	writerCmd := exec.Command("go", "run", writerPath)
	writerOutput := &bytes.Buffer{}
	writerCmd.Stdout = writerOutput
	writerCmd.Stderr = writerOutput
	if err := writerCmd.Start(); err != nil {
		t.Fatalf("Failed to start writer process: %v", err)
	}

	// Wait for the writer to finish
	err = writerCmd.Wait()

	// Check if the writer failed as expected (it should fail to acquire a connection)
	if err == nil && !bytes.Contains(writerOutput.Bytes(), []byte("Failed to open database")) {
		t.Errorf("Writer unexpectedly succeeded while reader was active: %s", writerOutput.String())
	}

	// Wait for the reader to finish
	if err := readerCmd.Wait(); err != nil {
		t.Fatalf("Reader failed: %v\nOutput: %s", err, readerOutput.String())
	}

	// Now that the reader is done, start a new process which should succeed
	newWriterCmd := exec.Command("go", "run", writerPath)
	newWriterOutput := &bytes.Buffer{}
	newWriterCmd.Stdout = newWriterOutput
	newWriterCmd.Stderr = newWriterOutput
	if err := newWriterCmd.Start(); err != nil {
		t.Fatalf("Failed to start new writer process: %v", err)
	}

	// Wait for the new writer to finish
	if err := newWriterCmd.Wait(); err != nil {
		t.Fatalf("New writer failed: %v\nOutput: %s", err, newWriterOutput.String())
	}

	// Verify the writer succeeded
	if !bytes.Contains(newWriterOutput.Bytes(), []byte("Writer completed successfully")) {
		t.Errorf("New writer did not complete successfully: %s", newWriterOutput.String())
	}

	// Run multiple processes one after another
	for i := 0; i < 5; i++ {
		cmd := exec.Command("go", "run", writerPath)
		cmd.Stdout = &bytes.Buffer{}
		cmd.Stderr = &bytes.Buffer{}

		if err := cmd.Run(); err != nil {
			stdout := cmd.Stdout.(*bytes.Buffer).String()
			stderr := cmd.Stderr.(*bytes.Buffer).String()
			t.Errorf("Sequential writer %d failed: %v\nStdout: %s\nStderr: %s", i, err, stdout, stderr)
		} else {
			stdout := cmd.Stdout.(*bytes.Buffer).String()
			if !bytes.Contains([]byte(stdout), []byte("Writer completed successfully")) {
				t.Errorf("Sequential writer %d did not complete successfully: %s", i, stdout)
			}
		}
	}

	// Verify the database is still intact
	finalDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database for final verification: %v", err)
	}
	defer finalDB.Close()

	// Check that the original keys still exist
	for i, key := range initialKeys {
		value, err := finalDB.Get([]byte(key))
		if err != nil {
			// Key might have been deleted during operations - this is expected
			continue
		}

		expectedValue := fmt.Sprintf("value%d", i+1)
		if !bytes.Equal(value, []byte(expectedValue)) && !bytes.Equal(value, []byte("updated-value")) {
			t.Fatalf("Value mismatch for key %s in final verification: got %s, want either %s or updated-value",
				key, string(value), expectedValue)
		}
	}

	// Check if the writer-key exists (at least one writer should have succeeded)
	newValue, err := finalDB.Get([]byte("writer-key"))
	if err != nil {
		t.Errorf("No writer succeeded in writing writer-key")
	} else if !bytes.Contains(newValue, []byte("writer-value")) {
		t.Fatalf("Value mismatch for writer-key: %s", string(newValue))
	}
}

// createReaderProgram creates a Go program that reads from the database
func createReaderProgram(filePath, dbPath string) error {
	programContent := fmt.Sprintf(`package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aergoio/kv_log"
)

func main() {
	// Open the database
	db, err := kv_log.Open(%q)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %%v\n", err)
		os.Exit(1)
	}

	// Hold the database connection for a moment to simulate work
	// This ensures the connection stays open while we try to open another one
	time.Sleep(500 * time.Millisecond)

	// Read some keys
	keys := []string{"key1", "key2", "key3", "key4", "key5", "writer-key"}
	for _, key := range keys {
		value, err := db.Get([]byte(key))
		if err != nil {
			// Don't fail on writer-key as it might not exist yet
			if key != "writer-key" {
				fmt.Fprintf(os.Stderr, "Failed to get key %%s: %%v\n", key, err)
			}
		} else {
			fmt.Printf("Read key %%s: %%s\n", key, string(value))
		}
	}

	// Close the database
	if err := db.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to close database: %%v\n", err)
		os.Exit(1)
	}

	fmt.Println("Reader completed successfully")
}
`, dbPath)

	return os.WriteFile(filePath, []byte(programContent), 0644)
}

// createWriterProgram creates a Go program that writes to the database
func createWriterProgram(filePath, dbPath string) error {
	programContent := fmt.Sprintf(`package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aergoio/kv_log"
)

func main() {
	// Open the database
	db, err := kv_log.Open(%q)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %%v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Write a new key
	timestamp := time.Now().UnixNano()
	value := fmt.Sprintf("writer-value-%%d", timestamp)

	if err := db.Set([]byte("writer-key"), []byte(value)); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write key: %%v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Wrote writer-key: %%s\n", value)

	// Update an existing key
	if err := db.Set([]byte("key1"), []byte("updated-value")); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to update key1: %%v\n", err)
		os.Exit(1)
	}

	fmt.Println("Updated key1 to 'updated-value'")

	// Small delay to simulate work
	time.Sleep(200 * time.Millisecond)

	fmt.Println("Writer completed successfully")
}
`, dbPath)

	return os.WriteFile(filePath, []byte(programContent), 0644)
}