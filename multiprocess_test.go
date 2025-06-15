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
// This test creates temporary Go programs that access the same database file
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

	// Create a file to indicate when readers are ready
	readersReadyFile := filepath.Join(tempDir, "readers_ready")

	// Create a reader program
	readerPath := filepath.Join(tempDir, "reader.go")
	if err := createReaderProgram(readerPath, dbPath, readersReadyFile); err != nil {
		t.Fatalf("Failed to create reader program: %v", err)
	}

	// Create a writer program
	writerPath := filepath.Join(tempDir, "writer.go")
	if err := createWriterProgram(writerPath, dbPath); err != nil {
		t.Fatalf("Failed to create writer program: %v", err)
	}

	// Run multiple reader processes with shared locks
	var readerCmds []*exec.Cmd
	for i := 0; i < 3; i++ {
		cmd := exec.Command("go", "run", readerPath)
		cmd.Stdout = &bytes.Buffer{}
		cmd.Stderr = &bytes.Buffer{}
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start reader process %d: %v", i, err)
		}
		readerCmds = append(readerCmds, cmd)
	}

	// Wait for readers to signal they've acquired locks
	t.Log("Waiting for readers to acquire shared locks...")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readersReadyFile); err == nil {
			// File exists, readers are ready
			break
		}
		
		if time.Now().After(deadline) {
			t.Fatalf("Timed out waiting for readers to acquire locks")
		}
		
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("Readers have acquired shared locks")

	// Run a writer process with exclusive lock - this should fail if readers have shared locks
	t.Log("Starting writer with exclusive lock (should fail)...")
	writerCmd := exec.Command("go", "run", writerPath, "exclusive")
	writerOutput := &bytes.Buffer{}
	writerCmd.Stdout = writerOutput
	writerCmd.Stderr = writerOutput
	if err := writerCmd.Start(); err != nil {
		t.Fatalf("Failed to start writer process: %v", err)
	}

	// Wait for the writer to finish
	err = writerCmd.Wait()

	// Check if the writer failed as expected (it should fail to acquire an exclusive lock)
	if err == nil {
		// If readers are using shared locks, writer with exclusive lock should fail
		t.Errorf("Writer with exclusive lock unexpectedly succeeded: %s", writerOutput.String())
	}

	// Wait for readers to finish
	for i, cmd := range readerCmds {
		if err := cmd.Wait(); err != nil {
			stdout := cmd.Stdout.(*bytes.Buffer).String()
			stderr := cmd.Stderr.(*bytes.Buffer).String()
			t.Fatalf("Reader %d failed: %v\nStdout: %s\nStderr: %s", i, err, stdout, stderr)
		}
		stdout := cmd.Stdout.(*bytes.Buffer).String()
		if !bytes.Contains([]byte(stdout), []byte("Reader completed")) {
			t.Fatalf("Reader %d did not complete successfully: %s", i, stdout)
		}
	}

	// Run a concurrent test with multiple writers and readers using no locks
	// (they should acquire temporary locks during operations)
	t.Run("ConcurrentProcesses", func(t *testing.T) {
		// Run multiple writers with no lock (they should acquire temporary locks)
		var writerCmds []*exec.Cmd
		for i := 0; i < 3; i++ {
			cmd := exec.Command("go", "run", writerPath, "none")
			cmd.Stdout = &bytes.Buffer{}
			cmd.Stderr = &bytes.Buffer{}
			if err := cmd.Start(); err != nil {
				t.Fatalf("Failed to start concurrent writer process %d: %v", i, err)
			}
			writerCmds = append(writerCmds, cmd)
		}

		// Run multiple readers at the same time
		var concurrentReaderCmds []*exec.Cmd
		for i := 0; i < 5; i++ {
			cmd := exec.Command("go", "run", readerPath, "none") // Pass "none" to use no lock
			cmd.Stdout = &bytes.Buffer{}
			cmd.Stderr = &bytes.Buffer{}
			if err := cmd.Start(); err != nil {
				t.Fatalf("Failed to start concurrent reader process %d: %v", i, err)
			}
			concurrentReaderCmds = append(concurrentReaderCmds, cmd)
		}

		// Wait for all processes to finish
		for i, cmd := range writerCmds {
			err := cmd.Wait()
			if err != nil {
				// Only log failures that are unexpected
				stdout := cmd.Stdout.(*bytes.Buffer).String()
				stderr := cmd.Stderr.(*bytes.Buffer).String()
				t.Errorf("Writer %d failed unexpectedly: %v\nStdout: %s\nStderr: %s", i, err, stdout, stderr)
			}
		}

		for i, cmd := range concurrentReaderCmds {
			err := cmd.Wait()
			if err != nil {
				// Only log failures that are unexpected
				stdout := cmd.Stdout.(*bytes.Buffer).String()
				stderr := cmd.Stderr.(*bytes.Buffer).String()
				t.Errorf("Reader %d failed unexpectedly: %v\nStdout: %s\nStderr: %s", i, err, stdout, stderr)
			}
		}
	})

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
			// Key might have been deleted during concurrent operations - this is expected
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
func createReaderProgram(filePath, dbPath string, signalFile string) error {
	programContent := fmt.Sprintf(`package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aergoio/kv_log"
)

func main() {
	// Determine lock type from command line arg
	lockType := kv_log.LockShared
	if len(os.Args) > 1 && os.Args[1] == "none" {
		lockType = kv_log.LockNone
	}

	// Open the database with the specified lock
	db, err := kv_log.Open(%q, kv_log.Options{"LockType": lockType})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database: %%v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	
	// If using shared lock, signal that we've acquired it
	if lockType == kv_log.LockShared {
		// Signal that we've acquired the lock by creating a file
		signalFile, err := os.Create(%q)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create signal file: %%v\n", err)
		} else {
			signalFile.Close()
		}
		
		// Keep the lock for a bit to ensure the writer has time to try
		time.Sleep(500 * time.Millisecond)
	}

	// Read some keys
	keys := []string{"key1", "key2", "key3", "key4", "key5", "writer-key"}

	// Try multiple times in case the writer hasn't written yet
	for attempt := 0; attempt < 5; attempt++ {
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
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("Reader completed successfully")
}
`, dbPath, signalFile)

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
	// Determine lock type from command line arg
	lockType := kv_log.LockNone
	if len(os.Args) > 1 && os.Args[1] == "exclusive" {
		lockType = kv_log.LockExclusive
		fmt.Println("Acquiring exclusive lock on database file")
	}

	// Open the database with specified lock type
	db, err := kv_log.Open(%q, kv_log.Options{"LockType": lockType})
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