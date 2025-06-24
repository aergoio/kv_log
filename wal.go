package kv_log

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"sync"
	"time"
)

// WAL constants
const (
	// WAL magic string for identification
	WalMagicString = "KV_WAL"
	// WAL header size
	WalHeaderSize = 32
	// WAL frame header size
	WalFrameHeaderSize = 20
)

// WalPageEntry represents a page entry in the WAL cache
type WalPageEntry struct {
	PageNumber     uint32     // Page number of the page in the main db file
	Data           []byte     // The page data
	SequenceNumber int64      // Transaction sequence number when this page was written
	Next           *WalPageEntry // Pointer to the next entry with the same page number (for rollback)
}

// WalInfo represents the WAL file information
type WalInfo struct {
	file           *os.File
	sequenceNumber int64
	salt1          uint32
	salt2          uint32
	walPath        string
	hasher         *crc32.Table // CRC32 table for checksum calculations
	checksum       uint32       // Current cumulative checksum
	lastCommitChecksum uint32   // Checksum value at the last commit
	pageCache      map[uint32]*WalPageEntry // In-memory cache of pages in the WAL
	cacheMutex     sync.RWMutex // Mutex to protect the page cache
	lastCommitPosition int64    // Position just after the last valid commit
	nextWritePosition  int64    // Position where the next frame will be written
}

// addToWalCache adds a page to the WAL cache
func (db *DB) addToWalCache(pageNumber uint32, data []byte, commitSequence int64) {
	if db.walInfo == nil {
		return
	}

	db.walInfo.cacheMutex.Lock()
	defer db.walInfo.cacheMutex.Unlock()

	// Create the cache map if it doesn't exist
	if db.walInfo.pageCache == nil {
		db.walInfo.pageCache = make(map[uint32]*WalPageEntry)
	}

	// Check if there's already an entry for this page from the current transaction
	existingEntry, exists := db.walInfo.pageCache[pageNumber]

	if exists && existingEntry.SequenceNumber == commitSequence {
		// Page from current transaction already exists, replace its data
		existingEntry.Data = data
		return
	}

	// Create a new entry with the current sequence number
	newEntry := &WalPageEntry{
		PageNumber:     pageNumber,
		Data:           data,
		SequenceNumber: commitSequence,
		Next:           existingEntry, // Link to previous version if it exists
	}

	// Update the cache with the new entry as the head of the list
	db.walInfo.pageCache[pageNumber] = newEntry
}

// discardNewerPages removes pages from the current transaction from the cache
func (db *DB) discardNewerPages(currentSeq int64) {
	db.walInfo.cacheMutex.Lock()
	defer db.walInfo.cacheMutex.Unlock()

	// Iterate through all pages in the cache
	for pageNumber, entry := range db.walInfo.pageCache {
		// Find the first entry that's not from the current transaction
		var newHead *WalPageEntry = entry
		for newHead != nil && newHead.SequenceNumber == currentSeq {
			newHead = newHead.Next
		}
		// Update the cache with the new head (or delete if no valid entries remain)
		if newHead != nil {
			db.walInfo.pageCache[pageNumber] = newHead
		} else {
			delete(db.walInfo.pageCache, pageNumber)
		}
	}
}

// discardOldPageVersions removes older versions of pages after a commit
func (db *DB) discardOldPageVersions() {
	db.walInfo.cacheMutex.Lock()
	defer db.walInfo.cacheMutex.Unlock()

	// Iterate through all pages in the cache
	for _, entry := range db.walInfo.pageCache {
		// Keep only the most recent version (head of the list) and remove older versions
		if entry != nil && entry.Next != nil {
			// This page is from the current transaction being committed, keep it and remove older versions
			entry.Next = nil
		}
	}
}

// getLatestFromWalCache gets the most recent version of a page from the WAL cache
func (db *DB) getLatestFromWalCache(pageNumber uint32) []byte {
	// If the WAL info is not initialized or the page cache is nil, return nil
	if db.walInfo == nil || db.walInfo.pageCache == nil {
		return nil
	}

	db.walInfo.cacheMutex.RLock()
	defer db.walInfo.cacheMutex.RUnlock()

	// Check if there's an entry for this page
	if entry, ok := db.walInfo.pageCache[pageNumber]; ok {
		// Return the most recent version (first in the linked list)
		return entry.Data
	}

	return nil
}

// writeToWAL writes an index page to the WAL file
func (db *DB) writeToWAL(pageData []byte, pageNumber uint32) error {
	// Check if we're in WAL mode
	if !db.useWAL {
		return nil
	}
	// Open or create the WAL file if it doesn't exist
	if db.walInfo == nil {
		err := db.openWAL()
		if err != nil {
			return err
		}
	}

	// Write the frame to the WAL file
	err := db.writeFrame(pageNumber, pageData)
	if err != nil {
		return err
	}

	// Add the page to the in-memory WAL cache
	db.addToWalCache(pageNumber, pageData, 0)  // TODO: add the correct sequence number

	return nil
}

func (db *DB) readFromWAL(pageNumber uint32) ([]byte, error) {
	// Check if we're in WAL mode
	if !db.useWAL {
		return nil, nil
	}
	// Open or create the WAL file if it doesn't exist
	if db.walInfo == nil {
		err := db.openWAL()
		if err != nil {
			return nil, err
		}
	}

	// Check if the page is in the WAL cache
	if cachedData := db.getLatestFromWalCache(pageNumber); cachedData != nil {
		return cachedData, nil
	}
	// The page is not in cache so it is not in the WAL file
	// There is no need to read from the WAL file again because all pages are already loaded when the WAL file is opened
	return nil, nil
}

// createWAL creates a new WAL file
func (db *DB) createWAL() error {

	// If WAL file is already open, return
	if db.walInfo != nil {
		return nil
	}

	// Generate WAL file path by appending "-wal" to the main db file path
	walPath := db.filePath + "-wal"

	// Create a new WAL file
	walFile, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}

	// Initialize WAL info
	db.walInfo = &WalInfo{
		file:      walFile,
		walPath:   walPath,
		hasher:    crc32.IEEETable,
		checksum:  0,
		pageCache: make(map[uint32]*WalPageEntry), // Initialize the page cache
	}

	// Initialize the WAL info
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	db.walInfo.salt1 = r.Uint32()
	db.walInfo.salt2 = r.Uint32()
	db.walInfo.sequenceNumber = 1
	db.walInfo.lastCommitPosition = WalHeaderSize // For a new file, commit position is right after header
	db.walInfo.nextWritePosition = WalHeaderSize  // Start writing after the header

	// Create header buffer
	header := make([]byte, WalHeaderSize)

	// Write magic string
	copy(header[0:6], WalMagicString)

	// Write version (currently 1)
	binary.BigEndian.PutUint16(header[6:8], 1)

	// Write sequence number
	binary.BigEndian.PutUint32(header[8:12], uint32(db.walInfo.sequenceNumber))

	// Write salts
	binary.BigEndian.PutUint32(header[12:16], db.walInfo.salt1)
	binary.BigEndian.PutUint32(header[16:20], db.walInfo.salt2)

	// Write database ID
	binary.BigEndian.PutUint64(header[20:28], db.databaseID)

	// Calculate checksum for header (first 28 bytes)
	checksum := crc32.ChecksumIEEE(header[0:28])
	binary.BigEndian.PutUint32(header[28:32], checksum)

	// Store the header checksum
	db.walInfo.checksum = checksum

	// Write header to WAL file
	if _, err := db.walInfo.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}

	// Sync if in full sync mode
	if db.syncMode == SyncOn {
		if err := db.walInfo.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file: %w", err)
		}
	}

	return nil
}

// writeFrameHeader writes a frame header to the WAL file
func (db *DB) writeFrameHeader(pageNumber uint32, commitFlag int, data []byte) (int64, error) {

	// Use the nextWritePosition instead of getting the file size
	frameOffset := db.walInfo.nextWritePosition

	// Create frame header buffer
	frameHeader := make([]byte, WalFrameHeaderSize)

	// Write page number
	binary.BigEndian.PutUint32(frameHeader[0:4], pageNumber)

	// Write commit flag (1 for commit records, 0 for normal frames)
	binary.BigEndian.PutUint32(frameHeader[4:8], uint32(commitFlag))

	// Write salts
	binary.BigEndian.PutUint32(frameHeader[8:12], db.walInfo.salt1)
	binary.BigEndian.PutUint32(frameHeader[12:16], db.walInfo.salt2)

	// Calculate cumulative checksum
	// Start with the current checksum value
	checksum := db.walInfo.checksum

	// Update checksum with first 12 bytes of frame header (page number and commit flag)
	checksum = crc32.Update(checksum, db.walInfo.hasher, frameHeader[0:8])

	// Update checksum with page data if provided
	if data != nil {
		checksum = crc32.Update(checksum, db.walInfo.hasher, data)
	}

	// Store the new checksum
	db.walInfo.checksum = checksum

	// Write the checksum to the frame header
	binary.BigEndian.PutUint32(frameHeader[16:20], checksum)

	// Write frame header to WAL file
	if _, err := db.walInfo.file.WriteAt(frameHeader, frameOffset); err != nil {
		return 0, fmt.Errorf("failed to write WAL frame header: %w", err)
	}

	return frameOffset, nil
}

// writeFrame writes a frame to the WAL file
func (db *DB) writeFrame(pageNumber uint32, pageData []byte) error {
	// Write the frame header
	frameOffset, err := db.writeFrameHeader(pageNumber, 0, pageData)
	if err != nil {
		return err
	}

	// Write page data after the header
	if _, err := db.walInfo.file.WriteAt(pageData, frameOffset+WalFrameHeaderSize); err != nil {
		return fmt.Errorf("failed to write WAL frame data: %w", err)
	}

	// Update the nextWritePosition for the next frame
	db.walInfo.nextWritePosition = frameOffset + WalFrameHeaderSize + int64(len(pageData))

	return nil
}

// scanWAL loads valid frames from the WAL file and populates the in-memory cache
func (db *DB) scanWAL() error {
	if db.walInfo == nil {
		return nil
	}

	// Get WAL file size
	walFileInfo, err := db.walInfo.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get WAL file size: %w", err)
	}

	// If file is empty or only has a header, there's nothing to scan
	if walFileInfo.Size() <= WalHeaderSize {
		// TODO: check if the header is valid
		db.walInfo.lastCommitPosition = WalHeaderSize
		db.walInfo.nextWritePosition = WalHeaderSize
		return nil
	}

	// Read WAL header first
	headerBuf := make([]byte, WalHeaderSize)
	if _, err := db.walInfo.file.ReadAt(headerBuf, 0); err != nil {
		return fmt.Errorf("failed to read WAL header: %w", err)
	}

	// Verify magic string
	if string(headerBuf[0:6]) != WalMagicString {
		return fmt.Errorf("invalid WAL file: magic string mismatch")
	}

	// Verify header checksum
	headerChecksum := binary.BigEndian.Uint32(headerBuf[28:32])
	calculatedHeaderChecksum := crc32.ChecksumIEEE(headerBuf[0:28])
	if headerChecksum != calculatedHeaderChecksum {
		return fmt.Errorf("invalid WAL file: header checksum mismatch")
	}

	// Extract sequence number and salts from header
	db.walInfo.sequenceNumber = int64(binary.BigEndian.Uint32(headerBuf[8:12]))
	db.walInfo.salt1 = binary.BigEndian.Uint32(headerBuf[12:16])
	db.walInfo.salt2 = binary.BigEndian.Uint32(headerBuf[16:20])

	// Extract database ID from header
	walDatabaseID := binary.BigEndian.Uint64(headerBuf[20:28])

	// Check if database ID matches
	if walDatabaseID != db.databaseID {
		// Database ID mismatch, delete the WAL file
		debugPrint("WAL database ID mismatch: %d vs %d, deleting WAL file\n", walDatabaseID, db.databaseID)
		if err := db.deleteWAL(); err != nil {
			return fmt.Errorf("failed to delete mismatched WAL file: %w", err)
		}
		// Create a new WAL file with the correct database ID
		return db.createWAL()
	}

	// Initialize page cache if needed
	if db.walInfo.pageCache == nil {
		db.walInfo.pageCache = make(map[uint32]*WalPageEntry)
	}

	// Start reading frames from after the header
	offset := int64(WalHeaderSize)
	lastCommitOffset := offset // Initialize to just after header
	commitSequence := int64(1)

	// Initialize running checksum for frame validation
	runningChecksum := headerChecksum
	lastCommitChecksum := headerChecksum

	// Track the maximum page number to update index file size
	maxPageNumber := uint32(0)

	for offset+WalFrameHeaderSize <= walFileInfo.Size() {
		// Read frame header
		frameHeader := make([]byte, WalFrameHeaderSize)
		if _, err := db.walInfo.file.ReadAt(frameHeader, offset); err != nil {
			// Error reading frame header, stop scanning
			break
		}

		// Verify salt values match the header
		frameSalt1 := binary.BigEndian.Uint32(frameHeader[8:12])
		frameSalt2 := binary.BigEndian.Uint32(frameHeader[12:16])
		if frameSalt1 != db.walInfo.salt1 || frameSalt2 != db.walInfo.salt2 {
			// Salt mismatch, stop scanning
			break
		}

		// Check if this is a commit record (commit flag = 1)
		commitFlag := binary.BigEndian.Uint32(frameHeader[4:8])
		isCommit := commitFlag == 1

		// Extract the frame checksum
		frameChecksum := binary.BigEndian.Uint32(frameHeader[16:20])

		// Calculate expected checksum
		// Update running checksum with first 12 bytes of frame header (page number and commit flag)
		runningChecksum = crc32.Update(runningChecksum, db.walInfo.hasher, frameHeader[0:8])

		if isCommit {
			// This is a commit record, update lastCommitOffset
			// Commit records have no page data, just the header

			// Verify checksum
			if frameChecksum != runningChecksum {
				// Checksum mismatch, stop scanning
				break
			}

			// Increment the commit sequence number
			commitSequence++

			// Update the last commit offset
			lastCommitOffset = offset + WalFrameHeaderSize

			// Store the checksum from the commit record
			lastCommitChecksum = frameChecksum

			// Clean up old page versions after commit
			db.discardOldPageVersions()

			// Move to the next frame
			offset = lastCommitOffset
			continue
		}

		// This is a regular frame with page data
		// Ensure we don't read past the end of the file
		pageSize := int64(PageSize)
		if offset+WalFrameHeaderSize+pageSize > walFileInfo.Size() {
			break
		}

		// Extract page number from frame header
		pageNumber := binary.BigEndian.Uint32(frameHeader[0:4])

		// Track the maximum page number
		if pageNumber > maxPageNumber {
			maxPageNumber = pageNumber
		}

		// Read the page data
		pageData := make([]byte, pageSize)
		if _, err := db.walInfo.file.ReadAt(pageData, offset+WalFrameHeaderSize); err != nil {
			// Error reading page data, stop scanning
			break
		}

		// Update running checksum with page data
		runningChecksum = crc32.Update(runningChecksum, db.walInfo.hasher, pageData)

		// Verify checksum
		if frameChecksum != runningChecksum {
			// Checksum mismatch, stop scanning
			break
		}

		// For pages from the current transaction, we need to handle them specially
		// If we already have this page in the cache from the current transaction, the new version should replace it
		// If we have this page only from a previous transaction, we should add the new version

		// Add the page directly to the in-memory cache
		db.addToWalCache(pageNumber, pageData, commitSequence)

		// Move to the next frame
		offset += WalFrameHeaderSize + pageSize
	}

	// If the last scanned position is beyond the last commit position, there were frames without a commit
	// In this case, we need to remove all pages from the uncommitted transaction from the cache
	if offset > lastCommitOffset {
		// Remove pages from the current uncommitted transaction from the cache
		db.discardNewerPages(commitSequence)
	}

	// Update the position fields
	db.walInfo.lastCommitPosition = lastCommitOffset
	db.walInfo.nextWritePosition = lastCommitOffset

	// Store the final checksum value
	db.walInfo.lastCommitChecksum = lastCommitChecksum
	db.walInfo.checksum = lastCommitChecksum

	// Update index file size to account for WAL pages
	if maxPageNumber > 0 {
		requiredSize := int64(maxPageNumber+1) * PageSize
		if requiredSize > db.indexFileSize {
			db.indexFileSize = requiredSize
		}
	}

	// Increment sequence number after successful scan
	db.walInfo.sequenceNumber++

	return nil
}

// WalCommit writes a commit record to the WAL file
func (db *DB) WalCommit() error {
	// If WAL info is not initialized, return
	if db.walInfo == nil {
		return nil
	}

	// Write a commit record - just a header with commit flag set to 1 and no page data
	frameOffset, err := db.writeFrameHeader(0, 1, nil)
	if err != nil {
		return err
	}

	// Update the next write position (only header, no page data for commit records)
	db.walInfo.nextWritePosition = frameOffset + WalFrameHeaderSize

	// Update the lastCommitPosition to the current nextWritePosition
	db.walInfo.lastCommitPosition = db.walInfo.nextWritePosition

	// Store the current checksum as the last committed checksum
	db.walInfo.lastCommitChecksum = db.walInfo.checksum

	// Sync if in full sync mode
	if db.syncMode == SyncOn {
		// Sync the main db file
		if err := db.indexFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync index file after commit: %w", err)
		}
		// Sync the WAL file
		if err := db.walInfo.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file after commit: %w", err)
		}
	}

	// Clean up old page versions from cache after successful commit
	db.discardOldPageVersions()

	// Increment sequence number after successful commit
	db.walInfo.sequenceNumber++

	// maybe notify the checkpoint thread worker that a commit record has been written
	//db.checkpointNotify <- true

	return nil
}

// WalRollback
func (db *DB) WalRollback() error {
	if db.walInfo == nil {
		return nil
	}

	// Reset the write position to the last commit position
	db.walInfo.nextWritePosition = db.walInfo.lastCommitPosition

	// Restore checksum to the value from the last commit
	db.walInfo.checksum = db.walInfo.lastCommitChecksum

	// Get current sequence number before rollback
	currentSeq := db.walInfo.sequenceNumber

	// Remove pages that are from the current transaction from the cache
	db.discardNewerPages(currentSeq)

	// Keep the same sequence number for the next transaction attempt
	// This is important to maintain consistency

	return nil
}

// doCheckpoint writes the current WAL file to the main db file and clears the cache
func (db *DB) doCheckpoint() error {
	if db.walInfo == nil {
		return nil
	}

	// Lock the cache during checkpoint
	db.walInfo.cacheMutex.Lock()
	defer db.walInfo.cacheMutex.Unlock()

	// TODO: Implement writing WAL pages to the main db file

	// After successful checkpoint, clear the cache
	db.walInfo.pageCache = make(map[uint32]*WalPageEntry)

	// Reset the checksum for the next WAL cycle
	db.walInfo.checksum = 0
	db.walInfo.lastCommitChecksum = 0

	return nil
}

// openWAL opens an existing WAL file without creating it
func (db *DB) openWAL() error {
	// If WAL file is already open, return
	if db.walInfo != nil {
		return nil
	}

	// Generate WAL file path by appending "-wal" to the main db file path
	walPath := db.filePath + "-wal"

	// Check if WAL file exists
	if _, err := os.Stat(walPath); err != nil {
		// WAL file doesn't exist, create it
		return db.createWAL()
	}

	// Open the existing WAL file
	walFile, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("failed to open existing WAL file: %w", err)
	}

	// Initialize WAL info
	db.walInfo = &WalInfo{
		file:      walFile,
		walPath:   walPath,
		hasher:    crc32.IEEETable,
		pageCache: make(map[uint32]*WalPageEntry),
	}

	// Scan the WAL file to load any existing frames
	if err := db.scanWAL(); err != nil {
		// If there's an error scanning the WAL (which could be due to a database ID mismatch),
		// the scanWAL function will handle it by deleting and recreating the WAL file
		return err
	}

	return nil
}

// deleteWAL deletes the WAL file associated with the database
func (db *DB) deleteWAL() error {
	// If the WAL info is not initialized, nothing to do
	if db.walInfo == nil {
		// Generate WAL file path by appending "-wal" to the main db file path
		walPath := db.filePath + "-wal"

		// Check if WAL file exists
		if _, err := os.Stat(walPath); err == nil {
			// WAL file exists, delete it
			if err := os.Remove(walPath); err != nil {
				return fmt.Errorf("failed to delete WAL file: %w", err)
			}
		}
		return nil
	}

	// Close the WAL file if it's open
	if db.walInfo.file != nil {
		if err := db.walInfo.file.Close(); err != nil {
			return fmt.Errorf("failed to close WAL file: %w", err)
		}
	}

	// Delete the WAL file
	if err := os.Remove(db.walInfo.walPath); err != nil {
		return fmt.Errorf("failed to delete WAL file: %w", err)
	}

	// Reset WAL info
	db.walInfo = nil

	return nil
}
