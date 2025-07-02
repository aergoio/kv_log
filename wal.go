package kv_log

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"sort"
	"time"
)

// WAL constants
const (
	// WAL magic string for identification
	WalMagicString = "KV_WAL"
	// WAL header size
	WalHeaderSize = 28
	// WAL frame header size
	WalFrameHeaderSize = 20
)

// WalInfo represents the WAL file information
type WalInfo struct {
	file           *os.File
	salt1          uint32
	salt2          uint32
	walPath        string
	hasher         *crc32.Table // CRC32 table for checksum calculations
	checksum       uint32       // Current cumulative checksum
	lastCommitChecksum uint32   // Checksum value at the last commit
	lastCommitPosition int64    // Position just after the last valid commit
	nextWritePosition  int64    // Position where the next frame will be written
	lastCommitSequence int64    // Sequence number of the last committed transaction
	lastCheckpointSequence int64 // Sequence number of the last checkpointed transaction
}

// ------------------------------------------------------------------------------------------------
// Local WAL cache (used only at initialization when scanning the WAL file)
// ------------------------------------------------------------------------------------------------

// WalPageEntry represents a page entry in the WAL cache
type WalPageEntry struct {
	PageNumber     uint32     // Page number of the page in the index file
	Data           []byte     // The page data
	SequenceNumber int64      // Transaction sequence number when this page was written
	Next           *WalPageEntry // Pointer to the next entry with the same page number (for rollback)
}

// localCache handles operations on the local WAL page cache
type localCache map[uint32]*WalPageEntry

// add adds a page to the WAL cache
func (cache localCache) add(pageNumber uint32, data []byte, commitSequence int64) {
	// Check if there's already an entry for this page from the current transaction
	existingEntry, exists := cache[pageNumber]

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
	cache[pageNumber] = newEntry
}

// discardNewerPages removes pages from the current transaction from the cache
func (cache localCache) discardNewerPages(currentSeq int64) {
	// Iterate through all pages in the cache
	for pageNumber, entry := range cache {
		// Find the first entry that's not from the current transaction
		var newHead *WalPageEntry = entry
		for newHead != nil && newHead.SequenceNumber == currentSeq {
			newHead = newHead.Next
		}
		// Update the cache with the new head (or delete if no valid entries remain)
		if newHead != nil {
			cache[pageNumber] = newHead
		} else {
			delete(cache, pageNumber)
		}
	}
}

// discardOldPageVersions removes older versions of pages after a commit
func (cache localCache) discardOldPageVersions() {
	// Iterate through all pages in the cache
	for _, entry := range cache {
		// Keep only the most recent version (head of the list) and remove older versions
		if entry != nil && entry.Next != nil {
			// This page is from the current transaction being committed, keep it and remove older versions
			entry.Next = nil
		}
	}
}

// ------------------------------------------------------------------------------------------------
// WAL file operations
// ------------------------------------------------------------------------------------------------

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
	//db.addToWalCache(pageNumber, pageData, db.txnSequence)

	return nil
}

// createWAL creates a new WAL file
func (db *DB) createWAL() error {
	var salt1 uint32  // this salt is incremented on each WAL reset
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	var lastCommitSequence int64 = 0
	var lastCheckpointSequence int64 = 0

	if db.walInfo == nil {
		// Generate a new salt1 for a new WAL file
		salt1 = r.Uint32()
	} else {
		// Use the salt1 from the existing WAL file
		salt1 = db.walInfo.salt1 + 1 // increment the salt1 on each WAL reset
		lastCommitSequence = db.walInfo.lastCommitSequence
		lastCheckpointSequence = db.walInfo.lastCheckpointSequence
	}

	// Generate WAL file path by appending "-wal" to the main db file path
	walPath := db.filePath + "-wal"

	// Open or create the WAL file
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
	}

	// Initialize the WAL info
	db.walInfo.salt1 = salt1
	db.walInfo.salt2 = r.Uint32()
	db.walInfo.lastCommitPosition = WalHeaderSize // For a new file, commit position is right after header
	db.walInfo.nextWritePosition = WalHeaderSize  // Start writing after the header
	db.walInfo.lastCommitSequence = lastCommitSequence
	db.walInfo.lastCheckpointSequence = lastCheckpointSequence

	// Create header buffer
	header := make([]byte, WalHeaderSize)

	// Write magic string
	copy(header[0:6], WalMagicString)

	// Write version (currently 1)
	binary.BigEndian.PutUint16(header[6:8], 1)

	// Write salts
	binary.BigEndian.PutUint32(header[8:12], db.walInfo.salt1)
	binary.BigEndian.PutUint32(header[12:16], db.walInfo.salt2)

	// Write database ID
	binary.BigEndian.PutUint64(header[16:24], db.databaseID)

	// Calculate checksum for header (first 24 bytes)
	checksum := crc32.ChecksumIEEE(header[0:24])
	binary.BigEndian.PutUint32(header[24:28], checksum)

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
	headerChecksum := binary.BigEndian.Uint32(headerBuf[24:28])
	calculatedHeaderChecksum := crc32.ChecksumIEEE(headerBuf[0:24])
	if headerChecksum != calculatedHeaderChecksum {
		return fmt.Errorf("invalid WAL file: header checksum mismatch")
	}

	// Extract salts from header
	db.walInfo.salt1 = binary.BigEndian.Uint32(headerBuf[8:12])
	db.walInfo.salt2 = binary.BigEndian.Uint32(headerBuf[12:16])

	// Extract database ID from header
	walDatabaseID := binary.BigEndian.Uint64(headerBuf[16:24])

	// Check if database ID matches
	if walDatabaseID != db.databaseID {
		// Database ID mismatch, reset the WAL file
		debugPrint("WAL database ID mismatch: %d vs %d, resetting WAL file\n", walDatabaseID, db.databaseID)
		// Reset the WAL file
		return db.resetWAL()
	}

	// Initialize page cache as a local variable
	localCache := make(localCache)

	// Start reading frames from after the header
	offset := int64(WalHeaderSize)
	lastCommitOffset := offset // Initialize to just after header
	commitSequence := int64(1)

	// Initialize running checksum for frame validation
	runningChecksum := headerChecksum
	lastCommitChecksum := headerChecksum

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

			// Update the last commit offset
			lastCommitOffset = offset + WalFrameHeaderSize

			// Store the checksum from the commit record
			lastCommitChecksum = frameChecksum

			// Clean up old page versions after commit
			localCache.discardOldPageVersions()

			if !db.readOnly {
				// Copy these pages to the index file
				db.copyPagesToIndexFile(localCache, commitSequence)
			}

			// Increment the commit sequence number
			commitSequence++

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
		localCache.add(pageNumber, pageData, commitSequence)

		// Move to the next frame
		offset += WalFrameHeaderSize + pageSize
	}

	// If the last scanned position is beyond the last commit position, there were frames without a commit
	// In this case, we need to remove all pages from the uncommitted transaction from the cache
	if offset > lastCommitOffset {
		// Remove pages from the current uncommitted transaction from the cache
		localCache.discardNewerPages(commitSequence)
	}

	// Update the position fields
	db.walInfo.lastCommitPosition = lastCommitOffset
	db.walInfo.nextWritePosition = lastCommitOffset

	// Store the final checksum value
	db.walInfo.lastCommitChecksum = lastCommitChecksum
	db.walInfo.checksum = lastCommitChecksum

	// Update the transaction sequence number
	db.txnSequence = commitSequence
	db.walInfo.lastCommitSequence = commitSequence
	db.walInfo.lastCheckpointSequence = commitSequence

	// Transfer the cached pages to the global page cache
	for pageNumber, entry := range localCache {
		if entry.Data[0] == ContentTypeRadix {
			_, err := db.parseRadixPage(entry.Data, pageNumber)
			if err != nil {
				return fmt.Errorf("failed to parse radix page: %w", err)
			}
		} else if entry.Data[0] == ContentTypeLeaf {
			_, err := db.parseLeafPage(entry.Data, pageNumber)
			if err != nil {
				return fmt.Errorf("failed to parse leaf page: %w", err)
			}
		} else if pageNumber == 0 {
			_, err := db.parseHeaderPage(entry.Data, pageNumber)
			if err != nil {
				return fmt.Errorf("failed to parse header page: %w", err)
			}
		} else {
			return fmt.Errorf("unknown page type: %d", entry.Data[0])
		}
	}

	if !db.readOnly {
		// Reset the WAL file as all content was checkpointed
		return db.resetWAL()
	}

	// Update the index file size to account for WAL pages
	return db.RefreshFileSize()
}

// walCommit writes a commit record to the WAL file
func (db *DB) walCommit() error {
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
		// Sync the WAL file
		if err := db.walInfo.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file after commit: %w", err)
		}
	}

	// Clean up old page versions from cache after successful commit
	db.discardOldPageVersions(true)

	// Update sequence number after successful commit
	db.walInfo.lastCommitSequence = db.txnSequence

	// Check if it should run a checkpoint
	if db.shouldCheckpoint() {
		// Checkpoint the WAL file into the index file
		if err := db.checkpointWAL(); err != nil {
			// Log error but don't fail the commit
			debugPrint("Checkpoint failed: %v", err)
		}
	}

	return nil
}

// walRollback rolls back the current transaction
func (db *DB) walRollback() error {
	if db.walInfo == nil {
		return nil
	}

	// Reset the write position to the last commit position
	db.walInfo.nextWritePosition = db.walInfo.lastCommitPosition

	// Restore checksum to the value from the last commit
	db.walInfo.checksum = db.walInfo.lastCommitChecksum

	// Get current sequence number before rollback
	currentSeq := db.txnSequence

	// Remove pages that are from the current transaction from the cache
	db.discardNewerPages(currentSeq)

	// Keep the same sequence number for the next transaction attempt
	// This is important to maintain consistency

	return nil
}

// shouldCheckpoint checks if the WAL file should be checkpointed
func (db *DB) shouldCheckpoint() bool {
	// Check WAL file size
	walFileInfo, err := db.walInfo.file.Stat()
	if err == nil {
		// Checkpoint if WAL file exceeds size threshold
		if walFileInfo.Size() > db.checkpointThreshold {
			return true
		}
	}
	return false
}

// checkpointWAL writes the current WAL file to the index file and clears the cache
func (db *DB) checkpointWAL() error {
	if db.walInfo == nil {
		return nil
	}

	// Lock the cache during checkpoint
	//db.walInfo.cacheMutex.Lock()

	// If not already synced on walCommit
	if db.syncMode == SyncOff {
		// Sync the WAL file to ensure all changes are persisted
		if err := db.walInfo.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL file before checkpoint: %w", err)
		}
	}

	// Get the start sequence number for the checkpoint
	//startSequence := db.walInfo.lastCheckpointSequence

	// Copy WAL pages to the index file
	if err := db.copyWALPagesToIndexFile(); err != nil {
		return fmt.Errorf("failed to copy pages to index file: %w", err)
	}

	// Sync the index file to ensure all changes are persisted
	if err := db.indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file after checkpoint: %w", err)
	}

	// Update the last checkpoint sequence number
	db.walInfo.lastCheckpointSequence = db.walInfo.lastCommitSequence + 1

	// Clear the page cache
	// Clear the isWAL flag for all pages and remove older versions
	db.discardOldPageVersions(false)

	// Reset the WAL file
	return db.resetWAL()
}

// copyPagesToIndexFile copies pages from the WAL cache to the index file
// only pages with the specified commit sequence number or higher are copied
func (db *DB) copyPagesToIndexFile(localCache localCache, commitSequence int64) error {
	if db.walInfo == nil || localCache == nil {
		return nil
	}

	// Get the list of page numbers to copy
	pageNumbers := make([]uint32, 0, len(localCache))
	for pageNumber, entry := range localCache {
		if entry.SequenceNumber >= commitSequence {
			pageNumbers = append(pageNumbers, pageNumber)
		}
	}

	// Sort page numbers for sequential access (faster writes)
	sort.Slice(pageNumbers, func(i, j int) bool {
		return pageNumbers[i] < pageNumbers[j]
	})

	// Copy pages to the index file in sorted order
	for _, pageNumber := range pageNumbers {
		entry := localCache[pageNumber]
		if entry.SequenceNumber != commitSequence {
			continue // Skip if sequence number doesn't match
		}

		// Calculate the offset in the index file
		offset := int64(pageNumber) * PageSize

		// Write the page data to the index file
		if _, err := db.indexFile.WriteAt(entry.Data, offset); err != nil {
			return fmt.Errorf("failed to write page %d to index file: %w", pageNumber, err)
		}

		// Ensure the index file size is updated if necessary
		requiredSize := offset + PageSize
		if requiredSize > db.indexFileSize {
			db.indexFileSize = requiredSize
		}
	}

	return nil
}

// copyWALPagesToIndexFile copies pages from the WAL cache to the index file
func (db *DB) copyWALPagesToIndexFile() error {
	if db.walInfo == nil {
		return nil
	}

	// Get the list of page numbers to copy using just a read lock
	db.cacheMutex.RLock()
	pageNumbers := make([]uint32, 0, len(db.pageCache))
	for pageNumber := range db.pageCache {
		pageNumbers = append(pageNumbers, pageNumber)
	}
	db.cacheMutex.RUnlock()

	// Sort page numbers for sequential access (faster writes)
	sort.Slice(pageNumbers, func(i, j int) bool {
		return pageNumbers[i] < pageNumbers[j]
	})

	// Copy pages to the index file in sorted order
	for _, pageNumber := range pageNumbers {
		// Get the head of the linked list for this page number
		db.cacheMutex.RLock()
		headPage := db.pageCache[pageNumber]
		db.cacheMutex.RUnlock()

		// Find the first WAL page in the linked list
		var walPage *Page = nil
		for page := headPage; page != nil; page = page.next {
			if page.isWAL {
				walPage = page
				break
			}
		}

		// Skip if no WAL page was found
		if walPage == nil {
			continue
		}

		// Calculate the offset in the index file
		offset := int64(pageNumber) * PageSize

		// Write the page data to the index file
		if _, err := db.indexFile.WriteAt(walPage.data, offset); err != nil {
			return fmt.Errorf("failed to write page %d to index file: %w", pageNumber, err)
		}
	}

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
	}

	// Scan the WAL file to load any existing frames
	if err := db.scanWAL(); err != nil {
		// If there's an error scanning the WAL (which could be due to a database ID mismatch),
		// the scanWAL function will handle it by deleting and recreating the WAL file
		return err
	}

	return nil
}

func (db *DB) closeWAL() error {
	if db.walInfo == nil {
		return nil
	}

	// Close the WAL file
	if err := db.walInfo.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Reset the WAL info
	db.walInfo = nil

	return nil
}

// resetWAL resets the WAL file
func (db *DB) resetWAL() error {
	if db.walInfo == nil {
		return nil
	}

	// Truncate the WAL file
	if err := db.walInfo.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}

	// Close the WAL file
	if err := db.walInfo.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Create a new WAL file
	return db.createWAL()
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
