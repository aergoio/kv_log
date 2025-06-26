package kv_log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aergoio/kv_log/varint"
)

const (
	// Debug mode (set to false to disable debug prints)
	DebugMode = false
	// Page size (4KB)
	PageSize = 4096
	// Magic strings for database identification (6 bytes)
	MainFileMagicString  string = "KV_LOG"
	IndexFileMagicString string = "KV_IDX"
	// Database version (2 bytes as a string)
	VersionString string = "\x00\x01"
	// Maximum key length
	MaxKeyLength = 2048
	// Maximum value length
	MaxValueLength = 2 << 26 // 128MB
	// Alignment for non-page content
	ContentAlignment = 8

	// Radix page header size
	RadixHeaderSize = 8
	// Leaf page header size
	LeafHeaderSize = 8
	// Number of sub-pages per radix page
	SubPagesPerRadixPage = 3
	// Size of each radix entry (page number + sub-page index)
	RadixEntrySize = 5 // 4 bytes page number + 1 byte sub-page index
	// Number of entries in each sub-page
	EntriesPerSubPage = 256
	// Size of each sub-page including the empty suffix offset
	SubPageSize = EntriesPerSubPage * RadixEntrySize + 8 // 256 entries * 5 bytes + 8 bytes for empty suffix offset
)

// Content types
const (
	ContentTypeData  = 'D' // Data content type
	ContentTypeRadix = 'R' // Radix page type
	ContentTypeLeaf  = 'L' // Leaf page type
)

// Lock types
const (
	LockNone    = 0 // No locking
	LockShared  = 1 // Shared lock (read-only)
	LockExclusive = 2 // Exclusive lock (read-write)
)

// Write modes
const (
	CallerThread_WAL_Sync      = "CallerThread_WAL_Sync"     // write to WAL on the caller thread, checkpoint on the background thread
	CallerThread_WAL_NoSync    = "CallerThread_WAL_NoSync"   // write to WAL on the caller thread, checkpoint on the background thread
	WorkerThread_WAL           = "WorkerThread_WAL"          // write to WAL and checkpoint on the background thread
	WorkerThread_NoWAL         = "WorkerThread_NoWAL"        // write directly to file on the background thread
	WorkerThread_NoWAL_NoSync  = "WorkerThread_NoWAL_NoSync" // write directly to file on the background thread
)

// Commit modes
const (
	CallerThread = 1 // Commit on the caller thread
	WorkerThread = 0 // Commit on a background worker thread
)

// Sync modes
const (
	SyncOn  = 1 // Sync after writes
	SyncOff = 0 // Don't sync after writes
)

// debugPrint prints a message if debug mode is enabled
func debugPrint(format string, args ...interface{}) {
	if DebugMode {
		fmt.Printf(format, args...)
	}
}

// DB represents the database instance
type DB struct {
	databaseID     uint64 // Unique identifier for the database
	filePath       string
	mainFile       *os.File
	indexFile      *os.File
	mutex          sync.RWMutex
	cacheMutex     sync.RWMutex   // Separate mutex for the page cache
	mainFileSize   int64 // Track main file size to avoid frequent stat calls
	indexFileSize  int64 // Track index file size to avoid frequent stat calls
	fileLocked     bool  // Track if the files are locked
	lockType       int   // Type of lock currently held
	readOnly       bool  // Track if the database is opened in read-only mode
	pageCache      map[uint32]*Page // Cache for all page types
	freeSubPagesHead *RadixPage // Head of linked list of radix pages with available sub-pages
	lastIndexedOffset int64 // Track the offset of the last indexed content in the main file
	headerDirty    bool  // Track if the header needs to be written during sync
	writeMode      string // Current write mode
	nextWriteMode  string // Next write mode to apply
	commitMode     int    // CallerThread or WorkerThread
	useWAL         bool   // Whether to use WAL or not
	syncMode       int    // SyncOn or SyncOff
	walInfo        *WalInfo // WAL file information
	inTransaction  bool   // Track if a transaction is open
	calledByTransaction bool // Track if the method was called by a transaction
	txnSequence    int64  // Current transaction sequence number
	flushSequence  int64  // Current flush up to this transaction sequence number
	accessCounter  uint64 // Counter for page access times
	dirtyPageCount int    // Count of dirty pages in cache
	cacheSizeThreshold int // Maximum number of pages in cache before cleanup
	dirtyPageThreshold int // Maximum number of dirty pages before flush
}

// Transaction represents a database transaction
type Transaction struct {
	db *DB
}

// Content represents a piece of content in the database
type Content struct {
	offset      int64 // File offset where this content is stored
	data        []byte
	key         []byte // Parsed key for ContentTypeData
	value       []byte // Parsed value for ContentTypeData
}

// Page is a unified struct containing fields for both RadixPage and LeafPage
type Page struct {
	pageNumber   uint32
	pageType     byte
	data         []byte
	dirty        bool   // Whether this page contains unsaved changes
	isWAL        bool   // Whether this page is part of the WAL
	accessTime   uint64 // Last time this page was accessed
	txnSequence  int64  // Transaction sequence number
	next         *Page  // Pointer to the next entry with the same page number
	// Fields for RadixPage
	SubPagesUsed uint8  // Number of sub-pages used
	NextFreePage uint32 // Pointer to the next radix page with free sub-pages (0 if none)
	// Fields for LeafPage
	ContentSize  uint16      // Total size of content on this page
	Entries      []LeafEntry // Parsed entries
}

// RadixPage is an alias for Page
type RadixPage = Page

// LeafPage is an alias for Page
type LeafPage = Page

// RadixSubPage represents a specific sub-page within a radix page
type RadixSubPage struct {
	Page      *RadixPage // Pointer to the parent radix page
	SubPageIdx uint8     // Index of the sub-page within the parent page
}

// LeafEntry represents an entry in a leaf page
type LeafEntry struct {
	Suffix     []byte
	DataOffset int64
}

// Options represents configuration options for the database
type Options map[string]interface{}

// Open opens or creates a database file with the given options
func Open(path string, options ...Options) (*DB, error) {

	mainFileExists := false
	if _, err := os.Stat(path); err == nil {
		mainFileExists = true
	}

	// Generate index file path by adding '-index' suffix
	indexPath := path + "-index"
	indexFileExists := false
	if _, err := os.Stat(indexPath); err == nil {
		indexFileExists = true
	}

	if !mainFileExists && indexFileExists {
		// Remove index file if main file doesn't exist
		os.Remove(indexPath)
		indexFileExists = false
	}
	if !indexFileExists {
		// Remove WAL file if index file doesn't exist
		os.Remove(path + "-wal")
	}

	// Default options
	lockType := LockExclusive // Default to use an exclusive lock
	readOnly := false
	writeMode := WorkerThread_WAL // Default to use WAL in a background thread

	// Parse options
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	if opts != nil {
		/*
		if val, ok := opts["LockType"]; ok {
			if lt, ok := val.(int); ok {
				lockType = lt
			}
		}
		*/
		if val, ok := opts["ReadOnly"]; ok {
			if ro, ok := val.(bool); ok {
				readOnly = ro
			}
		}
		if val, ok := opts["WriteMode"]; ok {
			if jm, ok := val.(string); ok {
				if jm == CallerThread_WAL_Sync || jm == CallerThread_WAL_NoSync || jm == WorkerThread_WAL || jm == WorkerThread_NoWAL || jm == WorkerThread_NoWAL_NoSync {
					writeMode = jm
				} else {
					return nil, fmt.Errorf("invalid value for WriteMode option")
				}
			}
		}
	}

	// Open main file with appropriate flags
	var mainFile *os.File
	var err error
	if readOnly {
		mainFile, err = os.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open main database file in read-only mode: %w", err)
		}
	} else {
		mainFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open main database file: %w", err)
		}
	}

	// Open index file with appropriate flags
	var indexFile *os.File
	if readOnly {
		indexFile, err = os.OpenFile(indexPath, os.O_RDONLY, 0666)
		if err != nil {
			mainFile.Close()
			return nil, fmt.Errorf("failed to open index database file in read-only mode: %w", err)
		}
	} else {
		indexFile, err = os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			mainFile.Close()
			return nil, fmt.Errorf("failed to open index database file: %w", err)
		}
	}

	// Get initial file sizes
	mainFileInfo, err := mainFile.Stat()
	if err != nil {
		mainFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to get main file size: %w", err)
	}

	indexFileInfo, err := indexFile.Stat()
	if err != nil {
		mainFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to get index file size: %w", err)
	}

	db := &DB{
		databaseID:     0, // Will be set on read or initialize
		filePath:       path,
		mainFile:       mainFile,
		indexFile:      indexFile,
		mainFileSize:   mainFileInfo.Size(),
		indexFileSize:  indexFileInfo.Size(),
		readOnly:       readOnly,
		lockType:       LockNone,
		pageCache:      make(map[uint32]*Page),
	}

	// Ensure indexFileSize is properly aligned to page boundaries for existing files
	if indexFileExists && indexFileInfo.Size() > 0 {
		// Round up to the nearest page boundary to ensure correct page allocation
		actualPages := (indexFileInfo.Size() + PageSize - 1) / PageSize
		db.indexFileSize = actualPages * PageSize
	}

	// Initialize internal write mode fields
	db.updateWriteMode(writeMode)
	db.nextWriteMode = writeMode

	// Apply file lock if requested
	if lockType != LockNone {
		if err := db.Lock(lockType); err != nil {
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to lock database files: %w", err)
		}
	}

	// Check if we need to initialize the database
	needsInitialization := !mainFileExists && !readOnly

	// Check if we need to rebuild the index
	needsIndexInitialization := mainFileExists && (!indexFileExists || indexFileInfo.Size() == 0) && !readOnly

	if needsInitialization {
		// Initialize new database
		if err := db.initialize(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to initialize database: %w", err)
		}
	} else if needsIndexInitialization {
		// Main file exists but index file is missing or empty
		// Read the main file header first to get the database ID
		if err := db.readMainFileHeader(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to read main file header: %w", err)
		}
		// Initialize a new index file
		debugPrint("Index file missing or empty, initializing new index\n")
		if err := db.initializeIndexFile(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to initialize index file: %w", err)
		}
		debugPrint("Index file rebuilt successfully\n")
	} else {
		// Read existing database headers
		if err := db.readHeader(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to read database header: %w", err)
		}
	}

	// If the index file is not up-to-date, reindex the remaining content
	if db.lastIndexedOffset < db.mainFileSize {
		if err := db.recoverUnindexedContent(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to reindex database: %w", err)
		}
	}

	return db, nil
}

// SetOption sets a database option after the database is open
func (db *DB) SetOption(name string, value interface{}) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	switch name {
	case "WriteMode":
		if jm, ok := value.(string); ok {
			if jm == CallerThread_WAL_Sync || jm == CallerThread_WAL_NoSync || jm == WorkerThread_WAL || jm == WorkerThread_NoWAL || jm == WorkerThread_NoWAL_NoSync {
				db.nextWriteMode = jm
				return nil
			}
			return fmt.Errorf("invalid value for WriteMode option")
		}
		return fmt.Errorf("WriteMode option value must be a string")
	default:
		return fmt.Errorf("unknown or immutable option: %s", name)
	}
}

// updateWriteMode updates the internal write mode fields based on the writeMode string
func (db *DB) updateWriteMode(writeMode string) {
	// Update the write mode
	db.writeMode = writeMode

	// Update the internal fields based on the write mode
	switch db.writeMode {
	case CallerThread_WAL_Sync:
		db.commitMode = CallerThread
		db.useWAL = true
		db.syncMode = SyncOn

	case CallerThread_WAL_NoSync:
		db.commitMode = CallerThread
		db.useWAL = true
		db.syncMode = SyncOff

	case WorkerThread_WAL:
		db.commitMode = WorkerThread
		db.useWAL = true
		db.syncMode = SyncOff

	case WorkerThread_NoWAL:
		db.commitMode = WorkerThread
		db.useWAL = false
		db.syncMode = SyncOn

	case WorkerThread_NoWAL_NoSync:
		db.commitMode = WorkerThread
		db.useWAL = false
		db.syncMode = SyncOff
	}
}

// Lock acquires a lock on the database file based on the specified lock type
func (db *DB) Lock(lockType int) error {
	var lockFlag int

	if db.fileLocked && db.lockType == lockType {
		return nil // Already locked with the same lock type
	}

	// If already locked with a different lock type, unlock first
	if db.fileLocked {
		if err := db.Unlock(); err != nil {
			return err
		}
	}

	switch lockType {
	case LockShared:
		lockFlag = syscall.LOCK_SH | syscall.LOCK_NB
		debugPrint("Acquiring shared lock on database file\n")
	case LockExclusive:
		lockFlag = syscall.LOCK_EX | syscall.LOCK_NB
		debugPrint("Acquiring exclusive lock on database file\n")
	default:
		return fmt.Errorf("invalid lock type: %d", lockType)
	}

	err := syscall.Flock(int(db.mainFile.Fd()), lockFlag)
	if err != nil {
		if lockType == LockShared {
			return fmt.Errorf("cannot acquire shared lock (another process may have an exclusive lock): %w", err)
		}
		return fmt.Errorf("cannot acquire exclusive lock (file may be in use): %w", err)
	}

	db.fileLocked = true
	db.lockType = lockType
	return nil
}

// Unlock releases the lock on the database file
func (db *DB) Unlock() error {
	if !db.fileLocked {
		return nil // Not locked
	}

	err := syscall.Flock(int(db.mainFile.Fd()), syscall.LOCK_UN)
	if err != nil {
		return fmt.Errorf("cannot release lock: %w", err)
	}

	db.fileLocked = false
	db.lockType = LockNone
	debugPrint("Database file unlocked\n")
	return nil
}

// acquireWriteLock temporarily acquires an exclusive lock for writing
func (db *DB) acquireWriteLock() error {
	// If we already have an exclusive lock, nothing to do
	if db.fileLocked && db.lockType == LockExclusive {
		return nil
	}

	// Acquire an exclusive lock
	return db.Lock(LockExclusive)
}

// releaseWriteLock releases a temporary write lock
// If the DB was originally opened with a different lock type, restore it
func (db *DB) releaseWriteLock(originalLockType int) error {
	// If the original lock type was none, just unlock
	if originalLockType == LockNone {
		return db.Unlock()
	}

	// Otherwise restore the original lock type
	return db.Lock(originalLockType)
}

// Close closes the database files
func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	var mainErr, indexErr, flushErr error

	if !db.readOnly {
		// Flush the index to disk
		flushErr = db.flushIndexToDisk()
	}

	// Close main file if open
	if db.mainFile != nil {
		// Release lock if acquired
		if db.fileLocked {
			if err := db.Unlock(); err != nil {
				mainErr = fmt.Errorf("failed to unlock database files: %w", err)
			}
		}
		// Close the file
		mainErr = db.mainFile.Close()
	}

	// Close index file if open
	if db.indexFile != nil {
		indexErr = db.indexFile.Close()
	}

	// Return first error encountered
	if flushErr != nil {
		return flushErr
	}
	if mainErr != nil {
		return mainErr
	}
	return indexErr
}

// Delete removes a key from the database
func (db *DB) Delete(key []byte) error {
	// Call Set with nil value to mark as deleted
	return db.Set(key, nil)
}

// Set sets a key-value pair in the database
func (db *DB) Set(key, value []byte) error {
	// Check if a transaction is open but this method wasn't called by the transaction object
	if db.inTransaction && !db.calledByTransaction {
		return fmt.Errorf("a transaction is open, use the transaction object instead")
	}
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot write: database opened in read-only mode")
	}

	// Validate key length
	keyLen := len(key)
	if keyLen == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	if keyLen > MaxKeyLength {
		return fmt.Errorf("key length exceeds maximum allowed size of %d bytes", MaxKeyLength)
	}

	// Lock the database
	db.mutex.Lock()

	// Start a transaction if not already in one
	if !db.inTransaction {
		db.beginTransaction()
	}

	// Set the key-value pair
	err := db.set(key, value)

	// Commit or rollback the transaction if not in an explicit transaction
	if !db.inTransaction {
		if err == nil {
			db.commitTransaction()
		} else {
			db.rollbackTransaction()
		}
	}

	// Unlock the database
	db.mutex.Unlock()

	// Check the page cache
	db.checkPageCache(true)

	// Return the error
	return err
}

// Internal function to set a key-value pair in the database
func (db *DB) set(key, value []byte) error {

	// If not already exclusively locked
	if db.lockType != LockExclusive {
		// Remember the original lock type
		originalLockType := db.lockType
		// Acquire a write lock
		if err := db.acquireWriteLock(); err != nil {
			return fmt.Errorf("failed to acquire write lock: %w", err)
		}
		// Release the lock on exit
		defer db.releaseWriteLock(originalLockType)
	}

	// Start with the root radix sub-page
	rootSubPage, err := db.getRootRadixSubPage()
	if err != nil {
		return fmt.Errorf("failed to get root radix sub-page: %w", err)
	}

	// Check if we're deleting (value is nil)
	isDelete := len(value) == 0

	// Process the key byte by byte
	currentSubPage := rootSubPage
	keyPos := 0

	// Traverse the radix trie until we reach a leaf page or the end of the key
	for keyPos < len(key) {
		// Get the current byte from the key
		byteValue := key[keyPos]

		// Get the next page number and sub-page index from the current sub-page
		nextPageNumber, nextSubPageIdx := db.getRadixEntry(currentSubPage, byteValue)

		// If there's no entry for this byte, create a new path
		if nextPageNumber == 0 {
			// If we're deleting, nothing to do
			if isDelete {
				return nil
			}

			// Append the data to the main file
			dataOffset, err := db.appendData(key, value)
			if err != nil {
				return fmt.Errorf("failed to append data: %w", err)
			}

			// Create a path for this byte
			return db.createPathForByte(currentSubPage, key, keyPos, dataOffset)
		}

		// There's an entry for this byte, load the page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %w", nextPageNumber, err)
		}

		// Check what type of page we got
		if page.pageType == ContentTypeRadix {
			// It's a radix page, continue traversing
			currentSubPage = &RadixSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			keyPos++
		} else if page.pageType == ContentTypeLeaf {
			// It's a leaf page, attempt to set the key and value using the leaf page
			return db.setOnLeafPage(page, key, keyPos, value, 0)
		} else {
			return fmt.Errorf("invalid page type")
		}
	}

	// We've processed all bytes of the key
	// Attempt to set the key and value on the empty suffix slot
	return db.setOnEmptySuffix(currentSubPage, key, value, 0)
}

// setKvOnIndex sets an existing key-value pair on the index (reindexing)
func (db *DB) setKvOnIndex(rootSubPage *RadixSubPage, key, value []byte, dataOffset int64) error {

	// Process the key byte by byte to find where to add it
	currentSubPage := rootSubPage
	keyPos := 0

	// Traverse the radix trie until we reach a leaf page or the end of the key
	for keyPos < len(key) {
		// Get the current byte from the key
		byteValue := key[keyPos]

		// Get the next page number and sub-page index from the current sub-page
		nextPageNumber, nextSubPageIdx := db.getRadixEntry(currentSubPage, byteValue)

		// If there's no entry for this byte, create a new path
		if nextPageNumber == 0 {
			// Create a path for this byte
			return db.createPathForByte(currentSubPage, key, keyPos, dataOffset)
		}

		// There's an entry for this byte, load the page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %w", nextPageNumber, err)
		}

		// Check what type of page we got
		if page.pageType == ContentTypeRadix {
			// It's a radix page, continue traversing
			currentSubPage = &RadixSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			keyPos++
		} else if page.pageType == ContentTypeLeaf {
			// It's a leaf page, attempt to set the key and value using the leaf page
			return db.setOnLeafPage(page, key, keyPos, value, dataOffset)
		} else {
			return fmt.Errorf("invalid page type")
		}
	}

	// If we've processed all bytes of the key
	// Set the content offset on the empty suffix slot
	return db.setOnEmptySuffix(currentSubPage, key, value, dataOffset)
}

// setContentOnIndex sets a suffix + content offset pair on the index
// it is used when converting a leaf page into a radix page
func (db *DB) setContentOnIndex(subPage *RadixSubPage, suffix []byte, suffixPos int, contentOffset int64) error {

	// Process the suffix byte by byte
	currentSubPage := subPage

	// Traverse the radix trie until we reach a leaf page or the end of the suffix
	for suffixPos < len(suffix) {
		// Get the current byte from the key's suffix
		byteValue := suffix[suffixPos]

		// Get the next page number and sub-page index from the current sub-page
		nextPageNumber, nextSubPageIdx := db.getRadixEntry(currentSubPage, byteValue)

		// If there's no entry for this byte, create a new path
		if nextPageNumber == 0 {
			// Create a path for this byte
			return db.createPathForByte(currentSubPage, suffix, suffixPos, contentOffset)
		}

		// There's an entry for this byte, load the page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %w", nextPageNumber, err)
		}

		// Check what type of page we got
		if page.pageType == ContentTypeRadix {
			// It's a radix page, continue traversing
			currentSubPage = &RadixSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			suffixPos++
		} else if page.pageType == ContentTypeLeaf {
			// It's a leaf page
			suffix = suffix[suffixPos+1:]
			// Try to add the entry with the suffix to the leaf page
			// If the leaf page is full, convert it to a radix page
			return db.addLeafEntry(page, suffix, contentOffset)
		} else {
			return fmt.Errorf("invalid page type")
		}
	}

	// We've processed all bytes of the key
	// Set the content offset on the empty suffix slot
	return db.setEmptySuffixOffset(currentSubPage, contentOffset)
}

// createPathForByte creates a new path for a byte in the key
func (db *DB) createPathForByte(subPage *RadixSubPage, key []byte, keyPos int, dataOffset int64) error {

	// Get the current byte from the key
	byteValue := key[keyPos]

	// The remaining part of the key is the suffix
	suffix := key[keyPos+1:]

	// Handle based on whether we have an empty suffix or not
	if len(suffix) == 0 {
		// For empty suffix, create a new radix sub-page and set the empty suffix offset
		childSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			return fmt.Errorf("failed to allocate radix sub-page: %w", err)
		}

		// Set the empty suffix offset
		err = db.setEmptySuffixOffset(childSubPage, dataOffset)
		if err != nil {
			return fmt.Errorf("failed to set empty suffix offset: %w", err)
		}

		// Update the radix entry to point to the new radix page
		err = db.setRadixEntry(subPage, byteValue, childSubPage.Page.pageNumber, childSubPage.SubPageIdx)
		if err != nil {
			return fmt.Errorf("failed to set radix entry for byte %d: %w", byteValue, err)
		}
	} else {
		// For non-empty suffix, create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			return fmt.Errorf("failed to allocate leaf page: %w", err)
		}

		// Add the entry with the suffix to the leaf page
		err = db.addLeafEntry(leafPage, suffix, dataOffset)
		if err != nil {
			return fmt.Errorf("failed to add leaf entry: %w", err)
		}

		// Update the radix entry to point to the new leaf page
		err = db.setRadixEntry(subPage, byteValue, leafPage.pageNumber, 0)
		if err != nil {
			return fmt.Errorf("failed to set radix entry for byte %d: %w", byteValue, err)
		}
	}

	// Don't write to disk, just keep pages in cache
	return nil
}

// setOnLeafPage attempts to set a key and value on a leaf page
// If dataOffset is 0, we're setting a new key-value pair
// Otherwise, it means we're reindexing already stored key-value pair
func (db *DB) setOnLeafPage(leafPage *LeafPage, key []byte, keyPos int, value []byte, dataOffset int64) error {
	var err error

	// Check if we're deleting
	isDelete := len(value) == 0

	// The remaining part of the key is the suffix
	suffix := key[keyPos+1:]

	// Search for the suffix in the leaf page entries
	for i, entry := range leafPage.Entries {
		if bytes.Equal(entry.Suffix, suffix) {
			// Found the entry
			var content *Content

			// If we're setting a new key-value pair
			if dataOffset == 0 {
				// Read the content from the main file
				content, err = db.readContent(entry.DataOffset)
				if err != nil {
					return fmt.Errorf("failed to read content: %w", err)
				}

				// Verify that the key matches
				if !equal(content.key, key) {
					return fmt.Errorf("invalid indexed key")
				}
			}

			// If we're deleting
			if isDelete {
				// If there is an existing value
				if dataOffset == 0 && content != nil && len(content.value) > 0 {
					// Log the deletion to the main file
					dataOffset, err = db.appendData(key, nil)
					if err != nil {
						return fmt.Errorf("failed to append deletion: %w", err)
					}
				}
				// Remove this entry
				if ok, err := db.removeLeafEntryAt(leafPage, i); err != nil {
					return fmt.Errorf("failed to remove leaf entry: %w", err)
				} else if !ok {
					return fmt.Errorf("failed to remove leaf entry")
				}
				return nil
			}

			// If we're setting a new key-value pair
			if dataOffset == 0 {
				// Check if value is the same
				if equal(content.value, value) {
					// Value is the same, nothing to do
					return nil
				}

				// Value is different, append new data
				dataOffset, err = db.appendData(key, value)
				if err != nil {
					return fmt.Errorf("failed to append data: %w", err)
				}
			}

			// Update the entry on the leaf page
			err = db.updateLeafEntryOffset(leafPage, i, dataOffset)
			if err != nil {
				return fmt.Errorf("failed to update leaf entry offset: %w", err)
			}

			// Mark the leaf page as dirty
			leafPage.dirty = true

			return nil
		}
	}

	// If we're deleting and didn't find the key, nothing to do
	if isDelete {
		return nil
	}

	// If we're setting a new key-value pair
	if dataOffset == 0 {
		// Suffix not found, append new data
		dataOffset, err = db.appendData(key, value)
		if err != nil {
			return fmt.Errorf("failed to append data: %w", err)
		}
	}

	// Try to add the entry with the suffix to the leaf page
	// If the leaf page is full, it converts it to a radix page
	return db.addLeafEntry(leafPage, suffix, dataOffset)
}

// setOnEmptySuffix attempts to set a key and value on an empty suffix in a radix sub-page
// If dataOffset is 0, we're setting a new key-value pair
// Otherwise, it means we're reindexing already stored key-value pair
func (db *DB) setOnEmptySuffix(subPage *RadixSubPage, key, value []byte, dataOffset int64) error {
	var err error

	// Check if we're deleting
	isDelete := len(value) == 0

	// Get the current empty suffix offset
	emptySuffixOffset := db.getEmptySuffixOffset(subPage)

	// If there's no empty suffix offset, nothing to delete
	if emptySuffixOffset == 0 && isDelete {
		return nil
	}

	// If we have an empty suffix offset, read the content to verify the key
	if emptySuffixOffset > 0 {
		var content *Content

		// If we're setting a new key-value pair
		if dataOffset == 0 {
			// Read the content at the offset
			content, err = db.readContent(emptySuffixOffset)
			if err != nil {
				return fmt.Errorf("failed to read content: %w", err)
			}

			// Verify that the key matches
			if !equal(content.key, key) {
				return fmt.Errorf("invalid indexed key")
			}
		}

		// If we're deleting
		if isDelete {
			// If there is an existing value
			if dataOffset == 0 && content != nil && len(content.value) > 0 {
				// Log the deletion to the main file
				dataOffset, err = db.appendData(key, nil)
				if err != nil {
					return fmt.Errorf("failed to append deletion: %w", err)
				}
			}
			// Clear the empty suffix offset
			return db.setEmptySuffixOffset(subPage, 0)
		}

		// If we're setting a new key-value pair
		if dataOffset == 0 {
			// Check if value is the same
			if equal(content.value, value) {
				// Value is the same, nothing to do
				return nil
			}
		}

		// Value is different, need to update it
		// We'll fall through to append the new data
	}

	// If we're setting a new key-value pair
	if dataOffset == 0 {
		// No empty suffix or key mismatch, append new data
		dataOffset, err = db.appendData(key, value)
		if err != nil {
			return fmt.Errorf("failed to append data: %w", err)
		}
	}

	// Set the empty suffix offset
	return db.setEmptySuffixOffset(subPage, dataOffset)
}

// Get retrieves a value for the given key
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mutex.RLock()
	defer func() {
		db.mutex.RUnlock()
		db.checkPageCache(false)
	}()

	// Validate key length
	keyLen := len(key)
	if keyLen == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}
	if keyLen > MaxKeyLength {
		return nil, fmt.Errorf("key length exceeds maximum allowed size of %d bytes", MaxKeyLength)
	}

	// Start with the root radix sub-page
	rootSubPage, err := db.getRootRadixSubPage()
	if err != nil {
		return nil, fmt.Errorf("failed to get root radix sub-page: %w", err)
	}

	// Process the key byte by byte
	var currentSubPage = rootSubPage
	var keyPos int

	// Traverse the radix trie until we reach a leaf page
	for keyPos < len(key) {
		// Get the current byte from the key
		byteValue := key[keyPos]

		// Get the next page number and sub-page index from the current sub-page
		nextPageNumber, nextSubPageIdx := db.getRadixEntry(currentSubPage, byteValue)

		// If there's no entry for this byte, the key doesn't exist
		if nextPageNumber == 0 {
			return nil, fmt.Errorf("key not found")
		}

		// Load the next page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to load page %d: %w", nextPageNumber, err)
		}

		// Check what type of page we got
		if page.pageType == ContentTypeRadix {
			// It's a radix page, continue traversing
			currentSubPage = &RadixSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			keyPos++
		} else if page.pageType == ContentTypeLeaf {
			// It's a leaf page, search for the suffix
			leafPage := page

			// The remaining part of the key is the suffix
			suffix := key[keyPos+1:]

			// Search for the suffix in the leaf page entries
			for _, entry := range leafPage.Entries {
				if bytes.Equal(entry.Suffix, suffix) {
					// Found the entry, read the content from the main file
					contentOffset := entry.DataOffset

					// Read the content at the offset
					content, err := db.readContent(contentOffset)
					if err != nil {
						return nil, fmt.Errorf("failed to read content: %w", err)
					}

					// Verify that the key matches
					if !equal(content.key, key) {
						return nil, fmt.Errorf("invalid indexed key")
					}

					// Return the value
					return content.value, nil
				}
			}

			// If we get here, the suffix wasn't found
			return nil, fmt.Errorf("key not found")
		} else {
			return nil, fmt.Errorf("invalid page type")
		}
	}

	// If we've processed all bytes but haven't found a leaf page,
	// check if there's an empty suffix in the current sub-page
	emptySuffixOffset := db.getEmptySuffixOffset(currentSubPage)
	if emptySuffixOffset > 0 {
		// Read the content at the offset
		content, err := db.readContent(emptySuffixOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to read content: %w", err)
		}

		// Verify that the key matches
		if !equal(content.key, key) {
			return nil, fmt.Errorf("invalid indexed key")
		}

		// Return the value
		return content.value, nil
	}

	return nil, fmt.Errorf("key not found")
}

// Helper functions

// initialize creates a new database file structure
func (db *DB) initialize() error {

	// If not already exclusively locked
	if db.lockType != LockExclusive {
		// Remember the original lock type
		originalLockType := db.lockType
		// Acquire a write lock
		if err := db.acquireWriteLock(); err != nil {
			return fmt.Errorf("failed to acquire write lock for initialization: %w", err)
		}
		// Release the lock on exit
		defer db.releaseWriteLock(originalLockType)
	}

	debugPrint("Initializing database\n")

	// Generate a random database ID
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	db.databaseID = r.Uint64()
	debugPrint("Generated new database ID: %d\n", db.databaseID)

	// Save the original journal mode and temporarily disable it during initialization
	originalWriteMode := db.writeMode
	db.updateWriteMode(WorkerThread_NoWAL_NoSync)

	// Initialize main file
	if err := db.initializeMainFile(); err != nil {
		return fmt.Errorf("failed to initialize main file: %w", err)
	}

	// Initialize index file
	if err := db.initializeIndexFile(); err != nil {
		return fmt.Errorf("failed to initialize index file: %w", err)
	}

	// Restore the original journal mode
	db.updateWriteMode(originalWriteMode)

	debugPrint("Database initialized\n")

	return nil
}

// initializeMainFile initializes the main data file
func (db *DB) initializeMainFile() error {
	// Write file header in root page (page 1)
	rootPage := make([]byte, PageSize)

	// Write the 6-byte magic string
	copy(rootPage[0:6], MainFileMagicString)

	// Write the 2-byte version
	copy(rootPage[6:8], VersionString)

	// Write the 8-byte database ID
	binary.LittleEndian.PutUint64(rootPage[8:16], db.databaseID)

	// The rest of the root page is reserved for future use

	debugPrint("Writing main file root page to disk\n")
	// Write the root page to the file
	if _, err := db.mainFile.Write(rootPage); err != nil {
		return err
	}

	// Update file size to include the root page
	db.mainFileSize = PageSize

	return nil
}

// initializeIndexFile initializes the index file
func (db *DB) initializeIndexFile() error {

	// Write the index file header
	debugPrint("Writing index file header\n")
	if err := db.writeIndexHeader(true); err != nil {
		return fmt.Errorf("failed to write index file header: %w", err)
	}

	// Allocate root radix page at page 1
	rootRadixPage, err := db.allocateRadixPage()
	if err != nil {
		return fmt.Errorf("failed to allocate root radix page: %w", err)
	}

	// Page number should be 1 (since we just allocated it after the header page)
	if rootRadixPage.pageNumber != 1 {
		return fmt.Errorf("unexpected root radix page number: %d", rootRadixPage.pageNumber)
	}

	// Cache this as the head of the free sub-pages list
	db.freeSubPagesHead = rootRadixPage

	// Initialize the first two levels of the radix tree
	if err := db.initializeRadixLevels(); err != nil {
		return fmt.Errorf("failed to initialize radix levels: %w", err)
	}

	// If using WAL mode, delete existing WAL file and open a new one
	if db.useWAL {
		// Delete existing WAL file
		if err := db.deleteWAL(); err != nil {
			return fmt.Errorf("failed to delete WAL file: %w", err)
		}
		// Open new WAL file
		if err := db.openWAL(); err != nil {
			return fmt.Errorf("failed to open WAL file: %w", err)
		}
	}

	// Flush the index to disk
	//if err := db.flushIndexToDisk(); err != nil {
	//	return fmt.Errorf("failed to flush index to disk: %w", err)
	//}

	return nil
}

// readHeader reads the database headers and preloads radix levels
func (db *DB) readHeader() error {
	// Read main file header
	if err := db.readMainFileHeader(); err != nil {
		return fmt.Errorf("failed to read main file header: %w", err)
	}

	// Check for existing WAL file if in WAL mode
	if db.useWAL {
		// Open existing WAL file if it exists
		if err := db.openWAL(); err != nil {
			return fmt.Errorf("failed to open WAL file: %w", err)
		}
	}

	// Read index file header
	if err := db.readIndexFileHeader(); err != nil {
		return fmt.Errorf("failed to read index file header: %w", err)
	}

	// Preload the first two levels of the radix tree
	if err := db.preloadRadixLevels(); err != nil {
		return fmt.Errorf("failed to preload radix levels: %w", err)
	}

	return nil
}

// readMainFileHeader reads the main file header
func (db *DB) readMainFileHeader() error {
	// Read the header (16 bytes) in root page (page 1)
	header := make([]byte, 16)
	if _, err := db.mainFile.ReadAt(header, 0); err != nil {
		return err
	}

	// Extract magic string (6 bytes)
	fileMagic := string(header[0:6])

	// Extract version (2 bytes)
	fileVersion := string(header[6:8])

	if fileMagic != MainFileMagicString {
		return fmt.Errorf("invalid main database file format")
	}

	if fileVersion != VersionString {
		return fmt.Errorf("unsupported main database version")
	}

	// Extract database ID (8 bytes)
	db.databaseID = binary.LittleEndian.Uint64(header[8:16])

	return nil
}

// readIndexFileHeader reads the index file header
func (db *DB) readIndexFileHeader() error {
	// Read the entire header page
	header, err := db.readIndexPage(0)
	if err != nil {
		return fmt.Errorf("failed to read index file header: %w", err)
	}

	// Extract magic string (6 bytes)
	fileMagic := string(header[0:6])

	// Extract version (2 bytes)
	fileVersion := string(header[6:8])

	if fileMagic != IndexFileMagicString {
		return fmt.Errorf("invalid index database file format")
	}

	if fileVersion != VersionString {
		return fmt.Errorf("unsupported index database version")
	}

	// Extract database ID (8 bytes)
	indexDatabaseID := binary.LittleEndian.Uint64(header[8:16])

	// Check if the database ID matches the main file
	if db.databaseID != 0 && indexDatabaseID != db.databaseID {
		// Database ID mismatch, delete the index file and recreate it
		debugPrint("Index file database ID mismatch: %d vs %d, recreating index file\n", indexDatabaseID, db.databaseID)

		// Close the index file
		if err := db.indexFile.Close(); err != nil {
			return fmt.Errorf("failed to close index file: %w", err)
		}

		// Delete the index file
		if err := os.Remove(db.filePath + "-index"); err != nil {
			return fmt.Errorf("failed to delete index file: %w", err)
		}

		// Reopen the index file
		db.indexFile, err = os.OpenFile(db.filePath + "-index", os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return fmt.Errorf("failed to reopen index file: %w", err)
		}

		// Initialize a new index file with the correct database ID
		return db.initializeIndexFile()
	}

	// Parse the last indexed offset from the header
	db.lastIndexedOffset = int64(binary.LittleEndian.Uint64(header[16:24]))
	if db.lastIndexedOffset == 0 {
		// If the last indexed offset is 0, default to PageSize
		db.lastIndexedOffset = PageSize
	}

	// Parse the free sub-pages head pointer
	freePageNum := binary.LittleEndian.Uint32(header[24:28])

	// If we have a valid free page pointer
	if freePageNum > 0 {
		radixPage, err := db.getRadixPage(freePageNum)
		if err != nil {
			return fmt.Errorf("failed to get radix page: %w", err)
		}
		db.freeSubPagesHead = radixPage
	}

	return nil
}

// writeIndexHeader writes metadata to the index file header
func (db *DB) writeIndexHeader(isInit bool) error {
	// Don't update if database is in read-only mode
	if db.readOnly {
		return nil
	}

	// The offset of the last indexed content in the main file
	lastIndexedOffset := int64(PageSize)
	if !isInit {
		lastIndexedOffset = db.mainFileSize
	}

	// The page number of the next free sub-page
	nextFreePageNumber := uint32(0)
	if !isInit && db.freeSubPagesHead != nil {
		nextFreePageNumber = db.freeSubPagesHead.pageNumber
	}

	// Allocate a buffer for the entire page
	data := make([]byte, PageSize)

	// Write the 6-byte magic string
	copy(data[0:6], IndexFileMagicString)

	// Write the 2-byte version
	copy(data[6:8], VersionString)

	// Write the 8-byte database ID
	binary.LittleEndian.PutUint64(data[8:16], db.databaseID)

	// Set last indexed offset (8 bytes)
	binary.LittleEndian.PutUint64(data[16:24], uint64(lastIndexedOffset))

	// Set free sub-pages head pointer (4 bytes)
	binary.LittleEndian.PutUint32(data[24:28], nextFreePageNumber)

	// If this is the first time we're writing the header, set the file size to PageSize
	if isInit {
		db.indexFileSize = PageSize
	}

	// Create a temporary Page struct for the root page
	headerPage := &Page{
		pageNumber: 0,
		data:       data,
	}

	// Write the entire root page to disk
	if err := db.writeIndexPage(headerPage); err != nil {
		return fmt.Errorf("failed to write index file root page: %w", err)
	}

	// Update the in-memory offset
	db.lastIndexedOffset = lastIndexedOffset

	// Header is no longer dirty
	db.headerDirty = false

	return nil
}

// ------------------------------------------------------------------------------------------------
// Main file
// ------------------------------------------------------------------------------------------------

// appendData appends a key-value pair to the end of the file and returns its offset
func (db *DB) appendData(key, value []byte) (int64, error) {
	// Use stored file size to determine where to append
	fileSize := db.mainFileSize

	// Calculate the total size needed
	keyLenSize := varint.Size(uint64(len(key)))
	valueLenSize := varint.Size(uint64(len(value)))
	totalSize := 1 + keyLenSize + len(key) + valueLenSize + len(value) // 1 byte for content type

	// Prepare the content buffer
	content := make([]byte, totalSize)
	offset := 0

	// Write content type
	content[offset] = ContentTypeData
	offset++

	// Write key length
	keyLenWritten := varint.Write(content[offset:], uint64(len(key)))
	offset += keyLenWritten

	// Write key
	copy(content[offset:], key)
	offset += len(key)

	// Write value length
	valueLenWritten := varint.Write(content[offset:], uint64(len(value)))
	offset += valueLenWritten

	// Write value
	copy(content[offset:], value)

	// Write the content to the end of the file
	if _, err := db.mainFile.Write(content); err != nil {
		return 0, fmt.Errorf("failed to write content: %w", err)
	}

	// Update the file size
	newFileSize := fileSize + int64(totalSize)
	db.mainFileSize = newFileSize

	debugPrint("Appended content at offset %d, size %d\n", fileSize, totalSize)

	// Return the offset where the content was written
	return fileSize, nil
}

// readContent reads content from a specific offset in the file
func (db *DB) readContent(offset int64) (*Content, error) {
	// Check if offset is valid
	if offset < 0 || offset >= db.mainFileSize {
		return nil, fmt.Errorf("offset out of file bounds: %d", offset)
	}

	// Read the content type first (1 byte)
	typeBuffer := make([]byte, 1)
	if _, err := db.mainFile.ReadAt(typeBuffer, offset); err != nil {
		return nil, fmt.Errorf("failed to read content type: %w", err)
	}

	contentType := typeBuffer[0]
	content := &Content{
		offset:      offset,
	}

	if contentType == ContentTypeData {
		// Read a small chunk to get the key length
		initialBuffer := make([]byte, 10) // Enough for type + varint key length in most cases
		_, err := db.mainFile.ReadAt(initialBuffer, offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read content header: %w", err)
		}

		// Parse key length
		keyLengthOffset := 1 // Skip content type byte
		keyLength64, bytesRead := varint.Read(initialBuffer[keyLengthOffset:])
		if bytesRead == 0 {
			return nil, fmt.Errorf("failed to parse key length")
		}
		keyLength := int(keyLength64)

		if keyLength > MaxKeyLength {
			return nil, fmt.Errorf("key length exceeds maximum allowed size: %d", keyLength)
		}

		// Read enough to get key + value length
		headerSize := 1 + bytesRead + keyLength + 10 // type + key length varint + key + estimated value length varint
		headerBuffer := make([]byte, headerSize)
		headerRead, err := db.mainFile.ReadAt(headerBuffer, offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read key data: %w", err)
		}

		// Make sure we got at least the key
		if headerRead < 1 + bytesRead + keyLength {
			return nil, fmt.Errorf("failed to read complete key data")
		}

		// Calculate key offset
		keyOffset := 1 + bytesRead

		// Parse value length
		valueLengthOffset := keyOffset + keyLength
		valueLength64, valueBytesRead := varint.Read(headerBuffer[valueLengthOffset:])
		if valueBytesRead == 0 {
			return nil, fmt.Errorf("failed to parse value length")
		}
		valueLength := int(valueLength64)

		if valueLength > MaxValueLength {
			return nil, fmt.Errorf("value length exceeds maximum allowed size: %d", valueLength)
		}

		// Calculate value offset
		valueOffset := valueLengthOffset + valueBytesRead

		// Calculate total size needed
		totalSize := valueOffset + valueLength

		// Check if total size exceeds file size
		if offset + int64(totalSize) > db.mainFileSize {
			return nil, fmt.Errorf("content extends beyond file size")
		}

		// Read all data at once
		buffer := make([]byte, totalSize)
		n, err := db.mainFile.ReadAt(buffer, offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read content: %w", err)
		}

		// Make sure we got all the data
		if n < totalSize {
			return nil, fmt.Errorf("failed to read complete content data")
		}

		// Store the full data buffer
		content.data = buffer

		// Set key and value as slices that reference the original buffer
		content.key = buffer[keyOffset:keyOffset+keyLength]
		content.value = buffer[valueOffset:valueOffset+valueLength]
	} else {
		return nil, fmt.Errorf("unknown content type on main file: %c", contentType)
	}

	return content, nil
}

// ------------------------------------------------------------------------------------------------
// Header page
// ------------------------------------------------------------------------------------------------

func (db *DB) parseHeaderPage(data []byte, pageNumber uint32) (*Page, error) {

	// Just store the data on the cache
	headerPage := &Page{
		pageNumber: pageNumber,
		data:       data,
	}

	// Add to cache
	db.addToCache(headerPage)

	// Update the access time
	db.accessCounter++   // TODO: not sure if this is correct for page 0
	headerPage.accessTime = db.accessCounter

	return headerPage, nil
}

// ------------------------------------------------------------------------------------------------
// Radix pages
// ------------------------------------------------------------------------------------------------

// parseRadixPage parses a radix page read from the disk
func (db *DB) parseRadixPage(data []byte, pageNumber uint32) (*RadixPage, error) {
	// Check if it's a radix page
	if data[0] != ContentTypeRadix {
		return nil, fmt.Errorf("not a radix page at page %d", pageNumber)
	}

	// Verify CRC32 checksum
	storedChecksum := binary.BigEndian.Uint32(data[4:8])
	// Zero out the checksum field for calculation
	binary.BigEndian.PutUint32(data[4:8], 0)
	// Calculate the checksum
	calculatedChecksum := crc32.ChecksumIEEE(data)
	// Restore the original checksum in the data
	binary.BigEndian.PutUint32(data[4:8], storedChecksum)
	// Verify the checksum
	if storedChecksum != calculatedChecksum {
		return nil, fmt.Errorf("radix page checksum mismatch at page %d: stored=%d, calculated=%d", pageNumber, storedChecksum, calculatedChecksum)
	}

	// Read the sub-pages used
	subPagesUsed := data[1]

	// Read the next free page number
	nextFreePage := binary.LittleEndian.Uint32(data[8:12])

	// Create structured radix page
	radixPage := &RadixPage{
		pageNumber:   pageNumber,
		pageType:     ContentTypeRadix,
		data:         data,
		dirty:        false,
		SubPagesUsed: subPagesUsed,
		NextFreePage: nextFreePage,
	}

	// Add to cache
	db.addToCache(radixPage)

	return radixPage, nil
}

// writeRadixPage writes a radix page to the database file
func (db *DB) writeRadixPage(radixPage *RadixPage) error {
	// Set page type in the data
	radixPage.data[0] = ContentTypeRadix  // Type identifier
	radixPage.data[1] = radixPage.SubPagesUsed // The number of sub-pages used

	// Store the NextFreePage field at bytes 8-11
	binary.LittleEndian.PutUint32(radixPage.data[8:12], radixPage.NextFreePage)

	// Calculate CRC32 checksum for the page data (excluding the checksum field itself)
	// Zero out the checksum field before calculating
	binary.BigEndian.PutUint32(radixPage.data[4:8], 0)
	// Calculate checksum of the entire page
	checksum := crc32.ChecksumIEEE(radixPage.data)
	// Write the checksum at position 4
	binary.BigEndian.PutUint32(radixPage.data[4:8], checksum)

	// Ensure the page number and offset are valid
	if radixPage.pageNumber == 0 {
		return fmt.Errorf("cannot write radix page with page number 0")
	}

	debugPrint("Writing radix page to index file at page %d\n", radixPage.pageNumber)

	// Write to disk at the specified page number
	return db.writeIndexPage(radixPage)
}

// ------------------------------------------------------------------------------------------------
// Leaf pages
// ------------------------------------------------------------------------------------------------

// parseLeafPage parses a leaf page read from the disk
func (db *DB) parseLeafPage(data []byte, pageNumber uint32) (*LeafPage, error) {
	// Check if it's a leaf page
	if data[0] != ContentTypeLeaf {
		return nil, fmt.Errorf("not a leaf page at page %d", pageNumber)
	}

	// Verify CRC32 checksum
	storedChecksum := binary.BigEndian.Uint32(data[4:8])
	// Zero out the checksum field for calculation
	binary.BigEndian.PutUint32(data[4:8], 0)
	// Calculate the checksum
	calculatedChecksum := crc32.ChecksumIEEE(data)
	// Restore the original checksum in the data
	binary.BigEndian.PutUint32(data[4:8], storedChecksum)
	// Verify the checksum
	if storedChecksum != calculatedChecksum {
		return nil, fmt.Errorf("leaf page checksum mismatch at page %d: stored=%d, calculated=%d", pageNumber, storedChecksum, calculatedChecksum)
	}

	// Get content size
	contentSize := binary.LittleEndian.Uint16(data[2:4])

	// Create structured leaf page
	leafPage := &LeafPage{
		pageNumber:  pageNumber,
		pageType:    ContentTypeLeaf,
		data:        data,
		dirty:       false,
		ContentSize: contentSize,
	}

	// Parse entries
	entries, err := db.parseLeafEntries(leafPage)
	if err != nil {
		return nil, fmt.Errorf("failed to parse leaf entries: %w", err)
	}
	leafPage.Entries = entries

	// Add to cache
	db.addToCache(leafPage)

	return leafPage, nil
}

// writeLeafPage writes a leaf page to the database file
func (db *DB) writeLeafPage(leafPage *LeafPage) error {
	// Set page type in the data
	leafPage.data[0] = ContentTypeLeaf  // Type identifier

	// Write content size
	binary.LittleEndian.PutUint16(leafPage.data[2:4], leafPage.ContentSize)

	// Calculate CRC32 checksum for the page data (excluding the checksum field itself)
	// Zero out the checksum field before calculating
	binary.BigEndian.PutUint32(leafPage.data[4:8], 0)
	// Calculate checksum of the entire page
	checksum := crc32.ChecksumIEEE(leafPage.data)
	// Write the checksum at position 4
	binary.BigEndian.PutUint32(leafPage.data[4:8], checksum)

	// If page number is 0, allocate a new page at the end of the file
	if leafPage.pageNumber == 0 {
		return fmt.Errorf("cannot write leaf page with page number 0")
	}

	// Write to disk at the specified page number
	return db.writeIndexPage(leafPage)
}

// ------------------------------------------------------------------------------------------------
// Index pages
// ------------------------------------------------------------------------------------------------

// readPage reads a page from the index file
// first read 1 byte to check the page type
// then read the page data
func (db *DB) readPage(pageNumber uint32) (*Page, error) {
	data, err := db.readIndexPage(pageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %w", err)
	}

	contentType := data[0]

	// Based on the page type, read the full page
	switch contentType {
	case ContentTypeRadix:
		// Read the radix page
		return db.parseRadixPage(data, pageNumber)

	case ContentTypeLeaf:
		// Read the leaf page
		return db.parseLeafPage(data, pageNumber)

	default:
		return nil, fmt.Errorf("unknown page type: %c", contentType)
	}
}

// writeIndexPage writes an index page to either the WAL file or the index file
func (db *DB) writeIndexPage(page *Page) error {
	var err error

	// If WAL is used, write to WAL file
	if db.useWAL {
		err = db.writeToWAL(page.data, page.pageNumber)
	// Otherwise, write directly to the index file
	} else {
		err = db.writeToIndexFile(page.data, page.pageNumber)
	}

	// If the page was written successfully
	if err == nil {
		// Mark it as clean
		page.dirty = false
		// If using WAL, mark it as part of the WAL
		if db.useWAL {
			page.isWAL = true
		}
		// Discard previous versions of this page
		db.cacheMutex.Lock()
		page.next = nil
		db.cacheMutex.Unlock()
	}

	return err
}

// readIndexPage reads an index page from the index file
func (db *DB) readIndexPage(pageNumber uint32) ([]byte, error) {
	return db.readFromIndexFile(pageNumber)
}

// writeToIndexFile writes an index page to the index file
func (db *DB) writeToIndexFile(data []byte, pageNumber uint32) error {
	// Calculate file offset from page number
	offset := int64(pageNumber) * PageSize

	// Check if offset is valid
	if offset < 0 || offset >= db.indexFileSize {
		return fmt.Errorf("page number %d out of index file bounds", pageNumber)
	}

	// Write the page data
	if _, err := db.indexFile.WriteAt(data, offset); err != nil {
		return fmt.Errorf("failed to write page data to index file: %w", err)
	}

	return nil
}

// readFromIndexFile reads an index page from the index file
func (db *DB) readFromIndexFile(pageNumber uint32) ([]byte, error) {
	// Calculate file offset from page number
	offset := int64(pageNumber) * PageSize

	// Check if offset is valid
	if offset < 0 || offset >= db.indexFileSize {
		return nil, fmt.Errorf("page number %d out of index file bounds", pageNumber)
	}

	data := make([]byte, PageSize)

	// Read the page data
	if _, err := db.indexFile.ReadAt(data, offset); err != nil {
		return nil, fmt.Errorf("failed to read page data from index file: %w", err)
	}

	return data, nil
}

// ------------------------------------------------------------------------------------------------
// ...
// ------------------------------------------------------------------------------------------------

// Sync flushes all dirty pages to disk and syncs the files
func (db *DB) Sync() error {
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot sync: database opened in read-only mode")
	}

	// Flush the index to disk
	if err := db.flushIndexToDisk(); err != nil {
		return fmt.Errorf("failed to flush index to disk: %w", err)
	}

	// Sync the main file
	if err := db.mainFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync main file: %w", err)
	}

	// Sync the index file
	if err := db.indexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file: %w", err)
	}

	return nil
}

// RefreshFileSize updates the cached file sizes from the actual files
func (db *DB) RefreshFileSize() error {
	// Refresh main file size
	mainFileInfo, err := db.mainFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get main file size: %w", err)
	}
	db.mainFileSize = mainFileInfo.Size()

	// Refresh index file size
	indexFileInfo, err := db.indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get index file size: %w", err)
	}
	db.indexFileSize = indexFileInfo.Size()

	return nil
}

// ------------------------------------------------------------------------------------------------
// Transaction API
// ------------------------------------------------------------------------------------------------

// Begin a new transaction
func (db *DB) Begin() (*Transaction, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// Check if a transaction is already open
	if db.inTransaction {
		return nil, fmt.Errorf("a transaction is already open")
	}

	// Mark transaction as open
	db.inTransaction = true

	// Start a transaction
	db.beginTransaction()

	// Create and return transaction object
	return &Transaction{db: db}, nil
}

// Commit a transaction
func (tx *Transaction) Commit() error {
	tx.db.mutex.Lock()
	defer tx.db.mutex.Unlock()

	// Check if transaction is open
	if !tx.db.inTransaction {
		return fmt.Errorf("no transaction is open")
	}

	// Commit the transaction
	tx.db.commitTransaction()

	// Mark transaction as closed
	tx.db.inTransaction = false

	return nil
}

// Rollback a transaction
func (tx *Transaction) Rollback() error {
	tx.db.mutex.Lock()
	defer tx.db.mutex.Unlock()

	// Check if transaction is open
	if !tx.db.inTransaction {
		return fmt.Errorf("no transaction is open")
	}

	// Rollback the transaction
	tx.db.rollbackTransaction()

	// Mark transaction as closed
	tx.db.inTransaction = false

	return nil
}

// Set a key-value pair within a transaction
func (tx *Transaction) Set(key, value []byte) error {
	// Call the database's set method
	tx.db.calledByTransaction = true
	err := tx.db.Set(key, value)
	tx.db.calledByTransaction = false
	return err
}

// Get a value for a key within a transaction
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	// Call the database's get method
	return tx.db.Get(key)
}

// Delete a key within a transaction
func (tx *Transaction) Delete(key []byte) error {
	// Call the database's delete method
	tx.db.calledByTransaction = true
	err := tx.db.Delete(key)
	tx.db.calledByTransaction = false
	return err
}

// ------------------------------------------------------------------------------------------------
// Internal Transactions
// ------------------------------------------------------------------------------------------------

// beginTransaction starts a new transaction
func (db *DB) beginTransaction() {
	// Increment transaction sequence number
	db.txnSequence++
	debugPrint("Beginning transaction %d\n", db.txnSequence)
}

// commitTransaction commits the current transaction
func (db *DB) commitTransaction() {
	debugPrint("Committing transaction %d\n", db.txnSequence)

	// If using WAL and in caller thread mode, flush to disk
	if db.useWAL && db.commitMode == CallerThread {
		db.flushIndexToDisk()
	}
}

// rollbackTransaction rolls back the current transaction
func (db *DB) rollbackTransaction() {
	debugPrint("Rolling back transaction %d\n", db.txnSequence)

	// It can do:
	// 1. Discard all dirty pages from this transaction
	// 2. Restore pages from WAL if needed


	// It could use an optimistic approach:
	// - do not clone pages for new transactions, only if there is a flush happening
	// on rollback:
	// - truncate the main db file to the stored size before the transaction started
	// - discard all dirty pages
	// - rebuild the index pages from the main db file (incremental reindexing)

	// PROBLEM: on a crash, the main file can contain uncommitted changes
	// SOLUTION: do "transactions" virtually, on memory, and only write to disk when the transaction is committed

}

// ------------------------------------------------------------------------------------------------
// Page Cache
// ------------------------------------------------------------------------------------------------

// addToCache adds a page to the cache
func (db *DB) addToCache(page *Page) {
	if page == nil {
		return
	}

	pageNumber := page.pageNumber

	db.cacheMutex.Lock()
	defer db.cacheMutex.Unlock()

	// If there is already a page with the same page number
	existingPage, exists := db.pageCache[pageNumber]
	if exists {
		// Link the new page to the existing page
		page.next = existingPage
	} else {
		// Clear the next pointer
		page.next = nil
	}

	// Add the new page to the cache
	db.pageCache[pageNumber] = page
}

// getPageFromCache gets a page from the cache by page number
func (db *DB) getPageFromCache(pageNumber uint32) (*Page, bool) {
	db.cacheMutex.RLock()
	page, exists := db.pageCache[pageNumber]
	db.cacheMutex.RUnlock()

	return page, exists
}

// getWritablePage gets a writable version of a page
// if the given page is already writable, it returns the page itself
func (db *DB) getWritablePage(page *Page) (*Page, error) {
	// We cannot write to a page that is part of the WAL
	needsClone := page.isWAL
	// If the page is marked to be flushed, we cannot modify its data
	if db.flushSequence != 0 && page.txnSequence <= db.flushSequence {
		needsClone = true
	}

	// If the page needs to be cloned, clone it
	if needsClone {
		var err error
		page, err = db.clonePage(page)
		if err != nil {
			return nil, fmt.Errorf("failed to clone page: %w", err)
		}
	}

	// Update the transaction sequence
	page.txnSequence = db.txnSequence

	// Return the page
	return page, nil
}

// clonePage clones a page
func (db *DB) clonePage(page *Page) (*Page, error) {
	var err error

	// Get a reference to the old page
	oldPage := page

	/*
	// Create a new page
	newPage := &Page{
		pageNumber: page.pageNumber,
		pageType:   page.pageType,
		data:       make([]byte, PageSize),
		dirty:      false,
		isWAL:      false,
		accessTime: page.accessTime,
		txnSequence: db.txnSequence,
	}
	*/

	// Clone the page data
	data := make([]byte, PageSize)
	copy(data, page.data)

	// Parse the page
	if page.pageType == ContentTypeRadix {
		page, err = db.parseRadixPage(data, page.pageNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to parse radix page: %w", err)
		}
	} else if page.pageType == ContentTypeLeaf {
		page, err = db.parseLeafPage(data, page.pageNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to parse leaf page: %w", err)
		}
	} else {
		return nil, fmt.Errorf("unknown page type: %d", page.pageType)
	}

	// Add to cache  -- already done by parseRadixPage or parseLeafPage
	//db.addToCache(newPage)  // TODO: make the cache support multiple versions of the same page

	// Update the access time
	//db.accessCounter++
	//page.accessTime = db.accessCounter
	page.accessTime = oldPage.accessTime

	return page, nil
}

// checkPageCache checks if the page cache is full and initiates a clean up or flush
// step 1: if the amount of dirty pages is above the threshold, flush it
//  if the page cache is below the threshold, return
// step 2: try to remove clean pages from the cache
// step 3: if the page cache is still above the threshold, flush it
// This function should not return an error, it can log an error and continue
func (db *DB) checkPageCache(isWrite bool) {

	// If the amount of dirty pages is above the threshold, flush it
	if isWrite && db.dirtyPageCount >= db.dirtyPageThreshold {
		// Check which thread should flush the pages
		if db.commitMode == CallerThread {
			// Write the pages to the WAL file
			db.flushDirtyIndexPages()
		} else {
			// Signal the worker thread to flush the pages
			//db.workerChannel <- "flush"    TODO
		}
		return
	}

	// If the size of the page cache is above the threshold, remove old pages
	if len(db.pageCache) >= db.cacheSizeThreshold {
		// Check which thread should remove the old pages
		if db.commitMode == CallerThread {
			// Try to remove the old clean pages from the cache
			db.removeOldPagesFromCache()
		} else {
			// Signal the worker thread to remove the old pages
			//db.workerChannel <- "clean"    TODO
		}
	}

}

// discardNewerPages removes pages from the current transaction from the cache
func (db *DB) discardNewerPages(currentSeq int64) {
	db.cacheMutex.Lock()
	defer db.cacheMutex.Unlock()

	// Iterate through all pages in the cache
	for pageNumber, page := range db.pageCache {
		// Find the first page that's not from the current transaction
		var newHead *Page = page
		for newHead != nil && newHead.txnSequence == currentSeq {
			newHead = newHead.next
		}
		// Update the cache with the new head (or delete if no valid entries remain)
		if newHead != nil {
			db.pageCache[pageNumber] = newHead
		} else {
			delete(db.pageCache, pageNumber)
		}
	}
}

// clearWALCache sets isWAL to false for all pages in which isWAL is true
// then removes older versions of the pages
func (db *DB) clearWALCache() {
	db.cacheMutex.Lock()

	// Iterate through all pages in the cache
	for _, firstPage := range db.pageCache {
		// Iterate through all pages in the linked list
		for page := firstPage; page != nil; page = page.next {
			if page.isWAL {
				page.isWAL = false
			}
		}
	}

	db.cacheMutex.Unlock()

	//db.discardOldPageVersions(false)
	// not sure if this can be done by the worker thread
	// the main thread can be allocating pages

}

// discardOldPageVersions removes older versions of pages after a commit
func (db *DB) discardOldPageVersions(keepWAL bool) {
	db.cacheMutex.Lock()
	defer db.cacheMutex.Unlock()

	// Iterate through all pages in the cache
	for _, entry := range db.pageCache {
		// Skip if there's no older version
		if entry == nil || entry.next == nil {
			continue
		}

		// First, collect all pages that need to be preserved
		var pagesToKeep []*Page

		// Iterate through older versions to find pages to keep
		for temp := entry.next; temp != nil; temp = temp.next {
			// Keep pages that are:
			// 1. WAL pages (if keepWAL is true)
			// 2. Dirty pages that need to be written to disk
			if (keepWAL && temp.isWAL) || temp.dirty {
				pagesToKeep = append(pagesToKeep, temp)
			}
		}

		// If no pages need to be kept, clear the next pointer
		if len(pagesToKeep) == 0 {
			entry.next = nil
			continue
		}

		// Rebuild the chain with the pages to keep
		entry.next = pagesToKeep[0]

		// Link the remaining pages
		for i := 0; i < len(pagesToKeep)-1; i++ {
			pagesToKeep[i].next = pagesToKeep[i+1]
		}

		// Terminate the chain
		pagesToKeep[len(pagesToKeep)-1].next = nil
	}
}

// removeOldPagesFromCache removes old clean pages from the cache
// it cannot remove pages that are part of the WAL
// as other threads can be accessing these pages, this thread can only remove pages that have not been accessed recently
func (db *DB) removeOldPagesFromCache() {
	// Define a struct to hold page information for sorting
	type pageInfo struct {
		pageNumber uint32
		accessTime uint64
	}

	// If cache is empty or too small, nothing to do
	if len(db.pageCache) <= db.cacheSizeThreshold/2 {
		return
	}

	// Compute the target size (aim to reduce to 75% of threshold)
	targetSize := db.cacheSizeThreshold * 3 / 4

	// Compute the number of pages to remove
	numPagesToRemove := len(db.pageCache) - targetSize

	// Step 1: Use a read lock to collect candidates
	var candidates []pageInfo

	db.cacheMutex.RLock()

	// Collect removable pages
	for pageNumber, page := range db.pageCache {
		// Skip dirty pages and WAL pages
		if page.dirty || page.isWAL {
			continue
		}

		// Add to candidates
		candidates = append(candidates, pageInfo{
			pageNumber: pageNumber,
			accessTime: page.accessTime,
		})
	}
	db.cacheMutex.RUnlock()

	// If no candidates, nothing to do
	if len(candidates) == 0 {
		return
	}

	// Step 2: Sort candidates by access time (oldest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].accessTime < candidates[j].accessTime
	})

	// Step 3: Acquire write lock and remove pages
	db.cacheMutex.Lock()

	// Remove the oldest pages
	removedCount := 0
	for i := 0; i < len(candidates) && removedCount < numPagesToRemove; i++ {
		pageNumber := candidates[i].pageNumber

		// Double-check the page still exists and is still removable
		if page, exists := db.pageCache[pageNumber]; exists {
			// Skip if the page is dirty or WAL
			if page.dirty || page.isWAL {
				continue
			}

			// Skip if the page is part of a linked list with dirty or WAL pages
			hasNonRemovable := false
			for p := page.next; p != nil; p = p.next {
				if p.dirty || p.isWAL {
					hasNonRemovable = true
					break
				}
			}
			if hasNonRemovable {
				continue
			}

			// Remove the page from the cache
			delete(db.pageCache, pageNumber)
			removedCount++
		}
	}

	db.cacheMutex.Unlock()

	debugPrint("Removed %d pages from cache, new size: %d\n", removedCount, len(db.pageCache))
}

// ------------------------------------------------------------------------------------------------
// Page access
// ------------------------------------------------------------------------------------------------

// getPage gets a page from the cache or from the disk
func (db *DB) getPage(pageNumber uint32) (*Page, error) {
	// First check the cache
	page, exists := db.getPageFromCache(pageNumber)

	// If not in cache, read it from disk
	if !exists {
		var err error
		page, err = db.readPage(pageNumber)
		if err != nil {
			return nil, err
		}
	}

	// Update the access time
	db.accessCounter++
	page.accessTime = db.accessCounter

	// Return the page
	return page, nil
}

// getRadixPage returns a radix page from the cache or from the disk
func (db *DB) getRadixPage(pageNumber uint32) (*RadixPage, error) {
	// Get the page from the cache or from the disk
	page, err := db.getPage(pageNumber)
	if err != nil {
		return nil, err
	}

	// If the page is not a radix page, return an error
	if page.pageType != ContentTypeRadix {
		return nil, fmt.Errorf("page %d is not a radix page", pageNumber)
	}

	// Return the radix page
	return page, nil
}

// getLeafPage returns a leaf page from the cache or from the disk
func (db *DB) getLeafPage(pageNumber uint32) (*LeafPage, error) {
	// Get the page from the cache or from the disk
	page, err := db.getPage(pageNumber)
	if err != nil {
		return nil, err
	}

	// If the page is not a leaf page, return an error
	if page.pageType != ContentTypeLeaf {
		return nil, fmt.Errorf("page %d is not a leaf page", pageNumber)
	}

	// Return the leaf page
	return page, nil
}

// GetCacheStats returns statistics about the page cache
func (db *DB) GetCacheStats() map[string]interface{} {
	db.cacheMutex.RLock()
	defer db.cacheMutex.RUnlock()

	stats := make(map[string]interface{})
	stats["cache_size"] = len(db.pageCache)

	// Count pages by type
	radixPages := 0
	leafPages := 0

	for _, page := range db.pageCache {
		if page.pageType == ContentTypeRadix {
			radixPages++
		} else if page.pageType == ContentTypeLeaf {
			leafPages++
		}
	}

	stats["radix_pages"] = radixPages
	stats["leaf_pages"] = leafPages

	return stats
}

// flushIndexToDisk flushes all dirty pages to disk and writes the index header
func (db *DB) flushIndexToDisk() error {
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot flush index to disk: database opened in read-only mode")
	}

	// Flush all dirty pages
	if err := db.flushDirtyIndexPages(); err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
	}

	// Write index header
	if err := db.writeIndexHeader(false); err != nil {
		return fmt.Errorf("failed to update index header: %w", err)
	}

	// Commit the transaction if using WAL
	if db.useWAL {
		if err := db.walCommit(); err != nil {
			return fmt.Errorf("failed to commit WAL: %w", err)
		}
	}

	return nil
}

// flushAllIndexPages writes all cached pages to disk
func (db *DB) flushAllIndexPages() error {
	db.cacheMutex.RLock()
	defer db.cacheMutex.RUnlock()

	// Get all page numbers and sort them
	pageNumbers := make([]uint32, 0, len(db.pageCache))
	for pageNumber := range db.pageCache {
		pageNumbers = append(pageNumbers, pageNumber)
	}

	// Sort page numbers in ascending order
	sort.Slice(pageNumbers, func(i, j int) bool {
		return pageNumbers[i] < pageNumbers[j]
	})

	// Process pages in order
	for _, pageNumber := range pageNumbers {
		page := db.pageCache[pageNumber]
		if page.pageType == ContentTypeRadix {
			if err := db.writeRadixPage(page); err != nil {
				return err
			}
		} else if page.pageType == ContentTypeLeaf {
			if err := db.writeLeafPage(page); err != nil {
				return err
			}
		}
	}

	return nil
}

// flushDirtyIndexPages writes all dirty pages to disk
func (db *DB) flushDirtyIndexPages() error {
	db.cacheMutex.RLock()
	defer db.cacheMutex.RUnlock()

	// Get all page numbers and sort them
	pageNumbers := make([]uint32, 0, len(db.pageCache))
	for pageNumber := range db.pageCache {
		pageNumbers = append(pageNumbers, pageNumber)
	}

	// Sort page numbers in ascending order
	sort.Slice(pageNumbers, func(i, j int) bool {
		return pageNumbers[i] < pageNumbers[j]
	})

	// Process pages in order
	for _, pageNumber := range pageNumbers {
		page := db.pageCache[pageNumber]
		if page.dirty {
			if page.pageType == ContentTypeRadix {
				if err := db.writeRadixPage(page); err != nil {
					return err
				}
			} else if page.pageType == ContentTypeLeaf {
				if err := db.writeLeafPage(page); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// ------------------------------------------------------------------------------------------------
// Utility functions
// ------------------------------------------------------------------------------------------------

// getRootRadixPage returns the root radix page (page 1) from the cache
func (db *DB) getRootRadixPage() (*RadixPage, error) {
	return db.getRadixPage(1)
}

// getRootRadixPage returns the root radix page (page 1) from the cache
func (db *DB) getRootRadixSubPage() (*RadixSubPage, error) {
	rootPage, err := db.getRadixPage(1)
	if err != nil {
		return nil, err
	}

	// Ensure the root page has at least one sub-page
	if rootPage.SubPagesUsed == 0 {
		return nil, fmt.Errorf("root radix page has no sub-pages")
	}

	return &RadixSubPage{
		Page:       rootPage,
		SubPageIdx: 0,
	}, nil
}

// equal compares two byte slices
func equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// ------------------------------------------------------------------------------------------------
// Radix pages
// ------------------------------------------------------------------------------------------------

// allocateRadixPage creates a new empty radix page and allocates a page number
func (db *DB) allocateRadixPage() (*RadixPage, error) {
	// Allocate the page data
	data := make([]byte, PageSize)

	// Calculate new page number
	pageNumber := uint32(db.indexFileSize / PageSize)

	// Update file size
	db.indexFileSize += PageSize

	radixPage := &RadixPage{
		pageNumber:  pageNumber,
		pageType:    ContentTypeRadix,
		data:        data,
		dirty:       false,
		SubPagesUsed: 0,
	}

	// Add to cache
	db.addToCache(radixPage)

	// Update the access time
	db.accessCounter++
	radixPage.accessTime = db.accessCounter

	// Update the transaction sequence
	radixPage.txnSequence = db.txnSequence

	debugPrint("Allocated new radix page at page %d\n", pageNumber)

	return radixPage, nil
}

// allocateRadixSubPage returns the next available radix sub-page or creates a new one if needed
// It returns a RadixSubPage struct
func (db *DB) allocateRadixSubPage() (*RadixSubPage, error) {

	// If we have a cached free radix page with available sub-pages
	if db.freeSubPagesHead != nil && db.freeSubPagesHead.SubPagesUsed < SubPagesPerRadixPage {
		// Get a reference to the radix page
		radixPage := db.freeSubPagesHead

		// Get a writable version of the page
		radixPage, err := db.getWritablePage(radixPage)
		if err != nil {
			return nil, fmt.Errorf("failed to get writable page: %w", err)
		}

		// Update the free sub-pages head
		db.freeSubPagesHead = radixPage

		// Use the next available sub-page index
		subPageIdx := radixPage.SubPagesUsed

		// Increment the sub-page counter (will be written when the page is updated)
		radixPage.SubPagesUsed++
		radixPage.dirty = true

		// Create a new radix sub-page
		radixSubPage := &RadixSubPage{
			Page:       radixPage,
			SubPageIdx: subPageIdx,
		}

		// If the page is now full, remove it from the free list
		if radixPage.SubPagesUsed >= SubPagesPerRadixPage {
			// Save the next free page pointer
			nextFreePage := radixPage.NextFreePage
			// Clear the next free page pointer
			radixPage.NextFreePage = 0

			// Get the next free page and update the head of the free list
			nextPage := (*RadixPage)(nil)
			if nextFreePage > 0 {
				// Load the next free page
				var err error
				nextPage, err = db.getRadixPage(nextFreePage)
				if err != nil {
					return nil, fmt.Errorf("failed to load next free page: %w", err)
				}
			}
			db.updateFreeSubPagesHead(nextPage)
		}

		// Return the new radix sub-page
		return radixSubPage, nil
	}

	// No available radix page found or free list is empty
	// Allocate a new radix page
	newRadixPage, err := db.allocateRadixPage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate radix page: %w", err)
	}

	// Mark first sub-page as used
	newRadixPage.SubPagesUsed = 1

	// Mark page as dirty
	newRadixPage.dirty = true

	// Add this page to the free sub-pages list
	db.addToFreeSubPagesList(newRadixPage)

	return &RadixSubPage{
		Page:       newRadixPage,
		SubPageIdx: 0,
	}, nil
}

// allocateLeafPage creates a new empty leaf page and allocates a page number
func (db *DB) allocateLeafPage() (*LeafPage, error) {
	// Allocate the page data
	data := make([]byte, PageSize)

	// Calculate new page number
	pageNumber := uint32(db.indexFileSize / PageSize)

	// Update file size
	db.indexFileSize += PageSize

	leafPage := &LeafPage{
		pageNumber:  pageNumber,
		pageType:    ContentTypeLeaf,
		data:        data,
		dirty:       false,
		ContentSize: LeafHeaderSize,
		Entries:     make([]LeafEntry, 0),
	}

	// Add to cache
	db.addToCache(leafPage)

	// Update the access time
	db.accessCounter++
	leafPage.accessTime = db.accessCounter

	// Update the transaction sequence
	leafPage.txnSequence = db.txnSequence

	debugPrint("Allocated new leaf page at page %d\n", pageNumber)

	return leafPage, nil
}

// ------------------------------------------------------------------------------------------------
// Leaf entries
// ------------------------------------------------------------------------------------------------

// parseLeafEntries parses the entries in a leaf page
func (db *DB) parseLeafEntries(leafPage *LeafPage) ([]LeafEntry, error) {
	var entries []LeafEntry

	// Start at header size
	pos := int(LeafHeaderSize)

	// Read entries until we reach content size
	for pos < int(leafPage.ContentSize) {
		// Read suffix length
		suffixLen64, bytesRead := varint.Read(leafPage.data[pos:])
		if bytesRead == 0 {
			return nil, fmt.Errorf("failed to read suffix length")
		}
		suffixLen := int(suffixLen64)
		pos += bytesRead

		// Get suffix as a slice of the page data
		suffix := leafPage.data[pos:pos+suffixLen]
		pos += suffixLen

		// Read data offset
		dataOffset := int64(binary.LittleEndian.Uint64(leafPage.data[pos:]))
		pos += 8

		// Add entry to list
		entries = append(entries, LeafEntry{
			Suffix:     suffix,
			DataOffset: dataOffset,
		})
	}

	return entries, nil
}

// addLeafEntry tries to add the entry with the suffix to the leaf page
// If the leaf page is full, it converts it to a radix page
func (db *DB) addLeafEntry(leafPage *LeafPage, suffix []byte, dataOffset int64) error {

	// Get a writable version of the page
	leafPage, err := db.getWritablePage(leafPage)
	if err != nil {
		return err
	}

	// Calculate size needed for this entry
	suffixLenSize := varint.Size(uint64(len(suffix)))
	entrySize := suffixLenSize + len(suffix) + 8 // suffix length + suffix + data offset

	// Check if there's enough space in the page
	if int(leafPage.ContentSize)+entrySize > PageSize {
		// Leaf page is full, convert it to a radix page
		return db.convertLeafPageToRadixPage(leafPage, suffix, dataOffset)
	}

	// Get current content position
	pos := int(leafPage.ContentSize)

	// Write suffix length
	suffixLen := len(suffix)
	suffixLenWritten := varint.Write(leafPage.data[pos:], uint64(suffixLen))
	pos += suffixLenWritten

	// Write suffix
	suffixPos := pos
	copy(leafPage.data[pos:], suffix)
	pos += suffixLen

	// Write data offset
	binary.LittleEndian.PutUint64(leafPage.data[pos:], uint64(dataOffset))
	pos += 8

	// Update content size
	leafPage.ContentSize = uint16(pos)

	// Add to entries list
	leafPage.Entries = append(leafPage.Entries, LeafEntry{
		Suffix:     leafPage.data[suffixPos:suffixPos+suffixLen],
		DataOffset: dataOffset,
	})

	// Mark page as dirty
	leafPage.dirty = true

	return nil
}

// removeLeafEntryAt removes an entry from a leaf page at the given index
// Returns true if the entry was removed
func (db *DB) removeLeafEntryAt(leafPage *LeafPage, index int) (bool, error) {
	// Check if index is valid
	if index < 0 || index >= len(leafPage.Entries) {
		return false, nil
	}

	// Get a writable version of the page
	leafPage, err := db.getWritablePage(leafPage)
	if err != nil {
		return false, err
	}

	// Remove the entry from the entries list
	leafPage.Entries = append(leafPage.Entries[:index], leafPage.Entries[index+1:]...)

	// Rebuild the page data with the updated entries list
	db.rebuildLeafPageData(leafPage)

	// Mark page as dirty
	leafPage.dirty = true

	return true, nil
}

// updateLeafEntryOffset updates the content offset for an existing leaf entry in the data buffer
func (db *DB) updateLeafEntryOffset(leafPage *LeafPage, index int, dataOffset int64) error {

	// Get a writable version of the page
	leafPage, err := db.getWritablePage(leafPage)
	if err != nil {
		return err
	}

	// Update the entry
	leafPage.Entries[index].DataOffset = dataOffset

	// We need to find the position of this entry in the data buffer
	pos := int(LeafHeaderSize)

	// Iterate through entries until we reach the one we want to update
	for i := 0; i < index; i++ {
		// Skip the suffix length
		suffixLen64, bytesRead := varint.Read(leafPage.data[pos:])
		suffixLen := int(suffixLen64)
		pos += bytesRead

		// Skip the suffix
		pos += suffixLen

		// Skip the data offset
		pos += 8
	}

	// Now we're at the start of our target entry
	// Skip the suffix length
	suffixLen64, bytesRead := varint.Read(leafPage.data[pos:])
	suffixLen := int(suffixLen64)
	pos += bytesRead

	// Skip the suffix
	pos += suffixLen

	// Write the new data offset
	binary.LittleEndian.PutUint64(leafPage.data[pos:], uint64(dataOffset))

	// Mark the page as dirty
	leafPage.dirty = true

	return nil
}

// rebuildLeafPageData rebuilds the leaf page data from the entries list
func (db *DB) rebuildLeafPageData(leafPage *LeafPage) {

	// Create new entries list
	newEntries := make([]LeafEntry, 0, len(leafPage.Entries))

	// Create a new data buffer
	newData := make([]byte, PageSize)

	// Copy the header
	copy(newData[:LeafHeaderSize], leafPage.data[:LeafHeaderSize])

	// Rebuild data from old entries (that reference the old data buffer)
	pos := int(LeafHeaderSize)

	for _, entry := range leafPage.Entries {
		// Write suffix length
		suffixLen := len(entry.Suffix)
		suffixLenWritten := varint.Write(newData[pos:], uint64(suffixLen))
		pos += suffixLenWritten

		// Write suffix to new data
		suffixPos := pos
		copy(newData[pos:], entry.Suffix)
		pos += suffixLen

		// Write data offset
		binary.LittleEndian.PutUint64(newData[pos:], uint64(entry.DataOffset))
		pos += 8

		// Add to new entries list with suffix as slice of new data
		newEntries = append(newEntries, LeafEntry{
			Suffix:     newData[suffixPos:suffixPos+suffixLen],
			DataOffset: entry.DataOffset,
		})
	}

	// Replace the old data with the new data
	leafPage.data = newData

	// Replace entries list with the new one
	leafPage.Entries = newEntries

	// Update content size
	leafPage.ContentSize = uint16(pos)
}

// ------------------------------------------------------------------------------------------------
// Radix entries (on sub-pages)
// ------------------------------------------------------------------------------------------------

// setRadixPageEntry sets an entry in a radix page
func (db *DB) setRadixPageEntry(radixPage *RadixPage, subPageIdx uint8, byteValue uint8, pageNumber uint32, nextSubPageIdx uint8) error {
	// Check if subPage is valid
	if subPageIdx >= SubPagesPerRadixPage {
		return fmt.Errorf("sub-page index out of range")
	}

	// Get a writable version of the page
	radixPage, err := db.getWritablePage(radixPage)
	if err != nil {
		return err
	}

	// Calculate base offset for this entry in the page data
	subPageOffset := RadixHeaderSize + int(subPageIdx) * SubPageSize
	entryOffset := subPageOffset + int(byteValue) * RadixEntrySize

	// Write page number (4 bytes)
	binary.LittleEndian.PutUint32(radixPage.data[entryOffset:entryOffset+4], pageNumber)

	// Write sub-page index (1 byte)
	radixPage.data[entryOffset+4] = nextSubPageIdx

	// Mark page as dirty
	radixPage.dirty = true

	return nil
}

// getRadixPageEntry gets an entry from a radix page
func (db *DB) getRadixPageEntry(radixPage *RadixPage, subPageIdx uint8, byteValue uint8) (pageNumber uint32, nextSubPageIdx uint8) {
	// Check if subPage is valid
	if subPageIdx >= SubPagesPerRadixPage || subPageIdx >= radixPage.SubPagesUsed {
		return 0, 0
	}

	// Calculate base offset for this entry in the page data
	subPageOffset := RadixHeaderSize + int(subPageIdx) * SubPageSize
	entryOffset := subPageOffset + int(byteValue) * RadixEntrySize

	// Read page number (4 bytes)
	pageNumber = binary.LittleEndian.Uint32(radixPage.data[entryOffset:entryOffset+4])

	// Read sub-page index (1 byte)
	nextSubPageIdx = radixPage.data[entryOffset+4]

	return pageNumber, nextSubPageIdx
}

// getEmptySuffixOffset gets the empty suffix offset for a radix sub-page
func (db *DB) getEmptySuffixOffset(subPage *RadixSubPage) int64 {
	// Calculate the offset for the empty suffix in the page data
	// Each sub-page has 256 entries of 5 bytes each, followed by an 8-byte empty suffix offset
	subPageOffset := RadixHeaderSize + int(subPage.SubPageIdx) * SubPageSize
	emptySuffixOffsetPos := subPageOffset + EntriesPerSubPage * RadixEntrySize

	// Read the 8-byte offset
	return int64(binary.LittleEndian.Uint64(subPage.Page.data[emptySuffixOffsetPos:emptySuffixOffsetPos+8]))
}

// setEmptySuffixOffset sets the empty suffix offset for a radix sub-page
func (db *DB) setEmptySuffixOffset(subPage *RadixSubPage, offset int64) error {
	// Get a reference to the radix page
	radixPage := subPage.Page

	// Get a writable version of the page
	radixPage, err := db.getWritablePage(radixPage)
	if err != nil {
		return err
	}

	// Calculate the offset for the empty suffix in the page data
	// Each sub-page has 256 entries of 5 bytes each, followed by an 8-byte empty suffix offset
	subPageOffset := RadixHeaderSize + int(subPage.SubPageIdx) * SubPageSize
	emptySuffixOffsetPos := subPageOffset + EntriesPerSubPage * RadixEntrySize

	// Write the 8-byte offset
	binary.LittleEndian.PutUint64(radixPage.data[emptySuffixOffsetPos:emptySuffixOffsetPos+8], uint64(offset))

	// Mark the page as dirty
	radixPage.dirty = true

	return nil
}

// setRadixEntry sets an entry in a radix sub-page
func (db *DB) setRadixEntry(subPage *RadixSubPage, byteValue uint8, pageNumber uint32, nextSubPageIdx uint8) error {
	return db.setRadixPageEntry(subPage.Page, subPage.SubPageIdx, byteValue, pageNumber, nextSubPageIdx)
}

// getRadixEntry gets an entry from a radix sub-page
func (db *DB) getRadixEntry(subPage *RadixSubPage, byteValue uint8) (pageNumber uint32, nextSubPageIdx uint8) {
	return db.getRadixPageEntry(subPage.Page, subPage.SubPageIdx, byteValue)
}

// ------------------------------------------------------------------------------------------------
// Preload
// ------------------------------------------------------------------------------------------------

// preloadRadixLevels preloads the first two levels of the radix tree into the cache
func (db *DB) preloadRadixLevels() error {
	// First, load the root radix sub-page
	rootSubPage, err := db.getRootRadixSubPage()
	if err != nil {
		return fmt.Errorf("failed to read root radix page: %w", err)
	}

	// For each entry in the root sub-page (first level), load the referenced sub-pages
	for byteValue := 0; byteValue < 256; byteValue++ {
		pageNumber, _ := db.getRadixEntry(rootSubPage, uint8(byteValue))
		if pageNumber > 0 {
			// Load this page into cache if not already there
			_, err := db.getRadixPage(pageNumber)
			if err != nil {
				// If it's not a radix page, just skip it
				if strings.Contains(err.Error(), "not a radix page") {
					continue
				}
				return fmt.Errorf("failed to preload radix page %d: %w", pageNumber, err)
			}
		}
	}

	return nil
}

// initializeRadixLevels initializes the first two levels of the radix tree
func (db *DB) initializeRadixLevels() error {
	// Get the root radix page (page 1)
	rootRadixPage, err := db.getRootRadixPage()
	if err != nil {
		return fmt.Errorf("failed to get root radix page: %w", err)
	}

	// First level: Mark the root page as having one sub-page used
	rootRadixPage.SubPagesUsed = 1
	rootRadixPage.dirty = true

	// Create root sub-page
	rootSubPage := &RadixSubPage{
		Page:       rootRadixPage,
		SubPageIdx: 0,
	}

	// For the first 256 possible values in the first byte, create entries in the radix tree
	for byteValue := 0; byteValue < 256; byteValue++ {
		// Allocate a new sub-page for this byte value
		childSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			return fmt.Errorf("failed to allocate sub-page for byte %d: %w", byteValue, err)
		}

		// Link from root page to this page/sub-page
		err = db.setRadixEntry(rootSubPage, uint8(byteValue), childSubPage.Page.pageNumber, childSubPage.SubPageIdx)
		if err != nil {
			return fmt.Errorf("failed to set radix entry for byte %d: %w", byteValue, err)
		}
	}

	return nil
}

// convertLeafPageToRadixPage converts a leaf page to a radix page when it's full
// It creates a new radix page with the same page number as the leaf page,
// and redistributes all entries from the leaf page to appropriate new leaf pages
func (db *DB) convertLeafPageToRadixPage(leafPage *LeafPage, newSuffix []byte, newDataOffset int64) error {
	debugPrint("Converting leaf page %d to radix page\n", leafPage.pageNumber)

	// Create a new radix page with the same page number
	radixPage := &RadixPage{
		pageNumber:   leafPage.pageNumber,
		pageType:     ContentTypeRadix,
		data:         make([]byte, PageSize),
		dirty:        true,
		isWAL:        false,
		accessTime:   leafPage.accessTime,
		txnSequence:  leafPage.txnSequence,
		SubPagesUsed: 1, // Start with one sub-page
	}

	// Update the cache to replace the leaf page with the radix page
	db.addToCache(radixPage)

	// Since we're only using one sub-page initially, add this to the free list
	db.addToFreeSubPagesList(radixPage)

	// Create a pointer to the radix sub-page
	radixSubPage := &RadixSubPage{
		Page:       radixPage,
		SubPageIdx: 0,
	}

	// Add the new entry to the collection of entries we need to redistribute
	entries := append(leafPage.Entries, LeafEntry{
		Suffix:     newSuffix,
		DataOffset: newDataOffset,
	})

	// Process all entries from the leaf page plus the new entry
	for _, entry := range entries {
		// Add it to the newly created radix sub-page
		// The first byte of the suffix determines which branch to take
		if err := db.setContentOnIndex(radixSubPage, entry.Suffix, 0, entry.DataOffset); err != nil {
			return fmt.Errorf("failed to convert leaf page to radix page: %w", err)
		}
	}

	// Mark the radix page as dirty
	radixPage.dirty = true

	return nil
}

// addToFreeSubPagesList adds a radix page with free sub-pages to the list
func (db *DB) addToFreeSubPagesList(radixPage *RadixPage) {
	// Only add if the page has free sub-pages
	if radixPage.SubPagesUsed >= SubPagesPerRadixPage {
		return
	}

	// Don't add if it's already the head of the list
	if db.freeSubPagesHead != nil && db.freeSubPagesHead.pageNumber == radixPage.pageNumber {
		return
	}

	// Link this page to the current head
	if db.freeSubPagesHead != nil {
		radixPage.NextFreePage = db.freeSubPagesHead.pageNumber
	} else {
		radixPage.NextFreePage = 0
	}

	// Mark the page as dirty
	radixPage.dirty = true

	// Make it the new head
	db.updateFreeSubPagesHead(radixPage)
}

// updateFreeSubPagesHead updates the in-memory pointer to the head of the free sub-pages list
// The change will be written to the index header during the next Sync operation
func (db *DB) updateFreeSubPagesHead(radixPage *RadixPage) {
	// Don't update if database is in read-only mode
	if db.readOnly {
		return
	}

	// Update the in-memory pointer
	db.freeSubPagesHead = radixPage

	// Mark the header as dirty so it gets written during the next sync
	db.headerDirty = true
}

// recoverUnindexedContent reads the main file starting from the last indexed offset
// and reindexes any content that hasn't been indexed yet
func (db *DB) recoverUnindexedContent() error {

	lastIndexedOffset := db.lastIndexedOffset

	// If the last indexed offset is 0, start from the beginning (after header)
	if lastIndexedOffset < int64(PageSize) {
		lastIndexedOffset = int64(PageSize)
	}

	// If the last indexed offset is already at the end of the file, nothing to do
	if lastIndexedOffset >= db.mainFileSize {
		return nil
	}

	debugPrint("Recovering unindexed content from offset %d to %d\n", lastIndexedOffset, db.mainFileSize)

	// Get the root radix sub-page
	rootSubPage, err := db.getRootRadixSubPage()
	if err != nil {
		return fmt.Errorf("failed to get root radix sub-page: %w", err)
	}

	// Start reading from the last indexed offset
	currentOffset := lastIndexedOffset

	// Process all content until we reach the end of the file
	for currentOffset < db.mainFileSize {
		// Read the content at the current offset
		content, err := db.readContent(currentOffset)
		if err != nil {
			return fmt.Errorf("failed to read content at offset %d: %w", currentOffset, err)
		}

		// Only process data content
		if content.data[0] == ContentTypeData {
			// Set the key-value pair on the index
			err := db.setKvOnIndex(rootSubPage, content.key, content.value, currentOffset)
			if err != nil {
				return fmt.Errorf("failed to set kv on index: %w", err)
			}
		}

		// Move to the next content
		currentOffset += int64(len(content.data))
	}

	if !db.readOnly {
		// Flush the index pages to disk
		if err := db.flushIndexToDisk(); err != nil {
			return fmt.Errorf("failed to flush index to disk: %w", err)
		}
	}

	debugPrint("Recovery complete, reindexed content up to offset %d\n", db.mainFileSize)
	return nil
}
