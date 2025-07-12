package kv_log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aergoio/kv_log/varint"
)

const (
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
	RadixHeaderSize = 10  // ContentType(1) + SubPagesUsed(1) + NextFreePage(4) + Checksum(4)
	// Leaf page header size
	LeafHeaderSize = 8    // ContentType(1) + Unused(1) + ContentSize(2) + Checksum(4)
	// Number of sub-pages per radix page
	SubPagesPerRadixPage = 3
	// Size of each radix entry (page number + sub-page index)
	RadixEntrySize = 5 // 4 bytes page number + 1 byte sub-page index
	// Number of entries in each sub-page
	EntriesPerSubPage = 256
	// Size of each radix sub-page including the empty suffix offset
	RadixSubPageSize = EntriesPerSubPage * RadixEntrySize + 8 // 256 entries * 5 bytes + 8 bytes for empty suffix offset

	// Leaf sub-page header size
	LeafSubPageHeaderSize = 3 // SubPageID(1) + SubPageSize(2)
)

// Content types
const (
	ContentTypeData   = 'D' // Data content type
	ContentTypeCommit = 'C' // Commit marker type
	ContentTypeRadix  = 'R' // Radix page type
	ContentTypeLeaf   = 'L' // Leaf page type
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

// cacheBucket represents a bucket in the page cache with its own mutex
type cacheBucket struct {
    mutex sync.RWMutex
    pages map[uint32]*Page  // Map of page numbers to pages
}

// DB represents the database instance
type DB struct {
	databaseID     uint64 // Unique identifier for the database
	filePath       string
	mainFile       *os.File
	indexFile      *os.File
	mutex          sync.RWMutex  // Mutex for the database
	seqMutex       sync.Mutex    // Mutex for transaction state and sequence numbers
	mainFileSize   int64 // Track main file size to avoid frequent stat calls
	indexFileSize  int64 // Track index file size to avoid frequent stat calls
	prevFileSize   int64 // Track main file size before the current transaction started
	flushFileSize  int64 // Track main file size for flush operations
	fileLocked     bool  // Track if the files are locked
	lockType       int   // Type of lock currently held
	readOnly       bool  // Track if the database is opened in read-only mode
	pageCache      [1024]cacheBucket // Page cache for all page types
	totalCachePages atomic.Int64     // Total number of pages in cache (including previous versions)
	freeRadixPagesHead uint32 // Page number of the head of linked list of radix pages with available sub-pages
	freeLeafPagesHead  uint32 // Page number of the head of linked list of leaf pages with available space
	lastIndexedOffset int64 // Track the offset of the last indexed content in the main file
	writeMode      string // Current write mode
	nextWriteMode  string // Next write mode to apply
	commitMode     int    // CallerThread or WorkerThread
	useWAL         bool   // Whether to use WAL or not
	syncMode       int    // SyncOn or SyncOff
	walInfo        *WalInfo // WAL file information
	inTransaction  bool   // Track if inside of a transaction
	inExplicitTransaction bool // Track if an explicit transaction is open
	calledByTransaction bool // Track if the method was called by a transaction
	txnSequence    int64  // Current transaction sequence number
	flushSequence  int64  // Current flush up to this transaction sequence number
	maxReadSequence int64 // Maximum transaction sequence number that can be read
	pruningSequence int64 // Last transaction sequence number when cache pruning was performed
	txnChecksum    uint32 // Running CRC32 checksum for current transaction
	accessCounter  uint64 // Counter for page access times
	dirtyPageCount int    // Count of dirty pages in cache
	cacheSizeThreshold int // Maximum number of pages in cache before cleanup
	dirtyPageThreshold int // Maximum number of dirty pages before flush
	checkpointThreshold int64 // Maximum WAL file size in bytes before checkpoint
	workerChannel  chan string // Channel for background worker commands
	workerWaitGroup sync.WaitGroup // WaitGroup to coordinate with worker thread
	pendingCommands map[string]bool // Map to track pending worker commands
	originalLockType int // Original lock type before transaction
	lockAcquiredForTransaction bool // Whether lock was acquired for transaction

	// Add a condition variable for transaction waiting
	transactionCond *sync.Cond
}

// Transaction represents a database transaction
type Transaction struct {
	db *DB
	txnSequence int64
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
	ContentSize  uint16             // Total size of content on this page
	SubPages     []*LeafSubPageInfo // Information about sub-pages in this leaf page (nil entries for unused IDs)
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

// LeafSubPage represents a reference to a specific sub-page within a leaf page
type LeafSubPage struct {
	Page      *LeafPage  // Pointer to the parent leaf page
	SubPageIdx uint8     // Index of the sub-page within the parent page
}

// LeafSubPageInfo stores metadata and entries for a leaf sub-page
type LeafSubPageInfo struct {
	Offset  uint16      // Offset in the page data where the sub-page starts
	Size    uint16      // Size of the sub-page data (excluding header)
	Entries []LeafEntry // Entries in this sub-page
}

// LeafEntry represents an entry in a leaf page
type LeafEntry struct {
	SuffixOffset int    // Offset in the data buffer where the suffix starts
	SuffixLen    int    // Length of the suffix
	DataOffset   int64  // Offset in the main file where the data starts
}

// Options represents configuration options for the database
type Options map[string]interface{}

// DebugMode controls whether debug prints are enabled
var DebugMode bool

// init initializes package-level variables
func init() {
	// Check for debug mode environment variable
	debugEnv := os.Getenv("KV_LOG_DEBUG")
	// Any non-empty value enables debug mode
	if debugEnv != "" {
		DebugMode = true
	}
}

// debugPrint prints a message if debug mode is enabled
func debugPrint(format string, args ...interface{}) {
	if DebugMode {
		fmt.Printf(format, args...)
	}
}

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
	cacheSizeThreshold := calculateDefaultCacheSize()  // Calculate based on system memory
	dirtyPageThreshold := cacheSizeThreshold / 2       // Default to 50% of cache size
	checkpointThreshold := int64(1024 * 1024)          // Default to 1MB

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
		/*
		if val, ok := opts["WriteMode"]; ok {
			if jm, ok := val.(string); ok {
				if jm == CallerThread_WAL_Sync || jm == CallerThread_WAL_NoSync || jm == WorkerThread_WAL || jm == WorkerThread_NoWAL || jm == WorkerThread_NoWAL_NoSync {
					writeMode = jm
				} else {
					return nil, fmt.Errorf("invalid value for WriteMode option")
				}
			}
		}
		*/
		if val, ok := opts["CacheSizeThreshold"]; ok {
			if cst, ok := val.(int); ok && cst > 0 {
				cacheSizeThreshold = cst
			}
		}
		if val, ok := opts["DirtyPageThreshold"]; ok {
			if dpt, ok := val.(int); ok && dpt > 0 {
				dirtyPageThreshold = dpt
			}
		}
		if val, ok := opts["CheckpointThreshold"]; ok {
			if cpt, ok := val.(int64); ok && cpt > 0 {
				checkpointThreshold = cpt
			} else if cpt, ok := val.(int); ok && cpt > 0 {
				checkpointThreshold = int64(cpt)
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
		databaseID:         0, // Will be set on read or initialize
		filePath:           path,
		mainFile:           mainFile,
		indexFile:          indexFile,
		mainFileSize:       mainFileInfo.Size(),
		indexFileSize:      indexFileInfo.Size(),
		readOnly:           readOnly,
		lockType:           LockNone,
		dirtyPageCount:     0,
		dirtyPageThreshold: dirtyPageThreshold,
		cacheSizeThreshold: cacheSizeThreshold,
		checkpointThreshold: checkpointThreshold,
		workerChannel:      make(chan string, 10), // Buffer size of 10 for commands
		pendingCommands:    make(map[string]bool), // Initialize the pending commands map
	}

	// Initialize each bucket's map
	for i := range db.pageCache {
		db.pageCache[i].pages = make(map[uint32]*Page)
	}

	// Initialize the total cache pages counter
	db.totalCachePages.Store(0)

	// Initialize the transaction condition variable
	db.transactionCond = sync.NewCond(&db.mutex)

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

	// Ensure txnSequence starts at 1
	db.txnSequence = 1

	// Start the background worker if not in read-only mode and using worker thread mode
	if !db.readOnly && db.commitMode == WorkerThread {
		db.startBackgroundWorker()
	}

	// Set a finalizer to close the database if it is not closed
	runtime.SetFinalizer(db, func(d *DB) {
		_ = d.Close()
	})

	return db, nil
}

// SetOption sets a database option after the database is open
func (db *DB) SetOption(name string, value interface{}) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	switch name {
	/*
	case "WriteMode":
		if jm, ok := value.(string); ok {
			if jm == CallerThread_WAL_Sync || jm == CallerThread_WAL_NoSync || jm == WorkerThread_WAL || jm == WorkerThread_NoWAL || jm == WorkerThread_NoWAL_NoSync {
				db.nextWriteMode = jm
				return nil
			}
			return fmt.Errorf("invalid value for WriteMode option")
		}
		return fmt.Errorf("WriteMode option value must be a string")
	*/
	case "CacheSizeThreshold":
		if cst, ok := value.(int); ok {
			if cst > 0 {
				db.cacheSizeThreshold = cst
				return nil
			}
			return fmt.Errorf("CacheSizeThreshold must be greater than 0")
		}
		return fmt.Errorf("CacheSizeThreshold value must be an integer")
	case "DirtyPageThreshold":
		if dpt, ok := value.(int); ok {
			if dpt > 0 {
				db.dirtyPageThreshold = dpt
				return nil
			}
			return fmt.Errorf("DirtyPageThreshold must be greater than 0")
		}
		return fmt.Errorf("DirtyPageThreshold value must be an integer")
	case "CheckpointThreshold":
		if cpt, ok := value.(int64); ok {
			if cpt > 0 {
				db.checkpointThreshold = cpt
				return nil
			}
			return fmt.Errorf("CheckpointThreshold must be greater than 0")
		}
		// Try to convert from int if int64 conversion failed
		if cpt, ok := value.(int); ok {
			if cpt > 0 {
				db.checkpointThreshold = int64(cpt)
				return nil
			}
			return fmt.Errorf("CheckpointThreshold must be greater than 0")
		}
		return fmt.Errorf("CheckpointThreshold value must be an integer")
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

	// Check if already closed
	if db.mainFile == nil && db.indexFile == nil {
		return nil // Already closed
	}

	if !db.readOnly {
		// If using worker thread mode
		if db.commitMode == WorkerThread {
			// Signal the worker thread to flush the index to disk, even if
			// a flush is already running (to flush the remaining pages)
			db.workerChannel <- "flush"

			// Signal the worker thread to exit
			db.workerChannel <- "exit"

			// Wait for the worker thread to finish
			db.workerWaitGroup.Wait()

			// Close the channel
			close(db.workerChannel)
		} else {
			// Flush the index to disk
			flushErr = db.flushIndexToDisk()
		}
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
		db.mainFile = nil
	}

	// Close index file if open
	if db.indexFile != nil {
		indexErr = db.indexFile.Close()
		db.indexFile = nil
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
	if db.inExplicitTransaction && !db.calledByTransaction {
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
	if !db.inExplicitTransaction {
		db.beginTransaction()
	}

	// Set the key-value pair
	err := db.set(key, value)

	// Commit or rollback the transaction if not in an explicit transaction
	if !db.inExplicitTransaction {
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

	// Start with the root radix sub-page
	rootSubPage, err := db.getRootRadixSubPage()
	if err != nil {
		return fmt.Errorf("failed to get root radix sub-page: %w", err)
	}

	// Check if we're deleting (value is nil)
	isDelete := len(value) == 0

	// Process the key byte by byte
	radixSubPage := rootSubPage
	keyPos := 0

	// Traverse the radix trie until we reach a leaf page or the end of the key
	for keyPos < len(key) {
		// Get the current byte from the key
		byteValue := key[keyPos]

		// Get the next page number and sub-page index from the current sub-page
		nextPageNumber, nextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)

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
			return db.createPathForByte(radixSubPage, key, keyPos, dataOffset)
		}

		// There's an entry for this byte, load the page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %w", nextPageNumber, err)
		}

		// Check what type of page we got
		if page.pageType == ContentTypeRadix {
			// It's a radix page, continue traversing
			radixSubPage = &RadixSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			keyPos++
		} else if page.pageType == ContentTypeLeaf {
			// It's a leaf page, get the leaf sub-page
			leafSubPage := &LeafSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			// Attempt to set the key and value on the leaf sub-page
			return db.setOnLeafSubPage(radixSubPage, leafSubPage, key, keyPos, value, 0)
		} else {
			return fmt.Errorf("invalid page type")
		}
	}

	// We've processed all bytes of the key
	// Attempt to set the key and value on the empty suffix slot
	return db.setOnEmptySuffix(radixSubPage, key, value, 0)
}

// setKvOnIndex sets an existing key-value pair on the index (reindexing)
func (db *DB) setKvOnIndex(rootSubPage *RadixSubPage, key, value []byte, dataOffset int64) error {

	// Process the key byte by byte to find where to add it
	radixSubPage := rootSubPage
	keyPos := 0

	// Traverse the radix trie until we reach a leaf page or the end of the key
	for keyPos < len(key) {
		// Get the current byte from the key
		byteValue := key[keyPos]

		// Get the next page number and sub-page index from the current sub-page
		nextPageNumber, nextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)

		// If there's no entry for this byte, create a new path
		if nextPageNumber == 0 {
			// Create a path for this byte
			return db.createPathForByte(radixSubPage, key, keyPos, dataOffset)
		}

		// There's an entry for this byte, load the page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %w", nextPageNumber, err)
		}

		// Check what type of page we got
		if page.pageType == ContentTypeRadix {
			// It's a radix page, continue traversing
			radixSubPage = &RadixSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			keyPos++
		} else if page.pageType == ContentTypeLeaf {
			// It's a leaf page, get the leaf sub-page
			leafSubPage := &LeafSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			// Attempt to set the key and value on the leaf sub-page
			return db.setOnLeafSubPage(radixSubPage, leafSubPage, key, keyPos, value, dataOffset)
		} else {
			return fmt.Errorf("invalid page type")
		}
	}

	// If we've processed all bytes of the key
	// Set the content offset on the empty suffix slot
	return db.setOnEmptySuffix(radixSubPage, key, value, dataOffset)
}

// setContentOnIndex sets a suffix + content offset pair on the index
// it is used when converting a leaf page into a radix page
func (db *DB) setContentOnIndex(subPage *RadixSubPage, suffix []byte, suffixPos int, contentOffset int64) error {

	// Process the suffix byte by byte
	radixSubPage := subPage

	// Traverse the radix trie until we reach a leaf page or the end of the suffix
	for suffixPos < len(suffix) {
		// Get the current byte from the key's suffix
		byteValue := suffix[suffixPos]

		// Get the next page number and sub-page index from the current sub-page
		nextPageNumber, nextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)

		// If there's no entry for this byte, create a new path
		if nextPageNumber == 0 {
			// Create a path for this byte
			return db.createPathForByte(radixSubPage, suffix, suffixPos, contentOffset)
		}

		// There's an entry for this byte, load the page
		page, err := db.getPage(nextPageNumber)
		if err != nil {
			return fmt.Errorf("failed to load page %d: %w", nextPageNumber, err)
		}

		// Check what type of page we got
		if page.pageType == ContentTypeRadix {
			// It's a radix page, continue traversing
			radixSubPage = &RadixSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}
			suffixPos++
		} else if page.pageType == ContentTypeLeaf {
			// It's a leaf page, get the leaf sub-page
			leafSubPage := &LeafSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}

			// The remaining part of the suffix
			remainingSuffix := suffix[suffixPos+1:]

			// Try to add the entry with the suffix to the leaf sub-page
			// If the leaf sub-page is full, it will be converted to a radix sub-page
			return db.addEntryToLeafSubPage(radixSubPage, byteValue, leafSubPage, remainingSuffix, contentOffset)
		} else {
			return fmt.Errorf("invalid page type")
		}
	}

	// We've processed all bytes of the key
	// Set the content offset on the empty suffix slot
	return db.setEmptySuffixOffset(radixSubPage, contentOffset)
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

		// If the above function cloned the page, update the subPage pointer
		if childSubPage.Page.pageNumber == subPage.Page.pageNumber {
			subPage.Page = childSubPage.Page
		}

		// Update the radix entry to point to the new radix page
		err = db.setRadixEntry(subPage, byteValue, childSubPage.Page.pageNumber, childSubPage.SubPageIdx)
		if err != nil {
			return fmt.Errorf("failed to set radix entry for byte %d: %w", byteValue, err)
		}
	} else {
		// Non-empty suffix
		// Add the entry with the suffix to a new leaf sub-page
		leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, dataOffset)
		if err != nil {
			return fmt.Errorf("failed to add leaf entry: %w", err)
		}

		// Update the subPage pointer, because the above function
		// could have cloned the same radix page used on this subPage
		subPage.Page, _ = db.getRadixPage(subPage.Page.pageNumber)

		// Update the radix entry to point to the new leaf page and sub-page
		err = db.setRadixEntry(subPage, byteValue, leafSubPage.Page.pageNumber, leafSubPage.SubPageIdx)
		if err != nil {
			return fmt.Errorf("failed to set radix entry for byte %d: %w", byteValue, err)
		}
	}

	// Don't write to disk, just keep pages in cache
	return nil
}

// setOnLeafSubPage attempts to set a key-value pair on an existing leaf sub-page
// If dataOffset is 0, we're setting a new key-value pair
// Otherwise, it means we're reindexing already stored key-value pair
func (db *DB) setOnLeafSubPage(parentSubPage *RadixSubPage, subPage *LeafSubPage, key []byte, keyPos int, value []byte, dataOffset int64) error {
	var err error
	leafPage := subPage.Page
	subPageIdx := subPage.SubPageIdx

	// Extract the byte value that led us to this leaf sub-page
	parentByteValue := key[keyPos]

	// Check if we're deleting
	isDelete := len(value) == 0

	// The remaining part of the key is the suffix
	suffix := key[keyPos+1:]

	// Get the sub-page info
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx] == nil {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := leafPage.SubPages[subPageIdx]

	// Search for the suffix in this sub-page
	for entryIdx, entry := range subPageInfo.Entries {
		// Get the suffix from the entry (using sub-page base + relative offset)
		subPageDataStart := int(subPageInfo.Offset) + 3 // Skip 3-byte header
		suffixOffset := subPageDataStart + entry.SuffixOffset
		entrySuffix := leafPage.data[suffixOffset:suffixOffset+entry.SuffixLen]
		// Compare it with the given suffix
		if bytes.Equal(entrySuffix, suffix) {
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

				// Remove this entry from the sub-page
				return db.removeEntryFromLeafSubPage(subPage, entryIdx)
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

			// Update the entry's data offset in the sub-page
			err = db.updateEntryInLeafSubPage(subPage, entryIdx, dataOffset)
			if err != nil {
				return fmt.Errorf("failed to update entry in sub-page: %w", err)
			}

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

	// Try to add the entry with the suffix to this sub-page
	// If the leaf sub-page is full, it will be converted to a radix sub-page
	return db.addEntryToLeafSubPage(parentSubPage, parentByteValue, subPage, suffix, dataOffset)
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

	// Determine the maximum transaction sequence number that can be read
	var maxReadSequence int64
	db.seqMutex.Lock()
	if db.calledByTransaction || !db.inTransaction {
		maxReadSequence = db.txnSequence
	} else {
		maxReadSequence = db.txnSequence - 1
	}
	db.seqMutex.Unlock()

	// Start with the root radix sub-page
	rootSubPage, err := db.getRootRadixSubPage(maxReadSequence)
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
		page, err := db.getPage(nextPageNumber, maxReadSequence)
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
			leafSubPage := &LeafSubPage{
				Page:       page,
				SubPageIdx: nextSubPageIdx,
			}

			// The remaining part of the key is the suffix
			suffix := key[keyPos+1:]

			// Get the sub-page info
			subPageInfo := leafSubPage.Page.SubPages[leafSubPage.SubPageIdx]
			if subPageInfo == nil {
				return nil, fmt.Errorf("sub-page with index %d not found", leafSubPage.SubPageIdx)
			}

			// Search for the suffix in this sub-page's entries
			for _, entry := range subPageInfo.Entries {
				// Get the suffix from the entry (using sub-page base + relative offset)
				subPageDataStart := int(subPageInfo.Offset) + 3 // Skip 3-byte header
				suffixOffset := subPageDataStart + entry.SuffixOffset
				entrySuffix := leafPage.data[suffixOffset:suffixOffset+entry.SuffixLen]
				// Compare it with the given suffix
				if bytes.Equal(entrySuffix, suffix) {
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

	// Save this page number as the head of the free sub-pages list
	db.freeRadixPagesHead = rootRadixPage.pageNumber

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

	// Read the index file header directly from the index file
	if err := db.readIndexFileHeader(false); err != nil {
		return fmt.Errorf("failed to read index file header: %w", err)
	}

	// Check for existing WAL file if in WAL mode
	if db.useWAL {
		// Open existing WAL file if it exists
		if err := db.openWAL(); err != nil {
			return fmt.Errorf("failed to open WAL file: %w", err)
		}
	}

	// Read the index file header from WAL/cache
	// This will update lastIndexedOffset and freePageNum with the latest values
	if err := db.readIndexFileHeader(true); err != nil {
		return fmt.Errorf("failed to read index file header from WAL: %w", err)
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
func (db *DB) readIndexFileHeader(finalRead bool) error {
	var header []byte
	var err error

	if finalRead {
		// Try to get the header page from the cache first (which includes pages from the WAL)
		headerPage, exists := db.getFromCache(0)
		if exists && headerPage != nil {
			header = headerPage.data
		}
	}
	// If not using WAL or if the page is not in cache
	if header == nil {
		// Read directly from the index file
		header, err = db.readFromIndexFile(0)
		if err != nil {
			return fmt.Errorf("failed to read index file header: %w", err)
		}
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

	// Only process lastIndexedOffset and freePageNum on the final read
	if finalRead {
		// Parse the last indexed offset from the header
		db.lastIndexedOffset = int64(binary.LittleEndian.Uint64(header[16:24]))
		if db.lastIndexedOffset == 0 {
			// If the last indexed offset is 0, default to PageSize
			db.lastIndexedOffset = PageSize
		}

		// Parse the free radix pages head pointer
		freeRadixPageNum := binary.LittleEndian.Uint32(header[24:28])
		// Parse the free leaf pages head pointer
		freeLeafPageNum := binary.LittleEndian.Uint32(header[28:32])

		// If we have a valid free radix page pointer
		if freeRadixPageNum > 0 {
			radixPage, err := db.getRadixPage(freeRadixPageNum)
			if err != nil {
				return fmt.Errorf("failed to get radix page: %w", err)
			}
			db.freeRadixPagesHead = radixPage.pageNumber
		}

		// If we have a valid free leaf page pointer
		if freeLeafPageNum > 0 {
			leafPage, err := db.getLeafPage(freeLeafPageNum)
			if err != nil {
				return fmt.Errorf("failed to get leaf page: %w", err)
			}
			db.freeLeafPagesHead = leafPage.pageNumber
		}
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
		lastIndexedOffset = db.flushFileSize
	}

	// The page number of the next free radix page
	nextFreeRadixPageNumber := uint32(0)
	if !isInit && db.freeRadixPagesHead > 0 {
		nextFreeRadixPageNumber = db.freeRadixPagesHead
	}

	// The page number of the next free leaf page
	nextFreeLeafPageNumber := uint32(0)
	if !isInit && db.freeLeafPagesHead > 0 {
		nextFreeLeafPageNumber = db.freeLeafPagesHead
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

	// Set free radix pages head pointer (4 bytes)
	binary.LittleEndian.PutUint32(data[24:28], nextFreeRadixPageNumber)

	// Set free leaf pages head pointer (4 bytes)
	binary.LittleEndian.PutUint32(data[28:32], nextFreeLeafPageNumber)

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

	// Update the running transaction checksum
	db.txnChecksum = crc32.Update(db.txnChecksum, crc32.IEEETable, content)

	// Update the file size
	db.mainFileSize += int64(totalSize)

	debugPrint("Appended content at offset %d, size %d\n", fileSize, totalSize)

	// Return the offset where the content was written
	return fileSize, nil
}

// appendCommitMarker appends a commit marker to the end of the main file
// The commit marker consists of:
// - 1 byte: ContentTypeCommit ('C')
// - 4 bytes: CRC32 checksum of all transaction data since the last commit
func (db *DB) appendCommitMarker() error {
	// Use the running transaction checksum
	checksum := db.txnChecksum

	// Prepare the commit marker buffer (1 byte type + 4 bytes checksum)
	commitMarker := make([]byte, 5)
	commitMarker[0] = ContentTypeCommit
	binary.BigEndian.PutUint32(commitMarker[1:5], checksum)

	// Write the commit marker to the end of the file
	if _, err := db.mainFile.Write(commitMarker); err != nil {
		return fmt.Errorf("failed to write commit marker: %w", err)
	}

	// Update the file size
	db.mainFileSize += 5

	// Reset the transaction checksum for the next transaction
	db.txnChecksum = 0

	debugPrint("Appended commit marker at offset %d with checksum %d\n", db.mainFileSize-5, checksum)

	return nil
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
		if keyLength64 > MaxKeyLength {
			return nil, fmt.Errorf("key length exceeds maximum allowed size: %d", keyLength64)
		}
		keyLength := int(keyLength64)

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
	} else if contentType == ContentTypeCommit {
		// Read commit marker (1 byte type + 4 bytes checksum)
		buffer := make([]byte, 5)
		n, err := db.mainFile.ReadAt(buffer, offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read commit marker: %w", err)
		}
		if n < 5 {
			return nil, fmt.Errorf("incomplete commit marker")
		}

		// Store the commit marker data
		content.data = buffer
	} else {
		return nil, fmt.Errorf("unknown content type on main file: %c", contentType)
	}

	return content, nil
}

// ------------------------------------------------------------------------------------------------
// Header page
// ------------------------------------------------------------------------------------------------

func (db *DB) parseHeaderPage(data []byte) (*Page, error) {

	// Just store the data on the cache
	headerPage := &Page{
		pageNumber: 0,
		data:       data,
	}

	// Update the access time
	db.accessCounter++   // TODO: not sure if this is correct for page 0
	headerPage.accessTime = db.accessCounter

	// Add to cache
	db.addToCache(headerPage)

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
	storedChecksum := binary.BigEndian.Uint32(data[6:10])
	// Zero out the checksum field for calculation
	binary.BigEndian.PutUint32(data[6:10], 0)
	// Calculate the checksum
	calculatedChecksum := crc32.ChecksumIEEE(data)
	// Restore the original checksum in the data
	binary.BigEndian.PutUint32(data[6:10], storedChecksum)
	// Verify the checksum
	if storedChecksum != calculatedChecksum {
		return nil, fmt.Errorf("radix page checksum mismatch at page %d: stored=%d, calculated=%d", pageNumber, storedChecksum, calculatedChecksum)
	}

	// Read the sub-pages used
	subPagesUsed := data[1]

	// Read the next free page number
	nextFreePage := binary.LittleEndian.Uint32(data[2:6])

	// Create structured radix page
	radixPage := &RadixPage{
		pageNumber:   pageNumber,
		pageType:     ContentTypeRadix,
		data:         data,
		dirty:        false,
		SubPagesUsed: subPagesUsed,
		NextFreePage: nextFreePage,
	}

	// Update the access time
	db.accessCounter++
	radixPage.accessTime = db.accessCounter

	// Add to cache
	db.addToCache(radixPage)

	return radixPage, nil
}

// writeRadixPage writes a radix page to the database file
func (db *DB) writeRadixPage(radixPage *RadixPage) error {
	// Set page type in the data
	radixPage.data[0] = ContentTypeRadix  // Type identifier
	radixPage.data[1] = radixPage.SubPagesUsed // The number of sub-pages used

	// Store the NextFreePage field at bytes 2-5
	binary.LittleEndian.PutUint32(radixPage.data[2:6], radixPage.NextFreePage)

	// Calculate CRC32 checksum for the page data (excluding the checksum field itself)
	// Zero out the checksum field before calculating
	binary.BigEndian.PutUint32(radixPage.data[6:10], 0)
	// Calculate checksum of the entire page
	checksum := crc32.ChecksumIEEE(radixPage.data)
	// Write the checksum at position 6
	binary.BigEndian.PutUint32(radixPage.data[6:10], checksum)

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
		SubPages:    make([]*LeafSubPageInfo, 256),
	}

	// Parse sub-page offsets and entries
	if err := db.parseLeafSubPages(leafPage); err != nil {
		return nil, fmt.Errorf("failed to parse leaf sub-pages: %w", err)
	}

	// Update the access time
	db.accessCounter++
	leafPage.accessTime = db.accessCounter

	// Add to cache
	db.addToCache(leafPage)

	return leafPage, nil
}

// parseLeafSubPages parses the sub-pages in a leaf page
func (db *DB) parseLeafSubPages(leafPage *LeafPage) error {
	// Start at header size
	pos := int(LeafHeaderSize)

	// If there are no sub-pages, nothing to do
	if leafPage.ContentSize == LeafHeaderSize {
		return nil
	}

	// Create array with 256 entries to handle any sub-page ID (0-255)
	leafPage.SubPages = make([]*LeafSubPageInfo, 256)

	// Read each sub-page until we reach the content size
	for pos < int(leafPage.ContentSize) {
		// Read sub-page ID (this is the index where we'll store this sub-page)
		subPageID := leafPage.data[pos]
		pos++

		// Read sub-page size
		subPageSize := binary.LittleEndian.Uint16(leafPage.data[pos:pos+2])
		pos += 2

		// Calculate the end of this sub-page
		subPageEnd := pos + int(subPageSize)

		// Ensure we don't read past the content size
		if subPageEnd > int(leafPage.ContentSize) {
			return fmt.Errorf("sub-page end %d exceeds content size %d", subPageEnd, leafPage.ContentSize)
		}

		// Parse entries in this sub-page
		subPageEntries, err := db.parseLeafSubPageEntries(leafPage, pos, subPageEnd)
		if err != nil {
			return fmt.Errorf("failed to parse sub-page %d entries: %w", subPageID, err)
		}

		// Create and store the sub-page info at the index corresponding to its ID
		leafPage.SubPages[subPageID] = &LeafSubPageInfo{
			Offset:  uint16(pos - 3), // Include the header in the offset
			Size:    subPageSize,
			Entries: subPageEntries,
		}

		// Move to the next sub-page
		pos = subPageEnd
	}

	return nil
}

// parseLeafSubPageEntries parses the entries in a leaf sub-page
func (db *DB) parseLeafSubPageEntries(leafPage *LeafPage, start, end int) ([]LeafEntry, error) {
	var entries []LeafEntry
	pos := start

	// Read entries until we reach the end of the sub-page
	for pos < end {
		// Read suffix length
		suffixLen64, bytesRead := varint.Read(leafPage.data[pos:])
		if bytesRead == 0 {
			return nil, fmt.Errorf("failed to read suffix length")
		}
		suffixLen := int(suffixLen64)
		pos += bytesRead

		// Store the suffix offset relative to the sub-page data start
		suffixOffset := pos - start
		pos += suffixLen

		// Check if we have enough space for the data offset
		if pos+8 > end {
			return nil, fmt.Errorf("insufficient space for data offset")
		}

		// Read data offset
		dataOffset := int64(binary.LittleEndian.Uint64(leafPage.data[pos:]))
		pos += 8

		// Add entry to list
		entries = append(entries, LeafEntry{
			SuffixOffset: suffixOffset, // Relative to sub-page data start
			SuffixLen:    suffixLen,
			DataOffset:   dataOffset,
		})
	}

	return entries, nil
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

	debugPrint("Writing leaf page to index file at page %d\n", leafPage.pageNumber)

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
	data, err := db.readFromIndexFile(pageNumber)
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
		db.markPageClean(page)
		// If using WAL, mark it as part of the WAL
		if db.useWAL {
			page.isWAL = true
		}
		// Discard previous versions of this page
		bucket := &db.pageCache[page.pageNumber & 1023]
		bucket.mutex.Lock()
		page.next = nil
		bucket.mutex.Unlock()
	}

	return err
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

	// Wait if a transaction is already open
	for db.inExplicitTransaction {
		db.transactionCond.Wait()
	}

	// Mark transaction as open
	db.inExplicitTransaction = true

	// Start a transaction
	db.beginTransaction()

	// Create the transaction object
	tx := &Transaction{db: db, txnSequence: db.txnSequence}

	// Set a finalizer to rollback the transaction if it is not committed or rolled back
	runtime.SetFinalizer(tx, func(t *Transaction) {
		t.db.mutex.Lock()
		defer t.db.mutex.Unlock()
		if t.db.inExplicitTransaction && t.txnSequence == t.db.txnSequence {
			_ = t.rollback()
		}
	})

	// Return the transaction object
	return tx, nil
}

// Commit a transaction
func (tx *Transaction) Commit() error {
	tx.db.mutex.Lock()
	defer tx.db.mutex.Unlock()

	// Check if transaction is open
	if !tx.db.inExplicitTransaction {
		return fmt.Errorf("no transaction is open")
	}

	// Commit the transaction
	tx.db.commitTransaction()

	// Mark transaction as closed
	tx.db.inExplicitTransaction = false

	// Signal waiting transactions
	tx.db.transactionCond.Signal()

	return nil
}

// Rollback a transaction
func (tx *Transaction) Rollback() error {
	tx.db.mutex.Lock()
	defer tx.db.mutex.Unlock()

	return tx.rollback()
}

// rollback is the internal rollback function that assumes the mutex is already held
func (tx *Transaction) rollback() error {
	// Check if transaction is open
	if !tx.db.inExplicitTransaction {
		return fmt.Errorf("no transaction is open")
	}

	// Rollback the transaction
	tx.db.rollbackTransaction()

	// Mark transaction as closed
	tx.db.inExplicitTransaction = false

	// Signal waiting transactions
	tx.db.transactionCond.Signal()

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
	// Set the flag to indicate this is called from a transaction
	tx.db.calledByTransaction = true
	value, err := tx.db.Get(key)
	tx.db.calledByTransaction = false
	return value, err
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

	// If not already exclusively locked, acquire write lock for transaction
	if db.lockType != LockExclusive {
		// Remember the original lock type
		db.originalLockType = db.lockType
		// Acquire a write lock
		if err := db.acquireWriteLock(); err != nil {
			// This is a critical error that should be handled by the caller
			panic(fmt.Sprintf("failed to acquire write lock for transaction: %v", err))
		}
		db.lockAcquiredForTransaction = true
	}

	db.seqMutex.Lock()

	// Mark the database as in a transaction
	db.inTransaction = true

	// Increment the transaction sequence number (to track the pages used in the transaction)
	db.txnSequence++

	// Store the current main file size to enable rollback (to truncate the main file)
	db.prevFileSize = db.mainFileSize

	// Reset the transaction checksum
	db.txnChecksum = 0

	db.seqMutex.Unlock()

	debugPrint("Beginning transaction %d\n", db.txnSequence)
}

// commitTransaction commits the current transaction
func (db *DB) commitTransaction() {
	debugPrint("Committing transaction %d\n", db.txnSequence)

	// Write commit marker to the main file if data was written in this transaction
	if !db.readOnly && db.mainFileSize > db.prevFileSize {
		if err := db.appendCommitMarker(); err != nil {
			debugPrint("Failed to write commit marker: %v\n", err)
			// Continue with commit even if marker fails
		}
	}

	// Release transaction lock if it was acquired for this transaction
	if db.lockAcquiredForTransaction {
		if err := db.releaseWriteLock(db.originalLockType); err != nil {
			debugPrint("Failed to release transaction lock: %v\n", err)
		}
		db.lockAcquiredForTransaction = false
	}

	db.seqMutex.Lock()
	db.inTransaction = false
	db.seqMutex.Unlock()

	// If using WAL and in caller thread mode, flush to disk
	if db.useWAL && db.commitMode == CallerThread {
		db.flushIndexToDisk()
	}
}

// rollbackTransaction rolls back the current transaction
func (db *DB) rollbackTransaction() {
	debugPrint("Rolling back transaction %d\n", db.txnSequence)

	// Truncate the main db file to the stored size before the transaction started
	if db.mainFileSize > db.prevFileSize {
		err := db.mainFile.Truncate(db.prevFileSize)
		if err != nil {
			debugPrint("Failed to truncate main file: %v\n", err)
		} else {
			debugPrint("Truncated main file to size %d\n", db.prevFileSize)
		}
		// Update the in-memory file size
		db.mainFileSize = db.prevFileSize
	}

	// Discard pages from this transaction (they should be reloaded from the index file)
	db.discardNewerPages(db.txnSequence)

	// Release transaction lock if it was acquired for this transaction
	if db.lockAcquiredForTransaction {
		if err := db.releaseWriteLock(db.originalLockType); err != nil {
			debugPrint("Failed to release transaction lock: %v\n", err)
		}
		db.lockAcquiredForTransaction = false
	}

	db.seqMutex.Lock()
	db.inTransaction = false
	db.seqMutex.Unlock()

	// It could use an optimistic approach:
	// - do not clone pages for new transactions, only if there is a flush happening
	// on rollback:
	// - truncate the main db file to the stored size before the transaction started
	// - discard all dirty pages
	// - rebuild the index pages from the main db file (incremental reindexing)

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
	bucket := &db.pageCache[pageNumber & 1023]

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	// If there is already a page with the same page number
	existingPage, exists := bucket.pages[pageNumber]
	if exists {
		// Avoid linking the page to itself
		if page == existingPage {
			return
		}
		// Link the new page to the existing page
		page.next = existingPage
	} else {
		// Clear the next pointer
		page.next = nil
	}

	// Add the new page to the cache
	bucket.pages[pageNumber] = page

	// Increment the total pages counter
	db.totalCachePages.Add(1)
}

// Get a page from the cache
func (db *DB) getFromCache(pageNumber uint32) (*Page, bool) {
	bucket := &db.pageCache[pageNumber & 1023]

	bucket.mutex.RLock()
	page, exists := bucket.pages[pageNumber]
	bucket.mutex.RUnlock()

	return page, exists
}

// getPageAndCall gets a page from the cache and calls a callback/lambda function with the page while the lock is held
func (db *DB) getPageAndCall(pageNumber uint32, callback func(*cacheBucket, uint32, *Page)) {
	bucket := &db.pageCache[pageNumber & 1023]
	bucket.mutex.RLock()
	page, exists := bucket.pages[pageNumber]
	if exists {
		callback(bucket, pageNumber, page)
	}
	bucket.mutex.RUnlock()
}

// iteratePages iterates through all pages in the cache and calls a callback/lambda function with the page while the lock is held
func (db *DB) iteratePages(callback func(*cacheBucket, uint32, *Page)) {
	for bucketIdx := 0; bucketIdx < 1024; bucketIdx++ {
		bucket := &db.pageCache[bucketIdx]
		bucket.mutex.RLock()
		// Iterate through all pages in this bucket
		for pageNumber, page := range bucket.pages {
			callback(bucket, pageNumber, page)
		}
		bucket.mutex.RUnlock()
	}
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
	// If the page is not part of the current transaction, we need to clone it
	if page.txnSequence != db.txnSequence {
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
	var newPage *Page

	debugPrint("Cloning page %d\n", page.pageNumber)

	// Clone based on page type
	if page.pageType == ContentTypeRadix {
		newPage, err = db.cloneRadixPage(page)
		if err != nil {
			return nil, fmt.Errorf("failed to clone radix page: %w", err)
		}
	} else if page.pageType == ContentTypeLeaf {
		newPage, err = db.cloneLeafPage(page)
		if err != nil {
			return nil, fmt.Errorf("failed to clone leaf page: %w", err)
		}
	} else {
		return nil, fmt.Errorf("unknown page type: %c", page.pageType)
	}

	return newPage, nil
}

// markPageDirty marks a page as dirty and increments the dirty page counter
func (db *DB) markPageDirty(page *Page) {
	if !page.dirty {
		page.dirty = true
		db.dirtyPageCount++
	}
}

// markPageClean marks a page as clean and decrements the dirty page counter
func (db *DB) markPageClean(page *Page) {
	if page.dirty {
		page.dirty = false
		db.dirtyPageCount--
	}
}

// checkPageCache checks if the page cache is full and initiates a clean up or flush
// step 1: if the amount of dirty pages is above the threshold, flush it
//  if the page cache is below the threshold, return
// step 2: try to remove clean pages from the cache
// step 3: if the page cache is still above the threshold, flush it
// This function should not return an error, it can log an error and continue
func (db *DB) checkPageCache(isWrite bool) {

	// If the amount of dirty pages is above the threshold, flush them to disk
	if isWrite && db.dirtyPageCount >= db.dirtyPageThreshold {
		// If already flushed up to the current transaction, skip
		if db.inTransaction && db.flushSequence == db.txnSequence - 1 {
			return
		}

		// Check which thread should flush the pages
		if db.commitMode == CallerThread {
			// Write the pages to the WAL file
			db.flushIndexToDisk()
		} else {
			// Signal the worker thread to flush the pages, if not already signaled
			db.seqMutex.Lock()
			if !db.pendingCommands["flush"] {
				db.pendingCommands["flush"] = true
				db.workerChannel <- "flush"
			}
			db.seqMutex.Unlock()
		}
		return
	}

	// If the size of the page cache is above the threshold, remove old pages
	if db.totalCachePages.Load() >= int64(db.cacheSizeThreshold) {
		// Check if we already pruned during the current transaction
		// When just reading, the inTransaction flag is false, so we can prune
		if db.inTransaction && db.pruningSequence == db.txnSequence {
			return
		}
		// Set the pruning sequence to the current transaction sequence
		db.seqMutex.Lock()
		db.pruningSequence = db.txnSequence
		db.seqMutex.Unlock()

		// Check which thread should remove the old pages
		if db.commitMode == CallerThread {
			// Discard previous versions of pages
			numPages := db.discardOldPageVersions(true)
			// If the number of pages is still greater than the cache size threshold
			if numPages > db.cacheSizeThreshold {
				// Remove old pages from cache
				db.removeOldPagesFromCache()
			}
		} else {
			// Signal the worker thread to remove the old pages, if not already signaled
			db.seqMutex.Lock()
			if !db.pendingCommands["clean"] {
				db.pendingCommands["clean"] = true
				db.workerChannel <- "clean"
			}
			db.seqMutex.Unlock()
		}
	}

}

// discardNewerPages removes pages from the current transaction from the cache
// This function is called by the main thread when a transaction is rolled back
func (db *DB) discardNewerPages(currentSeq int64) {
	// Iterate through all pages in the cache
	db.iteratePages(func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		// Skip pages from the current transaction
		// Find the first page that's not from the current transaction
		var newHead *Page = page
		var removedCount int64 = 0

		for newHead != nil && newHead.txnSequence == currentSeq {
			// Only decrement the dirty page counter if the current page is dirty
			// and the next one isn't (to avoid incorrect counter decrements)
			if newHead.dirty && (newHead.next == nil || !newHead.next.dirty) {
				db.dirtyPageCount--
			}
			// Move to the next page
			newHead = newHead.next
			removedCount++
		}

		// Update the cache with the new head (or delete if no valid entries remain)
		if newHead != nil {
			bucket.pages[pageNumber] = newHead
		} else {
			delete(bucket.pages, pageNumber)
		}
		// Decrement the total pages counter by the number of versions removed
		if removedCount > 0 {
			db.totalCachePages.Add(-removedCount)
		}
	})
}

// discardOldPageVersions removes older versions of pages from the cache
// keepWAL: if true, keep the first WAL page, otherwise clear the isWAL flag
// returns the number of pages kept
func (db *DB) discardOldPageVersions(keepWAL bool) int {
	db.seqMutex.Lock()
	var currentTxnSeq int64
	// If the main thread is in a transaction
	if db.inTransaction {
		// Keep pages from the previous transaction (because the current one can be rolled back)
		currentTxnSeq = db.txnSequence - 1
	} else {
		// Otherwise, keep pages from the last committed transaction
		currentTxnSeq = db.txnSequence
	}
	db.seqMutex.Unlock()

	var totalPages int64 = 0

	db.iteratePages(func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		// Count and process the chain
		current := page
		var lastKept *Page = nil
		foundFirstPage := false

		for current != nil {
			totalPages++

			// Skip pages from current transaction or higher - they should not be touched
			if current.txnSequence >= currentTxnSeq {
				// If we are not keeping WAL pages, clear the isWAL flag
				if !keepWAL && current.isWAL {
					current.isWAL = false
				}
				lastKept = current
				current = current.next
				continue
			}

			shouldKeep := false
			shouldStop := false

			if keepWAL && current.isWAL {
				// Keep only the very first WAL page
				shouldKeep = true
				// After first WAL page, discard everything else
				shouldStop = true
			} else {
				if !foundFirstPage {
					// Keep only the first page before WAL pages
					foundFirstPage = true
					shouldKeep = true
				}
				if !keepWAL && current.isWAL {
					// Clear the isWAL flag
					current.isWAL = false
				}
				// Stop if we are not keeping WAL pages
				if keepWAL {
					shouldStop = false
				} else if foundFirstPage {
					shouldStop = true
				}
			}

			if shouldKeep {
				// Keep this page
				lastKept = current
			} else {
				// Discard this page
				lastKept.next = current.next
				totalPages--
			}

			if shouldStop {
				// Discard everything after this page
				lastKept.next = nil
				// Stop processing
				break
			}

			current = current.next
		}
	})

	// Update the atomic counter with the accurate count from this function
	db.totalCachePages.Store(totalPages)

	return int(totalPages)
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

	// Get total pages in cache from atomic counter
	totalPages := int(db.totalCachePages.Load())

	// If cache is empty or too small, nothing to do
	if totalPages <= db.cacheSizeThreshold/2 {
		return
	}

	// Compute the target size (aim to reduce to 75% of threshold)
	targetSize := db.cacheSizeThreshold * 3 / 4

	// Compute the number of pages to remove
	numPagesToRemove := totalPages - targetSize

	// Step 1: Use read locks to collect candidates
	var candidates []pageInfo

	db.seqMutex.Lock()
	var currentTxnSeq int64
	// If the main thread is in a transaction
	if db.inTransaction {
		// Keep pages from the previous transaction (because the current one can be rolled back)
		currentTxnSeq = db.txnSequence - 1
	} else {
		// Otherwise, keep pages from the last committed transaction
		currentTxnSeq = db.txnSequence
	}
	lastAccessTime := db.accessCounter
	db.seqMutex.Unlock()

	// Collect removable pages from each bucket
	db.iteratePages(func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		// Skip dirty pages, WAL pages, and pages from the current transaction
		if page.dirty || page.isWAL || page.txnSequence >= currentTxnSeq {
			return
		}

		// Add to candidates
		candidates = append(candidates, pageInfo{
			pageNumber: pageNumber,
			accessTime: page.accessTime,
		})
	})

	// If no candidates, nothing to do
	if len(candidates) == 0 {
		return
	}

	// Step 2: Sort candidates by access time (oldest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].accessTime < candidates[j].accessTime
	})

	// Step 3: Remove pages one by one with appropriate locking
	removedCount := 0
	for i := 0; i < len(candidates) && removedCount < numPagesToRemove; i++ {
		pageNumber := candidates[i].pageNumber
		bucket := &db.pageCache[pageNumber & 1023]

		bucket.mutex.Lock()

		// Double-check the page still exists and is still removable
		if page, exists := bucket.pages[pageNumber]; exists {
			// Skip if the page is dirty, WAL, or from the current transaction
			if page.dirty || page.isWAL || page.txnSequence >= currentTxnSeq {
				bucket.mutex.Unlock()
				continue
			}

			// Skip if the page is part of a linked list with WAL pages
			hasNonRemovable := false
			for p := page.next; p != nil; p = p.next {
				if p.isWAL {
					hasNonRemovable = true
					break
				}
			}
			if hasNonRemovable {
				bucket.mutex.Unlock()
				continue
			}

			// If the page was not accessed after this function was called
			if page.accessTime < lastAccessTime {
				// Count how many page versions we're removing
				for p := page; p != nil; p = p.next {
					removedCount++
				}
				// Remove the page from the cache
				delete(bucket.pages, pageNumber)
			}
		}

		bucket.mutex.Unlock()
	}

	// Update the total pages counter
	db.totalCachePages.Add(-int64(removedCount))

	debugPrint("Removed %d pages from cache, new size: %d\n", removedCount, db.totalCachePages.Load())
}

// ------------------------------------------------------------------------------------------------
// Page access
// ------------------------------------------------------------------------------------------------

// getPage gets a page from the cache or from the disk
// If maxReadSequence > 0, only returns pages with txnSequence <= maxReadSequence
func (db *DB) getPage(pageNumber uint32, maxReadSeq ...int64) (*Page, error) {

	// Determine the maximum transaction sequence number that can be read
	var maxReadSequence int64
	if len(maxReadSeq) > 0 && maxReadSeq[0] > 0 {
		maxReadSequence = maxReadSeq[0]
	} else {
		// Default behavior: no filtering
		maxReadSequence = 0
	}

	// Get the page from the cache
	bucket := &db.pageCache[pageNumber & 1023]
	bucket.mutex.RLock()
	page, exists := bucket.pages[pageNumber]

	// Store the parent page to update the access time
	parentPage := page

	// If maxReadSequence is specified, find the latest version that's <= maxReadSequence
	if exists && maxReadSequence > 0 {
		for ; page != nil; page = page.next {
			if page.txnSequence <= maxReadSequence {
				break
			}
		}
		// If we did not find a valid page version
		if page == nil {
			exists = false
		}
	}

	// If the page is in cache, update the access time on the parent page
	if exists {
		db.accessCounter++
		parentPage.accessTime = db.accessCounter
	}

	// The mutex is still locked to avoid race conditions when updating the access time
	// Example: main thread gets page from cache, the worker thread checks the access time
	// and removes the page from the cache. the main thread will then update the access time
	// but the page will no longer be in the cache.
	bucket.mutex.RUnlock()

	// If not in cache or no valid version found, read it from disk
	if !exists {
		var err error
		page, err = db.readPage(pageNumber)
		if err != nil {
			return nil, err
		}
	}

	// Return the page
	return page, nil
}

// getRadixPage returns a radix page from the cache or from the disk
func (db *DB) getRadixPage(pageNumber uint32, maxReadSequence ...int64) (*RadixPage, error) {
	// Get the page from the cache or from the disk
	page, err := db.getPage(pageNumber, maxReadSequence...)
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
func (db *DB) getLeafPage(pageNumber uint32, maxReadSequence ...int64) (*LeafPage, error) {
	// Get the page from the cache or from the disk
	page, err := db.getPage(pageNumber, maxReadSequence...)
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
	stats := make(map[string]interface{})

	// Use the atomic counter for total pages
	totalPages := db.totalCachePages.Load()

	// Count pages by type
	radixPages := 0
	leafPages := 0

	for bucketIdx := 0; bucketIdx < 1024; bucketIdx++ {
		bucket := &db.pageCache[bucketIdx]
		bucket.mutex.RLock()

		// Count pages by type
		for _, page := range bucket.pages {
			if page.pageType == ContentTypeRadix {
				radixPages++
			} else if page.pageType == ContentTypeLeaf {
				leafPages++
			}
		}

		bucket.mutex.RUnlock()
	}

	stats["cache_size"] = totalPages
	stats["dirty_pages"] = db.dirtyPageCount
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

	db.seqMutex.Lock()
	// Set flush sequence number limit and determine the appropriate main file size for this flush
	if db.inTransaction {
		db.flushSequence = db.txnSequence - 1
		// For worker thread flushes during transactions, use the file size from before the current transaction
		// This ensures we only index content that has been committed
		db.flushFileSize = db.prevFileSize
	} else {
		db.flushSequence = db.txnSequence
		// When not in transaction, use current main file size
		db.flushFileSize = db.mainFileSize
	}
	db.seqMutex.Unlock()

	// Flush all dirty pages
	pagesWritten, err := db.flushDirtyIndexPages()
	if err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
	}

	// If no pages were written, abort the flush
	if pagesWritten == 0 {
		return nil
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

/*
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
*/

// flushDirtyIndexPages writes all dirty pages to disk
// Returns the number of dirty pages that were written to disk
func (db *DB) flushDirtyIndexPages() (int, error) {

	if db.flushSequence == 0 {
		return 0, fmt.Errorf("flush sequence is not set")
	}

	// Collect all page numbers
	var pageNumbers []uint32

	for bucketIdx := 0; bucketIdx < 1024; bucketIdx++ {
		bucket := &db.pageCache[bucketIdx]
		bucket.mutex.RLock()

		for pageNumber := range bucket.pages {
			pageNumbers = append(pageNumbers, pageNumber)
		}

		bucket.mutex.RUnlock()
	}

	// Sort page numbers in ascending order
	sort.Slice(pageNumbers, func(i, j int) bool {
		return pageNumbers[i] < pageNumbers[j]
	})

	// Track the number of pages written
	pagesWritten := 0

	// Process pages in order
	for _, pageNumber := range pageNumbers {
		bucket := &db.pageCache[pageNumber & 1023]
		bucket.mutex.Lock()

		// Get the page from the cache (it can be modified by another thread)
		page, _ := bucket.pages[pageNumber]

		// Find the first version of the page that was modified up to the flush sequence
		for ; page != nil; page = page.next {
			if page.txnSequence <= db.flushSequence {
				break
			}
		}
		bucket.mutex.Unlock()

		// If the page contains modifications, write it to disk
		if page != nil && page.dirty {
			var err error
			if page.pageType == ContentTypeRadix {
				err = db.writeRadixPage(page)
			} else if page.pageType == ContentTypeLeaf {
				err = db.writeLeafPage(page)
			}

			if err != nil {
				return pagesWritten, err
			}
			pagesWritten++
		}
	}

	return pagesWritten, nil
}

// ------------------------------------------------------------------------------------------------
// Utility functions
// ------------------------------------------------------------------------------------------------

// getRootRadixPage returns the root radix page (page 1) from the cache
func (db *DB) getRootRadixPage(maxReadSequence ...int64) (*RadixPage, error) {
	return db.getRadixPage(1, maxReadSequence...)
}

// getRootRadixSubPage returns the root radix sub-page (page 1) from the cache
func (db *DB) getRootRadixSubPage(maxReadSequence ...int64) (*RadixSubPage, error) {
	rootPage, err := db.getRadixPage(1, maxReadSequence...)
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

	// Update the access time
	db.accessCounter++
	radixPage.accessTime = db.accessCounter

	// Update the transaction sequence
	radixPage.txnSequence = db.txnSequence

	// Add to cache
	db.addToCache(radixPage)

	debugPrint("Allocated new radix page at page %d\n", pageNumber)

	return radixPage, nil
}

// allocateRadixSubPage returns the next available radix sub-page or creates a new one if needed
// It returns a RadixSubPage struct
func (db *DB) allocateRadixSubPage() (*RadixSubPage, error) {

	// If we have a cached free radix page with available sub-pages
	if db.freeRadixPagesHead > 0 {
		// Get a reference to the radix page
		radixPage, err := db.getRadixPage(db.freeRadixPagesHead)
		if err != nil {
			return nil, fmt.Errorf("failed to get writable page: %w", err)
		}

		// Check if this page has available sub-pages
		if radixPage.SubPagesUsed >= SubPagesPerRadixPage {
			// This page is full, update to next free page
			db.freeRadixPagesHead = radixPage.NextFreePage
			// Try again recursively
			return db.allocateRadixSubPage()
		}

		// Get a writable version of the page
		radixPage, err = db.getWritablePage(radixPage)
		if err != nil {
			return nil, fmt.Errorf("failed to get writable page: %w", err)
		}

		// Use the next available sub-page index
		subPageIdx := radixPage.SubPagesUsed

		// Increment the sub-page counter (will be written when the page is updated)
		radixPage.SubPagesUsed++
		db.markPageDirty(radixPage)

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

			// Update the free sub-pages head to the next free page
			db.freeRadixPagesHead = nextFreePage
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
	db.markPageDirty(newRadixPage)

	// Add this page to the free sub-pages list
	db.addToFreeRadixPagesList(newRadixPage)

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
		SubPages:    make([]*LeafSubPageInfo, 256),
	}

	// Update the access time
	db.accessCounter++
	leafPage.accessTime = db.accessCounter

	// Update the transaction sequence
	leafPage.txnSequence = db.txnSequence

	// Add to cache
	db.addToCache(leafPage)

	debugPrint("Allocated new leaf page at page %d\n", pageNumber)

	return leafPage, nil
}

// allocateLeafPageWithSpace returns a leaf sub-page with available space, either from the free list or creates a new one
func (db *DB) allocateLeafPageWithSpace(spaceNeeded int) (*LeafSubPage, error) {
	// Traverse the free list to find a page with enough space
	currentPageNum := db.freeLeafPagesHead
	var prevPage *LeafPage = nil

	for currentPageNum > 0 {
		// Get the current page
		leafPage, err := db.getLeafPage(currentPageNum)
		if err != nil {
			return nil, fmt.Errorf("failed to get leaf page %d: %w", currentPageNum, err)
		}

		// Check if this page has enough space
		freeSpace := PageSize - int(leafPage.ContentSize)
		if freeSpace >= spaceNeeded {
			// Found a page with enough space, proceed with allocation

			// Get a writable version of the page
			leafPage, err = db.getWritablePage(leafPage)
			if err != nil {
				return nil, fmt.Errorf("failed to get writable page: %w", err)
			}

			// Find the first available sub-page ID
			var subPageID uint8 = 0
			// Initialize SubPages array if not already done
			if leafPage.SubPages == nil {
				leafPage.SubPages = make([]*LeafSubPageInfo, 256)
			}
			// Find first available slot
			for subPageID < 255 && leafPage.SubPages[subPageID] != nil {
				subPageID++
			}
			if subPageID == 255 && leafPage.SubPages[subPageID] != nil {
				// No available sub-page IDs, remove this page from the free list and continue to next page
				nextPageNum := leafPage.NextFreePage
				if err := db.removeFromFreeLeafPagesList(leafPage, prevPage); err != nil {
					return nil, fmt.Errorf("failed to remove full page from free list: %w", err)
				}
				currentPageNum = nextPageNum
				continue
			}

			// Count how many sub-page slots are currently used (continue from the last found slot)
			usedSlots := int(subPageID) + 1
			for i := usedSlots; i < 256; i++ {
				if leafPage.SubPages[i] != nil {
					usedSlots++
				}
			}

			// Calculate the free space after adding the content
			freeSpaceAfter := freeSpace - spaceNeeded

			// If this page will be full after we use this slot, remove it from the free list
			if freeSpaceAfter < PageSize/10 || usedSlots >= 255 {
				// Remove this page from the free list
				if err := db.removeFromFreeLeafPagesList(leafPage, prevPage); err != nil {
					return nil, fmt.Errorf("failed to remove page from free list: %w", err)
				}
			}

			return &LeafSubPage{
				Page:       leafPage,
				SubPageIdx: subPageID,
			}, nil
		}

		// This page doesn't have enough space for the current allocation
		// Check if it should be removed from the free list (less than 10% free space)
		if freeSpace < PageSize/10 {
			// Remove this page from the free list
			nextPageNum := leafPage.NextFreePage
			if err := db.removeFromFreeLeafPagesList(leafPage, prevPage); err != nil {
				return nil, fmt.Errorf("failed to remove page from free list: %w", err)
			}

			// Move to the next page (don't update prevPage since we removed currentPageNum)
			currentPageNum = nextPageNum
		} else {
			// Keep this page in the free list, move to the next page
			prevPage = leafPage
			currentPageNum = leafPage.NextFreePage
		}
	}

	// No suitable page found in the free list, allocate a new one
	newLeafPage, err := db.allocateLeafPage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new leaf page: %w", err)
	}

	// Add the new page to the free list since it will have free space after allocation
	db.addToFreeLeafPagesList(newLeafPage)

	return &LeafSubPage{
		Page:       newLeafPage,
		SubPageIdx: 0,
	}, nil
}

// cloneRadixPage clones a radix page
func (db *DB) cloneRadixPage(page *RadixPage) (*RadixPage, error) {
	// Create a new page
	newPage := &RadixPage{
		pageNumber:   page.pageNumber,
		pageType:     page.pageType,
		data:         make([]byte, PageSize),
		dirty:        page.dirty,
		isWAL:        false,
		accessTime:   page.accessTime,
		txnSequence:  page.txnSequence,
		SubPagesUsed: page.SubPagesUsed,
		NextFreePage: page.NextFreePage,
	}

	// Copy the data
	copy(newPage.data, page.data)

	// Add to cache
	db.addToCache(newPage)

	return newPage, nil
}

// cloneLeafPage clones a leaf page
func (db *DB) cloneLeafPage(page *LeafPage) (*LeafPage, error) {
	// Create a new page object
	newPage := &LeafPage{
		pageNumber:   page.pageNumber,
		pageType:     page.pageType,
		data:         make([]byte, PageSize),
		dirty:        page.dirty,
		isWAL:        false,
		accessTime:   page.accessTime,
		txnSequence:  page.txnSequence,
		ContentSize:  page.ContentSize,
		SubPages:     make([]*LeafSubPageInfo, 256),
	}

	// Copy the data
	copy(newPage.data, page.data)

	// Deep copy the sub-pages with their entries
	for i := 0; i < 256; i++ {
		if page.SubPages[i] != nil {
			subPage := page.SubPages[i]
			// Create a new slice for entries
			entries := make([]LeafEntry, len(subPage.Entries))

			// Copy each entry
			copy(entries, subPage.Entries)

			// Store the sub-page with copied entries
			newPage.SubPages[i] = &LeafSubPageInfo{
				Offset:  subPage.Offset,
				Size:    subPage.Size,
				Entries: entries,
			}
		}
	}

	// Add to cache
	db.addToCache(newPage)

	return newPage, nil
}

// getLeafSubPage returns a LeafSubPage struct for the given page number and sub-page index
func (db *DB) getLeafSubPage(pageNumber uint32, subPageIdx uint8, maxReadSeq ...int64) (*LeafSubPage, error) {
	// Get the page from the cache
	leafPage, err := db.getLeafPage(pageNumber, maxReadSeq...)
	if err != nil {
		return nil, fmt.Errorf("failed to get leaf page: %w", err)
	}

	// Check if the sub-page exists
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx] == nil {
		return nil, fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}

	return &LeafSubPage{
		Page:       leafPage,
		SubPageIdx: subPageIdx,
	}, nil
}

// getRadixSubPage returns a RadixSubPage struct for the given page number and sub-page index
func (db *DB) getRadixSubPage(pageNumber uint32, subPageIdx uint8, maxReadSeq ...int64) (*RadixSubPage, error) {
	// Get the page from the cache
	radixPage, err := db.getRadixPage(pageNumber, maxReadSeq...)
	if err != nil {
		return nil, fmt.Errorf("failed to get radix page: %w", err)
	}

	// Check if the sub-page index is valid
	if subPageIdx >= radixPage.SubPagesUsed {
		return nil, fmt.Errorf("sub-page index %d out of range (max %d)", subPageIdx, radixPage.SubPagesUsed)
	}

	return &RadixSubPage{
		Page:       radixPage,
		SubPageIdx: subPageIdx,
	}, nil
}

// ------------------------------------------------------------------------------------------------
// Leaf sub-page entries
// ------------------------------------------------------------------------------------------------

/*
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

		// Store the suffix offset and length
		suffixOffset := pos
		pos += suffixLen

		// Read data offset
		dataOffset := int64(binary.LittleEndian.Uint64(leafPage.data[pos:]))
		pos += 8

		// Add entry to list
		entries = append(entries, LeafEntry{
			SuffixOffset: suffixOffset,
			SuffixLen:    suffixLen,
			DataOffset:   dataOffset,
		})
	}

	return entries, nil
}
*/

// addEntryToNewLeafSubPage creates a new sub-page, adds an entry to it, then search for a leaf page with enough space to insert the sub-page into
func (db *DB) addEntryToNewLeafSubPage(suffix []byte, dataOffset int64) (*LeafSubPage, error) {
	// Step 1: Create a new leaf sub-page data buffer and serialize the entry

	// Create a buffer for the sub-page data (without the 3-byte header)
	suffixLenSize := varint.Size(uint64(len(suffix)))
	entrySize := suffixLenSize + len(suffix) + 8 // suffix length + suffix + data offset

	subPageData := make([]byte, entrySize)
	pos := 0

	// Write suffix length
	suffixLenWritten := varint.Write(subPageData[pos:], uint64(len(suffix)))
	pos += suffixLenWritten

	suffixPos := pos
	copy(subPageData[pos:], suffix)
	pos += len(suffix)

	binary.LittleEndian.PutUint64(subPageData[pos:], uint64(dataOffset))
	pos += 8

	// Step 2: Calculate the total size of this sub-page (including header)
	subPageSize := uint16(len(subPageData)) // Size of the data (excluding the 3-byte header)
	totalSubPageSize := LeafSubPageHeaderSize + int(subPageSize) // 3 bytes header + data

	// Step 3: Try to find a leaf page with enough space, or create a new one

	// Allocate a leaf sub-page with enough space
	leafSubPage, err := db.allocateLeafPageWithSpace(totalSubPageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate leaf sub-page: %w", err)
	}

	leafPage := leafSubPage.Page
	subPageID := leafSubPage.SubPageIdx

	// Check if there's enough space in the leaf page
	if int(leafPage.ContentSize) + totalSubPageSize > PageSize {
		return nil, fmt.Errorf("sub-page too large to fit in a leaf page")
	}

	// Step 4: Insert the sub-page into the leaf page

	// Calculate the offset where the sub-page will be placed
	offset := int(leafPage.ContentSize)

	// Write the sub-page header at the calculated offset
	leafPage.data[offset] = subPageID                                              // Sub-page ID   (1 byte)
	binary.LittleEndian.PutUint16(leafPage.data[offset+1:offset+3], subPageSize)   // Sub-page size (2 bytes)

	// Write the sub-page data after the header
	copy(leafPage.data[offset+3:], subPageData)

	// Step 5: Update the leaf page metadata

	// Update the content size
	leafPage.ContentSize = uint16(offset + totalSubPageSize)

	// Create the LeafSubPageInfo and add it to the leaf page
	leafEntry := LeafEntry{
		SuffixOffset: suffixPos, // Relative to sub-page data start (after 3-byte header)
		SuffixLen:    len(suffix),
		DataOffset:   dataOffset,
	}

	subPageInfo := LeafSubPageInfo{
		Offset:  uint16(offset),
		Size:    subPageSize,
		Entries: []LeafEntry{leafEntry},
	}

	// Store the sub-page at the index corresponding to its ID
	leafPage.SubPages[subPageID] = &subPageInfo

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	// Add the leaf page to the free list if it has reasonable free space
	db.addToFreeLeafPagesList(leafPage)

	// Step 6: Return the LeafSubPage reference
	return &LeafSubPage{
		Page:       leafPage,
		SubPageIdx: subPageID,
	}, nil
}

// addEntryToLeafSubPage adds an entry to a specific leaf sub-page
func (db *DB) addEntryToLeafSubPage(parentSubPage *RadixSubPage, parentByteValue uint8, subPage *LeafSubPage, suffix []byte, dataOffset int64) error {
	leafPage := subPage.Page
	subPageIdx := subPage.SubPageIdx

	// Get the sub-page info
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx] == nil {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := leafPage.SubPages[subPageIdx]

	// Create a new sub-page data buffer with the existing entries plus the new one
	// First, calculate the size needed for the new entry
	suffixLenSize := varint.Size(uint64(len(suffix)))
	newEntrySize := suffixLenSize + len(suffix) + 8 // suffix length + suffix + data offset

	// Calculate the total size needed for the updated sub-page
	newSubPageSize := int(subPageInfo.Size) + newEntrySize

	// Check if there's enough space in the current page for the updated sub-page
	totalSpaceNeeded := LeafHeaderSize + LeafSubPageHeaderSize + newSubPageSize
	for i := range leafPage.SubPages {
		if leafPage.SubPages[i] != nil && uint8(i) != subPageIdx {
			totalSpaceNeeded += LeafSubPageHeaderSize + int(leafPage.SubPages[i].Size)
		}
	}

	if totalSpaceNeeded > PageSize {
		// Current leaf page doesn't have enough space for the expanded sub-page
		// Check if the sub-page (with new entry) can fit in a new empty leaf page
		subPageWithHeaderSize := LeafSubPageHeaderSize + newSubPageSize
		if LeafHeaderSize + subPageWithHeaderSize <= PageSize {
			// Sub-page can fit in a new leaf page, move it there
			err := db.moveSubPageToNewLeafPage(subPage, suffix, dataOffset)
			if err != nil {
				return fmt.Errorf("failed to move sub-page to new leaf page: %w", err)
			}
		} else {
			// Sub-page is too large even for a new leaf page, convert to radix sub-page
			err := db.convertLeafSubPageToRadixSubPage(subPage, suffix, dataOffset)
			if err != nil {
				return fmt.Errorf("failed to convert leaf sub-page to radix sub-page: %w", err)
			}
		}
		// Update the subPage pointer, because the above function
		// could have cloned the same radix page used on this subPage
		parentSubPage.Page, _ = db.getRadixPage(parentSubPage.Page.pageNumber)
		// Update the parent radix sub-page to point to the new radix sub-page
		return db.setRadixEntry(parentSubPage, parentByteValue, subPage.Page.pageNumber, subPage.SubPageIdx)
	}

	// There's enough space in the current page, proceed with the update
	// Create the new sub-page data buffer
	newSubPageData := make([]byte, newSubPageSize)
	pos := 0

	// Copy existing entries from the original sub-page
	originalStart := int(subPageInfo.Offset) + 3 // Skip header
	copy(newSubPageData[pos:], leafPage.data[originalStart:originalStart+int(subPageInfo.Size)])
	pos += int(subPageInfo.Size)

	// Add the new entry
	suffixLenWritten := varint.Write(newSubPageData[pos:], uint64(len(suffix)))
	pos += suffixLenWritten

	suffixPos := pos
	copy(newSubPageData[pos:], suffix)
	pos += len(suffix)

	binary.LittleEndian.PutUint64(newSubPageData[pos:], uint64(dataOffset))
	pos += 8

	// Create the new entries list
	newEntries := make([]LeafEntry, len(subPageInfo.Entries)+1)
	copy(newEntries, subPageInfo.Entries)

	// Add the new entry
	newEntries[len(subPageInfo.Entries)] = LeafEntry{
		SuffixOffset: suffixPos, // Relative to sub-page data start
		SuffixLen:    len(suffix),
		DataOffset:   dataOffset,
	}

	// Use the helper function to update the leaf page
	newSubPageIdx, err := db.updateLeafPage(leafPage, int(subPageIdx), newSubPageData, newEntries)
	if err != nil {
		return fmt.Errorf("failed to update leaf sub-page: %w", err)
	}

	// Update the subPage reference to point to the updated page and new sub-page index
	subPage.Page = leafPage
	subPage.SubPageIdx = uint8(newSubPageIdx)

	// If the sub-page ID changed, update the parent radix entry
	if uint8(newSubPageIdx) != subPageIdx {
		// Update the subPage pointer, because the above function
		// could have cloned the same radix page used on this subPage
		parentSubPage.Page, _ = db.getRadixPage(parentSubPage.Page.pageNumber)
		// Update the parent radix sub-page to point to the new sub-page
		return db.setRadixEntry(parentSubPage, parentByteValue, leafPage.pageNumber, uint8(newSubPageIdx))
	}

	return nil
}

// removeEntryFromLeafSubPage removes an entry from a leaf sub-page at the given index
func (db *DB) removeEntryFromLeafSubPage(subPage *LeafSubPage, entryIdx int) error {
	// Get a writable version of the page
	leafPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = leafPage

	// Get the sub-page info
	subPageIdx := subPage.SubPageIdx
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx] == nil {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := leafPage.SubPages[subPageIdx]

	// Create a new entries list without this entry
	newEntries := make([]LeafEntry, 0, len(subPageInfo.Entries)-1)
	newEntries = append(newEntries, subPageInfo.Entries[:entryIdx]...)
	newEntries = append(newEntries, subPageInfo.Entries[entryIdx+1:]...)

	// If there are no more entries, remove the entire sub-page
	if len(newEntries) == 0 {
		_, err := db.updateLeafPage(leafPage, int(subPageIdx), nil, nil)
		if err != nil {
			return fmt.Errorf("failed to remove empty sub-page: %w", err)
		}
		return nil
	}

	// Rebuild the sub-page data without the removed entry
	subPageDataStart := int(subPageInfo.Offset) + 3 // Skip 3-byte header
	newSubPageData := make([]byte, 0)
	for i, newEntry := range newEntries {
		// Get the suffix from the original entry
		origSuffixOffset := subPageDataStart + newEntry.SuffixOffset
		origSuffix := leafPage.data[origSuffixOffset:origSuffixOffset+newEntry.SuffixLen]

		// Calculate size needed for this entry
		suffixLenSize := varint.Size(uint64(len(origSuffix)))
		entrySize := suffixLenSize + len(origSuffix) + 8

		// Create buffer for this entry
		entryBuffer := make([]byte, entrySize)
		pos := 0

		// Write suffix length
		suffixLenWritten := varint.Write(entryBuffer[pos:], uint64(len(origSuffix)))
		pos += suffixLenWritten

		// Write suffix
		suffixPosInNewData := len(newSubPageData) + pos
		copy(entryBuffer[pos:], origSuffix)
		pos += len(origSuffix)

		// Write data offset
		binary.LittleEndian.PutUint64(entryBuffer[pos:], uint64(newEntry.DataOffset))

		// Update the entry's suffix offset to be relative to the new sub-page data
		newEntries[i].SuffixOffset = suffixPosInNewData

		// Append to sub-page data
		newSubPageData = append(newSubPageData, entryBuffer...)
	}

	// Update the leaf page with the new sub-page data
	_, err = db.updateLeafPage(leafPage, int(subPageIdx), newSubPageData, newEntries)
	if err != nil {
		return fmt.Errorf("failed to update sub-page after deletion: %w", err)
	}

	return nil
}

// updateEntryInLeafSubPage updates the data offset of an entry in a leaf sub-page
func (db *DB) updateEntryInLeafSubPage(subPage *LeafSubPage, entryIdx int, dataOffset int64) error {
	// Get a writable version of the page
	leafPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = leafPage

	// Get the sub-page info
	subPageIdx := subPage.SubPageIdx
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx] == nil {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := leafPage.SubPages[subPageIdx]

	// Create a new entries list with the updated entry
	newEntries := make([]LeafEntry, len(subPageInfo.Entries))
	copy(newEntries, subPageInfo.Entries)
	newEntries[entryIdx].DataOffset = dataOffset

	// Rebuild the sub-page data with the updated entry
	subPageDataStart := int(subPageInfo.Offset) + 3 // Skip 3-byte header
	newSubPageData := make([]byte, 0)
	for i, newEntry := range newEntries {
		// Get the suffix from the original entry
		origSuffixOffset := subPageDataStart + newEntry.SuffixOffset
		origSuffix := leafPage.data[origSuffixOffset:origSuffixOffset+newEntry.SuffixLen]

		// Calculate size needed for this entry
		suffixLenSize := varint.Size(uint64(len(origSuffix)))
		entrySize := suffixLenSize + len(origSuffix) + 8

		// Create buffer for this entry
		entryBuffer := make([]byte, entrySize)
		pos := 0

		// Write suffix length
		suffixLenWritten := varint.Write(entryBuffer[pos:], uint64(len(origSuffix)))
		pos += suffixLenWritten

		// Write suffix
		suffixPosInNewData := len(newSubPageData) + pos
		copy(entryBuffer[pos:], origSuffix)
		pos += len(origSuffix)

		// Write data offset (use the updated offset for the modified entry)
		binary.LittleEndian.PutUint64(entryBuffer[pos:], uint64(newEntry.DataOffset))

		// Update the entry's suffix offset to be relative to the new sub-page data
		newEntries[i].SuffixOffset = suffixPosInNewData

		// Append to sub-page data
		newSubPageData = append(newSubPageData, entryBuffer...)
	}

	// Update the leaf page with the new sub-page data
	_, err = db.updateLeafPage(leafPage, int(subPageIdx), newSubPageData, newEntries)
	if err != nil {
		return fmt.Errorf("failed to update sub-page after offset update: %w", err)
	}

	return nil
}

// updateLeafPage updates a leaf page by removing a sub-page and optionally adding a new sub-page
// Parameters:
// - leafPage: the leaf page to update
// - removeSubPageIdx: index of sub-page to remove (use -1 if no removal needed)
// - newSubPageData: data for the new sub-page to add (nil if no addition needed)
// - newSubPageEntries: entries for the new sub-page (nil if no addition needed)
// Returns the index of the newly added sub-page (or -1 if no sub-page was added)
func (db *DB) updateLeafPage(leafPage *LeafPage, removeSubPageIdx int, newSubPageData []byte, newSubPageEntries []LeafEntry) (int, error) {
	// Get a writable version of the page
	leafPage, err := db.getWritablePage(leafPage)
	if err != nil {
		return -1, fmt.Errorf("failed to get writable page: %w", err)
	}

	newSubPageIdx := -1 // Will be set if we add a new sub-page

	// Handle removal of a sub-page
	if removeSubPageIdx != -1 && removeSubPageIdx < len(leafPage.SubPages) && leafPage.SubPages[removeSubPageIdx] != nil {
		removedSubPage := leafPage.SubPages[removeSubPageIdx]
		removedStart := int(removedSubPage.Offset)
		removedSize := LeafSubPageHeaderSize + int(removedSubPage.Size)
		removedEnd := removedStart + removedSize

		// Move all data after the removed sub-page to fill the gap
		remainingDataStart := removedEnd
		remainingDataEnd := int(leafPage.ContentSize)
		if remainingDataStart < remainingDataEnd {
			copy(leafPage.data[removedStart:], leafPage.data[remainingDataStart:remainingDataEnd])
		}

		// Update offsets for sub-pages that come after the removed one
		for i := 0; i < len(leafPage.SubPages); i++ {
			if leafPage.SubPages[i] != nil && leafPage.SubPages[i].Offset > removedSubPage.Offset {
				leafPage.SubPages[i].Offset -= uint16(removedSize)
			}
		}

		// Remove the sub-page from the array
		leafPage.SubPages[removeSubPageIdx] = nil

		// Update content size
		leafPage.ContentSize -= uint16(removedSize)
	}

	// Add new sub-page if provided
	if newSubPageData != nil && newSubPageEntries != nil {
		subPageSize := uint16(len(newSubPageData))
		totalSubPageSize := LeafSubPageHeaderSize + int(subPageSize)

		// Check if there's enough space
		if int(leafPage.ContentSize) + totalSubPageSize > PageSize {
			return -1, fmt.Errorf("not enough space in leaf page for new sub-page")
		}

		// Find the first available sub-page ID
		for i := 0; i < len(leafPage.SubPages); i++ {
			if leafPage.SubPages[i] == nil {
				newSubPageIdx = i
				break
			}
		}

		// If no available slot found, expand the array
		if newSubPageIdx == -1 {
			if len(leafPage.SubPages) >= 256 {
				return -1, fmt.Errorf("no available sub-page IDs")
			}
			newSubPageIdx = len(leafPage.SubPages)
			// Expand the SubPages array
			newSubPages := make([]*LeafSubPageInfo, len(leafPage.SubPages)+1)
			copy(newSubPages, leafPage.SubPages)
			leafPage.SubPages = newSubPages
		}

		// Add the new sub-page at the end of the current content
		pos := int(leafPage.ContentSize)

		// Write sub-page header
		leafPage.data[pos] = uint8(newSubPageIdx)                                // Sub-page ID
		binary.LittleEndian.PutUint16(leafPage.data[pos+1:pos+3], subPageSize)   // Sub-page size

		// Copy sub-page data
		copy(leafPage.data[pos+3:], newSubPageData)

		// Add to sub-pages array at the found index
		leafPage.SubPages[newSubPageIdx] = &LeafSubPageInfo{
			Offset:  uint16(pos),
			Size:    subPageSize,
			Entries: newSubPageEntries,
		}

		// Update content size
		leafPage.ContentSize += uint16(totalSubPageSize)
	}

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	// Add the leaf page to the free list if it has reasonable free space
	db.addToFreeLeafPagesList(leafPage)

	return newSubPageIdx, nil
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
	subPageOffset := RadixHeaderSize + int(subPageIdx) * RadixSubPageSize
	entryOffset := subPageOffset + int(byteValue) * RadixEntrySize

	// Write page number (4 bytes)
	binary.LittleEndian.PutUint32(radixPage.data[entryOffset:entryOffset+4], pageNumber)

	// Write sub-page index (1 byte)
	radixPage.data[entryOffset+4] = nextSubPageIdx

	// Mark page as dirty
	db.markPageDirty(radixPage)

	return nil
}

// getRadixPageEntry gets an entry from a radix page
func (db *DB) getRadixPageEntry(radixPage *RadixPage, subPageIdx uint8, byteValue uint8) (pageNumber uint32, nextSubPageIdx uint8) {
	// Check if subPage is valid
	if subPageIdx >= SubPagesPerRadixPage || subPageIdx >= radixPage.SubPagesUsed {
		return 0, 0
	}

	// Calculate base offset for this entry in the page data
	subPageOffset := RadixHeaderSize + int(subPageIdx) * RadixSubPageSize
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
	subPageOffset := RadixHeaderSize + int(subPage.SubPageIdx) * RadixSubPageSize
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

	// Update the subPage pointer to point to the new page
	subPage.Page = radixPage

	// Calculate the offset for the empty suffix in the page data
	// Each sub-page has 256 entries of 5 bytes each, followed by an 8-byte empty suffix offset
	subPageOffset := RadixHeaderSize + int(subPage.SubPageIdx) * RadixSubPageSize
	emptySuffixOffsetPos := subPageOffset + EntriesPerSubPage * RadixEntrySize

	// Write the 8-byte offset
	binary.LittleEndian.PutUint64(radixPage.data[emptySuffixOffsetPos:emptySuffixOffsetPos+8], uint64(offset))

	// Mark the page as dirty
	db.markPageDirty(radixPage)

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
	db.markPageDirty(rootRadixPage)

	/*
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
	*/

	return nil
}

// convertLeafSubPageToRadixSubPage converts a leaf sub-page to a radix sub-page when it's too large
// It allocates a new radix sub-page and redistributes all entries from the leaf sub-page
// Returns the page number and sub-page index of the new radix sub-page
func (db *DB) convertLeafSubPageToRadixSubPage(subPage *LeafSubPage, newSuffix []byte, newDataOffset int64) (error) {
	// Get a writable version of the leaf page
	writableLeafPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable leaf page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = writableLeafPage

	leafPage := subPage.Page
	subPageIdx := subPage.SubPageIdx

	debugPrint("Converting leaf sub-page %d on page %d to radix sub-page\n", subPageIdx, leafPage.pageNumber)

	// Get the sub-page info
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx] == nil {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := leafPage.SubPages[subPageIdx]

	// Allocate a new radix sub-page
	newRadixSubPage, err := db.allocateRadixSubPage()
	if err != nil {
		return fmt.Errorf("failed to allocate radix sub-page: %w", err)
	}

	// Create a copy of all existing entries from the leaf sub-page
	var existingEntries []struct {
		suffix []byte
		dataOffset int64
	}

	for _, entry := range subPageInfo.Entries {
		// Get the suffix from the entry (using sub-page base + relative offset)
		subPageDataStart := int(subPageInfo.Offset) + 3 // Skip 3-byte header
		suffixOffset := subPageDataStart + entry.SuffixOffset
		suffix := make([]byte, entry.SuffixLen)
		copy(suffix, leafPage.data[suffixOffset:suffixOffset+entry.SuffixLen])

		existingEntries = append(existingEntries, struct {
			suffix []byte
			dataOffset int64
		}{
			suffix: suffix,
			dataOffset: entry.DataOffset,
		})
	}

	// Remove the old sub-page from the leaf page
	_, err = db.updateLeafPage(leafPage, int(subPageIdx), nil, nil)
	if err != nil {
		return fmt.Errorf("failed to remove sub-page from original leaf page: %w", err)
	}

	// Process all existing entries and add them to the newly created radix sub-page
	for _, entry := range existingEntries {
		// Add it to the newly created radix sub-page
		// The first byte of the suffix determines which branch to take
		if err := db.setContentOnIndex(newRadixSubPage, entry.suffix, 0, entry.dataOffset); err != nil {
			return fmt.Errorf("failed to convert leaf sub-page to radix sub-page: %w", err)
		}
	}

	// Process the new entry separately
	if err := db.setContentOnIndex(newRadixSubPage, newSuffix, 0, newDataOffset); err != nil {
		return fmt.Errorf("failed to add new entry to radix sub-page: %w", err)
	}

	// Update the sub-page to point to the new radix sub-page
	subPage.Page = newRadixSubPage.Page
	subPage.SubPageIdx = newRadixSubPage.SubPageIdx

	return nil
}

// moveSubPageToNewLeafPage moves a leaf sub-page to a new leaf page when it doesn't fit in the current page
// but is still small enough to fit in a new empty leaf page
func (db *DB) moveSubPageToNewLeafPage(subPage *LeafSubPage, newSuffix []byte, newDataOffset int64) error {
	// Get a writable version of the leaf page
	writableLeafPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable leaf page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = writableLeafPage

	leafPage := subPage.Page
	subPageIdx := subPage.SubPageIdx

	debugPrint("Moving leaf sub-page %d from page %d to new leaf page\n", subPageIdx, leafPage.pageNumber)

	// Get the sub-page info
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx] == nil {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := leafPage.SubPages[subPageIdx]

	// Calculate the size needed for the new entry
	suffixLenSize := varint.Size(uint64(len(newSuffix)))
	newEntrySize := suffixLenSize + len(newSuffix) + 8

	// Calculate the total size needed for the updated sub-page
	newSubPageSize := int(subPageInfo.Size) + newEntrySize

	// Create the new sub-page data buffer with all existing entries plus the new one
	newSubPageData := make([]byte, newSubPageSize)
	pos := 0

	// Copy existing entries from the original sub-page
	originalStart := int(subPageInfo.Offset) + 3 // Skip header
	copy(newSubPageData[pos:], leafPage.data[originalStart:originalStart+int(subPageInfo.Size)])
	pos += int(subPageInfo.Size)

	// Add the new entry
	suffixLenWritten := varint.Write(newSubPageData[pos:], uint64(len(newSuffix)))
	pos += suffixLenWritten

	suffixPos := pos
	copy(newSubPageData[pos:], newSuffix)
	pos += len(newSuffix)

	binary.LittleEndian.PutUint64(newSubPageData[pos:], uint64(newDataOffset))
	pos += 8

	// Create the new entries list
	newEntries := make([]LeafEntry, len(subPageInfo.Entries)+1)
	copy(newEntries, subPageInfo.Entries)

	// Add the new entry
	newEntries[len(subPageInfo.Entries)] = LeafEntry{
		SuffixOffset: suffixPos, // Relative to sub-page data start
		SuffixLen:    len(newSuffix),
		DataOffset:   newDataOffset,
	}

	// Allocate a leaf page with enough space for this sub-page
	totalSubPageSize := LeafSubPageHeaderSize + newSubPageSize
	newLeafSubPage, err := db.allocateLeafPageWithSpace(totalSubPageSize)
	if err != nil {
		return fmt.Errorf("failed to allocate new leaf sub-page: %w", err)
	}

	newLeafPage := newLeafSubPage.Page
	newSubPageID := newLeafSubPage.SubPageIdx

	// Calculate the offset where the sub-page will be placed in the new leaf page
	offset := int(newLeafPage.ContentSize)

	// Write the sub-page header at the calculated offset
	newLeafPage.data[offset] = newSubPageID // Sub-page ID
	binary.LittleEndian.PutUint16(newLeafPage.data[offset+1:offset+3], uint16(newSubPageSize))

	// Write the sub-page data after the header
	copy(newLeafPage.data[offset+3:], newSubPageData)

	// Update the new leaf page metadata
	newLeafPage.ContentSize = uint16(int(newLeafPage.ContentSize) + totalSubPageSize)

	// Create the LeafSubPageInfo and add it to the new leaf page
	newLeafPage.SubPages[newSubPageID] = &LeafSubPageInfo{
		Offset:  uint16(offset),
		Size:    uint16(newSubPageSize),
		Entries: newEntries,
	}

	// Mark the new page as dirty
	db.markPageDirty(newLeafPage)

	// Add the new leaf page to the free list if it has reasonable free space
	db.addToFreeLeafPagesList(newLeafPage)

	// Remove the sub-page from the original leaf page
	_, err = db.updateLeafPage(leafPage, int(subPageIdx), nil, nil)
	if err != nil {
		return fmt.Errorf("failed to remove sub-page from original leaf page: %w", err)
	}

	// Update the subPage reference to point to the new leaf page
	subPage.Page = newLeafPage
	subPage.SubPageIdx = newSubPageID

	return nil
}

// ------------------------------------------------------------------------------------------------
// Free lists
// ------------------------------------------------------------------------------------------------

// addToFreeRadixPagesList adds a radix page with free sub-pages to the list
func (db *DB) addToFreeRadixPagesList(radixPage *RadixPage) {
	// Only add if the page has free sub-pages
	if radixPage.SubPagesUsed >= SubPagesPerRadixPage {
		return
	}

	// Don't add if it's already the head of the list
	if db.freeRadixPagesHead == radixPage.pageNumber {
		return
	}

	// Link this page to the current head
	if db.freeRadixPagesHead > 0 {
		radixPage.NextFreePage = db.freeRadixPagesHead
	} else {
		radixPage.NextFreePage = 0
	}

	// Mark the page as dirty
	db.markPageDirty(radixPage)

	// Make it the new head
	db.freeRadixPagesHead = radixPage.pageNumber
}

// addToFreeLeafPagesList adds a leaf page with free space to the list
func (db *DB) addToFreeLeafPagesList(leafPage *LeafPage) {
	// Only add if the page has free space (less than 90% full)
	freeSpace := PageSize - int(leafPage.ContentSize)
	if freeSpace < PageSize/10 {
		return
	}

	// Don't add if it's already the head of the list
	if db.freeLeafPagesHead == leafPage.pageNumber {
		return
	}

	// Link this page to the current head
	if db.freeLeafPagesHead > 0 {
		leafPage.NextFreePage = db.freeLeafPagesHead
	} else {
		leafPage.NextFreePage = 0
	}

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	// Make it the new head
	db.freeLeafPagesHead = leafPage.pageNumber
}

// removeFromFreeLeafPagesList removes a leaf page from the free list
// If prevPage is nil, the page to remove is the head of the list
// If prevPage is provided, it should be the page that points to the page being removed
func (db *DB) removeFromFreeLeafPagesList(leafPage *LeafPage, prevPage *LeafPage) error {
	// If this page is the head of the list, update the head
	if prevPage == nil {
		db.freeLeafPagesHead = leafPage.NextFreePage
		leafPage.NextFreePage = 0
		db.markPageDirty(leafPage)
		return nil
	}

	// Get writable version of previous page
	prevPage, err := db.getWritablePage(prevPage)
	if err != nil {
		return fmt.Errorf("failed to get writable previous page: %w", err)
	}

	// Update the previous page to skip the page we're removing
	prevPage.NextFreePage = leafPage.NextFreePage
	db.markPageDirty(prevPage)

	// Clear the NextFreePage field of the removed page
	leafPage.NextFreePage = 0
	db.markPageDirty(leafPage)

	return nil
}

// ------------------------------------------------------------------------------------------------
// Recovery
// ------------------------------------------------------------------------------------------------

// recoverUnindexedContent reads the main file starting from the last indexed offset
// and reindexes any content that hasn't been indexed yet
// It also checks for commit markers and discards any uncommitted data
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

	// First pass: Find the last valid commit marker and truncate file if needed
	validFileSize, err := db.findLastValidCommit(lastIndexedOffset)
	if err != nil {
		return fmt.Errorf("failed to find last valid commit: %w", err)
	}

	// If we need to truncate the file due to uncommitted data
	if validFileSize < db.mainFileSize {
		debugPrint("Truncating main file from %d to %d due to uncommitted data\n", db.mainFileSize, validFileSize)
		if !db.readOnly {
			if err := db.mainFile.Truncate(validFileSize); err != nil {
				return fmt.Errorf("failed to truncate main file: %w", err)
			}
		}
		db.mainFileSize = validFileSize
	}

	// If there's nothing to recover after truncation
	if lastIndexedOffset >= db.mainFileSize {
		return nil
	}

	// Initialize the transaction sequence
	db.txnSequence = 1

	// Get the root radix sub-page
	rootSubPage, err := db.getRootRadixSubPage()
	if err != nil {
		return fmt.Errorf("failed to get root radix sub-page: %w", err)
	}

	// Update the transaction sequence on the root radix sub-page
	rootSubPage.Page.txnSequence = db.txnSequence

	// Second pass: Process all committed data
	currentOffset := lastIndexedOffset

	for currentOffset < db.mainFileSize {
		// Read the content at the current offset
		content, err := db.readContent(currentOffset)
		if err != nil {
			return fmt.Errorf("failed to read content at offset %d: %w", currentOffset, err)
		}

		if content.data[0] == ContentTypeData {
			// Set the key-value pair on the index
			err := db.setKvOnIndex(rootSubPage, content.key, content.value, currentOffset)
			if err != nil {
				return fmt.Errorf("failed to set kv on index: %w", err)
			}
		} else if content.data[0] == ContentTypeCommit {
			// Do nothing, we've already processed the commit marker
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

// findLastValidCommit scans the file from the given offset to find the last valid commit marker
// and returns the file size that should be used (truncating any uncommitted data)
func (db *DB) findLastValidCommit(startOffset int64) (int64, error) {
	currentOffset := startOffset
	lastValidOffset := startOffset
	runningChecksum := uint32(0)

	for currentOffset < db.mainFileSize {
		// Read the content type first
		typeBuffer := make([]byte, 1)
		if _, err := db.mainFile.ReadAt(typeBuffer, currentOffset); err != nil {
			if err == io.EOF {
				break
			}
			return 0, fmt.Errorf("failed to read content type at offset %d: %w", currentOffset, err)
		}

		contentType := typeBuffer[0]

		if contentType == ContentTypeData {
			// Read the full data content to get its size and update checksum
			content, err := db.readContent(currentOffset)
			if err != nil {
				// If we can't read the content, it's likely corrupted or incomplete
				debugPrint("Failed to read data content at offset %d: %v\n", currentOffset, err)
				break
			}
			// Update running checksum with this data content
			runningChecksum = crc32.Update(runningChecksum, crc32.IEEETable, content.data)
			currentOffset += int64(len(content.data))
		} else if contentType == ContentTypeCommit {
			// Read the checksum from the commit marker
			checksum := make([]byte, 4)
			if _, err := db.mainFile.ReadAt(checksum, currentOffset + 1); err != nil {
				debugPrint("Failed to read checksum at offset %d: %v\n", currentOffset + 1, err)
				break
			}
			// Extract the stored checksum
			storedChecksum := binary.BigEndian.Uint32(checksum)
			// Verify checksum matches our running checksum
			if storedChecksum != runningChecksum {
				debugPrint("Invalid commit marker at offset %d: checksum mismatch expected %d, got %d\n", currentOffset, runningChecksum, storedChecksum)
				break
			}
			// This is a valid commit, update our last valid position
			currentOffset += 5 // Commit marker is always 5 bytes
			lastValidOffset = currentOffset
			// Reset running checksum for next transaction
			runningChecksum = 0
		} else {
			// Unknown content type, stop processing
			debugPrint("Unknown content type '%c' at offset %d\n", contentType, currentOffset)
			break
		}
	}

	return lastValidOffset, nil
}

// ------------------------------------------------------------------------------------------------
// System
// ------------------------------------------------------------------------------------------------

// calculateDefaultCacheSize calculates the default cache size threshold based on system memory
// Returns the number of pages that can fit in 20% of the system memory
func calculateDefaultCacheSize() int {
	totalMemory := getTotalSystemMemory()

	// Use 20% of total memory for cache
	cacheMemory := int64(float64(totalMemory) * 0.2)

	// Calculate how many pages fit in the cache memory
	numPages := int(cacheMemory / PageSize)

	// Ensure we have a reasonable minimum
	if numPages < 300 {
		numPages = 300
	}

	debugPrint("System memory: %d bytes, Cache memory: %d bytes, Cache pages: %d\n",
		totalMemory, cacheMemory, numPages)

	return numPages
}

// getTotalSystemMemory returns the total physical memory of the system in bytes
func getTotalSystemMemory() int64 {
	var totalMemory int64

	// Try sysctl for BSD-based systems (macOS, FreeBSD, NetBSD, OpenBSD)
	if runtime.GOOS == "darwin" || runtime.GOOS == "freebsd" ||
	   runtime.GOOS == "netbsd" || runtime.GOOS == "openbsd" {
		// Use hw.memsize for macOS, hw.physmem for FreeBSD/NetBSD/OpenBSD
		var sysctlKey string
		if runtime.GOOS == "darwin" {
			sysctlKey = "hw.memsize"
		} else {
			sysctlKey = "hw.physmem"
		}

		cmd := exec.Command("sysctl", "-n", sysctlKey)
		output, err := cmd.Output()
		if err == nil {
			memStr := strings.TrimSpace(string(output))
			mem, err := strconv.ParseInt(memStr, 10, 64)
			if err == nil {
				totalMemory = mem
			}
		}
	} else if runtime.GOOS == "linux" {
		// For Linux, use /proc/meminfo
		cmd := exec.Command("grep", "MemTotal", "/proc/meminfo")
		output, err := cmd.Output()
		if err == nil {
			memStr := strings.TrimSpace(string(output))
			// Format is: "MemTotal:       16384516 kB"
			fields := strings.Fields(memStr)
			if len(fields) >= 2 {
				// Convert from KB to bytes
				mem, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					totalMemory = mem * 1024 // Convert KB to bytes
				}
			}
		}
	} else {
		// For other POSIX systems, try the generic 'free' command
		cmd := exec.Command("free", "-b")
		output, err := cmd.Output()
		if err == nil {
			lines := strings.Split(string(output), "\n")
			if len(lines) > 1 {
				// Parse the second line which contains memory info
				fields := strings.Fields(lines[1])
				if len(fields) > 1 {
					mem, err := strconv.ParseInt(fields[1], 10, 64)
					if err == nil {
						totalMemory = mem
					}
				}
			}
		}
	}

	// Fallback if we couldn't get system memory or on unsupported platforms
	if totalMemory <= 0 {
		// Use runtime memory stats as fallback
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		totalMemory = int64(mem.TotalAlloc)

		// Set a reasonable minimum if we couldn't determine actual memory
		if totalMemory < 1<<30 { // 1 GB
			totalMemory = 1 << 30
		}
	}

	return totalMemory
}

// startBackgroundWorker starts a background goroutine that listens for commands on the workerChannel
func (db *DB) startBackgroundWorker() {
	// Add 1 to the wait group before starting the goroutine
	db.workerWaitGroup.Add(1)

	go func() {
		// Ensure the wait group is decremented when the goroutine exits
		defer db.workerWaitGroup.Done()

		for cmd := range db.workerChannel {
			switch cmd {

			case "flush":
				db.flushIndexToDisk()
				// Clear the pending command flag
				db.seqMutex.Lock()
				delete(db.pendingCommands, "flush")
				db.seqMutex.Unlock()

			case "clean":
				// Discard previous versions of pages
				numPages := db.discardOldPageVersions(true)
				// If the number of pages is still greater than the cache size threshold
				if numPages > db.cacheSizeThreshold {
					// Remove old pages from cache
					db.removeOldPagesFromCache()
				}
				// Clear the pending command flag
				db.seqMutex.Lock()
				delete(db.pendingCommands, "clean")
				db.seqMutex.Unlock()

			case "checkpoint":
				db.checkpointWAL()
				// Clear the pending command flag
				db.seqMutex.Lock()
				delete(db.pendingCommands, "checkpoint")
				db.seqMutex.Unlock()

			case "exit":
				return

			default:
				debugPrint("Unknown worker command: %s\n", cmd)
			}
		}
	}()
}
