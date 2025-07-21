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
	// Minimum free space in bytes required to add a leaf page to the free space array
	MIN_FREE_SPACE = 64
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

// Free space tracking
const (
	MaxFreeSpaceEntries = 500 // Maximum number of free space entries in the array
)

// FreeSpaceEntry represents an entry in the free space array
type FreeSpaceEntry struct {
	PageNumber uint32 // Page number of the leaf page
	FreeSpace  uint16 // Amount of free space in bytes
}

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
	cloningFileSize int64 // Track main file size when a cloning mark was created
	fileLocked     bool  // Track if the files are locked
	lockType       int   // Type of lock currently held
	readOnly       bool  // Track if the database is opened in read-only mode
	pageCache      [1024]cacheBucket // Page cache for all page types
	totalCachePages atomic.Int64     // Total number of pages in cache (including previous versions)
	freeRadixPagesHead uint32 // Page number of the head of linked list of radix pages with available sub-pages
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
	cloningSequence int64 // Cloning mark sequence number
	fastRollback   bool   // Whether to use fast rollback (clone every transaction) or fast write (clone every 1000 transactions)
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
	headerPageForTransaction *Page // Pointer to the header page for transaction
	transactionCond *sync.Cond // Condition variable for transaction waiting
	lastFlushTime time.Time // Time of the last flush operation
	isClosing bool // Whether the database is closing
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
	ContentSize  int                // Total size of content on this page
	SubPages     []LeafSubPageInfo  // Information about sub-pages in this leaf page (allocated only for leaf pages)
	// Fields for HeaderPage (only used when pageNumber == 0)
	freeLeafSpaceArray []FreeSpaceEntry // Array of leaf pages with free space (allocated only for header page)
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

// LeafSubPageInfo stores metadata for a leaf sub-page
type LeafSubPageInfo struct {
	Offset  uint16      // Offset in the page data where the sub-page starts
	Size    uint16      // Size of the sub-page data (excluding header)
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
	checkpointThreshold := int64(1024 * 1024 * 100)    // Default to 100MB
	fastRollback := true                               // Default to slower transaction, faster rollback

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
		if val, ok := opts["FastRollback"]; ok {
			if fr, ok := val.(bool); ok {
				fastRollback = fr
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
		fastRollback:       fastRollback,
		workerChannel:      make(chan string, 10), // Buffer size of 10 for commands
		pendingCommands:    make(map[string]bool), // Initialize the pending commands map
		lastFlushTime:      time.Now(), // Initialize to current time
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
	/*
	case "FastRollback":
		if fr, ok := value.(bool); ok {
			db.fastRollback = fr
			return nil
		}
		return fmt.Errorf("FastRollback value must be a boolean")
		*/
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

	if db.isClosing {
		return nil // Already closing
	}
	db.isClosing = true

	if !db.readOnly {
		// If there's an open transaction, rollback before closing
		if db.inTransaction {
			db.rollbackTransaction()
		}

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

	// The remaining part of the key is the suffix. It can be empty
	suffix := key[keyPos+1:]

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
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}

	// Search for the suffix in this sub-page
	entryOffset, entrySize, existingDataOffset, err := db.findEntryInLeafSubPage(leafPage, subPageIdx, suffix)
	if err != nil {
		return fmt.Errorf("failed to search entries in sub-page: %w", err)
	}

	if entryOffset >= 0 {
		// Found the entry
		var content *Content

		// If we're setting a new key-value pair
		if dataOffset == 0 {
			// Read the content from the main file
			content, err = db.readContent(existingDataOffset)
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
			return db.removeEntryFromLeafSubPage(subPage, entryOffset, entrySize)
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
		err = db.updateEntryInLeafSubPage(subPage, entryOffset, entrySize, dataOffset)
		if err != nil {
			return fmt.Errorf("failed to update entry in sub-page: %w", err)
		}

		return nil
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
		// When FastRollback=false, db.Get() should see transaction changes
		// When FastRollback=true, db.Get() should not see transaction changes
		if db.fastRollback {
			maxReadSequence = db.txnSequence - 1
		} else {
			maxReadSequence = db.txnSequence
		}
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
			subPageInfo := &leafSubPage.Page.SubPages[leafSubPage.SubPageIdx]
			if subPageInfo.Offset == 0 {
				return nil, fmt.Errorf("sub-page with index %d not found", leafSubPage.SubPageIdx)
			}

			// Search for the suffix in this sub-page's entries
			entryOffset, _, dataOffset, err := db.findEntryInLeafSubPage(leafPage, leafSubPage.SubPageIdx, suffix)
			if err != nil {
				return nil, fmt.Errorf("failed to search entries in sub-page: %w", err)
			}

			if entryOffset >= 0 {
				// Found the entry, read the content from the main file
				content, err := db.readContent(dataOffset)
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

		// If we have a valid free radix page pointer
		if freeRadixPageNum > 0 {
			radixPage, err := db.getRadixPage(freeRadixPageNum)
			if err != nil {
				return fmt.Errorf("failed to get radix page: %w", err)
			}
			db.freeRadixPagesHead = radixPage.pageNumber
		}

		// The array of leaf pages with free space is parsed in parseHeaderPage
	}

	return nil
}

// writeIndexHeader writes metadata to the index file header
func (db *DB) writeIndexHeader(isInit bool) error {
	// Don't update if database is in read-only mode
	if db.readOnly {
		return nil
	}

	// Handle initialization separately for clarity
	if isInit {
		return db.initializeIndexHeader()
	}

	// Check which sequence number to use
	var maxReadSeq int64
	db.seqMutex.Lock()
	if db.inTransaction {
		maxReadSeq = db.flushSequence
	} else {
		maxReadSeq = db.txnSequence
	}
	db.seqMutex.Unlock()

	// Get the header page from cache
	headerPage, err := db.getPage(0, maxReadSeq)
	if err != nil {
		return fmt.Errorf("failed to get header page: %w", err)
	}

	data := headerPage.data

	// Update the header fields
	lastIndexedOffset := db.flushFileSize

	// Set last indexed offset (8 bytes)
	binary.LittleEndian.PutUint64(data[16:24], uint64(lastIndexedOffset))

	// Set free radix pages head pointer (4 bytes)
	nextFreeRadixPageNumber := uint32(0)
	if db.freeRadixPagesHead > 0 {
		nextFreeRadixPageNumber = db.freeRadixPagesHead
	}
	binary.LittleEndian.PutUint32(data[24:28], nextFreeRadixPageNumber)

	// Serialize the free leaf space array
	// Array count at offset 28 (2 bytes)
	arrayCount := len(headerPage.freeLeafSpaceArray)
	if arrayCount > MaxFreeSpaceEntries {
		arrayCount = MaxFreeSpaceEntries
	}
	binary.LittleEndian.PutUint16(data[28:30], uint16(arrayCount))

	// Serialize each entry (6 bytes each: 4 bytes page number + 2 bytes free space)
	for i := 0; i < arrayCount; i++ {
		offset := 30 + (i * 6)
		if offset+6 > len(data) {
			break // Avoid writing beyond page bounds
		}

		entry := headerPage.freeLeafSpaceArray[i]
		binary.LittleEndian.PutUint32(data[offset:offset+4], entry.PageNumber)
		binary.LittleEndian.PutUint16(data[offset+4:offset+6], entry.FreeSpace)
	}

	// Write the page to disk
	if err := db.writeIndexPage(headerPage); err != nil {
		return fmt.Errorf("failed to write index file header page: %w", err)
	}

	// Update the in-memory offset
	db.lastIndexedOffset = lastIndexedOffset

	return nil
}

// initializeIndexHeader creates and writes the initial index header
func (db *DB) initializeIndexHeader() error {
	// Allocate a buffer for the entire page
	data := make([]byte, PageSize)

	// Write the 6-byte magic string
	copy(data[0:6], IndexFileMagicString)

	// Write the 2-byte version
	copy(data[6:8], VersionString)

	// Write the 8-byte database ID
	binary.LittleEndian.PutUint64(data[8:16], db.databaseID)

	// Set initial last indexed offset (8 bytes)
	binary.LittleEndian.PutUint64(data[16:24], uint64(PageSize))

	// Set free radix pages head pointer (4 bytes)
	binary.LittleEndian.PutUint32(data[24:28], 0)

	// Initialize empty free leaf space array
	binary.LittleEndian.PutUint16(data[28:30], 0)

	// Set the file size to PageSize
	db.indexFileSize = PageSize

	// Create a Page struct for the header page
	headerPage := &Page{
		pageNumber:         0,
		data:               data,
		freeLeafSpaceArray: make([]FreeSpaceEntry, 0, MaxFreeSpaceEntries),
	}

	// Set the transaction sequence number
	headerPage.txnSequence = db.txnSequence

	// Update the access time
	db.accessCounter++
	headerPage.accessTime = db.accessCounter

	// Mark the page as dirty
	db.markPageDirty(headerPage)

	// Add to cache
	db.addToCache(headerPage)

	// Write the page to disk
	if err := db.writeIndexPage(headerPage); err != nil {
		return fmt.Errorf("failed to write initial index file header page: %w", err)
	}

	// Set the last indexed offset
	db.lastIndexedOffset = PageSize

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

	// Parse the free leaf space array
	// Array count is stored at offset 28 (2 bytes)
	arrayCount := int(binary.LittleEndian.Uint16(data[28:30]))

	// Initialize the array with the correct capacity
	freeLeafSpaceArray := make([]FreeSpaceEntry, 0, MaxFreeSpaceEntries)

	// Parse each entry (6 bytes each: 4 bytes page number + 2 bytes free space)
	for i := 0; i < arrayCount && i < MaxFreeSpaceEntries; i++ {
		offset := 30 + (i * 6)
		if offset+6 > len(data) {
			break // Avoid reading beyond header bounds
		}

		pageNumber := binary.LittleEndian.Uint32(data[offset:offset+4])
		freeSpace := binary.LittleEndian.Uint16(data[offset+4:offset+6])

		// Only add valid entries
		if pageNumber > 0 {
			freeLeafSpaceArray = append(freeLeafSpaceArray, FreeSpaceEntry{
				PageNumber: pageNumber,
				FreeSpace:  freeSpace,
			})
		}
	}

	// Create the header page
	headerPage := &Page{
		pageNumber: 0,
		data:       data,
		freeLeafSpaceArray: freeLeafSpaceArray,
		txnSequence: db.txnSequence,
	}

	// Update the access time
	db.accessCounter++
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
	contentSize := int(binary.LittleEndian.Uint16(data[2:4]))

	// Create structured leaf page
	leafPage := &LeafPage{
		pageNumber:  pageNumber,
		pageType:    ContentTypeLeaf,
		data:        data,
		dirty:       false,
		ContentSize: contentSize,
		SubPages:    make([]LeafSubPageInfo, 256),
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

// parseLeafSubPages parses the sub-pages in a leaf page (metadata only, not entries)
func (db *DB) parseLeafSubPages(leafPage *LeafPage) error {
	// Start at header size
	pos := LeafHeaderSize

	// If there are no sub-pages, nothing to do
	if leafPage.ContentSize == LeafHeaderSize {
		return nil
	}

	// Create slice with 256 entries to handle any sub-page ID (0-255)
	leafPage.SubPages = make([]LeafSubPageInfo, 256)

	// Read each sub-page until we reach the content size
	for pos < leafPage.ContentSize {
		// Read sub-page ID (this is the index where we'll store this sub-page)
		subPageID := leafPage.data[pos]
		pos++

		// Read sub-page size
		subPageSize := binary.LittleEndian.Uint16(leafPage.data[pos:pos+2])
		pos += 2

		// Calculate the end of this sub-page
		subPageEnd := pos + int(subPageSize)

		// Ensure we don't read past the content size
		if subPageEnd > leafPage.ContentSize {
			return fmt.Errorf("sub-page end %d exceeds content size %d", subPageEnd, leafPage.ContentSize)
		}

		// Create and store the sub-page info at the index corresponding to its ID
		leafPage.SubPages[subPageID] = LeafSubPageInfo{
			Offset:  uint16(pos - 3), // Include the header in the offset
			Size:    subPageSize,
		}

		// Move to the next sub-page
		pos = subPageEnd
	}

	return nil
}

// iterateLeafSubPageEntries iterates through entries in a leaf sub-page, calling the callback for each entry
// The callback receives the entry offset (relative to leaf page data), entry size, and entry data
// Returns true to continue or false to stop
func (db *DB) iterateLeafSubPageEntries(leafPage *LeafPage, subPageIdx uint8, callback func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool) error {
	// Get the sub-page info
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := &leafPage.SubPages[subPageIdx]

	// Calculate the start and end positions for this sub-page's data
	subPageDataStart := int(subPageInfo.Offset) + 3 // Skip 3-byte header
	subPageDataEnd := subPageDataStart + int(subPageInfo.Size)

	pos := subPageDataStart

	// Read entries until we reach the end of the sub-page
	for pos < subPageDataEnd {
		// Store the entry offset relative to the leaf page data
		entryOffset := pos

		// Read suffix length
		suffixLen64, bytesRead := varint.Read(leafPage.data[pos:])
		if bytesRead == 0 {
			return fmt.Errorf("failed to read suffix length")
		}
		suffixLen := int(suffixLen64)
		pos += bytesRead

		// Store the suffix offset relative to the leaf page data
		suffixOffset := pos
		pos += suffixLen

		// Check if we have enough space for the data offset
		if pos+8 > subPageDataEnd {
			return fmt.Errorf("insufficient space for data offset")
		}

		// Read data offset
		dataOffset := int64(binary.LittleEndian.Uint64(leafPage.data[pos:]))
		pos += 8

		// Calculate entry size
		entrySize := bytesRead + suffixLen + 8 // varint + suffix + data offset

		// Call the callback with the entry information
		if !callback(entryOffset, entrySize, suffixOffset, suffixLen, dataOffset) {
			break
		}
	}

	return nil
}

// findEntryInLeafSubPage finds an entry in a leaf sub-page that matches the given suffix
// Returns the entry offset (relative to leaf page data), entry size, and data offset if found
// Returns -1, 0, 0 if not found
func (db *DB) findEntryInLeafSubPage(leafPage *LeafPage, subPageIdx uint8, targetSuffix []byte) (int, int, int64, error) {
	var foundEntryOffset int = -1
	var foundEntrySize int = 0
	var foundDataOffset int64 = 0

	err := db.iterateLeafSubPageEntries(leafPage, subPageIdx, func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
		// Compare directly with target suffix (no copy needed)
		if suffixLen == len(targetSuffix) && bytes.Equal(leafPage.data[suffixOffset:suffixOffset+suffixLen], targetSuffix) {
			foundEntryOffset = entryOffset
			foundEntrySize   = entrySize
			foundDataOffset  = dataOffset
			return false // Stop iteration
		}

		return true // Continue iteration
	})

	return foundEntryOffset, foundEntrySize, foundDataOffset, err
}

// writeLeafPage writes a leaf page to the database file
func (db *DB) writeLeafPage(leafPage *LeafPage) error {
	// Set page type in the data
	leafPage.data[0] = ContentTypeLeaf  // Type identifier

	// Write content size
	binary.LittleEndian.PutUint16(leafPage.data[2:4], uint16(leafPage.ContentSize))

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
	// Read the page data
	data, err := db.readFromIndexFile(pageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %w", err)
	}

	// Get the page type
	contentType := data[0]

	// Parse the page based on the page type (or page number for header page)
	if contentType == ContentTypeRadix {
		return db.parseRadixPage(data, pageNumber)
	} else if contentType == ContentTypeLeaf {
		return db.parseLeafPage(data, pageNumber)
	} else if pageNumber == 0 {
		return db.parseHeaderPage(data)
	}

	return nil, fmt.Errorf("unknown page type: %c", contentType)
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

	// For fast rollback and slower writes, clone pages at every transaction
	if db.fastRollback {
		db.cloningSequence = db.txnSequence - 1
	// For fast writes and slow rollback, clone pages at every 1000 transactions
	} else {
		// If the page is marked to be flushed, we cannot modify its data (force cloning)
		if db.cloningSequence < db.flushSequence {
			db.cloningSequence = db.flushSequence
		}
		if db.txnSequence > db.cloningSequence + 1000 {
			db.cloningSequence = db.txnSequence - 1
			db.cloningFileSize = db.mainFileSize
		}
	}

	// Store the current main file size to enable rollback (to truncate the main file)
	db.prevFileSize = db.mainFileSize

	// Reset the transaction checksum
	db.txnChecksum = 0

	db.seqMutex.Unlock()


	// Get the header page
	headerPage, err := db.getPage(0)
	if err != nil {
		debugPrint("Failed to get header page: %v\n", err)
		return
	}

	// Get a writable version of the header page
	headerPage, err = db.getWritablePage(headerPage)
	if err != nil {
		debugPrint("Failed to get writable header page: %v\n", err)
		return
	}

	// Initialize the array if needed
	if headerPage.freeLeafSpaceArray == nil {
		headerPage.freeLeafSpaceArray = make([]FreeSpaceEntry, 0, MaxFreeSpaceEntries)
	}

	// Mark the header page as dirty
	db.markPageDirty(headerPage)

	// Store the header page for transaction
	db.headerPageForTransaction = headerPage

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

	// If in caller thread mode, flush to disk
	if db.commitMode == CallerThread {
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

	if db.fastRollback {
		// Fast rollback: discard pages from this transaction only
		db.discardNewerPages(db.txnSequence)
	} else {
		// Slow rollback: discard pages newer than the cloning mark and reindex
		db.discardNewerPages(db.cloningSequence + 1)

		// Get the last indexed offset
		lastIndexedOffset := db.cloningFileSize

		// Reindex the new content from the main file (incremental reindexing)
		if lastIndexedOffset < db.mainFileSize {
			err := db.reindexContent(lastIndexedOffset)
			if err != nil {
				debugPrint("Failed to reindex content during rollback: %v\n", err)
			}
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
	bucket.mutex.Lock()
	page, exists := bucket.pages[pageNumber]
	if exists {
		callback(bucket, pageNumber, page)
	}
	bucket.mutex.Unlock()
}

// iteratePages iterates through all pages in the cache and calls a callback/lambda function with the page while the lock is held
func (db *DB) iteratePages(callback func(*cacheBucket, uint32, *Page)) {
	for bucketIdx := 0; bucketIdx < 1024; bucketIdx++ {
		bucket := &db.pageCache[bucketIdx]
		bucket.mutex.Lock()
		// Iterate through all pages in this bucket
		for pageNumber, page := range bucket.pages {
			callback(bucket, pageNumber, page)
		}
		bucket.mutex.Unlock()
	}
}

// getWritablePage gets a writable version of a page
// if the given page is already writable, it returns the page itself
func (db *DB) getWritablePage(page *Page) (*Page, error) {
	// We cannot write to a page that is part of the WAL
	needsClone := page.isWAL
	// If the page is below the cloning mark, we need to clone it
	if page.txnSequence <= db.cloningSequence {
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
	} else if page.pageNumber == 0 {
		newPage, err = db.cloneHeaderPage(page)
		if err != nil {
			return nil, fmt.Errorf("failed to clone header page: %w", err)
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
		shouldFlush := true

		db.seqMutex.Lock()
		if db.fastRollback {
			// If already flushed up to the previous transaction, skip
			if db.inTransaction && db.flushSequence == db.txnSequence - 1 {
				shouldFlush = false
			}
		} else {
			// If already flushed up to the cloning mark, skip
			if db.inTransaction && db.flushSequence == db.cloningSequence {
				shouldFlush = false
			}
		}
		db.seqMutex.Unlock()

		if shouldFlush {
			// When the commit mode is caller thread it flushes on every commit
			// When it is worker thread, it flushes here
			if db.commitMode == WorkerThread {
				// Signal the worker thread to flush the pages, if not already signaled
				db.seqMutex.Lock()
				if !db.pendingCommands["flush"] {
					db.pendingCommands["flush"] = true
					db.workerChannel <- "flush"
				}
				db.seqMutex.Unlock()
				return
			}
		}
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
	debugPrint("Discarding pages from transaction %d and above\n", currentSeq)
	// Iterate through all pages in the cache
	db.iteratePages(func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		// Skip pages from the current transaction
		// Find the first page that's not from the current transaction
		var newHead *Page = page
		var removedCount int64 = 0

		for newHead != nil && newHead.txnSequence >= currentSeq {
			debugPrint("Discarding page %d from transaction %d\n", newHead.pageNumber, newHead.txnSequence)
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
			debugPrint("Keeping page %d from transaction %d\n", newHead.pageNumber, newHead.txnSequence)
			bucket.pages[pageNumber] = newHead
		} else {
			debugPrint("No pages left for page %d\n", pageNumber)
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
	var limitSequence int64
	if db.fastRollback {
		// If the main thread is in a transaction
		if db.inTransaction {
			// Keep pages from the previous transaction (because the current one can be rolled back)
			limitSequence = db.txnSequence - 1
		} else {
			// Otherwise, keep pages from the last committed transaction
			limitSequence = db.txnSequence
		}
	} else {
		// Keep pages below the cloning mark (because all pages after this mark can be rolled back)
		limitSequence = db.cloningSequence + 1
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

			// Skip pages above the limit sequence - they should not be touched
			if current.txnSequence >= limitSequence {
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
	var limitSequence int64
	if db.fastRollback {
		// If the main thread is in a transaction
		if db.inTransaction {
			// Keep pages from the previous transaction (because the current one can be rolled back)
			limitSequence = db.txnSequence - 1
		} else {
			// Otherwise, keep pages from the last committed transaction
			limitSequence = db.txnSequence
		}
	} else {
		// Keep pages below the cloning mark (because all pages after this mark can be rolled back)
		limitSequence = db.cloningSequence + 1
	}
	lastAccessTime := db.accessCounter
	db.seqMutex.Unlock()

	// Collect removable pages from each bucket
	db.iteratePages(func(bucket *cacheBucket, pageNumber uint32, page *Page) {
		// Skip dirty pages, WAL pages, and pages above the limit sequence
		if page.dirty || page.isWAL || page.txnSequence >= limitSequence {
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
			if page.dirty || page.isWAL || page.txnSequence >= limitSequence {
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

	// Set flush sequence number limit and determine the appropriate main file size for this flush
	db.seqMutex.Lock()
	// If a transaction is in progress
	if db.inTransaction {
		// When fast rollback is enabled
		if db.fastRollback {
			// Flush pages from the previous transaction
			db.flushSequence = db.txnSequence - 1
			// For worker thread flushes during transactions, use the file size from before the current transaction
			// This ensures we only index content that has been committed
			db.flushFileSize = db.prevFileSize
		// When fast rollback is disabled
		} else {
			// Flush at the cloning sequence number
			db.flushSequence = db.cloningSequence
			// Use the file size from when the cloning mark was set
			db.flushFileSize = db.cloningFileSize
		}
	// When no transaction is in progress
	} else {
		// Flush at the current transaction sequence number
		db.flushSequence = db.txnSequence
		// Use the current main file size
		db.flushFileSize = db.mainFileSize
	}
	db.seqMutex.Unlock()

	debugPrint("Flushing index to disk. Flush sequence: %d, Transaction sequence: %d\n", db.flushSequence, db.txnSequence)

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

	// Update last flush time on successful flush
	db.lastFlushTime = time.Now()

	return nil
}

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
		SubPages:    make([]LeafSubPageInfo, 256),
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
	debugPrint("Allocating leaf page with enough space: %d bytes\n", spaceNeeded)

	// Find a page with enough space
	pageNumber, freeSpace, position := db.findLeafPageWithSpace(spaceNeeded)

	if pageNumber > 0 {
		debugPrint("Found page %d with %d bytes free space\n", pageNumber, freeSpace)

		// Get the page
		leafPage, err := db.getLeafPage(pageNumber)
		if err != nil {
			debugPrint("Failed to get leaf page %d: %v\n", pageNumber, err)
			// Page not found, remove from array and try again
			db.removeFromFreeSpaceArray(position, pageNumber)
			return db.allocateLeafPageWithSpace(spaceNeeded)
		}

		// Get a writable version of the page
		leafPage, err = db.getWritablePage(leafPage)
		if err != nil {
			return nil, fmt.Errorf("failed to get writable page: %w", err)
		}

		// Get the current free space directly from the page
		freeSpace = PageSize - leafPage.ContentSize
		if freeSpace < spaceNeeded {
			debugPrint("Page %d has %d bytes free space, but %d bytes are needed\n", leafPage.pageNumber, freeSpace, spaceNeeded)
			// Remove the page from the free list
			db.updateFreeSpaceArray(position, leafPage.pageNumber, freeSpace)
			return db.allocateLeafPageWithSpace(spaceNeeded)
		}

		// Find the first available sub-page ID
		var subPageID uint8 = 0
		// Find first available slot
		for subPageID < 255 && leafPage.SubPages[subPageID].Offset != 0 {
			subPageID++
		}
		if subPageID == 255 && leafPage.SubPages[subPageID].Offset != 0 {
			debugPrint("No available sub-page IDs, removing page %d from free array\n", leafPage.pageNumber)
			// No available sub-page IDs, remove this page from the free array and try again
			db.removeFromFreeSpaceArray(position, leafPage.pageNumber)
			return db.allocateLeafPageWithSpace(spaceNeeded)
		}

		// Count how many sub-page slots are currently used (continue from the last found slot)
		usedSlots := int(subPageID) + 1
		for i := usedSlots; i < 256; i++ {
			if leafPage.SubPages[i].Offset != 0 {
				usedSlots++
			}
		}

		// Calculate the free space after adding the content
		freeSpaceAfter := freeSpace - spaceNeeded

		// if the free space is less than MIN_FREE_SPACE or there are no more available slots
		if freeSpaceAfter < MIN_FREE_SPACE || usedSlots == 256 {
			// Remove the page from the free list
			db.removeFromFreeSpaceArray(position, leafPage.pageNumber)
		} else {
			// Update the free space array with the new free space amount
			// This will automatically remove the page if free space is too low
			db.updateFreeSpaceArray(position, leafPage.pageNumber, freeSpaceAfter)
		}

		return &LeafSubPage{
			Page:       leafPage,
			SubPageIdx: subPageID,
		}, nil
	}

	// No suitable page found in the free list, allocate a new one
	newLeafPage, err := db.allocateLeafPage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate new leaf page: %w", err)
	}
	debugPrint("Allocated new leaf page %d\n", newLeafPage.pageNumber)

	// Add the new page to the free list since it will have free space after allocation
	freeSpaceAfter := PageSize - newLeafPage.ContentSize - spaceNeeded
	db.addToFreeLeafPagesList(newLeafPage, freeSpaceAfter)

	// Use sub-page ID 0 for the first sub-page in a new leaf page
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
		SubPages:     make([]LeafSubPageInfo, 256),
	}

	// Copy the data
	copy(newPage.data, page.data)

	// Copy the slice of sub-pages
	copy(newPage.SubPages, page.SubPages)

	// Add to cache
	db.addToCache(newPage)

	return newPage, nil
}

// cloneHeaderPage clones the header page
func (db *DB) cloneHeaderPage(page *Page) (*Page, error) {
	// Create a new page
	newPage := &Page{
		pageNumber:   page.pageNumber,
		pageType:     page.pageType,
		data:         make([]byte, PageSize),
		dirty:        page.dirty,
		isWAL:        false,
		accessTime:   page.accessTime,
		txnSequence:  page.txnSequence,
	}

	// Copy the data
	copy(newPage.data, page.data)

	// Deep copy the free leaf space array if it exists
	if page.freeLeafSpaceArray != nil {
		newPage.freeLeafSpaceArray = make([]FreeSpaceEntry, len(page.freeLeafSpaceArray))
		copy(newPage.freeLeafSpaceArray, page.freeLeafSpaceArray)
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
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx].Offset == 0 {
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

// addEntryToNewLeafSubPage creates a new sub-page, adds an entry to it, then search for a leaf page with enough space to insert the sub-page into
func (db *DB) addEntryToNewLeafSubPage(suffix []byte, dataOffset int64) (*LeafSubPage, error) {

	// Step 1: Compute the space requirements for the new sub-page
	suffixLenSize := varint.Size(uint64(len(suffix)))
	entrySize := suffixLenSize + len(suffix) + 8 // suffix length + suffix + data offset
	subPageSize := uint16(entrySize) // Size of the data (excluding the 3-byte header)
	totalSubPageSize := LeafSubPageHeaderSize + int(subPageSize) // 3 bytes header + data

	// Step 2: Allocate a leaf page with enough space
	leafSubPage, err := db.allocateLeafPageWithSpace(totalSubPageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate leaf sub-page: %w", err)
	}

	leafPage := leafSubPage.Page
	subPageID := leafSubPage.SubPageIdx

	// Verify there's enough space (this should always be true after allocateLeafPageWithSpace)
	if leafPage.ContentSize + totalSubPageSize > PageSize {
		return nil, fmt.Errorf("sub-page too large to fit in a leaf page")
	}

	// Step 3: Write data directly into the leaf page
	// Calculate the offset where the sub-page will be placed
	offset := leafPage.ContentSize

	// Write the sub-page header directly to the leaf page
	leafPage.data[offset] = subPageID                                              // Sub-page ID   (1 byte)
	binary.LittleEndian.PutUint16(leafPage.data[offset+1:offset+3], subPageSize)   // Sub-page size (2 bytes)

	// Write the entry data directly after the header
	dataPos := offset + 3

	// Write suffix length directly
	suffixLenWritten := varint.Write(leafPage.data[dataPos:], uint64(len(suffix)))
	dataPos += suffixLenWritten

	// Write suffix directly
	copy(leafPage.data[dataPos:], suffix)
	dataPos += len(suffix)

	// Write data offset directly
	binary.LittleEndian.PutUint64(leafPage.data[dataPos:], uint64(dataOffset))

	// Step 4: Update the leaf page metadata
	// Update the content size
	leafPage.ContentSize += totalSubPageSize

	// Create the LeafSubPageInfo and add it to the leaf page
	subPageInfo := LeafSubPageInfo{
		Offset:  uint16(offset),
		Size:    subPageSize,
	}

	// Store the sub-page at the index corresponding to its ID
	leafPage.SubPages[subPageID] = subPageInfo

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	// Step 5: Return the LeafSubPage reference
	return &LeafSubPage{
		Page:       leafPage,
		SubPageIdx: subPageID,
	}, nil
}

// addEntryToLeafSubPage adds an entry to a specific leaf sub-page
func (db *DB) addEntryToLeafSubPage(parentSubPage *RadixSubPage, parentByteValue uint8, subPage *LeafSubPage, suffix []byte, dataOffset int64) error {
	leafPage := subPage.Page
	subPageIdx := subPage.SubPageIdx

	// Get a writable version of the page
	leafPage, err := db.getWritablePage(leafPage)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = leafPage

	// Get the sub-page info
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := &leafPage.SubPages[subPageIdx]

	// Calculate the size needed for the new entry
	suffixLenSize := varint.Size(uint64(len(suffix)))
	newEntrySize := suffixLenSize + len(suffix) + 8 // suffix length + suffix + data offset

	// Calculate the total size needed for the updated sub-page
	newSubPageSize := int(subPageInfo.Size) + newEntrySize

	// Check if there's enough space in the current page for the updated sub-page
	if leafPage.ContentSize + newEntrySize > PageSize {
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
	// Step 1: Get the offset of the current sub-page (end of existing entries)
	currentSubPageStart := int(subPageInfo.Offset) + 3 // Skip header
	currentSubPageEnd := currentSubPageStart + int(subPageInfo.Size)

	// Step 2: Move data (all subsequent sub-pages after the current) to open space for the new entry
	if currentSubPageEnd < leafPage.ContentSize {
		// Move all data after this sub-page to make room for the new entry
		copy(leafPage.data[currentSubPageEnd+newEntrySize:], leafPage.data[currentSubPageEnd:leafPage.ContentSize])
	}

	// Step 3: Serialize the new entry directly in the opened space
	entryPos := currentSubPageEnd

	// Write suffix length directly
	suffixLenWritten := varint.Write(leafPage.data[entryPos:], uint64(len(suffix)))
	entryPos += suffixLenWritten

	// Write suffix directly
	copy(leafPage.data[entryPos:], suffix)
	entryPos += len(suffix)

	// Write data offset directly
	binary.LittleEndian.PutUint64(leafPage.data[entryPos:], uint64(dataOffset))

	// Step 4: Update metadata
	// Update the sub-page size in the header
	newSubPageSizeUint16 := uint16(newSubPageSize)
	binary.LittleEndian.PutUint16(leafPage.data[subPageInfo.Offset+1:subPageInfo.Offset+3], newSubPageSizeUint16)

	// Update the sub-page info
	subPageInfo.Size = newSubPageSizeUint16

	// Update offsets for sub-pages that come after this one
	for i := 0; i < len(leafPage.SubPages); i++ {
		if leafPage.SubPages[i].Offset != 0 && leafPage.SubPages[i].Offset > subPageInfo.Offset {
			leafPage.SubPages[i].Offset += uint16(newEntrySize)
		}
	}

	// Update the leaf page content size
	leafPage.ContentSize += newEntrySize

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	// Add the leaf page to the free list if it has reasonable free space
	db.addToFreeLeafPagesList(leafPage, 0)

	return nil
}

// removeEntryFromLeafSubPage removes an entry from a leaf sub-page at the given offset
func (db *DB) removeEntryFromLeafSubPage(subPage *LeafSubPage, entryOffset int, entrySize int) error {
	// Get a writable version of the page
	leafPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = leafPage

	// Get the sub-page info
	subPageIdx := subPage.SubPageIdx
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := &leafPage.SubPages[subPageIdx]

	// Calculate positions (entryOffset is already relative to leaf page data)
	entryEnd := entryOffset + entrySize

	// Single copy: move all data after the removed entry to fill the gap
	// This includes both data within the sub-page and all data after the sub-page
	if entryEnd < leafPage.ContentSize {
		copy(leafPage.data[entryOffset:], leafPage.data[entryEnd:leafPage.ContentSize])
	}

	// Update the sub-page size
	newSubPageSize := int(subPageInfo.Size) - entrySize
	subPageInfo.Size = uint16(newSubPageSize)

	// Update the sub-page size in the header
	binary.LittleEndian.PutUint16(leafPage.data[int(subPageInfo.Offset)+1:int(subPageInfo.Offset)+3], uint16(newSubPageSize))

	// Update offsets for sub-pages that come after this one
	for i := 0; i < len(leafPage.SubPages); i++ {
		if leafPage.SubPages[i].Offset != 0 && leafPage.SubPages[i].Offset > uint16(entryOffset) {
			leafPage.SubPages[i].Offset -= uint16(entrySize)
		}
	}

	// Update the leaf page content size
	leafPage.ContentSize -= entrySize

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	// Add the leaf page to the free list if it has reasonable free space
	db.addToFreeLeafPagesList(leafPage, 0)

	return nil
}

// updateEntryInLeafSubPage updates the data offset of an entry in a leaf sub-page
func (db *DB) updateEntryInLeafSubPage(subPage *LeafSubPage, entryOffset int, entrySize int, dataOffset int64) error {
	// Get a writable version of the page
	leafPage, err := db.getWritablePage(subPage.Page)
	if err != nil {
		return fmt.Errorf("failed to get writable page: %w", err)
	}
	// Update the subPage reference to point to the writable page
	subPage.Page = leafPage

	// Calculate the position where the data offset is stored (last 8 bytes of the entry)
	dataOffsetPosition := entryOffset + entrySize - 8

	// Update the data offset in-place
	binary.LittleEndian.PutUint64(leafPage.data[dataOffsetPosition:], uint64(dataOffset))

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	return nil
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
	leafPage := subPage.Page
	subPageIdx := subPage.SubPageIdx

	debugPrint("Converting leaf sub-page %d on page %d to radix sub-page\n", subPageIdx, leafPage.pageNumber)

	// Check if the sub-page exists
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}

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

	err = db.iterateLeafSubPageEntries(leafPage, subPageIdx, func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
		// Get the suffix from the entry (suffixOffset is already relative to leaf page data)
		suffix := make([]byte, suffixLen)
		copy(suffix, leafPage.data[suffixOffset:suffixOffset+suffixLen])

		existingEntries = append(existingEntries, struct {
			suffix []byte
			dataOffset int64
		}{
			suffix: suffix,
			dataOffset: dataOffset,
		})
		return true // Continue iteration
	})
	if err != nil {
		return fmt.Errorf("failed to iterate existing entries: %w", err)
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

	// Remove the old sub-page from the leaf page
	db.removeSubPageFromLeafPage(leafPage, subPageIdx)

	// Update the sub-page to point to the new radix sub-page
	subPage.Page = newRadixSubPage.Page
	subPage.SubPageIdx = newRadixSubPage.SubPageIdx

	return nil
}

// moveSubPageToNewLeafPage moves a leaf sub-page to a new leaf page when it doesn't fit in the current page
// but is still small enough to fit in a new empty leaf page
func (db *DB) moveSubPageToNewLeafPage(subPage *LeafSubPage, newSuffix []byte, newDataOffset int64) error {
	leafPage := subPage.Page
	subPageIdx := subPage.SubPageIdx

	debugPrint("Moving leaf sub-page %d from page %d to new leaf page\n", subPageIdx, leafPage.pageNumber)

	// Get the sub-page info
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx].Offset == 0 {
		return fmt.Errorf("sub-page with index %d not found", subPageIdx)
	}
	subPageInfo := &leafPage.SubPages[subPageIdx]

	// Step 1: Compute the total new space needed
	suffixLenSize := varint.Size(uint64(len(newSuffix)))
	newEntrySize := suffixLenSize + len(newSuffix) + 8
	newSubPageSize := int(subPageInfo.Size) + newEntrySize
	totalSubPageSize := LeafSubPageHeaderSize + newSubPageSize

	// Step 2: Get leaf page with enough space
	newLeafSubPage, err := db.allocateLeafPageWithSpace(totalSubPageSize)
	if err != nil {
		return fmt.Errorf("failed to allocate new leaf sub-page: %w", err)
	}

	newLeafPage := newLeafSubPage.Page
	newSubPageID := newLeafSubPage.SubPageIdx

	// Step 3: Copy sub-page directly from page A to page B (at the end)
	// Calculate the offset where the sub-page will be placed in the new leaf page
	offset := int(newLeafPage.ContentSize)

	// Write the sub-page header directly to the new leaf page
	newLeafPage.data[offset] = newSubPageID // Sub-page ID
	binary.LittleEndian.PutUint16(newLeafPage.data[offset+1:offset+3], uint16(newSubPageSize))

	// Copy existing entries directly from the original sub-page to the new page
	originalStart := int(subPageInfo.Offset) + 3 // Skip header
	dataPos := offset + 3 // Skip new header
	copy(newLeafPage.data[dataPos:], leafPage.data[originalStart:originalStart+int(subPageInfo.Size)])
	dataPos += int(subPageInfo.Size)

	// Step 4: Serialize the new entry directly in the new page
	// Write suffix length directly
	suffixLenWritten := varint.Write(newLeafPage.data[dataPos:], uint64(len(newSuffix)))
	dataPos += suffixLenWritten

	// Write suffix directly
	copy(newLeafPage.data[dataPos:], newSuffix)
	dataPos += len(newSuffix)

	// Write data offset directly
	binary.LittleEndian.PutUint64(newLeafPage.data[dataPos:], uint64(newDataOffset))

	// Update the new leaf page metadata
	newLeafPage.ContentSize += totalSubPageSize

	// Create the LeafSubPageInfo and add it to the new leaf page
	newLeafPage.SubPages[newSubPageID] = LeafSubPageInfo{
		Offset:  uint16(offset),
		Size:    uint16(newSubPageSize),
	}

	// Mark the new page as dirty
	db.markPageDirty(newLeafPage)

	// Remove the sub-page from the original leaf page
	db.removeSubPageFromLeafPage(leafPage, subPageIdx)

	// Update the subPage reference to point to the new leaf page
	subPage.Page = newLeafPage
	subPage.SubPageIdx = newSubPageID

	return nil
}

// removeSubPageFromLeafPage removes a sub-page from a leaf page
func (db *DB) removeSubPageFromLeafPage(leafPage *LeafPage, subPageIdx uint8) {
	// Get the sub-page info
	if int(subPageIdx) >= len(leafPage.SubPages) || leafPage.SubPages[subPageIdx].Offset == 0 {
		return // Sub-page doesn't exist, nothing to remove
	}
	subPageInfo := &leafPage.SubPages[subPageIdx]

	// Calculate the sub-page boundaries
	subPageStart := int(subPageInfo.Offset)
	subPageSize := LeafSubPageHeaderSize + int(subPageInfo.Size)
	subPageEnd := subPageStart + subPageSize

	// Move all data after this sub-page to fill the gap
	if subPageEnd < leafPage.ContentSize {
		copy(leafPage.data[subPageStart:], leafPage.data[subPageEnd:leafPage.ContentSize])
	}

	// Update offsets for sub-pages that come after this one
	for i := 0; i < len(leafPage.SubPages); i++ {
		if leafPage.SubPages[i].Offset != 0 && leafPage.SubPages[i].Offset > subPageInfo.Offset {
			leafPage.SubPages[i].Offset -= uint16(subPageSize)
		}
	}

	// Remove the sub-page from the array by clearing its offset
	leafPage.SubPages[subPageIdx] = LeafSubPageInfo{}

	// Update content size
	leafPage.ContentSize -= subPageSize

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	// Add the leaf page to the free list if it has reasonable free space
	db.addToFreeLeafPagesList(leafPage, 0)
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

// addToFreeLeafPagesList adds a leaf page with free space to the array
func (db *DB) addToFreeLeafPagesList(leafPage *LeafPage, freeSpace int) {
	db.addToFreeSpaceArray(leafPage, freeSpace)
}

// ------------------------------------------------------------------------------------------------
// Free space array management
// ------------------------------------------------------------------------------------------------

// addToFreeSpaceArray adds or updates a leaf page in the free space array
func (db *DB) addToFreeSpaceArray(leafPage *LeafPage, freeSpace int) {

	if freeSpace == 0 {
		// Calculate free space
		freeSpace = PageSize - leafPage.ContentSize
	}

	if leafPage.ContentSize > PageSize {
		debugPrint("PANIC_CONDITION_MET: Page %d has content size %d which is greater than PageSize %d\n", leafPage.pageNumber, leafPage.ContentSize, PageSize)
		// Panic to capture the stack trace
		panic(fmt.Sprintf("Page %d has content size greater than page size: %d", leafPage.pageNumber, leafPage.ContentSize))
	}

	// Only add if the page has reasonable free space (at least MIN_FREE_SPACE bytes)
	if freeSpace < MIN_FREE_SPACE {
		return
	}

	debugPrint("Adding page %d to free space array with %d bytes of free space\n", leafPage.pageNumber, freeSpace)

	// Get the header page
	headerPage := db.headerPageForTransaction

	// Find the entry with minimum free space by iterating through the array
	minFreeSpace := uint16(PageSize) // Start with max possible value
	minIndex := -1

	// Iterate through the array
	for i, entry := range headerPage.freeLeafSpaceArray {
		// Look for existing entry
		if entry.PageNumber == leafPage.pageNumber {
			// If found, update existing entry
			headerPage.freeLeafSpaceArray[i].FreeSpace = uint16(freeSpace)
			return
		}
		// Find the entry with minimum free space
		if entry.FreeSpace < minFreeSpace {
			minFreeSpace = entry.FreeSpace
			minIndex = i
		}
	}

	// Add new entry
	newEntry := FreeSpaceEntry{
		PageNumber: leafPage.pageNumber,
		FreeSpace:  uint16(freeSpace),
	}

	// If array is full, remove the entry with least free space
	if len(headerPage.freeLeafSpaceArray) >= MaxFreeSpaceEntries {
		// Only add if new entry has more free space than the minimum found
		if uint16(freeSpace) > minFreeSpace {
			// Replace the entry with the new entry
			headerPage.freeLeafSpaceArray[minIndex] = newEntry
			return
		} else {
			// Don't add this entry
			return
		}
	}

	// Add the new entry
	headerPage.freeLeafSpaceArray = append(headerPage.freeLeafSpaceArray, newEntry)
}

// removeFromFreeSpaceArray removes a leaf page from the free space array
func (db *DB) removeFromFreeSpaceArray(position int, pageNumber uint32) {
	debugPrint("Removing page %d from free space array\n", pageNumber)

	// Get the header page
	headerPage := db.headerPageForTransaction

	// Replace this entry with the last entry (to avoid memory move)
	arrayLen := len(headerPage.freeLeafSpaceArray)
	if position >= 0 && position < arrayLen {
		// Copy the last element to this position
		headerPage.freeLeafSpaceArray[position] = headerPage.freeLeafSpaceArray[arrayLen-1]
		// Shrink the array
		headerPage.freeLeafSpaceArray = headerPage.freeLeafSpaceArray[:arrayLen-1]
	}
}

// findLeafPageWithSpace finds a leaf page with at least the specified amount of free space
// Returns the page number and the amount of free space, or 0 if no suitable page is found
func (db *DB) findLeafPageWithSpace(spaceNeeded int) (uint32, int, int) {
	debugPrint("Finding leaf page with space: %d\n", spaceNeeded)

	// Get the header page
	headerPage := db.headerPageForTransaction

	// Optimization: iterate forward for better cache locality
	// Find the best fit (page with just enough space)
	bestFitPageNumber := uint32(0)
	bestFitSpace := PageSize + 1 // Start with a value larger than any possible free space
	bestFitPosition := -1

	// First pass: look for a page with exactly enough space or slightly more
	for position, entry := range headerPage.freeLeafSpaceArray {
		entrySpace := int(entry.FreeSpace)
		// If this is a better fit than what we've found so far
		if entrySpace >= spaceNeeded && entrySpace < bestFitSpace {
			bestFitPageNumber = entry.PageNumber
			bestFitSpace = entrySpace
			bestFitPosition = position
		}
	}

	// If we found any page with enough space, return it
	if bestFitPageNumber > 0 {
		return bestFitPageNumber, bestFitSpace, bestFitPosition
	}

	// No page found with enough space
	return 0, 0, 0
}

// updateFreeSpaceArray updates the free space for a specific page in the array
func (db *DB) updateFreeSpaceArray(position int, pageNumber uint32, newFreeSpace int) {
	debugPrint("Updating free space array for page %d to %d\n", pageNumber, newFreeSpace)

	// If free space is too low, just remove the entry
	if newFreeSpace < MIN_FREE_SPACE {
		db.removeFromFreeSpaceArray(position, pageNumber)
		return
	}

	// Get the header page
	headerPage := db.headerPageForTransaction

	// Update the entry
	headerPage.freeLeafSpaceArray[position].FreeSpace = uint16(newFreeSpace)
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
	db.beginTransaction()

	// Reindex the content
	err = db.reindexContent(lastIndexedOffset)

	if err != nil {
		db.rollbackTransaction()
		return fmt.Errorf("failed to reindex content: %w", err)
	}

	// Commit the transaction
	db.commitTransaction()

	if !db.readOnly {
		// Flush the index pages to disk
		if err := db.flushIndexToDisk(); err != nil {
			return fmt.Errorf("failed to flush index to disk: %w", err)
		}
	}

	debugPrint("Recovery complete, reindexed content up to offset %d\n", db.mainFileSize)
	return nil
}

func (db *DB) reindexContent(lastIndexedOffset int64) error {

	// If the last indexed offset is before the header page, set it to the header page
	if lastIndexedOffset < int64(PageSize) {
		lastIndexedOffset = int64(PageSize)
	}

	debugPrint("Reindexing content from offset %d to %d\n", lastIndexedOffset, db.mainFileSize)

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
			debugPrint("Reindexing data at offset %d - key: %s, value: %s\n", currentOffset, content.key, content.value)
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

		// Timer for periodic flush (15 seconds)
		flushInterval := 15 * time.Second
		timer := time.NewTimer(flushInterval)
		defer timer.Stop()

		for {
			select {
			case cmd, ok := <-db.workerChannel:
				if !ok {
					// Channel closed, exit
					return
				}

				switch cmd {
				case "flush":
					db.flushIndexToDisk()
					// Clear the pending command flag
					db.seqMutex.Lock()
					delete(db.pendingCommands, "flush")
					db.seqMutex.Unlock()

					// Reset timer after manual flush
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(flushInterval)

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

			case <-timer.C:
				// Timer expired, check if we need to flush
				timeSinceLastFlush := time.Since(db.lastFlushTime)
				if timeSinceLastFlush >= flushInterval {
					// Only flush if there are dirty pages or if it's been a while
					if db.dirtyPageCount > 0 {
						debugPrint("Timer-based flush triggered after %v\n", timeSinceLastFlush)
						db.flushIndexToDisk()
					}
				}
				// Reset timer for next interval
				timer.Reset(flushInterval)
			}
		}
	}()
}
