package kv_log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"

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

// debugPrint prints a message if debug mode is enabled
func debugPrint(format string, args ...interface{}) {
	if DebugMode {
		fmt.Printf(format, args...)
	}
}

// DB represents the database instance
type DB struct {
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
	pageCache      map[uint32]*CacheEntry // Cache for all page types
	lastRadixPage  *RadixPage // Last radix page with available sub-pages
}

// Content represents a piece of content in the database
type Content struct {
	offset      int64 // File offset where this content is stored
	data        []byte
	key         []byte // Parsed key for ContentTypeData
	value       []byte // Parsed value for ContentTypeData
}

// BasePage contains common fields for all page types
type BasePage struct {
	pageNumber  uint32
	offset      int64
	dirty       bool
	data        []byte
}

// RadixPage represents a radix page with sub-pages
type RadixPage struct {
	BasePage     // Embed the base Page
	SubPagesUsed uint8  // Number of sub-pages used
}

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

// LeafPage represents a leaf page with entries
type LeafPage struct {
	BasePage    // Embed the base Page
	ContentSize uint16      // Total size of content on this page
	Entries     []LeafEntry // Parsed entries
}

// CacheEntry represents a generic page in the cache
type CacheEntry struct {
	PageType   byte
	RadixPage  *RadixPage
	LeafPage   *LeafPage
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

	// Default options
	lockType := LockNone // Default to no lock
	readOnly := false

	// Parse options
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	if opts != nil {
		if val, ok := opts["LockType"]; ok {
			if lt, ok := val.(int); ok {
				lockType = lt
			}
		}
		if val, ok := opts["ReadOnly"]; ok {
			if ro, ok := val.(bool); ok {
				readOnly = ro
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
		mainFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
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
		filePath:       path,
		mainFile:       mainFile,
		indexFile:      indexFile,
		mainFileSize:   mainFileInfo.Size(),
		indexFileSize:  indexFileInfo.Size(),
		readOnly:       readOnly,
		lockType:       LockNone,
		pageCache:      make(map[uint32]*CacheEntry),
	}

	// Apply file lock if requested
	if lockType != LockNone {
		if err := db.Lock(lockType); err != nil {
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to lock database files: %w", err)
		}
	}

	if !mainFileExists && !readOnly {
		// Initialize new database
		if err := db.initialize(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to initialize database: %w", err)
		}
	} else {
		// Read existing database headers
		if err := db.readHeader(); err != nil {
			db.Unlock()
			mainFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to read database header: %w", err)
		}
	}

	return db, nil
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

	// Flush all cached index pages to disk
	if !db.readOnly && len(db.pageCache) > 0 {
		flushErr = db.flushAllIndexPages()
		if flushErr != nil {
			// Log the error but continue with closing
			debugPrint("Error flushing index pages: %v\n", flushErr)
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
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot delete: database opened in read-only mode")
	}

	// Call Set with nil value to mark as deleted
	return db.Set(key, nil)
}

// Set sets a key-value pair in the database
func (db *DB) Set(key, value []byte) error {
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot set: database opened in read-only mode")
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()

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

	// Validate key length
	keyLen := len(key)
	if keyLen == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	if keyLen > MaxKeyLength {
		return fmt.Errorf("key length exceeds maximum allowed size of %d bytes", MaxKeyLength)
	}

	// Hash the key with salt 0
	hash := hashKey(key, InitialSalt)

	// Calculate total entries in main index
	totalMainEntries := MaxIndexEntries * db.mainIndexPages

	// Determine which page of the main index to use
	mainIndexSlot := int(hash % uint64(totalMainEntries))
	pageNumber := mainIndexSlot / MaxIndexEntries
	slotInPage := mainIndexSlot % MaxIndexEntries

	// Main index starts at page 2
	pageOffset := int64(pageNumber + 1) * PageSize

	// Check if page offset is valid
	if pageOffset >= db.mainFileSize {
		return fmt.Errorf("main index page beyond file size")
	}

	// Read the index page
	indexPage, err := db.readIndexPage(pageOffset)
	if err != nil {
		return fmt.Errorf("failed to read index page: %w", err)
	}

	if indexPage.Salt != InitialSalt {
		return fmt.Errorf("wrong salt for main index page")
	}

	err = db.setOnIndex(key, value, indexPage, slotInPage)
	if err != nil {
		return err
	}

	if indexPage.Dirty {
		return db.writeIndexPage(indexPage)
	}

	return nil
}

// setOnIndex sets a key-value pair in the index page
func (db *DB) setOnIndex(key, value []byte, indexPage *IndexPage, forcedSlot ...int) error {
	var salt uint8 = indexPage.Salt
	var slot int

	if salt == InitialSalt && len(forcedSlot) > 0 {
		// Use the forced slot for main index (salt 0)
		slot = forcedSlot[0]
	} else {
		// Calculate slot for the key
		slot = db.getIndexSlot(key, salt)
	}

	// Check if slot is valid
	if slot < 0 || slot >= MaxIndexEntries {
		return fmt.Errorf("slot index is out of range: %d", slot)
	}

	// If value is nil, we're deleting the key
	isDelete := value == nil

	// Check if the slot is already used
	contentOffset := db.readIndexEntry(indexPage, slot)
	if contentOffset != 0 {
		// Check if offset is valid
		if contentOffset < 0 || contentOffset >= db.mainFileSize {
			return fmt.Errorf("index entry points outside file bounds")
		}

		// Slot is used, read the content at this offset
		content, err := db.readContent(contentOffset)
		if err != nil {
			return fmt.Errorf("failed to read content: %w", err)
		}

		if content.contentType == ContentTypeData {
			// It's data content, check the key
			existingKey := content.key
			existingValue := content.value

			// Compare the existing key with the new key
			if equal(existingKey, key) {
				// Key found

				// If we're deleting, just zero out the index entry
				if isDelete {
					db.writeIndexEntry(indexPage, slot, 0)
					return nil
				}

				// Check if value is the same
				if equal(existingValue, value) {
					// Value is the same, no need to update
					return nil
				}

				// Value is different, append new data to the end of the file
				newOffset, err := db.appendData(key, value)
				if err != nil {
					return err
				}

				// Update the index entry to point to the new data
				db.writeIndexEntry(indexPage, slot, newOffset)

				// Write the index page on the caller function
				return nil
			}

			if isDelete {
				// Key not found, nothing to do
				return nil
			}

			// Key collision but different keys, need to create an index page
			newSalt := generateNewSalt(salt)
			newIndexPage, err := db.createIndexPage(newSalt)
			if err != nil {
				return err
			}

			// Append the new key-value pair at the end of the file
			newDataOffset, err := db.appendData(key, value)
			if err != nil {
				return err
			}

			// Add both entries to the new index page
			// Check if the slots would collide with the current salt
			for {
				slot1, err := db.setIndexEntry(newIndexPage, existingKey, contentOffset)
				if err != nil {
					return err
				}
				slot2, err := db.setIndexEntry(newIndexPage, key, newDataOffset)
				if err != nil {
					return err
				}

				// If the slots are different, we're good
				if slot1 != slot2 {
					break
				}

				debugPrint(" --> hash collision for keys %s and %s \n", string(existingKey), string(key))

				// Slots collided, try a different salt
				newSalt = generateNewSalt(newSalt)
				// Use a new index page because the old one is already dirty
				newIndexPage, err = db.createIndexPage(newSalt)
				if err != nil {
					return err
				}
			}

			// Write the new index page at the end of the file
			if err := db.writeIndexPage(newIndexPage); err != nil {
				return err
			}

			// Update the original index to point to the new index page
			db.writeIndexEntry(indexPage, slot, newIndexPage.offset)

			// Write the index page on the caller function
			return nil

		} else if content.contentType == ContentTypeIndex {
			// It's an index content, parse it
			childIndexPage, err := db.readIndexPage(contentOffset)
			if err != nil {
				return fmt.Errorf("failed to parse index page: %w", err)
			}

			// Assert that the salt is not the initial salt
			if childIndexPage.Salt == InitialSalt {
				return fmt.Errorf("salt from internal index page is the initial salt")
			}

			// Set the key-value pair in the child index page
			// Don't pass forcedSlot for secondary indexes
			err = db.setOnIndex(key, value, childIndexPage)
			if err != nil {
				return err
			}

			if childIndexPage.Dirty {
				// Write the new index page
				return db.writeIndexPage(childIndexPage)
			}

			// Write the parent index page on the caller function
			return nil
		}
	}

	// For deletion, if we got here the key doesn't exist, so nothing to do
	if isDelete {
		return nil
	}

	// Slot is available, append the data and update the index
	dataOffset, err := db.appendData(key, value)
	if err != nil {
		return err
	}

	// Update index entry
	db.writeIndexEntry(indexPage, slot, dataOffset)

	// Write the index page on the caller function
	return nil
}

// Get retrieves a value for the given key
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	// Validate key length
	keyLen := len(key)
	if keyLen == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}
	if keyLen > MaxKeyLength {
		return nil, fmt.Errorf("key length exceeds maximum allowed size of %d bytes", MaxKeyLength)
	}

	// Hash the key with salt 0
	hash := hashKey(key, InitialSalt)

	// Calculate total entries in main index
	totalMainEntries := MaxIndexEntries * db.mainIndexPages

	// Determine which page of the main index to use
	mainIndexSlot := int(hash % uint64(totalMainEntries))
	pageNumber := mainIndexSlot / MaxIndexEntries
	slotInPage := mainIndexSlot % MaxIndexEntries

	// Main index starts at page 2
	pageOffset := int64(pageNumber + 1) * PageSize

	// Check if page offset is valid
	if pageOffset >= db.mainFileSize {
		return nil, fmt.Errorf("main index page beyond file size")
	}

	// Read the index page
	indexPage, err := db.readIndexPage(pageOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read index page: %w", err)
	}

	return db.getFromIndex(key, indexPage, InitialSalt, slotInPage)
}

// getFromIndex retrieves a value using the specified salt and index page
func (db *DB) getFromIndex(key []byte, indexPage *IndexPage, salt uint8, forcedSlot ...int) ([]byte, error) {
	var slot int
	if salt == InitialSalt && len(forcedSlot) > 0 {
		// Use the forced slot for main index (salt 0)
		slot = forcedSlot[0]
	} else {
		// Calculate slot for the key
		slot = db.getIndexSlot(key, salt)
	}

	// Check if slot has an entry
	contentOffset := db.readIndexEntry(indexPage, slot)
	if contentOffset == 0 {
		return nil, fmt.Errorf("key not found")
	}

	// Check if offset is valid
	if contentOffset < 0 || contentOffset >= db.mainFileSize {
		return nil, fmt.Errorf("index entry points outside file bounds")
	}

	// Read the content at the offset
	content, err := db.readContent(contentOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read content: %w", err)
	}

	if content.contentType == ContentTypeData {
		// It's data content, check if key matches
		if equal(content.key, key) {
			return content.value, nil
		}
		return nil, fmt.Errorf("key not found")

	} else if content.contentType == ContentTypeIndex {
		// It's another index page, follow the chain with its salt
		childIndexPage, err := db.readIndexPage(contentOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to read index page: %w", err)
		}

		// Don't pass forcedSlot for secondary indexes
		return db.getFromIndex(key, childIndexPage, childIndexPage.Salt)
	}

	return nil, fmt.Errorf("invalid content type")
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

	// Initialize main file
	if err := db.initializeMainFile(); err != nil {
		return fmt.Errorf("failed to initialize main file: %w", err)
	}

	// Initialize index file
	if err := db.initializeIndexFile(); err != nil {
		return fmt.Errorf("failed to initialize index file: %w", err)
	}

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

	// The rest of the root page is reserved for future use

	debugPrint("Writing main file root page to disk\n")
	if _, err := db.mainFile.WriteAt(rootPage, 0); err != nil {
		return err
	}

	// Update file size to include the root page
	db.mainFileSize = PageSize

	return nil
}

// initializeIndexFile initializes the index file
func (db *DB) initializeIndexFile() error {
	// Write file header in root page (page 0)
	rootPage := make([]byte, PageSize)

	// Write the 6-byte magic string
	copy(rootPage[0:6], IndexFileMagicString)

	// Write the 2-byte version
	copy(rootPage[6:8], VersionString)

	// The rest of the root page is reserved for future use

	debugPrint("Writing index file root page to disk\n")
	if _, err := db.indexFile.WriteAt(rootPage, 0); err != nil {
		return err
	}

	// Update file size to include the root page
	db.indexFileSize = PageSize

	// Allocate root radix page at page 1
	rootRadixPage, err := db.allocateRadixPage()
	if err != nil {
		return fmt.Errorf("failed to allocate root radix page: %w", err)
	}

	// Page number should be 1 (since we just allocated it after the header page)
	if rootRadixPage.PageNumber != 1 {
		return fmt.Errorf("unexpected root radix page number: %d", rootRadixPage.PageNumber)
	}

	// Initialize the first two levels of the radix tree
	if err := db.initializeRadixLevels(); err != nil {
		return fmt.Errorf("failed to initialize radix levels: %w", err)
	}

	// Write all index pages to disk
	if err := db.flushAllIndexPages(); err != nil {
		return fmt.Errorf("failed to write index pages: %w", err)
	}

	// Cache this as the last radix page
	db.lastRadixPage = rootRadixPage

	return nil
}

// readHeader reads the database headers and preloads radix levels
func (db *DB) readHeader() error {
	// Read main file header
	if err := db.readMainFileHeader(); err != nil {
		return fmt.Errorf("failed to read main file header: %w", err)
	}

	// Read index file header
	if err := db.readIndexFileHeader(); err != nil {
		return fmt.Errorf("failed to read index file header: %w", err)
	}

	// Preload the first two levels of the radix tree
	if err := db.preloadRadixLevels(); err != nil {
		return fmt.Errorf("failed to preload radix levels: %w", err)
	}

	// Initialize the last radix page
	if err := db.initLastRadixPage(); err != nil {
		return fmt.Errorf("failed to initialize last radix page: %w", err)
	}

	return nil
}

// readMainFileHeader reads the main file header
func (db *DB) readMainFileHeader() error {
	// Read the header (8 bytes) in root page (page 1)
	header := make([]byte, 8)
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

	return nil
}

// readIndexFileHeader reads the index file header
func (db *DB) readIndexFileHeader() error {
	// Read the header (8 bytes) in root page (page 1)
	header := make([]byte, 8)
	if _, err := db.indexFile.ReadAt(header, 0); err != nil {
		return err
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

	return nil
}

// initLastRadixPage initializes the lastRadixPage field by scanning the index file
func (db *DB) initLastRadixPage() error {
	// If the index file only has the header page, there's nothing to do
	if db.indexFileSize <= PageSize {
		return nil
	}

	// Scan backward from the end of the file to find the last radix page
	// Start from the last page
	lastPageNumber := uint32((db.indexFileSize - 1) / PageSize)

	// Skip the header page
	if lastPageNumber == 0 {
		lastPageNumber = 1
	}

	// Scan backward until we find the last radix page
	for pageNum := lastPageNumber; pageNum >= 1; pageNum-- {
		// Try to read the page
		page, err := db.readRadixPage(pageNum)
		if err != nil {
			// If it's not a radix page, continue
			if strings.Contains(err.Error(), "not a radix page") {
				continue
			}
			return fmt.Errorf("failed to read page %d: %w", pageNum, err)
		}
		// Found the last radix page
		db.lastRadixPage = page
		break
	}

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

	// Write the content to the file
	if _, err := db.mainFile.WriteAt(content, fileSize); err != nil {
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
// Index file
// ------------------------------------------------------------------------------------------------

// readRadixPage reads a radix page from the disk
func (db *DB) readRadixPage(pageNumber uint32) (*RadixPage, error) {
	debugPrint("Reading radix page %d\n", pageNumber)

	// Calculate file offset from page number
	offset := int64(pageNumber) * PageSize

	// Check if offset is valid
	if offset < 0 || offset >= db.indexFileSize {
		return nil, fmt.Errorf("page number %d out of index file bounds", pageNumber)
	}

	data := make([]byte, PageSize)

	if _, err := db.indexFile.ReadAt(data, offset); err != nil {
		return nil, err
	}

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

	// Create structured radix page
	radixPage := &RadixPage{
		BasePage: BasePage{
			PageNumber:  pageNumber,
			Offset:      offset,
			Dirty:       false,
			data:        data,
			contentType: ContentTypeRadix,
		},
		SubPagesUsed: data[1],
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

	// Calculate CRC32 checksum for the page data (excluding the checksum field itself)
	// Zero out the checksum field before calculating
	binary.BigEndian.PutUint32(radixPage.data[4:8], 0)
	// Calculate checksum of the entire page
	checksum := crc32.ChecksumIEEE(radixPage.data)
	// Write the checksum at position 4
	binary.BigEndian.PutUint32(radixPage.data[4:8], checksum)

	// Ensure the page number and offset are valid
	if radixPage.PageNumber == 0 {
		return fmt.Errorf("cannot write radix page with page number 0")
	}

	// Calculate offset from page number if not set
	if radixPage.offset == 0 {
		radixPage.offset = int64(radixPage.PageNumber) * PageSize
	}

	debugPrint("Writing radix page to index file at page %d\n", radixPage.PageNumber)

	// Write to disk at the specified offset
	_, err := db.indexFile.WriteAt(radixPage.data, radixPage.offset)

	// If the page was written successfully
	if err == nil {
		// Mark it as clean
		radixPage.Dirty = false

		// Update file size if this write extended the file
		newEndOffset := radixPage.offset + PageSize
		if newEndOffset > db.indexFileSize {
			db.indexFileSize = newEndOffset
		}
	}

	return err
}

// ------------------------------------------------------------------------------------------------
// Leaf pages
// ------------------------------------------------------------------------------------------------

// readLeafPage reads a leaf page from the given page number
func (db *DB) readLeafPage(pageNumber uint32) (*LeafPage, error) {
	debugPrint("Reading leaf page %d\n", pageNumber)

	// Calculate file offset from page number
	offset := int64(pageNumber) * PageSize

	// Check if offset is valid
	if offset < 0 || offset >= db.indexFileSize {
		return nil, fmt.Errorf("page number %d out of index file bounds", pageNumber)
	}

	data := make([]byte, PageSize)

	if _, err := db.indexFile.ReadAt(data, offset); err != nil {
		return nil, err
	}

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
		BasePage: BasePage{
			PageNumber:  pageNumber,
			Offset:      offset,
			Dirty:       false,
			data:        data,
			contentType: ContentTypeLeaf,
		},
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
	if leafPage.PageNumber == 0 {
		// Calculate new page number
		leafPage.PageNumber = uint32(db.indexFileSize / PageSize)

		// Update offset
		leafPage.offset = db.indexFileSize

		// Print some debug info
		debugPrint("Writing leaf page to end of index file at page %d\n", leafPage.PageNumber)
	} else {
		// Calculate offset from page number
		leafPage.offset = int64(leafPage.PageNumber) * PageSize

		debugPrint("Writing leaf page to index file at page %d\n", leafPage.PageNumber)
	}

	// Write to disk at the specified offset
	_, err := db.indexFile.WriteAt(leafPage.data, leafPage.offset)

	// If the page was written successfully
	if err == nil {
		// Mark it as clean
		leafPage.Dirty = false

		// Update file size if this write extended the file
		newEndOffset := leafPage.offset + PageSize
		if newEndOffset > db.indexFileSize {
			db.indexFileSize = newEndOffset
		}
	}
	return err
}

// Sync flushes all dirty pages to disk and syncs the files
func (db *DB) Sync() error {
	// Check if file is opened in read-only mode
	if db.readOnly {
		return fmt.Errorf("cannot sync: database opened in read-only mode")
	}

	// Flush all dirty pages
	if err := db.flushDirtyIndexPages(); err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
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
// Page Cache
// ------------------------------------------------------------------------------------------------

// addToCache adds a page to the cache
func (db *DB) addToCache(page interface{}) {
	entry := &CacheEntry{}

	switch p := page.(type) {
	case *RadixPage:
		entry.PageType = ContentTypeRadix
		entry.RadixPage = p
	case *LeafPage:
		entry.PageType = ContentTypeLeaf
		entry.LeafPage = p
	default:
		return // Unknown page type
	}

	pageNumber := uint32(0)
	if entry.PageType == ContentTypeRadix {
		pageNumber = entry.RadixPage.PageNumber
	} else if entry.PageType == ContentTypeLeaf {
		pageNumber = entry.LeafPage.PageNumber
	}

	if pageNumber == 0 {
		return // Invalid page number
	}

	db.cacheMutex.Lock()
	db.pageCache[pageNumber] = entry
	db.cacheMutex.Unlock()
}

// getPage gets a page from the cache by page number and type
func (db *DB) getPage(pageNumber uint32) (*CacheEntry, bool) {
	db.cacheMutex.RLock()
	entry, exists := db.pageCache[pageNumber]
	db.cacheMutex.RUnlock()

	return entry, exists
}

// getRadixPage returns a radix page from the cache or from the disk
func (db *DB) getRadixPage(pageNumber uint32) (*RadixPage, error) {
	// Check if the page is in the cache
	entry, exists := db.getPage(pageNumber)

	// If it exists and is a radix page, return it
	if exists {
		if entry.PageType != ContentTypeRadix || entry.RadixPage == nil {
			return nil, fmt.Errorf("page %d is not a radix page", pageNumber)
		}
		return entry.RadixPage, nil
	}

	// If not, read it from disk
	return db.readRadixPage(pageNumber)
}

// getLeafPage returns a leaf page from the cache or from the disk
func (db *DB) getLeafPage(pageNumber uint32) (*LeafPage, error) {
	// Check if the page is in the cache
	entry, exists := db.getPage(pageNumber)

	// If it exists and is a leaf page, return it
	if exists {
		if entry.PageType != ContentTypeLeaf || entry.LeafPage == nil {
			return nil, fmt.Errorf("page %d is not a leaf page", pageNumber)
		}
		return entry.LeafPage, nil
	}

	// If not, read it from disk
	return db.readLeafPage(pageNumber)
}

// loadRadixPage loads a radix page into the cache if it's not already there
func (db *DB) loadRadixPage(pageNumber uint32) (*RadixPage, error) {
	// Check if the page is already in cache
	entry, exists := db.getPage(pageNumber)

	if exists {
		if entry.PageType != ContentTypeRadix || entry.RadixPage == nil {
			return nil, fmt.Errorf("page %d is not a radix page", pageNumber)
		}
		return entry.RadixPage, nil
	}

	// Not in cache, load it from disk
	return db.readRadixPage(pageNumber)
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

	for _, entry := range db.pageCache {
		if entry.PageType == ContentTypeRadix {
			radixPages++
		} else if entry.PageType == ContentTypeLeaf {
			leafPages++
		}
	}

	stats["radix_pages"] = radixPages
	stats["leaf_pages"] = leafPages

	return stats
}

// flushAllIndexPages writes all cached pages to disk
func (db *DB) flushAllIndexPages() error {
	db.cacheMutex.RLock()
	defer db.cacheMutex.RUnlock()

	for _, entry := range db.pageCache {
		if entry.PageType == ContentTypeRadix && entry.RadixPage != nil {
			if err := db.writeRadixPage(entry.RadixPage); err != nil {
				return err
			}
		} else if entry.PageType == ContentTypeLeaf && entry.LeafPage != nil {
			if err := db.writeLeafPage(entry.LeafPage); err != nil {
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

	for _, entry := range db.pageCache {
		if entry.PageType == ContentTypeRadix && entry.RadixPage != nil && entry.RadixPage.Dirty {
			if err := db.writeRadixPage(entry.RadixPage); err != nil {
				return err
			}
		} else if entry.PageType == ContentTypeLeaf && entry.LeafPage != nil && entry.LeafPage.Dirty {
			if err := db.writeLeafPage(entry.LeafPage); err != nil {
				return err
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
	data[0] = ContentTypeRadix // Set content type in header
	data[1] = 0                // No sub-pages used initially

	// Calculate new page number
	pageNumber := uint32(db.indexFileSize / PageSize)

	// Calculate file offset
	offset := db.indexFileSize

	// Update file size
	db.indexFileSize += PageSize

	radixPage := &RadixPage{
		BasePage: BasePage{
			PageNumber:  pageNumber,
			Offset:      offset,
			Dirty:       false,
			data:        data,
			contentType: ContentTypeRadix,
		},
		SubPagesUsed: 0,
	}

	// Add to cache
	db.addToCache(radixPage)

	debugPrint("Allocated new radix page at page %d\n", pageNumber)

	return radixPage, nil
}

// allocateRadixSubPage returns the next available radix sub-page or creates a new one if needed
// It returns a RadixSubPage struct
func (db *DB) allocateRadixSubPage() (*RadixSubPage, error) {
	// If we have a cached last radix page, check if it has available sub-pages
	if db.lastRadixPage != nil {
		// If there are still available sub-pages
		if db.lastRadixPage.SubPagesUsed < SubPagesPerRadixPage {
			// Use the next available sub-page index
			subPageIdx := db.lastRadixPage.SubPagesUsed

			// Increment the sub-page counter (will be written when the page is updated)
			db.lastRadixPage.SubPagesUsed++
			db.lastRadixPage.Dirty = true

			// Update the page in the cache (no need to write to disk yet)
			db.addToCache(db.lastRadixPage)

			return &RadixSubPage{
				Page:      db.lastRadixPage,
				SubPageIdx: subPageIdx,
			}, nil
		}
	}

	// No available radix page found or lastRadixPage is full, create a new one
	newRadixPage, err := db.allocateRadixPage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate radix page: %w", err)
	}

	// Mark first sub-page as used
	newRadixPage.SubPagesUsed = 1

	// Cache this as the last radix page
	db.lastRadixPage = newRadixPage

	return &RadixSubPage{
		Page:      newRadixPage,
		SubPageIdx: 0,
	}, nil
}

// allocateLeafPage creates a new empty leaf page and allocates a page number
func (db *DB) allocateLeafPage() (*LeafPage, error) {
	// Allocate the page data
	data := make([]byte, PageSize)
	data[0] = ContentTypeLeaf // Set content type in header

	// Calculate new page number
	pageNumber := uint32(db.indexFileSize / PageSize)

	// Calculate file offset
	offset := db.indexFileSize

	// Update file size
	db.indexFileSize += PageSize

	leafPage := &LeafPage{
		BasePage: BasePage{
			PageNumber:  pageNumber,
			Offset:      offset,
			Dirty:       false,
			data:        data,
			contentType: ContentTypeLeaf,
		},
		ContentSize: LeafHeaderSize,
		Entries:     make([]LeafEntry, 0),
	}

	// Add to cache
	db.addToCache(leafPage)

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
	pos := LeafHeaderSize

	// Read entries until we reach content size
	for pos < leafPage.ContentSize {
		// Read suffix length
		suffixLen64, bytesRead := varint.Read(leafPage.data[pos:])
		if bytesRead == 0 {
			return nil, fmt.Errorf("failed to read suffix length")
		}
		suffixLen := int(suffixLen64)
		pos += uint16(bytesRead)

		// Read suffix
		suffix := make([]byte, suffixLen)
		copy(suffix, leafPage.data[pos:pos+uint16(suffixLen)])
		pos += uint16(suffixLen)

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

// addLeafEntry adds an entry to a leaf page
// Returns true if the entry was added, false if the page is full
func (db *DB) addLeafEntry(leafPage *LeafPage, suffix []byte, dataOffset int64) bool {
	// Calculate size needed for this entry
	suffixLenSize := varint.Size(uint64(len(suffix)))
	entrySize := suffixLenSize + len(suffix) + 8 // suffix length + suffix + data offset

	// Check if there's enough space in the page
	if int(leafPage.ContentSize)+entrySize > PageSize {
		return false // Page is full
	}

	// Get current content position
	pos := leafPage.ContentSize

	// Write suffix length
	suffixLenWritten := varint.Write(leafPage.data[pos:], uint64(len(suffix)))
	pos += uint16(suffixLenWritten)

	// Write suffix
	copy(leafPage.data[pos:], suffix)
	pos += uint16(len(suffix))

	// Write data offset
	binary.LittleEndian.PutUint64(leafPage.data[pos:], uint64(dataOffset))
	pos += 8

	// Update content size
	leafPage.ContentSize = pos

	// Add to entries list
	leafPage.Entries = append(leafPage.Entries, LeafEntry{
		Suffix:     suffix,
		DataOffset: dataOffset,
	})

	// Mark page as dirty
	leafPage.Dirty = true

	return true
}

// getLeafEntries returns all entries in a leaf page
func (db *DB) getLeafEntries(leafPage *LeafPage) ([]LeafEntry, error) {
	var entries []LeafEntry

	// Start at header size
	pos := LeafHeaderSize

	// Read entries until we reach content size
	for pos < leafPage.ContentSize {
		// Read suffix length
		suffixLen64, bytesRead := varint.Read(leafPage.data[pos:])
		if bytesRead == 0 {
			return nil, fmt.Errorf("failed to read suffix length")
		}
		suffixLen := int(suffixLen64)
		pos += uint16(bytesRead)

		// Read suffix
		suffix := make([]byte, suffixLen)
		copy(suffix, leafPage.data[pos:pos+uint16(suffixLen)])
		pos += uint16(suffixLen)

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

// ------------------------------------------------------------------------------------------------
// Radix entries (on sub-pages)
// ------------------------------------------------------------------------------------------------

// setRadixPageEntry sets an entry in a radix page
func (db *DB) setRadixPageEntry(radixPage *RadixPage, subPageIdx uint8, byteValue uint8, pageNumber uint32, nextSubPageIdx uint8) {
	// Check if subPage is valid
	if subPageIdx >= SubPagesPerRadixPage {
		return
	}

	// Calculate base offset for this entry in the page data
	entryOffset := RadixHeaderSize + int(subPageIdx)*EntriesPerSubPage*RadixEntrySize + int(byteValue)*RadixEntrySize

	// Write page number (4 bytes)
	binary.LittleEndian.PutUint32(radixPage.data[entryOffset:entryOffset+4], pageNumber)

	// Write sub-page index (1 byte)
	radixPage.data[entryOffset+4] = nextSubPageIdx

	// Mark page as dirty
	radixPage.Dirty = true
}

// getRadixPageEntry gets an entry from a radix page
func (db *DB) getRadixPageEntry(radixPage *RadixPage, subPageIdx uint8, byteValue uint8) (pageNumber uint32, nextSubPageIdx uint8) {
	// Check if subPage is valid
	if subPageIdx >= SubPagesPerRadixPage || subPageIdx >= radixPage.SubPagesUsed {
		return 0, 0
	}

	// Calculate base offset for this entry in the page data
	entryOffset := RadixHeaderSize + int(subPageIdx)*EntriesPerSubPage*RadixEntrySize + int(byteValue)*RadixEntrySize

	// Read page number (4 bytes)
	pageNumber = binary.LittleEndian.Uint32(radixPage.data[entryOffset:entryOffset+4])

	// Read sub-page index (1 byte)
	nextSubPageIdx = radixPage.data[entryOffset+4]

	return pageNumber, nextSubPageIdx
}

// setRadixEntry sets an entry in a radix sub-page
func (db *DB) setRadixEntry(subPage *RadixSubPage, byteValue uint8, pageNumber uint32, nextSubPageIdx uint8) {
	db.setRadixPageEntry(subPage.Page, subPage.SubPageIdx, byteValue, pageNumber, nextSubPageIdx)
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
	for byteValue := uint8(0); byteValue < 256; byteValue++ {
		pageNumber, _ := db.getRadixEntry(rootSubPage, byteValue)
		if pageNumber > 0 {
			// Load this page into cache if not already there
			_, err := db.loadRadixPage(pageNumber)
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

	// Create root sub-page
	rootSubPage := &RadixSubPage{
		Page:      rootRadixPage,
		SubPageIdx: 0,
	}

	// First level: Mark the root page as having one sub-page used
	rootRadixPage.SubPagesUsed = 1
	rootRadixPage.Dirty = true

	// For the first 256 possible values in the first byte, create entries in the radix tree
	for byteValue := uint8(0); byteValue < 256; byteValue++ {
		// Allocate a new sub-page for this byte value
		childSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			return fmt.Errorf("failed to allocate sub-page for byte %d: %w", byteValue, err)
		}

		// Link from root page to this page/sub-page
		db.setRadixEntry(rootSubPage, byteValue, childSubPage.Page.PageNumber, childSubPage.SubPageIdx)
	}

	return nil
}
