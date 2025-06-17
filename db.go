package kv_log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/aergoio/kv_log/varint"
)

const (
	// Debug mode (set to false to disable debug prints)
	DebugMode = false
	// Page size (4KB)
	PageSize = 4096
	// Base header size (just type identifier)
	BaseHeaderSize = 1
	// Index page header size (type identifier + salt)
	IndexHeaderSize = 8
	// Maximum number of entries in index page
	MaxIndexEntries = (PageSize - IndexHeaderSize) / 8 // 8 = offset pointer size
	// Magic string for database identification (6 bytes)
	MagicString string = "KV_LOG"
	// Database version (2 bytes as a string)
	VersionString string = "\x00\x01"
	// Number of pages in main index
	DefaultMainIndexPages = 256 // 1 MB / 4096 bytes per page
	// Initial salt
	InitialSalt = 0
	// Maximum key length
	MaxKeyLength = 2048
	// Maximum value length
	MaxValueLength = 2 << 26 // 128MB
	// Alignment for non-page content
	ContentAlignment = 8
)

// Content types
const (
	ContentTypeIndex = 'I' // Index content type
	ContentTypeData  = 'D' // Data content type
)

// Lock types
const (
	LockNone    = 0 // No locking
	LockShared  = 1 // Shared lock (read-only)
	LockExclusive = 2 // Exclusive lock (read-write)
)

// Sync modes
const (
	SyncFull   = 2 // use fsync() on every commit
	SyncNormal = 1 // use fsync() only on checkpoint (WAL only)
	SyncOff    = 0 // do not use fsync()
)

// Journal modes
const (
	JournalModeWAL = "WAL" // Write-Ahead Logging mode
	JournalModeOff = "OFF" // No journaling
)

// debugPrint prints a message if debug mode is enabled
func debugPrint(format string, args ...interface{}) {
	if DebugMode {
		fmt.Printf(format, args...)
	}
}

// DB represents the database instance
type DB struct {
	filePath        string
	file            *os.File
	mutex           sync.RWMutex
	mainIndexPages  int
	fileSize        int64  // Track file size to avoid frequent stat calls
	fileLocked      bool   // Track if the file is locked
	lockType        int    // Type of lock currently held
	readOnly        bool   // Track if the database is opened in read-only mode
	syncMode        int    // Sync mode for file operations
	journalMode     string // Current journal mode (WAL or OFF)
	nextJournalMode string // Next journal mode to apply
	walInfo         *WalInfo // WAL file information
}

// Content represents a piece of content in the database
type Content struct {
	contentType uint8
	offset      int64 // File offset where this content is stored
	data        []byte
	key         []byte // Parsed key for ContentTypeData
	value       []byte // Parsed value for ContentTypeData
}

// IndexPage represents an index page with entries
type IndexPage struct {
	Content  // Embed the base Content
	Salt     uint8
	Dirty    bool
}

// Options represents configuration options for the database
type Options map[string]interface{}

// Open opens or creates a database file with the given options
func Open(path string, options ...Options) (*DB, error) {

	fileExists := false
	if _, err := os.Stat(path); err == nil {
		fileExists = true
	}

	// Default options
	mainIndexPages := DefaultMainIndexPages
	lockType := LockNone // Default to no lock
	readOnly := false
	syncMode := SyncNormal // Default to normal sync mode
	journalMode := JournalModeWAL // Default to WAL journal mode

	// Parse options
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	if opts != nil {
		if val, ok := opts["MainIndexPages"]; ok {
			if pages, ok := val.(int); ok && pages > 0 {
				mainIndexPages = pages
			} else {
				return nil, fmt.Errorf("invalid value for MainIndexPages option")
			}
		}
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
		if val, ok := opts["SyncMode"]; ok {
			if sm, ok := val.(int); ok {
				if sm >= SyncOff && sm <= SyncFull {
					syncMode = sm
				} else {
					return nil, fmt.Errorf("invalid value for SyncMode option")
				}
			}
		}
		if val, ok := opts["JournalMode"]; ok {
			if jm, ok := val.(string); ok {
				if jm == JournalModeWAL || jm == JournalModeOff {
					journalMode = jm
				} else {
					return nil, fmt.Errorf("invalid value for JournalMode option")
				}
			}
		}
	}

	// Open file with appropriate flags
	var file *os.File
	var err error
	if readOnly {
		file, err = os.OpenFile(path, os.O_RDONLY, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open database file in read-only mode: %w", err)
		}
	} else {
		file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open database file: %w", err)
		}
	}

	// Get initial file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}

	db := &DB{
		file:            file,
		filePath:        path,
		mainIndexPages:  mainIndexPages,
		fileSize:        fileInfo.Size(),
		readOnly:        readOnly,
		lockType:        LockNone,
		syncMode:        syncMode,
		journalMode:     journalMode,
		nextJournalMode: journalMode,
	}

	// Apply file lock if requested
	if lockType != LockNone {
		if err := db.Lock(lockType); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to lock database file: %w", err)
		}
	}

	if !fileExists && !readOnly {
		// Initialize new database
		if err := db.initialize(); err != nil {
			db.Unlock()
			file.Close()
			return nil, fmt.Errorf("failed to initialize database: %w", err)
		}
	} else {
		// Read existing database header
		if err := db.readHeader(); err != nil {
			db.Unlock()
			file.Close()
			return nil, fmt.Errorf("failed to read database header: %w", err)
		}
	}

	// Check for existing WAL file if in WAL mode
	if db.journalMode == JournalModeWAL {
		// Open existing WAL file if it exists
		if err := db.openWAL(); err != nil {
			db.Unlock()
			file.Close()
			return nil, fmt.Errorf("failed to open WAL file: %w", err)
		}
	}

	return db, nil
}

// SetOption sets a database option after the database is open
func (db *DB) SetOption(name string, value interface{}) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	switch name {
	case "SyncMode":
		if sm, ok := value.(int); ok {
			if sm >= SyncOff && sm <= SyncFull {
				db.syncMode = sm
				return nil
			}
			return fmt.Errorf("invalid value for SyncMode option")
		}
		return fmt.Errorf("SyncMode option value must be an integer")
	case "JournalMode":
		if jm, ok := value.(string); ok {
			if jm == JournalModeWAL || jm == JournalModeOff {
				db.nextJournalMode = jm
				return nil
			}
			return fmt.Errorf("invalid value for JournalMode option")
		}
		return fmt.Errorf("JournalMode option value must be a string")
	default:
		return fmt.Errorf("unknown or immutable option: %s", name)
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

	err := syscall.Flock(int(db.file.Fd()), lockFlag)
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

	err := syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
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

// Close closes the database file
func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.file != nil {
		// Release lock if acquired
		if db.fileLocked {
			if err := db.Unlock(); err != nil {
				return fmt.Errorf("failed to unlock database file: %w", err)
			}
		}
		// Close the file
		return db.file.Close()
	}
	return nil
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
	if pageOffset >= db.fileSize {
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
		if contentOffset < 0 || contentOffset >= db.fileSize {
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
	if pageOffset >= db.fileSize {
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
	if contentOffset < 0 || contentOffset >= db.fileSize {
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

	// Save the original journal mode and temporarily disable it during initialization
	originalJournalMode := db.journalMode
	db.journalMode = JournalModeOff

	// Write file header in root page (page 1)
	rootPage := make([]byte, PageSize)

	// Write the 6-byte magic string
	copy(rootPage[0:6], MagicString)

	// Write the 2-byte version
	copy(rootPage[6:8], VersionString)

	// Write the number of main index pages (4 bytes)
	binary.BigEndian.PutUint32(rootPage[8:12], uint32(db.mainIndexPages))

	// The rest of the root page is reserved for future use

	debugPrint("Writing root page to disk\n")
	if _, err := db.file.WriteAt(rootPage, 0); err != nil {
		return err
	}

	// Update file size to include the root page
	db.fileSize = PageSize

	// Create all pages for the main index, starting at page 2
	for i := 0; i < db.mainIndexPages; i++ {
		indexPage := &IndexPage{
			Content: Content{
				contentType: ContentTypeIndex,
				offset:      int64(i + 1) * PageSize, // First page is at offset PageSize, second at 2*PageSize, etc.
				data:        make([]byte, PageSize),
			},
			Salt: InitialSalt,
		}

		// Write the index page
		if err := db.writeIndexPage(indexPage); err != nil {
			return err
		}
	}

	// Update file size after creating all index pages
	db.fileSize = int64(db.mainIndexPages + 1) * PageSize

	// Sync the file to ensure all writes are persisted
	if db.syncMode == SyncFull {
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync database file: %w", err)
		}
	}

	// Restore the original journal mode
	db.journalMode = originalJournalMode

	// If using WAL and one exists, delete it
	if db.journalMode == JournalModeWAL {
		// Delete the WAL file
		err := DeleteWAL(db)
		if err != nil {
			return fmt.Errorf("failed to discard WAL file: %w", err)
		}
	}

	debugPrint("Database initialized\n")

	return nil
}

// readHeader reads the database header
func (db *DB) readHeader() error {

	// Read the header (12 bytes) in root page (page 1)
	header := make([]byte, 12)
	if _, err := db.file.ReadAt(header, 0); err != nil {
		return err
	}

	// Extract magic string (6 bytes)
	fileMagic := string(header[0:6])

	// Extract version (2 bytes)
	fileVersion := string(header[6:8])

	if fileMagic != MagicString {
		return fmt.Errorf("invalid database file format")
	}

	if fileVersion != VersionString {
		return fmt.Errorf("unsupported database version")
	}

	// Extract main index pages (4 bytes)
	mainIndexPages := binary.BigEndian.Uint32(header[8:12])
	if mainIndexPages == 0 || mainIndexPages > 2<<24 { // 2^24 = 16M pages * 4096 bytes per page = 64GB
		return fmt.Errorf("invalid number of main index pages: %d", mainIndexPages)
	}
	db.mainIndexPages = int(mainIndexPages)

	return nil
}

// readIndexPage reads an index page from the given offset
func (db *DB) readIndexPage(offset int64) (*IndexPage, error) {
	var data []byte
	var err error

	debugPrint("Reading index page from offset %d\n", offset)

	if db.journalMode == JournalModeWAL {
		// Read from WAL file
		data, err = db.readFromWAL(offset)
		if err != nil {
			return nil, err
		}
	}
	// If WAL is not used, or not found in WAL
	if data == nil {
		data = make([]byte, PageSize)
		// Check if offset is valid
		if offset < 0 || offset >= db.fileSize {
			return nil, fmt.Errorf("offset out of file bounds: %d", offset)
		}
		// Read from main db file
		if _, err := db.file.ReadAt(data, offset); err != nil {
			return nil, err
		}
	}

	// Check if it's an index page
	if data[0] != ContentTypeIndex {
		return nil, fmt.Errorf("not an index page at offset %d", offset)
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
		return nil, fmt.Errorf("index page checksum mismatch at offset %d: stored=%d, calculated=%d", offset, storedChecksum, calculatedChecksum)
	}

	// Create structured index page
	indexPage := &IndexPage{
		Content: Content{
			contentType: data[0],
			offset:      offset,
			data:        data,
		},
		Salt: data[1],
	}

	return indexPage, nil
}

// createIndexPage creates a new empty index page
func (db *DB) createIndexPage(salt uint8) (*IndexPage, error) {

	data := make([]byte, PageSize)
	data[0] = ContentTypeIndex // Set content type in header
	data[1] = salt             // Set salt in header

	indexPage := &IndexPage{
		Content: Content{
			contentType: ContentTypeIndex,
			data:        data,
		},
		Salt: salt,
	}

	return indexPage, nil
}

// writeIndexPage writes an index page to the database file
func (db *DB) writeIndexPage(indexPage *IndexPage) error {
	// Set page type and salt in the data
	indexPage.data[0] = ContentTypeIndex  // Type identifier
	indexPage.data[1] = indexPage.Salt    // Salt for index pages

	// Calculate CRC32 checksum for the page data (excluding the checksum field itself)
	// Zero out the checksum field before calculating
	binary.BigEndian.PutUint32(indexPage.data[4:8], 0)
	// Calculate checksum of the entire page
	checksum := crc32.ChecksumIEEE(indexPage.data)
	// Write the checksum at position 4
	binary.BigEndian.PutUint32(indexPage.data[4:8], checksum)

	// If offset is 0, append to the end of the file
	if indexPage.offset == 0 {
		// Use stored file size to determine where to append
		fileSize := db.fileSize

		// Align to ContentAlignment if needed
		remainder := fileSize % ContentAlignment
		if remainder != 0 {
			padding := ContentAlignment - remainder
			paddingBytes := make([]byte, padding)
			if _, err := db.file.WriteAt(paddingBytes, fileSize); err != nil {
				return fmt.Errorf("failed to write padding: %w", err)
			}
			fileSize += padding
			db.fileSize = fileSize
		}

		// Set the offset for this index page
		indexPage.offset = fileSize

		// Print some debug info
		debugPrint("Writing index page to end of file at offset %d\n", indexPage.offset)

	// If it is an internal existing index page, write to WAL file
	} else if db.journalMode == JournalModeWAL {
		// Write to WAL file
		err := db.writeToWAL(indexPage)
		if err != nil {
			return err
		}
		indexPage.Dirty = false
		return nil

	} else {
		debugPrint("Writing index page to disk at offset %d\n", indexPage.offset)
	}

	// Write to disk at the specified offset
	_, err := db.file.WriteAt(indexPage.data, indexPage.offset)

	// If the page was written successfully
	if err == nil {
		// Mark it as clean
		indexPage.Dirty = false

		// Update file size if this write extended the file
		newEndOffset := indexPage.offset + PageSize
		if newEndOffset > db.fileSize {
			db.fileSize = newEndOffset
		}
	}
	return err
}

// appendData appends a key-value pair to the end of the file and returns its offset
func (db *DB) appendData(key, value []byte) (int64, error) {
	// Use stored file size to determine where to append
	fileSize := db.fileSize

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
	if _, err := db.file.WriteAt(content, fileSize); err != nil {
		return 0, fmt.Errorf("failed to write content: %w", err)
	}

	// Update the file size
	newFileSize := fileSize + int64(totalSize)
	db.fileSize = newFileSize

	debugPrint("Appended content at offset %d, size %d\n", fileSize, totalSize)

	// Return the offset where the content was written
	return fileSize, nil
}

// readContent reads content from a specific offset in the file
func (db *DB) readContent(offset int64) (*Content, error) {
	// Check if offset is valid
	if offset < 0 || offset >= db.fileSize {
		return nil, fmt.Errorf("offset out of file bounds: %d", offset)
	}

	// Read the content type first (1 byte)
	typeBuffer := make([]byte, 1)
	if _, err := db.file.ReadAt(typeBuffer, offset); err != nil {
		return nil, fmt.Errorf("failed to read content type: %w", err)
	}

	contentType := typeBuffer[0]
	content := &Content{
		contentType: contentType,
		offset:      offset,
	}

	if contentType == ContentTypeIndex {
		// Let the caller read the index page

	} else if contentType == ContentTypeData {
		// Read a small chunk to get the key length
		initialBuffer := make([]byte, 10) // Enough for type + varint key length in most cases
		_, err := db.file.ReadAt(initialBuffer, offset)
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
		headerRead, err := db.file.ReadAt(headerBuffer, offset)
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
		if offset + int64(totalSize) > db.fileSize {
			return nil, fmt.Errorf("content extends beyond file size")
		}

		// Read all data at once
		buffer := make([]byte, totalSize)
		n, err := db.file.ReadAt(buffer, offset)
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
		return nil, fmt.Errorf("unknown content type: %c", contentType)
	}

	return content, nil
}

// Utility functions

// generateNewSalt generates a new salt that's different from the old one
func generateNewSalt(oldSalt uint8) uint8 {
	return oldSalt + 1
}

// hashKey hashes the key with the given salt
func hashKey(key []byte, salt uint8) uint64 {
	// Simple FNV-1a hash implementation
	hash := uint64(14695981039346656037)

	// Process the salt
	hash ^= uint64(salt)
	hash *= 1099511628211

	// Process the key
	for _, b := range key {
		hash ^= uint64(b)
		hash *= 1099511628211
	}

	return hash
}

// getIndexSlot calculates the slot for a given key in an index page
func (db *DB) getIndexSlot(key []byte, salt uint8) int {
	// Hash the key with the salt from the index page
	hash := hashKey(key, salt)
	// Calculate slot in the index page
	return int(hash % uint64(MaxIndexEntries))
}

// setIndexEntry sets an entry in an index page for a specific key pointing to a content offset
func (db *DB) setIndexEntry(indexPage *IndexPage, key []byte, contentOffset int64) (int, error) {

	// Calculate slot for the key
	slot := db.getIndexSlot(key, indexPage.Salt)

	// Write the entry
	db.writeIndexEntry(indexPage, slot, contentOffset)

	return slot, nil
}

// readIndexEntry reads an index entry from the specified slot in an index page
func (db *DB) readIndexEntry(indexPage *IndexPage, slot int) int64 {
	if slot < 0 || slot >= MaxIndexEntries {
		return 0
	}

	offset := IndexHeaderSize + (slot * 8) // 8 bytes for offset
	return int64(binary.LittleEndian.Uint64(indexPage.data[offset:offset+8]))
}

// writeIndexEntry writes an index entry to the specified slot in an index page
func (db *DB) writeIndexEntry(indexPage *IndexPage, slot int, contentOffset int64) {
	if slot < 0 || slot >= MaxIndexEntries {
		return
	}

	offset := IndexHeaderSize + (slot * 8) // 8 bytes for offset
	binary.LittleEndian.PutUint64(indexPage.data[offset:offset+8], uint64(contentOffset))

	indexPage.Dirty = true
}

// equal compares two byte slices
func equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// RefreshFileSize updates the cached file size from the actual file
func (db *DB) RefreshFileSize() error {
	fileInfo, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file size: %w", err)
	}

	db.fileSize = fileInfo.Size()
	return nil
}
