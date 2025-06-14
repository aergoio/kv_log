package kv3db

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/aergoio/kv3db/varint"
)

const (
	// Debug mode (set to false to disable debug prints)
	DebugMode = true
	// Page size (4KB)
	PageSize = 4096
	// Base header size (just type identifier)
	BaseHeaderSize = 1
	// Index page header size (type identifier + salt)
	IndexHeaderSize = 2
	// Maximum number of entries in index page
	MaxIndexEntries = (PageSize - IndexHeaderSize) / 4 // 4 = page pointer size
	// Magic number for database identification
	MagicNumber uint32 = 0x48544854 // "HTHT" in ASCII
	// Database version
	Version uint32 = 1
	// Number of pages in main index
	DefaultMainIndexPages = 256 // 1 MB / 4096 bytes per page
	// Initial salt
	InitialSalt = 0
	// Maximum key length
	MaxKeyLength = 2048
	// Maximum value length
	MaxValueLength = 2 << 26 // 128MB
)

// Page types
const (
	PageTypeRoot = iota
	PageTypeIndex
	PageTypeData
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
	file           *os.File
	mu             sync.RWMutex
	mainIndexPages int
}

// Page represents a database page
type Page struct {
	pageType uint8
	pageNum  uint32 // Internal reference, not stored on disk
	data     []byte
}

// IndexPage represents an index page with entries
type IndexPage struct {
	Page     // Embed the base Page
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

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	// Set default options
	mainIndexPages := DefaultMainIndexPages

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
	}

	db := &DB{
		file:          file,
		filePath:      path,
		mainIndexPages: mainIndexPages,
	}

	if !fileExists {
		// Initialize new database
		if err := db.initialize(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to initialize database: %w", err)
		}
	} else {
		// Read existing database header
		if err := db.readHeader(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to read database header: %w", err)
		}
	}

	return db, nil
}

// Close closes the database file
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.file != nil {
		return db.file.Close()
	}
	return nil
}

// Set sets a key-value pair in the database
func (db *DB) Set(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

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
	pageOffset := mainIndexSlot / MaxIndexEntries
	slotInPage := mainIndexSlot % MaxIndexEntries

	// Main index starts at page 2
	mainIndexPageNum := uint32(2 + pageOffset)

	// Read the index page
	page, err := db.readPage(mainIndexPageNum)
	if err != nil {
		return fmt.Errorf("failed to read index page: %w", err)
	}
	indexPage, err := db.parseIndexPage(page)
	if err != nil {
		return fmt.Errorf("failed to parse index page: %w", err)
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
		// Hash the key with the current salt
		hash := hashKey(key, salt)
		// Calculate slot in the index page
		slot = int(hash % uint64(MaxIndexEntries))
	}

	// Check if slot is valid
	if slot >= MaxIndexEntries {
		return fmt.Errorf("hash collision: slot index exceeds maximum entries")
	}

	// Check if the slot is already used
	pageNum := db.readIndexEntry(indexPage, slot)
	if pageNum != 0 {
		// Slot is used, check if it's a data page or another index page
		targetPage, err := db.readPage(pageNum)
		if err != nil {
			return fmt.Errorf("failed to read target page: %w", err)
		}

		if targetPage.pageType == PageTypeData {
			// It's a data page, check if key exists or if we can add it
			if found, err := db.handleDataPageUpdate(targetPage, key, value, salt); err != nil {
				return err
			} else if found {
				return nil // Key was found and handled
			}

		} else if targetPage.pageType == PageTypeIndex {
			// It's another index page
			newIndexPage, err := db.parseIndexPage(targetPage)
			if err != nil {
				return fmt.Errorf("failed to parse index page: %w", err)
			}
			// Assert that the salt is not the initial salt
			if newIndexPage.Salt == InitialSalt {
				return fmt.Errorf("salt from internal index page is the initial salt")
			}
			// Set the key-value pair in the child index page
			// Don't pass forcedSlot for secondary indexes
			err = db.setOnIndex(key, value, newIndexPage)
			if err != nil {
				return err
			}
			if newIndexPage.Dirty {
				// Write the new index page
				return db.writeIndexPage(newIndexPage)
			}
			return nil
		}
	}

	// Slot is available or we need to create a new page
	dataPage, err := db.createDataPage()
	if err != nil {
		return err
	}

	// Insert entry into data page
	if err := db.insertIntoNewDataPage(dataPage, key, value); err != nil {
		return err
	}

	// Update index entry
	db.writeIndexEntry(indexPage, slot, dataPage.pageNum)

	// Write the index page on the caller function
	return nil
}

// handleDataPageUpdate handles the update of a data page or its conversion to an index page
func (db *DB) handleDataPageUpdate(dataPage *Page, key, value []byte, salt uint8) (bool, error) {

	// Calculate required space for new key-value pair with varint encoding
	keyLenSize := varint.Size(uint64(len(key)))
	valueLenSize := varint.Size(uint64(len(value)))

	// Calculate total required space
	requiredSpace := keyLenSize + len(key) + valueLenSize + len(value)

	// Try to find and update the key
	updated, offset, err := db.updateInDataPage(dataPage, key, value, requiredSpace)
	if err != nil {
		return false, err
	}
	if updated {
		return true, nil // Key found and updated
	}

	// Check if there's enough space at the returned offset
	if offset+requiredSpace <= len(dataPage.data) {
		// Enough space to insert at the returned offset
		if err := db.insertAtOffset(dataPage, key, value, offset); err != nil {
			return false, err
		}
		return true, nil
	}

	// Not enough space, need to convert to index page and rehash
	return false, db.convertToIndexAndRehash(dataPage, key, value, salt)
}

// convertToIndexAndRehash converts a data page to an index page and rehashes all its keys
func (db *DB) convertToIndexAndRehash(dataPage *Page, newKey, newValue []byte, oldSalt uint8) error {

	// Create a new index page with a new salt
	newSalt := generateNewSalt(oldSalt)
	newIndexPage, err := db.createIndexPage(newSalt)
	if err != nil {
		return err
	}

	// Use the same page number as the old data page
	newIndexPage.pageNum = dataPage.pageNum

	// Now rehash all keys from the data page and the new key-value pair
	// and set them in the new index page

	// Read all existing key-value pairs
	keys, values, _, err := db.readPairsFromPage(dataPage)
	if err != nil {
		return err
	}

	// Add the new key-value pair
	keys = append(keys, newKey)
	values = append(values, newValue)

	// Insert all key-value pairs using the new salt
	for i := range keys {
		if err := db.setOnIndex(keys[i], values[i], newIndexPage); err != nil {
			return err
		}
	}

	// Assert that the new index page was updated
	if !newIndexPage.Dirty {
		return fmt.Errorf("new index page was not updated")
	}

	// Write the new index page on disk
	return db.writeIndexPage(newIndexPage)
}

// WARNING: The returned slices reference the original page data directly.
// Do not modify the original page data while using the returned slices.
func (db *DB) readPairsFromPage(dataPage *Page) ([][]byte, [][]byte, int, error) {

	// Create slices to store keys and values
	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	// Read all key-value pairs from the data page
	offset := BaseHeaderSize
	for {
		// Check if we have enough bytes to read at least one byte
		if offset >= len(dataPage.data) {
			break
		}

		if dataPage.data[offset] == 0 {
			// No more key-value pairs
			break
		}

		// Read key length
		keyLength64, bytesRead := varint.Read(dataPage.data[offset:])
		if bytesRead == 0 || keyLength64 == 0 || keyLength64 > MaxKeyLength {
			return nil, nil, 0, fmt.Errorf("corrupted data page")
		}
		keyLength := int(keyLength64)
		offset += bytesRead

		// Check if we have enough bytes for the key
		if offset+keyLength > len(dataPage.data) {
			return nil, nil, 0, fmt.Errorf("corrupted data page")
		}

		key := make([]byte, keyLength)
		copy(key, dataPage.data[offset:offset+keyLength])
		offset += keyLength

		// Read value length
		valueLength64, bytesRead := varint.Read(dataPage.data[offset:])
		if bytesRead == 0 || valueLength64 > MaxValueLength {
			return nil, nil, 0, fmt.Errorf("corrupted data page")
		}
		valueLength := int(valueLength64)
		offset += bytesRead

		// Check if we have enough bytes for the value
		if offset+valueLength > len(dataPage.data) {
			return nil, nil, 0, fmt.Errorf("corrupted data page")
		}

		value := make([]byte, valueLength)
		if valueLength > 0 {
			copy(value, dataPage.data[offset:offset+valueLength])
			offset += valueLength
		}

		keys = append(keys, key)
		values = append(values, value)
	}

	return keys, values, offset, nil
}

// Get retrieves a value for the given key
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

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
	pageOffset := mainIndexSlot / MaxIndexEntries
	slotInPage := mainIndexSlot % MaxIndexEntries

	// Main index starts at page 2
	mainIndexPageNum := uint32(2 + pageOffset)

	page, err := db.readPage(mainIndexPageNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read index page: %w", err)
	}

	return db.getFromIndex(key, page, InitialSalt, slotInPage)
}

// getFromIndex retrieves a value using the specified salt and page number
func (db *DB) getFromIndex(key []byte, page *Page, salt uint8, forcedSlot ...int) ([]byte, error) {

	// Read the index page
	indexPage, err := db.parseIndexPage(page)
	if err != nil {
		return nil, fmt.Errorf("failed to read index page: %w", err)
	}

	var slot int
	if salt == InitialSalt && len(forcedSlot) > 0 {
		// Use the forced slot for main index (salt 0)
		slot = forcedSlot[0]
	} else {
		// Hash the key with the current salt
		hash := hashKey(key, salt)
		// Calculate slot in the index page
		slot = int(hash % uint64(MaxIndexEntries))
	}

	// Check if slot has an entry
	pageNum := db.readIndexEntry(indexPage, slot)
	if pageNum == 0 {
		return nil, fmt.Errorf("key not found")
	}

	// Read the target page
	targetPage, err := db.readPage(pageNum)
	if err != nil {
		return nil, fmt.Errorf("failed to read target page: %w", err)
	}

	if targetPage.pageType == PageTypeData {
		// It's a data page, search for key
		value, found, err := db.findInDataPage(targetPage, key)
		if err != nil {
			return nil, err
		}

		if found {
			return value, nil
		}

		return nil, fmt.Errorf("key not found")

	} else if targetPage.pageType == PageTypeIndex {
		// It's another index page, follow the chain with its salt
		salt := targetPage.data[1]
		// Don't pass forcedSlot for secondary indexes
		return db.getFromIndex(key, targetPage, salt)
	}

	return nil, fmt.Errorf("invalid page type")
}

// Helper functions

// initialize creates a new database file structure
func (db *DB) initialize() error {

	debugPrint("Initializing database\n")

	// Write file header (8 bytes) in root page (page 1)
	rootPage := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(rootPage[0:4], MagicNumber)
	binary.LittleEndian.PutUint32(rootPage[4:8], Version)

	// The rest of the root page is reserved for future use

	debugPrint("Writing root page to disk\n")
	if _, err := db.file.WriteAt(rootPage, 0); err != nil {
		return err
	}

	// Create all pages for the main index, starting at page 2
	for i := 0; i < db.mainIndexPages; i++ {
		indexPage := &IndexPage{
			Page: Page{
				pageType: PageTypeIndex,
				pageNum:  uint32(2 + i),
				data:     make([]byte, PageSize),
			},
			Salt: InitialSalt,
		}

		// Write the index page
		if err := db.writeIndexPage(indexPage); err != nil {
			return err
		}
	}

	debugPrint("Database initialized\n")

	return nil
}

// readHeader reads the database header
func (db *DB) readHeader() error {
	header := make([]byte, 8)
	if _, err := db.file.ReadAt(header, 0); err != nil {
		return err
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	version := binary.LittleEndian.Uint32(header[4:8])

	if magic != MagicNumber {
		return fmt.Errorf("invalid database file format")
	}

	if version != Version {
		return fmt.Errorf("unsupported database version: %d", version)
	}

	return nil
}

// readPage reads a page from the database file
func (db *DB) readPage(pageNum uint32) (*Page, error) {
	debugPrint("Reading page from disk %d\n", pageNum)
	offset := int64(pageNum - 1) * PageSize
	data := make([]byte, PageSize)

	if _, err := db.file.ReadAt(data, offset); err != nil {
		return nil, err
	}

	// All pages have at least a type identifier
	pageType := data[0]

	page := &Page{
		pageType: pageType,
		pageNum:  pageNum,
		data:     data,
	}

	return page, nil
}

// parseIndexPage parses an index page and returns its structured representation
func (db *DB) parseIndexPage(page *Page) (*IndexPage, error) {

	pageType := page.data[0]
	if pageType != PageTypeIndex {
		return nil, fmt.Errorf("not an index page")
	}

	// Create structured index page
	indexPage := &IndexPage{
		Page: Page{
			pageType: page.pageType,
			pageNum:  page.pageNum,
			data:     page.data,
		},
		Salt: page.data[1],
	}

	return indexPage, nil
}

// writePage writes a page to the database file
func (db *DB) writePage(page *Page) error {

	// If page number is 0, allocate a new page number
	if page.pageNum == 0 {
		newPageNum, err := db.getNextPageNum()
		if err != nil {
			return err
		}
		page.pageNum = newPageNum
	}

	// The page already contains the full data including header
	// Just write it directly to disk
	offset := int64(page.pageNum - 1) * PageSize
	debugPrint("Writing page to disk %d, offset %d\n", page.pageNum, offset)
	_, err := db.file.WriteAt(page.data, offset)
	return err
}

// writeIndexPage writes an index page to the database file
func (db *DB) writeIndexPage(indexPage *IndexPage) error {
	// Set page type and salt in the data
	indexPage.data[0] = PageTypeIndex  // Type identifier
	indexPage.data[1] = indexPage.Salt // Salt for index pages

	// If page number is 0, allocate a new page number
	if indexPage.pageNum == 0 {
		newPageNum, err := db.getNextPageNum()
		if err != nil {
			return err
		}
		indexPage.pageNum = newPageNum
	}

	// Write to disk
	offset := int64(indexPage.pageNum - 1) * PageSize
	debugPrint("Writing index page to disk %d, offset %d\n", indexPage.pageNum, offset)
	_, err := db.file.WriteAt(indexPage.data, offset)
	return err
}

func (db *DB) getNextPageNum() (uint32, error) {
	fileInfo, err := db.file.Stat()
	if err != nil {
		return 0, err
	}
	newPageNum := uint32(fileInfo.Size() / PageSize)
	if newPageNum == 0 {
		newPageNum = 2 // Skip header (page 1) which is reserved
	} else {
		newPageNum++
	}
	return newPageNum, nil
}

// createDataPage creates a new empty data page
func (db *DB) createDataPage() (*Page, error) {

	data := make([]byte, PageSize)
	data[0] = PageTypeData // Set page type in header

	page := &Page{
		pageType: PageTypeData,
		data:     data,
	}

	return page, nil
}

// createIndexPage creates a new empty index page
func (db *DB) createIndexPage(salt uint8) (*IndexPage, error) {

	data := make([]byte, PageSize)
	data[0] = PageTypeIndex // Set page type in header
	data[1] = salt          // Set salt in header

	indexPage := &IndexPage{
		Page: Page{
			pageType: PageTypeIndex,
			data:     data,
		},
		Salt:    salt,
	}

	return indexPage, nil
}

// updateInDataPage updates a key-value pair in a data page if it exists
// Returns (found, offset, error) where:
// - found: true if key was found and updated
// - offset: if key was not found, the offset where new data can be inserted
// - error: any error that occurred
func (db *DB) updateInDataPage(page *Page, key, value []byte, requiredSpace int) (bool, int, error) {

	// Load all key-value pairs from the page into memory first
	keys, values, offset, err := db.readPairsFromPage(page)
	if err != nil {
		return false, 0, err
	}

	// Check if the key already exists
	keyExists := false
	keyIndex := -1
	for i, existingKey := range keys {
		if equal(existingKey, key) {
			keyExists = true
			keyIndex = i
			break
		}
	}

	if keyExists {
		// Key exists, create a new page and rewrite all data
		newPage, err := db.createDataPage()
		if err != nil {
			return false, 0, err
		}

		// Replace the value for the matching key
		values[keyIndex] = value

		// Start writing at the beginning of the data area
		offset := BaseHeaderSize

		// Write all key-value pairs to the new page
		for i := range keys {
			// Write key length
			keyLenSize := varint.Write(newPage.data[offset:], uint64(len(keys[i])))
			offset += keyLenSize

			// Write key
			copy(newPage.data[offset:], keys[i])
			offset += len(keys[i])

			// Write value length
			valueLenSize := varint.Write(newPage.data[offset:], uint64(len(values[i])))
			offset += valueLenSize

			// Write value
			copy(newPage.data[offset:], values[i])
			offset += len(values[i])
		}

		// Update the page number to match the original page
		originalPageNum := page.pageNum
		newPage.pageNum = originalPageNum

		// Write the new page to disk
		if err := db.writePage(newPage); err != nil {
			return false, 0, err
		}

		return true, 0, nil
	}

	// Key doesn't exist, return the offset where it can be inserted
	return false, offset, nil
}

// insertIntoNewDataPage inserts a key-value pair into new data page(s)
func (db *DB) insertIntoNewDataPage(page *Page, key, value []byte) error {
	// Calculate required space for varint encoding
	keyLenSize := varint.Size(uint64(len(key)))
	valueLenSize := varint.Size(uint64(len(value)))

	// Calculate total required space
	requiredSpace := keyLenSize + len(key) + valueLenSize + len(value)

	/*
	// Find free space at end of data
	offset, err := db.findEndOfData(page)
	if err != nil {
		return err
	}
	*/
	offset := BaseHeaderSize

	// Check if there's enough space
	if offset+requiredSpace > len(page.data) {
		return fmt.Errorf("data page full")
	}

	// Use insertAtOffset to insert the key-value pair
	return db.insertAtOffset(page, key, value, offset)
}

// insertAtOffset inserts a key-value pair into a data page at the specified offset
func (db *DB) insertAtOffset(page *Page, key, value []byte, offset int) error {
	// Write key length with varint encoding directly to page.data
	keyLenSize := varint.Write(page.data[offset:], uint64(len(key)))
	offset += keyLenSize

	// Write key
	copy(page.data[offset:offset+len(key)], key)
	offset += len(key)

	// Write value length with varint encoding directly to page.data
	valueLenSize := varint.Write(page.data[offset:], uint64(len(value)))
	offset += valueLenSize

	// Write value
	copy(page.data[offset:offset+len(value)], value)

	return db.writePage(page)
}

// findInDataPage finds a key in a data page
func (db *DB) findInDataPage(page *Page, key []byte) ([]byte, bool, error) {
	offset := BaseHeaderSize

	// Iterate through all key-value pairs in the data page
	for {
		// Check if we have enough bytes to read at least one byte
		if offset >= len(page.data) {
			break
		}

		// Read key length
		keyLength64, bytesRead := varint.Decode(page.data[offset:])
		if bytesRead == 0 {
			break
		}
		keyLength := int(keyLength64)
		if keyLength == 0 {
			// End of data
			break
		}

		offset += bytesRead

		// Check if we have enough bytes for the key
		if offset+keyLength > len(page.data) {
			return nil, false, fmt.Errorf("corrupted data page")
		}

		// Check if key matches
		if keyLength == len(key) && equal(page.data[offset:offset+keyLength], key) {
			offset += keyLength

			// Read value length
			valueLength64, bytesRead := varint.Decode(page.data[offset:])
			if bytesRead == 0 {
				return nil, false, fmt.Errorf("corrupted data page")
			}
			valueLength := int(valueLength64)
			offset += bytesRead

			// Read value
			if offset+valueLength > len(page.data) {
				return nil, false, fmt.Errorf("corrupted data page")
			}
			value := make([]byte, valueLength)
			copy(value, page.data[offset:offset+valueLength])

			return value, true, nil
		}

		// Skip key
		offset += keyLength

		// Skip value
		valueLength64, bytesRead := varint.Decode(page.data[offset:])
		if bytesRead == 0 {
			return nil, false, fmt.Errorf("corrupted data page")
		}
		valueLength := int(valueLength64)
		offset += bytesRead

		// Make sure we have enough bytes for the value
		if offset+valueLength > len(page.data) {
			return nil, false, fmt.Errorf("corrupted data page")
		}

		offset += valueLength
	}

	return nil, false, nil
}

// findEndOfData finds the offset where data ends in a page
func (db *DB) findEndOfData(page *Page) (int, error) {
	offset := BaseHeaderSize

	// Format: [keyLength:varint][key:keyLength][valueLength:varint][value:valueLength]
	for {
		// Check if we have enough bytes to read at least one byte
		if offset >= len(page.data) {
			break
		}

		// Read key length
		keyLength64, bytesRead := varint.Decode(page.data[offset:])
		if bytesRead == 0 {
			break
		}
		keyLength := int(keyLength64)
		if keyLength == 0 {
			// End of data
			break
		}

		offset += bytesRead

		// Check if we have enough bytes for the key
		if offset+keyLength > len(page.data) {
			return 0, fmt.Errorf("corrupted data page")
		}

		// Skip key
		offset += keyLength

		// Skip value - read value length
		valueLength64, bytesRead := varint.Decode(page.data[offset:])
		if bytesRead == 0 {
			return 0, fmt.Errorf("corrupted data page")
		}
		valueLength := int(valueLength64)
		offset += bytesRead

		// Make sure we have enough bytes for the value
		if offset+valueLength > len(page.data) {
			return 0, fmt.Errorf("corrupted data page")
		}

		offset += valueLength
	}

	return offset, nil
}

// Utility functions

// hashKey hashes the key with the given salt
func hashKey(key []byte, salt uint8) uint64 {
	// Simple FNV-1a hash implementation
	hash := uint64(14695981039346656037)
	for _, b := range key {
		hash ^= uint64(b)
		hash *= 1099511628211
	}

	// Mix in the salt
	hash ^= uint64(salt)
	hash *= 1099511628211

	return hash
}

// generateNewSalt generates a new salt that's different from the old one
func generateNewSalt(oldSalt uint8) uint8 {
	return oldSalt + 1
}

// equal compares two byte slices
func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// readIndexEntry reads an index entry from the specified slot in an index page
func (db *DB) readIndexEntry(indexPage *IndexPage, slot int) uint32 {
	if slot < 0 || slot >= MaxIndexEntries {
		return 0
	}

	offset := IndexHeaderSize + (slot * 4) // 4 bytes for page number
	return binary.LittleEndian.Uint32(indexPage.data[offset:offset+4])
}

// writeIndexEntry writes an index entry to the specified slot in an index page
func (db *DB) writeIndexEntry(indexPage *IndexPage, slot int, pageNum uint32) {
	if slot < 0 || slot >= MaxIndexEntries {
		return
	}

	offset := IndexHeaderSize + (slot * 4) // 4 bytes for page number
	binary.LittleEndian.PutUint32(indexPage.data[offset:offset+4], pageNum)

	indexPage.Dirty = true
}
