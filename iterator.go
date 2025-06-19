package kv_log

// Iterator implements iteration over database key-value pairs
type Iterator struct {
	db           *DB
	currentKey   []byte    // Current key
	currentValue []byte    // Current value
	valid        bool      // Whether the iterator is valid
	closed       bool      // Whether the iterator is closed
	stack        []radixIterPos // Stack for depth-first traversal
	keyPrefix    []byte    // Current key prefix during traversal
}

// radixIterPos represents a position in the radix tree traversal
type radixIterPos struct {
	pageNumber  uint32 // Page number of the current page
	pageType    byte   // Type of the current page (radix or leaf)
	byteValue   int    // Current byte value being processed (0-255)
	subPageIdx  uint8  // Current sub-page index in radix page
	entryIdx    int    // Current entry index in leaf page
	emptySuffix bool   // Whether we've processed the empty suffix
}

// NewIterator returns a new iterator for the database
func (db *DB) NewIterator() *Iterator {
	// Create a new iterator
	it := &Iterator{
		db:         db,
		valid:      false,
		stack:      make([]radixIterPos, 0),
		keyPrefix:  make([]byte, 0, MaxKeyLength),
	}

	// Start with the root radix page (page 1)
	rootSubPage, err := db.getRootRadixSubPage()
	if err == nil {
		// Push the root page to the stack
		it.stack = append(it.stack, radixIterPos{
			pageNumber: rootSubPage.Page.pageNumber,
			pageType:   ContentTypeRadix,
			byteValue:  -1, // Start at -1 so Next() will move to byte 0
			subPageIdx: rootSubPage.SubPageIdx,
		})

		// Move to the first entry
		it.Next()
	}

	return it
}

// Next moves the iterator to the next key-value pair
func (it *Iterator) Next() {
	if it.closed {
		it.valid = false
		return
	}

	// Lock the database for reading
	it.db.mutex.RLock()
	defer it.db.mutex.RUnlock()

	// If the database is being modified by another process (no lock or wrong lock type)
	// we might get inconsistent data, but that's acceptable for read operations
	// The mutex above protects against concurrent modifications from the same process

	for len(it.stack) > 0 {
		// Get the current position from the top of the stack
		pos := &it.stack[len(it.stack)-1]

		// Process based on page type
		if pos.pageType == ContentTypeRadix {
			// Process radix page
			if !it.processRadixPage(pos) {
				// If we've exhausted this radix page, pop it from the stack and continue
				it.popStackAndTrimPrefix()
				continue
			}
			return
		} else if pos.pageType == ContentTypeLeaf {
			// Process leaf page
			if !it.processLeafPage(pos) {
				// If we've exhausted this leaf page, pop it from the stack and continue
				it.popStackAndTrimPrefix()
				continue
			}
			return
		}

		// If we get here with an unknown page type, pop it and continue
		it.popStackAndTrimPrefix()
	}

	// If we get here, we've exhausted all pages
	it.valid = false
}

// processRadixPage processes the current radix page position
// Returns true if a valid entry was found, false if the page is exhausted
func (it *Iterator) processRadixPage(pos *radixIterPos) bool {
	// Get the radix page
	radixPage, err := it.db.getRadixPage(pos.pageNumber)
	if err != nil {
		return false
	}

	// Create a radix sub-page
	subPage := &RadixSubPage{
		Page:      radixPage,
		SubPageIdx: pos.subPageIdx,
	}

	// Check if we need to process the empty suffix
	if !pos.emptySuffix {
		// First, check the empty suffix
		emptySuffixOffset := it.db.getEmptySuffixOffset(subPage)
		if emptySuffixOffset > 0 {
			// Read the content at the offset
			content, err := it.db.readContent(emptySuffixOffset)
			if err == nil {
				// Found a valid entry
				it.currentKey = content.key
				it.currentValue = content.value
				it.valid = true

				// Mark that we've processed the empty suffix
				pos.emptySuffix = true
				return true
			}
		}

		// Mark that we've processed the empty suffix
		pos.emptySuffix = true
	}

	// Process next byte value
	for pos.byteValue < 255 {
		pos.byteValue++
		byteValue := uint8(pos.byteValue)

		// Get the next page number and sub-page index
		nextPageNumber, nextSubPageIdx := it.db.getRadixEntry(subPage, byteValue)
		if nextPageNumber == 0 {
			// No entry for this byte, continue
			continue
		}

		// Found an entry, load the page
		page, err := it.db.getPage(nextPageNumber)
		if err != nil {
			// If we can't load the page, continue
			continue
		}

		// Add this byte to the key prefix
		it.keyPrefix = append(it.keyPrefix, byteValue)

		// Push the new page to the stack
		if page.PageType == ContentTypeRadix {
			it.stack = append(it.stack, radixIterPos{
				pageNumber: nextPageNumber,
				pageType:   ContentTypeRadix,
				byteValue:  -1, // Start at -1 so we'll move to byte 0
				subPageIdx: nextSubPageIdx,
			})
			return it.processRadixPage(&it.stack[len(it.stack)-1])
		} else if page.PageType == ContentTypeLeaf {
			it.stack = append(it.stack, radixIterPos{
				pageNumber: nextPageNumber,
				pageType:   ContentTypeLeaf,
				entryIdx:   -1, // Start at -1 so we'll move to entry 0
			})
			return it.processLeafPage(&it.stack[len(it.stack)-1])
		}
	}

	// If we get here, we've exhausted this radix page
	return false
}

// processLeafPage processes the current leaf page position
// Returns true if a valid entry was found, false if the page is exhausted
func (it *Iterator) processLeafPage(pos *radixIterPos) bool {
	// Get the leaf page
	leafPage, err := it.db.getLeafPage(pos.pageNumber)
	if err != nil {
		return false
	}

	// Move to the next entry
	pos.entryIdx++

	// Check if we've exhausted all entries
	if pos.entryIdx >= len(leafPage.Entries) {
		return false
	}

	// Get the current entry
	entry := leafPage.Entries[pos.entryIdx]

	// Read the content at the offset
	content, err := it.db.readContent(entry.DataOffset)
	if err != nil {
		// If we can't read the content, move to the next entry
		pos.entryIdx++
		return it.processLeafPage(pos)
	}

	// Found a valid entry
	it.currentKey = content.key
	it.currentValue = content.value
	it.valid = true
	return true
}

// popStackAndTrimPrefix pops the top position from the stack and trims the key prefix
func (it *Iterator) popStackAndTrimPrefix() {
	if len(it.stack) > 0 {
		// If we're popping a radix page and we added a byte to the prefix
		if it.stack[len(it.stack)-1].pageType == ContentTypeRadix &&
		   it.stack[len(it.stack)-1].byteValue >= 0 &&
		   len(it.keyPrefix) > 0 {
			// Remove the last byte from the key prefix
			it.keyPrefix = it.keyPrefix[:len(it.keyPrefix)-1]
		}

		// Pop the stack
		it.stack = it.stack[:len(it.stack)-1]
	}
}

// Valid returns whether the iterator is valid
func (it *Iterator) Valid() bool {
	return !it.closed && it.valid
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currentKey
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currentValue
}

// Close closes the iterator
func (it *Iterator) Close() {
	it.closed = true
	it.valid = false
}
