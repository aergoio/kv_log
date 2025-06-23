package kv_log

import (
	"bytes"
	"sort"
)

// leafEntry represents a key and its data offset from a leaf page
type leafEntry struct {
	key        []byte
	dataOffset int64
}

// Iterator implements iteration over database key-value pairs
type Iterator struct {
	db           *DB
	currentKey   []byte    // Current key
	currentValue []byte    // Current value
	valid        bool      // Whether the iterator is valid
	closed       bool      // Whether the iterator is closed
	stack        []radixIterPos // Stack for depth-first traversal
	keyPrefix    []byte    // Current key prefix during traversal
	start        []byte    // Start key for range filtering (inclusive)
	end          []byte    // End key for range filtering (exclusive)
	leafEntries  []leafEntry // Sorted entries from current leaf page
	leafIdx      int      // Current index in leafEntries
}

// radixIterPos represents a position in the radix tree traversal
type radixIterPos struct {
	pageNumber  uint32 // Page number of the current page
	pageType    byte   // Type of the current page (radix or leaf)
	byteValue   int    // Current byte value being processed (0-255)
	subPageIdx  uint8  // Current sub-page index in radix page
	emptySuffix bool   // Whether we've processed the empty suffix
	leafLoaded  bool   // Whether we've loaded and sorted the leaf entries
}

// NewIterator returns a new iterator for the database
func (db *DB) NewIterator(start, end []byte) *Iterator {
	// Create a new iterator
	it := &Iterator{
		db:          db,
		valid:       false,
		stack:       make([]radixIterPos, 0),
		keyPrefix:   make([]byte, 0, MaxKeyLength),
		start:       start,
		end:         end,
		leafEntries: make([]leafEntry, 0),
		leafIdx:     -1,
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

		// If we have a start key, seek to it
		if len(start) > 0 {
			it.seekToStart()
		} else {
			// Move to the first entry
			it.Next()
		}
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

			// Check if the found key is within our range
			if it.valid && !it.isKeyInRange(it.currentKey) {
				// Key is outside our range, check if we should stop or continue
				if len(it.end) > 0 && bytes.Compare(it.currentKey, it.end) >= 0 {
					// We've passed the end bound, stop iteration
					it.valid = false
					return
				}
				// Key is before start bound, continue searching
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

			// Check if the found key is within our range
			if it.valid && !it.isKeyInRange(it.currentKey) {
				// Key is outside our range, check if we should stop or continue
				if len(it.end) > 0 && bytes.Compare(it.currentKey, it.end) >= 0 {
					// We've passed the end bound, stop iteration
					it.valid = false
					return
				}
				// Key is outside our range, continue searching
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

		// Check if we should skip this subtree based on range constraints
		if it.shouldSkipSubtree(byteValue) {
			continue
		}

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
		if page.pageType == ContentTypeRadix {
			it.stack = append(it.stack, radixIterPos{
				pageNumber: nextPageNumber,
				pageType:   ContentTypeRadix,
				byteValue:  -1, // Start at -1 so we'll move to byte 0
				subPageIdx: nextSubPageIdx,
			})
			return it.processRadixPage(&it.stack[len(it.stack)-1])
		} else if page.pageType == ContentTypeLeaf {
			it.stack = append(it.stack, radixIterPos{
				pageNumber: nextPageNumber,
				pageType:   ContentTypeLeaf,
				emptySuffix: true,
				leafLoaded: false,
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
	// Load and sort leaf entries if not already done
	if !pos.leafLoaded {
		if !it.loadAndSortLeafEntries(pos) {
			return false
		}
		pos.leafLoaded = true
		it.leafIdx = -1 // Start at -1 so we'll move to index 0
	}

	// Move to the next entry in our sorted list
	it.leafIdx++

	// Check if we've exhausted all entries
	if it.leafIdx >= len(it.leafEntries) {
		return false
	}

	// Get the current entry
	entry := it.leafEntries[it.leafIdx]

	// Read the content at the offset to get the value
	content, err := it.db.readContent(entry.dataOffset)
	if err != nil {
		// If we can't read the content, move to the next entry
		it.leafIdx++
		return it.processLeafPage(pos)
	}

	// Set current key and value
	it.currentKey = content.key
	it.currentValue = content.value
	it.valid = true
	return true
}

// loadAndSortLeafEntries loads all entries from a leaf page and sorts them by key
func (it *Iterator) loadAndSortLeafEntries(pos *radixIterPos) bool {
	// Get the leaf page
	leafPage, err := it.db.getLeafPage(pos.pageNumber)
	if err != nil {
		return false
	}

	// Clear previous entries
	it.leafEntries = it.leafEntries[:0]

	// Load all entries from the leaf page - construct keys from prefix + suffix
	for _, entry := range leafPage.Entries {
		// Construct the full key from key prefix + suffix
		fullKey := make([]byte, len(it.keyPrefix)+len(entry.Suffix))
		copy(fullKey, it.keyPrefix)
		copy(fullKey[len(it.keyPrefix):], entry.Suffix)

		// Add to our list with the constructed key and data offset
		it.leafEntries = append(it.leafEntries, leafEntry{
			key:        fullKey,
			dataOffset: entry.DataOffset,
		})
	}

	// Sort entries by key
	sort.Slice(it.leafEntries, func(i, j int) bool {
		return bytes.Compare(it.leafEntries[i].key, it.leafEntries[j].key) < 0
	})

	return len(it.leafEntries) > 0
}

// popStackAndTrimPrefix pops the top position from the stack and trims the key prefix
func (it *Iterator) popStackAndTrimPrefix() {
	if len(it.stack) > 0 {
		// If we're popping a leaf page, clear the cached entries
		if it.stack[len(it.stack)-1].pageType == ContentTypeLeaf {
			it.leafEntries = it.leafEntries[:0]
			it.leafIdx = -1
		}

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

// seekToStart seeks the iterator to the start key position
func (it *Iterator) seekToStart() {
	// Reset the stack to start from root with the start key path
	it.stack = it.stack[:1] // Keep only the root
	it.keyPrefix = it.keyPrefix[:0] // Clear the key prefix

	// Navigate through the radix tree following the start key path
	startKey := it.start
	currentDepth := 0

	// Navigate as far as we can following the exact start key path
	for currentDepth < len(startKey) && len(it.stack) > 0 {
		pos := &it.stack[len(it.stack)-1]

		if pos.pageType == ContentTypeRadix {
			// Get the byte value we need to follow
			targetByte := startKey[currentDepth]

			// Get the radix page
			radixPage, err := it.db.getRadixPage(pos.pageNumber)
			if err != nil {
				break
			}

			subPage := &RadixSubPage{
				Page:      radixPage,
				SubPageIdx: pos.subPageIdx,
			}

			// Try to find the exact byte first
			nextPageNumber, nextSubPageIdx := it.db.getRadixEntry(subPage, targetByte)
			if nextPageNumber != 0 {
				// Found exact match, continue following this path
				page, err := it.db.getPage(nextPageNumber)
				if err != nil {
					break
				}

				// Add this byte to the key prefix and advance
				it.keyPrefix = append(it.keyPrefix, targetByte)
				currentDepth++

				// Set up the position to continue from this byte
				pos.byteValue = int(targetByte)
				pos.emptySuffix = true // We'll check empty suffix when we process this page

				// Push the new page to the stack
				if page.pageType == ContentTypeRadix {
					it.stack = append(it.stack, radixIterPos{
						pageNumber: nextPageNumber,
						pageType:   ContentTypeRadix,
						byteValue:  -1,
						subPageIdx: nextSubPageIdx,
					})
				} else if page.pageType == ContentTypeLeaf {
					it.stack = append(it.stack, radixIterPos{
						pageNumber: nextPageNumber,
						pageType:   ContentTypeLeaf,
						emptySuffix: true,
						leafLoaded: false,
					})
					break
				}
			} else {
				// No exact match for this byte, find the next larger byte that has an entry
				pos.byteValue = int(targetByte) - 1 // Set to one less so Next() will start from targetByte
				pos.emptySuffix = true // Skip empty suffix since we're looking for >= start
				break
			}
		} else {
			// We've reached a leaf page
			break
		}
	}

	// Now use the regular Next() to find the first valid key >= start
	it.Next()
}

// isKeyInRange checks if a key is within the iterator's range
func (it *Iterator) isKeyInRange(key []byte) bool {
	// Check start bound (inclusive)
	if len(it.start) > 0 && bytes.Compare(key, it.start) < 0 {
		return false
	}

	// Check end bound (exclusive)
	if len(it.end) > 0 && bytes.Compare(key, it.end) >= 0 {
		return false
	}

	return true
}
// shouldSkipSubtree checks if we should skip a subtree based on the current key prefix and next byte
func (it *Iterator) shouldSkipSubtree(nextByte uint8) bool {
	// If we have no range constraints, don't skip anything
	if len(it.start) == 0 && len(it.end) == 0 {
		return false
	}

	// Build the potential key prefix with the next byte
	potentialPrefix := make([]byte, len(it.keyPrefix)+1)
	copy(potentialPrefix, it.keyPrefix)
	potentialPrefix[len(it.keyPrefix)] = nextByte

	// Check if this prefix could contain keys in our range

	// If we have a start bound, check if this prefix could lead to keys >= start
	if len(it.start) > 0 {
		// If the potential prefix is longer than or equal to start key length
		if len(potentialPrefix) <= len(it.start) {
			// Compare the prefix with the corresponding part of start key
			startPrefix := it.start[:len(potentialPrefix)]
			if bytes.Compare(potentialPrefix, startPrefix) < 0 {
				// This prefix is before the start key prefix, skip it
				return true
			}
		}
	}

	// If we have an end bound, check if this prefix could lead to keys < end
	if len(it.end) > 0 {
		// If the potential prefix is already >= end key, skip it
		if bytes.Compare(potentialPrefix, it.end) >= 0 {
			return true
		}

		// If the potential prefix is longer than or equal to end key length
		if len(potentialPrefix) <= len(it.end) {
			// Compare the prefix with the corresponding part of end key
			endPrefix := it.end[:len(potentialPrefix)]
			if bytes.Compare(potentialPrefix, endPrefix) > 0 {
				// This prefix is after the end key prefix, skip it
				return true
			}
		}
	}

	return false
}
