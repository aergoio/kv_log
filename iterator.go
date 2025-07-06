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
	reverse      bool     // Whether to iterate in reverse order
}

// radixIterPos represents a position in the radix tree traversal
type radixIterPos struct {
	pageNumber  uint32 // Page number of the current page
	pageType    byte   // Type of the current page (radix or leaf)
	byteValue   int    // Current byte value being processed (0-255, -2 for empty suffix, -1 after empty suffix)
	subPageIdx  uint8  // Current sub-page index in radix page
	leafLoaded  bool   // Whether we've loaded and sorted the leaf entries
}

// NewIterator returns a new iterator for the database
// If start > end, the iterator will iterate in reverse order
// If start <= end, the iterator will iterate in forward order
// For forward iteration:
//   - start is inclusive (>=)
//   - end is exclusive (<)
// For reverse iteration:
//   - start is inclusive (<=)
//   - end is inclusive (>=)
func (db *DB) NewIterator(start, end []byte) *Iterator {
	// Determine if we should iterate in reverse order (start > end)
	reverse := false
	if len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) > 0 {
		// In reverse mode, we don't swap start and end
		// We just mark the iterator as reverse
		reverse = true
	}

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
		reverse:     reverse,
	}

	// Start with the root radix page (page 1)
	rootSubPage, err := db.getRootRadixSubPage()
	if err == nil {
		// Push the root page to the stack
		it.stack = append(it.stack, radixIterPos{
			pageNumber: rootSubPage.Page.pageNumber,
			pageType:   ContentTypeRadix,
			byteValue:  it.getStartingByte(),
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
				if it.reverse {
					if len(it.end) > 0 && bytes.Compare(it.currentKey, it.end) < 0 {
						// We've passed the end bound in reverse, stop iteration
						it.valid = false
						it.keyPrefix = it.keyPrefix[:0] // Reset keyPrefix
						return
					}
				} else {
					if len(it.end) > 0 && bytes.Compare(it.currentKey, it.end) >= 0 {
						// We've passed the end bound, stop iteration
						it.valid = false
						it.keyPrefix = it.keyPrefix[:0] // Reset keyPrefix
						return
					}
				}
				// Key is outside our range, continue searching
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
				if it.reverse {
					if len(it.end) > 0 && bytes.Compare(it.currentKey, it.end) < 0 {
						// We've passed the end bound in reverse, stop iteration
						it.valid = false
						it.keyPrefix = it.keyPrefix[:0] // Reset keyPrefix
						return
					}
				} else {
					if len(it.end) > 0 && bytes.Compare(it.currentKey, it.end) >= 0 {
						// We've passed the end bound, stop iteration
						it.valid = false
						it.keyPrefix = it.keyPrefix[:0] // Reset keyPrefix
						return
					}
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
	it.keyPrefix = it.keyPrefix[:0] // Reset keyPrefix when iteration is done
}

// processRadixByte processes a single byte value in a radix page
// Returns (foundEntry, shouldContinue):
// - foundEntry: true if a valid entry was found and we should return true from the calling function
// - shouldContinue: true if we should continue to the next byte in the loop, false if we should return
func (it *Iterator) processRadixByte(pos *radixIterPos, subPage *RadixSubPage, byteValue uint8) (bool, bool) {
	// Check if we should skip this subtree based on range constraints
	if it.shouldSkipSubtree(byteValue) {
		return false, true // No entry found, continue to next byte
	}

	// Get the next page number and sub-page index
	nextPageNumber, nextSubPageIdx := it.db.getRadixEntry(subPage, byteValue)
	if nextPageNumber == 0 {
		// No entry for this byte, continue
		return false, true // No entry found, continue to next byte
	}

	// Found an entry, load the page
	page, err := it.db.getPage(nextPageNumber)
	if err != nil {
		// If we can't load the page, continue
		return false, true // No entry found, continue to next byte
	}

	// Add this byte to the key prefix
	it.keyPrefix = append(it.keyPrefix, byteValue)

	// Push the new page to the stack
	if page.pageType == ContentTypeRadix {
		it.stack = append(it.stack, radixIterPos{
			pageNumber: nextPageNumber,
			pageType:   ContentTypeRadix,
			byteValue:  it.getStartingByte(), // Get starting byte based on iteration direction
			subPageIdx: nextSubPageIdx,
		})
		return it.processRadixPage(&it.stack[len(it.stack)-1]), false // Return result of processRadixPage, don't continue
	} else if page.pageType == ContentTypeLeaf {
		it.stack = append(it.stack, radixIterPos{
			pageNumber: nextPageNumber,
			pageType:   ContentTypeLeaf,
			leafLoaded: false,
		})
		return it.processLeafPage(&it.stack[len(it.stack)-1]), false // Return result of processLeafPage, don't continue
	}

	return false, true // No entry found, continue to next byte
}

// processEmptySuffix processes the empty suffix of a radix page
func (it *Iterator) processEmptySuffix(subPage *RadixSubPage) bool {
	// Process the empty suffix
	emptySuffixOffset := it.db.getEmptySuffixOffset(subPage)
	if emptySuffixOffset > 0 {
		// Read the content at the offset
		content, err := it.db.readContent(emptySuffixOffset)
		if err == nil {
			// Found a valid entry
			it.currentKey = content.key
			it.currentValue = content.value
			it.valid = true

			return true
		}
	}
	return false
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

	// Process next byte value
	if !it.reverse {
		// Check if we need to process the empty suffix now
		if pos.byteValue == -2 {
			// Mark that we've processed the empty suffix
			pos.byteValue++
			// Process the empty suffix
			if it.processEmptySuffix(subPage) {
				return true
			}
		}
		// Forward iteration (ascending order)
		for pos.byteValue < 255 {
			// Move to the next byte value
			pos.byteValue++
			byteValue := uint8(pos.byteValue)

			foundEntry, shouldContinue := it.processRadixByte(pos, subPage, byteValue)
			if !shouldContinue {
				return foundEntry
			}
			// If shouldContinue is true, we continue the loop
		}
	} else {
		// Reverse iteration (descending order)
		for pos.byteValue > 0 {
			// Move to the previous byte value
			pos.byteValue--
			byteValue := uint8(pos.byteValue)

			foundEntry, shouldContinue := it.processRadixByte(pos, subPage, byteValue)
			if !shouldContinue {
				return foundEntry
			}
			// If shouldContinue is true, we continue the loop
		}
		// Process the empty suffix for this radix node in reverse order
		if pos.byteValue == 0 {
			// Mark that we've processed the empty suffix
			pos.byteValue--
			// Process the empty suffix for this radix node in reverse order
			if it.processEmptySuffix(subPage) {
				// We've found a valid entry, so return true
				return true
			}
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
		//it.leafIdx++
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
		// Get the suffix from the leaf page data
		suffix := leafPage.data[entry.SuffixOffset:entry.SuffixOffset+entry.SuffixLen]
		// Construct the full key from key prefix + suffix
		fullKey := make([]byte, len(it.keyPrefix)+entry.SuffixLen)
		copy(fullKey, it.keyPrefix)
		copy(fullKey[len(it.keyPrefix):], suffix)

		// Add to our list with the constructed key and data offset
		it.leafEntries = append(it.leafEntries, leafEntry{
			key:        fullKey,
			dataOffset: entry.DataOffset,
		})
	}

	// Sort entries by key (ascending or descending based on reverse flag)
	sort.Slice(it.leafEntries, func(i, j int) bool {
		cmp := bytes.Compare(it.leafEntries[i].key, it.leafEntries[j].key)
		if it.reverse {
			return cmp > 0 // Descending order for reverse iteration
		}
		return cmp < 0 // Ascending order for forward iteration
	})

	return len(it.leafEntries) > 0
}

// popStackAndTrimPrefix pops the top position from the stack and trims the key prefix
func (it *Iterator) popStackAndTrimPrefix() {
	if len(it.stack) > 0 {
		// Get the last position in the stack
		lastPos := len(it.stack) - 1

		// If we're popping a leaf page, clear the cached entries
		if it.stack[lastPos].pageType == ContentTypeLeaf {
			it.leafEntries = it.leafEntries[:0]
			it.leafIdx = -1

			// Also remove the last byte from the key prefix because we added one when we
			// descended from the parent radix node into this leaf. Failing to remove it
			// causes the iterator to incorrectly believe that we are still inside that
			// branch, which breaks sibling traversal (especially in reverse iteration).
			if len(it.keyPrefix) > 0 {
				it.keyPrefix = it.keyPrefix[:len(it.keyPrefix)-1]
			}
		}

		// If we're popping a radix page and we added a byte to the prefix
		if it.stack[lastPos].pageType == ContentTypeRadix &&
		   it.stack[lastPos].byteValue >= 0 &&
		   len(it.keyPrefix) > 0 {
			// Remove the last byte from the key prefix
			it.keyPrefix = it.keyPrefix[:len(it.keyPrefix)-1]
		}

		// Pop the stack
		it.stack = it.stack[:lastPos]
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

	// Forward iteration - navigate through the radix tree following the start key path
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

				// Push the new page to the stack
				if page.pageType == ContentTypeRadix {
					it.stack = append(it.stack, radixIterPos{
						pageNumber: nextPageNumber,
						pageType:   ContentTypeRadix,
						byteValue:  it.getStartingByte(), // Get starting byte based on iteration direction
						subPageIdx: nextSubPageIdx,
					})
				} else if page.pageType == ContentTypeLeaf {
					it.stack = append(it.stack, radixIterPos{
						pageNumber: nextPageNumber,
						pageType:   ContentTypeLeaf,
						leafLoaded: false,
					})
					break
				}
			} else {
				// No exact match for this byte, find the next larger byte that has an entry
				if it.reverse {
					pos.byteValue = int(targetByte) + 1 // For reverse, set to one more so we'll start from targetByte
				} else {
					pos.byteValue = int(targetByte) - 1 // For forward, set to one less so we'll start from targetByte
				}
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
	// For reverse iteration, the bounds are flipped
	if it.reverse {
		// In reverse mode:
		// - start is the upper bound (inclusive)
		// - end is the lower bound (inclusive)

		// Check upper bound (inclusive)
		if len(it.start) > 0 && bytes.Compare(key, it.start) > 0 {
			return false
		}

		// Check lower bound (inclusive)
		if len(it.end) > 0 && bytes.Compare(key, it.end) < 0 {
			return false
		}
	} else {
		// In forward mode:
		// - start is the lower bound (inclusive)
		// - end is the upper bound (exclusive)

		// Check lower bound (inclusive)
		if len(it.start) > 0 && bytes.Compare(key, it.start) < 0 {
			return false
		}

		// Check upper bound (exclusive)
		if len(it.end) > 0 && bytes.Compare(key, it.end) >= 0 {
			return false
		}
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

	if it.reverse {
		// Check if prefix could lead to keys <= start (upper bound)
		if len(it.start) > 0 {
			if isPrefixGreaterThan(potentialPrefix, it.start) {
				return true
			}
		}

		// Check if prefix could lead to keys >= end (lower bound)
		if len(it.end) > 0 {
			if isPrefixLessThan(potentialPrefix, it.end) {
				return true
			}
		}
	} else {
		// Forward iteration

		// Check if prefix could lead to keys >= start (lower bound)
		if len(it.start) > 0 {
			if isPrefixLessThan(potentialPrefix, it.start) {
				return true
			}
		}

		// Check if prefix could lead to keys < end (upper bound)
		if len(it.end) > 0 {
			if isPrefixGreaterThanOrEqual(potentialPrefix, it.end) {
				return true
			}
		}
	}

	return false
}

// isPrefixLessThan returns true if the prefix is strictly less than the target key
// and can't lead to any keys >= target
func isPrefixLessThan(prefix, target []byte) bool {
	// If prefix is longer than target, compare only the common part
	if len(prefix) > len(target) {
		// If common parts are equal, prefix can lead to keys >= target
		// If common parts are greater, prefix can lead to keys >= target
		return bytes.Compare(prefix[:len(target)], target) < 0
	}

	// If prefix is shorter or equal to target
	cmp := bytes.Compare(prefix, target[:len(prefix)])
	// If prefix is less than the corresponding part of target,
	// AND the prefix is not a prefix of the target, then skip
	return cmp < 0 && (len(prefix) == len(target) || prefix[len(prefix)-1] != target[len(prefix)-1])
}

// isPrefixGreaterThan returns true if the prefix is strictly greater than the target key
// and can't lead to any keys <= target
func isPrefixGreaterThan(prefix, target []byte) bool {
	// If prefix is longer than target, compare only the common part
	if len(prefix) > len(target) {
		// If common parts are equal, prefix can't lead to keys <= target
		// If common parts are less, prefix can lead to keys <= target
		return bytes.Compare(prefix[:len(target)], target) > 0
	}

	// If prefix is shorter or equal to target
	return bytes.Compare(prefix, target) > 0
}

// isPrefixGreaterThanOrEqual returns true if the prefix is greater than or equal to the target key
// and can't lead to any keys < target
func isPrefixGreaterThanOrEqual(prefix, target []byte) bool {
	// If prefix is longer than target, compare only the common part
	if len(prefix) > len(target) {
		// If common parts are equal, prefix can't lead to keys < target
		// If common parts are less, prefix can lead to keys < target
		return bytes.Compare(prefix[:len(target)], target) > 0
	}

	// If prefix is shorter than target
	if len(prefix) < len(target) {
		cmp := bytes.Compare(prefix, target[:len(prefix)])
		// If prefix equals the corresponding part of target, don't skip
		if cmp == 0 {
			return false
		}
		// Otherwise, skip if prefix > corresponding part of target
		return cmp > 0
	}

	// If prefix is equal length to target
	return bytes.Compare(prefix, target) >= 0
}

// getStartingByte returns the appropriate starting byte value based on iteration direction
func (it *Iterator) getStartingByte() int {
	if it.reverse {
		// For reverse iteration, start from after the highest byte value
		// This is 256 because in processRadixPage, we decrement before using the value
		return 256
	} else {
		// For forward iteration, start from before the empty suffix
		return -2
	}
}
