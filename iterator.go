package kv_log

// DBIterator implements iteration over database key-value pairs
type DBIterator struct {
	db           *DB
	currentPage  int       // Current main index page
	currentSlot  int       // Current slot in the current page
	currentKey   []byte    // Current key
	currentValue []byte    // Current value
	valid        bool      // Whether the iterator is valid
	stack        []iterPos // Stack for depth-first traversal
	closed       bool      // Whether the iterator is closed
}

// iterPos represents a position in the iteration
type iterPos struct {
	offset int64 // Offset of the index page
	slot   int   // Current slot in the index page
}

// Iterator returns a new iterator for the database
func (db *DB) Iterator() *DBIterator {
	// Create a new iterator
	it := &DBIterator{
		db:          db,
		currentPage: 0,
		currentSlot: -1, // Start at -1 so Next() will move to slot 0
		valid:       false,
		stack:       make([]iterPos, 0),
	}

	// Move to the first entry
	it.Next()
	return it
}

// Next moves the iterator to the next key-value pair
func (it *DBIterator) Next() {
	if it.closed {
		it.valid = false
		return
	}

	// Lock the database for reading
	it.db.mutex.RLock()
	defer it.db.mutex.RUnlock()

	// If we have positions in the stack, process them first (depth-first traversal)
	if len(it.stack) > 0 {
		// Get the top position from the stack
		pos := it.stack[len(it.stack)-1]

		// Move to the next slot in the current index page
		pos.slot++

		// Update the stack
		it.stack[len(it.stack)-1] = pos

		// Check if the offset is valid
		if pos.offset < 0 || pos.offset >= it.db.fileSize {
			// Invalid offset, pop it from the stack and continue
			it.stack = it.stack[:len(it.stack)-1]
			it.Next()
			return
		}

		// Read the index page
		indexPage, err := it.db.readIndexPage(pos.offset)
		if err != nil {
			// If we can't read the page, pop it from the stack and continue
			it.stack = it.stack[:len(it.stack)-1]
			it.Next()
			return
		}

		// Find the next non-empty slot
		for pos.slot < MaxIndexEntries {
			contentOffset := it.db.readIndexEntry(indexPage, pos.slot)
			if contentOffset != 0 {
				// Check if the offset is valid
				if contentOffset < 0 || contentOffset >= it.db.fileSize {
					// Invalid offset, move to the next slot
					pos.slot++
					it.stack[len(it.stack)-1] = pos
					continue
				}

				// Found an entry, process it
				content, err := it.db.readContent(contentOffset)
				if err == nil {
					if content.contentType == ContentTypeData {
						// It's a data entry, return it
						it.currentKey = content.key
						it.currentValue = content.value
						it.valid = true
						return
					} else if content.contentType == ContentTypeIndex {
						// It's an index page, push it to the stack
						it.stack = append(it.stack, iterPos{
							offset: contentOffset,
							slot:   -1, // Start at -1 so we'll move to slot 0
						})
						// Process this new index page
						it.Next()
						return
					}
				}
			}

			// Move to the next slot
			pos.slot++
			it.stack[len(it.stack)-1] = pos
		}

		// If we get here, we've exhausted this index page
		it.stack = it.stack[:len(it.stack)-1]
		// Continue with the next entry
		it.Next()
		return
	}

	// Process main index pages
	for it.currentPage < it.db.mainIndexPages {
		// Move to the next slot
		it.currentSlot++

		// If we've exhausted the current page, move to the next page
		if it.currentSlot >= MaxIndexEntries {
			it.currentPage++
			it.currentSlot = 0

			// If we've exhausted all pages, we're done
			if it.currentPage >= it.db.mainIndexPages {
				it.valid = false
				return
			}
		}

		// Calculate the offset of the current main index page
		pageOffset := int64(it.currentPage + 1) * PageSize

		// Check if page offset is valid
		if pageOffset >= it.db.fileSize {
			// Invalid offset, move to the next page
			it.currentPage++
			it.currentSlot = -1 // Start at -1 so we'll move to slot 0
			continue
		}

		// Read the index page
		indexPage, err := it.db.readIndexPage(pageOffset)
		if err != nil {
			// If we can't read the page, move to the next page
			it.currentPage++
			it.currentSlot = -1 // Start at -1 so we'll move to slot 0
			continue
		}

		// Read the entry at the current slot
		contentOffset := it.db.readIndexEntry(indexPage, it.currentSlot)
		if contentOffset == 0 {
			// Empty slot, continue
			continue
		}

		// Check if content offset is valid
		if contentOffset < 0 || contentOffset >= it.db.fileSize {
			// Invalid offset, continue
			continue
		}

		// Read the content at the offset
		content, err := it.db.readContent(contentOffset)
		if err != nil {
			// If we can't read the content, continue
			continue
		}

		if content.contentType == ContentTypeData {
			// It's a data entry, return it
			it.currentKey = content.key
			it.currentValue = content.value
			it.valid = true
			return
		} else if content.contentType == ContentTypeIndex {
			// It's an index page, push it to the stack
			it.stack = append(it.stack, iterPos{
				offset: contentOffset,
				slot:   -1, // Start at -1 so we'll move to slot 0
			})
			// Process this index page
			it.Next()
			return
		}
	}

	// If we get here, we've exhausted all pages
	it.valid = false
}

// Valid returns whether the iterator is valid
func (it *DBIterator) Valid() bool {
	return !it.closed && it.valid
}

// Key returns the current key
func (it *DBIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currentKey
}

// Value returns the current value
func (it *DBIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currentValue
}

// Close closes the iterator
func (it *DBIterator) Close() {
	it.closed = true
	it.valid = false
}
