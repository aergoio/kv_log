package kv_log

import (
	"fmt"
	"runtime"
	"testing"
	"bytes"
	"path/filepath"
	"encoding/binary"
	"github.com/aergoio/kv_log/varint"
	"hash/crc32"
	"os"
	"strings"
)

// createTempFile creates a temporary database file for testing
func createTempFile(t *testing.T) string {
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "test.db")
}

// Helper function to count entries in a leaf sub-page
func countSubPageEntries(db *DB, leafPage *LeafPage, subPageIdx uint8) int {
	count := 0
	db.iterateLeafSubPageEntries(leafPage, subPageIdx, func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
		count++
		return true
	})
	return count
}

// Helper function to get entry information from a leaf sub-page
func getSubPageEntry(db *DB, leafPage *LeafPage, subPageIdx uint8, entryIndex int) (dataOffset int64, suffix []byte, found bool) {
	currentIndex := 0
	db.iterateLeafSubPageEntries(leafPage, subPageIdx, func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, entryDataOffset int64) bool {
		if currentIndex == entryIndex {
			found = true
			dataOffset = entryDataOffset
			suffix = make([]byte, suffixLen)
			copy(suffix, leafPage.data[suffixOffset:suffixOffset+suffixLen])
			return false // Stop iteration
		}
		currentIndex++
		return true
	})
	return
}

func TestShouldSkipSubtree(t *testing.T) {
	// Create a mock iterator with different range constraints

	// Test 1: No range constraints - should never skip
	it1 := &Iterator{
		start:     nil,
		end:       nil,
		keyPrefix: []byte("k"),
	}

	// Test various byte values - none should be skipped
	for i := 0; i < 256; i++ {
		if it1.shouldSkipSubtree(uint8(i)) {
			t.Errorf("shouldSkipSubtree with no range constraints should not skip byte %d", i)
		}
	}

	// Test 2: Range [key2, key5) - test specific cases
	it2 := &Iterator{
		start:     []byte("key2"),
		end:       []byte("key5"),
		keyPrefix: []byte{},
	}

	// Should not skip 'k' (first byte of "key2" and "key5")
	if it2.shouldSkipSubtree('k') {
		t.Error("shouldSkipSubtree should not skip 'k' for range [key2, key5)")
	}

	// Should skip 'a' (before "key2")
	if !it2.shouldSkipSubtree('a') {
		t.Error("shouldSkipSubtree should skip 'a' for range [key2, key5)")
	}

	// Should skip 'z' (after "key5")
	if !it2.shouldSkipSubtree('z') {
		t.Error("shouldSkipSubtree should skip 'z' for range [key2, key5)")
	}

	// Test 3: With prefix "ke" and range [key2, key5)
	it3 := &Iterator{
		start:     []byte("key2"),
		end:       []byte("key5"),
		keyPrefix: []byte("ke"),
	}

	// Should not skip 'y' (forms "key" which is valid prefix)
	if it3.shouldSkipSubtree('y') {
		t.Error("shouldSkipSubtree should not skip 'y' with prefix 'ke' for range [key2, key5)")
	}

	// Should skip 'a' (forms "kea" which is before "key2")
	if !it3.shouldSkipSubtree('a') {
		t.Error("shouldSkipSubtree should skip 'a' with prefix 'ke' for range [key2, key5)")
	}

	// Test 4: With prefix "key" and range [key2, key5)
	it4 := &Iterator{
		start:     []byte("key2"),
		end:       []byte("key5"),
		keyPrefix: []byte("key"),
	}

	// Should skip '1' (forms "key1" which is before "key2")
	if !it4.shouldSkipSubtree('1') {
		t.Error("shouldSkipSubtree should skip '1' with prefix 'key' for range [key2, key5)")
	}

	// Should not skip '2', '3', '4' (forms "key2", "key3", "key4" which are in range)
	for _, b := range []byte{'2', '3', '4'} {
		if it4.shouldSkipSubtree(b) {
			t.Errorf("shouldSkipSubtree should not skip '%c' with prefix 'key' for range [key2, key5)", b)
		}
	}

	// Should skip '5' and above (forms "key5" and higher which are >= end)
	for _, b := range []byte{'5', '6', '7', '8', '9'} {
		if !it4.shouldSkipSubtree(b) {
			t.Errorf("shouldSkipSubtree should skip '%c' with prefix 'key' for range [key2, key5)", b)
		}
	}

	// Test 5: Only start bound [key2, nil)
	it5 := &Iterator{
		start:     []byte("key2"),
		end:       nil,
		keyPrefix: []byte("key"),
	}

	// Should skip '1' (before start)
	if !it5.shouldSkipSubtree('1') {
		t.Error("shouldSkipSubtree should skip '1' with prefix 'key' for range [key2, nil)")
	}

	// Should not skip '2' and above
	for _, b := range []byte{'2', '3', '4', '5', '6', '7', '8', '9'} {
		if it5.shouldSkipSubtree(b) {
			t.Errorf("shouldSkipSubtree should not skip '%c' with prefix 'key' for range [key2, nil)", b)
		}
	}

	// Test 6: Only end bound [nil, key4)
	it6 := &Iterator{
		start:     nil,
		end:       []byte("key4"),
		keyPrefix: []byte("key"),
	}

	// Should not skip '1', '2', '3' (before end)
	for _, b := range []byte{'1', '2', '3'} {
		if it6.shouldSkipSubtree(b) {
			t.Errorf("shouldSkipSubtree should not skip '%c' with prefix 'key' for range [nil, key4)", b)
		}
	}

	// Should skip '4' and above (>= end)
	for _, b := range []byte{'4', '5', '6', '7', '8', '9'} {
		if !it6.shouldSkipSubtree(b) {
			t.Errorf("shouldSkipSubtree should skip '%c' with prefix 'key' for range [nil, key4)", b)
		}
	}
}

// TestGetTotalSystemMemory tests the getTotalSystemMemory function
func TestGetTotalSystemMemory(t *testing.T) {
	// Call the function
	totalMemory := getTotalSystemMemory()

	// Verify that we got a reasonable value
	if totalMemory <= 0 {
		t.Errorf("getTotalSystemMemory returned non-positive value: %d", totalMemory)
	}

	// Verify that we got at least 1GB (most modern systems have at least this much)
	// For embedded systems or very resource-constrained environments, this might need adjustment
	minMemory := int64(1 << 30) // 1 GB
	if totalMemory < minMemory {
		t.Logf("Warning: System memory is less than 1GB: %d bytes (%.2f GB)",
			totalMemory, float64(totalMemory)/(1<<30))

		// Don't fail the test, but log it as a warning
		// Some CI environments or embedded systems might have less memory
	}

	// Log the detected OS and memory size
	t.Logf("OS: %s, Architecture: %s", runtime.GOOS, runtime.GOARCH)

	// Additional check for specific platforms
	switch runtime.GOOS {
	case "darwin":
		t.Log("macOS detected, using hw.memsize")
	case "freebsd", "netbsd", "openbsd":
		t.Log("BSD system detected, using hw.physmem")
	case "linux":
		t.Log("Linux detected, using /proc/meminfo")
	default:
		t.Logf("Other OS detected: %s, using fallback methods", runtime.GOOS)
	}

	t.Logf("System memory: %d bytes (%.2f GB)", totalMemory, float64(totalMemory)/(1<<30))
}

// TestCalculateDefaultCacheSize tests the calculateDefaultCacheSize function
func TestCalculateDefaultCacheSize(t *testing.T) {
	// Call the function
	cacheSize := calculateDefaultCacheSize()

	// Verify that we got a reasonable value
	if cacheSize <= 0 {
		t.Errorf("calculateDefaultCacheSize returned non-positive value: %d", cacheSize)
	}

	// Verify that we got at least the minimum value
	minCacheSize := 300 // Minimum number of pages
	if cacheSize < minCacheSize {
		t.Errorf("calculateDefaultCacheSize returned less than minimum: %d pages", cacheSize)
	}

	// Calculate the memory size of the cache
	cacheSizeBytes := int64(cacheSize) * PageSize

	// Get the total system memory
	totalMemory := getTotalSystemMemory()

	// Verify that the cache size is not more than 30% of total memory
	// (allowing some flexibility over the 20% target)
	maxCacheBytes := int64(float64(totalMemory) * 0.3)
	if cacheSizeBytes > maxCacheBytes {
		t.Errorf("Cache size too large: %d bytes (%.2f GB), which is more than 30%% of system memory",
			cacheSizeBytes, float64(cacheSizeBytes)/(1<<30))
	}

	// Just for informational purposes, print the cache size
	t.Logf("Cache size: %d pages (%d bytes, %.2f GB)",
		cacheSize, cacheSizeBytes, float64(cacheSizeBytes)/(1<<30))
	t.Logf("Cache percentage of system memory: %.2f%%",
		100*float64(cacheSizeBytes)/float64(totalMemory))
}

// ExampleCalculateDefaultCacheSize provides an example of using calculateDefaultCacheSize
func ExampleCalculateDefaultCacheSize() {
	cacheSize := calculateDefaultCacheSize()
	cacheSizeBytes := int64(cacheSize) * PageSize
	totalMemory := getTotalSystemMemory()

	fmt.Printf("System memory: %.2f GB\n", float64(totalMemory)/(1<<30))
	fmt.Printf("Cache size: %d pages (%.2f GB)\n", cacheSize, float64(cacheSizeBytes)/(1<<30))
	fmt.Printf("Cache percentage: %.2f%%\n", 100*float64(cacheSizeBytes)/float64(totalMemory))

	// Output varies by system, so don't check output
}

// TestDiscardOldPageVersions tests the discardOldPageVersions function
// with different page version scenarios
func TestDiscardOldPageVersions(t *testing.T) {
	// Setup a test DB with sharded cache
	db := &DB{
		txnSequence: 100, // Current transaction sequence
	}

	// Initialize the page cache buckets
	for i := range db.pageCache {
		db.pageCache[i].pages = make(map[uint32]*Page)
	}

	// Helper function to create a page with specific properties
	createPage := func(pageNum uint32, txnSeq int64, dirty bool, isWAL bool) *Page {
		return &Page{
			pageNumber:  pageNum,
			pageType:    ContentTypeRadix,
			txnSequence: txnSeq,
			dirty:       dirty,
			isWAL:       isWAL,
		}
	}

	// Helper function to create a linked list of pages
	createPageChain := func(pageNum uint32, specs []struct {
		txnSeq int64
		dirty  bool
		isWAL  bool
	}) {
		var firstPage *Page
		var prevPage *Page

		for i, spec := range specs {
			page := createPage(pageNum, spec.txnSeq, spec.dirty, spec.isWAL)

			if i == 0 {
				firstPage = page
				bucketIdx := pageNum & 1023
				db.pageCache[bucketIdx].pages[pageNum] = firstPage
			} else {
				prevPage.next = page
			}

			prevPage = page
		}
	}

	// Helper function to verify page chain state after discardOldPageVersions
	verifyPageChain := func(t *testing.T, testCase int, pageNum uint32, expectedChain []struct {
		txnSeq int64
		dirty  bool
		isWAL  bool
	}) {
		bucketIdx := pageNum & 1023
		page := db.pageCache[bucketIdx].pages[pageNum]

		if page == nil && len(expectedChain) > 0 {
			t.Errorf("Case %d: Expected page %d to exist but it was nil", testCase, pageNum)
			return
		}

		if page == nil && len(expectedChain) == 0 {
			return // Correctly nil
		}

		for i, expected := range expectedChain {
			if page == nil {
				t.Errorf("Case %d: Expected %d pages in chain for page %d, but found only %d",
					testCase, len(expectedChain), pageNum, i)
				return
			}

			if page.txnSequence != expected.txnSeq {
				t.Errorf("Case %d: Page %d (position %d): Expected txnSequence=%d, got %d",
					testCase, pageNum, i, expected.txnSeq, page.txnSequence)
			}

			if page.dirty != expected.dirty {
				t.Errorf("Case %d: Page %d (position %d): Expected dirty=%v, got %v",
					testCase, pageNum, i, expected.dirty, page.dirty)
			}

			if page.isWAL != expected.isWAL {
				t.Errorf("Case %d: Page %d (position %d): Expected isWAL=%v, got %v",
					testCase, pageNum, i, expected.isWAL, page.isWAL)
			}

			page = page.next
		}

		if page != nil {
			t.Errorf("Case %d: Page %d chain is longer than expected", testCase, pageNum)
		}
	}

	// Setup test cases
	testCases := []struct {
		name           string
		pageNum        uint32
		inTransaction  bool
		keepWAL        bool
		initialChain   []struct {
			txnSeq int64
			dirty  bool
			isWAL  bool
		}
		expectedChain []struct {
			txnSeq int64
			dirty  bool
			isWAL  bool
		}
	}{
		// Base case 1: Clean page - 4 variations
		{
			name:          "1a. Clean page (inTxn=false, keepWAL=true)",
			pageNum:       1,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Clean page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep the clean page
			},
		},
		{
			name:          "1b. Clean page (inTxn=false, keepWAL=false)",
			pageNum:       2,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Clean page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep the clean page
			},
		},
		{
			name:          "1c. Clean page (inTxn=true, keepWAL=true)",
			pageNum:       3,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Clean page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep the clean page
			},
		},
		{
			name:          "1d. Clean page (inTxn=true, keepWAL=false)",
			pageNum:       4,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Clean page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep the clean page
			},
		},

		// Base case 2: Dirty page - 4 variations
		{
			name:          "2a. Dirty page (inTxn=false, keepWAL=true)",
			pageNum:       5,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Dirty page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the dirty page
			},
		},
		{
			name:          "2b. Dirty page (inTxn=false, keepWAL=false)",
			pageNum:       6,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Dirty page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the dirty page
			},
		},
		{
			name:          "2c. Dirty page (inTxn=true, keepWAL=true)",
			pageNum:       7,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Dirty page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the dirty page
			},
		},
		{
			name:          "2d. Dirty page (inTxn=true, keepWAL=false)",
			pageNum:       8,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Dirty page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the dirty page
			},
		},

		// Base case 3: Dirty + Clean - 4 variations
		{
			name:          "3a. Dirty + Clean (inTxn=false, keepWAL=true)",
			pageNum:       9,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, false, false}, // Clean page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep only the dirty page
			},
		},
		{
			name:          "3b. Dirty + Clean (inTxn=false, keepWAL=false)",
			pageNum:       10,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, false, false}, // Clean page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep only the dirty page
			},
		},
		{
			name:          "3c. Dirty + Clean (inTxn=true, keepWAL=true)",
			pageNum:       11,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction (>= currentTxnSeq=99)
				{98, false, false}, // Clean page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Should keep page >= currentTxnSeq
				{98, false, false}, // Should keep first page < currentTxnSeq
			},
		},
		{
			name:          "3d. Dirty + Clean (inTxn=true, keepWAL=false)",
			pageNum:       12,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction (>= currentTxnSeq=99)
				{98, false, false}, // Clean page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Should keep page >= currentTxnSeq
				{98, false, false}, // Should keep first page < currentTxnSeq
			},
		},

		// Base case 4: Current Dirty + Previous Dirty + Clean - 4 variations
		{
			name:          "4a. Current Dirty + Previous Dirty + Clean (inTxn=false, keepWAL=true)",
			pageNum:       13,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, true, false},  // Dirty page from older transaction
				{97, false, false}, // Clean page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the first dirty page
			},
		},
		{
			name:          "4b. Current Dirty + Previous Dirty + Clean (inTxn=false, keepWAL=false)",
			pageNum:       14,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, true, false},  // Dirty page from older transaction
				{97, false, false}, // Clean page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the first dirty page
			},
		},
		{
			name:          "4c. Current Dirty + Previous Dirty + Clean (inTxn=true, keepWAL=true)",
			pageNum:       15,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Dirty page from current transaction (>= currentTxnSeq=99)
				{99, true, false},  // Dirty page from previous transaction (>= currentTxnSeq=99)
				{98, false, false}, // Clean page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Should keep page >= currentTxnSeq
				{99, true, false},  // Should keep page >= currentTxnSeq
				{98, false, false}, // Should keep first page < currentTxnSeq
			},
		},
		{
			name:          "4d. Current Dirty + Previous Dirty + Clean (inTxn=true, keepWAL=false)",
			pageNum:       16,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Dirty page from current transaction (>= currentTxnSeq=99)
				{99, true, false},  // Dirty page from previous transaction (>= currentTxnSeq=99)
				{98, false, false}, // Clean page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Should keep page >= currentTxnSeq
				{99, true, false},  // Should keep page >= currentTxnSeq
				{98, false, false}, // Should keep first page < currentTxnSeq
			},
		},

		// Base case 5: Current Dirty + Previous Dirty - 4 variations
		{
			name:          "5a. Current Dirty + Previous Dirty (inTxn=false, keepWAL=true)",
			pageNum:       17,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Dirty page from previous transaction
				{98, true, false}, // Dirty page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep only the first dirty page
			},
		},
		{
			name:          "5b. Current Dirty + Previous Dirty (inTxn=false, keepWAL=false)",
			pageNum:       18,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Dirty page from previous transaction
				{98, true, false}, // Dirty page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep only the first dirty page
			},
		},
		{
			name:          "5c. Current Dirty + Previous Dirty (inTxn=true, keepWAL=true)",
			pageNum:       19,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Dirty page from current transaction
				{99, true, false},  // Dirty page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Should keep current transaction page
				{99, true, false},  // Should keep previous transaction page
			},
		},
		{
			name:          "5d. Current Dirty + Previous Dirty (inTxn=true, keepWAL=false)",
			pageNum:       20,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Dirty page from current transaction
				{99, true, false},  // Dirty page from previous transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Should keep current transaction page
				{99, true, false},  // Should keep previous transaction page
			},
		},

		// Base case 6: Current Dirty + Previous Dirty + WAL - 4 variations
		{
			name:          "6a. Current Dirty + Previous Dirty + WAL (inTxn=false, keepWAL=true)",
			pageNum:       21,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, true, false},  // Dirty page from older transaction
				{97, false, true},  // WAL page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the first dirty page
				{97, false, true}, // Should keep WAL page
			},
		},
		{
			name:          "6b. Current Dirty + Previous Dirty + WAL (inTxn=false, keepWAL=false)",
			pageNum:       22,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, true, false},  // Dirty page from older transaction
				{97, false, true},  // WAL page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Should keep the first dirty page
			},
		},
		{
			name:          "6c. Current Dirty + Previous Dirty + WAL (inTxn=true, keepWAL=true)",
			pageNum:       23,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Dirty page from current transaction
				{99, true, false},  // Dirty page from previous transaction
				{98, false, true},  // WAL page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Should keep current transaction page
				{99, true, false},  // Should keep previous transaction page
				{98, false, true},  // Should keep WAL page
			},
		},
		{
			name:          "6d. Current Dirty + Previous Dirty + WAL (inTxn=true, keepWAL=false)",
			pageNum:       24,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Dirty page from current transaction
				{99, true, false},  // Dirty page from previous transaction
				{98, false, true},  // WAL page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{100, true, false}, // Should keep current transaction page
				{99, true, false},  // Should keep previous transaction page
				{98, false, false}, // Should keep page but clear WAL flag
			},
		},

		// Base case 7: WAL + WAL - 4 variations
		{
			name:          "7a. WAL + WAL (inTxn=false, keepWAL=true)",
			pageNum:       25,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction
				{98, false, true}, // WAL page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep only the first WAL page
			},
		},
		{
			name:          "7b. WAL + WAL (inTxn=false, keepWAL=false)",
			pageNum:       26,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction
				{98, false, true}, // WAL page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep only the first page but clear WAL flag
			},
		},
		{
			name:          "7c. WAL + WAL (inTxn=true, keepWAL=true)",
			pageNum:       27,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, false, true}, // WAL page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep page >= currentTxnSeq
				{98, false, true}, // Should keep first WAL page < currentTxnSeq
			},
		},
		{
			name:          "7d. WAL + WAL (inTxn=true, keepWAL=false)",
			pageNum:       28,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, false, true}, // WAL page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep page >= currentTxnSeq but clear WAL flag
				{98, false, false}, // Should keep first page < currentTxnSeq but clear WAL flag
			},
		},

		// Base case 8: Previous Dirty + WAL + WAL - 4 variations
		{
			name:          "8a. Previous Dirty + WAL + WAL (inTxn=false, keepWAL=true)",
			pageNum:       29,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, false, true},  // WAL page from older transaction
				{97, false, true},  // WAL page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the dirty page
				{98, false, true}, // Should keep only the first WAL page
			},
		},
		{
			name:          "8b. Previous Dirty + WAL + WAL (inTxn=false, keepWAL=false)",
			pageNum:       30,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, false, true},  // WAL page from older transaction
				{97, false, true},  // WAL page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Should keep the dirty page
			},
		},
		{
			name:          "8c. Previous Dirty + WAL + WAL (inTxn=true, keepWAL=true)",
			pageNum:       31,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, false, true},  // WAL page from older transaction
				{97, false, true},  // WAL page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false}, // Should keep the dirty page
				{98, false, true}, // Should keep only the first WAL page
			},
		},
		{
			name:          "8d. Previous Dirty + WAL + WAL (inTxn=true, keepWAL=false)",
			pageNum:       32,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Dirty page from previous transaction
				{98, false, true},  // WAL page from older transaction
				{97, false, true},  // WAL page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Should keep the dirty page
				{98, false, false}, // Should keep first page but clear WAL flag
			},
		},

		// Base case 9: WAL + Dirty + WAL - 4 variations
		{
			name:          "9a. WAL + Dirty + WAL (inTxn=false, keepWAL=true)",
			pageNum:       33,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction
				{98, true, false}, // Dirty page from older transaction
				{97, false, true}, // WAL page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep only the first WAL page
			},
		},
		{
			name:          "9b. WAL + Dirty + WAL (inTxn=false, keepWAL=false)",
			pageNum:       34,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction
				{98, true, false}, // Dirty page from older transaction
				{97, false, true}, // WAL page from even older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep only the first page but clear WAL flag
			},
		},
		{
			name:          "9c. WAL + Dirty + WAL (inTxn=true, keepWAL=true)",
			pageNum:       35,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, true, false}, // Dirty page from older transaction (< currentTxnSeq=99)
				{97, false, true}, // WAL page from even older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep page >= currentTxnSeq
				{98, true, false}, // Should keep first page < currentTxnSeq
				{97, false, true}, // Should keep first WAL page < currentTxnSeq (and stop)
			},
		},
		{
			name:          "9d. WAL + Dirty + WAL (inTxn=true, keepWAL=false)",
			pageNum:       36,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, true, false}, // Dirty page from older transaction (< currentTxnSeq=99)
				{97, false, true}, // WAL page from even older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep page >= currentTxnSeq but clear WAL flag
				{98, true, false},  // Should keep first page < currentTxnSeq
			},
		},

		// Base case 10: WAL + Clean - 4 variations
		{
			name:          "10a. WAL + Clean (inTxn=false, keepWAL=true)",
			pageNum:       37,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // WAL page from previous transaction
				{98, false, false}, // Clean page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep only the WAL page
			},
		},
		{
			name:          "10b. WAL + Clean (inTxn=false, keepWAL=false)",
			pageNum:       38,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // WAL page from previous transaction
				{98, false, false}, // Clean page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep only the first page but clear WAL flag
			},
		},
		{
			name:          "10c. WAL + Clean (inTxn=true, keepWAL=true)",
			pageNum:       39,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, false, false}, // Clean page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // Should keep page >= currentTxnSeq
				{98, false, false}, // Should keep first page < currentTxnSeq
			},
		},
		{
			name:          "10d. WAL + Clean (inTxn=true, keepWAL=false)",
			pageNum:       40,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, false, false}, // Clean page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep page >= currentTxnSeq but clear WAL flag
				{98, false, false}, // Should keep first page < currentTxnSeq
			},
		},

		// Base case 11: WAL + Dirty - 4 variations
		{
			name:          "11a. WAL + Dirty (inTxn=false, keepWAL=true)",
			pageNum:       41,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction
				{98, true, false}, // Dirty page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep only the WAL page
			},
		},
		{
			name:          "11b. WAL + Dirty (inTxn=false, keepWAL=false)",
			pageNum:       42,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction
				{98, true, false}, // Dirty page from older transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep only the first page but clear WAL flag
			},
		},
		{
			name:          "11c. WAL + Dirty (inTxn=true, keepWAL=true)",
			pageNum:       43,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, true, false}, // Dirty page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep page >= currentTxnSeq
				{98, true, false}, // Should keep first page < currentTxnSeq
			},
		},
		{
			name:          "11d. WAL + Dirty (inTxn=true, keepWAL=false)",
			pageNum:       44,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, true, false}, // Dirty page from older transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep page >= currentTxnSeq but clear WAL flag
				{98, true, false},  // Should keep first page < currentTxnSeq
			},
		},

		// Base case 12: Complex case: WAL + Dirty + WAL + Dirty + Clean - 4 variations
		{
			name:          "12a. Complex case: WAL + Dirty + WAL + Dirty + Clean (inTxn=false, keepWAL=true)",
			pageNum:       45,
			inTransaction: false,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // WAL page from previous transaction
				{98, true, false},  // Dirty page from older transaction
				{97, false, true},  // WAL page from even older transaction
				{96, true, false},  // Dirty page from even older transaction
				{95, false, false}, // Clean page from oldest transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep only the first WAL page
			},
		},
		{
			name:          "12b. Complex case: WAL + Dirty + WAL + Dirty + Clean (inTxn=false, keepWAL=false)",
			pageNum:       46,
			inTransaction: false,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // WAL page from previous transaction
				{98, true, false},  // Dirty page from older transaction
				{97, false, true},  // WAL page from even older transaction
				{96, true, false},  // Dirty page from even older transaction
				{95, false, false}, // Clean page from oldest transaction
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep only the first page but clear WAL flag
			},
		},
		{
			name:          "12c. Complex case: WAL + Dirty + WAL + Dirty + Clean (inTxn=true, keepWAL=true)",
			pageNum:       47,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, true, false},  // Dirty page from older transaction (< currentTxnSeq=99)
				{97, false, true},  // WAL page from even older transaction (< currentTxnSeq=99)
				{96, true, false},  // Dirty page from even older transaction (< currentTxnSeq=99)
				{95, false, false}, // Clean page from oldest transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true}, // Should keep page >= currentTxnSeq
				{98, true, false}, // Should keep first page < currentTxnSeq
				{97, false, true}, // Should keep first WAL page < currentTxnSeq (and stop)
			},
		},
		{
			name:          "12d. Complex case: WAL + Dirty + WAL + Dirty + Clean (inTxn=true, keepWAL=false)",
			pageNum:       48,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},  // WAL page from previous transaction (>= currentTxnSeq=99)
				{98, true, false},  // Dirty page from older transaction (< currentTxnSeq=99)
				{97, false, true},  // WAL page from even older transaction (< currentTxnSeq=99)
				{96, true, false},  // Dirty page from even older transaction (< currentTxnSeq=99)
				{95, false, false}, // Clean page from oldest transaction (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false}, // Should keep page >= currentTxnSeq but clear WAL flag
				{98, true, false},  // Should keep first page < currentTxnSeq
			},
		},

		// Additional test cases to verify currentTxnSeq boundary conditions
		{
			name:          "13a. Boundary test: txnSeq=99 (inTxn=true, keepWAL=true)",
			pageNum:       49,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Exactly at currentTxnSeq boundary (99)
				{98, true, false},  // Below currentTxnSeq boundary
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, true, false},  // Should keep page >= currentTxnSeq
				{98, true, false},  // Should keep first page < currentTxnSeq
			},
		},
		{
			name:          "13b. Boundary test: txnSeq=98 (inTxn=true, keepWAL=true)",
			pageNum:       50,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{98, true, false},  // Below currentTxnSeq boundary
				{97, true, false},  // Below currentTxnSeq boundary
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{98, true, false},  // Should keep first page < currentTxnSeq
			},
		},
		{
			name:          "13c. Boundary test: mixed sequences (inTxn=true, keepWAL=true)",
			pageNum:       51,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{101, true, false},  // Above currentTxnSeq (should be kept)
				{100, true, false},  // Above currentTxnSeq (should be kept)
				{99, true, false},   // At currentTxnSeq (should be kept)
				{98, true, false},   // Below currentTxnSeq (first one should be kept)
				{97, false, false},  // Below currentTxnSeq (should be discarded)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{101, true, false},  // Should keep page >= currentTxnSeq
				{100, true, false},  // Should keep page >= currentTxnSeq
				{99, true, false},   // Should keep page >= currentTxnSeq
				{98, true, false},   // Should keep first page < currentTxnSeq
			},
		},
		{
			name:          "13d. Boundary test: WAL at boundary (inTxn=true, keepWAL=true)",
			pageNum:       52,
			inTransaction: true,
			keepWAL:       true,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},   // WAL at currentTxnSeq boundary (>= currentTxnSeq=99)
				{98, true, false},   // Below currentTxnSeq boundary (< currentTxnSeq=99)
				{97, false, true},   // WAL below currentTxnSeq boundary (< currentTxnSeq=99)
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},   // Should keep page >= currentTxnSeq
				{98, true, false},   // Should keep first page < currentTxnSeq
				{97, false, true},   // Should keep first WAL page < currentTxnSeq (and stop)
			},
		},
		{
			name:          "13e. Boundary test: WAL at boundary (inTxn=true, keepWAL=false)",
			pageNum:       53,
			inTransaction: true,
			keepWAL:       false,
			initialChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, true},   // WAL at currentTxnSeq boundary
				{98, true, false},   // Below currentTxnSeq boundary
				{97, false, true},   // WAL below currentTxnSeq boundary
			},
			expectedChain: []struct {
				txnSeq int64
				dirty  bool
				isWAL  bool
			}{
				{99, false, false},  // Should keep page >= currentTxnSeq but clear WAL flag
				{98, true, false},   // Should keep first page < currentTxnSeq
			},
		},
	}

	// Run each test case
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup the DB state
			db.inTransaction = tc.inTransaction

			// Create the page chain for this test
			createPageChain(tc.pageNum, tc.initialChain)

			// Call the function being tested
			totalPages := db.discardOldPageVersions(tc.keepWAL)

			// Verify the result
			verifyPageChain(t, i+1, tc.pageNum, tc.expectedChain)

			// Verify totalPages is at least the number of pages we created
			if totalPages < len(tc.initialChain) {
				t.Errorf("Case %d: Expected totalPages to be at least %d, got %d",
					i+1, len(tc.initialChain), totalPages)
			}
		})
	}
}

// TestAddLeafEntry tests the addLeafEntry function
// Note: addLeafEntry handles non-empty suffixes on leaf pages.
// Empty suffixes (when all key bytes are consumed) are handled by setOnEmptySuffix on radix pages.
// TestAddEntryToNewLeafSubPage tests the addEntryToNewLeafSubPage function
func TestAddEntryToNewLeafSubPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_addentrytonewleafsubpage.db")

	// Open the database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.rollbackTransaction()

	t.Run("AddEntryToNewSubPage", func(t *testing.T) {
		// Test 1: Add an entry to a new leaf sub-page
		suffix := []byte("test")
		dataOffset := int64(1000)

		leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, dataOffset)
		if err != nil {
			t.Fatalf("Failed to add entry to new leaf sub-page: %v", err)
		}

		// Verify the leaf sub-page was created
		if leafSubPage == nil {
			t.Fatal("Expected leaf sub-page to be created")
		}

		// Verify the leaf page was created
		if leafSubPage.Page == nil {
			t.Fatal("Expected leaf page to be created")
		}

		// Verify the sub-page index is valid (could be any available ID 0-255)
		if leafSubPage.SubPageIdx > 255 {
			t.Errorf("Expected sub-page index <= 255, got %d", leafSubPage.SubPageIdx)
		}

		// Verify the sub-page info exists
		if len(leafSubPage.Page.SubPages) <= int(leafSubPage.SubPageIdx) {
			t.Fatalf("SubPages array too small for index %d", leafSubPage.SubPageIdx)
		}
		subPageInfo := leafSubPage.Page.SubPages[leafSubPage.SubPageIdx]
		if subPageInfo == nil {
			t.Fatal("Expected sub-page info to exist")
		}

		// Verify the entry was added
		entryCount := 0
		var foundSuffixLen int
		var foundDataOffset int64
		var foundSuffix []byte

		db.iterateLeafSubPageEntries(leafSubPage.Page, leafSubPage.SubPageIdx, func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
			entryCount++
			foundSuffixLen = suffixLen
			foundDataOffset = dataOffset
			foundSuffix = make([]byte, suffixLen)
			copy(foundSuffix, leafSubPage.Page.data[suffixOffset:suffixOffset+suffixLen])
			return true
		})

		if entryCount != 1 {
			t.Fatalf("Expected 1 entry, got %d", entryCount)
		}

		// Verify the entry content
		if foundSuffixLen != len(suffix) {
			t.Errorf("Expected suffix length %d, got %d", len(suffix), foundSuffixLen)
		}
		if foundDataOffset != dataOffset {
			t.Errorf("Expected data offset %d, got %d", dataOffset, foundDataOffset)
		}

		// Verify the suffix content
		if !bytes.Equal(foundSuffix, suffix) {
			t.Errorf("Expected suffix %v, got %v", suffix, foundSuffix)
		}

		// Verify the sub-page header in the page data
		subPageOffset := int(subPageInfo.Offset)
		subPageID := leafSubPage.Page.data[subPageOffset]
		if subPageID != leafSubPage.SubPageIdx {
			t.Errorf("Expected sub-page ID %d in header, got %d", leafSubPage.SubPageIdx, subPageID)
		}

		subPageSize := binary.LittleEndian.Uint16(leafSubPage.Page.data[subPageOffset+1:subPageOffset+3])
		if subPageSize != subPageInfo.Size {
			t.Errorf("Expected sub-page size %d in header, got %d", subPageInfo.Size, subPageSize)
		}
	})

	t.Run("AddLongSuffixEntry", func(t *testing.T) {
		// Test 2: Add a very long suffix to test boundary conditions
		longSuffix := make([]byte, 1000)
		for i := range longSuffix {
			longSuffix[i] = byte(i % 256)
		}
		dataOffset2 := int64(2000)

		leafSubPage2, err := db.addEntryToNewLeafSubPage(longSuffix, dataOffset2)
		if err != nil {
			t.Fatalf("Failed to add long suffix entry: %v", err)
		}

		// Verify the long suffix entry
		entryCount := countSubPageEntries(db, leafSubPage2.Page, leafSubPage2.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry in second sub-page, got %d", entryCount)
		}

		dataOffset, actualLongSuffix, found := getSubPageEntry(db, leafSubPage2.Page, leafSubPage2.SubPageIdx, 0)
		if !found {
			t.Fatal("Expected to find entry at index 0")
		}
		if len(actualLongSuffix) != len(longSuffix) {
			t.Errorf("Expected suffix length %d, got %d", len(longSuffix), len(actualLongSuffix))
		}
		if dataOffset != dataOffset2 {
			t.Errorf("Expected data offset %d, got %d", dataOffset2, dataOffset)
		}

		// Verify the long suffix can be read correctly
		if !bytes.Equal(actualLongSuffix, longSuffix) {
			t.Error("Long suffix content mismatch")
		}
	})

	t.Run("AddEmptySuffixEntry", func(t *testing.T) {
		// Test 3: Add an empty suffix
		emptySuffix := []byte{}
		dataOffset3 := int64(3000)

		leafSubPage3, err := db.addEntryToNewLeafSubPage(emptySuffix, dataOffset3)
		if err != nil {
			t.Fatalf("Failed to add empty suffix entry: %v", err)
		}

		// Verify the empty suffix entry
		entryCount := countSubPageEntries(db, leafSubPage3.Page, leafSubPage3.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry with empty suffix, got %d", entryCount)
		}

		dataOffset, suffix, found := getSubPageEntry(db, leafSubPage3.Page, leafSubPage3.SubPageIdx, 0)
		if !found {
			t.Fatal("Expected to find entry at index 0")
		}
		if len(suffix) != 0 {
			t.Errorf("Expected suffix length 0, got %d", len(suffix))
		}
		if dataOffset != dataOffset3 {
			t.Errorf("Expected data offset %d, got %d", dataOffset3, dataOffset)
		}
	})

	t.Run("VerifyLeafPageStructure", func(t *testing.T) {
		// Test 4: Verify the leaf page structure is correct
		suffix := []byte("structure_test")
		dataOffset := int64(4000)

		leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, dataOffset)
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}

		leafPage := leafSubPage.Page

		// Verify page type
		if leafPage.pageType != ContentTypeLeaf {
			t.Errorf("Expected page type %c, got %c", ContentTypeLeaf, leafPage.pageType)
		}

		// Verify page is marked as dirty
		if !leafPage.dirty {
			t.Error("Expected page to be marked as dirty")
		}

		// Verify content size is reasonable
		if leafPage.ContentSize <= LeafHeaderSize {
			t.Errorf("Expected content size > %d, got %d", LeafHeaderSize, leafPage.ContentSize)
		}

		if leafPage.ContentSize > PageSize {
			t.Errorf("Content size %d exceeds page size %d", leafPage.ContentSize, PageSize)
		}

		// Verify sub-page count - should have at least 1 sub-page
		subPageCount := 0
		for _, subPage := range leafPage.SubPages {
			if subPage != nil {
				subPageCount++
			}
		}
		if subPageCount < 1 {
			t.Errorf("Expected at least 1 sub-page, got %d", subPageCount)
		}

		// Verify SubPages array is properly sized (should be 256 entries)
		if len(leafPage.SubPages) != 256 {
			t.Errorf("Expected SubPages array size 256, got %d", len(leafPage.SubPages))
		}
	})

	t.Run("MultipleSubPagesInSamePage", func(t *testing.T) {
		// Test 5: Add multiple sub-pages to the same leaf page
		// Note: addEntryToNewLeafSubPage will automatically allocate leaf pages as needed

		// Add multiple small entries that should fit in the same page
		suffixes := [][]byte{
			[]byte("a"),
			[]byte("b"),
			[]byte("c"),
		}
		dataOffsets := []int64{5000, 6000, 7000}

		var leafSubPages []*LeafSubPage

		for i, suffix := range suffixes {
			leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, dataOffsets[i])
			if err != nil {
				t.Fatalf("Failed to add entry %d: %v", i, err)
			}
			leafSubPages = append(leafSubPages, leafSubPage)
		}

		// Verify all sub-pages were created
		if len(leafSubPages) != 3 {
			t.Fatalf("Expected 3 sub-pages, got %d", len(leafSubPages))
		}

		// Verify they have different sub-page IDs
		usedIDs := make(map[uint8]bool)
		for i, subPage := range leafSubPages {
			if usedIDs[subPage.SubPageIdx] {
				t.Errorf("Sub-page ID %d used multiple times", subPage.SubPageIdx)
			}
			usedIDs[subPage.SubPageIdx] = true

			// Verify the entry content
			entryCount := countSubPageEntries(db, subPage.Page, subPage.SubPageIdx)
			if entryCount != 1 {
				t.Errorf("Sub-page %d: expected 1 entry, got %d", i, entryCount)
			}

			dataOffset, suffix, found := getSubPageEntry(db, subPage.Page, subPage.SubPageIdx, 0)
			if !found {
				t.Errorf("Sub-page %d: expected to find entry at index 0", i)
				continue
			}
			if len(suffix) != len(suffixes[i]) {
				t.Errorf("Sub-page %d: expected suffix length %d, got %d", i, len(suffixes[i]), len(suffix))
			}
			if dataOffset != dataOffsets[i] {
				t.Errorf("Sub-page %d: expected data offset %d, got %d", i, dataOffsets[i], dataOffset)
			}
		}

		// Count total sub-pages across all leaf pages
		// Since addEntryToNewLeafSubPage may create multiple pages, we need to count across all pages
		totalSubPageCount := 0
		pageSet := make(map[uint32]bool)

		for _, subPage := range leafSubPages {
			pageSet[subPage.Page.pageNumber] = true
		}

		for pageNum := range pageSet {
			for _, subPage := range leafSubPages {
				if subPage.Page.pageNumber == pageNum {
					for _, sp := range subPage.Page.SubPages {
						if sp != nil {
							totalSubPageCount++
						}
					}
					break
				}
			}
		}

		if totalSubPageCount < 3 {
			t.Errorf("Expected at least 3 sub-pages across all pages, got %d", totalSubPageCount)
		}
	})

	t.Run("SubPageIDAllocation", func(t *testing.T) {
		// Test 6: Verify sub-page ID allocation works correctly
		// Create many sub-pages to test ID allocation
		var leafSubPages []*LeafSubPage

		for i := 0; i < 10; i++ {
			suffix := []byte(fmt.Sprintf("test_%d", i))
			dataOffset := int64(8000 + i*100)

			leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, dataOffset)
			if err != nil {
				t.Fatalf("Failed to add entry %d: %v", i, err)
			}
			leafSubPages = append(leafSubPages, leafSubPage)
		}

		// Verify all sub-page IDs are unique
		usedIDs := make(map[uint8]bool)
		for i, subPage := range leafSubPages {
			if usedIDs[subPage.SubPageIdx] {
				t.Errorf("Sub-page ID %d used multiple times", subPage.SubPageIdx)
			}
			usedIDs[subPage.SubPageIdx] = true

			// Verify the ID is within valid range
			if subPage.SubPageIdx > 255 {
				t.Errorf("Sub-page %d: ID %d exceeds maximum 255", i, subPage.SubPageIdx)
			}
		}
	})
}

// TestSetOnLeafSubPage tests the setOnLeafSubPage function for insert, update, and delete operations
// Note: setOnLeafSubPage handles keys that have remaining suffix after processing key bytes.
// Empty suffixes (when all key bytes are consumed) are handled by setOnEmptySuffix on radix pages.
func TestSetOnLeafSubPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_setonleafsubpage.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction to create pages in current transaction sequence
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("InsertNewEntry", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Create a leaf sub-page with initial entry
		suffix := []byte("key")
		dataOffset := int64(1000)
		leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, dataOffset)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		// Test data - simulate a key "test_key" where keyPos=4, so suffix is "key"
		key := []byte("test_key")
		keyPos := 4
		value := []byte("test_value")

		// Mock appendData by writing test data to main file first
		dataOffset, err = db.appendData(key, value)
		if err != nil {
			t.Fatalf("Failed to append test data: %v", err)
		}

		// Now test setOnLeafSubPage
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, key, keyPos, value, dataOffset)
		if err != nil {
			t.Fatalf("Failed to set on leaf sub-page: %v", err)
		}

		// Verify the entry was added/updated
		subPageInfo := leafSubPage.Page.SubPages[leafSubPage.SubPageIdx]
		if subPageInfo == nil {
			t.Fatal("Expected sub-page info to exist")
		}

		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Errorf("Expected 1 entry, got %d", entryCount)
		}

		// Verify the entry details
		expectedSuffix := key[keyPos+1:] // "key"
		_, actualSuffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatal("Expected to find entry at index 0")
		}
		if !bytes.Equal(actualSuffix, expectedSuffix) {
			t.Errorf("Expected suffix %v, got %v", expectedSuffix, actualSuffix)
		}

		// Verify data can be read back
		content, err := db.readContent(dataOffset)
		if err != nil {
			t.Fatalf("Failed to read content: %v", err)
		}
		if !bytes.Equal(content.key, key) {
			t.Errorf("Expected key %v, got %v", key, content.key)
		}
		if !bytes.Equal(content.value, value) {
			t.Errorf("Expected value %v, got %v", value, content.value)
		}
	})

	t.Run("UpdateExistingEntry", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Test data
		key := []byte("test_key")
		keyPos := 4
		originalValue := []byte("original_value")
		updatedValue := []byte("updated_value")

		// Create a leaf sub-page with initial entry using the original value
		suffix := key[keyPos+1:]
		originalDataOffset, err := db.appendData(key, originalValue)
		if err != nil {
			t.Fatalf("Failed to append original data: %v", err)
		}

		leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, originalDataOffset)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		// Verify original entry exists
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry after insert, got %d", entryCount)
		}

		originalDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		originalContent, err := db.readContent(originalDataOffset)
		if err != nil {
			t.Fatalf("Failed to read original content: %v", err)
		}
		if !bytes.Equal(originalContent.value, originalValue) {
			t.Errorf("Expected original value %v, got %v", originalValue, originalContent.value)
		}

		// Now update the entry with new value
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, key, keyPos, updatedValue, 0)
		if err != nil {
			t.Fatalf("Failed to update entry: %v", err)
		}

		// Verify still only one entry
		entryCount = countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Errorf("Expected 1 entry after update, got %d", entryCount)
		}

		// Verify the entry was updated
		updatedDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		updatedContent, err := db.readContent(updatedDataOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(updatedContent.value, updatedValue) {
			t.Errorf("Expected updated value %v, got %v", updatedValue, updatedContent.value)
		}

		// Verify the data offset changed (new data was appended)
		if originalDataOffset == updatedDataOffset {
			t.Error("Expected data offset to change after update")
		}
	})

	t.Run("UpdateWithSameValue", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Test data
		key := []byte("test_key")
		keyPos := 4
		value := []byte("same_value")

		// Create a leaf sub-page with initial entry
		suffix := key[keyPos+1:]
		originalDataOffset, err := db.appendData(key, value)
		if err != nil {
			t.Fatalf("Failed to append original data: %v", err)
		}

		leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, originalDataOffset)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		// Get the original entry
		originalDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}

		// Record the main file size before the "update"
		originalMainFileSize := db.mainFileSize

		// Now "update" with the same value
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, key, keyPos, value, 0)
		if err != nil {
			t.Fatalf("Failed to update with same value: %v", err)
		}

		// Verify still only one entry
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Errorf("Expected 1 entry, got %d", entryCount)
		}

		// Verify the data offset didn't change (no new data was written)
		updatedDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		if originalDataOffset != updatedDataOffset {
			t.Error("Expected data offset to remain the same when updating with same value")
		}

		// Verify no new data was appended to main file
		if db.mainFileSize != originalMainFileSize {
			t.Error("Expected main file size to remain the same when updating with same value")
		}
	})

	t.Run("UpdateFirstEntry", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Add multiple entries to test updating the first one
		// All entries must share the same prefix up to keyPos for them to be on the same leaf sub-page
		// For keyPos=7, all keys share prefix "common_" and have different suffixes
		testEntries := []struct {
			key    []byte
			keyPos int
			value  []byte
		}{
			{[]byte("common_alpha"), 7, []byte("first_value")},
			{[]byte("common_beta"), 7, []byte("second_value")},
			{[]byte("common_gamma"), 7, []byte("third_value")},
		}

		var leafSubPage *LeafSubPage

		// Insert all entries
		for i, testEntry := range testEntries {
			suffix := testEntry.key[testEntry.keyPos+1:]
			dataOffset, err := db.appendData(testEntry.key, testEntry.value)
			if err != nil {
				t.Fatalf("Failed to append data for entry %d: %v", i, err)
			}

			if i == 0 {
				// Create the first leaf sub-page
				leafSubPage, err = db.addEntryToNewLeafSubPage(suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to create leaf sub-page: %v", err)
				}
			} else {
				// Add to existing leaf sub-page
				err = db.addEntryToLeafSubPage(parentSubPage, testEntry.key[testEntry.keyPos], leafSubPage, suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to add entry %d to leaf sub-page: %v", i, err)
				}
			}
		}

		// Verify all entries were added
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 3 {
			t.Fatalf("Expected 3 entries, got %d", entryCount)
		}

		// Update the first entry
		newValue := []byte("updated_first_value")
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, testEntries[0].key, testEntries[0].keyPos, newValue, 0)
		if err != nil {
			t.Fatalf("Failed to update first entry: %v", err)
		}

		// Verify still 3 entries
		entryCount = countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 3 {
			t.Errorf("Expected 3 entries after update, got %d", entryCount)
		}

		// Verify the first entry was updated
		firstDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		content, err := db.readContent(firstDataOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(content.value, newValue) {
			t.Errorf("Expected updated value %v, got %v", newValue, content.value)
		}

		// Verify other entries remain unchanged
		for i := 1; i < 3; i++ {
			dataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, i)
			if !found {
				t.Fatalf("Expected to find entry at index %d", i)
			}
			content, err := db.readContent(dataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for entry %d: %v", i, err)
			}
			if !bytes.Equal(content.value, testEntries[i].value) {
				t.Errorf("Entry %d value changed unexpectedly: expected %v, got %v", i, testEntries[i].value, content.value)
			}
		}
	})

	t.Run("UpdateMiddleEntry", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Add multiple entries to test updating the middle one
		// All entries share the same prefix "entry" up to keyPos=5
		testEntries := []struct {
			key    []byte
			keyPos int
			value  []byte
		}{
			{[]byte("entry_a"), 5, []byte("value_a")},
			{[]byte("entry_b"), 5, []byte("value_b")},
			{[]byte("entry_c"), 5, []byte("value_c")},
		}

		var leafSubPage *LeafSubPage

		// Insert all entries
		for i, testEntry := range testEntries {
			suffix := testEntry.key[testEntry.keyPos+1:]
			dataOffset, err := db.appendData(testEntry.key, testEntry.value)
			if err != nil {
				t.Fatalf("Failed to append data for entry %d: %v", i, err)
			}

			if i == 0 {
				// Create the first leaf sub-page
				leafSubPage, err = db.addEntryToNewLeafSubPage(suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to create leaf sub-page: %v", err)
				}
			} else {
				// Add to existing leaf sub-page
				err = db.addEntryToLeafSubPage(parentSubPage, testEntry.key[testEntry.keyPos], leafSubPage, suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to add entry %d to leaf sub-page: %v", i, err)
				}
			}
		}

		// Update the middle entry (index 1)
		newValue := []byte("updated_middle_value")
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, testEntries[1].key, testEntries[1].keyPos, newValue, 0)
		if err != nil {
			t.Fatalf("Failed to update middle entry: %v", err)
		}

		// Verify still 3 entries
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 3 {
			t.Errorf("Expected 3 entries after update, got %d", entryCount)
		}

		// Verify the middle entry was updated
		middleDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 1)
		if !found {
			t.Fatalf("Expected to find entry at index 1")
		}
		content, err := db.readContent(middleDataOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(content.value, newValue) {
			t.Errorf("Expected updated value %v, got %v", newValue, content.value)
		}

		// Verify other entries remain unchanged
		for _, idx := range []int{0, 2} {
			dataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, idx)
			if !found {
				t.Fatalf("Expected to find entry at index %d", idx)
			}
			content, err := db.readContent(dataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for entry %d: %v", idx, err)
			}
			expectedValue := testEntries[idx].value
			if !bytes.Equal(content.value, expectedValue) {
				t.Errorf("Entry %d value changed unexpectedly: expected %v, got %v", idx, expectedValue, content.value)
			}
		}
	})

	t.Run("UpdateLastEntry", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Add multiple entries to test updating the last one
		// All entries share the same prefix "item" up to keyPos=4
		testEntries := []struct {
			key    []byte
			keyPos int
			value  []byte
		}{
			{[]byte("item_1"), 4, []byte("data_1")},
			{[]byte("item_2"), 4, []byte("data_2")},
			{[]byte("item_3"), 4, []byte("data_3")},
		}

		var leafSubPage *LeafSubPage

		// Insert all entries
		for i, testEntry := range testEntries {
			suffix := testEntry.key[testEntry.keyPos+1:]
			dataOffset, err := db.appendData(testEntry.key, testEntry.value)
			if err != nil {
				t.Fatalf("Failed to append data for entry %d: %v", i, err)
			}

			if i == 0 {
				// Create the first leaf sub-page
				leafSubPage, err = db.addEntryToNewLeafSubPage(suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to create leaf sub-page: %v", err)
				}
			} else {
				// Add to existing leaf sub-page
				err = db.addEntryToLeafSubPage(parentSubPage, testEntry.key[testEntry.keyPos], leafSubPage, suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to add entry %d to leaf sub-page: %v", i, err)
				}
			}
		}

		// Update the last entry (index 2)
		newValue := []byte("updated_last_value")
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, testEntries[2].key, testEntries[2].keyPos, newValue, 0)
		if err != nil {
			t.Fatalf("Failed to update last entry: %v", err)
		}

		// Verify still 3 entries
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 3 {
			t.Errorf("Expected 3 entries after update, got %d", entryCount)
		}

		// Verify the last entry was updated
		lastEntryDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 2)
		if !found {
			t.Fatalf("Expected to find entry at index 2")
		}
		content, err := db.readContent(lastEntryDataOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(content.value, newValue) {
			t.Errorf("Expected updated value %v, got %v", newValue, content.value)
		}

		// Verify other entries remain unchanged
		for i := 0; i < 2; i++ {
			dataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, i)
			if !found {
				t.Fatalf("Expected to find entry at index %d", i)
			}
			content, err := db.readContent(dataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for entry %d: %v", i, err)
			}
			if !bytes.Equal(content.value, testEntries[i].value) {
				t.Errorf("Entry %d value changed unexpectedly: expected %v, got %v", i, testEntries[i].value, content.value)
			}
		}
	})

	t.Run("DeleteFirstEntry", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Add multiple entries to test deleting the first one
		// All entries share the same prefix "prefix" up to keyPos=6
		testEntries := []struct {
			key    []byte
			keyPos int
			value  []byte
		}{
			{[]byte("prefix_delete"), 6, []byte("first_value")},
			{[]byte("prefix_keep_1"), 6, []byte("second_value")},
			{[]byte("prefix_keep_2"), 6, []byte("third_value")},
		}

		var leafSubPage *LeafSubPage

		// Insert all entries
		for i, testEntry := range testEntries {
			suffix := testEntry.key[testEntry.keyPos+1:]
			dataOffset, err := db.appendData(testEntry.key, testEntry.value)
			if err != nil {
				t.Fatalf("Failed to append data for entry %d: %v", i, err)
			}

			if i == 0 {
				// Create the first leaf sub-page
				leafSubPage, err = db.addEntryToNewLeafSubPage(suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to create leaf sub-page: %v", err)
				}
			} else {
				// Add to existing leaf sub-page
				err = db.addEntryToLeafSubPage(parentSubPage, testEntry.key[testEntry.keyPos], leafSubPage, suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to add entry %d to leaf sub-page: %v", i, err)
				}
			}
		}

		// Verify all entries were added
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 3 {
			t.Fatalf("Expected 3 entries, got %d", entryCount)
		}

		// Delete the first entry by setting empty value
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, testEntries[0].key, testEntries[0].keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete first entry: %v", err)
		}

		// Verify entry was removed
		entryCount = countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Errorf("Expected 2 entries after deletion, got %d", entryCount)
		}

		// Verify remaining entries are correct and shifted
		expectedRemaining := testEntries[1:]
		for i, expectedEntry := range expectedRemaining {
			dataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, i)
			if !found {
				t.Fatalf("Expected to find entry at index %d", i)
			}
			content, err := db.readContent(dataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for remaining entry %d: %v", i, err)
			}
			if !bytes.Equal(content.key, expectedEntry.key) {
				t.Errorf("Remaining entry %d key mismatch: expected %v, got %v", i, expectedEntry.key, content.key)
			}
			if !bytes.Equal(content.value, expectedEntry.value) {
				t.Errorf("Remaining entry %d value mismatch: expected %v, got %v", i, expectedEntry.value, content.value)
			}
		}
	})

	t.Run("DeleteMiddleEntry", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Add multiple entries to test deleting the middle one
		// All entries share the same prefix "shared" up to keyPos=6
		testEntries := []struct {
			key    []byte
			keyPos int
			value  []byte
		}{
			{[]byte("shared_first"), 6, []byte("value_1")},
			{[]byte("shared_middle"), 6, []byte("value_2")},
			{[]byte("shared_last"), 6, []byte("value_3")},
		}

		var leafSubPage *LeafSubPage

		// Insert all entries
		for i, testEntry := range testEntries {
			suffix := testEntry.key[testEntry.keyPos+1:]
			dataOffset, err := db.appendData(testEntry.key, testEntry.value)
			if err != nil {
				t.Fatalf("Failed to append data for entry %d: %v", i, err)
			}

			if i == 0 {
				// Create the first leaf sub-page
				leafSubPage, err = db.addEntryToNewLeafSubPage(suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to create leaf sub-page: %v", err)
				}
			} else {
				// Add to existing leaf sub-page
				err = db.addEntryToLeafSubPage(parentSubPage, testEntry.key[testEntry.keyPos], leafSubPage, suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to add entry %d to leaf sub-page: %v", i, err)
				}
			}
		}

		// Delete the middle entry (index 1)
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, testEntries[1].key, testEntries[1].keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete middle entry: %v", err)
		}

		// Verify entry was removed
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Errorf("Expected 2 entries after deletion, got %d", entryCount)
		}

		// Verify remaining entries are correct
		expectedRemaining := []struct {
			key    []byte
			keyPos int
			value  []byte
		}{
			testEntries[0], // first entry
			testEntries[2], // last entry (shifted to index 1)
		}

		for i, expectedEntry := range expectedRemaining {
			dataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, i)
			if !found {
				t.Fatalf("Expected to find entry at index %d", i)
			}
			content, err := db.readContent(dataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for remaining entry %d: %v", i, err)
			}
			if !bytes.Equal(content.key, expectedEntry.key) {
				t.Errorf("Remaining entry %d key mismatch: expected %v, got %v", i, expectedEntry.key, content.key)
			}
			if !bytes.Equal(content.value, expectedEntry.value) {
				t.Errorf("Remaining entry %d value mismatch: expected %v, got %v", i, expectedEntry.value, content.value)
			}
		}
	})

	t.Run("DeleteLastEntry", func(t *testing.T) {
		// Create a parent radix sub-page to simulate the tree structure
		parentRadixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix page: %v", err)
		}
		parentSubPage := &RadixSubPage{
			Page:       parentRadixPage,
			SubPageIdx: 0,
		}

		// Add multiple entries to test deleting the last one
		// All entries share the same prefix "data" up to keyPos=4
		testEntries := []struct {
			key    []byte
			keyPos int
			value  []byte
		}{
			{[]byte("data_one"), 4, []byte("data_one")},
			{[]byte("data_two"), 4, []byte("data_two")},
			{[]byte("data_remove"), 4, []byte("data_remove")},
		}

		var leafSubPage *LeafSubPage

		// Insert all entries
		for i, testEntry := range testEntries {
			suffix := testEntry.key[testEntry.keyPos+1:]
			dataOffset, err := db.appendData(testEntry.key, testEntry.value)
			if err != nil {
				t.Fatalf("Failed to append data for entry %d: %v", i, err)
			}

			if i == 0 {
				// Create the first leaf sub-page
				leafSubPage, err = db.addEntryToNewLeafSubPage(suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to create leaf sub-page: %v", err)
				}
			} else {
				// Add to existing leaf sub-page
				err = db.addEntryToLeafSubPage(parentSubPage, testEntry.key[testEntry.keyPos], leafSubPage, suffix, dataOffset)
				if err != nil {
					t.Fatalf("Failed to add entry %d to leaf sub-page: %v", i, err)
				}
			}
		}

		// Delete the last entry (index 2)
		err = db.setOnLeafSubPage(parentSubPage, leafSubPage, testEntries[2].key, testEntries[2].keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete last entry: %v", err)
		}

		// Verify entry was removed
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Errorf("Expected 2 entries after deletion, got %d", entryCount)
		}

		// Verify remaining entries are correct (first two entries)
		for i := 0; i < 2; i++ {
			dataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, i)
			if !found {
				t.Fatalf("Expected to find entry at index %d", i)
			}
			content, err := db.readContent(dataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for remaining entry %d: %v", i, err)
			}
			if !bytes.Equal(content.key, testEntries[i].key) {
				t.Errorf("Remaining entry %d key mismatch: expected %v, got %v", i, testEntries[i].key, content.key)
			}
			if !bytes.Equal(content.value, testEntries[i].value) {
				t.Errorf("Remaining entry %d value mismatch: expected %v, got %v", i, testEntries[i].value, content.value)
			}
		}
	})

	t.Run("InsertNewEntryInSubPage", func(t *testing.T) {
		// Create a leaf sub-page with an existing entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("existing"), 1000)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Test data - simulate a key "test_key_new" where keyPos=8, so suffix is "new"
		key := []byte("test_key_new")
		keyPos := 8
		value := []byte("new_value")

		// Test setOnLeafSubPage for a new insert into existing sub-page
		err = db.setOnLeafSubPage(parentRadixSubPage, leafSubPage, key, keyPos, value, 0)
		if err != nil {
			t.Fatalf("Failed to set on leaf sub-page: %v", err)
		}

		// Verify the entry was added (should have 2 entries now)
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Errorf("Expected 2 entries, got %d", entryCount)
		}

		// Find the new entry by suffix using findEntryInLeafSubPage
		expectedSuffix := key[keyPos+1:] // "new"
		_, _, dataOffset, err := db.findEntryInLeafSubPage(leafSubPage.Page, leafSubPage.SubPageIdx, expectedSuffix)
		if err != nil {
			t.Fatalf("Failed to search for entry: %v", err)
		}

		// Check if entry was found
		if dataOffset == 0 {
			t.Fatal("Expected to find new entry with suffix 'new'")
		}

		// Use the found dataOffset
		newEntryDataOffset := dataOffset

		// Verify data can be read back
		content, err := db.readContent(newEntryDataOffset)
		if err != nil {
			t.Fatalf("Failed to read content: %v", err)
		}
		if !bytes.Equal(content.key, key) {
			t.Errorf("Expected key %v, got %v", key, content.key)
		}
		if !bytes.Equal(content.value, value) {
			t.Errorf("Expected value %v, got %v", value, content.value)
		}
	})

	t.Run("DeleteExistingEntryFromSubPage", func(t *testing.T) {
		// Create proper data offsets by writing to the database
		keepKey := []byte("prefix_keep_me")
		keepValue := []byte("keep_value")
		keepDataOffset, err := db.appendData(keepKey, keepValue)
		if err != nil {
			t.Fatalf("Failed to append keep data: %v", err)
		}

		deleteKey := []byte("prefix_delete_me")
		deleteValue := []byte("delete_value")
		deleteDataOffset, err := db.appendData(deleteKey, deleteValue)
		if err != nil {
			t.Fatalf("Failed to append delete data: %v", err)
		}

		// Create a leaf sub-page with multiple entries
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("keep_me"), keepDataOffset)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Add another entry to delete
		err = db.addEntryToLeafSubPage(parentRadixSubPage, 'd', leafSubPage, []byte("delete_me"), deleteDataOffset)
		if err != nil {
			t.Fatalf("Failed to add second entry: %v", err)
		}

		// Verify we have 2 entries
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Fatalf("Expected 2 entries before delete, got %d", entryCount)
		}

		// Test data for deletion
		key := []byte("prefix_delete_me")
		keyPos := 6 // So suffix is "delete_me"

		// Delete the entry (empty value means delete)
		err = db.setOnLeafSubPage(parentRadixSubPage, leafSubPage, key, keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete entry: %v", err)
		}

		// Verify we now have 1 entry
		entryCount = countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Errorf("Expected 1 entry after delete, got %d", entryCount)
		}

		// Verify the remaining entry is the correct one
		_, remainingSuffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		if !bytes.Equal(remainingSuffix, []byte("keep_me")) {
			t.Errorf("Expected remaining suffix 'keep_me', got %v", remainingSuffix)
		}
	})

	t.Run("DeleteNonExistentEntryFromSubPage", func(t *testing.T) {
		// Create a leaf sub-page with one entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("existing"), 6000)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Test data for non-existent entry
		key := []byte("prefix_nonexistent")
		keyPos := 6 // So suffix is "nonexistent"

		// Try to delete a non-existent entry
		err = db.setOnLeafSubPage(parentRadixSubPage, leafSubPage, key, keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Delete of non-existent entry should not fail: %v", err)
		}

		// Verify the original entry is still there
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Errorf("Expected 1 entry to remain, got %d", entryCount)
		}
	})

	t.Run("ReindexingMode", func(t *testing.T) {
		// Create a leaf sub-page
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("reindex"), 7000)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Test reindexing mode where dataOffset is provided (non-zero)
		key := []byte("test_reindex")
		keyPos := 4 // So suffix is "reindex"
		value := []byte("reindex_value")
		dataOffset := int64(12345) // Non-zero means reindexing

		// In reindexing mode, we don't append new data
		err = db.setOnLeafSubPage(parentRadixSubPage, leafSubPage, key, keyPos, value, dataOffset)
		if err != nil {
			t.Fatalf("Failed to set in reindexing mode: %v", err)
		}

		// Verify the entry was updated with the provided offset
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Errorf("Expected 1 entry, got %d", entryCount)
		}

		entryDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		if entryDataOffset != dataOffset {
			t.Errorf("Expected data offset %d, got %d", dataOffset, entryDataOffset)
		}
	})
}

// ================================================================================================
// Page Cache Tests
// ================================================================================================

func TestPageCacheAfterLeafOperations(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_cache.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.commitTransaction()

	// Get initial cache stats
	initialStats := db.GetCacheStats()
	t.Logf("Initial cache stats: %+v", initialStats)

	// Create a leaf page
	leafPage, err := db.allocateLeafPage()
	if err != nil {
		t.Fatalf("Failed to allocate leaf page: %v", err)
	}

	pageNumber := leafPage.pageNumber
	// Make sure the page number is greater than 1
	if pageNumber <= 1 {
		t.Fatalf("Expected page number greater than 1, got %d", pageNumber)
	}

	// Add the page to the free list so addEntryToNewLeafSubPage can use it
	db.addToFreeLeafPagesList(leafPage, 0)

	t.Logf("Test allocated leaf page %d and added to free list", pageNumber)

	// Verify the page is in cache
	cachedPage, exists := db.getFromCache(pageNumber)
	if !exists {
		t.Error("Newly allocated leaf page should be in cache")
	}
	if cachedPage.pageType != ContentTypeLeaf {
		t.Errorf("Expected leaf page type, got %c", cachedPage.pageType)
	}
	if cachedPage != leafPage {
		t.Error("Cached page should be the same as the leaf page")
	}

	// Add some entries to the page using the new multi-sub-page format
	// Note: Each call to addEntryToNewLeafSubPage may create new pages
	var leafSubPages []*LeafSubPage
	for i := 0; i < 5; i++ {
		// Create proper keys and suffixes - suffix should be part of the key
		key := []byte(fmt.Sprintf("test_key_%d", i))
		value := []byte(fmt.Sprintf("test_value_%d", i))

		// Create proper data offsets by writing to the database
		dataOffset, err := db.appendData(key, value)
		if err != nil {
			t.Fatalf("Failed to append data for entry %d: %v", i, err)
		}

		// Use the key suffix (skip first byte to simulate radix path)
		suffix := key[1:] // Skip first byte as if we're at radix depth 1

		// Debug: Check what's in the free list before allocation
		t.Logf("Before adding entry %d, checking free list state", i)

		leafSubPage, err := db.addEntryToNewLeafSubPage(suffix, dataOffset)
		if err != nil {
			t.Fatalf("Failed to add entry %d: %v", i, err)
		}
		leafSubPages = append(leafSubPages, leafSubPage)
		t.Logf("Added entry %d (key=%s, suffix=%s) to page %d", i, key, suffix, leafSubPage.Page.pageNumber)
	}

	// Verify the pages with sub-pages are marked as dirty
	dirtyPageFound := false
	for _, subPage := range leafSubPages {
		if subPage.Page.dirty {
			dirtyPageFound = true
			t.Logf("Page %d is correctly marked as dirty", subPage.Page.pageNumber)
			break
		}
	}
	if !dirtyPageFound {
		t.Error("At least one leaf page should be marked as dirty after modifications")
	}

	// Get updated cache stats
	updatedStats := db.GetCacheStats()
	t.Logf("Updated cache stats: %+v", updatedStats)

	// Verify dirty page count increased
	if updatedStats["dirty_pages"].(int) <= initialStats["dirty_pages"].(int) {
		t.Error("Dirty page count should have increased")
	}

	// Verify leaf page count increased
	if updatedStats["leaf_pages"].(int) <= initialStats["leaf_pages"].(int) {
		t.Error("Leaf page count should have increased")
	}

	// Verify sub-pages are preserved in the new format
	// Count sub-pages across all pages created
	totalSubPageCount := 0
	pageSet := make(map[uint32]*LeafPage)

	for _, subPage := range leafSubPages {
		pageSet[subPage.Page.pageNumber] = subPage.Page
	}

	// Test that pages remain accessible through cache
	// Note: The original page may not have the sub-pages since addEntryToNewLeafSubPage may create new pages
	for pageNum := range pageSet {
		retrievedPage, err := db.getLeafPage(pageNum)
		if err != nil {
			t.Fatalf("Failed to retrieve leaf page %d from cache: %v", pageNum, err)
		}

		// Verify it's the same page instance (should be from cache)
		if retrievedPage.pageNumber != pageNum {
			t.Errorf("Expected page number %d, got %d", pageNum, retrievedPage.pageNumber)
		}
	}

	for pageNum, page := range pageSet {
		subPageCountForPage := 0
		for i, subPage := range page.SubPages {
			if subPage != nil {
				subPageCountForPage++
				totalSubPageCount++

				// Log entries within this sub-page using iterateLeafSubPageEntries
				entryIndex := 0
				err := db.iterateLeafSubPageEntries(page, uint8(i), func(entryOffset int, entrySize int, suffixOffset int, suffixLen int, dataOffset int64) bool {
					// Extract suffix from page data
					suffix := page.data[suffixOffset:suffixOffset+suffixLen]
					t.Logf("Page %d SubPage[%d] Entry[%d]: suffix=%q, dataOffset=%d",
						pageNum, i, entryIndex, suffix, dataOffset)
					entryIndex++
					return true // Continue iteration
				})
				if err != nil {
					t.Errorf("Failed to iterate sub-page entries: %v", err)
				}
			}
		}
		t.Logf("Page %d has %d sub-pages", pageNum, subPageCountForPage)
	}

	t.Logf("Total sub-pages across all pages: %d", totalSubPageCount)
	if totalSubPageCount < 5 {
		t.Errorf("Expected at least 5 sub-pages across all pages, got %d", totalSubPageCount)
	}

	// Test page access time is updated for the original page
	originalAccessTime := cachedPage.accessTime
	t.Logf("Original access time: %d", originalAccessTime)

	_, err = db.getLeafPage(pageNumber)
	if err != nil {
		t.Fatalf("Failed to access page again: %v", err)
	}

	// Access time should have been updated
	updatedPage, _ := db.getFromCache(pageNumber)
	t.Logf("Updated access time: %d", updatedPage.accessTime)

	if updatedPage.accessTime <= originalAccessTime {
		t.Error("Access time should have been updated")
	}
}

// ================================================================================================
// Leaf Entry Management Tests
// ================================================================================================

// TestParseLeafSubPages tests the parseLeafSubPages function with the new multi-sub-page format
func TestParseLeafSubPages(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_parseleafsubpages.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction to create pages in current transaction sequence
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("ParseEmptyLeafPage", func(t *testing.T) {
		// Create a new empty leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Parse sub-pages from empty page
		err = db.parseLeafSubPages(leafPage)
		if err != nil {
			t.Fatalf("Failed to parse empty leaf page: %v", err)
		}

		// Should have 256 sub-page slots, all nil
		if len(leafPage.SubPages) != 256 {
			t.Errorf("Expected 256 sub-page slots, got %d", len(leafPage.SubPages))
		}

		// Count non-nil sub-pages
		subPageCount := 0
		for _, subPage := range leafPage.SubPages {
			if subPage != nil {
				subPageCount++
			}
		}
		if subPageCount != 0 {
			t.Errorf("Expected 0 sub-pages in empty page, got %d", subPageCount)
		}
	})

	t.Run("ParseSingleSubPage", func(t *testing.T) {
		// Create a leaf sub-page with one entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("test_suffix"), 1000)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		leafPage := leafSubPage.Page

		// Clear sub-pages and re-parse to test parsing
		leafPage.SubPages = nil
		err = db.parseLeafSubPages(leafPage)
		if err != nil {
			t.Fatalf("Failed to parse leaf sub-pages: %v", err)
		}

		// Should have 256 sub-page slots
		if len(leafPage.SubPages) != 256 {
			t.Errorf("Expected 256 sub-page slots, got %d", len(leafPage.SubPages))
		}

		// Should have one non-nil sub-page
		subPageCount := 0
		var parsedSubPage *LeafSubPageInfo
		for _, subPage := range leafPage.SubPages {
			if subPage != nil {
				subPageCount++
				parsedSubPage = subPage
			}
		}
		if subPageCount != 1 {
			t.Errorf("Expected 1 sub-page, got %d", subPageCount)
		}

		// Verify the parsed sub-page
		if parsedSubPage == nil {
			t.Fatal("Expected non-nil parsed sub-page")
		}

		entryCount := countSubPageEntries(db, leafPage, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Errorf("Expected 1 entry in sub-page, got %d", entryCount)
		}

		dataOffset, suffix, found := getSubPageEntry(db, leafPage, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatal("Expected to find entry at index 0")
		}
		if len(suffix) != 11 { // "test_suffix" length
			t.Errorf("Expected suffix length 11, got %d", len(suffix))
		}
		if dataOffset != 1000 {
			t.Errorf("Expected data offset 1000, got %d", dataOffset)
		}
	})

	t.Run("ParseCorruptedSubPage", func(t *testing.T) {
		// Create a leaf page with corrupted sub-page data
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Create corrupted sub-page data - sub-page size exceeds content size
		offset := int(LeafHeaderSize)
		leafPage.data[offset] = 0 // Sub-page ID
		binary.LittleEndian.PutUint16(leafPage.data[offset+1:], 1000) // Size larger than available space
		leafPage.ContentSize = offset + 10 // Small content size

		// Try to parse - should fail
		err = db.parseLeafSubPages(leafPage)
		if err == nil {
			t.Error("Expected error for corrupted sub-page, got nil")
		}
		if !strings.Contains(err.Error(), "exceeds content size") {
			t.Errorf("Expected 'exceeds content size' error, got: %v", err)
		}
	})

	t.Run("ParseMultipleSubPages", func(t *testing.T) {
		// Create multiple leaf sub-pages to test parsing multiple sub-pages
		leafSubPage1, err := db.addEntryToNewLeafSubPage([]byte("entry1"), 1000)
		if err != nil {
			t.Fatalf("Failed to create first leaf sub-page: %v", err)
		}

		// Add more sub-pages to the same leaf page
		_, err = db.addEntryToNewLeafSubPage([]byte("entry2"), 2000)
		if err != nil {
			t.Fatalf("Failed to create second leaf sub-page: %v", err)
		}

		// Get the leaf page (should be the same for both sub-pages if they fit)
		leafPage := leafSubPage1.Page

		// Clear sub-pages and re-parse to test parsing
		leafPage.SubPages = nil
		err = db.parseLeafSubPages(leafPage)
		if err != nil {
			t.Fatalf("Failed to parse multiple leaf sub-pages: %v", err)
		}

		// Count non-nil sub-pages
		subPageCount := 0
		for _, subPage := range leafPage.SubPages {
			if subPage != nil {
				subPageCount++
			}
		}

		// Should have at least 1 sub-page (might be 2 if they fit on same page)
		if subPageCount < 1 {
			t.Errorf("Expected at least 1 sub-page, got %d", subPageCount)
		}

		// Verify each non-nil sub-page has valid entries
		for i, subPage := range leafPage.SubPages {
			if subPage != nil {
				entryCount := countSubPageEntries(db, leafPage, uint8(i))
				if entryCount == 0 {
					t.Errorf("Sub-page %d has no entries", i)
				}
				for j := 0; j < entryCount; j++ {
					dataOffset, suffix, found := getSubPageEntry(db, leafPage, uint8(i), j)
					if !found {
						t.Errorf("Failed to get entry %d from sub-page %d", j, i)
					}
					if len(suffix) <= 0 {
						t.Errorf("Sub-page %d entry %d has invalid suffix length: %d", i, j, len(suffix))
					}
					if dataOffset <= 0 {
						t.Errorf("Sub-page %d entry %d has invalid data offset: %d", i, j, dataOffset)
					}
				}
			}
		}
	})
}

// TestParseLeafPage tests the parseLeafPage function with the new multi-sub-page format
func TestParseLeafPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_parseleafpage.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction to create pages in current transaction sequence
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("ParseValidLeafPageWithSubPages", func(t *testing.T) {
		// Create a leaf sub-page with entries using the new format
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("test_key"), 1000)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		leafPage := leafSubPage.Page

		// Write the leaf page to get proper data format
		err = db.writeLeafPage(leafPage)
		if err != nil {
			t.Fatalf("Failed to write leaf page: %v", err)
		}

		// Now test parsing the written page data
		parsedPage, err := db.parseLeafPage(leafPage.data, leafPage.pageNumber)
		if err != nil {
			t.Fatalf("Failed to parse leaf page: %v", err)
		}

		// Verify parsed fields
		if parsedPage.pageNumber != leafPage.pageNumber {
			t.Errorf("Expected page number %d, got %d", leafPage.pageNumber, parsedPage.pageNumber)
		}

		if parsedPage.pageType != ContentTypeLeaf {
			t.Errorf("Expected page type %c, got %c", ContentTypeLeaf, parsedPage.pageType)
		}

		if parsedPage.ContentSize != leafPage.ContentSize {
			t.Errorf("Expected ContentSize %d, got %d", leafPage.ContentSize, parsedPage.ContentSize)
		}

		// Verify SubPages array is properly initialized
		if len(parsedPage.SubPages) != 256 {
			t.Errorf("Expected 256 sub-page slots, got %d", len(parsedPage.SubPages))
		}

		// Count non-nil sub-pages
		subPageCount := 0
		for _, subPage := range parsedPage.SubPages {
			if subPage != nil {
				subPageCount++
			}
		}
		if subPageCount == 0 {
			t.Error("Expected at least one sub-page after parsing")
		}

		// Verify the parsed sub-page has entries
		var foundSubPage *LeafSubPageInfo
		for _, subPage := range parsedPage.SubPages {
			if subPage != nil {
				foundSubPage = subPage
				break
			}
		}

		if foundSubPage == nil {
			t.Fatal("Expected to find at least one sub-page")
		}

		entryCount := countSubPageEntries(db, parsedPage, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Errorf("Expected 1 entry in sub-page, got %d", entryCount)
		}

		dataOffset, suffix, found := getSubPageEntry(db, parsedPage, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatal("Expected to find entry at index 0")
		}
		if len(suffix) != 8 { // "test_key" length
			t.Errorf("Expected suffix length 8, got %d", len(suffix))
		}
		if dataOffset != 1000 {
			t.Errorf("Expected data offset 1000, got %d", dataOffset)
		}

		if parsedPage.dirty {
			t.Error("Expected parsed page to not be dirty")
		}

		if parsedPage.accessTime == 0 {
			t.Error("Expected access time to be set")
		}
	})

	t.Run("ParseLeafPageWrongType", func(t *testing.T) {
		// Create test data with wrong content type
		data := make([]byte, PageSize)
		data[0] = ContentTypeRadix    // Wrong type for leaf page
		binary.LittleEndian.PutUint16(data[2:4], LeafHeaderSize) // ContentSize

		// Calculate and set checksum
		binary.BigEndian.PutUint32(data[4:8], 0)
		checksum := crc32.ChecksumIEEE(data)
		binary.BigEndian.PutUint32(data[4:8], checksum)

		// Parse should fail
		_, err = db.parseLeafPage(data, 1)
		if err == nil {
			t.Error("Expected error for wrong content type, got nil")
		}
		if !strings.Contains(err.Error(), "not a leaf page") {
			t.Errorf("Expected 'not a leaf page' error, got: %v", err)
		}
	})

	t.Run("ParseLeafPageBadChecksum", func(t *testing.T) {
		// Create test data with bad checksum
		data := make([]byte, PageSize)
		data[0] = ContentTypeLeaf
		binary.LittleEndian.PutUint16(data[2:4], LeafHeaderSize) // ContentSize
		binary.BigEndian.PutUint32(data[4:8], 0xCAFEBABE) // Wrong checksum

		// Parse should fail
		_, err = db.parseLeafPage(data, 1)
		if err == nil {
			t.Error("Expected error for bad checksum, got nil")
		}
		if !strings.Contains(err.Error(), "checksum mismatch") {
			t.Errorf("Expected 'checksum mismatch' error, got: %v", err)
		}
	})

	t.Run("ParseLeafPageEmptyContent", func(t *testing.T) {
		// Create test data for empty leaf page (only header)
		data := make([]byte, PageSize)
		data[0] = ContentTypeLeaf
		binary.LittleEndian.PutUint16(data[2:4], LeafHeaderSize) // Only header content

		// Calculate and set checksum
		binary.BigEndian.PutUint32(data[4:8], 0)
		checksum := crc32.ChecksumIEEE(data)
		binary.BigEndian.PutUint32(data[4:8], checksum)

		// Parse should succeed
		leafPage, err := db.parseLeafPage(data, 1)
		if err != nil {
			t.Fatalf("Failed to parse empty leaf page: %v", err)
		}

		// Verify empty page properties
		if leafPage.ContentSize != LeafHeaderSize {
			t.Errorf("Expected ContentSize %d, got %d", LeafHeaderSize, leafPage.ContentSize)
		}

		// Count non-nil sub-pages
		subPageCount := 0
		for _, subPage := range leafPage.SubPages {
			if subPage != nil {
				subPageCount++
			}
		}
		if subPageCount != 0 {
			t.Errorf("Expected 0 sub-pages in empty page, got %d", subPageCount)
		}
	})

	t.Run("ParseLeafPageAddsToCache", func(t *testing.T) {
		// Get initial cache count
		initialCacheCount := db.totalCachePages.Load()

		// Create valid test data
		data := make([]byte, PageSize)
		data[0] = ContentTypeLeaf
		binary.LittleEndian.PutUint16(data[2:4], LeafHeaderSize)

		// Calculate and set checksum
		binary.BigEndian.PutUint32(data[4:8], 0)
		checksum := crc32.ChecksumIEEE(data)
		binary.BigEndian.PutUint32(data[4:8], checksum)

		// Parse the page
		_, err = db.parseLeafPage(data, 25)
		if err != nil {
			t.Fatalf("Failed to parse leaf page: %v", err)
		}

		// Verify page was added to cache
		newCacheCount := db.totalCachePages.Load()
		if newCacheCount <= initialCacheCount {
			t.Error("Expected cache count to increase after parsing page")
		}

		// Verify we can get the page from cache
		cachedPage, exists := db.getFromCache(25)
		if !exists {
			t.Error("Expected page to be in cache")
		}
		if cachedPage.pageType != ContentTypeLeaf {
			t.Error("Expected cached page to be leaf type")
		}
	})
}

// TestRemoveLeafEntryAt tests the removeLeafEntryAt function with edge cases
// TestRemoveEntryFromLeafSubPage tests the removeEntryFromLeafSubPage function
func TestRemoveEntryFromLeafSubPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_removeentryfromleafsubpage.db")

	// Open the database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.rollbackTransaction()

	t.Run("RemoveEntryFromSubPage", func(t *testing.T) {
		// Create a leaf sub-page with multiple entries
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("entry1"), 1000)
		if err != nil {
			t.Fatalf("Failed to add first entry: %v", err)
		}

		// Create a mock parent radix sub-page for testing
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Add more entries to the same sub-page
		err = db.addEntryToLeafSubPage(parentRadixSubPage, 'a', leafSubPage, []byte("entry2"), 2000)
		if err != nil {
			t.Fatalf("Failed to add second entry: %v", err)
		}

		err = db.addEntryToLeafSubPage(parentRadixSubPage, 'a', leafSubPage, []byte("entry3"), 3000)
		if err != nil {
			t.Fatalf("Failed to add third entry: %v", err)
		}

		// Verify we have 3 entries
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 3 {
			t.Fatalf("Expected 3 entries, got %d", entryCount)
		}

		// Remove the middle entry (index 1)
		// First, get the suffix of the entry we want to remove
		_, middleEntrySuffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 1)
		if !found {
			t.Fatalf("Failed to find entry at index 1")
		}

		// Now use findEntryInLeafSubPage to get the entry offset and size
		entryOffset, entrySize, _, err := db.findEntryInLeafSubPage(leafSubPage.Page, leafSubPage.SubPageIdx, middleEntrySuffix)
		if err != nil {
			t.Fatalf("Failed to find entry details: %v", err)
		}
		if entryOffset < 0 {
			t.Fatalf("Failed to find entry offset")
		}

		// Now remove the entry with the correct parameters
		err = db.removeEntryFromLeafSubPage(leafSubPage, entryOffset, entrySize)
		if err != nil {
			t.Fatalf("Failed to remove entry: %v", err)
		}

		// Verify we now have 2 entries
		entryCount = countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Fatalf("Expected 2 entries after removal, got %d", entryCount)
		}

		// Verify the remaining entries are correct
		entry1DataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		if entry1DataOffset != 1000 {
			t.Errorf("Expected first entry data offset 1000, got %d", entry1DataOffset)
		}

		entry3DataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 1)
		if !found {
			t.Fatalf("Expected to find entry at index 1")
		}
		if entry3DataOffset != 3000 {
			t.Errorf("Expected third entry data offset 3000, got %d", entry3DataOffset)
		}
	})

	t.Run("RemoveLastEntry", func(t *testing.T) {
		// Create a leaf sub-page with one entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("single_entry"), 5000)
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}

		// Verify we have 1 entry
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry, got %d", entryCount)
		}

		// Remove the only entry
		// First, get the suffix of the entry we want to remove
		_, singleEntrySuffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Failed to find entry at index 0")
		}

		// Now use findEntryInLeafSubPage to get the entry offset and size
		entryOffset, entrySize, _, err := db.findEntryInLeafSubPage(leafSubPage.Page, leafSubPage.SubPageIdx, singleEntrySuffix)
		if err != nil {
			t.Fatalf("Failed to find entry details: %v", err)
		}
		if entryOffset < 0 {
			t.Fatalf("Failed to find entry offset")
		}

		// Now remove the entry with the correct parameters
		err = db.removeEntryFromLeafSubPage(leafSubPage, entryOffset, entrySize)
		if err != nil {
			t.Fatalf("Failed to remove entry: %v", err)
		}

		// Verify the sub-page is still present but has zero size
		if leafSubPage.Page.SubPages[leafSubPage.SubPageIdx] == nil {
			t.Error("Sub-page should not be removed when last entry is deleted")
		} else if leafSubPage.Page.SubPages[leafSubPage.SubPageIdx].Size != 0 {
			t.Errorf("Expected sub-page size to be 0, got %d", leafSubPage.Page.SubPages[leafSubPage.SubPageIdx].Size)
		}

		// Verify there are no entries in the sub-page
		entryCount = countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 0 {
			t.Errorf("Expected 0 entries after removal, got %d", entryCount)
		}
	})

	t.Run("RemoveInvalidIndex", func(t *testing.T) {
		// Create a leaf sub-page with one entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("test_entry"), 6000)
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}

		// Try to remove with an invalid offset (beyond the content size)
		invalidOffset := leafSubPage.Page.ContentSize + 10
		err = db.removeEntryFromLeafSubPage(leafSubPage, invalidOffset, 0)

		// The function doesn't validate if the offset is valid, it just tries to copy data
		// which may not cause an error if the offset is beyond the content size
		// So we verify that the entry is still there
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry to remain, got %d", entryCount)
		}

		// Verify the entry data is still correct
		dataOffset, suffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		if dataOffset != 6000 {
			t.Errorf("Expected data offset 6000, got %d", dataOffset)
		}
		if !bytes.Equal(suffix, []byte("test_entry")) {
			t.Errorf("Expected suffix 'test_entry', got %v", suffix)
		}
	})
}

// TestAddEntryToLeafSubPage tests the addEntryToLeafSubPage function with the new multi-sub-page format
func TestAddEntryToLeafSubPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_addentrytoLeafSubPage.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.rollbackTransaction()

	t.Run("AddEntryToExistingSubPage", func(t *testing.T) {
		// Create a leaf sub-page with one entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("entry1"), 1000)
		if err != nil {
			t.Fatalf("Failed to create initial leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Add a second entry to the same sub-page
		err = db.addEntryToLeafSubPage(parentRadixSubPage, 'a', leafSubPage, []byte("entry2"), 2000)
		if err != nil {
			t.Fatalf("Failed to add second entry: %v", err)
		}

		// Verify we have 2 entries
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Fatalf("Expected 2 entries, got %d", entryCount)
		}

		// Verify the entries are correct
		entry1DataOffset, entry1Suffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		if entry1DataOffset != 1000 {
			t.Errorf("Expected first entry data offset 1000, got %d", entry1DataOffset)
		}
		if !bytes.Equal(entry1Suffix, []byte("entry1")) {
			t.Errorf("Expected suffix1 'entry1', got %v", entry1Suffix)
		}

		entry2DataOffset, entry2Suffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 1)
		if !found {
			t.Fatalf("Expected to find entry at index 1")
		}
		if entry2DataOffset != 2000 {
			t.Errorf("Expected second entry data offset 2000, got %d", entry2DataOffset)
		}
		if !bytes.Equal(entry2Suffix, []byte("entry2")) {
			t.Errorf("Expected suffix2 'entry2', got %v", entry2Suffix)
		}
	})

	t.Run("AddMultipleEntriesToSubPage", func(t *testing.T) {
		// Create a leaf sub-page with one entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("base"), 1000)
		if err != nil {
			t.Fatalf("Failed to create initial leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Add multiple entries
		entries := []struct {
			suffix     []byte
			dataOffset int64
		}{
			{[]byte("alpha"), 2000},
			{[]byte("beta"), 3000},
			{[]byte("gamma"), 4000},
			{[]byte("delta"), 5000},
		}

		for i, entry := range entries {
			err = db.addEntryToLeafSubPage(parentRadixSubPage, 'x', leafSubPage, entry.suffix, entry.dataOffset)
			if err != nil {
				t.Fatalf("Failed to add entry %d: %v", i, err)
			}
		}

		// Verify we have all entries (base + 4 new ones)
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		expectedCount := 1 + len(entries)
		if entryCount != expectedCount {
			t.Fatalf("Expected %d entries, got %d", expectedCount, entryCount)
		}

		// Verify each entry can be read correctly
		for i := 1; i < countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx); i++ { // Skip base entry at index 0
			dataOffset, suffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, i)
			if !found {
				t.Fatalf("Expected to find entry at index %d", i)
			}
			expectedSuffix := entries[i-1].suffix
			expectedDataOffset := entries[i-1].dataOffset

			if dataOffset != expectedDataOffset {
				t.Errorf("Entry %d: expected data offset %d, got %d", i, expectedDataOffset, dataOffset)
			}

			if !bytes.Equal(suffix, expectedSuffix) {
				t.Errorf("Entry %d: expected suffix %v, got %v", i, expectedSuffix, suffix)
			}
		}
	})

	t.Run("AddEntryToFullSubPage", func(t *testing.T) {
		// Create a leaf sub-page
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("start"), 1000)
		if err != nil {
			t.Fatalf("Failed to create initial leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Fill the sub-page with many entries to make it full
		// Add entries until we approach the page size limit
		addedCount := 0
		for {
			suffix := []byte(fmt.Sprintf("entry_%d", addedCount))
			dataOffset := int64(2000 + addedCount*100)

			// Try to add the entry
			err = db.addEntryToLeafSubPage(parentRadixSubPage, 'y', leafSubPage, suffix, dataOffset)
			if err != nil {
				// If we get an error, it might be because the sub-page is full
				// This is expected behavior - the function should handle this gracefully
				break
			}

			addedCount++

			// Safety check to prevent infinite loop
			if addedCount > 100 {
				break
			}
		}

		// Verify we added at least some entries
		if addedCount < 5 {
			t.Errorf("Expected to add at least 5 entries before hitting limit, got %d", addedCount)
		}

		// Verify the sub-page structure is still valid
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		// We expect the entry count to be addedCount + 1 (for the initial "start" entry)
		expectedCount := addedCount + 1
		if entryCount != expectedCount {
			t.Errorf("Expected %d entries in sub-page, got %d", expectedCount, entryCount)
		}
	})

	t.Run("AddEntryWithEmptySuffix", func(t *testing.T) {
		// Create a leaf sub-page
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("normal"), 1000)
		if err != nil {
			t.Fatalf("Failed to create initial leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Add an entry with empty suffix
		err = db.addEntryToLeafSubPage(parentRadixSubPage, 'z', leafSubPage, []byte{}, 2000)
		if err != nil {
			t.Fatalf("Failed to add entry with empty suffix: %v", err)
		}

		// Verify we have 2 entries
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Fatalf("Expected 2 entries, got %d", entryCount)
		}

		// Verify the empty suffix entry
		emptyEntryDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 1)
		if !found {
			t.Fatalf("Expected to find entry at index 1")
		}
		if emptyEntryDataOffset != 2000 {
			t.Errorf("Expected data offset 2000, got %d", emptyEntryDataOffset)
		}
	})

	t.Run("AddEntryWithLargeSuffix", func(t *testing.T) {
		// Create a leaf sub-page
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("small"), 1000)
		if err != nil {
			t.Fatalf("Failed to create initial leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Add an entry with large suffix
		largeSuffix := make([]byte, 500)
		for i := range largeSuffix {
			largeSuffix[i] = byte('A' + (i % 26))
		}

		err = db.addEntryToLeafSubPage(parentRadixSubPage, 'L', leafSubPage, largeSuffix, 3000)
		if err != nil {
			t.Fatalf("Failed to add entry with large suffix: %v", err)
		}

		// Verify we have 2 entries
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Fatalf("Expected 2 entries, got %d", entryCount)
		}

		// Verify the large suffix entry
		largeEntryDataOffset, retrievedSuffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 1)
		if !found {
			t.Fatalf("Expected to find entry at index 1")
		}
		if largeEntryDataOffset != 3000 {
			t.Errorf("Expected data offset 3000, got %d", largeEntryDataOffset)
		}

		// Verify the large suffix is correct
		if !bytes.Equal(retrievedSuffix, largeSuffix) {
			t.Errorf("Large suffix mismatch: expected length %d, got length %d",
				len(largeSuffix), len(retrievedSuffix))
		}
	})

	t.Run("VerifySubPageGrowth", func(t *testing.T) {
		// Create a leaf sub-page
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("initial"), 1000)
		if err != nil {
			t.Fatalf("Failed to create initial leaf sub-page: %v", err)
		}

		// Create a mock parent radix sub-page
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Record initial sub-page size
		initialSubPageInfo := leafSubPage.Page.SubPages[leafSubPage.SubPageIdx]
		initialSize := initialSubPageInfo.Size

		// Add an entry
		err = db.addEntryToLeafSubPage(parentRadixSubPage, 'g', leafSubPage, []byte("growth_test"), 2000)
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}

		// Verify sub-page size increased
		updatedSubPageInfo := leafSubPage.Page.SubPages[leafSubPage.SubPageIdx]
		updatedSize := updatedSubPageInfo.Size

		if updatedSize <= initialSize {
			t.Errorf("Expected sub-page size to increase from %d, got %d", initialSize, updatedSize)
		}

		// Verify the leaf page content size also increased
		if leafSubPage.Page.ContentSize <= LeafHeaderSize {
			t.Errorf("Expected leaf page content size > %d, got %d", LeafHeaderSize, leafSubPage.Page.ContentSize)
		}
	})
}

// TestUpdateEntryInLeafSubPage tests the updateEntryInLeafSubPage function
func TestUpdateEntryInLeafSubPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_updateentryinleafsubpage.db")

	// Open the database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.rollbackTransaction()

	t.Run("UpdateEntryInSubPage", func(t *testing.T) {
		// Create a leaf sub-page with multiple entries
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("entry1"), 1000)
		if err != nil {
			t.Fatalf("Failed to add first entry: %v", err)
		}

		// Create a mock parent radix sub-page for testing
		parentRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate parent radix sub-page: %v", err)
		}

		// Add more entries to the same sub-page
		err = db.addEntryToLeafSubPage(parentRadixSubPage, 'b', leafSubPage, []byte("entry2"), 2000)
		if err != nil {
			t.Fatalf("Failed to add second entry: %v", err)
		}

		// Verify we have 2 entries
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 2 {
			t.Fatalf("Expected 2 entries, got %d", entryCount)
		}

		// Update the first entry's data offset
		newDataOffset := int64(9999)

		// First, get the suffix of the entry we want to update
		_, entrySuffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Failed to find entry at index 0")
		}

		// Now use findEntryInLeafSubPage to get the entry offset and size
		entryOffset, entrySize, _, err := db.findEntryInLeafSubPage(leafSubPage.Page, leafSubPage.SubPageIdx, entrySuffix)
		if err != nil {
			t.Fatalf("Failed to find entry details: %v", err)
		}
		if entryOffset < 0 {
			t.Fatalf("Failed to find entry offset")
		}

		// Now update the entry with the correct parameters
		err = db.updateEntryInLeafSubPage(leafSubPage, entryOffset, entrySize, newDataOffset)
		if err != nil {
			t.Fatalf("Failed to update entry: %v", err)
		}

		// Verify the entry was updated
		updatedDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		if updatedDataOffset != newDataOffset {
			t.Errorf("Expected updated data offset %d, got %d", newDataOffset, updatedDataOffset)
		}
	})

	t.Run("UpdateSingleEntry", func(t *testing.T) {
		// Create a leaf sub-page with one entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("single_entry"), 5000)
		if err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}

		// Update the only entry
		newDataOffset := int64(7777)

		// First, get the suffix of the entry we want to update
		_, entrySuffix, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Failed to find entry at index 0")
		}

		// Now use findEntryInLeafSubPage to get the entry offset and size
		entryOffset, entrySize, _, err := db.findEntryInLeafSubPage(leafSubPage.Page, leafSubPage.SubPageIdx, entrySuffix)
		if err != nil {
			t.Fatalf("Failed to find entry details: %v", err)
		}
		if entryOffset < 0 {
			t.Fatalf("Failed to find entry offset")
		}

		// Now update the entry with the correct parameters
		err = db.updateEntryInLeafSubPage(leafSubPage, entryOffset, entrySize, newDataOffset)
		if err != nil {
			t.Fatalf("Failed to update entry: %v", err)
		}

		// Verify the entry was updated
		entryCount := countSubPageEntries(db, leafSubPage.Page, leafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry after update, got %d", entryCount)
		}

		entryDataOffset, _, found := getSubPageEntry(db, leafSubPage.Page, leafSubPage.SubPageIdx, 0)
		if !found {
			t.Fatalf("Expected to find entry at index 0")
		}
		if entryDataOffset != newDataOffset {
			t.Errorf("Expected updated data offset %d, got %d", newDataOffset, entryDataOffset)
		}
	})
}

// TestRemoveSubPageFromLeafPage tests the removeSubPageFromLeafPage function
func TestRemoveSubPageFromLeafPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_removesubpage.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction to create pages in current transaction sequence
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("RemoveSingleSubPage", func(t *testing.T) {
		// Create a leaf sub-page with one entry
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("test_entry"), 1000)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		leafPage := leafSubPage.Page
		subPageIdx := leafSubPage.SubPageIdx
		originalContentSize := leafPage.ContentSize

		// Verify sub-page exists
		if leafPage.SubPages[subPageIdx] == nil {
			t.Fatal("Sub-page should exist before removal")
		}

		// Store the sub-page info before removal
		subPageInfo := leafPage.SubPages[subPageIdx]
		expectedRemovedSize := LeafSubPageHeaderSize + int(subPageInfo.Size)

		// Remove the sub-page
		db.removeSubPageFromLeafPage(leafPage, subPageIdx)

		// Verify sub-page was removed
		if leafPage.SubPages[subPageIdx] != nil {
			t.Error("Sub-page should be removed")
		}

		// Verify content size was updated correctly
		expectedNewContentSize := originalContentSize - expectedRemovedSize
		if leafPage.ContentSize != expectedNewContentSize {
			t.Errorf("Expected content size %d, got %d", expectedNewContentSize, leafPage.ContentSize)
		}

		// Verify page is marked as dirty
		if !leafPage.dirty {
			t.Error("Leaf page should be marked as dirty after sub-page removal")
		}
	})

	t.Run("RemoveFromMultipleSubPages", func(t *testing.T) {
		// Create multiple sub-pages
		leafSubPage1, err := db.addEntryToNewLeafSubPage([]byte("entry1"), 1001)
		if err != nil {
			t.Fatalf("Failed to create first leaf sub-page: %v", err)
		}

		leafPage := leafSubPage1.Page

		// Add a second entry to create another sub-page on the same page
		leafSubPage2, err := db.addEntryToNewLeafSubPage([]byte("entry2"), 1002)
		if err != nil {
			t.Fatalf("Failed to create second leaf sub-page: %v", err)
		}

		// Verify both sub-pages are on the same page (this might not always be the case,
		// but for this test we'll check if they are)
		if leafSubPage2.Page.pageNumber == leafPage.pageNumber {
			subPageIdx1 := leafSubPage1.SubPageIdx
			subPageIdx2 := leafSubPage2.SubPageIdx

			// Store original offsets
			originalOffset2 := leafPage.SubPages[subPageIdx2].Offset

			// Remove the first sub-page
			db.removeSubPageFromLeafPage(leafPage, subPageIdx1)

			// Verify first sub-page was removed
			if leafPage.SubPages[subPageIdx1] != nil {
				t.Error("First sub-page should be removed")
			}

			// Verify second sub-page still exists
			if leafPage.SubPages[subPageIdx2] == nil {
				t.Error("Second sub-page should still exist")
			}

			// Verify second sub-page offset was updated (should be shifted down)
			newOffset2 := leafPage.SubPages[subPageIdx2].Offset
			if newOffset2 >= originalOffset2 {
				t.Error("Second sub-page offset should be shifted down after first sub-page removal")
			}
		}
	})

	t.Run("RemoveNonExistentSubPage", func(t *testing.T) {
		// Create a leaf page
		leafSubPage, err := db.addEntryToNewLeafSubPage([]byte("test_entry"), 1003)
		if err != nil {
			t.Fatalf("Failed to create leaf sub-page: %v", err)
		}

		leafPage := leafSubPage.Page
		originalContentSize := leafPage.ContentSize

		// Try to remove a non-existent sub-page (should not crash or change anything)
		nonExistentIdx := uint8(200) // Use a high index that's unlikely to exist
		db.removeSubPageFromLeafPage(leafPage, nonExistentIdx)

		// Verify content size didn't change
		if leafPage.ContentSize != originalContentSize {
			t.Error("Content size should not change when removing non-existent sub-page")
		}
	})
}

// ------------------------------------------------------------------------------------------------
// Radix page tests
// ------------------------------------------------------------------------------------------------

// TestSetRadixEntry tests the setRadixEntry function for radix entry setting
func TestSetRadixEntry(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_setradixentry.db")

	db, err := Open(dbPath)
		if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("SetSingleEntry", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data
		byteValue := uint8('a')
		pageNumber := uint32(12345)
		nextSubPageIdx := uint8(7)

		// Set the radix entry
		db.setRadixEntry(radixSubPage, byteValue, pageNumber, nextSubPageIdx)

		// Verify the page is marked as dirty
		if !radixSubPage.Page.dirty {
			t.Error("Radix page should be marked as dirty after setting entry")
		}

		// Verify the entry was set correctly using getRadixEntry
		retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if retrievedPageNumber != pageNumber {
			t.Errorf("Expected page number %d, got %d", pageNumber, retrievedPageNumber)
		}
		if retrievedNextSubPageIdx != nextSubPageIdx {
			t.Errorf("Expected next sub-page index %d, got %d", nextSubPageIdx, retrievedNextSubPageIdx)
		}
	})

	t.Run("SetMultipleEntries", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data - multiple entries with different byte values
		testEntries := []struct {
			byteValue       uint8
			pageNumber      uint32
			nextSubPageIdx  uint8
		}{
			{'a', 1000, 1},
			{'z', 2000, 2},
			{0x00, 3000, 3}, // Null byte
			{0xFF, 4000, 4}, // Max byte value
			{'A', 5000, 5},  // Upper case
			{' ', 6000, 6},  // Space character
			{'\t', 7000, 7}, // Tab character
			{128, 8000, 8},  // High bit set
		}

		// Set all entries
		for _, testEntry := range testEntries {
			db.setRadixEntry(radixSubPage, testEntry.byteValue, testEntry.pageNumber, testEntry.nextSubPageIdx)
		}

		// Verify all entries were set correctly
		for _, testEntry := range testEntries {
			retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, testEntry.byteValue)
			if retrievedPageNumber != testEntry.pageNumber {
				t.Errorf("Byte %d: expected page number %d, got %d", testEntry.byteValue, testEntry.pageNumber, retrievedPageNumber)
			}
			if retrievedNextSubPageIdx != testEntry.nextSubPageIdx {
				t.Errorf("Byte %d: expected next sub-page index %d, got %d", testEntry.byteValue, testEntry.nextSubPageIdx, retrievedNextSubPageIdx)
			}
		}
	})

	t.Run("OverwriteExistingEntry", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data
		byteValue := uint8('x')
		originalPageNumber := uint32(1000)
		originalNextSubPageIdx := uint8(10)
		newPageNumber := uint32(2000)
		newNextSubPageIdx := uint8(20)

		// Set the original entry
		db.setRadixEntry(radixSubPage, byteValue, originalPageNumber, originalNextSubPageIdx)

		// Verify original entry
		retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if retrievedPageNumber != originalPageNumber {
			t.Errorf("Expected original page number %d, got %d", originalPageNumber, retrievedPageNumber)
		}
		if retrievedNextSubPageIdx != originalNextSubPageIdx {
			t.Errorf("Expected original next sub-page index %d, got %d", originalNextSubPageIdx, retrievedNextSubPageIdx)
		}

		// Overwrite with new entry
		db.setRadixEntry(radixSubPage, byteValue, newPageNumber, newNextSubPageIdx)

		// Verify new entry
		retrievedPageNumber, retrievedNextSubPageIdx = db.getRadixEntry(radixSubPage, byteValue)
		if retrievedPageNumber != newPageNumber {
			t.Errorf("Expected new page number %d, got %d", newPageNumber, retrievedPageNumber)
		}
		if retrievedNextSubPageIdx != newNextSubPageIdx {
			t.Errorf("Expected new next sub-page index %d, got %d", newNextSubPageIdx, retrievedNextSubPageIdx)
		}
	})

	t.Run("SetAllPossibleByteValues", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Set entries for all possible byte values (0-255)
		for i := 0; i <= 255; i++ {
			byteValue := uint8(i)
			pageNumber := uint32(10000 + i)
			nextSubPageIdx := uint8(i % 256) // Ensure it stays within uint8 range

			db.setRadixEntry(radixSubPage, byteValue, pageNumber, nextSubPageIdx)
		}

		// Verify all entries
		for i := 0; i <= 255; i++ {
			byteValue := uint8(i)
			expectedPageNumber := uint32(10000 + i)
			expectedNextSubPageIdx := uint8(i % 256)

			retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
			if retrievedPageNumber != expectedPageNumber {
				t.Errorf("Byte %d: expected page number %d, got %d", i, expectedPageNumber, retrievedPageNumber)
			}
			if retrievedNextSubPageIdx != expectedNextSubPageIdx {
				t.Errorf("Byte %d: expected next sub-page index %d, got %d", i, expectedNextSubPageIdx, retrievedNextSubPageIdx)
			}
		}
	})

	t.Run("SetEntryWithZeroValues", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test setting entry with zero values
		byteValue := uint8('m')
		pageNumber := uint32(0)
		nextSubPageIdx := uint8(0)

		db.setRadixEntry(radixSubPage, byteValue, pageNumber, nextSubPageIdx)

		// Verify zero values are stored correctly
		retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if retrievedPageNumber != pageNumber {
			t.Errorf("Expected page number %d, got %d", pageNumber, retrievedPageNumber)
		}
		if retrievedNextSubPageIdx != nextSubPageIdx {
			t.Errorf("Expected next sub-page index %d, got %d", nextSubPageIdx, retrievedNextSubPageIdx)
		}
	})

	t.Run("SetEntryWithMaxValues", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test setting entry with maximum values
		byteValue := uint8(255)
		pageNumber := uint32(4294967295) // Max uint32
		nextSubPageIdx := uint8(255)     // Max uint8

		db.setRadixEntry(radixSubPage, byteValue, pageNumber, nextSubPageIdx)

		// Verify maximum values are stored correctly
		retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if retrievedPageNumber != pageNumber {
			t.Errorf("Expected page number %d, got %d", pageNumber, retrievedPageNumber)
		}
		if retrievedNextSubPageIdx != nextSubPageIdx {
			t.Errorf("Expected next sub-page index %d, got %d", nextSubPageIdx, retrievedNextSubPageIdx)
		}
	})

	t.Run("SetEntriesWithSamePageNumberDifferentSubPageIdx", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test setting multiple entries with same page number but different sub-page indexes
		testEntries := []struct {
			byteValue       uint8
			nextSubPageIdx  uint8
		}{
			{'a', 1},
			{'b', 2},
			{'c', 3},
		}

		pageNumber := uint32(9999) // Same page number for all

		// Set all entries with same page number
		for _, testEntry := range testEntries {
			db.setRadixEntry(radixSubPage, testEntry.byteValue, pageNumber, testEntry.nextSubPageIdx)
		}

		// Verify all entries
		for _, testEntry := range testEntries {
			retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, testEntry.byteValue)
			if retrievedPageNumber != pageNumber {
				t.Errorf("Byte %c: expected page number %d, got %d", testEntry.byteValue, pageNumber, retrievedPageNumber)
			}
			if retrievedNextSubPageIdx != testEntry.nextSubPageIdx {
				t.Errorf("Byte %c: expected next sub-page index %d, got %d", testEntry.byteValue, testEntry.nextSubPageIdx, retrievedNextSubPageIdx)
			}
		}
	})
}

// TestGetRadixEntry tests the getRadixEntry function for radix entry retrieval
func TestGetRadixEntry(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_getradixentry.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("GetFromEmptyRadixPage", func(t *testing.T) {
		// Create a fresh radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Try to get entry from empty page
		byteValue := uint8('a')
		pageNumber, nextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)

		// Should return zero values for empty entry
		if pageNumber != 0 {
			t.Errorf("Expected page number 0 for empty entry, got %d", pageNumber)
		}
		if nextSubPageIdx != 0 {
			t.Errorf("Expected next sub-page index 0 for empty entry, got %d", nextSubPageIdx)
		}
	})

	t.Run("GetExistingEntry", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Set an entry first
		byteValue := uint8('x')
		expectedPageNumber := uint32(5555)
		expectedNextSubPageIdx := uint8(15)

		db.setRadixEntry(radixSubPage, byteValue, expectedPageNumber, expectedNextSubPageIdx)

		// Get the entry
		retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)

		// Verify the values
		if retrievedPageNumber != expectedPageNumber {
			t.Errorf("Expected page number %d, got %d", expectedPageNumber, retrievedPageNumber)
		}
		if retrievedNextSubPageIdx != expectedNextSubPageIdx {
			t.Errorf("Expected next sub-page index %d, got %d", expectedNextSubPageIdx, retrievedNextSubPageIdx)
		}
	})

	t.Run("GetNonExistentEntry", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Set some entries but not the one we'll try to get
		db.setRadixEntry(radixSubPage, 'a', 1000, 1)
		db.setRadixEntry(radixSubPage, 'b', 2000, 2)
		db.setRadixEntry(radixSubPage, 'c', 3000, 3)

		// Try to get an entry that wasn't set
		byteValue := uint8('z')
		pageNumber, nextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)

		// Should return zero values for non-existent entry
		if pageNumber != 0 {
			t.Errorf("Expected page number 0 for non-existent entry, got %d", pageNumber)
		}
		if nextSubPageIdx != 0 {
			t.Errorf("Expected next sub-page index 0 for non-existent entry, got %d", nextSubPageIdx)
		}
	})

	t.Run("GetAllPossibleByteValues", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Set entries for specific byte values
		setEntries := map[uint8]struct {
			pageNumber     uint32
			nextSubPageIdx uint8
		}{
			0:   {1000, 10},
			1:   {1001, 11},
			127: {1127, 127},
			128: {1128, 128},
			254: {1254, 254},
			255: {1255, 255},
		}

		// Set the specific entries
		for byteValue, values := range setEntries {
			db.setRadixEntry(radixSubPage, byteValue, values.pageNumber, values.nextSubPageIdx)
		}

		// Test getting all possible byte values (0-255)
		for i := 0; i <= 255; i++ {
			byteValue := uint8(i)
			pageNumber, nextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)

			if values, exists := setEntries[byteValue]; exists {
				// This byte value was set, should return the set values
				if pageNumber != values.pageNumber {
					t.Errorf("Byte %d: expected page number %d, got %d", i, values.pageNumber, pageNumber)
				}
				if nextSubPageIdx != values.nextSubPageIdx {
					t.Errorf("Byte %d: expected next sub-page index %d, got %d", i, values.nextSubPageIdx, nextSubPageIdx)
				}
			} else {
				// This byte value was not set, should return zero values
				if pageNumber != 0 {
					t.Errorf("Byte %d: expected page number 0 for unset entry, got %d", i, pageNumber)
				}
				if nextSubPageIdx != 0 {
					t.Errorf("Byte %d: expected next sub-page index 0 for unset entry, got %d", i, nextSubPageIdx)
				}
			}
		}
	})

	t.Run("GetAfterOverwrite", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		byteValue := uint8('t')

		// Set original entry
		originalPageNumber := uint32(7777)
		originalNextSubPageIdx := uint8(77)
		db.setRadixEntry(radixSubPage, byteValue, originalPageNumber, originalNextSubPageIdx)

		// Verify original entry
		pageNumber, nextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if pageNumber != originalPageNumber {
			t.Errorf("Expected original page number %d, got %d", originalPageNumber, pageNumber)
		}
		if nextSubPageIdx != originalNextSubPageIdx {
			t.Errorf("Expected original next sub-page index %d, got %d", originalNextSubPageIdx, nextSubPageIdx)
		}

		// Overwrite with new entry
		newPageNumber := uint32(8888)
		newNextSubPageIdx := uint8(88)
		db.setRadixEntry(radixSubPage, byteValue, newPageNumber, newNextSubPageIdx)

		// Verify new entry is returned
		pageNumber, nextSubPageIdx = db.getRadixEntry(radixSubPage, byteValue)
		if pageNumber != newPageNumber {
			t.Errorf("Expected new page number %d, got %d", newPageNumber, pageNumber)
		}
		if nextSubPageIdx != newNextSubPageIdx {
			t.Errorf("Expected new next sub-page index %d, got %d", newNextSubPageIdx, nextSubPageIdx)
		}
	})

	t.Run("GetZeroValues", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Set entry with zero values
		byteValue := uint8('z')
		pageNumber := uint32(0)
		nextSubPageIdx := uint8(0)

		db.setRadixEntry(radixSubPage, byteValue, pageNumber, nextSubPageIdx)

		// Get the entry and verify zero values are returned correctly
		retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if retrievedPageNumber != pageNumber {
			t.Errorf("Expected page number %d, got %d", pageNumber, retrievedPageNumber)
		}
		if retrievedNextSubPageIdx != nextSubPageIdx {
			t.Errorf("Expected next sub-page index %d, got %d", nextSubPageIdx, retrievedNextSubPageIdx)
		}
	})

	t.Run("GetMaxValues", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Set entry with maximum values
		byteValue := uint8('M')
		pageNumber := uint32(4294967295) // Max uint32
		nextSubPageIdx := uint8(255)     // Max uint8

		db.setRadixEntry(radixSubPage, byteValue, pageNumber, nextSubPageIdx)

		// Get the entry and verify maximum values are returned correctly
		retrievedPageNumber, retrievedNextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if retrievedPageNumber != pageNumber {
			t.Errorf("Expected page number %d, got %d", pageNumber, retrievedPageNumber)
		}
		if retrievedNextSubPageIdx != nextSubPageIdx {
			t.Errorf("Expected next sub-page index %d, got %d", nextSubPageIdx, retrievedNextSubPageIdx)
		}
	})

	t.Run("GetAfterMultipleOperations", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Perform multiple set and get operations to test consistency
		operations := []struct {
			operation       string
			byteValue       uint8
			pageNumber      uint32
			nextSubPageIdx  uint8
		}{
			{"set", 'a', 1000, 10},
			{"set", 'b', 2000, 20},
			{"set", 'c', 3000, 30},
			{"set", 'a', 1001, 11}, // Overwrite 'a'
			{"set", 'd', 4000, 40},
			{"set", 'b', 0, 0},     // Set 'b' to zero values
		}

		// Current expected state
		expectedState := make(map[uint8]struct {
			pageNumber     uint32
			nextSubPageIdx uint8
		})

		for _, op := range operations {
			if op.operation == "set" {
				db.setRadixEntry(radixSubPage, op.byteValue, op.pageNumber, op.nextSubPageIdx)
				expectedState[op.byteValue] = struct {
					pageNumber     uint32
					nextSubPageIdx uint8
				}{op.pageNumber, op.nextSubPageIdx}
			}

			// Verify current state after each operation
			for byteValue, expected := range expectedState {
				pageNumber, nextSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
				if pageNumber != expected.pageNumber {
					t.Errorf("After operation %v, byte %c: expected page number %d, got %d",
						op, byteValue, expected.pageNumber, pageNumber)
				}
				if nextSubPageIdx != expected.nextSubPageIdx {
					t.Errorf("After operation %v, byte %c: expected next sub-page index %d, got %d",
						op, byteValue, expected.nextSubPageIdx, nextSubPageIdx)
				}
			}
		}
	})
}

// TestCreatePathForByte tests the createPathForByte function with the new multi-sub-page format
func TestCreatePathForByte(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_createpathforbyte.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("CreatePathForEmptySuffix", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data - key where we're at the last byte (empty suffix after this byte)
		key := []byte("test")
		keyPos := 3 // Last position, so suffix after this byte is empty
		dataOffset := int64(1000)

		// Call createPathForByte
		err = db.createPathForByte(radixSubPage, key, keyPos, dataOffset)
		if err != nil {
			t.Fatalf("Failed to create path for byte: %v", err)
		}

		// Verify a radix entry was created for the byte 't' (key[3])
		byteValue := key[keyPos]
		childPageNumber, childSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if childPageNumber == 0 {
			t.Error("Expected radix entry to be created for the byte")
		}

		// Verify the child is a radix sub-page with empty suffix offset set
		childRadixSubPage, err := db.getRadixSubPage(childPageNumber, childSubPageIdx)
		if err != nil {
			t.Fatalf("Failed to get child radix sub-page: %v", err)
		}

		emptySuffixOffset := db.getEmptySuffixOffset(childRadixSubPage)
		if emptySuffixOffset != dataOffset {
			t.Errorf("Expected empty suffix offset %d, got %d", dataOffset, emptySuffixOffset)
		}
	})

	t.Run("CreatePathForNonEmptySuffix", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data - key where we have remaining suffix after this byte
		key := []byte("testing")
		keyPos := 3 // Position 't', so suffix is "ing"
		dataOffset := int64(2000)

		// Call createPathForByte
		err = db.createPathForByte(radixSubPage, key, keyPos, dataOffset)
		if err != nil {
			t.Fatalf("Failed to create path for byte: %v", err)
		}

		// Verify a radix entry was created for the byte 't' (key[3])
		byteValue := key[keyPos]
		childPageNumber, childSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if childPageNumber == 0 {
			t.Error("Expected radix entry to be created for the byte")
		}

		// Verify the child is a leaf sub-page with the suffix "ing"
		childLeafSubPage, err := db.getLeafSubPage(childPageNumber, childSubPageIdx)
		if err != nil {
			t.Fatalf("Failed to get child leaf sub-page: %v", err)
		}

		// Verify the leaf sub-page has the correct entry
		entryCount := countSubPageEntries(db, childLeafSubPage.Page, childLeafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry in leaf sub-page, got %d", entryCount)
		}

		entryDataOffset, entrySuffix, found := getSubPageEntry(db, childLeafSubPage.Page, childLeafSubPage.SubPageIdx, 0)
		if !found {
			t.Errorf("Expected to find entry at index 0")
		}
		if entryDataOffset != dataOffset {
			t.Errorf("Expected data offset %d, got %d", dataOffset, entryDataOffset)
		}

		// Verify the suffix is correct
		expectedSuffix := key[keyPos+1:] // "ing"
		if !bytes.Equal(entrySuffix, expectedSuffix) {
			t.Errorf("Expected suffix %v, got %v", expectedSuffix, entrySuffix)
		}
	})

	t.Run("CreatePathForSingleCharacterSuffix", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data - key where we have a single character suffix
		key := []byte("ab")
		keyPos := 0 // Position 'a', so suffix is "b"
		dataOffset := int64(3000)

		// Call createPathForByte
		err = db.createPathForByte(radixSubPage, key, keyPos, dataOffset)
		if err != nil {
			t.Fatalf("Failed to create path for byte: %v", err)
		}

		// Verify a radix entry was created for the byte 'a' (key[0])
		byteValue := key[keyPos]
		childPageNumber, childSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if childPageNumber == 0 {
			t.Error("Expected radix entry to be created for the byte")
		}

		// Verify the child is a leaf sub-page with the suffix "b"
		childLeafSubPage, err := db.getLeafSubPage(childPageNumber, childSubPageIdx)
		if err != nil {
			t.Fatalf("Failed to get child leaf sub-page: %v", err)
		}

		// Verify the leaf sub-page has the correct entry
		entryCount := countSubPageEntries(db, childLeafSubPage.Page, childLeafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry in leaf sub-page, got %d", entryCount)
		}

		entryDataOffset, entrySuffix, found := getSubPageEntry(db, childLeafSubPage.Page, childLeafSubPage.SubPageIdx, 0)
		if !found {
			t.Errorf("Expected to find entry at index 0")
		}
		if entryDataOffset != dataOffset {
			t.Errorf("Expected data offset %d, got %d", dataOffset, entryDataOffset)
		}

		// Verify the suffix is "b"
		expectedSuffix := []byte("b")
		if !bytes.Equal(entrySuffix, expectedSuffix) {
			t.Errorf("Expected suffix %v, got %v", expectedSuffix, entrySuffix)
		}
	})

	t.Run("CreatePathForLongSuffix", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data - key with a long suffix
		key := []byte("prefix_very_long_suffix_that_should_be_stored_in_leaf")
		keyPos := 6 // Position '_', so suffix is "very_long_suffix_that_should_be_stored_in_leaf"
		dataOffset := int64(4000)

		// Call createPathForByte
		err = db.createPathForByte(radixSubPage, key, keyPos, dataOffset)
		if err != nil {
			t.Fatalf("Failed to create path for byte: %v", err)
		}

		// Verify a radix entry was created for the byte '_' (key[6])
		byteValue := key[keyPos]
		childPageNumber, childSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
		if childPageNumber == 0 {
			t.Error("Expected radix entry to be created for the byte")
		}

		// Verify the child is a leaf sub-page with the long suffix
		childLeafSubPage, err := db.getLeafSubPage(childPageNumber, childSubPageIdx)
		if err != nil {
			t.Fatalf("Failed to get child leaf sub-page: %v", err)
		}

		// Verify the leaf sub-page has the correct entry
		entryCount := countSubPageEntries(db, childLeafSubPage.Page, childLeafSubPage.SubPageIdx)
		if entryCount != 1 {
			t.Fatalf("Expected 1 entry in leaf sub-page, got %d", entryCount)
		}

		entryDataOffset, entrySuffix, found := getSubPageEntry(db, childLeafSubPage.Page, childLeafSubPage.SubPageIdx, 0)
		if !found {
			t.Errorf("Expected to find entry at index 0")
		}
		if entryDataOffset != dataOffset {
			t.Errorf("Expected data offset %d, got %d", dataOffset, entryDataOffset)
		}

		// Verify the long suffix is correct
		expectedSuffix := key[keyPos+1:] // "very_long_suffix_that_should_be_stored_in_leaf"
		if !bytes.Equal(entrySuffix, expectedSuffix) {
			t.Errorf("Expected suffix %v, got %v", expectedSuffix, entrySuffix)
		}
	})

	t.Run("CreatePathForDifferentByteValues", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data - different byte values
		testCases := []struct {
			key       []byte
			keyPos    int
			dataOffset int64
		}{
			{[]byte("a_suffix"), 0, 5000}, // byte 'a'
			{[]byte("z_suffix"), 0, 6000}, // byte 'z'
			{[]byte{0x00, 's', 'u', 'f', 'f', 'i', 'x'}, 0, 7000}, // byte 0x00
			{[]byte{0xFF, 's', 'u', 'f', 'f', 'i', 'x'}, 0, 8000}, // byte 0xFF
			{[]byte("A_suffix"), 0, 9000}, // byte 'A' (uppercase)
		}

		for i, testCase := range testCases {
			err = db.createPathForByte(radixSubPage, testCase.key, testCase.keyPos, testCase.dataOffset)
			if err != nil {
				t.Fatalf("Test case %d: failed to create path for byte: %v", i, err)
			}

			// Verify the radix entry was created
			byteValue := testCase.key[testCase.keyPos]
			childPageNumber, childSubPageIdx := db.getRadixEntry(radixSubPage, byteValue)
			if childPageNumber == 0 {
				t.Errorf("Test case %d: expected radix entry to be created for byte %d", i, byteValue)
			}

			// Verify the child is a leaf sub-page with the correct suffix
			childLeafSubPage, err := db.getLeafSubPage(childPageNumber, childSubPageIdx)
			if err != nil {
				t.Fatalf("Test case %d: failed to get child leaf sub-page: %v", i, err)
			}

			entryCount := countSubPageEntries(db, childLeafSubPage.Page, childLeafSubPage.SubPageIdx)
			if entryCount != 1 {
				t.Fatalf("Test case %d: expected 1 entry in leaf sub-page, got %d", i, entryCount)
			}

			entryDataOffset, entrySuffix, found := getSubPageEntry(db, childLeafSubPage.Page, childLeafSubPage.SubPageIdx, 0)
			if !found {
				t.Errorf("Expected to find entry at index 0")
			}
			if entryDataOffset != testCase.dataOffset {
				t.Errorf("Test case %d: expected data offset %d, got %d", i, testCase.dataOffset, entryDataOffset)
			}

			// Verify the suffix is correct
			expectedSuffix := testCase.key[testCase.keyPos+1:]
			if !bytes.Equal(entrySuffix, expectedSuffix) {
				t.Errorf("Test case %d: expected suffix %v, got %v", i, expectedSuffix, entrySuffix)
			}
		}
	})

	t.Run("CreatePathUpdatesParentPointer", func(t *testing.T) {
		// Create a radix sub-page
		originalRadixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Store the original page number
		originalPageNumber := originalRadixSubPage.Page.pageNumber

		// Test data
		key := []byte("test_update")
		keyPos := 4 // Position '_', so suffix is "update"
		dataOffset := int64(10000)

		// Call createPathForByte
		err = db.createPathForByte(originalRadixSubPage, key, keyPos, dataOffset)
		if err != nil {
			t.Fatalf("Failed to create path for byte: %v", err)
		}

		// Verify the function might have updated the page pointer due to cloning
		// The page number should remain the same, but the page instance might be different
		if originalRadixSubPage.Page.pageNumber != originalPageNumber {
			t.Errorf("Expected page number to remain %d, got %d", originalPageNumber, originalRadixSubPage.Page.pageNumber)
		}

		// Verify the radix entry was still created correctly
		byteValue := key[keyPos]
		childPageNumber, _ := db.getRadixEntry(originalRadixSubPage, byteValue)
		if childPageNumber == 0 {
			t.Error("Expected radix entry to be created for the byte")
		}
	})
}

// TestEmptySuffixOnRadixPages tests empty suffix operations on radix pages
func TestEmptySuffixOnRadixPages(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_emptysuffix.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("SetEmptySuffixOffset", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data
		dataOffset := int64(12345)

		// Set empty suffix offset
		err = db.setEmptySuffixOffset(radixSubPage, dataOffset)
		if err != nil {
			t.Fatalf("Failed to set empty suffix offset: %v", err)
		}

		// Verify the offset was set correctly
		retrievedOffset := db.getEmptySuffixOffset(radixSubPage)
		if retrievedOffset != dataOffset {
			t.Errorf("Expected empty suffix offset %d, got %d", dataOffset, retrievedOffset)
		}

		// Verify the page is marked as dirty
		if !radixSubPage.Page.dirty {
			t.Error("Radix page should be marked as dirty after setting empty suffix offset")
		}
	})

	t.Run("GetEmptySuffixOffsetWhenZero", func(t *testing.T) {
		// Create a new radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Get empty suffix offset on a fresh page (should be 0)
		offset := db.getEmptySuffixOffset(radixSubPage)
		if offset != 0 {
			t.Errorf("Expected empty suffix offset 0 on fresh page, got %d", offset)
		}
	})

	t.Run("SetOnEmptySuffixInsert", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data
		key := []byte("test_key")
		value := []byte("test_value")

		// Test setOnEmptySuffix for a new insert
		err = db.setOnEmptySuffix(radixSubPage, key, value, 0)
		if err != nil {
			t.Fatalf("Failed to set on empty suffix: %v", err)
		}

		// Verify the empty suffix offset was set
		offset := db.getEmptySuffixOffset(radixSubPage)
		if offset == 0 {
			t.Error("Expected non-zero empty suffix offset after insert")
		}

		// Verify we can read the content back
		content, err := db.readContent(offset)
		if err != nil {
			t.Fatalf("Failed to read content: %v", err)
		}
		if !bytes.Equal(content.key, key) {
			t.Errorf("Expected key %v, got %v", key, content.key)
		}
		if !bytes.Equal(content.value, value) {
			t.Errorf("Expected value %v, got %v", value, content.value)
		}
	})

	t.Run("SetOnEmptySuffixUpdate", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data
		key := []byte("test_key")
		originalValue := []byte("original_value")
		updatedValue := []byte("updated_value")

		// First insert
		err = db.setOnEmptySuffix(radixSubPage, key, originalValue, 0)
		if err != nil {
			t.Fatalf("Failed to insert on empty suffix: %v", err)
		}

		originalOffset := db.getEmptySuffixOffset(radixSubPage)

		// Update with new value
		err = db.setOnEmptySuffix(radixSubPage, key, updatedValue, 0)
		if err != nil {
			t.Fatalf("Failed to update on empty suffix: %v", err)
		}

		// Verify the offset changed (new data was appended)
		updatedOffset := db.getEmptySuffixOffset(radixSubPage)
		if originalOffset == updatedOffset {
			t.Error("Expected offset to change after update")
		}

		// Verify the updated content
		content, err := db.readContent(updatedOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(content.value, updatedValue) {
			t.Errorf("Expected updated value %v, got %v", updatedValue, content.value)
		}
	})

	t.Run("SetOnEmptySuffixUpdateSameValue", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data
		key := []byte("test_key")
		value := []byte("same_value")

		// First insert
		err = db.setOnEmptySuffix(radixSubPage, key, value, 0)
		if err != nil {
			t.Fatalf("Failed to insert on empty suffix: %v", err)
		}

		originalOffset := db.getEmptySuffixOffset(radixSubPage)
		originalMainFileSize := db.mainFileSize

		// "Update" with same value
		err = db.setOnEmptySuffix(radixSubPage, key, value, 0)
		if err != nil {
			t.Fatalf("Failed to update with same value: %v", err)
		}

		// Verify the offset didn't change (no new data written)
		updatedOffset := db.getEmptySuffixOffset(radixSubPage)
		if originalOffset != updatedOffset {
			t.Error("Expected offset to remain the same when updating with same value")
		}

		// Verify no new data was appended to main file
		if db.mainFileSize != originalMainFileSize {
			t.Error("Expected main file size to remain the same when updating with same value")
		}
	})

	t.Run("SetOnEmptySuffixDelete", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data
		key := []byte("test_key")
		value := []byte("value_to_delete")

		// First insert
		err = db.setOnEmptySuffix(radixSubPage, key, value, 0)
		if err != nil {
			t.Fatalf("Failed to insert on empty suffix: %v", err)
		}

		// Verify entry exists
		offset := db.getEmptySuffixOffset(radixSubPage)
		if offset == 0 {
			t.Error("Expected non-zero offset after insert")
		}

		// Delete the entry (empty value means delete)
		err = db.setOnEmptySuffix(radixSubPage, key, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete from empty suffix: %v", err)
		}

		// Verify the offset was cleared
		deletedOffset := db.getEmptySuffixOffset(radixSubPage)
		if deletedOffset != 0 {
			t.Errorf("Expected offset to be 0 after delete, got %d", deletedOffset)
		}
	})

	t.Run("SetOnEmptySuffixDeleteNonExistent", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test data
		key := []byte("non_existent_key")

		// Try to delete a non-existent entry
		err = db.setOnEmptySuffix(radixSubPage, key, []byte{}, 0)
		if err != nil {
			t.Fatalf("Delete of non-existent entry should not fail: %v", err)
		}

		// Verify offset remains 0
		offset := db.getEmptySuffixOffset(radixSubPage)
		if offset != 0 {
			t.Errorf("Expected offset to remain 0, got %d", offset)
		}
	})

	t.Run("ReindexingMode", func(t *testing.T) {
		// Create a radix sub-page
		radixSubPage, err := db.allocateRadixSubPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix sub-page: %v", err)
		}

		// Test reindexing mode where dataOffset is provided (non-zero)
		key := []byte("test_key")
		value := []byte("test_value")
		dataOffset := int64(54321) // Non-zero means reindexing

		// In reindexing mode, we don't append new data
		err = db.setOnEmptySuffix(radixSubPage, key, value, dataOffset)
		if err != nil {
			t.Fatalf("Failed to set in reindexing mode: %v", err)
		}

		// Verify the offset was set to the provided value
		retrievedOffset := db.getEmptySuffixOffset(radixSubPage)
		if retrievedOffset != dataOffset {
			t.Errorf("Expected offset %d, got %d", dataOffset, retrievedOffset)
		}
	})
}

// TestFindLastValidCommit tests the findLastValidCommit function
func TestFindLastValidCommit(t *testing.T) {
	// Helper function to create test content
	createTestContent := func(key, value []byte) []byte {
		keyLenSize := varint.Size(uint64(len(key)))
		valueLenSize := varint.Size(uint64(len(value)))
		totalSize := 1 + keyLenSize + len(key) + valueLenSize + len(value)

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

		return content
	}

	// Helper function to create commit marker
	createCommitMarker := func(checksum uint32) []byte {
		marker := make([]byte, 5)
		marker[0] = ContentTypeCommit
		binary.BigEndian.PutUint32(marker[1:5], checksum)
		return marker
	}

	t.Run("EmptyContent", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Test with empty content (just the header)
		validSize, err := db.findLastValidCommit(PageSize)
		if err != nil {
			t.Fatalf("Failed to find last valid commit: %v", err)
		}

		if validSize != PageSize {
			t.Errorf("Expected valid size %d, got %d", PageSize, validSize)
		}
	})

	t.Run("SingleValidTransaction", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Manually write a transaction to main file
		content1 := createTestContent([]byte("key1"), []byte("value1"))
		content2 := createTestContent([]byte("key2"), []byte("value2"))

		// Calculate checksum
		checksum := crc32.ChecksumIEEE(content1)
		checksum = crc32.Update(checksum, crc32.IEEETable, content2)

		commitMarker := createCommitMarker(checksum)

		// Write to main file
		startOffset := db.mainFileSize
		db.mainFile.Write(content1)
		db.mainFile.Write(content2)
		db.mainFile.Write(commitMarker)

		// Update file size
		newSize := startOffset + int64(len(content1)) + int64(len(content2)) + int64(len(commitMarker))
		db.mainFileSize = newSize

		// Test findLastValidCommit
		validSize, err := db.findLastValidCommit(startOffset)
		if err != nil {
			t.Fatalf("Failed to find last valid commit: %v", err)
		}

		if validSize != newSize {
			t.Errorf("Expected valid size %d, got %d", newSize, validSize)
		}
	})

	t.Run("MultipleValidTransactions", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		startOffset := db.mainFileSize

		// First transaction
		content1 := createTestContent([]byte("key1"), []byte("value1"))
		checksum1 := crc32.ChecksumIEEE(content1)
		commit1 := createCommitMarker(checksum1)

		db.mainFile.Write(content1)
		db.mainFile.Write(commit1)

		// Second transaction
		content2 := createTestContent([]byte("key2"), []byte("value2"))
		checksum2 := crc32.ChecksumIEEE(content2)
		commit2 := createCommitMarker(checksum2)

		db.mainFile.Write(content2)
		db.mainFile.Write(commit2)

		// Update file size
		newSize := startOffset + int64(len(content1)) + int64(len(commit1)) + int64(len(content2)) + int64(len(commit2))
		db.mainFileSize = newSize

		// Test findLastValidCommit
		validSize, err := db.findLastValidCommit(startOffset)
		if err != nil {
			t.Fatalf("Failed to find last valid commit: %v", err)
		}

		if validSize != newSize {
			t.Errorf("Expected valid size %d, got %d", newSize, validSize)
		}
	})

	t.Run("InvalidChecksum", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		startOffset := db.mainFileSize

		// Valid transaction
		content1 := createTestContent([]byte("key1"), []byte("value1"))
		checksum1 := crc32.ChecksumIEEE(content1)
		commit1 := createCommitMarker(checksum1)

		db.mainFile.Write(content1)
		db.mainFile.Write(commit1)

		validTxnEnd := db.mainFileSize + int64(len(content1)) + int64(len(commit1))

		// Invalid transaction (wrong checksum)
		content2 := createTestContent([]byte("key2"), []byte("value2"))
		wrongChecksum := uint32(12345) // Intentionally wrong
		commit2 := createCommitMarker(wrongChecksum)

		db.mainFile.Write(content2)
		db.mainFile.Write(commit2)

		// Update file size to include invalid transaction
		db.mainFileSize = validTxnEnd + int64(len(content2)) + int64(len(commit2))

		// Test findLastValidCommit - should stop at the first valid transaction
		validSize, err := db.findLastValidCommit(startOffset)
		if err != nil {
			t.Fatalf("Failed to find last valid commit: %v", err)
		}

		if validSize != validTxnEnd {
			t.Errorf("Expected valid size %d, got %d", validTxnEnd, validSize)
		}
	})

	t.Run("IncompleteTransaction", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		startOffset := db.mainFileSize

		// Valid transaction
		content1 := createTestContent([]byte("key1"), []byte("value1"))
		checksum1 := crc32.ChecksumIEEE(content1)
		commit1 := createCommitMarker(checksum1)

		db.mainFile.Write(content1)
		db.mainFile.Write(commit1)

		validTxnEnd := db.mainFileSize + int64(len(content1)) + int64(len(commit1))

		// Incomplete transaction (content without commit marker)
		content2 := createTestContent([]byte("key2"), []byte("value2"))
		db.mainFile.Write(content2)

		// Update file size to include incomplete transaction
		db.mainFileSize = validTxnEnd + int64(len(content2))

		// Test findLastValidCommit - should stop at the last valid commit
		validSize, err := db.findLastValidCommit(startOffset)
		if err != nil {
			t.Fatalf("Failed to find last valid commit: %v", err)
		}

		if validSize != validTxnEnd {
			t.Errorf("Expected valid size %d, got %d", validTxnEnd, validSize)
		}
	})
}

// TestRecoverUnindexedContent tests the recoverUnindexedContent function
func TestRecoverUnindexedContent(t *testing.T) {
	t.Run("NoUnindexedContent", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Set lastIndexedOffset to current file size (everything is indexed)
		db.lastIndexedOffset = db.mainFileSize

		// Test recovery - should be a no-op
		err = db.recoverUnindexedContent()
		if err != nil {
			t.Fatalf("Failed to recover unindexed content: %v", err)
		}
	})

	t.Run("RecoverSingleTransaction", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Set lastIndexedOffset to header size (nothing indexed yet)
		db.lastIndexedOffset = PageSize

		// Add some data using normal operations to create committed transactions
		err = db.Set([]byte("key1"), []byte("value1"))
		if err != nil {
			t.Fatalf("Failed to set key1: %v", err)
		}

		err = db.Set([]byte("key2"), []byte("value2"))
		if err != nil {
			t.Fatalf("Failed to set key2: %v", err)
		}

		// Reset lastIndexedOffset to simulate unindexed content
		db.lastIndexedOffset = PageSize

		// Clear the index by reinitializing it
		db.indexFile.Truncate(0)
		db.indexFileSize = 0
		err = db.initializeIndexFile()
		if err != nil {
			t.Fatalf("Failed to reinitialize index: %v", err)
		}

		// Test recovery
		err = db.recoverUnindexedContent()
		if err != nil {
			t.Fatalf("Failed to recover unindexed content: %v", err)
		}

		// Verify that we can read the recovered data
		value1, err := db.Get([]byte("key1"))
		if err != nil {
			t.Fatalf("Failed to get key1 after recovery: %v", err)
		}
		if !bytes.Equal(value1, []byte("value1")) {
			t.Errorf("Expected value1, got %v", value1)
		}

		value2, err := db.Get([]byte("key2"))
		if err != nil {
			t.Fatalf("Failed to get key2 after recovery: %v", err)
		}
		if !bytes.Equal(value2, []byte("value2")) {
			t.Errorf("Expected value2, got %v", value2)
		}
	})

	t.Run("RecoverWithUncommittedData", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Add committed data
		err = db.Set([]byte("key1"), []byte("value1"))
		if err != nil {
			t.Fatalf("Failed to set key1: %v", err)
		}

		originalFileSize := db.mainFileSize

		// Manually append uncommitted data to the main file
		db.beginTransaction()
		_, err = db.appendData([]byte("key2"), []byte("value2"))
		if err != nil {
			t.Fatalf("Failed to append data: %v", err)
		}
		// Don't commit - leave it uncommitted
		db.rollbackTransaction()

		// Reset file size to include uncommitted data
		fileInfo, _ := db.mainFile.Stat()
		db.mainFileSize = fileInfo.Size()

		// Reset lastIndexedOffset to simulate unindexed content
		db.lastIndexedOffset = PageSize

		// Clear the index
		db.indexFile.Truncate(0)
		db.indexFileSize = 0
		err = db.initializeIndexFile()
		if err != nil {
			t.Fatalf("Failed to reinitialize index: %v", err)
		}

		// Test recovery - should truncate uncommitted data
		err = db.recoverUnindexedContent()
		if err != nil {
			t.Fatalf("Failed to recover unindexed content: %v", err)
		}

		// Verify that uncommitted data was truncated
		if db.mainFileSize > originalFileSize {
			t.Errorf("Expected file size to be truncated to %d, got %d", originalFileSize, db.mainFileSize)
		}

		// Verify that committed data is still accessible
		value1, err := db.Get([]byte("key1"))
		if err != nil {
			t.Fatalf("Failed to get key1 after recovery: %v", err)
		}
		if !bytes.Equal(value1, []byte("value1")) {
			t.Errorf("Expected value1, got %v", value1)
		}

		// Verify that uncommitted data is not accessible
		_, err = db.Get([]byte("key2"))
		if err == nil {
			t.Error("Expected key2 to not be found after recovery")
		}
	})

	t.Run("RecoverPartiallyCorruptedData", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Add valid committed data
		err = db.Set([]byte("key1"), []byte("value1"))
		if err != nil {
			t.Fatalf("Failed to set key1: %v", err)
		}

		validFileSize := db.mainFileSize

		// Manually append corrupted data
		corruptedData := []byte{ContentTypeData, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF} // Invalid varint
		db.mainFile.Write(corruptedData)
		db.mainFileSize += int64(len(corruptedData))

		// Reset lastIndexedOffset
		db.lastIndexedOffset = PageSize

		// Clear the index
		db.indexFile.Truncate(0)
		db.indexFileSize = 0
		err = db.initializeIndexFile()
		if err != nil {
			t.Fatalf("Failed to reinitialize index: %v", err)
		}

		// Test recovery - should stop at corruption and truncate
		err = db.recoverUnindexedContent()
		if err != nil {
			t.Fatalf("Failed to recover unindexed content: %v", err)
		}

		// Verify that file was truncated to remove corruption
		if db.mainFileSize != validFileSize {
			t.Errorf("Expected file size %d after truncation, got %d", validFileSize, db.mainFileSize)
		}

		// Verify that valid data is still accessible
		value1, err := db.Get([]byte("key1"))
		if err != nil {
			t.Fatalf("Failed to get key1 after recovery: %v", err)
		}
		if !bytes.Equal(value1, []byte("value1")) {
			t.Errorf("Expected value1, got %v", value1)
		}
	})

	t.Run("RecoverMultipleTransactions", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Add multiple transactions
		keys := []string{"key1", "key2", "key3", "key4", "key5"}
		values := []string{"value1", "value2", "value3", "value4", "value5"}

		for i, key := range keys {
			err = db.Set([]byte(key), []byte(values[i]))
			if err != nil {
				t.Fatalf("Failed to set %s: %v", key, err)
			}
		}

		// Reset lastIndexedOffset to simulate all content being unindexed
		db.lastIndexedOffset = PageSize

		// Clear the index
		db.indexFile.Truncate(0)
		db.indexFileSize = 0
		err = db.initializeIndexFile()
		if err != nil {
			t.Fatalf("Failed to reinitialize index: %v", err)
		}

		// Test recovery
		err = db.recoverUnindexedContent()
		if err != nil {
			t.Fatalf("Failed to recover unindexed content: %v", err)
		}

		// Verify all data is accessible after recovery
		for i, key := range keys {
			value, err := db.Get([]byte(key))
			if err != nil {
				t.Fatalf("Failed to get %s after recovery: %v", key, err)
			}
			if !bytes.Equal(value, []byte(values[i])) {
				t.Errorf("Expected %s, got %v", values[i], value)
			}
		}
	})

	t.Run("RecoverWithDeletions", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Add data
		err = db.Set([]byte("key1"), []byte("value1"))
		if err != nil {
			t.Fatalf("Failed to set key1: %v", err)
		}

		err = db.Set([]byte("key2"), []byte("value2"))
		if err != nil {
			t.Fatalf("Failed to set key2: %v", err)
		}

		// Delete key1
		err = db.Delete([]byte("key1"))
		if err != nil {
			t.Fatalf("Failed to delete key1: %v", err)
		}

		// Reset lastIndexedOffset
		db.lastIndexedOffset = PageSize

		// Clear the index
		db.indexFile.Truncate(0)
		db.indexFileSize = 0
		err = db.initializeIndexFile()
		if err != nil {
			t.Fatalf("Failed to reinitialize index: %v", err)
		}

		// Test recovery
		err = db.recoverUnindexedContent()
		if err != nil {
			t.Fatalf("Failed to recover unindexed content: %v", err)
		}

		// Verify key1 is deleted and key2 exists
		_, err = db.Get([]byte("key1"))
		if err == nil {
			t.Error("Expected key1 to be deleted after recovery")
		}

		value2, err := db.Get([]byte("key2"))
		if err != nil {
			t.Fatalf("Failed to get key2 after recovery: %v", err)
		}
		if !bytes.Equal(value2, []byte("value2")) {
			t.Errorf("Expected value2, got %v", value2)
		}
	})

	t.Run("ReadOnlyMode", func(t *testing.T) {
		// Create a test database with some data
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		// First, create database with data
		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}

		err = db.Set([]byte("key1"), []byte("value1"))
		if err != nil {
			t.Fatalf("Failed to set key1: %v", err)
		}

		db.Close()

		// Reopen in read-only mode
		opts := Options{"ReadOnly": true}
		db, err = Open(tmpFile, opts)
		if err != nil {
			t.Fatalf("Failed to open database in read-only mode: %v", err)
		}
		defer db.Close()

		// Simulate unindexed content
		db.lastIndexedOffset = PageSize

		// Test recovery in read-only mode - should still work for reading
		err = db.recoverUnindexedContent()
		if err != nil {
			t.Fatalf("Failed to recover unindexed content in read-only mode: %v", err)
		}

		// Should be able to read the data
		value1, err := db.Get([]byte("key1"))
		if err != nil {
			t.Fatalf("Failed to get key1 in read-only mode: %v", err)
		}
		if !bytes.Equal(value1, []byte("value1")) {
			t.Errorf("Expected value1, got %v", value1)
		}
	})
}

// TestWriteRadixPage tests the writeRadixPage function
func TestWriteRadixPage(t *testing.T) {
	t.Run("WriteValidRadixPage", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create a radix page
		radixPage := &RadixPage{
			pageNumber:   1,
			pageType:     ContentTypeRadix,
			data:         make([]byte, PageSize),
			dirty:        true,
			SubPagesUsed: 2,
			NextFreePage: 5,
		}

		// Write the radix page
		err = db.writeRadixPage(radixPage)
		if err != nil {
			t.Fatalf("Failed to write radix page: %v", err)
		}

		// Verify the page data was set correctly
		if radixPage.data[0] != ContentTypeRadix {
			t.Errorf("Expected content type %c, got %c", ContentTypeRadix, radixPage.data[0])
		}

		if radixPage.data[1] != 2 {
			t.Errorf("Expected SubPagesUsed 2, got %d", radixPage.data[1])
		}

		nextFreePage := binary.LittleEndian.Uint32(radixPage.data[2:6])
		if nextFreePage != 5 {
			t.Errorf("Expected NextFreePage 5, got %d", nextFreePage)
		}

		// Verify checksum was calculated and stored
		storedChecksum := binary.BigEndian.Uint32(radixPage.data[6:10])
		if storedChecksum == 0 {
			t.Error("Expected non-zero checksum")
		}

		// Verify the page is no longer dirty after writing
		if radixPage.dirty {
			t.Error("Expected page to be clean after writing")
		}
	})

	t.Run("WriteRadixPageWithZeroPageNumber", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create a radix page with page number 0
		radixPage := &RadixPage{
			pageNumber:   0,
			pageType:     ContentTypeRadix,
			data:         make([]byte, PageSize),
			dirty:        true,
			SubPagesUsed: 1,
			NextFreePage: 0,
		}

		// Write should fail for page number 0
		err = db.writeRadixPage(radixPage)
		if err == nil {
			t.Error("Expected error for page number 0, got nil")
		}
		if !strings.Contains(err.Error(), "cannot write radix page with page number 0") {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})
}

// TestWriteLeafPage tests the writeLeafPage function
func TestWriteLeafPage(t *testing.T) {
	t.Run("WriteValidLeafPage", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create a leaf page with some test data using the new sub-page format
		leafPage := &LeafPage{
			pageNumber:  2,
			pageType:    ContentTypeLeaf,
			data:        make([]byte, PageSize),
			dirty:       true,
			ContentSize: 20,
			SubPages:    make([]*LeafSubPageInfo, 256),
		}

		// Add a sample sub-page to test with
		leafPage.SubPages[0] = &LeafSubPageInfo{
			Offset:  8,
			Size:    12,
		}

		// Add entry data to the page
		// Write suffix length (1 byte for length 4)
		leafPage.data[11] = 4
		// Write suffix "test"
		copy(leafPage.data[12:], []byte("test"))
		// Write data offset
		binary.LittleEndian.PutUint64(leafPage.data[16:], 1000)

		// Write the leaf page
		err = db.writeLeafPage(leafPage)
		if err != nil {
			t.Fatalf("Failed to write leaf page: %v", err)
		}

		// Verify the page data was set correctly
		if leafPage.data[0] != ContentTypeLeaf {
			t.Errorf("Expected content type %c, got %c", ContentTypeLeaf, leafPage.data[0])
		}

		contentSize := binary.LittleEndian.Uint16(leafPage.data[2:4])
		if contentSize != 20 {
			t.Errorf("Expected ContentSize 20, got %d", contentSize)
		}

		// Verify entry data was preserved
		// Check suffix length
		if leafPage.data[11] != 4 {
			t.Errorf("Expected suffix length 4, got %d", leafPage.data[11])
		}

		// Check suffix content
		expectedSuffix := []byte("test")
		actualSuffix := leafPage.data[12:16]
		if !bytes.Equal(actualSuffix, expectedSuffix) {
			t.Errorf("Expected suffix %q, got %q", expectedSuffix, actualSuffix)
		}

		// Check data offset
		dataOffset := binary.LittleEndian.Uint64(leafPage.data[16:])
		if dataOffset != 1000 {
			t.Errorf("Expected data offset 1000, got %d", dataOffset)
		}

		// Verify checksum was calculated and stored
		storedChecksum := binary.BigEndian.Uint32(leafPage.data[4:8])
		if storedChecksum == 0 {
			t.Error("Expected non-zero checksum")
		}

		// Verify the page is no longer dirty after writing
		if leafPage.dirty {
			t.Error("Expected page to be clean after writing")
		}
	})

	t.Run("WriteLeafPageWithZeroPageNumber", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create a leaf page with page number 0
		leafPage := &LeafPage{
			pageNumber:  0,
			pageType:    ContentTypeLeaf,
			data:        make([]byte, PageSize),
			dirty:       true,
			ContentSize: 8,
			SubPages:    make([]*LeafSubPageInfo, 256),
		}

		// Write should fail for page number 0
		err = db.writeLeafPage(leafPage)
		if err == nil {
			t.Error("Expected error for page number 0, got nil")
		}
		if !strings.Contains(err.Error(), "cannot write leaf page with page number 0") {
			t.Errorf("Expected specific error message, got: %v", err)
		}
	})

	t.Run("WriteLeafPageWithLargeContentSize", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create a leaf page with maximum content size
		leafPage := &LeafPage{
			pageNumber:  3,
			pageType:    ContentTypeLeaf,
			data:        make([]byte, PageSize),
			dirty:       true,
			ContentSize: PageSize - 1, // Near maximum
			SubPages:    make([]*LeafSubPageInfo, 256),
		}

		// Write should succeed
		err = db.writeLeafPage(leafPage)
		if err != nil {
			t.Fatalf("Failed to write leaf page with large content size: %v", err)
		}

		// Verify content size was written correctly
		contentSize := binary.LittleEndian.Uint16(leafPage.data[2:4])
		if contentSize != uint16(PageSize-1) {
			t.Errorf("Expected ContentSize %d, got %d", PageSize-1, contentSize)
		}
	})
}

// TestParseRadixPage tests the parseRadixPage function
func TestParseRadixPage(t *testing.T) {
	t.Run("ParseValidRadixPage", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create test data for a radix page
		data := make([]byte, PageSize)
		data[0] = ContentTypeRadix    // Content type
		data[1] = 3                   // SubPagesUsed
		binary.LittleEndian.PutUint32(data[2:6], 10) // NextFreePage

		// Calculate and set checksum
		binary.BigEndian.PutUint32(data[6:10], 0) // Zero out checksum field
		checksum := crc32.ChecksumIEEE(data)
		binary.BigEndian.PutUint32(data[6:10], checksum)

		// Parse the radix page
		radixPage, err := db.parseRadixPage(data, 5)
		if err != nil {
			t.Fatalf("Failed to parse radix page: %v", err)
		}

		// Verify parsed fields
		if radixPage.pageNumber != 5 {
			t.Errorf("Expected page number 5, got %d", radixPage.pageNumber)
		}

		if radixPage.pageType != ContentTypeRadix {
			t.Errorf("Expected page type %c, got %c", ContentTypeRadix, radixPage.pageType)
		}

		if radixPage.SubPagesUsed != 3 {
			t.Errorf("Expected SubPagesUsed 3, got %d", radixPage.SubPagesUsed)
		}

		if radixPage.NextFreePage != 10 {
			t.Errorf("Expected NextFreePage 10, got %d", radixPage.NextFreePage)
		}

		if radixPage.dirty {
			t.Error("Expected parsed page to not be dirty")
		}

		if radixPage.accessTime == 0 {
			t.Error("Expected access time to be set")
		}
	})

	t.Run("ParseRadixPageWrongType", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create test data with wrong content type
		data := make([]byte, PageSize)
		data[0] = ContentTypeLeaf     // Wrong type for radix page
		data[1] = 1
		binary.LittleEndian.PutUint32(data[2:6], 0)

		// Calculate and set checksum
		binary.BigEndian.PutUint32(data[6:10], 0)
		checksum := crc32.ChecksumIEEE(data)
		binary.BigEndian.PutUint32(data[6:10], checksum)

		// Parse should fail
		_, err = db.parseRadixPage(data, 1)
		if err == nil {
			t.Error("Expected error for wrong content type, got nil")
		}
		if !strings.Contains(err.Error(), "not a radix page") {
			t.Errorf("Expected 'not a radix page' error, got: %v", err)
		}
	})

	t.Run("ParseRadixPageBadChecksum", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Create test data with bad checksum
		data := make([]byte, PageSize)
		data[0] = ContentTypeRadix
		data[1] = 1
		binary.LittleEndian.PutUint32(data[2:6], 0)
		binary.BigEndian.PutUint32(data[6:10], 0xDEADBEEF) // Wrong checksum

		// Parse should fail
		_, err = db.parseRadixPage(data, 1)
		if err == nil {
			t.Error("Expected error for bad checksum, got nil")
		}
		if !strings.Contains(err.Error(), "checksum mismatch") {
			t.Errorf("Expected 'checksum mismatch' error, got: %v", err)
		}
	})

	t.Run("ParseRadixPageAddsToCache", func(t *testing.T) {
		// Create a test database
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile)

		db, err := Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		// Clear cache to start fresh
		initialCacheCount := db.totalCachePages.Load()

		// Create valid test data
		data := make([]byte, PageSize)
		data[0] = ContentTypeRadix
		data[1] = 2
		binary.LittleEndian.PutUint32(data[2:6], 0)

		// Calculate and set checksum
		binary.BigEndian.PutUint32(data[6:10], 0)
		checksum := crc32.ChecksumIEEE(data)
		binary.BigEndian.PutUint32(data[6:10], checksum)

		// Parse the page
		_, err = db.parseRadixPage(data, 15)
		if err != nil {
			t.Fatalf("Failed to parse radix page: %v", err)
		}

		// Verify page was added to cache
		newCacheCount := db.totalCachePages.Load()
		if newCacheCount <= initialCacheCount {
			t.Error("Expected cache count to increase after parsing page")
		}

		// Verify we can get the page from cache
		cachedPage, exists := db.getFromCache(15)
		if !exists {
			t.Error("Expected page to be in cache")
		}
		if cachedPage.pageType != ContentTypeRadix {
			t.Error("Expected cached page to be radix type")
		}
	})
}

// TestGetWritablePage tests the getWritablePage function
func TestGetWritablePage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_getwritablepage.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("AlreadyWritablePage", func(t *testing.T) {
		// Create a new page that should be writable (in current transaction)
		radixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix page: %v", err)
		}

		// Get a writable version - should be the same page
		writablePage, err := db.getWritablePage(radixPage)
		if err != nil {
			t.Fatalf("Failed to get writable page: %v", err)
		}

		// Verify it's the same page instance (no cloning needed)
		if writablePage != radixPage {
			t.Error("Expected same page instance for already writable page")
		}

		// Verify the page number remains the same
		if writablePage.pageNumber != radixPage.pageNumber {
			t.Errorf("Page number changed: expected %d, got %d", radixPage.pageNumber, writablePage.pageNumber)
		}

		// Verify transaction sequence is set
		if writablePage.txnSequence != db.txnSequence {
			t.Errorf("Transaction sequence not set correctly: expected %d, got %d", db.txnSequence, writablePage.txnSequence)
		}
	})

	t.Run("WALPageRequiresClone", func(t *testing.T) {
		// Create a new page
		radixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix page: %v", err)
		}

		// Mark it as part of WAL
		radixPage.isWAL = true

		// Get a writable version - should clone the page
		writablePage, err := db.getWritablePage(radixPage)
		if err != nil {
			t.Fatalf("Failed to get writable page: %v", err)
		}

		// Verify it's a different page instance (cloned)
		if writablePage == radixPage {
			t.Error("Expected different page instance for WAL page")
		}

		// Verify the page number remains the same
		if writablePage.pageNumber != radixPage.pageNumber {
			t.Errorf("Page number changed: expected %d, got %d", radixPage.pageNumber, writablePage.pageNumber)
		}

		// Verify the cloned page is not marked as WAL
		if writablePage.isWAL {
			t.Error("Cloned page should not be marked as WAL")
		}

		// Verify transaction sequence is set
		if writablePage.txnSequence != db.txnSequence {
			t.Errorf("Transaction sequence not set correctly: expected %d, got %d", db.txnSequence, writablePage.txnSequence)
		}

		// Verify both pages are in cache
		cachedPage, exists := db.getFromCache(radixPage.pageNumber)
		if !exists {
			t.Error("Page should exist in cache")
		}
		if cachedPage != writablePage {
			t.Error("Most recent page in cache should be the cloned page")
		}

		// Verify the original page is linked from the cloned page
		if cachedPage.next != radixPage {
			t.Error("Original page should be linked from cloned page")
		}
	})

	t.Run("OldTransactionRequiresClone", func(t *testing.T) {
		// Create a new page
		radixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix page: %v", err)
		}

		// Set an old transaction sequence
		radixPage.txnSequence = db.txnSequence - 1

		// Get a writable version - should clone the page
		writablePage, err := db.getWritablePage(radixPage)
		if err != nil {
			t.Fatalf("Failed to get writable page: %v", err)
		}

		// Verify it's a different page instance (cloned)
		if writablePage == radixPage {
			t.Error("Expected different page instance for page from older transaction")
		}

		// Verify the page number remains the same
		if writablePage.pageNumber != radixPage.pageNumber {
			t.Errorf("Page number changed: expected %d, got %d", radixPage.pageNumber, writablePage.pageNumber)
		}

		// Verify transaction sequence is updated
		if writablePage.txnSequence != db.txnSequence {
			t.Errorf("Transaction sequence not updated: expected %d, got %d", db.txnSequence, writablePage.txnSequence)
		}

		// Verify both pages are in cache
		cachedPage, exists := db.getFromCache(radixPage.pageNumber)
		if !exists {
			t.Error("Page should exist in cache")
		}
		if cachedPage != writablePage {
			t.Error("Most recent page in cache should be the cloned page")
		}

		// Verify the original page is linked from the cloned page
		if cachedPage.next != radixPage {
			t.Error("Original page should be linked from cloned page")
		}
	})

	t.Run("FlushSequenceRequiresClone", func(t *testing.T) {
		// Set a flush sequence
		db.flushSequence = db.txnSequence - 1

		// Create a new page
		radixPage, err := db.allocateRadixPage()
		if err != nil {
			t.Fatalf("Failed to allocate radix page: %v", err)
		}

		// Set transaction sequence to be <= flush sequence
		radixPage.txnSequence = db.flushSequence

		// Get a writable version - should clone the page
		writablePage, err := db.getWritablePage(radixPage)
		if err != nil {
			t.Fatalf("Failed to get writable page: %v", err)
		}

		// Verify it's a different page instance (cloned)
		if writablePage == radixPage {
			t.Error("Expected different page instance for page marked for flush")
		}

		// Verify the page number remains the same
		if writablePage.pageNumber != radixPage.pageNumber {
			t.Errorf("Page number changed: expected %d, got %d", radixPage.pageNumber, writablePage.pageNumber)
		}

		// Verify transaction sequence is updated
		if writablePage.txnSequence != db.txnSequence {
			t.Errorf("Transaction sequence not updated: expected %d, got %d", db.txnSequence, writablePage.txnSequence)
		}

		// Verify both pages are in cache
		cachedPage, exists := db.getFromCache(radixPage.pageNumber)
		if !exists {
			t.Error("Page should exist in cache")
		}
		if cachedPage != writablePage {
			t.Error("Most recent page in cache should be the cloned page")
		}

		// Reset flush sequence for other tests
		db.flushSequence = 0
	})
}

// TestCloneRadixPage tests the cloneRadixPage function
func TestCloneRadixPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_cloneradixpage.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.commitTransaction()

	// Create a radix page with some data
	originalPage, err := db.allocateRadixPage()
	if err != nil {
		t.Fatalf("Failed to allocate radix page: %v", err)
	}

	// Set some test values
	originalPage.SubPagesUsed = 2
	originalPage.NextFreePage = 42
	originalPage.dirty = true
	originalPage.isWAL = true
	originalPage.txnSequence = 100

	// Set some data in the page
	testByte := byte(123)
	originalPage.data[100] = testByte

	// Clone the page
	clonedPage, err := db.cloneRadixPage(originalPage)
	if err != nil {
		t.Fatalf("Failed to clone radix page: %v", err)
	}

	// Verify page number remains the same
	if clonedPage.pageNumber != originalPage.pageNumber {
		t.Errorf("Page number changed: expected %d, got %d", originalPage.pageNumber, clonedPage.pageNumber)
	}

	// Verify page type remains the same
	if clonedPage.pageType != originalPage.pageType {
		t.Errorf("Page type changed: expected %c, got %c", originalPage.pageType, clonedPage.pageType)
	}

	// Verify dirty flag is copied
	if clonedPage.dirty != originalPage.dirty {
		t.Errorf("Dirty flag not copied: expected %v, got %v", originalPage.dirty, clonedPage.dirty)
	}

	// Verify WAL flag is reset
	if clonedPage.isWAL {
		t.Error("WAL flag should be reset in cloned page")
	}

	// Verify transaction sequence is copied
	if clonedPage.txnSequence != originalPage.txnSequence {
		t.Errorf("Transaction sequence not copied: expected %d, got %d", originalPage.txnSequence, clonedPage.txnSequence)
	}

	// Verify access time is copied
	if clonedPage.accessTime != originalPage.accessTime {
		t.Errorf("Access time not copied: expected %d, got %d", originalPage.accessTime, clonedPage.accessTime)
	}

	// Verify SubPagesUsed is copied
	if clonedPage.SubPagesUsed != originalPage.SubPagesUsed {
		t.Errorf("SubPagesUsed not copied: expected %d, got %d", originalPage.SubPagesUsed, clonedPage.SubPagesUsed)
	}

	// Verify NextFreePage is copied
	if clonedPage.NextFreePage != originalPage.NextFreePage {
		t.Errorf("NextFreePage not copied: expected %d, got %d", originalPage.NextFreePage, clonedPage.NextFreePage)
	}

	// Verify data is copied (deep copy)
	if clonedPage.data[100] != testByte {
		t.Errorf("Data not copied correctly: expected %d, got %d", testByte, clonedPage.data[100])
	}

	// Verify modifying cloned page doesn't affect original
	clonedPage.data[100] = testByte + 1
	if originalPage.data[100] != testByte {
		t.Error("Modifying cloned page data affected original page")
	}

	// Verify both pages are in cache
	cachedPage, exists := db.getFromCache(originalPage.pageNumber)
	if !exists {
		t.Error("Page should exist in cache")
	}
	if cachedPage != clonedPage {
		t.Error("Most recent page in cache should be the cloned page")
	}

	// Verify the original page is linked from the cloned page
	if cachedPage.next != originalPage {
		t.Error("Original page should be linked from cloned page")
	}
}

// TestCloneLeafPage tests the cloneLeafPage function with the new multi-sub-page format
func TestCloneLeafPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_cloneleafpage.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction
	db.beginTransaction()
	defer db.commitTransaction()

	// Create a leaf page with some data using the new format
	originalPage, err := db.allocateLeafPage()
	if err != nil {
		t.Fatalf("Failed to allocate leaf page: %v", err)
	}

	// Add some sub-pages with entries to the page using the new format
	leafSubPage1, err := db.addEntryToNewLeafSubPage([]byte("test_suffix_1"), 1000)
	if err != nil {
		t.Fatalf("Failed to add first leaf sub-page: %v", err)
	}

	_, err = db.addEntryToNewLeafSubPage([]byte("test_suffix_2"), 2000)
	if err != nil {
		t.Fatalf("Failed to add second leaf sub-page: %v", err)
	}

	// Use the page from one of the sub-pages as our original page
	originalPage = leafSubPage1.Page

	// Set some test values
	originalPage.ContentSize = 100
	originalPage.dirty = true
	originalPage.isWAL = true
	originalPage.txnSequence = 100

	// Set some data in the page
	testByte := byte(123)
	originalPage.data[50] = testByte

	// Clone the page
	clonedPage, err := db.cloneLeafPage(originalPage)
	if err != nil {
		t.Fatalf("Failed to clone leaf page: %v", err)
	}

	// Verify page number remains the same
	if clonedPage.pageNumber != originalPage.pageNumber {
		t.Errorf("Page number changed: expected %d, got %d", originalPage.pageNumber, clonedPage.pageNumber)
	}

	// Verify page type remains the same
	if clonedPage.pageType != originalPage.pageType {
		t.Errorf("Page type changed: expected %c, got %c", originalPage.pageType, clonedPage.pageType)
	}

	// Verify dirty flag is copied
	if clonedPage.dirty != originalPage.dirty {
		t.Errorf("Dirty flag not copied: expected %v, got %v", originalPage.dirty, clonedPage.dirty)
	}

	// Verify WAL flag is reset
	if clonedPage.isWAL {
		t.Error("WAL flag should be reset in cloned page")
	}

	// Verify transaction sequence is copied
	if clonedPage.txnSequence != originalPage.txnSequence {
		t.Errorf("Transaction sequence not copied: expected %d, got %d", originalPage.txnSequence, clonedPage.txnSequence)
	}

	// Verify access time is copied
	if clonedPage.accessTime != originalPage.accessTime {
		t.Errorf("Access time not copied: expected %d, got %d", originalPage.accessTime, clonedPage.accessTime)
	}

	// Verify ContentSize is copied
	if clonedPage.ContentSize != originalPage.ContentSize {
		t.Errorf("ContentSize not copied: expected %d, got %d", originalPage.ContentSize, clonedPage.ContentSize)
	}

	// Verify SubPages array is properly initialized
	if len(clonedPage.SubPages) != 256 {
		t.Errorf("SubPages array not properly initialized: expected 256 slots, got %d", len(clonedPage.SubPages))
	}

	// Count non-nil sub-pages in both original and cloned pages
	originalSubPageCount := 0
	clonedSubPageCount := 0
	for i := 0; i < 256; i++ {
		if originalPage.SubPages[i] != nil {
			originalSubPageCount++
		}
		if clonedPage.SubPages[i] != nil {
			clonedSubPageCount++
		}
	}

	// Verify sub-pages are copied
	if clonedSubPageCount != originalSubPageCount {
		t.Errorf("Sub-pages not copied correctly: expected %d sub-pages, got %d", originalSubPageCount, clonedSubPageCount)
	}

	// Verify data is copied (deep copy)
	if clonedPage.data[50] != testByte {
		t.Errorf("Data not copied correctly: expected %d, got %d", testByte, clonedPage.data[50])
	}

	// Verify modifying cloned page doesn't affect original
	clonedPage.data[50] = testByte + 1
	if originalPage.data[50] != testByte {
		t.Error("Modifying cloned page data affected original page")
	}

	// Verify both pages are in cache
	cachedPage, exists := db.getFromCache(originalPage.pageNumber)
	if !exists {
		t.Error("Page should exist in cache")
	}
	if cachedPage != clonedPage {
		t.Error("Most recent page in cache should be the cloned page")
	}

	// Verify the original page is linked from the cloned page
	if cachedPage.next != originalPage {
		t.Error("Original page should be linked from cloned page")
	}

	// Check that sub-page data is correctly copied
	for i := 0; i < 256; i++ {
		originalSubPage := originalPage.SubPages[i]
		clonedSubPage := clonedPage.SubPages[i]

		// Both should be nil or both should be non-nil
		if (originalSubPage == nil) != (clonedSubPage == nil) {
			t.Errorf("Sub-page %d mismatch: original is nil=%v, cloned is nil=%v", i, originalSubPage == nil, clonedSubPage == nil)
			continue
		}

		if originalSubPage != nil && clonedSubPage != nil {
			// Verify sub-page metadata is copied
			if clonedSubPage.Offset != originalSubPage.Offset {
				t.Errorf("Sub-page %d offset not copied: expected %d, got %d", i, originalSubPage.Offset, clonedSubPage.Offset)
			}

			if clonedSubPage.Size != originalSubPage.Size {
				t.Errorf("Sub-page %d size not copied: expected %d, got %d", i, originalSubPage.Size, clonedSubPage.Size)
			}

			// Verify entries are copied correctly using getSubPageEntry
			entryCount := countSubPageEntries(db, originalPage, uint8(i))
			clonedEntryCount := countSubPageEntries(db, clonedPage, uint8(i))

			if clonedEntryCount != entryCount {
				t.Errorf("Sub-page %d entries not copied correctly: expected %d entries, got %d", i, entryCount, clonedEntryCount)
				continue
			}

			// Verify each entry's suffix content matches (not necessarily the data offset)
			for j := 0; j < entryCount; j++ {
				_, originalSuffix, originalFound := getSubPageEntry(db, originalPage, uint8(i), j)
				_, clonedSuffix, clonedFound := getSubPageEntry(db, clonedPage, uint8(i), j)

				if !originalFound || !clonedFound {
					t.Errorf("Sub-page %d entry %d not found: original=%v, cloned=%v", i, j, originalFound, clonedFound)
					continue
				}

				if !bytes.Equal(clonedSuffix, originalSuffix) {
					t.Errorf("Sub-page %d entry %d suffix not copied: expected %v, got %v", i, j, originalSuffix, clonedSuffix)
				}
			}
		}
	}
}

func TestMoveSubPageToNewLeafPage(t *testing.T) {
	// Create a temporary database file
	dbPath := createTempFile(t)
	defer os.Remove(dbPath)

	// Open the database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	db.beginTransaction()
	defer db.commitTransaction()

	// Create a leaf page with some existing entries
	leafPage, err := db.allocateLeafPage()
	if err != nil {
		t.Fatalf("Failed to allocate leaf page: %v", err)
	}

	// Create test data for the existing entries
	existingEntries := []struct {
		suffix     []byte
		dataOffset int64
	}{
		{[]byte("entry1"), 1000},
		{[]byte("entry2"), 2000},
		{[]byte("entry3"), 3000},
	}

	// Build the sub-page data manually
	subPageData := make([]byte, 0, 1000)
	var leafEntries []LeafEntry

	for _, entry := range existingEntries {
		// Write suffix length (varint)
		suffixLenBytes := make([]byte, 10)
		suffixLenSize := varint.Write(suffixLenBytes, uint64(len(entry.suffix)))

		// Calculate the offset for this entry within the sub-page data
		suffixOffset := len(subPageData) + suffixLenSize

		// Add the entry to our tracking
		leafEntries = append(leafEntries, LeafEntry{
			SuffixOffset: suffixOffset,
			SuffixLen:    len(entry.suffix),
			DataOffset:   entry.dataOffset,
		})

		// Add suffix length to sub-page data
		subPageData = append(subPageData, suffixLenBytes[:suffixLenSize]...)

		// Add suffix to sub-page data
		subPageData = append(subPageData, entry.suffix...)

		// Add data offset (8 bytes)
		offsetBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsetBytes, uint64(entry.dataOffset))
		subPageData = append(subPageData, offsetBytes...)
	}

	// Add the sub-page to the leaf page
	subPageIdx := uint8(0)
	offset := LeafHeaderSize

	// Write sub-page header
	leafPage.data[offset] = subPageIdx // Sub-page ID
	binary.LittleEndian.PutUint16(leafPage.data[offset+1:offset+3], uint16(len(subPageData))) // Size

	// Write sub-page data
	copy(leafPage.data[offset+3:], subPageData)

	// Update leaf page metadata
	totalSubPageSize := LeafSubPageHeaderSize + len(subPageData)
	leafPage.ContentSize = LeafHeaderSize + totalSubPageSize

	// Initialize SubPages array if needed
	if leafPage.SubPages == nil {
		leafPage.SubPages = make([]*LeafSubPageInfo, 256)
	}

	// Create the sub-page info
	leafPage.SubPages[subPageIdx] = &LeafSubPageInfo{
		Offset:  uint16(offset),
		Size:    uint16(len(subPageData)),
	}

	// Mark the page as dirty
	db.markPageDirty(leafPage)

	// Create a LeafSubPage struct
	originalSubPage := &LeafSubPage{
		Page:       leafPage,
		SubPageIdx: subPageIdx,
	}

	// Store original page number for verification
	originalPageNumber := leafPage.pageNumber

	// Test data for the new entry that will trigger the move
	newSuffix := []byte("new_entry_that_will_trigger_move")
	newDataOffset := int64(4000)

	// Get the original sub-page info for verification
	originalEntryCount := countSubPageEntries(db, leafPage, subPageIdx)

	// Call the function under test
	err = db.moveSubPageToNewLeafPage(originalSubPage, newSuffix, newDataOffset)
	if err != nil {
		t.Fatalf("moveSubPageToNewLeafPage failed: %v", err)
	}

	// Verify that the sub-page now points to a different page
	if originalSubPage.Page.pageNumber == originalPageNumber {
		t.Error("Expected sub-page to be moved to a new page, but it's still on the original page")
	}

	// Verify that the new page has the correct structure
	newPage := originalSubPage.Page
	newSubPageIdx := originalSubPage.SubPageIdx

	// Check that the new page has the sub-page
	if newPage.SubPages == nil || newPage.SubPages[newSubPageIdx] == nil {
		t.Fatal("New page should have the moved sub-page")
	}

	// Verify that the new sub-page has all the original entries plus the new one
	expectedEntryCount := originalEntryCount + 1
	entryCount := countSubPageEntries(db, newPage, newSubPageIdx)
	if entryCount != expectedEntryCount {
		t.Errorf("Expected %d entries in moved sub-page, got %d", expectedEntryCount, countSubPageEntries(db, newPage, newSubPageIdx))
	}

	// Verify that the new entry is present using findEntryInLeafSubPage
	_, _, dataOffset, err := db.findEntryInLeafSubPage(newPage, newSubPageIdx, newSuffix)
	if err != nil {
		t.Errorf("Failed to find entry: %v", err)
	}

	foundNewEntry := dataOffset != 0
	if !foundNewEntry {
		t.Error("Failed to find the new entry")
	} else if dataOffset != newDataOffset {
		t.Errorf("Expected data offset %d, got %d", newDataOffset, dataOffset)
	}

	// Verify that all original entries are still present
	for _, originalEntry := range existingEntries {
		// Use findEntryInLeafSubPage to find the entry by suffix
		_, _, dataOffset, err := db.findEntryInLeafSubPage(newPage, newSubPageIdx, originalEntry.suffix)
		if err != nil {
			t.Errorf("Failed to find entry: %v", err)
			continue
		}

		found := dataOffset != 0
		if !found {
			t.Errorf("Failed to find original entry with suffix %s", string(originalEntry.suffix))
		} else if dataOffset != originalEntry.dataOffset {
			t.Errorf("Expected data offset %d, got %d", originalEntry.dataOffset, dataOffset)
		}
	}

	// Verify that the original page no longer has the sub-page
	originalPage, err := db.getLeafPage(originalPageNumber)
	if err != nil {
		t.Fatalf("Failed to get original page: %v", err)
	}

	if originalPage.SubPages != nil && originalPage.SubPages[subPageIdx] != nil {
		t.Error("Original page should no longer have the moved sub-page")
	}

	// Verify that the new page is marked as dirty
	if !newPage.dirty {
		t.Error("New page should be marked as dirty")
	}

	// Verify that the new page has reasonable content size
	if newPage.ContentSize <= LeafHeaderSize {
		t.Errorf("New page content size should be greater than header size (%d), got %d", LeafHeaderSize, newPage.ContentSize)
	}

	// Verify that the new page is in the free list (it should have been added)
	// We can't directly test this without accessing internal state, but we can verify
	// that the page has reasonable free space
	freeSpace := PageSize - int(newPage.ContentSize)
	if freeSpace < PageSize/10 {
		t.Logf("New page has limited free space: %d bytes", freeSpace)
	}

	// Test error case: try to move a sub-page that doesn't exist
	invalidSubPage := &LeafSubPage{
		Page:       newPage,
		SubPageIdx: 255, // Invalid index
	}

	err = db.moveSubPageToNewLeafPage(invalidSubPage, []byte("test"), 5000)
	if err == nil {
		t.Error("Expected error when moving non-existent sub-page, but got nil")
	}

	if !strings.Contains(err.Error(), "sub-page with index 255 not found") {
		t.Errorf("Expected specific error message for non-existent sub-page, got: %v", err)
	}

	t.Logf("Successfully tested moveSubPageToNewLeafPage:")
	t.Logf("  - Original page: %d", originalPageNumber)
	t.Logf("  - New page: %d", newPage.pageNumber)
	t.Logf("  - Original entries: %d", originalEntryCount)
	t.Logf("  - Final entries: %d", countSubPageEntries(db, newPage, newSubPageIdx))
	t.Logf("  - New page content size: %d", newPage.ContentSize)
	t.Logf("  - New page free space: %d", freeSpace)
}
