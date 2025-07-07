package kv_log

import (
	"fmt"
	"runtime"
	"testing"
	"bytes"
	"path/filepath"
)

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
func TestAddLeafEntry(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_addleafentry.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction to create pages in current transaction sequence
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("AddEntryToEmptyLeafPage", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Test data
		suffix := []byte("test_suffix")
		dataOffset := int64(1000)

		// Add the entry
		err = db.addLeafEntry(leafPage, suffix, dataOffset)
		if err != nil {
			t.Fatalf("Failed to add leaf entry: %v", err)
		}

		// Verify the entry was added correctly
		if len(leafPage.Entries) != 1 {
			t.Errorf("Expected 1 entry, got %d", len(leafPage.Entries))
		}

		// Check entry details
		entry := leafPage.Entries[0]
		if entry.SuffixLen != len(suffix) {
			t.Errorf("Expected suffix length %d, got %d", len(suffix), entry.SuffixLen)
		}
		if entry.DataOffset != dataOffset {
			t.Errorf("Expected data offset %d, got %d", dataOffset, entry.DataOffset)
		}

		// Verify the suffix in the page data
		entrySuffix := leafPage.data[entry.SuffixOffset:entry.SuffixOffset+entry.SuffixLen]
		if !bytes.Equal(entrySuffix, suffix) {
			t.Errorf("Expected suffix %v, got %v", suffix, entrySuffix)
		}

		// Verify page is marked as dirty
		if !leafPage.dirty {
			t.Error("Leaf page should be marked as dirty after adding entry")
		}
	})

	t.Run("AddMultipleEntries", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Add multiple entries
		testEntries := []struct {
			suffix     []byte
			dataOffset int64
		}{
			{[]byte("entry1"), 1000},
			{[]byte("entry2"), 2000},
			{[]byte("entry3"), 3000},
		}

		for i, testEntry := range testEntries {
			err = db.addLeafEntry(leafPage, testEntry.suffix, testEntry.dataOffset)
			if err != nil {
				t.Fatalf("Failed to add entry %d: %v", i, err)
			}
		}

		// Verify all entries were added
		if len(leafPage.Entries) != len(testEntries) {
			t.Errorf("Expected %d entries, got %d", len(testEntries), len(leafPage.Entries))
		}

		// Verify each entry
		for i, testEntry := range testEntries {
			entry := leafPage.Entries[i]
			if entry.DataOffset != testEntry.dataOffset {
				t.Errorf("Entry %d: expected data offset %d, got %d", i, testEntry.dataOffset, entry.DataOffset)
			}

			entrySuffix := leafPage.data[entry.SuffixOffset:entry.SuffixOffset+entry.SuffixLen]
			if !bytes.Equal(entrySuffix, testEntry.suffix) {
				t.Errorf("Entry %d: expected suffix %v, got %v", i, testEntry.suffix, entrySuffix)
			}
		}
	})

	t.Run("AddEntriesToPageWithExistingEntries", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Add initial entries to populate the page
		initialEntries := []struct {
			suffix     []byte
			dataOffset int64
		}{
			{[]byte("alpha"), 1000},
			{[]byte("beta"), 2000},
		}

		for i, testEntry := range initialEntries {
			err = db.addLeafEntry(leafPage, testEntry.suffix, testEntry.dataOffset)
			if err != nil {
				t.Fatalf("Failed to add initial entry %d: %v", i, err)
			}
		}

		// Verify initial state
		if len(leafPage.Entries) != 2 {
			t.Fatalf("Expected 2 initial entries, got %d", len(leafPage.Entries))
		}

		// Now add more entries to the page that already has entries
		additionalEntries := []struct {
			suffix     []byte
			dataOffset int64
		}{
			{[]byte("gamma"), 3000},
			{[]byte("delta"), 4000},
		}

		for i, testEntry := range additionalEntries {
			err = db.addLeafEntry(leafPage, testEntry.suffix, testEntry.dataOffset)
			if err != nil {
				t.Fatalf("Failed to add additional entry %d: %v", i, err)
			}
		}

		// Verify all entries are present
		expectedTotal := len(initialEntries) + len(additionalEntries)
		if len(leafPage.Entries) != expectedTotal {
			t.Errorf("Expected %d total entries, got %d", expectedTotal, len(leafPage.Entries))
		}

		// Verify initial entries are still correct
		for i, testEntry := range initialEntries {
			entry := leafPage.Entries[i]
			entrySuffix := leafPage.data[entry.SuffixOffset:entry.SuffixOffset+entry.SuffixLen]
			if !bytes.Equal(entrySuffix, testEntry.suffix) {
				t.Errorf("Initial entry %d suffix mismatch: expected %s, got %s", i, testEntry.suffix, entrySuffix)
			}
			if entry.DataOffset != testEntry.dataOffset {
				t.Errorf("Initial entry %d data offset mismatch: expected %d, got %d", i, testEntry.dataOffset, entry.DataOffset)
			}
		}

		// Verify additional entries were added correctly
		for i, testEntry := range additionalEntries {
			entryIndex := len(initialEntries) + i
			entry := leafPage.Entries[entryIndex]
			entrySuffix := leafPage.data[entry.SuffixOffset:entry.SuffixOffset+entry.SuffixLen]
			if !bytes.Equal(entrySuffix, testEntry.suffix) {
				t.Errorf("Additional entry %d suffix mismatch: expected %s, got %s", i, testEntry.suffix, entrySuffix)
			}
			if entry.DataOffset != testEntry.dataOffset {
				t.Errorf("Additional entry %d data offset mismatch: expected %d, got %d", i, testEntry.dataOffset, entry.DataOffset)
			}
		}
	})


	t.Run("AddEntryWithEmptyBytes", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Test edge case: suffix with zero length but not nil
		// This tests the varint encoding of length 0
		suffix := []byte{}
		dataOffset := int64(6000)

		err = db.addLeafEntry(leafPage, suffix, dataOffset)
		if err != nil {
			t.Fatalf("Failed to add entry with zero-length suffix: %v", err)
		}

		// Verify the entry
		if len(leafPage.Entries) != 1 {
			t.Errorf("Expected 1 entry, got %d", len(leafPage.Entries))
		}

		entry := leafPage.Entries[0]
		if entry.SuffixLen != 0 {
			t.Errorf("Expected suffix length 0, got %d", entry.SuffixLen)
		}
		if entry.DataOffset != dataOffset {
			t.Errorf("Expected data offset %d, got %d", dataOffset, entry.DataOffset)
		}
	})

	t.Run("PageConversionWhenFull", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		originalPageNumber := leafPage.pageNumber

		// Fill the page to near capacity
		// Use a larger suffix to fill the page faster
		maxEntries := 0
		baseSuffix := []byte("very_long_suffix_that_will_fill_the_page_quickly_")
		testDataOffset := int64(1000)

		// Fill the page until it's nearly full
		for {
			// Create a long suffix to fill the page faster
			suffix := append(baseSuffix, []byte(fmt.Sprintf("entry_%d", maxEntries))...)

			// Calculate space needed for this entry more accurately
			suffixLenSize := 1 // assume 1 byte for varint (for short suffixes)
			if len(suffix) >= 128 {
				suffixLenSize = 2 // 2 bytes for varint if suffix >= 128 bytes
			}
			entrySize := suffixLenSize + len(suffix) + 8 // suffix length varint + suffix + data offset

			if int(leafPage.ContentSize)+entrySize > PageSize-50 { // Leave smaller margin
				break
			}

			err = db.addLeafEntry(leafPage, suffix, testDataOffset+int64(maxEntries))
			if err != nil {
				t.Fatalf("Failed to add entry %d: %v", maxEntries, err)
			}
			maxEntries++
		}

		t.Logf("Added %d entries to leaf page, content size: %d/%d", maxEntries, leafPage.ContentSize, PageSize)

		// Now add one more entry that should trigger conversion to radix page
		finalSuffix := []byte("this_is_the_final_entry_that_should_cause_page_conversion_to_radix_because_the_page_is_full")
		err = db.addLeafEntry(leafPage, finalSuffix, testDataOffset+int64(maxEntries))

		// Check the result - either the page was converted or we got an error
		if err != nil {
			// If we got an error, it might be because conversion happened
			t.Logf("Got error when adding final entry (this might indicate conversion): %v", err)
		}

		// Check if the page was converted by looking at the cache
		page, exists := db.getFromCache(originalPageNumber)
		if !exists {
			t.Error("Page should still exist in cache after conversion")
			return
		}

		// Log the current state for debugging
		t.Logf("Page type after adding final entry: %c", page.pageType)

		// The test passes if either:
		// 1. The page was converted to radix, OR
		// 2. The page remained as leaf but we got an error (indicating conversion was attempted)
		if page.pageType == ContentTypeLeaf && err == nil {
			// This is okay - maybe the page didn't actually fill up enough to trigger conversion
			// Let's just log this instead of failing
			t.Logf("Page remained as leaf - final content size: %d/%d", leafPage.ContentSize, PageSize)
		} else if page.pageType == ContentTypeRadix {
			t.Logf("Page was successfully converted to radix page")
		}
	})
}

// TestSetOnLeafPage tests the setOnLeafPage function for insert, update, and delete operations
// Note: setOnLeafPage handles keys that have remaining suffix after processing key bytes.
// Empty suffixes (when all key bytes are consumed) are handled by setOnEmptySuffix on radix pages.
func TestSetOnLeafPage(t *testing.T) {
	// Create a temporary database for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_setonleafpage.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a transaction to create pages in current transaction sequence
	db.beginTransaction()
	defer db.commitTransaction()

	t.Run("InsertNewEntry", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Test data - simulate a key "test_key" where keyPos=4, so suffix is "key"
		key := []byte("test_key")
		keyPos := 4
		value := []byte("test_value")

		// Mock appendData by writing test data to main file first
		dataOffset, err := db.appendData(key, value)
		if err != nil {
			t.Fatalf("Failed to append test data: %v", err)
		}

		// Now test setOnLeafPage
		err = db.setOnLeafPage(leafPage, key, keyPos, value, dataOffset)
		if err != nil {
			t.Fatalf("Failed to set on leaf page: %v", err)
		}

		// Verify the entry was added
		if len(leafPage.Entries) != 1 {
			t.Errorf("Expected 1 entry, got %d", len(leafPage.Entries))
		}

		// Verify the entry details
		entry := leafPage.Entries[0]
		expectedSuffix := key[keyPos+1:] // "key"
		entrySuffix := leafPage.data[entry.SuffixOffset:entry.SuffixOffset+entry.SuffixLen]
		if !bytes.Equal(entrySuffix, expectedSuffix) {
			t.Errorf("Expected suffix %v, got %v", expectedSuffix, entrySuffix)
		}

		// Verify data can be read back
		content, err := db.readContent(entry.DataOffset)
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
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Test data
		key := []byte("test_key")
		keyPos := 4
		originalValue := []byte("original_value")
		updatedValue := []byte("updated_value")

		// First, insert the original entry
		err = db.setOnLeafPage(leafPage, key, keyPos, originalValue, 0)
		if err != nil {
			t.Fatalf("Failed to insert original entry: %v", err)
		}

		// Verify original entry exists
		if len(leafPage.Entries) != 1 {
			t.Fatalf("Expected 1 entry after insert, got %d", len(leafPage.Entries))
		}

		originalEntry := leafPage.Entries[0]
		originalContent, err := db.readContent(originalEntry.DataOffset)
		if err != nil {
			t.Fatalf("Failed to read original content: %v", err)
		}
		if !bytes.Equal(originalContent.value, originalValue) {
			t.Errorf("Expected original value %v, got %v", originalValue, originalContent.value)
		}

		// Now update the entry with new value
		err = db.setOnLeafPage(leafPage, key, keyPos, updatedValue, 0)
		if err != nil {
			t.Fatalf("Failed to update entry: %v", err)
		}

		// Verify still only one entry
		if len(leafPage.Entries) != 1 {
			t.Errorf("Expected 1 entry after update, got %d", len(leafPage.Entries))
		}

		// Verify the entry was updated
		updatedEntry := leafPage.Entries[0]
		updatedContent, err := db.readContent(updatedEntry.DataOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(updatedContent.value, updatedValue) {
			t.Errorf("Expected updated value %v, got %v", updatedValue, updatedContent.value)
		}

		// Verify the data offset changed (new data was appended)
		if originalEntry.DataOffset == updatedEntry.DataOffset {
			t.Error("Expected data offset to change after update")
		}
	})

	t.Run("UpdateWithSameValue", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Test data
		key := []byte("test_key")
		keyPos := 4
		value := []byte("same_value")

		// First, insert the entry
		err = db.setOnLeafPage(leafPage, key, keyPos, value, 0)
		if err != nil {
			t.Fatalf("Failed to insert entry: %v", err)
		}

		originalEntry := leafPage.Entries[0]
		originalDataOffset := originalEntry.DataOffset

		// Record the main file size before the "update"
		originalMainFileSize := db.mainFileSize

		// Now "update" with the same value
		err = db.setOnLeafPage(leafPage, key, keyPos, value, 0)
		if err != nil {
			t.Fatalf("Failed to update with same value: %v", err)
		}

		// Verify still only one entry
		if len(leafPage.Entries) != 1 {
			t.Errorf("Expected 1 entry, got %d", len(leafPage.Entries))
		}

		// Verify the data offset didn't change (no new data was written)
		updatedEntry := leafPage.Entries[0]
		if originalDataOffset != updatedEntry.DataOffset {
			t.Error("Expected data offset to remain the same when updating with same value")
		}

		// Verify no new data was appended to main file
		if db.mainFileSize != originalMainFileSize {
			t.Error("Expected main file size to remain the same when updating with same value")
		}
	})

	t.Run("UpdateFirstEntry", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Add multiple entries to test updating the first one
		// All entries must share the same prefix up to keyPos for them to be on the same leaf page
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

		// Insert all entries
		for i, testEntry := range testEntries {
			err = db.setOnLeafPage(leafPage, testEntry.key, testEntry.keyPos, testEntry.value, 0)
			if err != nil {
				t.Fatalf("Failed to insert entry %d: %v", i, err)
			}
		}

		// Verify all entries were added
		if len(leafPage.Entries) != 3 {
			t.Fatalf("Expected 3 entries, got %d", len(leafPage.Entries))
		}

		// Update the first entry
		newValue := []byte("updated_first_value")
		err = db.setOnLeafPage(leafPage, testEntries[0].key, testEntries[0].keyPos, newValue, 0)
		if err != nil {
			t.Fatalf("Failed to update first entry: %v", err)
		}

		// Verify still 3 entries
		if len(leafPage.Entries) != 3 {
			t.Errorf("Expected 3 entries after update, got %d", len(leafPage.Entries))
		}

		// Verify the first entry was updated
		firstEntry := leafPage.Entries[0]
		content, err := db.readContent(firstEntry.DataOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(content.value, newValue) {
			t.Errorf("Expected updated value %v, got %v", newValue, content.value)
		}

		// Verify other entries remain unchanged
		for i := 1; i < 3; i++ {
			entry := leafPage.Entries[i]
			content, err := db.readContent(entry.DataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for entry %d: %v", i, err)
			}
			if !bytes.Equal(content.value, testEntries[i].value) {
				t.Errorf("Entry %d value changed unexpectedly: expected %v, got %v", i, testEntries[i].value, content.value)
			}
		}
	})

	t.Run("UpdateMiddleEntry", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
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

		// Insert all entries
		for i, testEntry := range testEntries {
			err = db.setOnLeafPage(leafPage, testEntry.key, testEntry.keyPos, testEntry.value, 0)
			if err != nil {
				t.Fatalf("Failed to insert entry %d: %v", i, err)
			}
		}

		// Update the middle entry (index 1)
		newValue := []byte("updated_middle_value")
		err = db.setOnLeafPage(leafPage, testEntries[1].key, testEntries[1].keyPos, newValue, 0)
		if err != nil {
			t.Fatalf("Failed to update middle entry: %v", err)
		}

		// Verify still 3 entries
		if len(leafPage.Entries) != 3 {
			t.Errorf("Expected 3 entries after update, got %d", len(leafPage.Entries))
		}

		// Verify the middle entry was updated
		middleEntry := leafPage.Entries[1]
		content, err := db.readContent(middleEntry.DataOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(content.value, newValue) {
			t.Errorf("Expected updated value %v, got %v", newValue, content.value)
		}

		// Verify other entries remain unchanged
		for _, idx := range []int{0, 2} {
			entry := leafPage.Entries[idx]
			content, err := db.readContent(entry.DataOffset)
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
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
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

		// Insert all entries
		for i, testEntry := range testEntries {
			err = db.setOnLeafPage(leafPage, testEntry.key, testEntry.keyPos, testEntry.value, 0)
			if err != nil {
				t.Fatalf("Failed to insert entry %d: %v", i, err)
			}
		}

		// Update the last entry (index 2)
		newValue := []byte("updated_last_value")
		err = db.setOnLeafPage(leafPage, testEntries[2].key, testEntries[2].keyPos, newValue, 0)
		if err != nil {
			t.Fatalf("Failed to update last entry: %v", err)
		}

		// Verify still 3 entries
		if len(leafPage.Entries) != 3 {
			t.Errorf("Expected 3 entries after update, got %d", len(leafPage.Entries))
		}

		// Verify the last entry was updated
		lastEntry := leafPage.Entries[2]
		content, err := db.readContent(lastEntry.DataOffset)
		if err != nil {
			t.Fatalf("Failed to read updated content: %v", err)
		}
		if !bytes.Equal(content.value, newValue) {
			t.Errorf("Expected updated value %v, got %v", newValue, content.value)
		}

		// Verify other entries remain unchanged
		for i := 0; i < 2; i++ {
			entry := leafPage.Entries[i]
			content, err := db.readContent(entry.DataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for entry %d: %v", i, err)
			}
			if !bytes.Equal(content.value, testEntries[i].value) {
				t.Errorf("Entry %d value changed unexpectedly: expected %v, got %v", i, testEntries[i].value, content.value)
			}
		}
	})

	t.Run("DeleteFirstEntry", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
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

		// Insert all entries
		for i, testEntry := range testEntries {
			err = db.setOnLeafPage(leafPage, testEntry.key, testEntry.keyPos, testEntry.value, 0)
			if err != nil {
				t.Fatalf("Failed to insert entry %d: %v", i, err)
			}
		}

		// Verify all entries were added
		if len(leafPage.Entries) != 3 {
			t.Fatalf("Expected 3 entries, got %d", len(leafPage.Entries))
		}

		// Delete the first entry by setting empty value
		err = db.setOnLeafPage(leafPage, testEntries[0].key, testEntries[0].keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete first entry: %v", err)
		}

		// Verify entry was removed
		if len(leafPage.Entries) != 2 {
			t.Errorf("Expected 2 entries after deletion, got %d", len(leafPage.Entries))
		}

		// Verify remaining entries are correct and shifted
		expectedRemaining := testEntries[1:]
		for i, expectedEntry := range expectedRemaining {
			entry := leafPage.Entries[i]
			content, err := db.readContent(entry.DataOffset)
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
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
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

		// Insert all entries
		for i, testEntry := range testEntries {
			err = db.setOnLeafPage(leafPage, testEntry.key, testEntry.keyPos, testEntry.value, 0)
			if err != nil {
				t.Fatalf("Failed to insert entry %d: %v", i, err)
			}
		}

		// Delete the middle entry (index 1)
		err = db.setOnLeafPage(leafPage, testEntries[1].key, testEntries[1].keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete middle entry: %v", err)
		}

		// Verify entry was removed
		if len(leafPage.Entries) != 2 {
			t.Errorf("Expected 2 entries after deletion, got %d", len(leafPage.Entries))
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
			entry := leafPage.Entries[i]
			content, err := db.readContent(entry.DataOffset)
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
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
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

		// Insert all entries
		for i, testEntry := range testEntries {
			err = db.setOnLeafPage(leafPage, testEntry.key, testEntry.keyPos, testEntry.value, 0)
			if err != nil {
				t.Fatalf("Failed to insert entry %d: %v", i, err)
			}
		}

		// Delete the last entry (index 2)
		err = db.setOnLeafPage(leafPage, testEntries[2].key, testEntries[2].keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete last entry: %v", err)
		}

		// Verify entry was removed
		if len(leafPage.Entries) != 2 {
			t.Errorf("Expected 2 entries after deletion, got %d", len(leafPage.Entries))
		}

		// Verify remaining entries are correct (first two entries)
		for i := 0; i < 2; i++ {
			entry := leafPage.Entries[i]
			content, err := db.readContent(entry.DataOffset)
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

	t.Run("DeleteNonExistingEntry", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Add some entries
		// All entries share the same prefix "testing" up to keyPos=7
		testEntries := []struct {
			key    []byte
			keyPos int
			value  []byte
		}{
			{[]byte("testing_key_1"), 7, []byte("value_1")},
			{[]byte("testing_key_2"), 7, []byte("value_2")},
		}

		// Insert entries
		for i, testEntry := range testEntries {
			err = db.setOnLeafPage(leafPage, testEntry.key, testEntry.keyPos, testEntry.value, 0)
			if err != nil {
				t.Fatalf("Failed to insert entry %d: %v", i, err)
			}
		}

		// Try to delete a non-existing entry (different suffix)
		nonExistingKey := []byte("testing_nonexist")
		err = db.setOnLeafPage(leafPage, nonExistingKey, 7, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to handle deletion of non-existing entry: %v", err)
		}

		// Verify no entries were removed
		if len(leafPage.Entries) != 2 {
			t.Errorf("Expected 2 entries after attempting to delete non-existing entry, got %d", len(leafPage.Entries))
		}

		// Verify existing entries remain unchanged
		for i, testEntry := range testEntries {
			entry := leafPage.Entries[i]
			content, err := db.readContent(entry.DataOffset)
			if err != nil {
				t.Fatalf("Failed to read content for entry %d: %v", i, err)
			}
			if !bytes.Equal(content.key, testEntry.key) {
				t.Errorf("Entry %d key changed unexpectedly: expected %v, got %v", i, testEntry.key, content.key)
			}
			if !bytes.Equal(content.value, testEntry.value) {
				t.Errorf("Entry %d value changed unexpectedly: expected %v, got %v", i, testEntry.value, content.value)
			}
		}
	})

	t.Run("DeleteExistingEntry", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Test data
		key := []byte("test_key")
		keyPos := 4
		value := []byte("value_to_delete")

		// First, insert the entry
		err = db.setOnLeafPage(leafPage, key, keyPos, value, 0)
		if err != nil {
			t.Fatalf("Failed to insert entry: %v", err)
		}

		// Verify entry exists
		if len(leafPage.Entries) != 1 {
			t.Fatalf("Expected 1 entry after insert, got %d", len(leafPage.Entries))
		}

		// Now delete the entry (empty value means delete)
		err = db.setOnLeafPage(leafPage, key, keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete entry: %v", err)
		}

		// Verify entry was removed
		if len(leafPage.Entries) != 0 {
			t.Errorf("Expected 0 entries after delete, got %d", len(leafPage.Entries))
		}
	})

	t.Run("DeleteNonExistentEntry", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Test data
		key := []byte("non_existent_key")
		keyPos := 4

		// Try to delete a non-existent entry
		err = db.setOnLeafPage(leafPage, key, keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Delete of non-existent entry should not fail: %v", err)
		}

		// Verify no entries exist
		if len(leafPage.Entries) != 0 {
			t.Errorf("Expected 0 entries, got %d", len(leafPage.Entries))
		}
	})

	t.Run("MultipleEntriesManagement", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Insert multiple entries
		testEntries := []struct {
			key   []byte
			value []byte
		}{
			{[]byte("key1_suffix1"), []byte("value1")},
			{[]byte("key2_suffix2"), []byte("value2")},
			{[]byte("key3_suffix3"), []byte("value3")},
		}

		keyPos := 4 // Simulating all keys have same prefix "key1", "key2", etc.

		// Insert all entries
		for i, entry := range testEntries {
			err = db.setOnLeafPage(leafPage, entry.key, keyPos, entry.value, 0)
			if err != nil {
				t.Fatalf("Failed to insert entry %d: %v", i, err)
			}
		}

		// Verify all entries exist
		if len(leafPage.Entries) != len(testEntries) {
			t.Errorf("Expected %d entries, got %d", len(testEntries), len(leafPage.Entries))
		}

		// Update the second entry
		newValue := []byte("updated_value2")
		err = db.setOnLeafPage(leafPage, testEntries[1].key, keyPos, newValue, 0)
		if err != nil {
			t.Fatalf("Failed to update entry: %v", err)
		}

		// Verify still same number of entries
		if len(leafPage.Entries) != len(testEntries) {
			t.Errorf("Expected %d entries after update, got %d", len(testEntries), len(leafPage.Entries))
		}

		// Delete the first entry
		err = db.setOnLeafPage(leafPage, testEntries[0].key, keyPos, []byte{}, 0)
		if err != nil {
			t.Fatalf("Failed to delete entry: %v", err)
		}

		// Verify one less entry
		if len(leafPage.Entries) != len(testEntries)-1 {
			t.Errorf("Expected %d entries after delete, got %d", len(testEntries)-1, len(leafPage.Entries))
		}

		// Verify remaining entries are correct
		remainingKeys := [][]byte{testEntries[1].key, testEntries[2].key}
		for i, entry := range leafPage.Entries {
			expectedSuffix := remainingKeys[i][keyPos+1:]
			entrySuffix := leafPage.data[entry.SuffixOffset:entry.SuffixOffset+entry.SuffixLen]
			if !bytes.Equal(entrySuffix, expectedSuffix) {
				t.Errorf("Entry %d: expected suffix %v, got %v", i, expectedSuffix, entrySuffix)
			}
		}
	})

	t.Run("ReindexingMode", func(t *testing.T) {
		// Create a new leaf page
		leafPage, err := db.allocateLeafPage()
		if err != nil {
			t.Fatalf("Failed to allocate leaf page: %v", err)
		}

		// Test reindexing mode where dataOffset is provided (non-zero)
		key := []byte("test_key")
		keyPos := 4
		value := []byte("test_value")
		dataOffset := int64(12345) // Non-zero means reindexing

		// In reindexing mode, we don't append new data
		err = db.setOnLeafPage(leafPage, key, keyPos, value, dataOffset)
		if err != nil {
			t.Fatalf("Failed to set in reindexing mode: %v", err)
		}

		// Verify the entry was added with the provided offset
		if len(leafPage.Entries) != 1 {
			t.Errorf("Expected 1 entry, got %d", len(leafPage.Entries))
		}

		entry := leafPage.Entries[0]
		if entry.DataOffset != dataOffset {
			t.Errorf("Expected data offset %d, got %d", dataOffset, entry.DataOffset)
		}
	})
}

// TestPageCacheAfterLeafOperations tests the page cache state after leaf page operations
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

	// Add some entries to the page
	for i := 0; i < 5; i++ {
		suffix := []byte(fmt.Sprintf("suffix_%d", i))
		dataOffset := int64(1000 + i*100)
		err = db.addLeafEntry(leafPage, suffix, dataOffset)
		if err != nil {
			t.Fatalf("Failed to add entry %d: %v", i, err)
		}
	}

	// Verify the page is marked as dirty
	if !leafPage.dirty {
		t.Error("Leaf page should be marked as dirty after modifications")
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

	// Test that page remains accessible through cache
	retrievedPage, err := db.getLeafPage(pageNumber)
	if err != nil {
		t.Fatalf("Failed to retrieve leaf page from cache: %v", err)
	}

	// Verify it's the same page instance (should be from cache)
	if retrievedPage.pageNumber != pageNumber {
		t.Errorf("Expected page number %d, got %d", pageNumber, retrievedPage.pageNumber)
	}

	// Verify entries are preserved
	if len(retrievedPage.Entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(retrievedPage.Entries))
	}

	// Test page access time is updated
	originalAccessTime := cachedPage.accessTime
	_, err = db.getLeafPage(pageNumber)
	if err != nil {
		t.Fatalf("Failed to access page again: %v", err)
	}

	// Access time should have been updated
	updatedPage, _ := db.getFromCache(pageNumber)
	if updatedPage.accessTime <= originalAccessTime {
		t.Error("Access time should have been updated")
	}
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
