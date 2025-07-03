package kv_log

import (
	"fmt"
	"runtime"
	"testing"
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
