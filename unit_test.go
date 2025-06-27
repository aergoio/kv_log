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
