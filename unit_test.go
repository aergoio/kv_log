package kv_log

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSet(t *testing.T) {
	//
}

func TestSlotDistribution(t *testing.T) {
	// Create a mock DB instance for testing
	db := &DB{
		mainIndexPages: 1, // Just need a single page for this test
	}

	// Number of slots to test
	const numSlots = MaxIndexEntries
	// Number of keys to generate
	const numKeys = 1000000

	// Create histograms for sequential and random keys
	seqHistogram := make([]int, numSlots)
	randHistogram := make([]int, numSlots)

	// Test with sequential keys
	fmt.Println("Testing with sequential keys...")
	for i := 0; i < numKeys; i++ {
		// Generate sequential key
		key := []byte(fmt.Sprintf("key-%d", i))
		// Calculate slot using the initial salt
		slot := db.getIndexSlot(key, InitialSalt)
		// Increment the histogram counter
		seqHistogram[slot]++
	}

	// Test with random keys
	fmt.Println("Testing with random keys...")
	for i := 0; i < numKeys; i++ {
		// Generate random key of random length (5-20 bytes)
		keyLen := rand.Intn(16) + 5
		key := make([]byte, keyLen)
		rand.Read(key)
		// Calculate slot using the initial salt
		slot := db.getIndexSlot(key, InitialSalt)
		// Increment the histogram counter
		randHistogram[slot]++
	}

	// Print statistics for sequential keys
	printHistogramStats(t, "Sequential keys", seqHistogram, numKeys)

	// Print statistics for random keys
	printHistogramStats(t, "Random keys", randHistogram, numKeys)
}

// printHistogramStats prints statistics about the histogram
func printHistogramStats(t *testing.T, title string, histogram []int, totalKeys int) {
	min := totalKeys
	max := 0
	sum := 0
	emptySlots := 0

	for _, count := range histogram {
		if count < min {
			min = count
		}
		if count > max {
			max = count
		}
		sum += count
		if count == 0 {
			emptySlots++
		}
	}

	avg := float64(sum) / float64(len(histogram))

	fmt.Printf("\n--- %s Distribution Statistics ---\n", title)
	fmt.Printf("Total keys: %d\n", totalKeys)
	fmt.Printf("Number of slots: %d\n", len(histogram))
	fmt.Printf("Min keys per slot: %d\n", min)
	fmt.Printf("Max keys per slot: %d\n", max)
	fmt.Printf("Avg keys per slot: %.2f\n", avg)
	fmt.Printf("Empty slots: %d (%.2f%%)\n", emptySlots, float64(emptySlots)*100/float64(len(histogram)))
	fmt.Printf("Load factor: %.2f%%\n", float64(sum)*100/float64(len(histogram)*max))

	// Check if distribution is reasonably uniform
	expectedPerSlot := totalKeys / len(histogram)
	maxDeviation := float64(max - expectedPerSlot) / float64(expectedPerSlot) * 100

	fmt.Printf("Max deviation from expected: %.2f%%\n", maxDeviation)

	if maxDeviation > 30 {
		t.Logf("Warning: %s distribution shows high deviation (%.2f%%)", title, maxDeviation)
	}

	// Print a simplified histogram visualization
	fmt.Println("\nSimplified histogram (each '*' represents 1% of total keys):")
	for i := 0; i < len(histogram); i += len(histogram)/16 {
		percentage := float64(histogram[i]) * 100 / float64(totalKeys)
		stars := int(percentage)
		fmt.Printf("Slot %3d: %s (%5.2f%%)\n", i, generateStars(stars), percentage)
	}
}

// generateStars returns a string with the specified number of stars
func generateStars(count int) string {
	if count > 50 {
		count = 50 // Cap for readability
	}
	stars := ""
	for i := 0; i < count; i++ {
		stars += "*"
	}
	return stars
}
