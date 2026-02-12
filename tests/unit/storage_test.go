package unit

import (
	"sync"
	"testing"

	"github.com/milossdjuric/logstream/internal/storage"
)

func TestMemoryLog_AppendAndRead(t *testing.T) {
	log := storage.NewMemoryLog()

	// Append some records
	data1 := []byte("record 1")
	data2 := []byte("record 2")
	data3 := []byte("record 3")

	offset1, err := log.Append(data1)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if offset1 != 0 {
		t.Errorf("Expected first offset to be 0, got %d", offset1)
	}

	offset2, err := log.Append(data2)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if offset2 != 1 {
		t.Errorf("Expected second offset to be 1, got %d", offset2)
	}

	offset3, err := log.Append(data3)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if offset3 != 2 {
		t.Errorf("Expected third offset to be 2, got %d", offset3)
	}

	// Read back records
	readData1, err := log.Read(0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(readData1) != "record 1" {
		t.Errorf("Expected 'record 1', got '%s'", readData1)
	}

	readData2, err := log.Read(1)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(readData2) != "record 2" {
		t.Errorf("Expected 'record 2', got '%s'", readData2)
	}

	readData3, err := log.Read(2)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(readData3) != "record 3" {
		t.Errorf("Expected 'record 3', got '%s'", readData3)
	}
}

func TestMemoryLog_ReadOutOfRange(t *testing.T) {
	log := storage.NewMemoryLog()

	// Try reading from empty log
	_, err := log.Read(0)
	if err == nil {
		t.Error("Expected error reading from empty log")
	}

	// Append one record
	log.Append([]byte("test"))

	// Try reading beyond end
	_, err = log.Read(1)
	if err == nil {
		t.Error("Expected error reading beyond end")
	}

	// Try reading negative (will be interpreted as large uint64)
	_, err = log.Read(^uint64(0))
	if err == nil {
		t.Error("Expected error for invalid offset")
	}
}

func TestMemoryLog_HighestOffset(t *testing.T) {
	log := storage.NewMemoryLog()

	// Empty log
	_, err := log.HighestOffset()
	if err == nil {
		t.Error("Expected error for empty log")
	}

	// Add records
	log.Append([]byte("1"))
	highest, err := log.HighestOffset()
	if err != nil {
		t.Fatalf("HighestOffset failed: %v", err)
	}
	if highest != 0 {
		t.Errorf("Expected highest offset 0, got %d", highest)
	}

	log.Append([]byte("2"))
	log.Append([]byte("3"))
	highest, err = log.HighestOffset()
	if err != nil {
		t.Fatalf("HighestOffset failed: %v", err)
	}
	if highest != 2 {
		t.Errorf("Expected highest offset 2, got %d", highest)
	}
}

func TestMemoryLog_LowestOffset(t *testing.T) {
	log := storage.NewMemoryLog()

	// Even empty log has base offset
	lowest := log.LowestOffset()
	if lowest != 0 {
		t.Errorf("Expected lowest offset 0, got %d", lowest)
	}

	// After adding records
	log.Append([]byte("1"))
	log.Append([]byte("2"))
	lowest = log.LowestOffset()
	if lowest != 0 {
		t.Errorf("Expected lowest offset still 0, got %d", lowest)
	}
}

func TestMemoryLog_TruncateTo(t *testing.T) {
	log := storage.NewMemoryLog()

	// Add 5 records
	for i := 0; i < 5; i++ {
		log.Append([]byte{byte(i)})
	}

	// Truncate to offset 2 (keep offsets 0, 1, 2)
	err := log.TruncateTo(2)
	if err != nil {
		t.Fatalf("TruncateTo failed: %v", err)
	}

	// Verify highest is now 2
	highest, err := log.HighestOffset()
	if err != nil {
		t.Fatalf("HighestOffset failed: %v", err)
	}
	if highest != 2 {
		t.Errorf("Expected highest offset 2 after truncate, got %d", highest)
	}

	// Verify offset 3 is gone
	_, err = log.Read(3)
	if err == nil {
		t.Error("Expected error reading truncated offset")
	}

	// Verify offset 2 still exists
	data, err := log.Read(2)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if data[0] != 2 {
		t.Errorf("Expected data[0]=2, got %d", data[0])
	}
}

func TestMemoryLog_TruncateToEmpty(t *testing.T) {
	log := storage.NewMemoryLog()

	// Truncate empty log should not panic
	err := log.TruncateTo(0)
	if err != nil {
		t.Fatalf("TruncateTo on empty log failed: %v", err)
	}

	// Add records
	log.Append([]byte("1"))
	log.Append([]byte("2"))

	// Truncate to below base offset (clear everything)
	err = log.TruncateTo(0)
	if err != nil {
		t.Fatalf("TruncateTo failed: %v", err)
	}

	// Should only have offset 0 now
	highest, _ := log.HighestOffset()
	if highest != 0 {
		t.Errorf("Expected highest offset 0, got %d", highest)
	}
}

func TestMemoryLog_Close(t *testing.T) {
	log := storage.NewMemoryLog()

	log.Append([]byte("test"))

	// Close should not fail for memory log
	err := log.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestMemoryLog_DataIsolation(t *testing.T) {
	log := storage.NewMemoryLog()

	// Append data
	original := []byte("original data")
	log.Append(original)

	// Modify original slice
	original[0] = 'X'

	// Read should return original data, not modified
	data, _ := log.Read(0)
	if data[0] == 'X' {
		t.Error("Log should copy data on append, not store reference")
	}

	// Modify read data
	data[0] = 'Y'

	// Re-read should still be original
	data2, _ := log.Read(0)
	if data2[0] == 'Y' {
		t.Error("Log should copy data on read, not return reference")
	}
}

func TestMemoryLog_ConcurrentAppend(t *testing.T) {
	log := storage.NewMemoryLog()
	numGoroutines := 10
	appendsPerGoroutine := 100

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < appendsPerGoroutine; j++ {
				_, err := log.Append([]byte{byte(id), byte(j)})
				if err != nil {
					t.Errorf("Concurrent append failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify total records
	highest, err := log.HighestOffset()
	if err != nil {
		t.Fatalf("HighestOffset failed: %v", err)
	}

	expectedCount := uint64(numGoroutines * appendsPerGoroutine)
	actualCount := highest + 1 // offset is 0-indexed
	if actualCount != expectedCount {
		t.Errorf("Expected %d records, got %d", expectedCount, actualCount)
	}
}

func TestMemoryLog_ConcurrentReadWrite(t *testing.T) {
	log := storage.NewMemoryLog()

	// Pre-populate
	for i := 0; i < 100; i++ {
		log.Append([]byte{byte(i)})
	}

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				log.Append([]byte{byte(j)})
			}
		}()
	}

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				// Read from existing offsets
				log.Read(uint64(j % 100))
			}
		}()
	}

	wg.Wait()

	// Should not panic or deadlock
	t.Log("Concurrent read/write completed without issues")
}

func TestMemoryLog_LargeData(t *testing.T) {
	log := storage.NewMemoryLog()

	// Append 1MB of data
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	offset, err := log.Append(largeData)
	if err != nil {
		t.Fatalf("Append large data failed: %v", err)
	}

	// Read it back
	readData, err := log.Read(offset)
	if err != nil {
		t.Fatalf("Read large data failed: %v", err)
	}

	if len(readData) != len(largeData) {
		t.Errorf("Expected %d bytes, got %d", len(largeData), len(readData))
	}

	// Verify content
	for i := 0; i < 100; i++ { // Spot check
		if readData[i] != largeData[i] {
			t.Errorf("Data mismatch at byte %d", i)
			break
		}
	}
}

func BenchmarkMemoryLog_Append(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Append(data)
	}
}

func BenchmarkMemoryLog_Read(b *testing.B) {
	log := storage.NewMemoryLog()
	data := make([]byte, 1024)

	for i := 0; i < 10000; i++ {
		log.Append(data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Read(uint64(i % 10000))
	}
}
