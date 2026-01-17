//script to read logs from the logs directory

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/milossdjuric/logstream/internal/storage"
)

func main() {
	// Scan for subdirectories in ./logs or use the first one found
	baseDir := "./logs"
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		log.Fatal(err)
	}

	var logDir string
	for _, e := range entries {
		if e.IsDir() {
			logDir = filepath.Join(baseDir, e.Name())
			fmt.Printf("Reading from log directory: %s\n", logDir)
			break // just read the first one for now
		}
	}
	if logDir == "" {
		fmt.Println("No log directories found in ./logs")
		return
	}

	l, err := storage.NewLog(logDir, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	start := l.LowestOffset()
	end, err := l.HighestOffset()
	if err != nil {
		fmt.Println("Log is empty or error reading highest offset:", err)
		return
	}

	fmt.Printf("Reading logs from offset %d to %d...\n", start, end)

	for i := start; i <= end; i++ {
		raw, err := l.Read(i)
		if err != nil {
			fmt.Printf("Error reading offset %d: %v\n", i, err)
			continue
		}

		ts, data, err := storage.DecodeRecord(raw)
		if err != nil {
			fmt.Printf("[%d] Error decoding record: %v\n", i, err)
			continue
		}

		// Format timestamp
		t := time.Unix(0, ts)
		// Try to print as string, but show byte length too
		fmt.Printf("[%d] %s (%d bytes): %s\n", i, t.Format(time.RFC3339), len(data), string(data))
	}
}
