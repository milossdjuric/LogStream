//script to read logs from the logs directory

package main

import (
	"fmt"
	"log"

	"github.com/milossdjuric/logstream/internal/storage"
)

func main() {
	// Open the logs directory
	logDir := "./logs"
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
		data, err := l.Read(i)
		if err != nil {
			fmt.Printf("Error reading offset %d: %v\n", i, err)
			continue
		}
		// Try to print as string, but show byte length too
		fmt.Printf("[%d] (%d bytes): %s\n", i, len(data), string(data))
	}
}
