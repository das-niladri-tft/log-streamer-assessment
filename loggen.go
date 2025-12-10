package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

// loggen.go writes a new log line to a specified file at a given interval.
func main() {
	filePath := flag.String("file", "sample.log", "Path to the log file to append to")
	intervalStr := flag.String("interval", "1s", "Interval at which to write new lines")
	flag.Parse()

	interval, err := time.ParseDuration(*intervalStr)
	if err != nil {
		log.Fatalf("Invalid interval duration: %v", err)
	}

	log.Printf("Loggen.go: Starting log generator for file: %s with interval: %s", *filePath, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var counter int
	for range ticker.C {
		f, err := os.OpenFile(*filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Printf("Loggen.go: Failed to open file %s: %v", *filePath, err)
			continue
		}

		logLine := fmt.Sprintf("%s New Log Created: counter=%d", time.Now().Format("2006-01-02 15:04:05"), counter)

		_, err = fmt.Fprintln(f, logLine)
		f.Close()

		if err != nil {
			log.Printf("Loggen.go: Failed to write to file %s: %v", *filePath, err)
		} else {
			log.Print(logLine)
		}

		counter++
	}
}
