package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var (
	filePath     string
	initialLines int
)

func main() {
	// Parse command-line arguments
	flag.StringVar(&filePath, "file", "", "Path to the log file to tail (required)")
	flag.IntVar(&initialLines, "lines", 10, "Number of initial lines to send (default: 10)")
	port := flag.Int("port", 8080, "Port to listen on (default: 8080)")
	flag.Parse()

	// Validate arguments
	if filePath == "" {
		fmt.Println("Error: -file argument is required")
		flag.Usage()
		os.Exit(1)
	}

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Fatalf("Error: File does not exist: %s", filePath)
	}

	log.Printf("Starting Real-Time Log Streamer")
	log.Printf("File: %s", filePath)
	log.Printf("Initial lines: %d", initialLines)
	log.Printf("Port: %d", *port)

	// TODO: Initialize broadcaster and start it in a goroutine

	// TODO: Start file watcher in a goroutine

	// TODO: Set up HTTP routes for "/" and "/ws"

	// TODO: Start HTTP server
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Server starting on http://localhost%s", addr)
	// Uncomment when ready: log.Fatal(http.ListenAndServe(addr, nil))
}
