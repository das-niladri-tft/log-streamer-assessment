package main

import (
	"flag"
	"fmt"
	"log"
	"log-streamer/internal/broadcaster"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"log-streamer/internal/fileWatcher"

	"github.com/gorilla/websocket"
)

var (
	filePath        string
	initialLines    int
	ServerStartTime time.Time
)

const (
	pollInterval = 500 * time.Millisecond
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow cross-origin for local testing
	},
}

// HTTP:
// serveHome serves the index.html file
func serveHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Attempt to read and serve index.html
	htmlPath := "index.html"
	content, err := os.ReadFile(htmlPath)
	if err == nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(content)
	} else {
		// Fallback for when the file isn't in the execution path
		http.Error(w, "index.html not found. Check the execution path.", http.StatusInternalServerError)
	}
}

// serveWS handles the WebSocket connection upgrade and client lifecycle.
func serveWS(hub *broadcaster.Hub, w http.ResponseWriter, r *http.Request) {
	log.Printf("WS request from %s %s", r.RemoteAddr, r.URL.Path)
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	log.Printf("WebSocket connected: %s", r.RemoteAddr)

	client := &broadcaster.Client{Hub: hub, Conn: conn, Send: make(chan []byte, 256)}
	client.Hub.Register <- client

	// Send initial history
	initialMessages := fileWatcher.ReadLastNLines(filePath, initialLines)
	for _, msg := range initialMessages {
		client.Send <- []byte(msg)
	}

	go client.WritePump()
	go client.ReadPump()
}

func serveMetrics(hub *broadcaster.Hub, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clientCount, linesStreamed := hub.GetMetrics()

	uptime := time.Since(ServerStartTime)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "Connected Clients: %d\n", clientCount)
	fmt.Fprintf(w, "Total Lines Streamed: %d\n", linesStreamed)
	fmt.Fprintf(w, "Server uptime: %d\n", int64(uptime.Seconds()))
}

func main() {
	flag.StringVar(&filePath, "file", "sample.log", "Path to the log file to tail (required)")
	flag.IntVar(&initialLines, "lines", 10, "Number of initial lines to send (default: 10)")
	port := flag.Int("port", 8080, "Port to listen on (default: 8080)")
	flag.Parse()

	if filePath == "" {
		fmt.Println("Error: -file argument is required")
		flag.Usage()
		os.Exit(1)
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Fatalf("Error: File does not exist: %s", filePath)
	}

	absPath, err := filepath.Abs(filePath)
	if err == nil {
		filePath = absPath
	}

	log.Printf("Starting Real-Time Log Streamer")
	log.Printf("File: %s", filePath)
	log.Printf("Initial lines: %d", initialLines)
	log.Printf("Port: %d", *port)

	// Initialize broadcaster and start it in a goroutine
	hub := broadcaster.NewHub()
	go hub.Broadcast()

	//Start file watcher in a goroutine
	go fileWatcher.FileWatcher(hub, filePath, pollInterval)

	// HTTP routes
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		serveMetrics(hub, w, r)
	})

	// Start HTTP server
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Server starting on http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
