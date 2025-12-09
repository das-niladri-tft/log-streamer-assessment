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

// --- Command Line Arguments ---
var (
	filePath     string
	initialLines int
)

// --- Constants ---
const (
	// File polling interval as recommended in README.
	pollInterval = 500 * time.Millisecond
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow cross-origin for local testing
	},
}

// --- Component 1: Broadcaster (Hub) ---

// // Hub maintains the set of active clients and broadcasts messages.
// type Hub struct {
// 	// Registered clients.
// 	clients map[*Client]bool

// 	// Inbound messages from the file watcher.
// 	broadcast chan []byte

// 	// Register requests from the clients.
// 	register chan *Client

// 	// Unregister requests from clients.
// 	unregister chan *Client
// }

// func newHub() *Hub {
// 	return &Hub{
// 		broadcast:  make(chan []byte),
// 		register:   make(chan *Client),
// 		unregister: make(chan *Client),
// 		clients:    make(map[*Client]bool),
// 	}
// }

// func (h *Hub) run() {
// 	for {
// 		select {
// 		case client := <-h.register:
// 			h.clients[client] = true
// 			log.Printf("Client registered. Total clients: %d", len(h.clients))
// 		case client := <-h.unregister:
// 			if _, ok := h.clients[client]; ok {
// 				delete(h.clients, client)
// 				close(client.send)
// 				log.Printf("Client unregistered. Total clients: %d", len(h.clients))
// 			}
// 		case message := <-h.broadcast:
// 			for client := range h.clients {
// 				select {
// 				case client.send <- message:
// 				default:
// 					// If sending fails (client buffer full), unregister and close
// 					close(client.send)
// 					delete(h.clients, client)
// 				}
// 			}
// 		}
// 	}
// }

// --- Component 2: Client Handler ---

// // Client is a middleman between the websocket connection and the hub.
// type Client struct {
// 	hub *Hub
// 	// The websocket connection.
// 	conn *websocket.Conn
// 	// Buffered channel of outbound messages.
// 	send chan []byte
// }

// var upgrader = websocket.Upgrader{
// 	ReadBufferSize:  1024,
// 	WriteBufferSize: 1024,
// 	CheckOrigin: func(r *http.Request) bool {
// 		return true // Allow cross-origin for local testing
// 	},
// }

// // readPump reads messages from the websocket connection (primarily for disconnects).
// func (c *Client) readPump() {
// 	defer func() {
// 		c.hub.unregister <- c
// 		c.conn.Close()
// 	}()
// 	c.conn.SetReadDeadline(time.Now().Add(pongWait))
// 	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
// 	for {
// 		// Read message to detect closed connection/error from the client side
// 		_, _, err := c.conn.ReadMessage()
// 		if err != nil {
// 			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
// 				log.Printf("Error reading from client: %v", err)
// 			}
// 			break
// 		}
// 	}
// }

// // writePump pumps messages from the hub to the websocket connection.
// func (c *Client) writePump() {
// 	ticker := time.NewTicker(pingPeriod)
// 	defer func() {
// 		ticker.Stop()
// 		c.conn.Close()
// 	}()
// 	for {
// 		select {
// 		case message, ok := <-c.send:
// 			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
// 			if !ok {
// 				// The hub closed the channel.
// 				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
// 				return
// 			}

// 			w, err := c.conn.NextWriter(websocket.TextMessage)
// 			if err != nil {
// 				return
// 			}
// 			w.Write(message)

// 			// Send additional queued messages (burst handling)
// 			n := len(c.send)
// 			for i := 0; i < n; i++ {
// 				w.Write([]byte("\n"))
// 				w.Write(<-c.send)
// 			}

// 			if err := w.Close(); err != nil {
// 				return
// 			}
// 		case <-ticker.C:
// 			// Send a periodic ping message
// 			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
// 			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
// 				return
// 			}
// 		}
// 	}
// }

// HTTP:
// serveHome serves the index.html file (Required feature)
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
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `
			<!DOCTYPE html>
			<html lang="en">
			<head><title>Log Viewer</title></head>
			<body>
				<h1>Log Streamer</h1>
				<p>Error: Could not read index.html file. Ensure it is in the execution directory.</p>
				<p>Websocket endpoint is ready at /ws</p>
			</body>
			</html>
		`)
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

func main() {
	// Parse command-line arguments
	flag.StringVar(&filePath, "file", "sample.log", "Path to the log file to tail (required)")
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
	// Convert relative path to absolute path for reliable file watching
	absPath, err := filepath.Abs(filePath)
	if err == nil {
		filePath = absPath
	}

	log.Printf("Starting Real-Time Log Streamer")
	log.Printf("File: %s", filePath)
	log.Printf("Initial lines: %d", initialLines)
	log.Printf("Port: %d", *port)

	// 1. Initialize broadcaster and start it in a goroutine
	hub := broadcaster.NewHub()
	go hub.Broadcast()

	// 2. Start file watcher in a goroutine
	go fileWatcher.FileWatcher(hub, filePath, pollInterval)

	// 3. Set up HTTP routes for "/" and "/ws"
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})
	// simple favicon handler to avoid 404 noise from browsers
	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	// 4. Start HTTP server
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Server starting on http://localhost%s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
