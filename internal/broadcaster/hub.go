package broadcaster

import (
	"log"
	"sync"
)

// Hub maintains the set of active clients and broadcastChans messages.
type Hub struct {
	// Inbound messages from the file watcher.
	BroadcastChan chan []byte
	// Register requests from the clients.
	Register chan *Client
	// Unregister requests from clients.
	Unregister chan *Client

	mu sync.RWMutex

	// **Metrics**
	Clients            map[*Client]bool
	TotalLinesStreamed uint64
}

func NewHub() *Hub {
	return &Hub{
		BroadcastChan:      make(chan []byte),
		Register:           make(chan *Client),
		Unregister:         make(chan *Client),
		Clients:            make(map[*Client]bool),
		TotalLinesStreamed: 0,
	}
}

func (h *Hub) Broadcast() {
	for {
		select {
		case client := <-h.Register:
			h.mu.Lock()
			h.Clients[client] = true
			h.mu.Unlock()
			log.Printf("Client registered. Total clients: %d", len(h.Clients))
		case client := <-h.Unregister:
			h.mu.Lock()
			if _, ok := h.Clients[client]; ok {
				delete(h.Clients, client)
				close(client.Send)
				log.Printf("Client unregistered. Total clients: %d", len(h.Clients))
			}
			h.mu.Unlock()
		case message := <-h.BroadcastChan:
			// --- METRICS UPDATE
			h.mu.Lock()
			h.TotalLinesStreamed++
			h.mu.Unlock()

			h.mu.RLock() // Use RLock when only reading the Clients map
			for client := range h.Clients {
				select {
				case client.Send <- message:
				default:
					log.Printf("Client buffer full. Unregistering client %s", client.Conn.RemoteAddr())
					close(client.Send)
					h.Unregister <- client // Safely sends client back to the main select loop for deletion
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Hub) GetMetrics() (int, uint64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.Clients), h.TotalLinesStreamed
}
