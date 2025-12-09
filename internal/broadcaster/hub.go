package broadcaster

import "log"

// Hub maintains the set of active clients and broadcastChans messages.
type Hub struct {
	// Registered clients.
	Clients map[*Client]bool
	// Inbound messages from the file watcher.
	BroadcastChan chan []byte
	// Register requests from the clients.
	Register chan *Client
	// Unregister requests from clients.
	Unregister chan *Client
}

func NewHub() *Hub {
	return &Hub{
		BroadcastChan: make(chan []byte),
		Register:      make(chan *Client),
		Unregister:    make(chan *Client),
		Clients:       make(map[*Client]bool),
	}
}

func (h *Hub) Broadcast() {
	for {
		select {
		case client := <-h.Register:
			h.Clients[client] = true
			log.Printf("Client registered. Total clients: %d", len(h.Clients))
		case client := <-h.Unregister:
			if _, ok := h.Clients[client]; ok {
				delete(h.Clients, client)
				close(client.Send)
				log.Printf("Client unregistered. Total clients: %d", len(h.Clients))
			}
		case message := <-h.BroadcastChan:
			for client := range h.Clients {
				select {
				case client.Send <- message:
				default:
					// If sending fails (client buffer full), unregister and close
					close(client.Send)
					delete(h.Clients, client)
				}
			}
		}
	}
}
