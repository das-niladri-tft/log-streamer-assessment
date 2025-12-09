# Real-Time Log Streamer Assessment

## Overview

This coding assessment evaluates your ability to build a real-time log streaming application using Go, WebSockets, and concurrent programming patterns. You will implement a simplified version of the Unix `tail -f` command that streams log file updates to web clients in real-time.

## Objective

Implement a Go-based WebSocket server that monitors a log file and broadcasts new lines to all connected clients as they are written to the file.

## Requirements

### 1. Go Server (`server.go`)

Your server must:

- **Accept command-line arguments**:

  - `-file`: Path to the log file to monitor (required)
  - `-lines`: Number of initial lines to send when a client connects (default: 10)
  - `-port`: Port to listen on (default: 8080)

- **Serve WebSocket endpoint** at `/ws`

- **Send initial history**: When a client connects, immediately send the last N lines of the file

- **Real-time monitoring**: Continuously monitor the file for new lines using a polling mechanism

  - Poll the file periodically (recommend 500ms interval)
  - Track file size and position
  - Handle file truncation/rotation

- **Broadcasting**: Send new lines to all connected WebSocket clients simultaneously

- **Concurrency**: Use Goroutines and Channels for:

  - File watching (dedicated goroutine)
  - Each client connection (goroutine per client)
  - Message broadcasting (central broadcaster goroutine)

- **Serve HTML client** at `/` (index.html is provided)

### 2. File Monitoring Architecture

Your implementation should follow this architecture:

```
┌─────────────────┐
│  File Watcher   │ (Goroutine)
│   - Poll file   │
│   - Read new    │
│     lines       │
└────────┬────────┘
         │
         ▼
  ┌──────────────┐
  │ Broadcast    │
  │ Channel      │
  └──────┬───────┘
         │
         ├─────────┬─────────┬─────────►
         ▼         ▼         ▼
    ┌────────┐ ┌────────┐ ┌────────┐
    │Client 1│ │Client 2│ │Client 3│
    │Handler │ │Handler │ │Handler │
    └────────┘ └────────┘ └────────┘
```

### 3. Key Components to Implement

#### Broadcaster

- Manages all connected clients
- Receives messages from file watcher
- Distributes messages to all clients
- Handles client registration/unregistration

#### Client Handler

- Manages individual WebSocket connections
- Sends initial N lines on connect
- Forwards broadcast messages to the client
- Detects and handles disconnections

#### File Watcher

- Polls file for changes
- Reads new lines when file grows
- Handles file rotation/truncation
- Sends lines to broadcast channel

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Understanding of:
  - Go channels and goroutines
  - HTTP and WebSocket protocols
  - File I/O operations
  - Concurrent programming patterns

### Project Structure

```
log-streamer-assessment/
├── server.go          # Minimal skeleton - implement your solution here
├── index.html         # HTML client (provided, no changes needed)
├── sample.log         # Sample log file
├── go.mod            # Go module file
└── README.md         # This file
```

### What's Provided

- **server.go**: Basic skeleton with command-line argument parsing and validation
- **index.html**: Complete HTML/JavaScript WebSocket client
- **loggen.go**: Utility to generate test log entries
- **sample.log**: Sample log file with initial data
- **go.mod**: Go module configuration with required dependencies

### Setup

1. Review the project structure and requirements
2. Note that `server.go` provides a basic skeleton with argument parsing
3. Install dependencies:
   ```bash
   go mod download
   ```

### Implementation Approach

You need to implement a complete WebSocket server with the following components:

1. **Data Structures**:

   - Design structures to manage clients and message broadcasting
   - Consider what fields are needed for WebSocket connections

2. **File Operations**:

   - Implement efficient reading of last N lines from a file
   - Implement continuous file monitoring (polling approach recommended)
   - Handle file growth, truncation, and rotation

3. **WebSocket Handling**:

   - Upgrade HTTP connections to WebSocket
   - Send initial N lines to new clients
   - Manage client lifecycle (connect, disconnect)

4. **Concurrency**:

   - Use goroutines for file watching and client management
   - Use channels for message broadcasting
   - Ensure thread-safe access to shared data

5. **HTTP Server**:
   - Serve the HTML client at `/`
   - Serve WebSocket endpoint at `/ws`
   - Handle routing and errors

The skeleton in `server.go` provides command-line argument parsing and validation. The rest of the architecture and implementation is up to you.

### Testing Your Implementation

1. **Start the server**:

   ```bash
   go run server.go -file sample.log -lines 10
   ```

2. **Open browser**:

   - Navigate to http://localhost:8080
   - You should see the last 10 lines from sample.log

3. **Test real-time streaming**:
   In another terminal, run the log generator:

   ```bash
   go run loggen.go -file sample.log -interval 1s
   ```

   You should see new lines appearing in real-time in your browser.

4. **Test multiple clients**:

   - Open http://localhost:8080 in multiple browser tabs
   - All clients should receive the same updates simultaneously

5. **Test disconnection handling**:
   - Close a browser tab
   - Check server logs to ensure client was unregistered
   - Other clients should continue working

## Bonus Challenges (Optional)

If you finish early, consider implementing:

1. **Metrics endpoint**: Add `/metrics` that shows:

   - Number of connected clients
   - Total lines streamed
   - Server uptime

2. **Line filtering**: Add `-pattern` flag to only stream lines matching a regex

3. **Multiple file support**: Allow tailing multiple files simultaneously

4. **Better performance**: Use `inotify`/`fsnotify` instead of polling

## Submission

When complete, ensure:

1. ✅ Code compiles without errors
2. ✅ All requirements are implemented
3. ✅ Multiple clients can connect simultaneously
4. ✅ Code is properly formatted (`go fmt`)

## Questions?

If you have questions about requirements during the assessment, note them down and we'll discuss during the code review session.

Good luck! 🚀
# log-streamer-assessment
