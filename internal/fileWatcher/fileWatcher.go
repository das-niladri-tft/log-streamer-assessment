package fileWatcher

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"log-streamer/internal/broadcaster"
	"os"
	"sync"
	"time"
)

// FileWatcher polls the file for new content and broadcasts it.
func FileWatcher(hub *broadcaster.Hub, path string, interval time.Duration) {
	var offset int64
	var mu sync.Mutex

	// Initialize offset to current file size so we only stream new lines after start.
	mu.Lock()
	fileInfo, err := os.Stat(path)
	if err == nil {
		offset = fileInfo.Size()
	}
	mu.Unlock()

	for {
		time.Sleep(interval)

		lines, newOffset, err := readNewLines(path, offset)
		if err != nil {
			log.Printf("Error watching file: %v", err)
			continue
		}

		// Update offset
		mu.Lock()
		offset = newOffset
		mu.Unlock()

		// Broadcast new lines
		for _, line := range lines {
			hub.BroadcastChan <- []byte(line)
		}
	}
}

func ReadLastNLines(path string, n int) []string {
	log.Printf("FilePath: %s", path)

	file, err := os.Open(path)
	if err != nil {
		log.Printf("Error opening file for history: %v", err)
		return nil
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Printf("Error stating file for history: %v", err)
		return nil
	}
	fileSize := fileInfo.Size()

	log.Print("FileSize: ", fileSize)

	if fileSize == 0 {
		return nil
	}

	var lines []string
	var lineCount int
	var startPos int64 = 0

	// Scan backward byte by byte to find the starting position (offset) of the N-th line
	for cursor := fileSize - 1; cursor >= 0; cursor-- {
		// Seek and read one byte
		file.Seek(cursor, io.SeekStart)
		buf := make([]byte, 1)
		file.Read(buf)

		if buf[0] == '\n' {
			lineCount++
			if lineCount == n {
				startPos = cursor + 1 // Start reading immediately after the N-th newline
				break
			}
		}
		// If we reach the start of the file before finding N newlines, start at 0
		if cursor == 0 {
			startPos = 0
			break
		}
	}

	// Explicitly seek to the determined start position
	// This ensures the bufio.Reader starts from the correct byte offset.
	_, err = file.Seek(startPos, io.SeekStart)
	if err != nil {
		log.Printf("Error seeking file to start position %d: %v", startPos, err)
		return nil
	}

	// 3. Read lines from the start position to EOF
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			// Remove trailing newline if present and add to list
			if line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading history lines: %v", err)
			break
		}
	}

	return lines
}

// readNewLines reads content written since the last offset.
func readNewLines(path string, offset int64) ([]string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, offset, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, offset, err
	}
	currentSize := fileInfo.Size()

	// 1. Handle file truncation/rotation (Required feature)
	if currentSize < offset {
		log.Printf("File size decreased (rotation/truncation detected). Resetting offset from %d to 0.", offset)
		offset = 0
	}

	// 2. No new content
	if currentSize == offset {
		return nil, offset, nil
	}

	// 3. New content exists - seek and read
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, offset, fmt.Errorf("error seeking file: %w", err)
	}

	var newLines []string
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			// Remove trailing newline if present and add to list
			if line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			newLines = append(newLines, line)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, offset, fmt.Errorf("error reading new lines: %w", err)
		}
	}

	// New offset is the current size of the file
	return newLines, currentSize, nil
}
