package fileWatcher

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"log-streamer/internal/broadcaster"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
)

// FileWatcher polls the file for new content and broadcasts it.
func FileWatcher(hub *broadcaster.Hub, path string, interval time.Duration, patternRegex *regexp.Regexp, eventDriven bool) {
	if eventDriven {
		eventDrivenFileWatcher(hub, path, patternRegex)
	} else {
		pollingBasedFileWatcher(hub, path, interval, patternRegex)
	}
}

// This polls the file for new content and broadcasts it.
func pollingBasedFileWatcher(hub *broadcaster.Hub, path string, interval time.Duration, patternRegex *regexp.Regexp) {
	var offset int64
	var mu sync.Mutex

	// Initialize offset to current file size so we only stream new lines after start.
	mu.Lock()
	fileInfo, err := os.Stat(path)
	if err == nil {
		offset = fileInfo.Size()
	}
	mu.Unlock()

	log.Printf("Watcher started for file: %s. Mode Polling", path)

	for {
		time.Sleep(interval)

		lines, newOffset, err := readNewLines(path, offset, patternRegex)
		if err != nil {
			log.Printf("Error watching file %s: %v", path, err)
			continue
		}

		// Update offset
		mu.Lock()
		offset = newOffset
		mu.Unlock()

		// Broadcast new lines
		for _, line := range lines {
			hub.BroadcastChan <- []byte(fmt.Sprintf("[%s] %s", filepath.Base(path), line))
		}
	}
}

// eventDrivenFileWatcher uses fsnotify to watch for file changes and broadcasts new lines.
func eventDrivenFileWatcher(hub *broadcaster.Hub, path string, patternRegex *regexp.Regexp) {
	var offset int64
	var mu sync.Mutex

	mu.Lock()
	fileInfo, err := os.Stat(path)
	if err == nil {
		offset = fileInfo.Size()
	}
	mu.Unlock()

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Error creating fsnotify watcher for %s: %v", path, err)
	}
	defer watcher.Close()

	if err := watcher.Add(path); err != nil {
		log.Fatalf("Error adding file %s to watcher: %v", path, err)
	}

	log.Printf("Watcher started for file: %s. Mode Event-Driven", path)

	// A slow fallback ticker helps catch missed events (e.g., rotation edge cases, NFS).
	fallbackTicker := time.NewTicker(5 * time.Second)
	defer fallbackTicker.Stop()

	// Main event loop
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				log.Printf("eventDrivenFileWatcher:Watcher events channel closed for %s.", path)
				return
			}

			// WRITE: New content was added. This is the primary trigger.
			// CREATE: File might have been re-created after rotation/deletion.
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				lines, newOffset, err := readNewLines(path, offset, patternRegex)
				if err != nil {
					log.Printf("eventDrivenFileWatcher:Error reading lines from %s on WRITE/CREATE: %v", path, err)
					continue
				}

				mu.Lock()
				offset = newOffset
				mu.Unlock()

				for _, line := range lines {
					hub.BroadcastChan <- []byte(fmt.Sprintf("[%s] %s", filepath.Base(path), line))
				}

			} else if event.Op&fsnotify.Remove == fsnotify.Remove || event.Op&fsnotify.Rename == fsnotify.Rename {
				// File was deleted or rotated (renamed). Attempt to re-establish the watch.

				// Remove the watch on the old path.
				if err := watcher.Remove(path); err != nil && !os.IsNotExist(err) {
					log.Printf("eventDrivenFileWatcher:Warning: Failed to remove watch on %s: %v", path, err)
				}

				// PauseTime to allow OS/app to complete rotation
				time.Sleep(100 * time.Millisecond)

				// Attempt to re-add the watch (this handles the new file appearing).
				if err := watcher.Add(path); err != nil {
					log.Printf("eventDrivenFileWatcher:File %s missing after rename/remove. Will re-attempt watch via fallback.", path)
				} else {
					log.Printf("eventDrivenFileWatcher:Successfully re-established watch on %s.", path)
					mu.Lock()
					offset = 0
					mu.Unlock()
				}
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				log.Printf("eventDrivenFileWatcher:Watcher errors channel closed for %s.", path)
				return
			}
			log.Printf("eventDrivenFileWatcher:Fsnotify error for %s: %v", path, err)

		case <-fallbackTicker.C:
			// Slow check to handle missed events or re-add watches for files that reappeared.
			if _, statErr := os.Stat(path); statErr == nil {
				if err := watcher.Add(path); err != nil {
					log.Printf("Fallback: Could not add watch to existing file %s: %v", path, err)
				}
			}

			// Perform a read just in case events were missed.
			lines, newOffset, readErr := readNewLines(path, offset, patternRegex)
			if readErr == nil {
				mu.Lock()
				offset = newOffset
				mu.Unlock()

				for _, line := range lines {
					taggedLine := fmt.Sprintf("[%s] %s", filepath.Base(path), line)
					hub.BroadcastChan <- []byte(taggedLine)
				}
			}
		}
	}
}

func ReadLastNLines(path string, n int, patternRegex *regexp.Regexp) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "[%s]FileWatcher:ReadLastNLines:Error opening file for history", filepath.Base(path))
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "[%s]FileWatcher:ReadLastNLines:Error stating file for history", filepath.Base(path))
	}

	fileSize := fileInfo.Size()
	if fileSize == 0 {
		return nil, nil
	}

	var (
		lines     []string
		lineCount int
		startPos  int64 = 0
	)

	// Scan backward byte by byte to find the starting position of the N-th line
	isTrailingNewlineSkipped := false
	for cursor := fileSize - 1; cursor >= 0; cursor-- {
		file.Seek(cursor, io.SeekStart)
		buf := make([]byte, 1)
		file.Read(buf)

		if buf[0] == '\n' {
			if !isTrailingNewlineSkipped && cursor == fileSize-1 {
				isTrailingNewlineSkipped = true
				continue
			}
			lineCount++
			// Check if n-th line found
			if lineCount == n {
				startPos = cursor + 1
				break
			}
		}

		if cursor == 0 {
			startPos = 0
			break
		}
	}

	// Explicitly seek to the determined start position to ensure the bufio.Reader starts from the correct byte offset
	_, err = file.Seek(startPos, io.SeekStart)
	if err != nil {
		return nil, errors.Wrapf(err, "[%s]FileWatcher:ReadLastNLines:Error seeking file for history. StartPos: %d", filepath.Base(path), startPos)
	}

	// Read lines from the start position to EOF
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimSpace(line)

			if line != "" {
				if patternRegex == nil || patternRegex.MatchString(line) {
					lines = append(lines, fmt.Sprintf("[%s] %s", filepath.Base(path), line))
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				return nil, errors.Wrapf(err, "[%s]FileWatcher:ReadLastNLines:Error reading history lines", filepath.Base(path))
			}
			break
		}
	}

	return lines, nil
}

// readNewLines reads content written since the last offset.
func readNewLines(path string, offset int64, patternRegex *regexp.Regexp) ([]string, int64, error) {
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

	// Handle file truncation/rotation
	if currentSize < offset {
		log.Printf("[%s] File size decreased (rotation/truncation detected). Resetting offset from %d to 0.", filepath.Base(path), offset)
		offset = 0
	}

	// Return if no new content
	if currentSize == offset {
		return nil, offset, nil
	}

	// If new content exists - seek and read
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, offset, errors.Wrapf(err, "[%s]Error seeking file to offset %d", filepath.Base(path), offset)
	}

	var newLines []string
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimSpace(line)
			if line != "" {
				if patternRegex == nil || patternRegex.MatchString(line) {
					newLines = append(newLines, line)
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				return nil, offset, errors.Wrapf(err, "[%s]Error reading file from offset %d", filepath.Base(path), offset)
			}
			break
		}
	}

	// Set new offset to the current size of the file
	return newLines, currentSize, nil
}
