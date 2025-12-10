package fileWatcher

import (
	"bufio"
	"io"
	"log"
	"log-streamer/internal/broadcaster"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// FileWatcher polls the file for new content and broadcasts it.
func FileWatcher(hub *broadcaster.Hub, path string, interval time.Duration, patternRegex *regexp.Regexp) {
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

		lines, newOffset, err := readNewLines(path, offset, patternRegex)
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

func ReadLastNLines(path string, n int, patternRegex *regexp.Regexp) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "FileWatcher:ReadLastNLines:Error opening file for history")
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "FileWatcher:ReadLastNLines:Error stating file for history")
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
	for cursor := fileSize - 1; cursor >= 0; cursor-- {
		file.Seek(cursor, io.SeekStart)
		buf := make([]byte, 1)
		file.Read(buf)

		if buf[0] == '\n' {
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
		return nil, errors.Wrapf(err, "FileWatcher:ReadLastNLines:Error seeking file for history. StartPos: %d", startPos)
	}

	// Read lines from the start position to EOF
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimSpace(line)

			if line != "" {
				if patternRegex == nil || patternRegex.MatchString(line) {
					lines = append(lines, line)
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				return nil, errors.Wrap(err, "FileWatcher:ReadLastNLines:Error reading history lines")
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
		log.Printf("File size decreased (rotation/truncation detected). Resetting offset from %d to 0.", offset)
		offset = 0
	}

	// Return if no new content
	if currentSize == offset {
		return nil, offset, nil
	}

	// If new content exists - seek and read
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, offset, errors.Wrapf(err, "Error seeking file to offset %d", offset)
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
				return nil, offset, errors.Wrapf(err, "Error reading file from offset %d", offset)
			}
			break
		}
	}

	// Set new offset to the current size of the file
	return newLines, currentSize, nil
}
