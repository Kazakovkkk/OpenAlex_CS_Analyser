package ingest

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type checkpointLine struct {
	URL string    `json:"url"`
	At  time.Time `json:"at"`
}

type Checkpoint struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

func OpenCheckpoint(path string) (*Checkpoint, error) {
	if path == "" {
		return nil, errors.New("ingest: empty checkpoint path")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("ingest: create checkpoint dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("ingest: open checkpoint %s: %w", path, err)
	}
	return &Checkpoint{path: path, f: f}, nil
}

func (c *Checkpoint) Path() string { return c.path }

func (c *Checkpoint) MarkDone(url string) error {
	line, err := json.Marshal(checkpointLine{URL: url, At: time.Now().UTC()})
	if err != nil {
		return err
	}
	line = append(line, '\n')

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.f == nil {
		return errors.New("ingest: checkpoint is closed")
	}
	if _, err := c.f.Write(line); err != nil {
		return fmt.Errorf("ingest: checkpoint write: %w", err)
	}
	return nil
}

func (c *Checkpoint) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.f == nil {
		return nil
	}
	err := c.f.Close()
	c.f = nil
	return err
}

type Ledger interface {
	LoadDone() (map[string]struct{}, error)
	MarkDone(objectKey string) error
}

type fileLedger struct{ ck *Checkpoint }

func newFileLedger(path string) (*fileLedger, error) {
	ck, err := OpenCheckpoint(path)
	if err != nil {
		return nil, err
	}
	return &fileLedger{ck: ck}, nil
}

func (f *fileLedger) LoadDone() (map[string]struct{}, error) { return LoadDone(f.ck.Path()) }
func (f *fileLedger) MarkDone(objectKey string) error        { return f.ck.MarkDone(objectKey) }
func (f *fileLedger) Close() error                           { return f.ck.Close() }

func LoadDone(path string) (map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	done := make(map[string]struct{}, 2048)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var parsed checkpointLine
		if err := json.Unmarshal(line, &parsed); err != nil || parsed.URL == "" {
			continue
		}
		done[parsed.URL] = struct{}{}
	}
	if err := sc.Err(); err != nil {
		return done, err
	}
	return done, nil
}
