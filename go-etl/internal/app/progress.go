package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-etl/internal/httpapi"
	"go-etl/internal/ingest"
)

// progressJournal is the diagnostic run's own progress log. It is deliberately
// separate from etl_checkpoints: a limited run must never leave marks that a
// later full load would treat as completed files.
type progressJournal struct {
	file *os.File
}

func newProgressJournal(req httpapi.IngestRequest) (*progressJournal, error) {
	dir := filepath.Join("tmp", "progress")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("app: create progress directory: %w", err)
	}
	name := fmt.Sprintf("%s-shard%d-of%d-maxfiles%d-maxrecords%d-from%s.jsonl",
		req.Source, req.ShardIndex, req.ShardTotal, req.MaxFiles, req.MaxRecords, safeName(req.FromDate))
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("app: open progress journal: %w", err)
	}
	return &progressJournal{file: f}, nil
}

func (p *progressJournal) Write(res ingest.Result) {
	if p == nil || p.file == nil {
		return
	}
	rec := map[string]any{
		"at":             time.Now().UTC().Format(time.RFC3339),
		"files_done":     res.FilesDone,
		"files_failed":   res.FilesFailed,
		"files_skipped":  res.FilesSkipped,
		"scanned":        res.Scanned,
		"matched":        res.Matched,
		"written":        res.Written,
		"dedup_dropped":  res.DedupDropped,
		"rejected":       res.Rejected,
		"bytes_read":     res.BytesRead,
		"elapsed_sec":    res.Elapsed.Seconds(),
		"stopped":        res.Stopped,
	}
	_ = json.NewEncoder(p.file).Encode(rec)
}

func (p *progressJournal) Close() {
	if p != nil && p.file != nil {
		_ = p.file.Close()
	}
}

func safeName(s string) string {
	if strings.TrimSpace(s) == "" {
		return "all"
	}
	return strings.NewReplacer(":", "", "/", "-").Replace(s)
}
