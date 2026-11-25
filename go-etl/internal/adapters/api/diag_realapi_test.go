//go:build inspect

package api

//	go test -v -count=1 -timeout 5m -tags=inspect ./internal/adapters/api -run TestDiagRealAPI
import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go-etl/internal/metrics"
)

func TestDiagRealAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var out strings.Builder
	fmt.Fprintf(&out, "\n=== Real OpenAlex API contract check ===\n")

	jm := metrics.NewJobMetrics()
	c := NewClient(Options{
		RPSLimit: 2,
		Email:    "go-etl-diag@example.com",
		Metrics:  jm,
	})
	var firstID string
	var firstFieldID string
	stats, err := c.IterateWorks(ctx, WorksQuery{FieldID: 17, PerPage: 2, MaxRecords: 2}, func(raw []byte) error {
		s := string(raw)
		if firstID == "" {
			firstID = extractJSONField(s, "https://openalex.org/W")
		}
		if firstFieldID == "" {
			firstFieldID = extractJSONField(s, "https://openalex.org/fields/")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	fmt.Fprintf(&out, "page 1 (no key): records=%d pages=%d requests=%d\n", stats.Records, stats.Pages, stats.Requests)
	fmt.Fprintf(&out, "  first work id: %s\n", firstID)
	fmt.Fprintf(&out, "  first primary_topic.field.id: %s\n", firstFieldID)
	if stats.Records != 2 {
		t.Fatalf("records = %d, want 2", stats.Records)
	}
	if !strings.Contains(firstID, "openalex.org/W") {
		t.Fatalf("unexpected work id %q", firstID)
	}
	if firstFieldID != "https://openalex.org/fields/17" {
		t.Fatalf("filter returned field %q, want fields/17", firstFieldID)
	}

	if _, err := c.IterateWorks(ctx, WorksQuery{FieldID: 17, PerPage: 2, MaxRecords: 5}, func([]byte) error { return nil }); !errors.Is(err, ErrAPIKeyRequired) {
		t.Fatalf("cursor paging without a key: err = %v, want ErrAPIKeyRequired", err)
	}
	fmt.Fprintf(&out, "page 2 (no key): client refused locally with ErrAPIKeyRequired (no extra request)\n")

	rawURL := c.BaseURL() + "/works?filter=primary_topic.field.id:17&per-page=2&cursor=IlsxMDAuMCwgMTUzNDY2LCAnaHR0cHM6Ly9vcGVuYWxleC5vcmcvVzI2MjY3NzgzMjgnXSI%3D"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("cursor probe: %v", err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	fmt.Fprintf(&out, "  raw cursor request -> HTTP %d, body: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("cursor without key: status = %d, want 401", resp.StatusCode)
	}

	for _, payload := range []string{`{}`, `{"email":"not-an-email"}`} {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL()+"/api-keys", strings.NewReader(payload))
		if err != nil {
			t.Fatalf("build api-keys request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("api-keys probe: %v", err)
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		fmt.Fprintf(&out, "POST /api-keys %s -> HTTP %d, body: %s\n",
			payload, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	t.Log(out.String())
}

func extractJSONField(raw, marker string) string {
	i := strings.Index(raw, marker)
	if i < 0 {
		return ""
	}
	rest := raw[i:]
	if end := strings.IndexByte(rest, '"'); end >= 0 {
		return rest[:end]
	}
	return rest
}
