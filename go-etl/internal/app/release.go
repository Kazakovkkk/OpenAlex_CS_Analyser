package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-etl/internal/adapters/s3"
	"go-etl/internal/pipeline"
)

// Release is one OpenAlex release: both mandatory manifests of the same date.
// Institutions stay diagnostic and never block a working load.
type Release struct {
	Works  *s3.Manifest
	Topics *s3.Manifest
	Date   time.Time
	ID     string
}

func (r *Release) DateString() string { return r.Date.Format("2006-01-02") }

type releaseFile struct {
	URL           string `json:"url"`
	ContentLength int64  `json:"content_length"`
}

type releaseManifest struct {
	Entity string        `json:"entity"`
	Format string        `json:"format"`
	Date   string        `json:"date"`
	Files  []releaseFile `json:"files"`
}

type releaseIdentity struct {
	Manifests []releaseManifest `json:"manifests"`
}

// LoadRelease fetches and validates the Works and Topics manifests of one
// release. Both are mandatory for a working load: Topics must load even when
// the Institutions manifest is unavailable.
func LoadRelease(ctx context.Context, client *s3.Client) (*Release, error) {
	if client == nil {
		return nil, fmt.Errorf("app: nil s3 client")
	}
	works, err := fetchValidatedManifest(ctx, client, "works")
	if err != nil {
		return nil, err
	}
	topics, err := fetchValidatedManifest(ctx, client, "topics")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(works.Date) != strings.TrimSpace(topics.Date) {
		return nil, &s3.ManifestInvalidError{
			Key:    s3.ParquetManifestKey("topics"),
			Reason: fmt.Sprintf("topics date %q differs from works date %q", topics.Date, works.Date),
		}
	}
	date, err := time.Parse("2006-01-02", strings.TrimSpace(works.Date))
	if err != nil {
		return nil, &s3.ManifestInvalidError{
			Key:    s3.ParquetManifestKey("works"),
			Reason: fmt.Sprintf("date %q is not YYYY-MM-DD", works.Date),
		}
	}
	return &Release{
		Works:  works,
		Topics: topics,
		Date:   date.UTC(),
		ID:     computeReleaseID(works, topics),
	}, nil
}

func fetchValidatedManifest(ctx context.Context, client *s3.Client, entity string) (*s3.Manifest, error) {
	key := s3.ParquetManifestKey(entity)
	m, err := client.FetchManifest(ctx, key)
	if err != nil {
		return nil, err
	}
	if err := m.Validate(entity); err != nil {
		return nil, err
	}
	return m, nil
}

// computeReleaseID is the reproducible identity of the whole release: a typed
// list of both manifests, entities sorted by name, the copy of every file list
// sorted by URL, serialized with json.Marshal and hashed with SHA-256. The
// original file order stays untouched for sharding.
func computeReleaseID(works, topics *s3.Manifest) string {
	manifests := []releaseManifest{
		manifestIdentity("topics", topics),
		manifestIdentity("works", works),
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].Entity < manifests[j].Entity })
	raw, err := json.Marshal(releaseIdentity{Manifests: manifests})
	if err != nil {
		// The structure is a fixed DTO; a marshal failure cannot be recovered
		// meaningfully, but never return a non-reproducible id.
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func manifestIdentity(entity string, m *s3.Manifest) releaseManifest {
	files := make([]releaseFile, 0, len(m.Files))
	for _, f := range m.Files {
		files = append(files, releaseFile{URL: f.URL, ContentLength: f.Meta.ContentLength})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].URL < files[j].URL })
	return releaseManifest{
		Entity: entity,
		Format: m.Format,
		Date:   m.Date,
		Files:  files,
	}
}

// loadPlan is the complete file list of one job. The phase decides whether
// Works are read fully or only from partitions newer than the published
// snapshot; Topics are always read fully for a new release.
type loadPlan struct {
	Release     *Release
	Phase       pipeline.LoadPhase
	Works       []s3.ManifestEntry
	Topics      []s3.ManifestEntry
	PublishedAt *time.Time
	Deletions   bool
}

const firstTransitionSnapshot = "2026-10-14"

// buildLoadPlan applies the release rules of spec step 2.3:
//   - full load or confirmed restart reads every Works file;
//   - incremental reads Works partitions with updated_date > published date;
//   - the first transition away from the 2026-10-14 snapshot also includes
//     that day's partitions once;
//   - Topics are read fully for a new release, and deletions are processed
//     even when no Works file is new.
func buildLoadPlan(rel *Release, published *time.Time, phase pipeline.LoadPhase, shardIndex, shardTotal, maxFiles int) (*loadPlan, error) {
	if rel == nil {
		return nil, fmt.Errorf("app: nil release")
	}
	if phase != pipeline.PhaseFull && phase != pipeline.PhaseIncremental {
		return nil, fmt.Errorf("app: load plan needs a full or incremental phase, got %q", phase)
	}
	works, err := rel.Works.SelectEntries(shardIndex, shardTotal, maxFiles)
	if err != nil {
		return nil, fmt.Errorf("app: select works files: %w", err)
	}
	topics, err := rel.Topics.SelectEntries(shardIndex, shardTotal, maxFiles)
	if err != nil {
		return nil, fmt.Errorf("app: select topics files: %w", err)
	}

	plan := &loadPlan{
		Release: rel,
		Phase:   phase,
		Topics:  topics,
	}
	if phase == pipeline.PhaseFull {
		plan.Works = works
		return plan, nil
	}
	if published == nil {
		return nil, fmt.Errorf("app: incremental plan needs a published snapshot")
	}
	selected, err := filterWorksSince(works, *published, rel.Date)
	if err != nil {
		return nil, err
	}
	plan.Works = selected
	plan.PublishedAt = published
	plan.Deletions = true
	// Deletions are needed on any transition away from a published release,
	// even if no Works file changed.
	return plan, nil
}

// filterWorksSince keeps partitions with updated_date strictly after the
// published date. The first transition away from 2026-10-14 also includes that
// day once, because the previous snapshot published those partitions itself.
func filterWorksSince(entries []s3.ManifestEntry, published, releaseDate time.Time) ([]s3.ManifestEntry, error) {
	published = published.UTC().Truncate(24 * time.Hour)
	releaseDate = releaseDate.UTC().Truncate(24 * time.Hour)
	includeEqual := published.Format("2006-01-02") == firstTransitionSnapshot && releaseDate.After(published)

	out := make([]s3.ManifestEntry, 0, len(entries))
	for _, e := range entries {
		part := s3.ExtractPartition(e.URL)
		if part == "" {
			return nil, &s3.ManifestInvalidError{Reason: fmt.Sprintf("file %q has no updated_date partition", e.URL)}
		}
		day, err := time.Parse("2006-01-02", strings.TrimPrefix(part, "updated_date="))
		if err != nil {
			return nil, &s3.ManifestInvalidError{Reason: fmt.Sprintf("file %q has an invalid partition %q", e.URL, part)}
		}
		day = day.UTC()
		if day.After(published) || (includeEqual && day.Equal(published)) {
			out = append(out, e)
		}
	}
	return out, nil
}
