package pipeline

import (
	"errors"
	"testing"
	"time"
)

func TestDateFromYMD(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   uint32
		want string
	}{
		{20261014, "2026-10-14"},
		{20270113, "2027-01-13"},
		{0, ""},
		{20261399, ""},           // month 13
		{20260230, "2026-03-02"}, // time.Date normalizes overflow; ClickHouse never returns such a date
	}
	for _, tc := range cases {
		got := dateFromYMD(tc.in)
		if tc.want == "" {
			if got != nil {
				t.Fatalf("dateFromYMD(%d) = %v, want nil", tc.in, got)
			}
			continue
		}
		if got == nil {
			t.Fatalf("dateFromYMD(%d) = nil, want %s", tc.in, tc.want)
		}
		if got.Format("2006-01-02") != tc.want {
			t.Fatalf("dateFromYMD(%d) = %s, want %s", tc.in, got.Format("2006-01-02"), tc.want)
		}
	}
}

func TestUnixMicros(t *testing.T) {
	t.Parallel()
	if got := unixMicros(0); got != nil {
		t.Fatalf("unixMicros(0) = %v, want nil", got)
	}
	want := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	got := unixMicros(want.UnixMicro())
	if got == nil || !got.Equal(want) {
		t.Fatalf("unixMicros(%d) = %v, want %v", want.UnixMicro(), got, want)
	}
}

func TestStatePredicates(t *testing.T) {
	t.Parallel()
	snap := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)

	published := State{LoadState: LoadIdle, DataVersion: 1, SnapshotDate: &snap, ManifestID: "works-2026-10-14-abc"}
	if !published.Published() {
		t.Fatal("Published() = false for a complete release")
	}
	if published.SnapshotYear() != 2026 {
		t.Fatalf("SnapshotYear() = %d, want 2026", published.SnapshotYear())
	}
	if !published.ManifestMatches("works-2026-10-14-abc") {
		t.Fatal("ManifestMatches must accept the published id")
	}
	if published.ManifestMatches("other") {
		t.Fatal("ManifestMatches must reject another id")
	}
	if published.ManifestMatches("") {
		t.Fatal("ManifestMatches must reject an empty id")
	}

	loading := State{LoadState: LoadLoading, DataVersion: 1, SnapshotDate: &snap, ManifestID: "m"}
	if loading.Published() {
		t.Fatal("a loading state must not be published")
	}
	if loading.ManifestMatches("m") {
		t.Fatal("ManifestMatches must be false while loading")
	}

	empty := State{LoadState: LoadIdle}
	if empty.Published() {
		t.Fatal("an empty release must not be published")
	}
	if empty.SnapshotYear() != 0 {
		t.Fatalf("SnapshotYear() = %d, want 0", empty.SnapshotYear())
	}
}

func TestNewStoreValidation(t *testing.T) {
	t.Parallel()
	if _, err := NewStore(nil, "openalex"); err == nil {
		t.Fatal("NewStore(nil) = nil error")
	}
	if _, err := NewCheckpointStore(nil, "openalex"); err == nil {
		t.Fatal("NewCheckpointStore(nil) = nil error")
	}
}

func TestNullableTime(t *testing.T) {
	t.Parallel()
	if got := nullableTime(nil); got != (*time.Time)(nil) {
		t.Fatalf("nullableTime(nil) = %#v, want a typed nil", got)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if got, ok := nullableTime(&now).(time.Time); !ok || !got.Equal(now) {
		t.Fatalf("nullableTime(&now) = %#v, want %v", got, now)
	}
}

func TestErrorSentinels(t *testing.T) {
	t.Parallel()
	// The exported errors must stay distinguishable for the caller.
	for _, err := range []error{ErrRevisionConflict, ErrLoadInProgress, ErrNotLoading} {
		if errors.Is(err, ErrRevisionConflict) && err != ErrRevisionConflict {
			t.Fatalf("%v unexpectedly matches ErrRevisionConflict", err)
		}
	}
}
