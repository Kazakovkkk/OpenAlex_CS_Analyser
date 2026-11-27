package ingest

import (
	"testing"
	"time"
)

func TestUpdateFrom(t *testing.T) {
	t.Parallel()
	cases := []struct{ published, want string }{
		{"2026-10-14", "2026-10-14"},
		{"2026-11-06", "2026-11-06"},
		{"2026-11-07", "2026-11-08"},
		{"2027-01-13", "2027-01-14"},
	}
	for _, tc := range cases {
		p, err := time.Parse("2006-01-02", tc.published)
		if err != nil {
			t.Fatal(err)
		}
		if got := UpdateFrom(p).Format("2006-01-02"); got != tc.want {
			t.Fatalf("UpdateFrom(%s) = %s, want %s", tc.published, got, tc.want)
		}
	}
}
