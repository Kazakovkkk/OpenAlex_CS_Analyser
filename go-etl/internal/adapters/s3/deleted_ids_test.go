package s3

import (
	"testing"
)

func TestParseDeletedLine(t *testing.T) {
	t.Parallel()
	cases := []struct {
		line    string
		wantID  uint64
		wantDay string
		wantHas bool
	}{
		{"W123,2026-10-01", 123, "2026-10-01", true},
		{"https://openalex.org/W2741809807,2026-10-14", 2741809807, "2026-10-14", true},
		{"  W42 , 2026-01-05 ", 42, "2026-01-05", true},
		{"W7", 7, "", false},
		{"W7,garbage", 7, "", false},
		{"work_id,deleted_date", 0, "", false},
		{"", 0, "", false},
		{"not-a-work,2026-10-01", 0, "", false},
	}
	for _, tc := range cases {
		id, at, has := ParseDeletedLine(tc.line)
		if id != tc.wantID {
			t.Fatalf("ParseDeletedLine(%q) id = %d, want %d", tc.line, id, tc.wantID)
		}
		if has != tc.wantHas {
			t.Fatalf("ParseDeletedLine(%q) hasDate = %v, want %v", tc.line, has, tc.wantHas)
		}
		if !has {
			continue
		}
		if got := at.Format("2006-01-02"); got != tc.wantDay {
			t.Fatalf("ParseDeletedLine(%q) date = %s, want %s", tc.line, got, tc.wantDay)
		}
	}
}

func TestDeletedIDsKey(t *testing.T) {
	t.Parallel()
	if DeletedIDsKey != "data/parquet/works/deleted_ids.csv.gz" {
		t.Fatalf("DeletedIDsKey = %q", DeletedIDsKey)
	}
}
