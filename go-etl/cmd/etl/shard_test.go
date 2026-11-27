package main

import "testing"

func TestParseShard(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw        string
		idx, total int
		wantErr    bool
	}{
		{"1/4", 0, 4, false},
		{"4/4", 3, 4, false},
		{" 2 / 3 ", 1, 3, false},
		{"1/1", 0, 1, false},
		{"0/4", 0, 0, true},
		{"5/4", 0, 0, true},
		{"1/0", 0, 0, true},
		{"1", 0, 0, true},
		{"a/2", 0, 0, true},
		{"1/b", 0, 0, true},
	}
	for _, tc := range cases {
		idx, total, err := parseShard(tc.raw)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseShard(%q) = %d/%d, want error", tc.raw, idx, total)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseShard(%q): %v", tc.raw, err)
		}
		if idx != tc.idx || total != tc.total {
			t.Fatalf("parseShard(%q) = %d/%d, want %d/%d", tc.raw, idx, total, tc.idx, tc.total)
		}
	}
}
