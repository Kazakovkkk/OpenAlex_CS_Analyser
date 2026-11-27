package main

import (
	"fmt"
	"strconv"
	"strings"
)

// parseShard parses the 1-based k/N form into a 0-based index and a total.
func parseShard(raw string) (int, int, error) {
	left, right, ok := strings.Cut(strings.TrimSpace(raw), "/")
	if !ok {
		return 0, 0, fmt.Errorf("--shard expects k/N, got %q", raw)
	}
	k, err := strconv.Atoi(strings.TrimSpace(left))
	if err != nil {
		return 0, 0, fmt.Errorf("--shard: bad index %q", left)
	}
	n, err := strconv.Atoi(strings.TrimSpace(right))
	if err != nil || n < 1 {
		return 0, 0, fmt.Errorf("--shard: bad total %q", right)
	}
	if k < 1 || k > n {
		return 0, 0, fmt.Errorf("--shard: %d is outside 1..%d", k, n)
	}
	return k - 1, n, nil
}
