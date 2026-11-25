package app

import (
	"os"
	"testing"
)

func TestResolveAPIKey(t *testing.T) {
	t.Setenv("OPENALEX_API_KEY", "")

	if got := ResolveAPIKey("explicit"); got != "explicit" {
		t.Fatalf("explicit key = %q", got)
	}
	if got := ResolveAPIKey("  "); got != "" {
		t.Fatalf("blank explicit key with empty env = %q, want empty", got)
	}
	if err := os.Setenv("OPENALEX_API_KEY", " from-env "); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	defer func() { _ = os.Unsetenv("OPENALEX_API_KEY") }()

	if got := ResolveAPIKey(""); got != "from-env" {
		t.Fatalf("env fallback = %q, want from-env", got)
	}
	if got := ResolveAPIKey("explicit"); got != "explicit" {
		t.Fatalf("explicit must win over env, got %q", got)
	}
}
