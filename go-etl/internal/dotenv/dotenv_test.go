package dotenv

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func TestParse(t *testing.T) {
	t.Parallel()

	path := writeFile(t, `
# comment
OPENALEX_API_KEY=secret-key
export ETL_WORKERS = 8
QUOTED="  spaced value  "
SINGLE='single'
EMPTY=
WITH_EQUALS=a=b=c
`)
	pairs, err := Parse(path)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := map[string]string{
		"OPENALEX_API_KEY": "secret-key",
		"ETL_WORKERS":      "8",
		"QUOTED":           "  spaced value  ",
		"SINGLE":           "single",
		"EMPTY":            "",
		"WITH_EQUALS":      "a=b=c",
	}
	for k, v := range want {
		if got := pairs[k]; got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if len(pairs) != len(want) {
		t.Errorf("pairs = %d, want %d (%v)", len(pairs), len(want), pairs)
	}
}

func TestParseInvalidKey(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "BAD-KEY=1\n")
	if _, err := Parse(path); err == nil {
		t.Fatal("expected an error for an invalid key")
	}
}

func TestParseMalformedLine(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "GOOD=1\nthis line has no equals sign\n")
	if _, err := Parse(path); err == nil {
		t.Fatal("expected an error for a line without KEY=VALUE")
	}
}

func TestLoadDoesNotOverrideRealEnv(t *testing.T) {
	path := writeFile(t, "ETL_TEST_KEY=from-file\nETL_TEST_ONLY_FILE=file-only\n")
	t.Setenv("ETL_TEST_KEY", "from-env")

	applied, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied = %d, want 1 (the existing variable must win)", applied)
	}
	if got := os.Getenv("ETL_TEST_KEY"); got != "from-env" {
		t.Fatalf("ETL_TEST_KEY = %q, want from-env", got)
	}
	if got := os.Getenv("ETL_TEST_ONLY_FILE"); got != "file-only" {
		t.Fatalf("ETL_TEST_ONLY_FILE = %q", got)
	}
	_ = os.Unsetenv("ETL_TEST_ONLY_FILE")
}

func TestLoadDefaultMissingFileIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })
	t.Setenv(EnvFileVar, "")

	applied, path, err := LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault: %v", err)
	}
	if applied != 0 || path != "" {
		t.Fatalf("applied=%d path=%q, want 0/\"\"", applied, path)
	}
}

func TestLoadDefaultUsesEnvFileVar(t *testing.T) {
	path := writeFile(t, "ETL_FROM_CUSTOM=yes\n")
	t.Setenv(EnvFileVar, path)
	_ = os.Unsetenv("ETL_FROM_CUSTOM")

	applied, usedPath, err := LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault: %v", err)
	}
	if applied != 1 || usedPath != path {
		t.Fatalf("applied=%d path=%q", applied, usedPath)
	}
	if got := os.Getenv("ETL_FROM_CUSTOM"); got != "yes" {
		t.Fatalf("ETL_FROM_CUSTOM = %q", got)
	}
	_ = os.Unsetenv("ETL_FROM_CUSTOM")
}

func TestParseToleratesUTF8BOM(t *testing.T) {
	t.Parallel()

	path := writeFile(t, "\ufeff# comment\nETL_BOM_KEY=1\n")
	pairs, err := Parse(path)
	if err != nil {
		t.Fatalf("Parse with BOM: %v", err)
	}
	if pairs["ETL_BOM_KEY"] != "1" {
		t.Fatalf("pairs = %v", pairs)
	}
}
