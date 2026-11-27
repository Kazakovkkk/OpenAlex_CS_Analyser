package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fullTOML = `
[compose]
start_clickhouse = true
start_go_etl = true

[actions]
download_data = true
refresh_analysis = true
offer_shutdown_on_exit = true

[clickhouse]
host = "clickhouse"
port = 9000
database = "openalex"
secure = false
reader_user = "report_reader"
reader_password_env = "CLICKHOUSE_REPORT_READER_PASSWORD"
pipeline_writer_user = "pipeline_writer"
python_pipeline_writer_password_env = "CLICKHOUSE_PYTHON_WRITER_PASSWORD"

[go_etl]
base_url = "http://go-etl:8081"
etl_writer_user = "etl_writer"
etl_writer_password_env = "CLICKHOUSE_ETL_WRITER_PASSWORD"
go_pipeline_writer_password_env = "CLICKHOUSE_GO_WRITER_PASSWORD"

[unknown_future_section]
whatever = 1
`

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadFullFileIgnoresForeignSections(t *testing.T) {
	cfg, err := Load(writeTemp(t, fullTOML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.Addr(), "clickhouse:9000"; got != want {
		t.Fatalf("Addr() = %q, want %q", got, want)
	}
	if got, want := cfg.ClickHouse.Database, "openalex"; got != want {
		t.Fatalf("Database = %q, want %q", got, want)
	}
	if got, want := cfg.GoETL.ETLWriterUser, "etl_writer"; got != want {
		t.Fatalf("ETLWriterUser = %q, want %q", got, want)
	}
	if cfg.ClickHouse.Secure {
		t.Fatal("Secure = true, want false")
	}
}

func TestLoadNoPath(t *testing.T) {
	if _, err := Load(""); !errors.Is(err, ErrNoPath) {
		t.Fatalf("Load(\"\") error = %v, want ErrNoPath", err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.toml")); err == nil {
		t.Fatal("Load(missing) = nil error, want failure")
	}
}

func TestDefaults(t *testing.T) {
	body := `
[clickhouse]
host = "127.0.0.1"
pipeline_writer_user = "pipeline_writer"

[go_etl]
etl_writer_user = "etl_writer"
etl_writer_password_env = "ETL_PW"
go_pipeline_writer_password_env = "GO_PW"
`
	cfg, err := Load(writeTemp(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ClickHouse.Port != DefaultPort {
		t.Fatalf("Port = %d, want %d", cfg.ClickHouse.Port, DefaultPort)
	}
	if cfg.ClickHouse.Database != DefaultDatabase {
		t.Fatalf("Database = %q, want %q", cfg.ClickHouse.Database, DefaultDatabase)
	}
}

func TestValidationFailures(t *testing.T) {
	base := `
[clickhouse]
host = "h"
pipeline_writer_user = "pw"

[go_etl]
etl_writer_user = "ew"
etl_writer_password_env = "ETL_PW"
go_pipeline_writer_password_env = "GO_PW"
`
	cases := []struct {
		name    string
		mutate  func(string) string
		wantSub string
	}{
		{"empty host", func(s string) string { return strings.Replace(s, `host = "h"`, `host = ""`, 1) }, "clickhouse.host is required"},
		{"bad port", func(s string) string { return strings.Replace(s, "host = \"h\"", "host = \"h\"\nport = 70000", 1) }, "out of range"},
		{"bad database", func(s string) string {
			return strings.Replace(s, "host = \"h\"", "host = \"h\"\ndatabase = \"open-alex\"", 1)
		}, "not a valid identifier"},
		{"no pipeline user", func(s string) string {
			return strings.Replace(s, `pipeline_writer_user = "pw"`, `pipeline_writer_user = ""`, 1)
		}, "pipeline_writer_user is required"},
		{"no etl user", func(s string) string { return strings.Replace(s, `etl_writer_user = "ew"`, `etl_writer_user = ""`, 1) }, "etl_writer_user is required"},
		{"bad env name", func(s string) string {
			return strings.Replace(s, `etl_writer_password_env = "ETL_PW"`, `etl_writer_password_env = "1 BAD"`, 1)
		}, "not a valid environment variable name"},
		{"same env", func(s string) string {
			return strings.Replace(s, `go_pipeline_writer_password_env = "GO_PW"`, `go_pipeline_writer_password_env = "ETL_PW"`, 1)
		}, "must differ"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.mutate(base)))
			if err == nil {
				t.Fatal("Load = nil error, want failure")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not contain %q", err, tc.wantSub)
			}
		})
	}
}

func TestMalformedTOML(t *testing.T) {
	if _, err := Load(writeTemp(t, "[clickhouse\nhost=1")); err == nil {
		t.Fatal("Load(bad toml) = nil error, want failure")
	}
}

func TestCredentialsAndRedaction(t *testing.T) {
	t.Setenv("CLICKHOUSE_ETL_WRITER_PASSWORD", "s3cret-etl")
	t.Setenv("CLICKHOUSE_GO_WRITER_PASSWORD", "s3cret-go")

	cfg, err := Load(writeTemp(t, fullTOML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	user, pass, err := cfg.ETLWriterCredentials()
	if err != nil {
		t.Fatalf("ETLWriterCredentials: %v", err)
	}
	if user != "etl_writer" || pass.Value() != "s3cret-etl" || !pass.Set() {
		t.Fatalf("unexpected etl credentials: user=%q set=%t", user, pass.Set())
	}

	_, goPass, err := cfg.PipelineWriterCredentials()
	if err != nil {
		t.Fatalf("PipelineWriterCredentials: %v", err)
	}
	if goPass.Value() != "s3cret-go" {
		t.Fatalf("pipeline password = %q", goPass.Value())
	}

	for _, verb := range []string{"%v", "%+v", "%s", "%#v"} {
		rendered := fmt.Sprintf(verb, pass)
		if strings.Contains(rendered, "s3cret-etl") {
			t.Fatalf("verb %s leaked the password: %s", verb, rendered)
		}
	}
	if rendered := fmt.Sprintf("%v", cfg); strings.Contains(rendered, "s3cret-") {
		t.Fatalf("config print leaked a password: %s", rendered)
	}
	if red := cfg.Redacted(); strings.Contains(red, "s3cret-") {
		t.Fatalf("Redacted() leaked a password: %s", red)
	}
}

func TestLookupSecretUnset(t *testing.T) {
	if _, err := LookupSecret("ETL_SURELY_UNSET_VARIABLE"); err == nil {
		t.Fatal("LookupSecret(unset) = nil error, want failure")
	}
	if _, err := LookupSecret(""); err == nil {
		t.Fatal("LookupSecret(empty) = nil error, want failure")
	}
}

func TestSecureFlagParsed(t *testing.T) {
	body := strings.Replace(fullTOML, "secure = false", "secure = true", 1)
	cfg, err := Load(writeTemp(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.ClickHouse.Secure {
		t.Fatal("Secure = false, want true")
	}
}
