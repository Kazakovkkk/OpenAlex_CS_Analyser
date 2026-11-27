// Package config loads the distribution config file (config/app.toml).
//
// The file is owned by the Python distributor: it carries sections Go does not
// need ([compose], [actions]) next to the ones it does, so unknown keys are
// ignored on purpose.
//
// Passwords never live in the file. It only names environment variables
// (*_password_env); those names are resolved at connect time into Secret
// values that redact themselves when printed, so a Config can be logged.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// DefaultPath is the distribution path from the spec.
const DefaultPath = "config/app.toml"

// DefaultPort is ClickHouse's native TCP port.
const DefaultPort = 9000

// DefaultDatabase is the MVP database name from schema.sql.
const DefaultDatabase = "openalex"

var (
	identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	envNameRE    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// Config mirrors the subset of config/app.toml that Go reads.
type Config struct {
	ClickHouse ClickHouse `toml:"clickhouse"`
	GoETL      GoETL      `toml:"go_etl"`
}

// ClickHouse is the [clickhouse] section. Password fields hold env var names.
type ClickHouse struct {
	Host          string `toml:"host"`
	Port          int    `toml:"port"`
	Database      string `toml:"database"`
	Secure        bool   `toml:"secure"`
	ReaderUser    string `toml:"reader_user"`
	ReaderPassEnv string `toml:"reader_password_env"`

	PipelineWriterUser string `toml:"pipeline_writer_user"`
	// PythonPipelineWriterPasswordEnv belongs to the Python side; Go parses it
	// only to check that the file is complete, and never reads that variable.
	PythonPipelineWriterPasswordEnv string `toml:"python_pipeline_writer_password_env"`
}

// GoETL is the [go_etl] section.
type GoETL struct {
	BaseURL string `toml:"base_url"`

	ETLWriterUser    string `toml:"etl_writer_user"`
	ETLWriterPassEnv string `toml:"etl_writer_password_env"`

	GoPipelineWriterPassEnv string `toml:"go_pipeline_writer_password_env"`
}

// Load reads and validates a config file. An empty path means "no ClickHouse":
// the caller keeps the diagnostic NoopSink and Load returns ErrNoPath.
func Load(path string) (*Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrNoPath
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var cfg Config
	// Unknown keys are ignored: [compose] and [actions] belong to Python.
	if err := toml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return &cfg, nil
}

// ErrNoPath reports that no config file was given.
var ErrNoPath = errors.New("config: no config path")

// Validate applies defaults and rejects an unusable file.
func (c *Config) Validate() error {
	ch := &c.ClickHouse
	ch.Host = strings.TrimSpace(ch.Host)
	if ch.Host == "" {
		return errors.New("clickhouse.host is required")
	}
	if ch.Port == 0 {
		ch.Port = DefaultPort
	}
	if ch.Port < 1 || ch.Port > 65535 {
		return fmt.Errorf("clickhouse.port %d is out of range", ch.Port)
	}
	ch.Database = strings.TrimSpace(ch.Database)
	if ch.Database == "" {
		ch.Database = DefaultDatabase
	}
	if !identifierRE.MatchString(ch.Database) {
		return fmt.Errorf("clickhouse.database %q is not a valid identifier", ch.Database)
	}
	ch.PipelineWriterUser = strings.TrimSpace(ch.PipelineWriterUser)
	if ch.PipelineWriterUser == "" {
		return errors.New("clickhouse.pipeline_writer_user is required")
	}
	c.GoETL.ETLWriterUser = strings.TrimSpace(c.GoETL.ETLWriterUser)
	if c.GoETL.ETLWriterUser == "" {
		return errors.New("go_etl.etl_writer_user is required")
	}
	if err := checkEnvName("go_etl.etl_writer_password_env", c.GoETL.ETLWriterPassEnv); err != nil {
		return err
	}
	if err := checkEnvName("go_etl.go_pipeline_writer_password_env", c.GoETL.GoPipelineWriterPassEnv); err != nil {
		return err
	}
	// Optional: only checked for shape when present.
	if c.ClickHouse.PythonPipelineWriterPasswordEnv != "" {
		if !envNameRE.MatchString(c.ClickHouse.PythonPipelineWriterPasswordEnv) {
			return fmt.Errorf("clickhouse.python_pipeline_writer_password_env %q is not a valid environment variable name",
				c.ClickHouse.PythonPipelineWriterPasswordEnv)
		}
	}
	if c.GoETL.ETLWriterPassEnv == c.GoETL.GoPipelineWriterPassEnv {
		return fmt.Errorf("go_etl: etl_writer_password_env and go_pipeline_writer_password_env must differ, both are %q",
			c.GoETL.ETLWriterPassEnv)
	}
	return nil
}

func checkEnvName(field, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !envNameRE.MatchString(name) {
		return fmt.Errorf("%s %q is not a valid environment variable name", field, name)
	}
	return nil
}

// Addr returns host:port for the native protocol.
func (c *Config) Addr() string {
	port := c.ClickHouse.Port
	if port == 0 {
		port = DefaultPort
	}
	return c.ClickHouse.Host + ":" + strconv.Itoa(port)
}

// ETLWriterCredentials resolves the data-writing user.
func (c *Config) ETLWriterCredentials() (string, Secret, error) {
	secret, err := LookupSecret(c.GoETL.ETLWriterPassEnv)
	return c.GoETL.ETLWriterUser, secret, err
}

// PipelineWriterCredentials resolves the management-session user.
func (c *Config) PipelineWriterCredentials() (string, Secret, error) {
	secret, err := LookupSecret(c.GoETL.GoPipelineWriterPassEnv)
	return c.ClickHouse.PipelineWriterUser, secret, err
}

// Secret is a resolved password. It redacts itself under any fmt verb, so a
// struct containing it can be logged without leaking the value.
type Secret struct {
	name  string
	value string
}

// LookupSecret reads envName from the environment.
func LookupSecret(envName string) (Secret, error) {
	envName = strings.TrimSpace(envName)
	if envName == "" {
		return Secret{}, errors.New("config: empty password environment variable name")
	}
	value, ok := os.LookupEnv(envName)
	if !ok || value == "" {
		return Secret{name: envName}, fmt.Errorf("config: environment variable %s is not set", envName)
	}
	return Secret{name: envName, value: value}, nil
}

// Value returns the plaintext password. Call it only where the driver expects it.
func (s Secret) Value() string { return s.value }

// Set reports whether a value was resolved.
func (s Secret) Set() bool { return s.value != "" }

// Name returns the environment variable the secret came from.
func (s Secret) Name() string { return s.name }

// String always redacts.
func (s Secret) String() string {
	if s.name == "" {
		return "<secret>"
	}
	if s.value == "" {
		return "<unset:" + s.name + ">"
	}
	return "<redacted:" + s.name + ">"
}

// GoString keeps %#v from printing the value.
func (s Secret) GoString() string { return s.String() }

// Redacted returns a log-safe summary of the ClickHouse connection.
func (c *Config) Redacted() string {
	return fmt.Sprintf("host=%s database=%s secure=%t etl_writer=%s pipeline_writer=%s",
		c.Addr(), c.ClickHouse.Database, c.ClickHouse.Secure,
		c.GoETL.ETLWriterUser, c.ClickHouse.PipelineWriterUser)
}
