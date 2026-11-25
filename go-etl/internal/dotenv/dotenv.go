// Package dotenv loads a simple KEY=VALUE file into the process environment.
package dotenv

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

const DefaultFile = ".env"

const EnvFileVar = "ETL_ENV_FILE"

func Parse(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make(map[string]string, 16)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 16<<10), 1<<20)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		text := sc.Text()
		if lineNo == 1 {
			text = strings.TrimPrefix(text, "\ufeff")
		}
		key, value, ok, err := parseLine(text)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, lineNo, err)
		}
		if ok {
			out[key] = value
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return out, nil
}

func parseLine(raw string) (string, string, bool, error) {
	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false, nil
	}
	line = strings.TrimPrefix(line, "export ")

	key, value, found := strings.Cut(line, "=")
	if !found {
		return "", "", false, errors.New("expected KEY=VALUE")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", false, errors.New("empty key")
	}
	for i := range key {
		c := key[i]
		if c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (i > 0 && c >= '0' && c <= '9') {
			continue
		}
		return "", "", false, fmt.Errorf("invalid key %q", key)
	}

	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
			value = value[1 : len(value)-1]
		}
	}
	return key, value, true, nil
}

func LoadDefault() (applied int, path string, err error) {
	if custom := strings.TrimSpace(os.Getenv(EnvFileVar)); custom != "" {
		applied, err = Load(custom)
		return applied, custom, err
	}
	if _, statErr := os.Stat(DefaultFile); statErr != nil {
		return 0, "", nil
	}
	applied, err = Load(DefaultFile)
	return applied, DefaultFile, err
}

func Load(path string) (int, error) {
	pairs, err := Parse(path)
	if err != nil {
		return 0, err
	}
	applied := 0
	for key, value := range pairs {
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return applied, fmt.Errorf("set %s: %w", key, err)
		}
		applied++
	}
	return applied, nil
}
