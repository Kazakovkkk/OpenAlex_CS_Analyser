package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type APIKeyResult struct {
	Message string `json:"message"`
	APIKey  string `json:"api_key"`
}

func (c *Client) RequestAPIKey(ctx context.Context, email string) (APIKeyResult, error) {
	email = strings.TrimSpace(email)
	if email == "" || !strings.Contains(email, "@") {
		return APIKeyResult{}, errors.New("api: a valid email is required to request an API key")
	}

	body, err := json.Marshal(map[string]string{"email": email})
	if err != nil {
		return APIKeyResult{}, err
	}
	payload, err := c.requestWithAttempts(ctx, http.MethodPost, c.opts.BaseURL+"/api-keys", body, "application/json", 1)
	if err != nil {
		return APIKeyResult{}, err
	}

	var out APIKeyResult
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &out); err != nil {
			out.Message = strings.TrimSpace(string(payload))
			if len(out.Message) > 500 {
				out.Message = out.Message[:500]
			}
		}
	}
	if out.Message == "" && out.APIKey == "" {
		out.Message = "request accepted"
	}
	if out.APIKey != "" {
		c.SetAPIKey(out.APIKey)
	}
	return out, nil
}

// apiKey returns the key under the read lock.
func (c *Client) apiKey() string {
	c.keyMu.RLock()
	defer c.keyMu.RUnlock()
	return c.opts.APIKey
}

func (c *Client) SetAPIKey(key string) {
	c.keyMu.Lock()
	c.opts.APIKey = strings.TrimSpace(key)
	c.keyMu.Unlock()
}

func (c *Client) HasAPIKey() bool { return c.apiKey() != "" }

func (c *Client) String() string {
	return fmt.Sprintf("openalex api %s (rps %.1f, key=%t)", c.opts.BaseURL, c.opts.RPSLimit, c.opts.APIKey != "")
}
