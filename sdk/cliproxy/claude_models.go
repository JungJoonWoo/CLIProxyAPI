package cliproxy

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	log "github.com/sirupsen/logrus"
)

type claudeModelSnapshot struct {
	fingerprint [32]byte
	models      []*ModelInfo
}

func claudeDiscoveryFingerprint(a *coreauth.Auth) [32]byte {
	// Token rotation and identity replacement must never inherit another snapshot.
	identity := []any{a.ID, a.RegistrationEpoch}
	for _, key := range []string{"access_token", "account_uuid", "organization_uuid", "email"} {
		value, _ := a.Metadata[key].(string)
		identity = append(identity, value)
	}
	data, _ := json.Marshal(identity)
	return sha256.Sum256(data)
}

func claudeDiscoveryEligible(a *coreauth.Auth, cfg *config.Config) bool {
	if (cfg != nil && cfg.Home.Enabled) || a == nil || a.ID == "" || a.Disabled || a.Status == coreauth.StatusDisabled ||
		!strings.EqualFold(strings.TrimSpace(a.Provider), "claude") || a.AuthKind() != coreauth.AuthKindOAuth {
		return false
	}
	token, _ := a.Metadata["access_token"].(string)
	if strings.TrimSpace(token) == "" || strings.TrimSpace(a.Attributes["api_key"]) != "" {
		return false
	}
	metadataBase, _ := a.Metadata["base_url"].(string)
	for _, base := range []string{a.Attributes["base_url"], metadataBase} {
		if base = strings.TrimRight(strings.TrimSpace(base), "/"); base != "" && base != "https://api.anthropic.com" {
			return false
		}
	}
	if entry := resolveConfigClaudeKeyForConfig(cfg, a); entry != nil {
		base := strings.TrimRight(strings.TrimSpace(entry.BaseURL), "/")
		if len(entry.Models) > 0 || entry.APIKey != "" || (base != "" && base != "https://api.anthropic.com") {
			return false
		}
	}
	return true
}

// mergeClaudeDiscoveredModels is cache-only; registration never initiates discovery.
func (s *Service) mergeClaudeDiscoveredModels(a *coreauth.Auth, cfg *config.Config, models []*ModelInfo) []*ModelInfo {
	if !claudeDiscoveryEligible(a, cfg) {
		return models
	}
	s.claudeModelsMu.RLock()
	snapshot, ok := s.claudeModels[a.ID]
	s.claudeModelsMu.RUnlock()
	if !ok || snapshot.fingerprint != claudeDiscoveryFingerprint(a) {
		return models
	}
	seen := make(map[string]bool, len(models))
	for _, m := range models {
		if m != nil {
			seen[m.ID] = true
		}
	}
	for _, m := range snapshot.models {
		if seen[m.ID] {
			continue
		}
		// Copy mutable fields so aliases, prefixes and callers cannot mutate the cache.
		clone := *m
		clone.SupportedInputModalities = append([]string(nil), m.SupportedInputModalities...)
		if m.Thinking != nil {
			thinking := *m.Thinking
			thinking.Levels = append([]string(nil), m.Thinking.Levels...)
			clone.Thinking = &thinking
		}
		models = append(models, &clone)
	}
	return models
}

func (s *Service) runClaudeModelDiscovery(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	for {
		s.refreshClaudeModelDiscovery(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) refreshClaudeModelDiscovery(ctx context.Context) {
	if s == nil || s.coreManager == nil || !s.claudeModelsCycleMu.TryLock() {
		return
	}
	defer s.claudeModelsCycleMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	s.cfgMu.RLock()
	if s.cfg != nil && s.cfg.Home.Enabled {
		s.cfgMu.RUnlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	s.claudeModelsMu.Lock()
	s.claudeDiscoveryCancel = cancel
	s.claudeModelsMu.Unlock()
	s.cfgMu.RUnlock()
	defer func() {
		cancel()
		s.claudeModelsMu.Lock()
		s.claudeDiscoveryCancel = nil
		s.claudeModelsMu.Unlock()
	}()
	for _, item := range s.coreManager.List() {
		if ctx.Err() != nil {
			return
		}
		a, exists := s.coreManager.GetByID(item.ID)
		if !exists {
			continue
		}
		s.cfgMu.RLock()
		cfg := s.cfg
		s.cfgMu.RUnlock()
		if !claudeDiscoveryEligible(a, cfg) || a.Status != coreauth.StatusActive {
			continue
		}
		fingerprint := claudeDiscoveryFingerprint(a)
		models := fetchClaudeDiscoveredModels(ctx, cfg, a)
		if len(models) == 0 || ctx.Err() != nil {
			continue
		}
		// Serialize publication with service-owned auth changes, never with network I/O.
		s.authUpdateMu.Lock()
		current, ok := s.coreManager.GetByID(a.ID)
		s.cfgMu.RLock()
		cfg = s.cfg
		s.cfgMu.RUnlock()
		if !ok || !claudeDiscoveryEligible(current, cfg) || current.Status != coreauth.StatusActive ||
			claudeDiscoveryFingerprint(current) != fingerprint || ctx.Err() != nil {
			s.authUpdateMu.Unlock()
			continue
		}
		snapshot := claudeModelSnapshot{fingerprint: fingerprint, models: models}
		s.claudeModelsMu.Lock()
		previous, existed := s.claudeModels[a.ID]
		changed := !reflect.DeepEqual(previous, snapshot)
		if changed {
			if s.claudeModels == nil {
				s.claudeModels = make(map[string]claudeModelSnapshot)
			}
			s.claudeModels[a.ID] = snapshot
		}
		s.claudeModelsMu.Unlock()
		registered := changed && s.refreshModelRegistrationForAuthWithContext(ctx, current, nil)
		if changed && !registered {
			s.claudeModelsMu.Lock()
			if existed {
				s.claudeModels[a.ID] = previous
			} else {
				delete(s.claudeModels, a.ID)
			}
			s.claudeModelsMu.Unlock()
		}
		s.authUpdateMu.Unlock()
		if registered {
			log.WithField("model_count", len(models)).Info("claude model discovery updated")
		}
	}
}

type claudeDiscoverySupport struct {
	Supported bool `json:"supported"`
}

type claudeDiscoveredModel struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	DisplayName    string `json:"display_name"`
	CreatedAt      string `json:"created_at"`
	MaxInputTokens int    `json:"max_input_tokens"`
	MaxTokens      int    `json:"max_tokens"`
	Capabilities   struct {
		Thinking struct {
			Supported bool `json:"supported"`
			Types     struct {
				Enabled  claudeDiscoverySupport `json:"enabled"`
				Adaptive claudeDiscoverySupport `json:"adaptive"`
			} `json:"types"`
		} `json:"thinking"`
		Effort struct {
			Supported                     bool `json:"supported"`
			Low, Medium, High, XHigh, Max claudeDiscoverySupport
		} `json:"effort"`
		ImageInput claudeDiscoverySupport `json:"image_input"`
	} `json:"capabilities"`
}

func fetchClaudeDiscoveredModels(ctx context.Context, cfg *config.Config, a *coreauth.Auth) []*ModelInfo {
	// Bound the complete metadata fetch, including pagination and response bodies.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	const maxPageBytes = 4 << 20
	// Only fixed reason codes and HTTP status are logged, never upstream error text.
	failure := func(reason string, status int) []*ModelInfo {
		entry := log.WithField("reason", reason)
		if status != 0 {
			entry = entry.WithField("http_status", status)
		}
		if ctx.Err() != nil {
			entry.Debug("claude model discovery canceled")
		} else {
			entry.Warn("claude model discovery failed; retaining last good snapshot")
		}
		return nil
	}
	models := make(map[string]*ModelInfo)
	cursors := make(map[string]bool)
	cursor := ""
	exec := executor.NewClaudeExecutor(cfg)
	for pageNumber := 0; pageNumber < 20; pageNumber++ {
		target := "https://api.anthropic.com/v1/models?limit=1000"
		if cursor != "" {
			target += "&after_id=" + url.QueryEscape(cursor)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return failure("request", 0)
		}
		req.Header.Set("anthropic-version", "2023-06-01")
		req.Header.Set("anthropic-beta", "oauth-2025-04-20")
		resp, err := exec.HttpRequest(ctx, a, req)
		if err != nil {
			return failure("fetch", 0)
		}
		if resp == nil || resp.Body == nil {
			return failure("empty_response", 0)
		}
		body, errRead := io.ReadAll(io.LimitReader(resp.Body, maxPageBytes+1))
		errClose := resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return failure("http_status", resp.StatusCode)
		}
		if errRead != nil || errClose != nil {
			return failure("body_io", resp.StatusCode)
		}
		if len(body) > maxPageBytes {
			return failure("body_limit", resp.StatusCode)
		}
		var page struct {
			Data    []claudeDiscoveredModel `json:"data"`
			HasMore *bool                   `json:"has_more"`
			LastID  string                  `json:"last_id"`
		}
		if json.Unmarshal(body, &page) != nil {
			return failure("parse", resp.StatusCode)
		}
		if len(page.Data) == 0 || page.HasMore == nil {
			return failure("empty_or_incomplete_page", resp.StatusCode)
		}
		for _, item := range page.Data {
			if item.Type != "model" || strings.TrimSpace(item.ID) == "" || item.ID != strings.TrimSpace(item.ID) ||
				item.MaxInputTokens < 0 || item.MaxTokens < 0 {
				return failure("invalid_model", resp.StatusCode)
			}
			m := &ModelInfo{ID: item.ID, Object: "model", OwnedBy: "anthropic", Type: "claude",
				DisplayName: item.DisplayName, ContextLength: item.MaxInputTokens, MaxCompletionTokens: item.MaxTokens,
				SupportedInputModalities: []string{"text"}}
			if created, err := time.Parse(time.RFC3339, item.CreatedAt); err == nil {
				m.Created = created.Unix()
			}
			if item.Capabilities.ImageInput.Supported {
				m.SupportedInputModalities = append(m.SupportedInputModalities, "image")
			}
			effort := item.Capabilities.Effort
			thinking := item.Capabilities.Thinking
			if thinking.Supported && (thinking.Types.Enabled.Supported || thinking.Types.Adaptive.Supported) {
				m.Thinking = &registry.ThinkingSupport{}
			}
			if thinking.Supported && thinking.Types.Adaptive.Supported && effort.Supported {
				levels := []string{}
				for i, support := range []claudeDiscoverySupport{effort.Low, effort.Medium, effort.High, effort.XHigh, effort.Max} {
					if support.Supported {
						levels = append(levels, []string{"low", "medium", "high", "xhigh", "max"}[i])
					}
				}
				if len(levels) > 0 {
					m.Thinking = &registry.ThinkingSupport{Levels: levels}
				}
			}
			models[m.ID] = m
		}
		if !*page.HasMore {
			result := make([]*ModelInfo, 0, len(models))
			for _, m := range models {
				result = append(result, m)
			}
			sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
			return result
		}
		if strings.TrimSpace(page.LastID) == "" || cursors[page.LastID] {
			return failure("invalid_cursor", resp.StatusCode)
		}
		cursor = page.LastID
		cursors[cursor] = true
	}
	return failure("page_limit", 0)
}
