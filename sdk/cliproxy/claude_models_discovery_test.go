package cliproxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	registry "github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// trace: gateway-auto-models-20260929; pre_gate=REVISED; mode=IMPLEMENTATION_ONLY.
// The private cycle is the contract, not any production cache representation.
// Startup/ticker wiring is intentionally left to the parent's lifecycle tests.
type claudeDiscoveryCycle interface{ refreshClaudeModelDiscovery(context.Context) }
type claudeDiscoveryTransport func(*http.Request) (*http.Response, error)

func (f claudeDiscoveryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

const claudeDiscoveryFuture = "claude-sonnet-2099-7-discovery-test"
const claudeDiscoveryNext = "claude-opus-2099-8-discovery-test"

func claudeDiscoveryFixture(t *testing.T, cfg *config.Config) (*Service, *coreauth.Auth, claudeDiscoveryCycle) {
	t.Helper()
	s := &Service{cfg: cfg, coreManager: coreauth.NewManager(nil, nil, nil)}
	cycle, ok := any(s).(claudeDiscoveryCycle)
	if !ok {
		t.Fatal("missing feature: *Service must implement refreshClaudeModelDiscovery(context.Context)")
	}
	s.coreManager.RegisterExecutor(executor.NewClaudeExecutor(cfg))
	a := &coreauth.Auth{ID: "claude-discovery-" + t.Name(), Provider: "claude", Status: coreauth.StatusActive,
		Attributes: map[string]string{"auth_kind": "oauth"}, Metadata: map[string]any{"access_token": "sk-ant-oat01-discovery-fake-token"}}
	registered, err := s.coreManager.Register(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	GlobalModelRegistry().UnregisterClient(a.ID)
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(a.ID); s.coreManager.Remove(context.Background(), a.ID) })
	return s, registered, cycle
}

func claudeDiscoveryContext(t *testing.T, token string, fn claudeDiscoveryTransport) context.Context {
	t.Helper()
	var calls atomic.Int32
	return context.WithValue(context.Background(), "cliproxy.roundtripper", claudeDiscoveryTransport(func(r *http.Request) (*http.Response, error) {
		// Also bounds broken pagination without a wall-clock timeout or live network.
		if calls.Add(1) > 24 {
			return nil, errors.New("discovery request budget exceeded")
		}
		if r.Method != http.MethodGet || r.URL.Scheme != "https" || r.URL.Host != "api.anthropic.com" || r.URL.Path != "/v1/models" {
			t.Errorf("unexpected discovery request: %s %s", r.Method, r.URL)
			return nil, errors.New("unexpected endpoint")
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("discovery did not use this account's fake OAuth bearer")
		}
		return fn(r)
	}))
}

func claudeDiscoveryResponse(status int, body string) (*http.Response, error) {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func claudeDiscoveryPage(ids []string, more bool, cursor string) string {
	data := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		levels := map[string]any{"supported": true}
		for _, level := range []string{"low", "medium", "high", "xhigh", "max"} {
			levels[level] = map[string]any{"supported": true}
		}
		data = append(data, map[string]any{"type": "model", "id": id, "display_name": "Discovered " + id,
			"created_at": "2026-09-28T00:00:00Z", "max_input_tokens": 1000000, "max_tokens": 128000,
			"capabilities": map[string]any{"effort": levels, "image_input": map[string]any{"supported": true}, "thinking": map[string]any{"supported": true, "types": map[string]any{"adaptive": map[string]any{"supported": true}}}}})
	}
	b, err := json.Marshal(map[string]any{"data": data, "has_more": more, "last_id": cursor})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func claudeDiscoveryModels(t *testing.T, id string) map[string]*registry.ModelInfo {
	t.Helper()
	out := map[string]*registry.ModelInfo{}
	for _, m := range registry.GetGlobalRegistry().GetModelsForClient(id) {
		if m == nil {
			t.Fatal("nil registry model")
		}
		if out[m.ID] != nil {
			t.Fatalf("duplicate registry ID %q", m.ID)
		}
		out[m.ID] = m
	}
	return out
}
func claudeDiscoveryRequireModel(t *testing.T, id, model string) *registry.ModelInfo {
	t.Helper()
	m := claudeDiscoveryModels(t, id)[model]
	if m == nil || !registry.GetGlobalRegistry().ClientSupportsModel(id, model) {
		t.Fatalf("discovered model %q is not routable for %q", model, id)
	}
	return m
}

func TestClaudeModelDiscovery_PaginationMergeMetadataAndChangedCycle(t *testing.T) {
	s, a, cycle := claudeDiscoveryFixture(t, &config.Config{})
	static := registry.GetClaudeModels()
	if len(static) == 0 {
		t.Fatal("expected static Claude catalog")
	}
	var requests atomic.Int32
	second := false
	ctx := claudeDiscoveryContext(t, a.Metadata["access_token"].(string), func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		cursor := r.URL.Query().Get("after_id")
		if cursor == "" {
			return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{static[0].ID, claudeDiscoveryFuture}, true, claudeDiscoveryFuture))
		}
		if cursor != claudeDiscoveryFuture {
			t.Errorf("after_id=%q, want previous last_id", cursor)
			return claudeDiscoveryResponse(400, `{}`)
		}
		ids := []string{claudeDiscoveryFuture}
		if second {
			ids = append(ids, claudeDiscoveryNext)
		}
		return claudeDiscoveryResponse(200, claudeDiscoveryPage(ids, false, ids[len(ids)-1]))
	})
	s.registerModelsForAuth(ctx, a)
	before := claudeDiscoveryModels(t, a.ID)
	if requests.Load() != 0 {
		t.Fatal("registration performed discovery HTTP")
	}
	if before[claudeDiscoveryFuture] != nil {
		t.Fatal("future fixture ID must not be static")
	}
	cycle.refreshClaudeModelDiscovery(ctx)
	if requests.Load() != 2 {
		t.Fatalf("wanted two paginated requests, got %d", requests.Load())
	}
	m := claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryFuture)
	if m.DisplayName != "Discovered "+claudeDiscoveryFuture || m.ContextLength != 1000000 || m.MaxCompletionTokens != 128000 {
		t.Fatalf("official metadata not mapped: %+v", m)
	}
	if m.Thinking == nil || !reflect.DeepEqual(m.Thinking.Levels, []string{"low", "medium", "high", "xhigh", "max"}) {
		t.Fatalf("official effort levels not mapped: %+v", m.Thinking)
	}
	image := false
	for _, modality := range m.SupportedInputModalities {
		image = image || strings.EqualFold(modality, "image")
	}
	if !image {
		t.Fatal("supported image_input capability was lost")
	}
	after := claudeDiscoveryModels(t, a.ID)
	for id, old := range before {
		if !reflect.DeepEqual(old, after[id]) {
			t.Fatalf("static metadata changed or ID removed: %s", id)
		}
	}
	second = true
	cycle.refreshClaudeModelDiscovery(ctx)
	claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryNext)
	if requests.Load() != 4 {
		t.Fatalf("second cycle did not fetch both pages: %d", requests.Load())
	}
	s.registerModelsForAuth(ctx, a)
	if requests.Load() != 4 {
		t.Fatal("re-registration fetched instead of using account snapshot")
	}
	claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryFuture)
	claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryNext)
}

func TestClaudeModelDiscovery_LastGoodAndCooldownPreserved(t *testing.T) {
	cases := []struct {
		name           string
		status         int
		body           string
		transportError bool
		paginated      bool
	}{
		{name: "unchanged", status: 200, body: claudeDiscoveryPage([]string{claudeDiscoveryFuture}, false, claudeDiscoveryFuture)},
		{name: "http-error", status: 503, body: `{}`},
		{name: "transport-error", transportError: true},
		{name: "empty", status: 200, body: claudeDiscoveryPage(nil, false, "")},
		{name: "malformed-json", status: 200, body: `{"data":`},
		{name: "missing-data", status: 200, body: `{"has_more":false}`},
		{name: "invalid-model", status: 200, body: `{"data":[{"type":"model","id":""}],"has_more":false}`},
		{name: "missing-cursor", status: 200, body: claudeDiscoveryPage([]string{claudeDiscoveryNext}, true, "")},
		{name: "page-two-error", status: 503, body: `{}`, paginated: true},
		{name: "page-two-empty", status: 200, body: claudeDiscoveryPage(nil, false, ""), paginated: true},
		{name: "page-two-malformed", status: 200, body: `{`, paginated: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, a, cycle := claudeDiscoveryFixture(t, &config.Config{})
			token := a.Metadata["access_token"].(string)
			good := claudeDiscoveryContext(t, token, func(*http.Request) (*http.Response, error) {
				return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryFuture}, false, claudeDiscoveryFuture))
			})
			cycle.refreshClaudeModelDiscovery(good)
			claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryFuture)
			before := claudeDiscoveryModels(t, a.ID)
			reg := registry.GetGlobalRegistry()
			reg.SetModelQuotaExceeded(a.ID, claudeDiscoveryFuture)
			reg.SuspendClientModel(a.ID, claudeDiscoveryFuture, "test-cooldown")
			var calls atomic.Int32
			bad := claudeDiscoveryContext(t, token, func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				if tc.paginated && r.URL.Query().Get("after_id") == "" {
					return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryNext}, true, claudeDiscoveryNext))
				}
				if tc.transportError {
					return nil, errors.New("synthetic transport failure")
				}
				return claudeDiscoveryResponse(tc.status, tc.body)
			})
			cycle.refreshClaudeModelDiscovery(bad)
			if calls.Load() == 0 {
				t.Fatal("cycle did not attempt refresh")
			}
			if tc.paginated && calls.Load() < 2 {
				t.Fatal("second page was never requested")
			}
			if !reflect.DeepEqual(before, claudeDiscoveryModels(t, a.ID)) {
				t.Fatal("unsuccessful/unchanged cycle replaced last-good registry snapshot")
			}
			if !reg.IsModelQuotaExceededForClient(a.ID, claudeDiscoveryFuture) || !reg.IsModelSuspendedForClient(a.ID, claudeDiscoveryFuture) {
				t.Fatal("unsuccessful/unchanged cycle cleared cooldown")
			}
			// Read back through registration too: retaining only the registry is insufficient.
			s.registerModelsForAuth(bad, a)
			if !reflect.DeepEqual(before, claudeDiscoveryModels(t, a.ID)) {
				t.Fatal("last-good account cache was not preserved")
			}
		})
	}
}

func TestClaudeModelDiscovery_ExclusionsAndExplicitModels(t *testing.T) {
	for _, mode := range []string{"global-exclusion", "attribute-exclusion", "explicit-models"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{}
			if mode == "global-exclusion" {
				cfg.OAuthExcludedModels = map[string][]string{"claude": {claudeDiscoveryNext}}
			}
			if mode == "explicit-models" {
				cfg.ClaudeKey = []config.ClaudeKey{{BaseURL: "https://api.anthropic.com", Models: []internalconfig.ClaudeModel{{Name: "manual-upstream", Alias: "manual-only"}}}}
			}
			s, a, cycle := claudeDiscoveryFixture(t, cfg)
			if mode == "attribute-exclusion" {
				a.Attributes["excluded_models"] = claudeDiscoveryNext
			}
			if mode == "explicit-models" {
				a.Attributes["base_url"] = "https://api.anthropic.com"
			}
			var err error
			a, err = s.coreManager.Update(context.Background(), a)
			if err != nil {
				t.Fatal(err)
			}
			ctx := claudeDiscoveryContext(t, a.Metadata["access_token"].(string), func(*http.Request) (*http.Response, error) {
				return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryFuture, claudeDiscoveryNext}, false, claudeDiscoveryNext))
			})
			s.registerModelsForAuth(ctx, a)
			cycle.refreshClaudeModelDiscovery(ctx)
			models := claudeDiscoveryModels(t, a.ID)
			if models[claudeDiscoveryNext] != nil {
				t.Fatal("discovery bypassed model filter")
			}
			if mode == "explicit-models" {
				if len(models) != 1 || models["manual-only"] == nil {
					t.Fatalf("manual allowlist replaced/expanded: %+v", models)
				}
			} else {
				claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryFuture)
			}
		})
	}
}

func TestClaudeModelDiscovery_AuthChangesDuringFetchDiscardLateResponse(t *testing.T) {
	for _, mode := range []string{"deleted", "disabled", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			s, a, cycle := claudeDiscoveryFixture(t, &config.Config{})
			s.registerModelsForAuth(context.Background(), a)
			var requests atomic.Int32
			// Mutating inside RoundTrip deterministically happens after the fetch snapshot
			// and before the response; no goroutines, sleeps, or timing assumptions.
			ctx := claudeDiscoveryContext(t, a.Metadata["access_token"].(string), func(*http.Request) (*http.Response, error) {
				requests.Add(1)
				if mode == "deleted" {
					s.coreManager.Remove(context.Background(), a.ID)
				} else {
					current, ok := s.coreManager.GetByID(a.ID)
					if !ok {
						t.Error("auth disappeared unexpectedly")
						return nil, errors.New("missing auth")
					}
					if mode == "disabled" {
						current.Disabled = true
						current.Status = coreauth.StatusDisabled
					} else {
						current.Metadata["access_token"] = "sk-ant-oat01-replacement-fake-token"
					}
					if _, err := s.coreManager.Update(context.Background(), current); err != nil {
						t.Error(err)
					}
				}
				GlobalModelRegistry().UnregisterClient(a.ID)
				return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryFuture}, false, claudeDiscoveryFuture))
			})
			cycle.refreshClaudeModelDiscovery(ctx)
			if requests.Load() == 0 {
				t.Fatal("cycle never exercised in-flight auth mutation")
			}
			if len(claudeDiscoveryModels(t, a.ID)) != 0 {
				t.Fatal("stale fetch response re-registered changed/deleted/disabled auth")
			}
			if mode == "replaced" {
				current, _ := s.coreManager.GetByID(a.ID)
				s.registerModelsForAuth(ctx, current)
				if claudeDiscoveryModels(t, a.ID)[claudeDiscoveryFuture] != nil {
					t.Fatal("stale fetch poisoned replacement account cache")
				}
			}
		})
	}
}

func TestClaudeModelDiscovery_SnapshotsArePerAuth(t *testing.T) {
	s, a, cycle := claudeDiscoveryFixture(t, &config.Config{})
	b := a.Clone()
	b.ID += "-second"
	b.Metadata["access_token"] = "sk-ant-oat01-second-fake-token"
	var err error
	b, err = s.coreManager.Register(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(b.ID); s.coreManager.Remove(context.Background(), b.ID) })
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", claudeDiscoveryTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Host != "api.anthropic.com" || r.URL.Path != "/v1/models" {
			t.Error("unexpected discovery endpoint")
			return nil, errors.New("unexpected endpoint")
		}
		switch r.Header.Get("Authorization") {
		case "Bearer " + a.Metadata["access_token"].(string):
			return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryFuture}, false, claudeDiscoveryFuture))
		case "Bearer " + b.Metadata["access_token"].(string):
			return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryNext}, false, claudeDiscoveryNext))
		default:
			t.Error("unknown account bearer")
			return nil, errors.New("unknown bearer")
		}
	}))
	cycle.refreshClaudeModelDiscovery(ctx)
	claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryFuture)
	claudeDiscoveryRequireModel(t, b.ID, claudeDiscoveryNext)
	if claudeDiscoveryModels(t, a.ID)[claudeDiscoveryNext] != nil || claudeDiscoveryModels(t, b.ID)[claudeDiscoveryFuture] != nil {
		t.Fatal("discovery cache leaked between accounts")
	}
}
