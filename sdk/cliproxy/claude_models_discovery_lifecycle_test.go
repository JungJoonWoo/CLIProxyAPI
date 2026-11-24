package cliproxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// trace: gateway-auto-models-20260929; independent lifecycle regressions.
type claudeDiscoveryBlockedBody struct{ ctx context.Context }

func (b claudeDiscoveryBlockedBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (claudeDiscoveryBlockedBody) Close() error { return nil }
func TestClaudeModelDiscovery_WholeAccountDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, a, cycle := claudeDiscoveryFixture(t, &config.Config{})
		b := a.Clone()
		b.ID += "-second"
		b.Metadata["access_token"] = "sk-ant-oat01-lifecycle-second"
		if _, err := s.coreManager.Register(context.Background(), b); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(b.ID); s.coreManager.Remove(context.Background(), b.ID) })
		seed := context.WithValue(context.Background(), "cliproxy.roundtripper", claudeDiscoveryTransport(func(*http.Request) (*http.Response, error) {
			return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryFuture}, false, ""))
		}))
		cycle.refreshClaudeModelDiscovery(seed)
		claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryFuture)
		claudeDiscoveryRequireModel(t, b.ID, claudeDiscoveryFuture)
		first, calls := "", 0
		start := time.Now()
		var deadline time.Time
		var blocked context.Context
		ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", claudeDiscoveryTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			if calls > 4 {
				return nil, errors.New("unexpected extra page")
			}
			bearer := r.Header.Get("Authorization")
			if first == "" {
				first = bearer
			}
			if bearer != first {
				if blocked == nil || !errors.Is(blocked.Err(), context.DeadlineExceeded) {
					t.Error("next account fetched before first account timed out")
				}
				return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryNext}, false, ""))
			}
			d, ok := r.Context().Deadline()
			if !ok || d.After(start.Add(30*time.Second)) || !d.After(time.Now()) {
				t.Error("metadata fetch requires a finite whole-account deadline within 30s")
				return nil, errors.New("invalid deadline; avoid blocking baseline")
			}
			if r.URL.Query().Get("after_id") == "" {
				deadline = d
				time.Sleep(time.Second) // Virtual time only, inside the synctest bubble.
				return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryNext}, true, claudeDiscoveryNext))
			}
			if d.After(deadline) {
				t.Error("pagination extended the account deadline")
				return nil, errors.New("extended deadline")
			}
			blocked = r.Context()
			response, _ := claudeDiscoveryResponse(200, "")
			response.Body = claudeDiscoveryBlockedBody{blocked}
			return response, nil
		}))
		cycle.refreshClaudeModelDiscovery(ctx)
		if time.Since(start) > 30*time.Second {
			t.Error("account fetch exceeded 30s")
		}
		if blocked == nil {
			t.Error("stuck response body was not exercised")
		}
		failed, healthy := a, b
		if first == "Bearer "+b.Metadata["access_token"].(string) {
			failed, healthy = b, a
		}
		s.registerModelsForAuth(context.Background(), failed)
		claudeDiscoveryRequireModel(t, failed.ID, claudeDiscoveryFuture)
		if claudeDiscoveryModels(t, failed.ID)[claudeDiscoveryNext] != nil {
			t.Error("partial page replaced last-good cache")
		}
		claudeDiscoveryRequireModel(t, healthy.ID, claudeDiscoveryNext)
		cycle.refreshClaudeModelDiscovery(seed)
		claudeDiscoveryRequireModel(t, healthy.ID, claudeDiscoveryFuture)
	})
}
func TestClaudeModelDiscovery_HomeCommitCancelsAndResumes(t *testing.T) {
	s, a, cycle := claudeDiscoveryFixture(t, &config.Config{})
	homeEnabledCfg := &config.Config{}
	homeEnabledCfg.Home.Enabled = true
	calls := 0
	ctx := claudeDiscoveryContext(t, a.Metadata["access_token"].(string), func(r *http.Request) (*http.Response, error) {
		calls++
		s.commitConfigUpdate(homeEnabledCfg)
		if !errors.Is(r.Context().Err(), context.Canceled) {
			t.Error("Home commit did not immediately cancel in-flight discovery")
		}
		return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryFuture}, false, ""))
	})
	cycle.refreshClaudeModelDiscovery(ctx)
	if calls != 1 {
		t.Fatalf("in-flight Home mutation calls=%d, want 1", calls)
	}
	if claudeDiscoveryModels(t, a.ID)[claudeDiscoveryFuture] != nil {
		t.Error("Home transition published a late response")
	}
	s.commitConfigUpdate(&config.Config{})
	s.registerModelsForAuth(context.Background(), a)
	if claudeDiscoveryModels(t, a.ID)[claudeDiscoveryFuture] != nil {
		t.Error("late response poisoned the account cache")
	}
	good := claudeDiscoveryContext(t, a.Metadata["access_token"].(string), func(*http.Request) (*http.Response, error) {
		return claudeDiscoveryResponse(200, claudeDiscoveryPage([]string{claudeDiscoveryNext}, false, ""))
	})
	cycle.refreshClaudeModelDiscovery(good)
	claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryNext)
}
func TestClaudeModelDiscovery_ThinkingCapabilityEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, thinking   string
		manual, adaptive bool
	}{
		{"missing", "", false, false},
		{"null", "null", false, false},
		{"false", `{"supported":false,"types":{"adaptive":{"supported":true}}}`, false, false},
		{"missing-supported", `{"types":{"adaptive":{"supported":true}}}`, false, false},
		{"missing-adaptive", `{"supported":true}`, false, false},
		{"false-adaptive", `{"supported":true,"types":{"adaptive":{"supported":false}}}`, false, false},
		{"manual", `{"supported":true,"types":{"enabled":{"supported":true}}}`, true, false},
		{"adaptive", `{"supported":true,"types":{"adaptive":{"supported":true}}}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, a, cycle := claudeDiscoveryFixture(t, &config.Config{})
			var page map[string]any
			if err := json.Unmarshal([]byte(claudeDiscoveryPage([]string{claudeDiscoveryFuture}, false, "")), &page); err != nil {
				t.Fatal(err)
			}
			caps := page["data"].([]any)[0].(map[string]any)["capabilities"].(map[string]any)
			delete(caps, "thinking")
			if tc.thinking != "" {
				var thinking any
				if err := json.Unmarshal([]byte(tc.thinking), &thinking); err != nil {
					t.Fatal(err)
				}
				caps["thinking"] = thinking
			}
			body, err := json.Marshal(page)
			if err != nil {
				t.Fatal(err)
			}
			ctx := claudeDiscoveryContext(t, a.Metadata["access_token"].(string), func(*http.Request) (*http.Response, error) { return claudeDiscoveryResponse(200, string(body)) })
			cycle.refreshClaudeModelDiscovery(ctx)
			thinking := claudeDiscoveryRequireModel(t, a.ID, claudeDiscoveryFuture).Thinking
			if tc.adaptive {
				if thinking == nil || !reflect.DeepEqual(thinking.Levels, []string{"low", "medium", "high", "xhigh", "max"}) {
					t.Fatalf("explicit adaptive levels lost: %+v", thinking)
				}
			} else if thinking != nil && len(thinking.Levels) != 0 {
				t.Errorf("effort alone invented adaptive levels: %+v", thinking)
			}
			if tc.manual && (thinking == nil || thinking.Min != 0 || thinking.Max != 0 || thinking.ZeroAllowed || thinking.DynamicAllowed) {
				t.Errorf("manual thinking must be retained without invented budgets: %+v", thinking)
			}
		})
	}
}
