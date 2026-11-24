package cliproxy

import (
	"context"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type claudeDiscoveryPublicationHook struct{ run func() }

func (h *claudeDiscoveryPublicationHook) GetClientID() string {
	h.run()
	return ""
}

func TestClaudeModelDiscovery_RetriesCanceledPublication(t *testing.T) {
	s, a, cycle := claudeDiscoveryFixture(t, &config.Config{})
	future := a.ID + "-future"
	hookActivations, requests := 0, 0
	cacheObserved := false
	hook := &claudeDiscoveryPublicationHook{run: func() {
		if hookActivations != 0 {
			return
		}
		hookActivations++
		// Registration must reach Runtime only after caching the fetched snapshot.
		s.claudeModelsMu.RLock()
		snapshot := s.claudeModels[a.ID]
		s.claudeModelsMu.RUnlock()
		for _, model := range snapshot.models {
			if model != nil && model.ID == future {
				cacheObserved = true
			}
		}
		homeCfg := &config.Config{}
		homeCfg.Home.Enabled = true
		s.commitConfigUpdate(homeCfg)
	}}
	a.Runtime = hook
	if a.Clone().Runtime != hook {
		t.Fatal("Auth.Clone must preserve the Runtime hook")
	}
	var err error
	a, err = s.coreManager.Update(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	body := claudeDiscoveryPage([]string{future}, false, future)
	ctx := claudeDiscoveryContext(t, a.Metadata["access_token"].(string), func(*http.Request) (*http.Response, error) {
		requests++
		return claudeDiscoveryResponse(http.StatusOK, body)
	})

	cycle.refreshClaudeModelDiscovery(ctx)
	if hookActivations != 1 || !cacheObserved || requests != 1 {
		t.Fatalf("cancellation boundary not exercised: hook=%d cached=%t requests=%d", hookActivations, cacheObserved, requests)
	}
	if claudeDiscoveryModels(t, a.ID)[future] != nil {
		t.Fatal("Home cancellation did not prevent publication")
	}

	// Commit only: the next discovery cycle must perform the missing registration.
	s.commitConfigUpdate(&config.Config{})
	cycle.refreshClaudeModelDiscovery(ctx)
	if requests != 2 || hookActivations != 1 {
		t.Fatalf("expected identical refetch and one cancellation: requests=%d hook=%d", requests, hookActivations)
	}
	claudeDiscoveryRequireModel(t, a.ID, future)
}
