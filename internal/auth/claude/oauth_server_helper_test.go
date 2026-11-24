package claude

import (
	"context"
	"net"
	"testing"
)

func TestHelperOAuthServerBindAddress(t *testing.T) {
	for _, tc := range []struct {
		name     string
		helper   string
		wantHost string
	}{
		{name: "helper_loopback_only", helper: "1", wantHost: "127.0.0.1"},
		{name: "ordinary_unset", helper: "", wantHost: ""},
		{name: "ordinary_disabled", helper: "0", wantHost: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLIPROXY_CLAUDE_LOGIN_HELPER", tc.helper)
			// The OS selects a free port; no callback or real OAuth is performed.
			server := NewOAuthServer(0)
			t.Cleanup(func() {
				if errStop := server.Stop(context.Background()); errStop != nil {
					t.Errorf("Stop: %v", errStop)
				}
			})
			if errStart := server.Start(); errStart != nil {
				t.Fatalf("Start: %v", errStart)
			}
			select {
			case errServe := <-server.errorChan:
				t.Fatalf("listener failed: %v", errServe)
			default:
			}
			if !server.IsRunning() || server.server == nil {
				t.Fatal("Start did not create a running HTTP server")
			}
			// Inspect the existing HTTP server's actual ListenAndServe address.
			host, _, errAddr := net.SplitHostPort(server.server.Addr)
			if errAddr != nil {
				t.Fatalf("invalid server address %q: %v", server.server.Addr, errAddr)
			}
			if host != tc.wantHost {
				t.Errorf("CLIPROXY_CLAUDE_LOGIN_HELPER=%q: server.Addr=%q, host=%q; want %q",
					tc.helper, server.server.Addr, host, tc.wantHost)
			}
		})
	}
}
