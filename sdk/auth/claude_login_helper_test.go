package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

// These tests never obtain tokens: rejected callbacks stop before exchange, and
// accepted synthetic codes meet a local proxy that refuses every CONNECT.
func TestHelperMachineEvents(t *testing.T) {
	for _, mode := range []string{"callback_rejected", "before_token_exchange_completes"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("CLIPROXY_CLAUDE_LOGIN_HELPER", "1")
			capturePath := filepath.Join(t.TempDir(), "helper.stdout")
			capture, errCreate := os.Create(capturePath)
			if errCreate != nil {
				t.Fatal(errCreate)
			}
			originalStdout := os.Stdout
			os.Stdout = capture
			t.Cleanup(func() {
				os.Stdout = originalStdout
				if errClose := capture.Close(); errClose != nil {
					t.Errorf("close capture: %v", errClose)
				}
			})

			type observation struct {
				output string
				method string
			}
			exchangeStarted := make(chan observation, 1)
			releaseExchange := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseExchange) }) }
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, _ := os.ReadFile(capturePath)
				exchangeStarted <- observation{string(data), r.Method}
				<-releaseExchange
				http.Error(w, "synthetic exchange blocked", http.StatusBadGateway)
			}))
			t.Cleanup(proxy.Close)
			t.Cleanup(release)

			reservation, errListen := net.Listen("tcp4", "127.0.0.1:0")
			if errListen != nil {
				t.Fatal(errListen)
			}
			port := reservation.Addr().(*net.TCPAddr).Port
			if errClose := reservation.Close(); errClose != nil {
				t.Fatal(errClose)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			t.Cleanup(cancel)
			finished := make(chan error, 1)
			cfg := &config.Config{AuthDir: t.TempDir()}
			cfg.ProxyURL = proxy.URL
			authenticator := &ClaudeAuthenticator{CallbackPort: port}
			go func() {
				_, errLogin := authenticator.Login(ctx, cfg, &LoginOptions{NoBrowser: true})
				finished <- errLogin
			}()
			// Join Login before restoring its stdout on every assertion path.
			t.Cleanup(func() {
				release()
				cancel()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("Login failed to stop after context cancellation")
				}
			})

			var first map[string]any
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
		waitURL:
			for {
				data, errRead := os.ReadFile(capturePath)
				if errRead != nil {
					t.Fatal(errRead)
				}
				for _, line := range strings.SplitAfter(string(data), "\n") {
					if !strings.HasSuffix(line, "\n") || strings.TrimSpace(line) == "" {
						continue
					}
					if errJSON := json.Unmarshal([]byte(line), &first); errJSON != nil {
						t.Fatalf("helper stdout is not newline JSON: %q", line)
					}
					break waitURL
				}
				select {
				case <-ctx.Done():
					t.Fatal("auth_url was not emitted while Login waits for a callback")
				case <-ticker.C:
				}
			}
			if first["type"] != "claude_oauth" || first["event"] != "auth_url" || first["expires_in"] != float64(300) {
				t.Fatalf("unexpected auth_url event: %v", first)
			}
			urlText, ok := first["url"].(string)
			if !ok {
				t.Fatal("auth_url does not contain a URL string")
			}
			authorize, errURL := url.Parse(urlText)
			if errURL != nil || authorize.Scheme != "https" || authorize.Host != "claude.ai" || authorize.Path != "/oauth/authorize" {
				t.Fatalf("unexpected authorize URL: %q (%v)", urlText, errURL)
			}
			query := authorize.Query()
			if query.Get("redirect_uri") != "http://localhost:54545/callback" || query.Get("state") == "" || query.Get("code_challenge") == "" {
				t.Fatalf("incomplete authorize parameters: %v", query)
			}
			callback := fmt.Sprintf("http://127.0.0.1:%d/callback?error=access_denied", port)
			if mode == "before_token_exchange_completes" {
				callback = fmt.Sprintf("http://127.0.0.1:%d/callback?code=synthetic-code&state=%s", port, url.QueryEscape(query.Get("state")))
			}
			client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil},
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			defer client.CloseIdleConnections()
			response, errCallback := client.Get(callback)
			if errCallback != nil {
				t.Fatal(errCallback)
			}
			if errClose := response.Body.Close(); errClose != nil {
				t.Fatal(errClose)
			}
			var output string
			if mode == "before_token_exchange_completes" {
				select {
				case seen := <-exchangeStarted:
					if seen.method != http.MethodConnect {
						t.Errorf("local blocker got %s; want CONNECT", seen.method)
					}
					output = seen.output
				case <-ctx.Done():
					t.Fatal("synthetic token exchange did not reach the local blocker")
				}
			} else {
				select {
				case errLogin := <-finished:
					finished <- errLogin
					if errLogin == nil {
						t.Error("rejected callback unexpectedly succeeded")
					}
				case <-ctx.Done():
					t.Fatal("rejected callback did not finish Login")
				}
				data, _ := os.ReadFile(capturePath)
				output = string(data)
				select {
				case <-exchangeStarted:
					t.Error("rejected callback attempted token exchange")
				default:
				}
			}
			lines := strings.Split(strings.TrimSuffix(output, "\n"), "\n")
			if !strings.HasSuffix(output, "\n") || len(lines) != 2 {
				t.Fatalf("want two immediately emitted newline JSON events; got %q", output)
			}
			var callbackEvent map[string]any
			if errJSON := json.Unmarshal([]byte(lines[1]), &callbackEvent); errJSON != nil {
				t.Fatal(errJSON)
			}
			if callbackEvent["type"] != "claude_oauth" || callbackEvent["event"] != "callback_received" || len(callbackEvent) != 2 {
				t.Errorf("unexpected callback event: %v", callbackEvent)
			}
		})
	}
}
