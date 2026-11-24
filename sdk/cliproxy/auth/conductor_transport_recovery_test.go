package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"syscall"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type transportRecoveryExecutor struct {
	mu          sync.Mutex
	calls       int
	failure     error
	streamStage string
}

func (*transportRecoveryExecutor) Identifier() string { return "claude" }
func (e *transportRecoveryExecutor) attempt(ctx context.Context) bool {
	cliproxyexecutor.MarkUpstreamAttempt(ctx)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	return e.calls == 1
}
func (e *transportRecoveryExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}
func (e *transportRecoveryExecutor) Execute(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e.attempt(ctx) {
		return cliproxyexecutor.Response{}, e.failure
	}
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}
func (e *transportRecoveryExecutor) CountTokens(ctx context.Context, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return e.Execute(ctx, a, req, opts)
}
func (e *transportRecoveryExecutor) ExecuteStream(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	first := e.attempt(ctx)
	if first && e.streamStage == "before_stream" {
		return nil, e.failure
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	if first {
		if e.streamStage == "after_payload" {
			chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("partial")}
		}
		chunks <- cliproxyexecutor.StreamChunk{Err: e.failure}
	} else {
		chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("ok")}
	}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}
func (*transportRecoveryExecutor) Refresh(_ context.Context, a *Auth) (*Auth, error) { return a, nil }
func (*transportRecoveryExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func transportRecoveryInvoke(t *testing.T, m *Manager, kind string) (string, error) {
	t.Helper()
	// Each invocation is a new request; no pinned auth or request-scoped policy is injected.
	ctx := context.Background()
	req := cliproxyexecutor.Request{Model: "transport-recovery-model"}
	switch kind {
	case "execute":
		resp, err := m.Execute(ctx, []string{"claude"}, req, cliproxyexecutor.Options{})
		return string(resp.Payload), err
	case "count":
		resp, err := m.ExecuteCount(ctx, []string{"claude"}, req, cliproxyexecutor.Options{})
		return string(resp.Payload), err
	default:
		stream, err := m.ExecuteStream(ctx, []string{"claude"}, req, cliproxyexecutor.Options{Stream: true})
		if err != nil {
			return "", err
		}
		if stream == nil || stream.Chunks == nil {
			t.Fatal("successful ExecuteStream returned no stream")
		}
		var payload string
		var streamErr error
		// Drain through closure so MarkResult finishes before the next independent request.
		for chunk := range stream.Chunks {
			payload += string(chunk.Payload)
			if chunk.Err != nil {
				if streamErr != nil {
					t.Error("stream emitted more than one terminal error")
				}
				streamErr = chunk.Err
			}
		}
		return payload, streamErr
	}
}

// v8 replays pre-payload transport failures within the same request when
// request-retry allows it. Failures after payload has been emitted are never
// replayed. Either way the credential must stay available.
func TestTransportRecoveryPublicRequestsRetryBeforePayloadAndRecover(t *testing.T) {
	transportRecoveryEnableCooling(t)
	paths := []struct {
		name        string
		failure     error
		wantPayload string
	}{
		{"execute", transportRecoverySocket(syscall.EPIPE), ""},
		{"count", transportRecoverySocket(syscall.ECONNRESET), ""},
		{"before_stream", transportRecoverySocket(syscall.ETIMEDOUT), ""},
		{"before_payload", transportRecoverySocket(syscall.EPIPE), ""},
		{"after_payload", transportRecoverySocket(syscall.ECONNRESET), "partial"},
	}
	for _, path := range paths {
		for _, retry := range []int{0, 1, 3} {
			t.Run(fmt.Sprintf("%s/retry=%d", path.name, retry), func(t *testing.T) {
				m := NewManager(nil, nil, nil)
				m.SetRetryConfig(retry, 0, 0)
				executor := &transportRecoveryExecutor{failure: path.failure, streamStage: path.name}
				m.RegisterExecutor(executor)
				a := transportRecoveryRegister(t, m)
				reg := registry.GetGlobalRegistry()
				reg.RegisterClient(a.ID, a.Provider, []*registry.ModelInfo{{ID: "transport-recovery-model"}})
				t.Cleanup(func() { reg.UnregisterClient(a.ID) })

				retried := retry > 0 && path.name != "after_payload"
				wantFirstCalls := 1
				payload, err := transportRecoveryInvoke(t, m, path.name)
				if retried {
					wantFirstCalls = 2
					if err != nil || payload != "ok" {
						t.Errorf("first request = (%q, %v), want in-request retry success", payload, err)
					}
				} else {
					if !errors.Is(err, path.failure) {
						t.Errorf("first request error = %v, want original socket failure", err)
					}
					if payload != path.wantPayload {
						t.Errorf("first payload = %q, want %q", payload, path.wantPayload)
					}
				}
				if calls := executor.callCount(); calls != wantFirstCalls {
					t.Errorf("first request made %d executor calls, want %d with retry=%d", calls, wantFirstCalls, retry)
				}
				// Check availability before a successful request could clear failure state.
				t.Run("available_after_failure", func(t *testing.T) { assertNoCooldown(t, m, a.ID, "transport-recovery-model") })

				payload, err = transportRecoveryInvoke(t, m, path.name)
				if err != nil || payload != "ok" {
					t.Errorf("next independent request = (%q, %v), want immediate success", payload, err)
				}
				if calls := executor.callCount(); calls != wantFirstCalls+1 {
					t.Errorf("total executor calls = %d, want %d across independent requests", calls, wantFirstCalls+1)
				}
			})
		}
	}
}
