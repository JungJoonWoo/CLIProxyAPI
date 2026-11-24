package auth

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"
)

type transportRecoveryTimeout struct{}

func (transportRecoveryTimeout) Error() string   { return "socket read timed out" }
func (transportRecoveryTimeout) Timeout() bool   { return true }
func (transportRecoveryTimeout) Temporary() bool { return true }

func transportRecoverySocket(cause error) error {
	return fmt.Errorf("upstream transport: %w", &url.Error{
		Op: "Post", URL: "https://example.invalid/v1/messages",
		Err: &net.OpError{Op: "write", Net: "tcp", Err: &os.SyscallError{Syscall: "write", Err: cause}},
	})
}

func transportRecoveryErrors() []struct {
	name string
	err  error
} {
	return []struct {
		name string
		err  error
	}{
		{"EPIPE", transportRecoverySocket(syscall.EPIPE)},
		{"ECONNRESET", transportRecoverySocket(syscall.ECONNRESET)},
		{"ETIMEDOUT", transportRecoverySocket(syscall.ETIMEDOUT)},
		{"net_OpError_timeout", fmt.Errorf("upstream transport: %w", &url.Error{
			Op: "Post", URL: "https://example.invalid/v1/messages",
			Err: &net.OpError{Op: "read", Net: "tcp", Err: transportRecoveryTimeout{}},
		})},
	}
}

func transportRecoveryEnableCooling(t *testing.T) {
	t.Helper()
	previous := quotaCooldownDisabled.Swap(false)
	previousTransient := transientErrorCooldownSeconds.Load()
	SetTransientErrorCooldownSeconds(60)
	t.Cleanup(func() {
		quotaCooldownDisabled.Store(previous)
		transientErrorCooldownSeconds.Store(previousTransient)
	})
}

func transportRecoveryRegister(t *testing.T, m *Manager) *Auth {
	t.Helper()
	a := &Auth{ID: "transport-recovery-" + t.Name(), Provider: "claude", Metadata: map[string]any{"disable_cooling": false}}
	if _, err := m.Register(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if m.cooldownDisabledForAuth(a) {
		t.Fatal("fixture must have credential cooling enabled")
	}
	return a
}

func TestTransportRecoveryTypedErrorsDoNotCreateCooldown(t *testing.T) {
	transportRecoveryEnableCooling(t)
	for _, tc := range transportRecoveryErrors() {
		for _, model := range []string{"", "transport-recovery-model"} {
			t.Run(tc.name+"/model="+model, func(t *testing.T) {
				m := NewManager(nil, nil, nil)
				a := transportRecoveryRegister(t, m)
				resultErr := resultErrorFromError(tc.err)
				if resultErr == nil {
					t.Fatal("missing converted error")
				}
				if resultErr.HTTPStatus != 0 || resultErr.IsRequestScoped() || isRequestInvalidError(tc.err) {
					t.Fatalf("socket error must remain statusless and non-request-scoped: %#v", resultErr)
				}
				m.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: a.Provider, Model: model, Error: resultErr})
				assertNoCooldown(t, m, a.ID, model)
			})
		}
	}
}

func TestTransportRecoveryPreservesHomeLifecyclePolicy(t *testing.T) {
	for _, tc := range transportRecoveryErrors() {
		t.Run(tc.name, func(t *testing.T) {
			// Cooldown exemption must not broaden the shared Home lifecycle predicate.
			if isConnectionLifecycleError(tc.err) {
				t.Error("socket error changed the existing lifecycle classification")
			}
			if !shouldExcludeHomeAuthAfterStreamError(context.Background(), &Auth{Provider: "claude"}, tc.err) {
				t.Error("socket error must still exclude the Home auth within this request")
			}
		})
	}
}

type transportRecoveryStatusError struct {
	status int
	cause  error
}

func (e *transportRecoveryStatusError) Error() string   { return e.cause.Error() }
func (e *transportRecoveryStatusError) StatusCode() int { return e.status }
func (e *transportRecoveryStatusError) Unwrap() error   { return e.cause }

func TestTransportRecoveryCoolableErrorsStayCoolable(t *testing.T) {
	transportRecoveryEnableCooling(t)
	socketErr := transportRecoverySocket(syscall.EPIPE)
	cases := []struct {
		name string
		err  error
	}{
		{"timeout_text", errors.New("read tcp: timeout")},
		{"HTTP401", &transportRecoveryStatusError{http.StatusUnauthorized, socketErr}},
		{"HTTP429", &transportRecoveryStatusError{http.StatusTooManyRequests, socketErr}},
		{"HTTP500", &transportRecoveryStatusError{http.StatusInternalServerError, socketErr}},
		{"force_cooldown", errors.Join(&Error{Code: ErrorCodeForceCooldown, Message: "forced socket cooldown"}, socketErr)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			a := transportRecoveryRegister(t, m)
			const model = "transport-recovery-model"
			before := time.Now()
			converted := resultErrorFromError(tc.err)
			if shouldSkipCredentialCooldown(converted) {
				t.Errorf("unexpected cooldown exemption: %#v", converted)
			}
			m.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: a.Provider, Model: model, Error: converted})
			updated, _ := m.GetByID(a.ID)
			state := updated.ModelStates[model]
			if state == nil || !state.Unavailable || !state.NextRetryAfter.After(before) {
				t.Fatalf("existing cooldown policy was lost: %#v", state)
			}
		})
	}
}

func TestTransportRecoveryPreservesExistingQuotaCooldown(t *testing.T) {
	transportRecoveryEnableCooling(t)
	for _, model := range []string{"", "transport-recovery-model"} {
		t.Run("model="+model, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			a := transportRecoveryRegister(t, m)
			retryAfter := 5 * time.Minute
			m.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: a.Provider, Model: model,
				Error: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota exhausted"}, RetryAfter: &retryAfter})
			before, _ := m.GetByID(a.ID)
			if !before.Unavailable || !before.NextRetryAfter.After(time.Now()) {
				t.Fatal("fixture did not establish a quota cooldown")
			}
			m.MarkResult(context.Background(), Result{AuthID: a.ID, Provider: a.Provider, Model: model,
				Error: resultErrorFromError(transportRecoverySocket(syscall.ECONNRESET))})
			after, _ := m.GetByID(a.ID)
			if after.Unavailable != before.Unavailable || !after.NextRetryAfter.Equal(before.NextRetryAfter) ||
				after.Quota.Exceeded != before.Quota.Exceeded || after.Quota.Reason != before.Quota.Reason ||
				!after.Quota.NextRecoverAt.Equal(before.Quota.NextRecoverAt) {
				t.Errorf("socket error changed existing auth quota cooldown: before=%#v after=%#v", before, after)
			}
			if model != "" {
				b, a := before.ModelStates[model], after.ModelStates[model]
				if a == nil || a.Unavailable != b.Unavailable || !a.NextRetryAfter.Equal(b.NextRetryAfter) ||
					a.Quota.Exceeded != b.Quota.Exceeded || a.Quota.Reason != b.Quota.Reason || !a.Quota.NextRecoverAt.Equal(b.Quota.NextRecoverAt) {
					t.Errorf("socket error changed existing model quota cooldown: before=%#v after=%#v", b, a)
				}
			}
		})
	}
}

// v8 classifies untyped broken-pipe text as a transient transport failure,
// which must not cool the credential.
func TestTransportRecoveryBrokenPipeTextIsTransient(t *testing.T) {
	converted := resultErrorFromError(errors.New("write tcp: broken pipe"))
	if converted == nil || converted.Code != transientTransportErrorCode {
		t.Fatalf("converted = %#v, want code %q", converted, transientTransportErrorCode)
	}
	if !shouldSkipCredentialCooldown(converted) {
		t.Errorf("broken pipe text should be exempt from cooldown: %#v", converted)
	}
}
