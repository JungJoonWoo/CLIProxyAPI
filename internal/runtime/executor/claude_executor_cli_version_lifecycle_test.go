package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	gjson "github.com/MoeForge/moego/ojson"
)

func TestClaudeExecutorCLIVersion_ReplacementAndRollbackWithoutRestart(t *testing.T) {
	for _, replacement := range []string{"executable", "symlink"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", replacement, stream), func(t *testing.T) {
				cli := newClaudeVersionTestCLI(t)
				install := func(version string, generation int) {
					if replacement == "symlink" {
						target := &claudeVersionTestCLI{path: cli.path + "." + version, callsPath: cli.callsPath}
						target.install(t, version)
						// Targets have equal size and mtime; retargeting must still invalidate.
						setClaudeVersionLifecycleTimestamp(t, target.path, 0)
						if errLink := os.Symlink(target.path, cli.path+".next"); errLink != nil {
							t.Fatal(errLink)
						}
					} else {
						next := &claudeVersionTestCLI{path: cli.path + ".next", callsPath: cli.callsPath}
						next.install(t, version)
						setClaudeVersionLifecycleTimestamp(t, next.path, generation)
					}
					if errRename := os.Rename(cli.path+".next", cli.path); errRename != nil {
						t.Fatal(errRename)
					}
				}
				install("2.1.283", 0)
				e := NewClaudeExecutor(claudeVersionTestConfig(t, cli.path, ""))
				for index, version := range []string{"2.1.283", "2.1.284", "2.1.283"} {
					if index != 0 {
						install(version, index)
					}
					// No sleep, config reload or executor restart is allowed here.
					wire := captureClaudeVersionTestRequest(t, e, stream, nil, nil)
					assertClaudeVersionTestWire(t, wire, "claude-cli/"+version+" (external, cli)", version)
					if got := cli.calls(t); got != index+1 {
						t.Errorf("after version %s: CLI executions = %d, want %d", version, got, index+1)
					}
				}
			})
		}
	}
}

func TestClaudeExecutorCLIVersion_OneRequestUsesOneVersion(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			cli := newClaudeVersionTestCLI(t)
			cli.install(t, "2.1.284")
			script, errRead := os.ReadFile(cli.path)
			if errRead != nil {
				t.Fatal(errRead)
			}
			// The first --version atomically replaces its executable before returning
			// 2.1.284. A second lookup in this request would see 2.1.285 instead.
			swap := "#!/bin/sh\nif [ -f \"$0.next\" ]; then /bin/mv \"$0.next\" \"$0\" || exit 98; fi\n"
			if errWrite := os.WriteFile(cli.path, []byte(swap+strings.TrimPrefix(string(script), "#!/bin/sh\n")), 0o700); errWrite != nil {
				t.Fatal(errWrite)
			}
			setClaudeVersionLifecycleTimestamp(t, cli.path, 0)
			next := &claudeVersionTestCLI{path: cli.path + ".next", callsPath: cli.callsPath}
			next.install(t, "2.1.285")
			setClaudeVersionLifecycleTimestamp(t, next.path, 1)

			e := NewClaudeExecutor(claudeVersionTestConfig(t, cli.path, ""))
			first := captureClaudeVersionTestRequest(t, e, stream, nil, nil)
			assertClaudeVersionTestWire(t, first, "claude-cli/2.1.284 (external, cli)", "2.1.284")
			if got := cli.calls(t); got != 1 {
				t.Errorf("first request CLI executions = %d, want one version snapshot", got)
			}
			second := captureClaudeVersionTestRequest(t, e, stream, nil, nil)
			assertClaudeVersionTestWire(t, second, "claude-cli/2.1.285 (external, cli)", "2.1.285")
			if got := cli.calls(t); got != 2 {
				t.Errorf("after replacement CLI executions = %d, want 2", got)
			}
		})
	}
}

func TestClaudeExecutorCLIVersion_DoesNotChangeNativeClientDetection(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			cli := newClaudeVersionTestCLI(t)
			cli.install(t, "2.1.284")
			headers := claudeNativeHelperHeaders(claudeNativeHelperCoreBetas, "gzip, deflate, br, zstd")
			payload := []byte(`{"model":"claude-haiku-4-5-20251001","max_tokens":1,"messages":[{"role":"user","content":"helper probe"}],"metadata":{"user_id":"` + strings.ReplaceAll(claudeNativeHelperUserID, `"`, `\"`) + `"}}`)
			wire := captureClaudeVersionTestRequest(t, NewClaudeExecutor(claudeVersionTestConfig(t, cli.path, "")), stream, payload, headers)
			if got := gjson.GetBytes(wire.body, "system"); got.Exists() {
				t.Errorf("native helper gained a system prompt: %s", got.Raw)
			}
			if got := gjson.GetBytes(wire.body, "messages.0.content").String(); got != "helper probe" {
				t.Errorf("native helper content = %q, want preserved string", got)
			}
			assertClaudeNativeHelperHeaders(t, wire.header, headers)
		})
	}
}

func TestClaudeExecutorCLIVersion_UnchangedBinaryProbedOnce(t *testing.T) {
	cli := newClaudeVersionTestCLI(t)
	cli.install(t, "2.1.284")
	e := NewClaudeExecutor(claudeVersionTestConfig(t, cli.path, ""))
	// Both entrypoints share the cache for the same configured executable.
	for index, stream := range []bool{false, true, false, true} {
		wire := captureClaudeVersionTestRequest(t, e, stream, nil, nil)
		assertClaudeVersionTestWire(t, wire, "claude-cli/2.1.284 (external, cli)", "2.1.284")
		if got := cli.calls(t); got != 1 {
			t.Errorf("request %d: CLI executions = %d, want 1 for unchanged binary", index+1, got)
		}
	}
}

func TestClaudeExecutorCLIVersion_UnavailableVersionFallsBack(t *testing.T) {
	for _, test := range []struct {
		name    string
		missing bool
		output  string
	}{
		{name: "missing", missing: true},
		{name: "malformed", output: "not-a-version"},
		{name: "incomplete-semver", output: "2.1"},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", test.name, stream), func(t *testing.T) {
				cli := newClaudeVersionTestCLI(t)
				if !test.missing {
					cli.install(t, test.output)
				}
				e := NewClaudeExecutor(claudeVersionTestConfig(t, cli.path, ""))
				wire := captureClaudeVersionTestRequest(t, e, stream, nil, nil)
				assertClaudeVersionTestWire(t, wire, "claude-cli/2.1.321 (external, cli)", "2.1.321")
				if !test.missing && cli.calls(t) == 0 {
					t.Error("malformed --version output was never exercised")
				}
				// A failed probe must not hide a repaired installation until a timer expires.
				next := &claudeVersionTestCLI{path: cli.path + ".next", callsPath: cli.callsPath}
				next.install(t, "2.1.284")
				setClaudeVersionLifecycleTimestamp(t, next.path, 1)
				if errRename := os.Rename(next.path, cli.path); errRename != nil {
					t.Fatal(errRename)
				}
				recovered := captureClaudeVersionTestRequest(t, e, stream, nil, nil)
				assertClaudeVersionTestWire(t, recovered, "claude-cli/2.1.284 (external, cli)", "2.1.284")
			})
		}
	}
}

func TestClaudeExecutorCLIVersion_CredentialHeaderOverridesPreserved(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			cli := newClaudeVersionTestCLI(t)
			cli.install(t, "2.1.284")
			e := NewClaudeExecutor(claudeVersionTestConfig(t, cli.path, ""))
			auth := &cliproxyauth.Auth{
				ID: "cli-version-override-" + t.Name(),
				Attributes: map[string]string{
					"api_key":                            "sk-ant-oat-cli-version-override-test",
					"base_url":                           "http://claude-cli-version.invalid",
					"header:User-Agent":                  "operator-gateway/1.0",
					"header:X-Stainless-Package-Version": "0.80.0",
					"header:X-Stainless-Runtime-Version": "v24.5.0",
				},
				Metadata: claudeOAuthTestMetadata(),
			}
			wire := captureClaudeVersionLifecycleCredentialRequest(t, e, auth, stream)
			if got := wire.header.Get("User-Agent"); got != "operator-gateway/1.0" {
				t.Errorf("credential User-Agent = %q, want operator-gateway/1.0", got)
			}
			assertClaudeVersionTestSDK(t, wire, "0.80.0", "v24.5.0")
		})
	}
}

func setClaudeVersionLifecycleTimestamp(t *testing.T, path string, generation int) {
	t.Helper()
	// Explicit timestamps avoid sleeps and filesystem timestamp granularity races.
	stamp := time.Unix(1700000000+int64(generation), 0)
	if errTimes := os.Chtimes(path, stamp, stamp); errTimes != nil {
		t.Fatal(errTimes)
	}
}

// The original capture helper deliberately fixes auth. This fixture changes only
// the credential input while preserving the in-memory transport boundary.
func captureClaudeVersionLifecycleCredentialRequest(t *testing.T, e *ClaudeExecutor, auth *cliproxyauth.Auth, stream bool) claudeVersionTestWire {
	t.Helper()
	captured := make(chan claudeVersionTestWire, 1)
	transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "claude-cli-version.invalid" || req.URL.Path != "/v1/messages" {
			return nil, fmt.Errorf("unexpected test upstream: %s", req.URL)
		}
		body, errRead := io.ReadAll(req.Body)
		if errRead != nil {
			return nil, errRead
		}
		select {
		case captured <- claudeVersionTestWire{header: req.Header.Clone(), body: body}:
		default:
			return nil, fmt.Errorf("unexpected additional upstream request")
		}
		contentType := "application/json"
		response := `{"id":"msg_cli_override","type":"message","role":"assistant","model":"claude-haiku-4-5-20251001","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`
		if stream {
			contentType = "text/event-stream"
			response = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_cli_override\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-haiku-4-5-20251001\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(response)), Request: req}, nil
	})
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(transport))
	payload := []byte(`{"model":"claude-haiku-4-5-20251001","max_tokens":100,"messages":[{"role":"user","content":"Say OK."}]}`)
	req := cliproxyexecutor.Request{Model: "claude-haiku-4-5-20251001", Payload: payload}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, OriginalRequest: payload}
	if stream {
		result, errStream := e.ExecuteStream(ctx, auth, req, opts)
		if errStream != nil {
			t.Fatalf("ExecuteStream() error = %v", errStream)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Errorf("stream chunk error = %v", chunk.Err)
			}
		}
	} else if _, errExecute := e.Execute(ctx, auth, req, opts); errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	select {
	case wire := <-captured:
		return wire
	default:
		t.Fatal("executor did not send an upstream request")
		return claudeVersionTestWire{}
	}
}
