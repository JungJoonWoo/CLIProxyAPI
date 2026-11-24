package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	gjson "github.com/MoeForge/moego/ojson"
)

func TestClaudeExecutorCLIVersion_ConfiguredPath(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			cli := newClaudeVersionTestCLI(t)
			cli.install(t, "2.1.284")
			e := NewClaudeExecutor(claudeVersionTestConfig(t, cli.path, ""))
			wire := captureClaudeVersionTestRequest(t, e, stream, nil, nil)
			assertClaudeVersionTestWire(t, wire, "claude-cli/2.1.284 (external, cli)", "2.1.284")
			if cli.calls(t) == 0 {
				t.Error("configured absolute cli-path was not invoked with --version")
			}
		})
	}
}

func TestClaudeExecutorCLIVersion_ManualUserAgentSkipsCLI(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			cli := newClaudeVersionTestCLI(t)
			cli.install(t, "2.1.284")
			const manual = "claude-cli/2.1.70 (external, cli)"
			cfg := claudeVersionTestConfig(t, cli.path, manual)
			cfg.ClaudeHeaderDefaults.PackageVersion = "0.80.0"
			cfg.ClaudeHeaderDefaults.RuntimeVersion = "v24.5.0"
			wire := captureClaudeVersionTestRequest(t, NewClaudeExecutor(cfg), stream, nil, nil)
			assertClaudeVersionTestIdentity(t, wire, manual, "2.1.70")
			assertClaudeVersionTestSDK(t, wire, "0.80.0", "v24.5.0")
			if got := cli.calls(t); got != 0 {
				t.Errorf("CLI executions = %d, want 0 when user-agent is configured", got)
			}
		})
	}
}

func TestClaudeExecutorCLIVersion_UnconfiguredKeepsDefaults(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			cli := newClaudeVersionTestCLI(t)
			cli.install(t, "2.1.284")
			// A discoverable CLI must not implicitly opt into version probing.
			t.Setenv("PATH", filepath.Dir(cli.path))
			wire := captureClaudeVersionTestRequest(t, NewClaudeExecutor(claudeVersionTestConfig(t, "", "")), stream, nil, nil)
			assertClaudeVersionTestWire(t, wire, "claude-cli/2.1.321 (external, cli)", "2.1.321")
			if got := cli.calls(t); got != 0 {
				t.Errorf("CLI executions = %d, want 0 without cli-path", got)
			}
		})
	}
}

func claudeVersionTestConfig(t *testing.T, cliPath, userAgent string) *config.Config {
	t.Helper()
	yaml := "claude-header-defaults:\n  stabilize-device-profile: false\n"
	if cliPath != "" {
		if !filepath.IsAbs(cliPath) {
			t.Fatalf("test CLI path must be absolute: %q", cliPath)
		}
		yaml += fmt.Sprintf("  cli-path: %q\n", cliPath)
	}
	if userAgent != "" {
		yaml += fmt.Sprintf("  user-agent: %q\n", userAgent)
	}
	cfg, errParse := config.ParseConfigBytes([]byte(yaml))
	if errParse != nil {
		t.Fatalf("ParseConfigBytes() error = %v", errParse)
	}
	return cfg
}

type claudeVersionTestWire struct {
	header http.Header
	body   []byte
}

func captureClaudeVersionTestRequest(t *testing.T, e *ClaudeExecutor, stream bool, payload []byte, headers http.Header) claudeVersionTestWire {
	t.Helper()
	if payload == nil {
		payload = []byte(`{"model":"claude-haiku-4-5-20251001","max_tokens":100,"messages":[{"role":"user","content":"Say OK."}]}`)
	}
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
		response := `{"id":"msg_cli_version","type":"message","role":"assistant","model":"claude-haiku-4-5-20251001","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`
		contentType := "application/json"
		if stream {
			contentType = "text/event-stream"
			response = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_cli_version\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-haiku-4-5-20251001\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(response)), Request: req}, nil
	})
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(transport))
	auth := &cliproxyauth.Auth{
		ID:         "cli-version-" + t.Name(),
		Attributes: map[string]string{"api_key": "sk-ant-oat-cli-version-test", "base_url": "http://claude-cli-version.invalid"},
		Metadata:   claudeOAuthTestMetadata(),
	}
	req := cliproxyexecutor.Request{Model: "claude-haiku-4-5-20251001", Payload: payload}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, OriginalRequest: payload, Headers: headers}
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

func assertClaudeVersionTestWire(t *testing.T, wire claudeVersionTestWire, userAgent, version string) {
	t.Helper()
	assertClaudeVersionTestIdentity(t, wire, userAgent, version)
	assertClaudeVersionTestSDK(t, wire, "0.128.0", "v26.12.1")
}

func assertClaudeVersionTestIdentity(t *testing.T, wire claudeVersionTestWire, userAgent, version string) {
	t.Helper()
	if got := wire.header.Get("User-Agent"); got != userAgent {
		t.Errorf("outgoing User-Agent = %q, want %q", got, userAgent)
	}
	billingVersion := regexp.MustCompile(`\bcc_version=([0-9]+\.[0-9]+\.[0-9]+)(?:\.[0-9a-f]{3})?;`)
	matches := billingVersion.FindAllStringSubmatch(gjson.GetBytes(wire.body, "system").Raw, -1)
	if len(matches) != 1 {
		t.Errorf("body cc_version count = %d, want 1; system = %s", len(matches), gjson.GetBytes(wire.body, "system").Raw)
		return
	}
	if got := matches[0][1]; got != version {
		t.Errorf("outgoing body cc_version = %q, want %q", got, version)
	}
}

func assertClaudeVersionTestSDK(t *testing.T, wire claudeVersionTestWire, packageVersion, runtimeVersion string) {
	t.Helper()
	for name, want := range map[string]string{"X-Stainless-Package-Version": packageVersion, "X-Stainless-Runtime-Version": runtimeVersion} {
		if got := wire.header.Get(name); got != want {
			t.Errorf("%s = %q, want unchanged %q", name, got, want)
		}
	}
}

type claudeVersionTestCLI struct {
	path      string
	callsPath string
}

func newClaudeVersionTestCLI(t *testing.T) *claudeVersionTestCLI {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake CLI fixture requires /bin/sh")
	}
	// Spaces exercise absolute-path execution without shell tokenization.
	dir := filepath.Join(t.TempDir(), "CLI with spaces")
	if errMkdir := os.Mkdir(dir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	cli := &claudeVersionTestCLI{path: filepath.Join(dir, "claude"), callsPath: filepath.Join(dir, "calls")}
	if errWrite := os.WriteFile(cli.callsPath, nil, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	return cli
}

func (cli *claudeVersionTestCLI) install(t *testing.T, version string) {
	t.Helper()
	callsPath := "'" + strings.ReplaceAll(cli.callsPath, "'", "'\"'\"'") + "'"
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >> %s\n[ \"$#\" -eq 1 ] && [ \"$1\" = '--version' ] || exit 97\nprintf '%%s\\n' '%s (Claude Code)'\n", callsPath, version)
	if errWrite := os.WriteFile(cli.path, []byte(script), 0o700); errWrite != nil {
		t.Fatal(errWrite)
	}
}

func (cli *claudeVersionTestCLI) calls(t *testing.T) int {
	t.Helper()
	data, errRead := os.ReadFile(cli.callsPath)
	if errRead != nil {
		t.Fatal(errRead)
	}
	if len(data) == 0 {
		return 0
	}
	calls := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	for _, args := range calls {
		if args != "--version" {
			t.Errorf("fake CLI arguments = %q, want exactly --version", args)
		}
	}
	return len(calls)
}
