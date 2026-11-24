package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	_ "github.com/router-for-me/CLIProxyAPI/v8/internal/translator/claude/openai/responses"
	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	execution "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	translator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	log "github.com/sirupsen/logrus"
	gjson "github.com/MoeForge/moego/ojson"
)

type terminationHook struct {
	mu      sync.Mutex
	entries []*log.Entry
}

func (*terminationHook) Levels() []log.Level { return log.AllLevels }
func (h *terminationHook) Fire(e *log.Entry) error {
	if e.Message == "claude_stream_termination" {
		h.mu.Lock()
		copyEntry := *e
		copyEntry.Data = e.Dup().Data
		h.entries = append(h.entries, &copyEntry)
		h.mu.Unlock()
	}
	return nil
}
func (h *terminationHook) snapshot() []*log.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*log.Entry(nil), h.entries...)
}
func terminationCapture(t *testing.T) *terminationHook {
	t.Helper()
	h := &terminationHook{}
	logger := log.StandardLogger()
	level := logger.GetLevel()
	old := logger.ReplaceHooks(make(log.LevelHooks))
	logger.AddHook(h)
	logger.SetLevel(log.InfoLevel)
	t.Cleanup(func() { logger.ReplaceHooks(old); logger.SetLevel(level) })
	return h
}
func terminationEvent(s string) string { return "data: " + s + "\n\n" }
func terminationStart(id string) string {
	return terminationEvent(fmt.Sprintf(`{"type":"message_start","message":{"id":%q,"type":"message","role":"assistant","model":"claude-haiku-4-5-20251001","content":[],"usage":{"input_tokens":4,"output_tokens":0}}}`, id))
}
func terminationBlock(index int, kind, value string) string {
	block := fmt.Sprintf(`{"type":%q}`, kind)
	switch kind {
	case "text":
		block = `{"type":"text","text":""}`
	case "thinking":
		block = `{"type":"thinking","thinking":"","signature":"SECRET_SIGNATURE"}`
	case "tool_use":
		block = `{"type":"tool_use","id":"toolu_1","name":"SECRET_TOOL","input":{}}`
	}
	result := terminationEvent(fmt.Sprintf(`{"type":"content_block_start","index":%d,"content_block":%s}`, index, block))
	if kind == "text" || kind == "thinking" {
		result += terminationEvent(fmt.Sprintf(`{"type":"content_block_delta","index":%d,"delta":{"type":%q,%q:%q}}`, index, kind+"_delta", kind, value))
	}
	if kind == "tool_use" {
		result += terminationEvent(fmt.Sprintf(`{"type":"content_block_delta","index":%d,"delta":{"type":"input_json_delta","partial_json":"{\"arg\":\"SECRET_ARGS\"}"}}`, index))
	}
	return result + terminationEvent(fmt.Sprintf(`{"type":"content_block_stop","index":%d}`, index))
}
func terminationEnd(reason string, tokens int) string {
	return terminationEvent(fmt.Sprintf(`{"type":"message_delta","delta":{"stop_reason":%q,"stop_sequence":null},"usage":{"output_tokens":%d}}`, reason, tokens)) + terminationEvent(`{"type":"message_stop"}`)
}

type terminationCase struct {
	name, id, body, mode, reason               string
	text, thinking, tool, other, bytes, tokens int
	stopped                                    bool
}
type terminationResult struct {
	payload  string
	errors   []error
	err      error
	requests int
}

func terminationRun(ctx context.Context, format translator.Format, c terminationCase) terminationResult {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		requests++
		mu.Unlock()
		wire := terminationStart(c.id) + c.body
		if c.mode == "restore" {
			alias := gjson.GetBytes(body, "tools.0.name").String()
			parts := strings.Split(alias, "__")
			if len(parts) < 3 {
				w.WriteHeader(500)
				return
			}
			wire += terminationEvent(fmt.Sprintf(`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":%q,"input":{}}}`, "mcp__"+parts[1]+"__unknown_SECRET_RESTORE"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Termination-Fixture", "preserved")
		if c.mode == "scanner" {
			w.Header().Set("Content-Length", fmt.Sprint(len(wire)+100))
		}
		_, _ = io.WriteString(w, wire)
		w.(http.Flusher).Flush()
		if c.mode == "cancel" {
			<-r.Context().Done()
		}
	}))
	defer server.Close()
	credential := &auth.Auth{ID: c.id, Attributes: map[string]string{"api_key": "SECRET_AUTH", "base_url": server.URL, "cloak_mode": "never"}}
	if c.mode == "restore" {
		credential = claudeNativeHelperOAuthAuth(server.URL)
		credential.Attributes["fingerprint_profile"] = "claude-code-cli"
		credential.Attributes["cloak_mode"] = "always"
	}
	payload := []byte(`{"model":"claude-haiku-4-5-20251001","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"name":"read_file","input_schema":{"type":"object","properties":{}}}],"stream":true}`)
	result, err := NewClaudeExecutor(&config.Config{}).ExecuteStream(ctx, credential, execution.Request{Model: "claude-haiku-4-5-20251001", Payload: payload}, execution.Options{SourceFormat: translator.FormatClaude, ResponseFormat: format, OriginalRequest: payload})
	got := terminationResult{err: err}
	if err == nil {
		if result.Headers.Get("X-Termination-Fixture") != "preserved" {
			got.err = fmt.Errorf("response header lost")
		}
		for chunk := range result.Chunks {
			got.payload += string(chunk.Payload)
			if chunk.Err != nil {
				got.errors = append(got.errors, chunk.Err)
			}
			if c.mode == "cancel" && len(chunk.Payload) > 0 {
				cancel()
			}
		}
	}
	mu.Lock()
	got.requests = requests
	mu.Unlock()
	return got
}
func terminationAssert(t *testing.T, entries []*log.Entry, c terminationCase) {
	t.Helper()
	if len(entries) != 1 {
		t.Fatalf("claude_stream_termination count = %d, want exactly 1", len(entries))
	}
	e := entries[0]
	if e.Level != log.InfoLevel {
		t.Errorf("level = %s, want info", e.Level)
	}
	want := map[string]any{"message_id": c.id, "stop_reason": c.reason, "message_stop": c.stopped, "text_blocks": c.text, "thinking_blocks": c.thinking, "tool_use_blocks": c.tool, "other_blocks": c.other, "text_bytes": c.bytes, "output_tokens": c.tokens}
	if c.id != "" && (len(c.id) > 96 || !strings.HasPrefix(c.id, "msg_") || strings.ContainsAny(c.id, " /\n")) {
		want["message_id"] = "invalid"
	}
	// JSON normalization allows all numeric Go types but rejects numeric strings.
	encoded, err := json.Marshal(e.Data)
	if err != nil {
		t.Fatal(err)
	}
	var actual map[string]any
	if err = json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	expectedJSON, _ := json.Marshal(want)
	var expected map[string]any
	_ = json.Unmarshal(expectedJSON, &expected)
	if len(actual) != len(expected) {
		t.Errorf("unexpected diagnostic fields: %s", encoded)
	}
	for key, value := range expected {
		if actual[key] != value {
			t.Errorf("field %s = %v, want %v", key, actual[key], value)
		}
	}
	if strings.Contains(string(encoded), "SECRET_") {
		t.Error("secret marker leaked into termination diagnostic")
	}
	// Exercise the production formatter, not just the hook's structured data.
	formatEntry := *e
	formatEntry.Buffer = nil
	formatted, err := (&logging.LogFormatter{}).Format(&formatEntry)
	if err != nil {
		t.Fatalf("LogFormatter.Format: %v", err)
	}
	fields := make(map[string]bool)
	for _, field := range strings.Fields(string(formatted)) {
		fields[field] = true
	}
	for key, value := range want {
		field := fmt.Sprintf("%s=%v", key, value)
		if !fields[field] {
			t.Errorf("production log missing field %s", field)
		}
	}
	if strings.Contains(string(formatted), "SECRET_") {
		t.Error("secret marker leaked into production log")
	}
}
func TestClaudeStreamTermination(t *testing.T) {
	text := terminationBlock(0, "text", "SECRET_TEXT")
	thinking := terminationBlock(0, "thinking", "SECRET_REASONING")
	cases := []terminationCase{
		{name: "text", body: text + terminationEnd("end_turn", 7), reason: "end_turn", text: 1, bytes: 11, tokens: 7, stopped: true},
		{name: "tool", body: terminationBlock(0, "tool_use", "") + terminationEnd("tool_use", 7), reason: "tool_use", tool: 1, tokens: 7, stopped: true},
		{name: "thinking_only", body: thinking + terminationEnd("max_tokens", 7), reason: "max_tokens", thinking: 1, tokens: 7, stopped: true},
		{name: "thinking_korean", body: thinking + terminationBlock(1, "text", "이") + terminationEnd("end_turn", 7), reason: "end_turn", thinking: 1, text: 1, bytes: 3, tokens: 7, stopped: true},
		{name: "eof", body: text, reason: "missing", text: 1, bytes: 11},
		{name: "scanner", body: text, mode: "scanner", reason: "missing", text: 1, bytes: 11},
		{name: "cancel", mode: "cancel", reason: "missing"},
		{name: "restore", mode: "restore", reason: "missing", tool: 1},
		{name: "malformed_unknown", body: "data: {SECRET_MALFORMED\n\n" + terminationBlock(0, "SECRET_TYPE", "") + terminationEnd("SECRET_STOP", -8), reason: "unknown", other: 1, stopped: true},
		{name: "invalid_id", id: "SECRET_ID/unsafe", body: terminationEnd("end_turn", 0), reason: "end_turn", stopped: true},
		{name: "long_id", id: "msg_" + strings.Repeat("a", 93), body: terminationEnd("end_turn", 0), reason: "end_turn", stopped: true},
	}
	for _, reason := range []string{"stop_sequence", "pause_turn", "refusal", "model_context_window_exceeded"} {
		cases = append(cases, terminationCase{name: reason, body: terminationEnd(reason, 1), reason: reason, tokens: 1, stopped: true})
	}
	for _, format := range []translator.Format{translator.FormatClaude, translator.FormatOpenAIResponse} {
		t.Run(string(format), func(t *testing.T) {
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					if c.id == "" {
						c.id = "msg_" + c.name
					}
					hook := terminationCapture(t)
					got := terminationRun(context.Background(), format, c)
					if got.err != nil {
						t.Fatalf("ExecuteStream fixture: %v", got.err)
					}
					terminationAssert(t, hook.snapshot(), c)
					if got.requests != 1 {
						t.Errorf("requests = %d, want 1", got.requests)
					}
					// v8 reports any stream that ends without message_stop as an error, so
					// truncated fixtures (eof, restore) surface exactly one stream error.
					if c.mode == "scanner" || c.mode == "restore" || c.name == "eof" {
						if len(got.errors) != 1 {
							t.Errorf("stream errors = %d, want 1", len(got.errors))
						}
					} else if c.mode != "cancel" && len(got.errors) != 0 {
						t.Errorf("unexpected stream errors: %v", got.errors)
					}
					if c.name == "eof" && len(got.errors) > 0 && !strings.Contains(got.errors[0].Error(), "message_stop") {
						t.Errorf("wrong eof error: %v", got.errors[0])
					}
					if format == translator.FormatClaude && c.mode == "" && got.payload != terminationStart(c.id)+c.body {
						t.Error("native payload changed")
					}
					if format == translator.FormatOpenAIResponse && c.name == "text" {
						var delta strings.Builder
						completed := false
						for _, line := range strings.Split(got.payload, "\n") {
							if !strings.HasPrefix(line, "data:") {
								continue
							}
							v := gjson.Parse(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
							switch v.Get("type").String() {
							case "response.output_text.delta":
								delta.WriteString(v.Get("delta").String())
							case "response.completed":
								completed = true
								if v.Get("response.usage.output_tokens").Int() != 7 {
									t.Error("translated usage changed")
								}
							}
						}
						if delta.String() != "SECRET_TEXT" || !completed {
							t.Error("translated text/completion changed")
						}
					}
				})
			}
		})
	}
}
func TestClaudeStreamTerminationConcurrent(t *testing.T) {
	hook := terminationCapture(t)
	const count = 8
	var wg sync.WaitGroup
	results := make([]terminationResult, count)
	cases := make([]terminationCase, count)
	ready := make(chan struct{})
	for i := range count {
		cases[i] = terminationCase{id: fmt.Sprintf("msg_concurrent_%d", i), body: terminationBlock(0, "text", strings.Repeat("x", i+1)) + terminationEnd("end_turn", i+1), reason: "end_turn", text: 1, bytes: i + 1, tokens: i + 1, stopped: true}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-ready
			format := translator.FormatClaude
			if i%2 == 1 {
				format = translator.FormatOpenAIResponse
			}
			results[i] = terminationRun(context.Background(), format, cases[i])
		}(i)
	}
	close(ready)
	wg.Wait()
	entries := hook.snapshot()
	if len(entries) != count {
		t.Fatalf("claude_stream_termination count = %d, want %d", len(entries), count)
	}
	for i, c := range cases {
		if results[i].err != nil || len(results[i].errors) > 0 {
			t.Fatalf("stream %d failed: %+v", i, results[i])
		}
		var matched []*log.Entry
		for _, e := range entries {
			if e.Data["message_id"] == c.id {
				matched = append(matched, e)
			}
		}
		terminationAssert(t, matched, c)
	}
}
