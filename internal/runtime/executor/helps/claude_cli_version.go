package helps

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

const (
	claudeCLIVersionTimeout     = 2 * time.Second
	claudeCLIVersionRetry       = 5 * time.Second
	claudeCLIVersionOutputLimit = 4096
)

// Only a complete release-version line from Claude Code is accepted.
var claudeCLIVersionOutput = regexp.MustCompile(`^((?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)) \(Claude Code\)(?:\r?\n)?$`)

type claudeCLIVersionContextKey struct{}

// WithClaudeCLIVersionSnapshot selects a single immutable version for a reconstructed
// CLI request. Manual User-Agent configuration and an absent path remain untouched.
func WithClaudeCLIVersionSnapshot(ctx context.Context, cfg *config.Config) context.Context {
	if cfg == nil || strings.TrimSpace(cfg.ClaudeHeaderDefaults.UserAgent) != "" {
		return ctx
	}
	path := strings.TrimSpace(cfg.ClaudeHeaderDefaults.CLIPath)
	if !filepath.IsAbs(path) {
		return ctx
	}
	version := installedClaudeCLIVersion.detect(path)
	if version == "" {
		version = DefaultClaudeVersion(cfg)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, claudeCLIVersionContextKey{}, version)
}

// ClaudeCLIVersionSnapshot returns only the version selected for this request.
func ClaudeCLIVersionSnapshot(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	version, _ := ctx.Value(claudeCLIVersionContextKey{}).(string)
	return version
}

// ClaudeVersionFromContext keeps the existing baseline for all other callers.
func ClaudeVersionFromContext(ctx context.Context, cfg *config.Config) string {
	if version := ClaudeCLIVersionSnapshot(ctx); version != "" {
		return version
	}
	return DefaultClaudeVersion(cfg)
}

type claudeCLIFile struct {
	target string
	info   os.FileInfo
}

func statClaudeCLI(path string) claudeCLIFile {
	target, errResolve := filepath.EvalSymlinks(path)
	if errResolve != nil {
		return claudeCLIFile{}
	}
	info, errStat := os.Stat(target)
	if errStat != nil {
		return claudeCLIFile{target: target}
	}
	return claudeCLIFile{target: target, info: info}
}

func (f claudeCLIFile) same(other claudeCLIFile) bool {
	if f.target != other.target {
		return false
	}
	if f.info == nil || other.info == nil {
		return f.info == nil && other.info == nil
	}
	return os.SameFile(f.info, other.info) && f.info.Size() == other.info.Size() &&
		f.info.ModTime().Equal(other.info.ModTime()) && f.info.Mode() == other.info.Mode()
}

// One cached path bounds memory. Holding the mutex through the probe coalesces
// concurrent requests. The clock and runner are small seams for deterministic tests.
type claudeCLIVersionDetector struct {
	mu      sync.Mutex
	now     func() time.Time
	run     func(context.Context, string) ([]byte, error)
	path    string
	file    claudeCLIFile
	version string
	retryAt time.Time
}

var installedClaudeCLIVersion = claudeCLIVersionDetector{
	now: time.Now,
	run: runClaudeCLIVersion,
}

func (d *claudeCLIVersionDetector) detect(path string) string {
	if !filepath.IsAbs(path) {
		return ""
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	file := statClaudeCLI(path)
	if d.path == path && d.file.same(file) {
		if d.version != "" || d.now().Before(d.retryAt) {
			return d.version
		}
	}
	d.path, d.file, d.version = path, file, ""
	d.retryAt = d.now().Add(claudeCLIVersionRetry)
	if file.info == nil || !file.info.Mode().IsRegular() {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), claudeCLIVersionTimeout)
	defer cancel()
	output, errRun := d.run(ctx, file.target)
	d.retryAt = d.now().Add(claudeCLIVersionRetry)
	version := ""
	if errRun == nil && ctx.Err() == nil && len(output) <= claudeCLIVersionOutputLimit {
		if match := claudeCLIVersionOutput.FindSubmatch(output); match != nil {
			version = string(match[1])
		}
	}
	// A successful probe still describes this request's snapshot when the file
	// changes during execution. Do not cache it under either file identity, and
	// force the next request to probe again, even if the file is rolled back.
	if !file.same(statClaudeCLI(path)) {
		d.path = ""
		return version
	}
	d.version = version
	return version
}

type claudeCLIVersionBuffer struct {
	data []byte
}

func (b *claudeCLIVersionBuffer) Write(p []byte) (int, error) {
	remaining := claudeCLIVersionOutputLimit - len(b.data)
	if len(p) > remaining {
		b.data = append(b.data, p[:remaining]...)
		return remaining, io.ErrShortBuffer
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func runClaudeCLIVersion(ctx context.Context, path string) ([]byte, error) {
	// An absolute resolved path avoids shell evaluation and PATH discovery.
	cmd := exec.CommandContext(ctx, path, "--version")
	output := &claudeCLIVersionBuffer{}
	cmd.Stdout = output
	cmd.Stderr = io.Discard
	// Bound waits even if a child process inherits the output pipes.
	cmd.WaitDelay = 100 * time.Millisecond
	errRun := cmd.Run()
	return output.data, errRun
}
