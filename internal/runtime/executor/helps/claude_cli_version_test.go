package helps

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClaudeExecutorCLIVersion_DetectorRetriesFailureWithFakeClock(t *testing.T) {
	path := newClaudeVersionDetectorTestPath(t)
	now := time.Unix(1700000000, 0)
	calls := 0
	detector := claudeCLIVersionDetector{
		now: func() time.Time { return now },
		run: func(_ context.Context, gotPath string) ([]byte, error) {
			if gotPath != path {
				t.Errorf("runner path = %q, want %q", gotPath, path)
			}
			calls++
			if calls == 1 {
				return nil, errors.New("temporary probe failure")
			}
			return []byte("2.1.284 (Claude Code)\n"), nil
		},
	}
	if got := detector.detect(path); got != "" {
		t.Errorf("failed probe version = %q, want empty fallback signal", got)
	}
	now = now.Add(claudeCLIVersionRetry - time.Nanosecond)
	if got := detector.detect(path); got != "" || calls != 1 {
		t.Errorf("before retry: version=%q calls=%d, want empty and 1", got, calls)
	}
	now = now.Add(time.Nanosecond)
	if got := detector.detect(path); got != "2.1.284" || calls != 2 {
		t.Errorf("at retry: version=%q calls=%d, want 2.1.284 and 2", got, calls)
	}
	now = now.Add(24 * time.Hour)
	if got := detector.detect(path); got != "2.1.284" || calls != 2 {
		t.Errorf("unchanged successful binary: version=%q calls=%d, want 2.1.284 and 2", got, calls)
	}
}

func TestClaudeExecutorCLIVersion_DetectorValidatesCompleteBoundedOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		output string
		want   string
	}{
		{name: "valid", output: "2.1.284 (Claude Code)\n", want: "2.1.284"},
		{name: "valid-crlf", output: "2.1.284 (Claude Code)\r\n", want: "2.1.284"},
		{name: "empty"},
		{name: "incomplete", output: "2.1 (Claude Code)\n"},
		{name: "unidentified", output: "2.1.284\n"},
		{name: "prefixed-log", output: "warning\n2.1.284 (Claude Code)\n"},
		{name: "multiple-lines", output: "2.1.284 (Claude Code)\n2.1.285 (Claude Code)\n"},
		{name: "overlong-semver", output: strings.Repeat("9", 5000) + ".1.284 (Claude Code)\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := newClaudeVersionDetectorTestPath(t)
			calls := 0
			detector := claudeCLIVersionDetector{
				now: func() time.Time { return time.Unix(1700000000, 0) },
				run: func(context.Context, string) ([]byte, error) {
					calls++
					return []byte(test.output), nil
				},
			}
			if got := detector.detect(path); got != test.want {
				t.Errorf("detected version = %q, want %q", got, test.want)
			}
			if calls != 1 {
				t.Errorf("runner calls = %d, want 1", calls)
			}
		})
	}
}

func TestClaudeExecutorCLIVersion_DetectorConcurrentRequestsProbeOnce(t *testing.T) {
	path := newClaudeVersionDetectorTestPath(t)
	start, entered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var enteredOnce sync.Once
	var calls atomic.Int32
	detector := claudeCLIVersionDetector{
		now: func() time.Time { return time.Unix(1700000000, 0) },
		run: func(ctx context.Context, _ string) ([]byte, error) {
			calls.Add(1)
			enteredOnce.Do(func() { close(entered) })
			select {
			case <-release:
				return []byte("2.1.284 (Claude Code)\n"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	const workers = 16
	results := make(chan string, workers)
	var ready sync.WaitGroup
	ready.Add(workers)
	for range workers {
		go func() {
			ready.Done()
			<-start
			results <- detector.detect(path)
		}()
	}
	ready.Wait()
	close(start)
	<-entered
	close(release)
	for range workers {
		if got := <-results; got != "2.1.284" {
			t.Errorf("concurrent result = %q, want 2.1.284", got)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("concurrent runner calls = %d, want 1", got)
	}
}

func TestClaudeExecutorCLIVersion_DetectorPermissionRecoveryIsImmediate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires POSIX executable permission bits")
	}
	path := newClaudeVersionDetectorTestPath(t)
	if errChmod := os.Chmod(path, 0o600); errChmod != nil {
		t.Fatal(errChmod)
	}
	calls := 0
	detector := claudeCLIVersionDetector{
		now: func() time.Time { return time.Unix(1700000000, 0) },
		run: func(_ context.Context, path string) ([]byte, error) {
			calls++
			info, errStat := os.Stat(path)
			if errStat != nil {
				return nil, errStat
			}
			if info.Mode().Perm()&0o111 == 0 {
				return nil, os.ErrPermission
			}
			return []byte("2.1.284 (Claude Code)\n"), nil
		},
	}
	if got := detector.detect(path); got != "" {
		t.Errorf("nonexecutable version = %q, want empty fallback signal", got)
	}
	callsBeforeRepair := calls
	if errChmod := os.Chmod(path, 0o700); errChmod != nil {
		t.Fatal(errChmod)
	}
	// The clock never advances: the permission change must bypass failed-probe TTL.
	if got := detector.detect(path); got != "2.1.284" {
		t.Errorf("repaired version = %q, want immediate 2.1.284", got)
	}
	if calls != callsBeforeRepair+1 {
		t.Errorf("repair runner calls = %d, want %d", calls, callsBeforeRepair+1)
	}
}

func TestClaudeExecutorCLIVersion_RunnerCancelsRealProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake CLI fixture requires /bin/sh")
	}
	path := newClaudeVersionDetectorTestPath(t)
	// Shell builtins keep this to one process, without a sleeping child or TTL sleep.
	script := "#!/bin/sh\n[ \"$#\" -eq 1 ] && [ \"$1\" = '--version' ] || exit 97\nwhile :; do :; done\n"
	if errWrite := os.WriteFile(path, []byte(script), 0o700); errWrite != nil {
		t.Fatal(errWrite)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, errRun := runClaudeCLIVersion(ctx, path)
	elapsed := time.Since(started)
	if errRun == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Errorf("timeout result: err=%v context=%v, want process termination at deadline", errRun, ctx.Err())
	}
	if elapsed > 2*time.Second {
		t.Errorf("process timeout took %s, want at most 2s", elapsed)
	}
}

func newClaudeVersionDetectorTestPath(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "claude")
	if errWrite := os.WriteFile(path, []byte("#!/bin/sh\nexit 99\n"), 0o700); errWrite != nil {
		t.Fatal(errWrite)
	}
	resolved, errResolve := filepath.EvalSymlinks(path)
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	return resolved
}
