package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// stubHolderProbes replaces the three injectable holder probes for the
// duration of a test. They are package vars, so callers must not run parallel.
func stubHolderProbes(t *testing.T, link string, linkErr error) {
	t.Helper()
	oldReader, oldStat, oldProcStat := supervisorExeLinkReader, supervisorExeStat, supervisorExeProcStat
	t.Cleanup(func() {
		supervisorExeLinkReader, supervisorExeStat, supervisorExeProcStat = oldReader, oldStat, oldProcStat
	})
	supervisorExeLinkReader = func(int) (string, error) { return link, linkErr }
}

// TestClassifySupervisorHolderLiveImage pins the ordinary case: a readlink
// target with no kernel marker is a live image and needs no stat at all.
func TestClassifySupervisorHolderLiveImage(t *testing.T) {
	stubHolderProbes(t, "/usr/local/bin/gc", nil)
	state, path := classifySupervisorHolder(4242)
	if state != supervisorHolderLive {
		t.Fatalf("state = %v, want supervisorHolderLive", state)
	}
	if path != "/usr/local/bin/gc" {
		t.Fatalf("path = %q, want /usr/local/bin/gc", path)
	}
}

// TestClassifySupervisorHolderDeletedImage covers the gc-f1081 holder: the
// marker is present and the literal path does not exist, which is definitive
// proof the running inode was unlinked.
func TestClassifySupervisorHolderDeletedImage(t *testing.T) {
	gone := filepath.Join(t.TempDir(), "gc")
	stubHolderProbes(t, gone+supervisorDeletedExeSuffix, nil)
	state, path := classifySupervisorHolder(3204367)
	if state != supervisorHolderStaleImage {
		t.Fatalf("state = %v, want supervisorHolderStaleImage", state)
	}
	if path != gone {
		t.Fatalf("path = %q, want %q (marker stripped)", path, gone)
	}
}

// TestClassifySupervisorHolderReplacedInode covers the same marker when a
// *new* binary already occupies the path — the `go install` case. The running
// inode is still gone, so the verdict stays stale.
func TestClassifySupervisorHolderReplacedInode(t *testing.T) {
	dir := t.TempDir()
	replaced := filepath.Join(dir, "gc")
	if err := os.WriteFile(replaced, []byte("new binary"), 0o755); err != nil {
		t.Fatalf("write replacement binary: %v", err)
	}
	other := filepath.Join(dir, "running")
	if err := os.WriteFile(other, []byte("old binary"), 0o755); err != nil {
		t.Fatalf("write running binary: %v", err)
	}
	stubHolderProbes(t, replaced+supervisorDeletedExeSuffix, nil)
	supervisorExeProcStat = func(int) (os.FileInfo, error) { return os.Stat(other) }

	state, path := classifySupervisorHolder(3204367)
	if state != supervisorHolderStaleImage {
		t.Fatalf("state = %v, want supervisorHolderStaleImage", state)
	}
	if path != replaced {
		t.Fatalf("path = %q, want %q", path, replaced)
	}
}

// TestClassifySupervisorHolderGenuinelyNamedDeleted covers the one ambiguous
// reading of the marker: a binary whose real filename ends in " (deleted)".
// Same inode as the procfs link means the image is live, not unlinked — a
// stale verdict here would make a legitimate start terminal.
func TestClassifySupervisorHolderGenuinelyNamedDeleted(t *testing.T) {
	dir := t.TempDir()
	odd := filepath.Join(dir, "gc"+supervisorDeletedExeSuffix)
	if err := os.WriteFile(odd, []byte("really named that"), 0o755); err != nil {
		t.Fatalf("write oddly named binary: %v", err)
	}
	stubHolderProbes(t, odd, nil)
	supervisorExeProcStat = func(int) (os.FileInfo, error) { return os.Stat(odd) }

	state, _ := classifySupervisorHolder(4242)
	if state != supervisorHolderLive {
		t.Fatalf("state = %v, want supervisorHolderLive (same inode is not an unlinked image)", state)
	}
}

// TestClassifySupervisorHolderFailsClosed pins the fail-closed posture. A
// stale verdict turns a start terminal, so every non-definitive probe error
// must degrade to Unknown, which callers treat exactly like a live holder.
func TestClassifySupervisorHolderFailsClosed(t *testing.T) {
	t.Run("unreadable link", func(t *testing.T) {
		stubHolderProbes(t, "", errors.New("permission denied"))
		if state, _ := classifySupervisorHolder(4242); state != supervisorHolderUnknown {
			t.Fatalf("state = %v, want supervisorHolderUnknown", state)
		}
	})
	t.Run("non-definitive stat error", func(t *testing.T) {
		stubHolderProbes(t, "/some/gc"+supervisorDeletedExeSuffix, nil)
		supervisorExeStat = func(string) (os.FileInfo, error) { return nil, errors.New("permission denied") }
		if state, _ := classifySupervisorHolder(4242); state != supervisorHolderUnknown {
			t.Fatalf("state = %v, want supervisorHolderUnknown", state)
		}
	})
	t.Run("unresolvable procfs exe", func(t *testing.T) {
		dir := t.TempDir()
		existing := filepath.Join(dir, "gc"+supervisorDeletedExeSuffix)
		if err := os.WriteFile(existing, []byte("x"), 0o755); err != nil {
			t.Fatalf("write: %v", err)
		}
		stubHolderProbes(t, existing, nil)
		supervisorExeProcStat = func(int) (os.FileInfo, error) { return nil, errors.New("permission denied") }
		if state, _ := classifySupervisorHolder(4242); state != supervisorHolderUnknown {
			t.Fatalf("state = %v, want supervisorHolderUnknown", state)
		}
	})
}

// TestSupervisorAlreadyRunningMessageLiveHolder keeps the ordinary refusal
// wording stable — three call sites print it and must not drift.
func TestSupervisorAlreadyRunningMessageLiveHolder(t *testing.T) {
	for _, state := range []supervisorHolderState{supervisorHolderLive, supervisorHolderUnknown} {
		got := supervisorAlreadyRunningMessage("gc supervisor", 99, state, "/usr/local/bin/gc")
		want := "gc supervisor: supervisor already running (PID 99)\n"
		if got != want {
			t.Fatalf("state %v: message = %q, want %q", state, got, want)
		}
	}
}

// TestSupervisorAlreadyRunningMessageStaleHolder pins the loud form. This is
// the diagnostic that was missing while the supervisor exited 83439 times
// against a deleted-inode holder, so it must name the state, the PID, the
// remedy, and the one-line verification the operator can run.
func TestSupervisorAlreadyRunningMessageStaleHolder(t *testing.T) {
	got := supervisorAlreadyRunningMessage("gc supervisor", 3204367, supervisorHolderStaleImage, "/home/someone/go/bin/gc")
	for _, want := range []string{
		"DELETED image",
		"PID 3204367",
		"/home/someone/go/bin/gc",
		"gc stop --force && gc start",
		"ls -la /proc/3204367/exe",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stale-holder message missing %q; got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "\n\n") {
		t.Fatalf("stale-holder message has a blank line; got:\n%s", got)
	}
}

// TestSupervisorSystemdTemplatePreventsRestartOnDuplicateHolder is the
// regression guard for the crash loop itself. `gc supervisor run` returning a
// bare 1 was not covered by RestartPreventExitStatus, so systemd retried every
// 5s for five days. The already-running exit code must be listed alongside the
// port-in-use one, and the two must stay distinct.
func TestSupervisorSystemdTemplatePreventsRestartOnDuplicateHolder(t *testing.T) {
	if supervisorExitCodeAlreadyRunning == supervisorExitCodePortInUse {
		t.Fatalf("already-running and port-in-use share exit code %d; they must be distinguishable", supervisorExitCodeAlreadyRunning)
	}
	if supervisorExitCodeAlreadyRunning == 0 || supervisorExitCodeAlreadyRunning == 1 {
		t.Fatalf("already-running exit code %d is not a distinct terminal code", supervisorExitCodeAlreadyRunning)
	}
	data := &supervisorServiceData{
		GCPath:                 "/usr/local/bin/gc",
		LogPath:                "/home/someone/.gc/supervisor.log",
		GCHome:                 "/home/someone/.gc",
		Path:                   "/usr/bin",
		PortInUseExitCode:      supervisorExitCodePortInUse,
		AlreadyRunningExitCode: supervisorExitCodeAlreadyRunning,
	}
	content, err := renderSupervisorTemplate(supervisorSystemdTemplate, data)
	if err != nil {
		t.Fatalf("render systemd template: %v", err)
	}
	want := "RestartPreventExitStatus=" + strconv.Itoa(supervisorExitCodePortInUse) + " " + strconv.Itoa(supervisorExitCodeAlreadyRunning)
	if !strings.Contains(content, want) {
		t.Fatalf("systemd unit missing %q; got:\n%s", want, content)
	}
	// Genuine crashes must still restart — Restart=always stays in force.
	if !strings.Contains(content, "Restart=always") {
		t.Fatalf("systemd unit lost Restart=always; got:\n%s", content)
	}
}

// TestBuildSupervisorServiceDataCarriesAlreadyRunningExitCode proves the
// template field is actually populated in production. Without this, the unit
// would render "RestartPreventExitStatus=3 0" and silently stop preventing
// anything.
func TestBuildSupervisorServiceDataCarriesAlreadyRunningExitCode(t *testing.T) {
	data, err := buildSupervisorServiceData()
	if err != nil {
		t.Fatalf("buildSupervisorServiceData: %v", err)
	}
	if data.AlreadyRunningExitCode != supervisorExitCodeAlreadyRunning {
		t.Fatalf("AlreadyRunningExitCode = %d, want %d", data.AlreadyRunningExitCode, supervisorExitCodeAlreadyRunning)
	}
}
