package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// supervisorDeletedExeSuffix is the marker the kernel appends to
// /proc/<pid>/exe when the running binary's inode has been unlinked — the
// signature of a process whose image no longer exists on disk.
const supervisorDeletedExeSuffix = " (deleted)"

// supervisorHolderState classifies the supervisor process that already owns
// the city, from the point of view of a second supervisor trying to start.
type supervisorHolderState int

const (
	// supervisorHolderLive means the holder is running an image that is
	// still present on disk. Nothing can be concluded about whether it is
	// the *right* image, only that it is a reachable one.
	supervisorHolderLive supervisorHolderState = iota
	// supervisorHolderStaleImage means the holder is running an unlinked
	// image. No rebuild and no restart of *this* process can ever reach it,
	// so retrying the start is provably futile.
	supervisorHolderStaleImage
	// supervisorHolderUnknown means the holder's image could not be
	// classified — a non-Linux host with no /proc, a uid mismatch, or any
	// non-definitive filesystem error.
	supervisorHolderUnknown
)

// readSupervisorExeLink returns the raw /proc/<pid>/exe readlink target,
// including any kernel-appended " (deleted)" marker.
//
// Callers that want the on-disk path use readSupervisorExePath, which strips
// the marker; callers that want to know whether the image is still on disk
// use classifySupervisorHolder, which interprets it.
func readSupervisorExeLink(pid int) (string, error) {
	return os.Readlink(supervisorExeProcPath(pid))
}

// supervisorExeProcPath is the procfs link to a process's running image.
func supervisorExeProcPath(pid int) string {
	return filepath.Join("/proc", strconv.Itoa(pid), "exe")
}

// These follow the package's test-double convention; tests that replace them
// must not run in parallel. classifySupervisorHolder's verdict depends on
// filesystem state that cannot be staged for a real process, so the three
// probes it makes are injectable.
var (
	supervisorExeLinkReader = readSupervisorExeLink
	supervisorExeStat       = os.Stat
	supervisorExeProcStat   = func(pid int) (os.FileInfo, error) { return os.Stat(supervisorExeProcPath(pid)) }
)

// classifySupervisorHolder reports whether the supervisor holding the city is
// running an image that still exists on disk, and returns the holder's
// executable path for the diagnostic.
//
// Classification is deliberately fail-closed towards supervisorHolderUnknown:
// a stale verdict makes a start terminal (see supervisorExitCodeAlreadyRunning),
// so it is returned only on definitive evidence that the image is unlinked.
// A missing /proc (darwin), a permission error from a holder running under a
// different uid, or any other non-definitive stat error yields Unknown, which
// callers treat exactly like a live holder.
//
// The " (deleted)" suffix is ambiguous in one case: a binary whose real name
// genuinely ends in " (deleted)" produces an identical readlink target. That
// is resolved the way cwdStateFromLink resolves it for working directories —
// treat the readlink text as a literal path and compare inodes with the
// procfs link, which the kernel resolves to the actual running image.
func classifySupervisorHolder(pid int) (supervisorHolderState, string) {
	link, err := supervisorExeLinkReader(pid)
	if err != nil {
		return supervisorHolderUnknown, ""
	}
	if !strings.HasSuffix(link, supervisorDeletedExeSuffix) {
		return supervisorHolderLive, link
	}
	// onDisk is the path the marker points at with the suffix removed — where
	// a replacement binary lives — and is what the diagnostic reports.
	onDisk := strings.TrimSuffix(link, supervisorDeletedExeSuffix)
	// Disambiguate against the literal readlink text, not the stripped path: a
	// binary genuinely named "... (deleted)" yields a target identical to the
	// kernel marker, and only inode identity separates the two readings.
	literalInfo, statErr := supervisorExeStat(link)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			// No file is literally named that, so the suffix is the kernel's
			// unlinked marker: the running image is definitively gone.
			return supervisorHolderStaleImage, onDisk
		}
		// Permission or I/O error: not definitive proof, so fail closed.
		return supervisorHolderUnknown, onDisk
	}
	exeInfo, exeErr := supervisorExeProcStat(pid)
	if exeErr != nil {
		// A file with that literal name exists, but the running image could
		// not be resolved to compare inodes; without that proof, fail closed.
		return supervisorHolderUnknown, onDisk
	}
	if os.SameFile(literalInfo, exeInfo) {
		// Same inode: the binary really is named "... (deleted)" and is live.
		return supervisorHolderLive, link
	}
	// A file with that literal name exists but is a different inode than the
	// running image, so the suffix is the genuine kernel unlinked marker.
	return supervisorHolderStaleImage, onDisk
}

// supervisorAlreadyRunningMessage renders the diagnostic printed when a
// supervisor start is refused because another supervisor already holds the
// city. prefix is the command context ("gc supervisor", "gc supervisor start").
//
// Three call sites print this sentence — `gc supervisor run`, `gc supervisor
// start`, and the systemd delegate — and they must not drift, so they share
// this one renderer.
//
// A stale holder gets the loud form. It is the case that cannot resolve
// itself: the holder runs an unlinked image, so every retry meets the same
// wrong process, and under systemd's Restart=always that is an unbounded
// crash loop (83439 restarts over five days in gc-f1081). Naming the state and
// the remedy is what turns a silent loop into something an operator can act on.
func supervisorAlreadyRunningMessage(prefix string, pid int, state supervisorHolderState, exePath string) string {
	if state != supervisorHolderStaleImage {
		return fmt.Sprintf("%s: supervisor already running (PID %d)\n", prefix, pid)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: supervisor already running (PID %d) on a DELETED image", prefix, pid)
	if exePath != "" {
		fmt.Fprintf(&b, " (%s)", exePath)
	}
	b.WriteString("\n")
	fmt.Fprintf(&b, "%s: that process runs a binary that no longer exists on disk, so no rebuild can reach it and restarting this one can never take over.\n", prefix)
	fmt.Fprintf(&b, "%s: the city is being served by a stale image until the holder is replaced. Stop it and start a current one:\n", prefix)
	fmt.Fprintf(&b, "%s:     gc stop --force && gc start\n", prefix)
	fmt.Fprintf(&b, "%s:     (or, if that does not clear it: kill %d, then gc start)\n", prefix, pid)
	fmt.Fprintf(&b, "%s: exiting terminally instead of restarting; verify with: ls -la /proc/%d/exe\n", prefix, pid)
	return b.String()
}
