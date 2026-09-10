//go:build linux

package execgrace

import (
	"bytes"
	"os"
	"strconv"
)

// foregroundGroupMembers returns the pids in process group pgid other than the
// leader, read from /proc. It is used to identify the foreground child(ren)
// blocking a shell's rollback trap so the re-interrupt ladder can target them
// without touching processes the trap later spawns.
//
// An unreadable /proc or /proc/<pid>/stat entry is skipped rather than failed:
// the scan races process exit by nature, and a missed member only forgoes one
// re-interrupt of an already-vanishing process.
func foregroundGroupMembers(pgid, leader int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var members []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == leader {
			continue
		}
		if pgrp, ok := statProcessGroup(entry.Name()); ok && pgrp == pgid {
			members = append(members, pid)
		}
	}
	return members
}

// statProcessGroup reads the process group id (field 5) from /proc/<pid>/stat.
// The comm field (2) is parenthesized and may itself contain spaces and
// parens, so fields are counted from after the final ')': the remainder is
// "state ppid pgrp ...", making pgrp the third whitespace-separated field.
func statProcessGroup(pid string) (int, bool) {
	data, err := os.ReadFile("/proc/" + pid + "/stat")
	if err != nil {
		return 0, false
	}
	commEnd := bytes.LastIndexByte(data, ')')
	if commEnd < 0 {
		return 0, false
	}
	fields := bytes.Fields(data[commEnd+1:])
	if len(fields) < 3 {
		return 0, false
	}
	pgrp, err := strconv.Atoi(string(fields[2]))
	if err != nil {
		return 0, false
	}
	return pgrp, true
}
