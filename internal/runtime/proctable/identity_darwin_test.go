//go:build darwin

package proctable

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDarwinStartIdentityKeepsMicrosecondPrecision(t *testing.T) {
	first, err := darwinStartIdentity(unix.Timeval{Sec: 123, Usec: 1})
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	second, err := darwinStartIdentity(unix.Timeval{Sec: 123, Usec: 2})
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if first == second {
		t.Fatalf("same-second process starts collapsed to %q", first)
	}
}

func TestDarwinProcessRecordUsesOneKinfoIdentity(t *testing.T) {
	process := unix.KinfoProc{
		Proc: unix.ExternProc{
			P_pid:       42,
			P_starttime: unix.Timeval{Sec: 123, Usec: 456},
		},
		Eproc: unix.Eproc{Ppid: 11, Pgid: 12},
	}
	copy(process.Proc.P_comm[:], []byte("pane-shell\x00foreign"))

	record, ok := darwinProcessRecord(process)
	if !ok {
		t.Fatal("valid KinfoProc was rejected")
	}
	wantIdentity, err := darwinStartIdentity(process.Proc.P_starttime)
	if err != nil {
		t.Fatalf("start identity: %v", err)
	}
	if record.PID != 42 || record.PPID != 11 || record.PGID != 12 || record.StartTime != wantIdentity || record.Name != "pane-shell" {
		t.Fatalf("darwinProcessRecord = %+v, want PID/PPID/PGID/start/name from one KinfoProc", record)
	}
}

func TestSnapshotProcessIdentityMatchesCurrentReader(t *testing.T) {
	self := os.Getpid()
	records, err := SnapshotProcesses()
	if err != nil {
		t.Fatalf("SnapshotProcesses: %v", err)
	}
	var snapshotIdentity string
	for _, record := range records {
		if record.PID == self {
			snapshotIdentity = record.StartTime
			break
		}
	}
	if snapshotIdentity == "" {
		t.Fatalf("current process %d missing from coherent kernel snapshot", self)
	}

	currentIdentity, err := ProcessIdentity(self)
	if err != nil {
		t.Fatalf("ProcessIdentity(%d): %v", self, err)
	}
	if currentIdentity != snapshotIdentity {
		t.Fatalf("current identity = %q, snapshot identity = %q; destructive selection and signal recheck must share one contract", currentIdentity, snapshotIdentity)
	}
}
