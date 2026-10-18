package session

import "strings"

// LaunchTaskWorkDirMetadataKey records on a session row the task work_dir its
// current incarnation was launched in: the work_dir of a bead the session
// works on, which start preparation chose over the session's own work dir.
// Empty means the incarnation was launched in its own work dir. Start
// preparation reads it to keep a second session out of that directory. It is
// a key of its own, not worker_dir, because the worker_dir auto-prune removes
// a retiring pool session's worker_dir, and a task work_dir belongs to its
// bead, not to the session launched in it.
const LaunchTaskWorkDirMetadataKey = "launch_task_work_dir"

// LaunchTaskWorkDirPatch is the patch recording dir as the task work_dir a
// start launches info in, "" for a start in the session's own work dir. It is
// empty when info already records dir, so a start that leaves the session
// where it was writes nothing.
func LaunchTaskWorkDirPatch(info Info, dir string) MetadataPatch {
	dir = strings.TrimSpace(dir)
	if strings.TrimSpace(info.LaunchTaskWorkDir) == dir {
		return MetadataPatch{}
	}
	return MetadataPatch{LaunchTaskWorkDirMetadataKey: dir}
}

// WorkerDirFromInfo returns the agent process working directory recorded on a
// session, reading the canonical worker_dir mirror (Info.WorkerDir) first and
// falling back to the legacy work_dir mirror (Info.WorkDir) when the canonical
// value is absent or whitespace-only. Empty result means "no worker dir
// recorded."
//
// It is the session.Info form of contract.WorkerDirFromMetadata: because
// Info.WorkerDir mirrors beadmeta.WorkerDirMetadataKey verbatim and Info.WorkDir
// mirrors the legacy work_dir key verbatim, the canonical→legacy precedence and
// the whitespace-normalizing TrimSpace are byte-identical to the raw-metadata
// read. TestWorkerDirFromInfoMatchesContract pins that equivalence.
func WorkerDirFromInfo(info Info) string {
	if v := strings.TrimSpace(info.WorkerDir); v != "" {
		return v
	}
	return strings.TrimSpace(info.WorkDir)
}
