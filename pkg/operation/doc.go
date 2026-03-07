// Package operation tracks long-running operations (workspace creation,
// bundle compilation, checkpoint save) through a phase-based lifecycle:
// pending -> running -> succeeded/failed/canceled.
package operation
