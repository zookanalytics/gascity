// Package checkpoint defines the recovery manifest format and store interface
// for workspace checkpointing. A checkpoint captures the precise state needed
// to resume a workspace after migration or failure: snapshot ID, event cursor,
// bead sequence, and transcript position.
//
// The Store interface has a single filesystem-backed implementation (LocalStore)
// for development and testing. Cloud implementations are provided separately.
package checkpoint
