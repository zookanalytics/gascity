// Package lifecycle provides state machines for hosted workspace and runtime resources.
//
// WorkspaceState tracks the lifecycle of a hosted workspace (creation,
// suspension, deletion). RuntimeState tracks an agent runtime execution
// within a workspace. Both are pure value types with validated transitions
// — no I/O, no dependencies.
//
// StateCategory provides forward-compatible grouping so that consumers can
// handle unknown future states gracefully (e.g., treat any "transitioning"
// state as busy).
package lifecycle
