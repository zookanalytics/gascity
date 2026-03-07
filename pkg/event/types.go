package event

// Hosted event type constants. These extend the internal/events vocabulary
// for hosted operations.
const (
	WorkspaceCreated   = "workspace.created"
	WorkspaceSuspended = "workspace.suspended"
	WorkspaceResumed   = "workspace.resumed"
	WorkspaceDeleted   = "workspace.deleted"
	WorkspaceFailed    = "workspace.failed"

	RuntimeStarted   = "runtime.started"
	RuntimePaused    = "runtime.paused"
	RuntimeResumed   = "runtime.resumed"
	RuntimeCompleted = "runtime.completed"
	RuntimeFailed    = "runtime.failed"
	RuntimeExpired   = "runtime.expired"

	BundleCompiled = "bundle.compiled"
	BundleDeployed = "bundle.deployed"

	CheckpointSaved    = "checkpoint.saved"
	CheckpointRestored = "checkpoint.restored"

	RuntimeSelfFenced = "runtime.self-fenced"

	OperationStarted   = "operation.started"
	OperationCompleted = "operation.completed"
	OperationFailed    = "operation.failed"
)
