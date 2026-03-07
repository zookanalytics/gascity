// Package bundle defines the bundle manifest format and verification
// functions. A bundle is a deployable unit containing a city configuration
// snapshot: config, templates, and metadata needed to launch a workspace.
//
// This package covers format, manifest validation, checksum verification,
// and schema version compatibility. The actual compilation logic (wiring
// to config pack loading) is a separate concern.
package bundle
