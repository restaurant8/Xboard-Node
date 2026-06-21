// Package buildinfo exposes build-time version metadata to the whole binary.
//
// The values are injected at link time via -ldflags. Both cmd/xboard-node and
// cmd/xbctl share these variables so the running service can report its own
// version over WebSocket (used by the panel's backend-management UI) without
// each package having to plumb the values down from main.
package buildinfo

var (
	// Version is the release tag (e.g. "v1.2.3") or "dev" for local builds.
	Version = "dev"
	// BuildTime is the UTC build timestamp.
	BuildTime = "unknown"
	// Commit is the short git commit hash.
	Commit = "unknown"
)
