package kernel

import (
	"context"

	"github.com/cedar2025/xboard-node/internal/panel"
	"golang.org/x/time/rate"
)

// Kernel is the interface for proxy kernel backends (sing-box, xray, etc.)
type Kernel interface {
	// Name returns the kernel identifier (e.g. "sing-box")
	Name() string
	// ApplyConfig generates kernel config from panel data and (re)starts the process
	ApplyConfig(nodeConfig *panel.NodeConfig, users []panel.User, certFile, keyFile string) error
	// Stop gracefully stops the kernel process
	Stop()
	// IsRunning returns whether the kernel is currently active
	IsRunning() bool
	// GetConnections returns all active proxy connections
	GetConnections(ctx context.Context) ([]Connection, error)
	// CloseConnection terminates a specific connection by ID
	CloseConnection(ctx context.Context, connID string) error
	// SetSpeedLimitFunc configures optional per-user bandwidth throttling.
	// The function resolves a user UUID to a *rate.Limiter (nil = unlimited).
	// For sing-box, the limiter is embedded in the connection tracker wrapper
	// so both byte counting and rate limiting happen in a single CountFunc
	// callback, allowing splice/zero-copy paths.
	SetSpeedLimitFunc(fn func(uuid string) *rate.Limiter)
	// Reload hot-swaps the inbound users and routing rules without full kernel restart.
	// Existing connections may be briefly interrupted, but routes/outbounds stay alive.
	Reload(nodeConfig *panel.NodeConfig, users []panel.User, certFile, keyFile string) error
}

// Connection represents an active proxy connection reported by the kernel
type Connection struct {
	ID       string
	UserID   int    // extracted user ID (0 if unknown)
	Upload   int64  // cumulative upload bytes
	Download int64  // cumulative download bytes
	SourceIP string // client source IP (cleaned, no port/brackets)
}
