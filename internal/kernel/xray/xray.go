package xray

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	xrayCore "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/infra/conf/serial"
	"golang.org/x/time/rate"

	_ "github.com/xtls/xray-core/main/distro/all"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/kernel/geodata"
	"github.com/cedar2025/xboard-node/internal/panel"
)

// drainTimeout is how long stop() waits for in-flight connections to finish
// naturally before hard-killing the xray instance.
const drainTimeout = 5 * time.Second

// Xray implements kernel.Kernel by embedding xray-core as a Go library.
//
// Device limits and speed limits are enforced through a custom dispatcher
// wrapper (LimitDispatcher) that hooks into session.InboundFromContext(ctx)
// — the same approach used by V2bX and XrayR. This works for ALL protocols
// because the dispatcher is called AFTER the protocol handler has identified
// the user and recorded the source IP in the session context.
type Xray struct {
	cfg config.KernelConfig

	mu       sync.Mutex
	instance *xrayCore.Instance
	users    []panel.User
	protocol string

	inboundTag string

	// limitDispatcher is the per-instance LimitDispatcher created during
	// xrayCore.New(). Stored here (instead of using the global pointer)
	// so that multi-node mode with multiple Xray instances works correctly.
	limitDispatcher *LimitDispatcher

	// lastKernelHash caches a hash of the kernel-affecting config + user
	// identities. Used by Reload to skip unnecessary full restarts.
	lastKernelHash string

	// cumTraffic keeps running totals for the aggregate stats fallback.
	cumTraffic map[int][2]int64
}

func New(cfg config.KernelConfig) *Xray {
	return &Xray{
		cfg:        cfg,
		cumTraffic: make(map[int][2]int64),
	}
}

func (x *Xray) Name() string { return "xray" }

func (x *Xray) ApplyConfig(nodeConfig *panel.NodeConfig, users []panel.User, certFile, keyFile string) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	// Auto-download geo databases and set XRAY_LOCATION_ASSET when panel
	// routes reference geoip:/geosite: entries.
	if kernel.NeedsGeoIP(nodeConfig.Routes) || kernel.NeedsGeoSite(nodeConfig.Routes) {
		geoDataDir := x.cfg.GeoDataDir
		if err := geodata.Ensure(geoDataDir,
			kernel.NeedsGeoIP(nodeConfig.Routes),
			kernel.NeedsGeoSite(nodeConfig.Routes),
			"xray",
		); err != nil {
			slog.Warn("geo database unavailable; geoip/geosite rules may not match", "error", err)
		}
		os.Setenv("XRAY_LOCATION_ASSET", geoDataDir)
	}

	cfgMap := buildConfig(x.cfg, nodeConfig, users, certFile, keyFile)
	data, err := json.Marshal(cfgMap)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	slog.Debug("xray config generated", "len", len(data))

	pbConfig, err := serial.LoadJSONConfig(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("parse xray config: %w", err)
	}

	x.stop()

	// Hold the global creation mutex around xrayCore.New() + capture so that
	// concurrent Xray instances (multi-node) don't clobber each other's
	// LimitDispatcher reference.
	xrayCreationMu.Lock()
	instance, err := xrayCore.New(pbConfig)
	if err != nil {
		xrayCreationMu.Unlock()
		return fmt.Errorf("create xray instance: %w", err)
	}
	x.limitDispatcher = globalLimitDispatcher.Load()
	xrayCreationMu.Unlock()

	if err := instance.Start(); err != nil {
		instance.Close()
		return fmt.Errorf("start xray: %w", err)
	}

	x.instance = instance
	x.users = users
	x.protocol = nodeConfig.Protocol
	x.inboundTag = nodeConfig.Protocol + "-in"
	x.cumTraffic = make(map[int][2]int64)
	x.lastKernelHash = computeKernelHash(nodeConfig, users)

	// Configure the limit dispatcher with user limits
	x.updateDispatcherLimits(users)

	slog.Info("xray started (in-process)",
		"users", len(users),
		"protocol", nodeConfig.Protocol,
		"device_limits", "enabled (all protocols via dispatcher hook)",
		"speed_limits", "enabled (all protocols via dispatcher hook)",
	)

	return nil
}

func (x *Xray) Reload(nodeConfig *panel.NodeConfig, users []panel.User, certFile, keyFile string) error {
	// Always update dispatcher limits (instant, no restart).
	x.updateDispatcherLimits(users)

	// Skip full restart if kernel-affecting fields are unchanged.
	// This covers user-only limit changes where identities (ID, UUID) and
	// config (routes, protocol settings, port) haven't changed.
	newHash := computeKernelHash(nodeConfig, users)
	x.mu.Lock()
	same := x.lastKernelHash == newHash
	x.mu.Unlock()
	if same {
		x.mu.Lock()
		x.users = users
		x.mu.Unlock()
		slog.Info("xray: config/users unchanged, limits updated without restart")
		return nil
	}

	return x.ApplyConfig(nodeConfig, users, certFile, keyFile)
}

func (x *Xray) Stop() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.stop()
}

func (x *Xray) stop() {
	if x.instance != nil {
		// Drain: wait for in-flight connections to finish naturally.
		if x.limitDispatcher != nil {
			deadline := time.Now().Add(drainTimeout)
			for time.Now().Before(deadline) {
				x.limitDispatcher.mu.RLock()
				n := len(x.limitDispatcher.conns)
				x.limitDispatcher.mu.RUnlock()
				if n == 0 {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		x.instance.Close()
		x.instance = nil
	}
	if x.limitDispatcher != nil {
		x.limitDispatcher.ResetConns()
		x.limitDispatcher = nil
	}
}

func (x *Xray) IsRunning() bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.instance != nil
}

// GetConnections returns per-connection data from the dispatcher.
// Each connection has a real source IP and user ID, enabling device limit
// enforcement by the upper-layer limiter.
func (x *Xray) GetConnections(ctx context.Context) ([]kernel.Connection, error) {
	x.mu.Lock()
	defer x.mu.Unlock()

	if x.instance == nil {
		return nil, nil
	}

	// Primary: use LimitDispatcher's per-connection tracking
	if x.limitDispatcher != nil {
		conns := x.limitDispatcher.Snapshot()
		if len(conns) > 0 {
			return conns, nil
		}
	}

	// Fallback: aggregate stats (for edge cases)
	return x.getAggregateStats()
}

func (x *Xray) getAggregateStats() ([]kernel.Connection, error) {
	sm := x.instance.GetFeature(stats.ManagerType())
	if sm == nil {
		return nil, nil
	}
	statsManager, ok := sm.(stats.Manager)
	if !ok {
		return nil, nil
	}

	var conns []kernel.Connection
	for _, u := range x.users {
		email := userEmail(u.ID)

		var deltaUp, deltaDown int64
		if c := statsManager.GetCounter(fmt.Sprintf("user>>>%s>>>traffic>>>uplink", email)); c != nil {
			deltaUp = c.Set(0)
		}
		if c := statsManager.GetCounter(fmt.Sprintf("user>>>%s>>>traffic>>>downlink", email)); c != nil {
			deltaDown = c.Set(0)
		}

		if deltaUp > 0 || deltaDown > 0 {
			cum := x.cumTraffic[u.ID]
			cum[0] += deltaUp
			cum[1] += deltaDown
			x.cumTraffic[u.ID] = cum
		}

		cum := x.cumTraffic[u.ID]
		if cum[0] > 0 || cum[1] > 0 {
			conns = append(conns, kernel.Connection{
				ID:       fmt.Sprintf("xray-%d", u.ID),
				UserID:   u.ID,
				Upload:   cum[0],
				Download: cum[1],
			})
		}
	}
	return conns, nil
}

func (x *Xray) CloseConnection(_ context.Context, connID string) error {
	x.mu.Lock()
	ld := x.limitDispatcher
	x.mu.Unlock()
	if ld != nil {
		ld.CloseConn(connID)
	}
	return nil
}

func (x *Xray) SetSpeedLimitFunc(_ func(string) *rate.Limiter) {}

var _ kernel.Kernel = (*Xray)(nil)

// updateDispatcherLimits configures the global LimitDispatcher with
// per-user device limits and speed limits.
func (x *Xray) updateDispatcherLimits(users []panel.User) {
	x.mu.Lock()
	ld := x.limitDispatcher
	x.mu.Unlock()
	if ld == nil {
		return
	}

	emailToUID := make(map[string]int, len(users))
	deviceLimits := make(map[string]int)
	speedLimits := make(map[string]int)

	for _, u := range users {
		email := userEmail(u.ID)
		emailToUID[email] = u.ID
		// SOCKS/HTTP protocols use the raw UUID as User.Email in xray's session,
		// while VMess/VLESS/Trojan use the "user@<id>" format. Map both so that
		// limits work for all protocols.
		emailToUID[u.UUID] = u.ID
		if u.DeviceLimit > 0 {
			deviceLimits[email] = u.DeviceLimit
			deviceLimits[u.UUID] = u.DeviceLimit
		}
		if u.SpeedLimit > 0 {
			speedLimits[email] = u.SpeedLimit
			speedLimits[u.UUID] = u.SpeedLimit
		}
	}

	ld.UpdateLimits(emailToUID, deviceLimits, speedLimits)
}

// xrayCreationMu serialises xrayCore.New() + globalLimitDispatcher capture
// so that concurrent Xray instances in multi-node mode each capture their
// own LimitDispatcher.
var xrayCreationMu sync.Mutex

// computeKernelHash returns a hash of the config + user identities (ID, UUID)
// that would require a kernel restart if changed. Limit-only changes are
// excluded so they can be applied without restart.
func computeKernelHash(nc *panel.NodeConfig, users []panel.User) string {
	h := sha256.New()
	configData, _ := json.Marshal(nc)
	h.Write(configData)
	sorted := make([]panel.User, len(users))
	copy(sorted, users)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	for _, u := range sorted {
		fmt.Fprintf(h, "%d:%s,", u.ID, u.UUID)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
