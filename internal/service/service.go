package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/cedar2025/xboard-node/internal/cert"
	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/kernel/singbox"
	"github.com/cedar2025/xboard-node/internal/kernel/xray"
	"github.com/cedar2025/xboard-node/internal/limiter"
	"github.com/cedar2025/xboard-node/internal/monitor"
	"github.com/cedar2025/xboard-node/internal/panel"
	"github.com/cedar2025/xboard-node/internal/tracker"
)

type Service struct {
	cfg          *config.Config
	panel        *panel.Client
	kernel       kernel.Kernel
	tracker      *tracker.Tracker
	limiter      *limiter.Limiter
	speedTracker *limiter.SpeedTracker
	cert         *cert.Manager

	lastConfig *panel.NodeConfig
	lastUsers  []panel.User

	lastAppliedPort int    // the port currently bound by the kernel
	lastAppliedIP   string // the listen IP currently bound by the kernel

	pushInterval int // seconds
	pullInterval int // seconds

	lastUserHash   string     // hash of user list for change detection
	lastConfigHash string     // hash of full config for change detection
	pullBackoff    apiBackoff // backoff for panel pull failures
	pushBackoff    apiBackoff // backoff for panel push failures

	wsClient       *panel.WSClient           // WebSocket client (nil if WS not enabled)
	wsEvents       chan panel.WSEvent        // receives data events from WS client
	wsStatusCh     chan panel.WSStatusChange // receives WS connect/disconnect notifications
	wsCancel       context.CancelFunc        // cancels the WS client goroutine
	wsDisconnectAt time.Time                 // when WS last disconnected (zero if connected)
}

// apiBackoff implements simple exponential backoff for API failures.
type apiBackoff struct {
	skipRemaining int
}

func (b *apiBackoff) shouldSkip() bool {
	if b.skipRemaining > 0 {
		b.skipRemaining--
		return true
	}
	return false
}

func (b *apiBackoff) onSuccess() { b.skipRemaining = 0 }
func (b *apiBackoff) onFailure() {
	if b.skipRemaining <= 0 {
		b.skipRemaining = 1
	} else if b.skipRemaining < 8 {
		b.skipRemaining *= 2
	}
}

func New(cfg *config.Config) *Service {
	panelClient := panel.NewClient(cfg.Panel)
	certMgr := cert.NewManager(cfg.Cert)

	var k kernel.Kernel
	switch cfg.Kernel.Type {
	case "singbox":
		k = singbox.New(cfg.Kernel)
	case "xray":
		k = xray.New(cfg.Kernel)
	default:
		slog.Error("unsupported kernel type, defaulting to sing-box", "type", cfg.Kernel.Type)
		k = singbox.New(cfg.Kernel)
	}

	l := limiter.New()
	st := limiter.NewSpeedTracker(l)

	return &Service{
		cfg:          cfg,
		panel:        panelClient,
		kernel:       k,
		tracker:      tracker.New(),
		limiter:      l,
		speedTracker: st,
		cert:         certMgr,
		wsEvents:     make(chan panel.WSEvent, 16),
		wsStatusCh:   make(chan panel.WSStatusChange, 4),
	}
}

func (s *Service) Run(ctx context.Context) error {
	// Start cert manager (handles auto-TLS or manual cert verification)
	if err := s.cert.Start(ctx); err != nil {
		return fmt.Errorf("cert manager: %w", err)
	}
	defer s.cert.Stop()

	// Handshake: get WS config + initial data in one call
	if err := s.initialSetup(ctx); err != nil {
		return fmt.Errorf("initial setup: %w", err)
	}
	defer s.kernel.Stop()

	// Set up tickers
	trackTicker := time.NewTicker(10 * time.Second)
	pushInterval := time.Duration(math.Max(float64(s.pushInterval), 5)) * time.Second
	pullInterval := time.Duration(s.pullInterval) * time.Second
	reportTicker := time.NewTicker(pushInterval)
	pullTicker := time.NewTicker(pullInterval)

	// WS discovery: when in REST-only mode, periodically re-handshake to check
	// if WS has been enabled. When WS is disconnected for too long, re-check
	// if it's still available.
	wsDiscoveryTicker := time.NewTicker(5 * time.Minute)

	defer trackTicker.Stop()
	defer reportTicker.Stop()
	defer pullTicker.Stop()
	defer wsDiscoveryTicker.Stop()

	s.startWSClient(ctx)

	sMode := "disabled"
	if s.wsClient != nil {
		sMode = "connecting"
	}
	slog.Info("service started",
		"kernel", s.kernel.Name(),
		"push_interval", s.pushInterval,
		"pull_interval", s.pullInterval,
		"websocket", sMode,
	)

	for {
		select {
		case <-ctx.Done():
			slog.Info("service shutting down")
			s.pushReport()
			return nil

		case <-trackTicker.C:
			s.trackAndEnforce(ctx)

		case <-reportTicker.C:
			s.pushReport()

		case <-pullTicker.C:
			// When WebSocket is connected, skip REST polling entirely.
			// Config/user updates arrive via WS push.
			if s.wsClient != nil && s.wsClient.IsConnected() {
				slog.Debug("ws connected, skipping REST pull")
				continue
			}
			slog.Info("ws not connected or disabled, polling from API")
			s.pullViaAPI(ctx)

		case <-wsDiscoveryTicker.C:
			s.wsDiscovery(ctx)

		case status := <-s.wsStatusCh:
			s.handleWSStatus(ctx, status)

		case event := <-s.wsEvents:
			s.handleWSEvent(ctx, event)
		}
	}
}

func (s *Service) initialSetup(ctx context.Context) error {
	// Register speed limit lookup with kernel unconditionally (before WS/V1 branch).
	// This ensures speedLimitFunc is set regardless of which code path applies config.
	s.kernel.SetSpeedLimitFunc(s.speedTracker.GetLimiter)

	slog.Info("performing handshake with panel")

	var hs *panel.HandshakeResponse
	var err error

	// Retry loop for initial handshake
	for attempt := 1; ; attempt++ {
		hs, err = s.panel.Handshake()
		if err != nil {
			slog.Error("handshake failed", "attempt", attempt, "error", err)
			if attempt >= 5 {
				return fmt.Errorf("handshake failed after %d attempts: %w", attempt, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*5) * time.Second):
				continue
			}
		}
		break
	}

	// Apply settings from panel handshake (Handshake provides default intervals)
	if s.cfg.Node.PushInterval == 0 && hs.Settings.PushInterval > 0 {
		s.pushInterval = hs.Settings.PushInterval
	} else {
		s.pushInterval = s.cfg.Node.PushInterval
	}
	if s.pushInterval == 0 {
		s.pushInterval = 60
	}

	if s.cfg.Node.PullInterval == 0 && hs.Settings.PullInterval > 0 {
		s.pullInterval = hs.Settings.PullInterval
	} else {
		s.pullInterval = s.cfg.Node.PullInterval
	}
	if s.pullInterval == 0 {
		s.pullInterval = 60
	}

	// If WebSocket is enabled, skip initial V1 APIs fetch and wait for推送
	if hs.WebSocket.Enabled && hs.WebSocket.WSURL != "" {
		s.wsClient = s.newWSClient(hs.WebSocket.WSURL)
		slog.Info("handshake complete (websocket enabled), waiting for data push...",
			"ws_url", hs.WebSocket.WSURL,
		)
		return nil
	}

	// Falls back to V1 APIs only if WebSocket is disabled in panel
	slog.Info("websocket disabled or missing, falling back to V1 APIs")

	// Fetch initial config and users via V1 APIs
	nodeConfig, err := s.panel.GetConfig()
	if err != nil {
		return fmt.Errorf("initial config fetch: %w", err)
	}
	if nodeConfig == nil {
		return fmt.Errorf("initial config is nil")
	}

	users, err := s.panel.GetUsers()
	if err != nil {
		return fmt.Errorf("initial user fetch: %w", err)
	}

	// Apply initial data
	s.lastConfig = nodeConfig
	s.lastUsers = users
	s.lastUserHash = computeUserHash(users)
	s.lastConfigHash = computeConfigHash(nodeConfig)
	s.limiter.UpdateUsers(users)
	s.speedTracker.UpdateBuckets()

	slog.Info("handshake complete (V1 fallback)",
		"protocol", nodeConfig.Protocol,
		"port", nodeConfig.ServerPort,
		"users", len(users),
	)

	if len(users) == 0 {
		slog.Warn("no users, kernel will not start until users are available")
		return nil
	}

	// Update service-level config overrides from remote NodeConfig if present
	s.applyRemoteOverrides(ctx, nodeConfig)

	s.lastAppliedPort = nodeConfig.ServerPort
	s.lastAppliedIP = nodeConfig.ListenIP

	certFile, keyFile := s.cert.CertFile(), s.cert.KeyFile()
	if err := s.kernel.ApplyConfig(nodeConfig, users, certFile, keyFile); err != nil {
		return fmt.Errorf("apply config: %w", err)
	}

	return nil
}

// applyRemoteOverrides updates service-level settings (log level, cert config)
// from the panel's NodeConfig. Returns true if cert paths changed (kernel restart needed).
func (s *Service) applyRemoteOverrides(ctx context.Context, nc *panel.NodeConfig) bool {
	if nc == nil {
		return false
	}

	// Dynamic Log Level (Kernel)
	if nc.KernelLogLevel != "" && nc.KernelLogLevel != s.cfg.Kernel.LogLevel {
		slog.Info("cert: kernel log level override", "old", s.cfg.Kernel.LogLevel, "new", nc.KernelLogLevel)
		s.cfg.Kernel.LogLevel = nc.KernelLogLevel
	}

	// Certificate configuration from panel (panel-first: takes precedence over local config)
	if nc.CertConfig != nil {
		return s.applyPanelCert(ctx, nc.CertConfig)
	}

	// Legacy fields (deprecated: prefer cert_config)
	if nc.AutoTLS != s.cfg.Cert.AutoTLS {
		slog.Info("cert: auto_tls policy changed (deprecated field)", "new", nc.AutoTLS)
		s.cfg.Cert.AutoTLS = nc.AutoTLS
	}
	if nc.Domain != "" && nc.Domain != s.cfg.Cert.Domain {
		s.cfg.Cert.Domain = nc.Domain
	}

	return false
}

// applyPanelCert converts a panel CertConfig into the local config format and
// reconfigures the cert manager. Reports whether cert paths changed.
func (s *Service) applyPanelCert(ctx context.Context, pc *panel.CertConfig) bool {
	newCfg := config.CertConfig{
		CertMode:    pc.CertMode,
		Domain:      pc.Domain,
		Email:       pc.Email,
		DNSProvider: pc.DNSProvider,
		DNSEnv:      pc.DNSEnv,
		HTTPPort:    pc.HTTPPort,
		CertFile:    pc.CertFile,
		KeyFile:     pc.KeyFile,
		CertContent: pc.CertContent,
		KeyContent:  pc.KeyContent,
		// Preserve local storage dir — only the operator controls where certs live.
		CertDir: s.cfg.Cert.CertDir,
	}

	changed, err := s.cert.Reconfigure(ctx, newCfg)
	if err != nil {
		slog.Error("failed to apply panel cert config", "mode", pc.CertMode, "error", err)
		return false
	}
	s.cfg.Cert = newCfg
	if changed {
		slog.Info("cert: paths updated from panel", "cert", s.cert.CertFile(), "key", s.cert.KeyFile())
	}
	return changed
}

// startWSClient starts the WS client goroutine if a client is configured.
func (s *Service) startWSClient(ctx context.Context) {
	if s.wsClient == nil {
		return
	}
	wsCtx, wsCancel := context.WithCancel(ctx)
	s.wsCancel = wsCancel
	go s.wsClient.Run(wsCtx)
}

// newWSClient creates a WSClient with standard event/status callbacks.
func (s *Service) newWSClient(wsURL string) *panel.WSClient {
	return panel.NewWSClient(
		wsURL,
		s.cfg.Panel.Token,
		s.cfg.Panel.NodeID,
		func(event panel.WSEvent) {
			select {
			case s.wsEvents <- event:
			default:
				slog.Warn("ws event channel full, dropping event", "type", event.Type)
			}
		},
		func(status panel.WSStatusChange) {
			select {
			case s.wsStatusCh <- status:
			default:
			}
		},
	)
}

// handleWSStatus reacts to WS connectivity changes.
//
// - On disconnect: record timestamp, immediately REST poll.
// - On reconnect: clear disconnect timestamp, REST poll to catch missed events.
func (s *Service) handleWSStatus(ctx context.Context, status panel.WSStatusChange) {
	if status.Connected {
		s.wsDisconnectAt = time.Time{}
		slog.Info("ws connected, waiting for server-side full sync push")
		// No REST pull needed — server detects new connection and pushes
		// full config + users automatically on auth success.
	} else {
		if s.wsDisconnectAt.IsZero() {
			s.wsDisconnectAt = time.Now()
		}
		slog.Info("ws disconnected, falling back to REST polling")
		s.pullViaAPI(ctx)
	}
}

// wsDiscovery periodically checks WS availability:
//
//  1. REST-only mode (wsClient == nil): Re-handshake to check if panel now has
//     WS enabled. If so, create and start a WS client. This handles the case
//     where WS was not enabled at startup but enabled later.
//
//  2. WS disconnected for >10 min: Re-handshake to check if WS config changed.
//     If WS is now disabled, stop the WS client and switch to REST-only.
//     If WS config changed (different URL/channel), restart with new config.
func (s *Service) wsDiscovery(ctx context.Context) {
	needsCheck := false

	if s.wsClient == nil {
		needsCheck = true
		slog.Debug("ws discovery: no WS client, checking if panel enabled WS")
	} else if !s.wsDisconnectAt.IsZero() && time.Since(s.wsDisconnectAt) > 10*time.Minute {
		needsCheck = true
		slog.Debug("ws discovery: WS disconnected for >10min, re-checking WS config")
	}

	if !needsCheck {
		return
	}

	hs, err := s.panel.Handshake()
	if err != nil {
		slog.Debug("ws discovery: handshake failed", "error", err)
		return
	}

	// Apply any latest config/user changes via dedicated APIs
	s.pullViaAPI(ctx)

	if hs.WebSocket.Enabled && hs.WebSocket.WSURL != "" {
		if s.wsClient == nil {
			slog.Info("ws discovery: panel has WS enabled, creating WS client")
			s.wsClient = s.newWSClient(hs.WebSocket.WSURL)
			s.wsDisconnectAt = time.Time{}
			s.startWSClient(ctx)
		}
	} else if s.wsClient != nil {
		slog.Info("ws discovery: panel disabled WS, switching to REST-only")
		if s.wsCancel != nil {
			s.wsCancel()
		}
		s.wsClient = nil
		s.wsCancel = nil
		s.wsDisconnectAt = time.Time{}
	}
}

// handleWSEvent processes data events received via WebSocket
func (s *Service) handleWSEvent(ctx context.Context, event panel.WSEvent) {
	switch event.Type {
	case panel.WSEventSyncConfig:
		if event.Config == nil {
			slog.Warn("ws: sync.config event with nil config, ignoring")
			return
		}
		newConfigHash := computeConfigHash(event.Config)
		if newConfigHash == s.lastConfigHash {
			slog.Debug("ws: config unchanged (same hash), skipping")
			return
		}
		slog.Info("ws: applying config update", "protocol", event.Config.Protocol)
		s.lastConfig = event.Config
		s.lastConfigHash = newConfigHash
		s.applyRemoteOverrides(ctx, event.Config)
		s.applyChanges(ctx, true, false)

	case panel.WSEventSyncUsers:
		if event.Users == nil {
			slog.Warn("ws: sync.users event with nil users, ignoring")
			return
		}
		newHash := computeUserHash(event.Users)
		if newHash == s.lastUserHash {
			slog.Debug("ws: users unchanged (same hash), skipping")
			return
		}
		slog.Info("ws: applying user update", "count", len(event.Users))
		s.applyUserUpdate(ctx, event.Users, newHash)

	case panel.WSEventSyncUserDelta:
		if len(event.DeltaUsers) == 0 {
			slog.Warn("ws: sync.user.delta event with empty users, ignoring")
			return
		}
		slog.Info("ws: applying user delta", "action", event.DeltaAction, "count", len(event.DeltaUsers))
		s.applyUserDelta(ctx, event.DeltaAction, event.DeltaUsers)

	default:
		slog.Debug("ws: unknown event type", "type", event.Type)
	}
}

// pullViaAPI re-calls the config and user APIs for polling fallback.
// Also opportunistically detects WS enablement when in REST-only mode.
func (s *Service) pullViaAPI(ctx context.Context) {
	if s.pullBackoff.shouldSkip() {
		slog.Debug("skipping pull due to backoff")
		return
	}

	config, err := s.panel.GetConfig()
	if err != nil {
		slog.Error("poll config failed", "error", err)
		s.pullBackoff.onFailure()
		return
	}

	users, err := s.panel.GetUsers()
	if err != nil {
		slog.Error("poll users failed", "error", err)
		s.pullBackoff.onFailure()
		return
	}

	s.pullBackoff.onSuccess()

	configChanged := false

	if s.cert.CertRenewed() {
		slog.Info("certificate renewed, kernel restart needed")
		configChanged = true
	}

	if config != nil {
		newConfigHash := computeConfigHash(config)
		if s.lastConfigHash != newConfigHash {
			configChanged = true
			slog.Info("config updated from panel",
				"protocol", config.Protocol, "port", config.ServerPort)
			s.lastConfig = config
			s.lastConfigHash = newConfigHash
			// Apply kernel overrides and cert updates from REST pull
			if s.applyRemoteOverrides(ctx, config) {
				configChanged = true // Restart kernel if cert paths changed
			}
		}
	}

	if users != nil {
		newHash := computeUserHash(users)
		usersChanged := newHash != s.lastUserHash

		if usersChanged && !configChanged {
			s.applyUserUpdate(ctx, users, newHash)
		} else if usersChanged {
			// Config is also changing — ApplyConfig will restart the kernel with the
			// updated user list, so skip the intermediate ReloadUsers.
			s.syncUserState(ctx, users, newHash, false)
		}
	}

	if configChanged {
		s.applyChanges(ctx, true, false)
	}
}

// syncUserState updates user tracking state (limiter, speedTracker, lastUsers)
// without touching the kernel. Set kickRemoved=false when a full kernel restart
// is about to happen anyway — removed users' connections will be killed then.
func (s *Service) syncUserState(ctx context.Context, users []panel.User, newHash string, kickRemoved bool) {
	removedIDs := s.limiter.UpdateUsers(users)
	s.speedTracker.UpdateBuckets()

	if kickRemoved && len(removedIDs) > 0 && s.kernel.IsRunning() {
		conns, err := s.kernel.GetConnections(ctx)
		if err == nil {
			kicks := s.limiter.KickUsers(conns, removedIDs)
			s.executeKicks(ctx, kicks)
			slog.Info("kicked removed users", "removed_ids", removedIDs, "kicked_conns", len(kicks))
		}
	}

	s.lastUsers = users
	s.lastUserHash = newHash
	slog.Info("users updated", "count", len(users))
}

// applyUserUpdate updates user state and hot-reloads the kernel inbounds.
func (s *Service) applyUserUpdate(ctx context.Context, users []panel.User, newHash string) {
	s.syncUserState(ctx, users, newHash, true)
	s.applyChanges(ctx, false, true)
}

// applyUserDelta applies an incremental user change (add or remove).
func (s *Service) applyUserDelta(ctx context.Context, action string, deltaUsers []panel.User) {
	switch action {
	case "add":
		// Build a map of current users for fast lookup
		userMap := make(map[int]panel.User, len(s.lastUsers))
		for _, u := range s.lastUsers {
			userMap[u.ID] = u
		}
		for _, u := range deltaUsers {
			userMap[u.ID] = u // add or update
		}
		merged := make([]panel.User, 0, len(userMap))
		for _, u := range userMap {
			merged = append(merged, u)
		}
		newHash := computeUserHash(merged)
		s.applyUserUpdate(ctx, merged, newHash)

	case "remove":
		removeSet := make(map[int]struct{}, len(deltaUsers))
		for _, u := range deltaUsers {
			removeSet[u.ID] = struct{}{}
		}
		filtered := make([]panel.User, 0, len(s.lastUsers))
		for _, u := range s.lastUsers {
			if _, ok := removeSet[u.ID]; !ok {
				filtered = append(filtered, u)
			}
		}
		newHash := computeUserHash(filtered)
		s.applyUserUpdate(ctx, filtered, newHash)
	default:
		slog.Warn("ws: unknown user delta action", "action", action)
	}
}

// applyChanges applies config and/or user changes to the kernel
func (s *Service) applyChanges(ctx context.Context, configChanged, usersChanged bool) {
	certFile, keyFile := s.cert.CertFile(), s.cert.KeyFile()

	if configChanged {
		if s.lastConfig != nil && len(s.lastUsers) > 0 {
			// If the port or listen IP has changed, we MUST perform a full restart
			// because hot-reload (s.kernel.Reload) typically only updates users/rules
			// and doesn't re-bind existing listeners in most kernels.
			needsRestart := s.kernel.IsRunning() && (s.lastConfig.ServerPort != s.lastAppliedPort || s.lastConfig.ListenIP != s.lastAppliedIP)

			if !needsRestart {
				// Try hot-reload for other config changes (could be route/user changes).
				if err := s.kernel.Reload(s.lastConfig, s.lastUsers, certFile, keyFile); err != nil {
					slog.Warn("reload failed, falling back to full restart", "error", err)
					needsRestart = true
				}
			}

			if needsRestart || !s.kernel.IsRunning() {
				if err := s.kernel.ApplyConfig(s.lastConfig, s.lastUsers, certFile, keyFile); err != nil {
					slog.Error("failed to apply config", "error", err)
				} else {
					// Update tracking state for next change
					s.lastAppliedPort = s.lastConfig.ServerPort
					s.lastAppliedIP = s.lastConfig.ListenIP
				}
			}
		} else if len(s.lastUsers) == 0 {
			slog.Warn("no users, stopping kernel")
			s.kernel.Stop()
		}
	} else if usersChanged && s.kernel.IsRunning() {
		// Hot-reload for user-only changes
		if err := s.kernel.Reload(s.lastConfig, s.lastUsers, certFile, keyFile); err != nil {
			slog.Warn("inbound reload failed, falling back to full restart", "error", err)
			if err := s.kernel.ApplyConfig(s.lastConfig, s.lastUsers, certFile, keyFile); err != nil {
				slog.Error("failed to apply config (fallback)", "error", err)
			} else {
				s.lastAppliedPort = s.lastConfig.ServerPort
				s.lastAppliedIP = s.lastConfig.ListenIP
			}
		}
	} else if usersChanged && !s.kernel.IsRunning() && len(s.lastUsers) > 0 {
		// Kernel was stopped (no users), now we have users — start it
		if err := s.kernel.ApplyConfig(s.lastConfig, s.lastUsers, certFile, keyFile); err != nil {
			slog.Error("failed to start kernel with new users", "error", err)
		}
	}
}

func (s *Service) trackAndEnforce(ctx context.Context) {
	if !s.kernel.IsRunning() {
		return
	}

	conns, err := s.kernel.GetConnections(ctx)
	if err != nil {
		slog.Debug("get connections failed", "error", err)
		return
	}

	s.tracker.Process(conns)
	s.tracker.LogStats()

	kicks := s.limiter.Check(conns)
	s.executeKicks(ctx, kicks)
}

func (s *Service) executeKicks(ctx context.Context, kicks []limiter.KickAction) {
	for _, kick := range kicks {
		slog.Info("kicking connection",
			"user_id", kick.UserID,
			"conn_id", kick.ConnID,
			"reason", kick.Reason,
		)
		if err := s.kernel.CloseConnection(ctx, kick.ConnID); err != nil {
			slog.Error("failed to close connection", "conn_id", kick.ConnID, "error", err)
		}
	}
}

// pushReport sends consolidated traffic + alive + status to the panel
func (s *Service) pushReport() {
	if s.pushBackoff.shouldSkip() {
		slog.Debug("skipping report due to backoff")
		return
	}

	traffic := s.tracker.FlushTraffic()
	aliveIPs := s.tracker.FlushAliveIPs()
	online := s.tracker.CurrentOnline()
	status := monitor.Collect()

	metrics := s.buildMetrics(status)

	if len(traffic) == 0 && len(aliveIPs) == 0 && len(online) == 0 {
		// Still send status periodically
		if err := s.panel.Report(
			nil, nil, nil,
			status.CPU,
			[2]uint64{status.MemTotal, status.MemUsed},
			[2]uint64{status.SwapTotal, status.SwapUsed},
			[2]uint64{status.DiskTotal, status.DiskUsed},
			metrics,
		); err != nil {
			slog.Error("failed to push status report", "error", err)
			s.pushBackoff.onFailure()
			return
		}
		s.pushBackoff.onSuccess()
		return
	}

	if err := s.panel.Report(
		traffic, aliveIPs, online,
		status.CPU,
		[2]uint64{status.MemTotal, status.MemUsed},
		[2]uint64{status.SwapTotal, status.SwapUsed},
		[2]uint64{status.DiskTotal, status.DiskUsed},
		metrics,
	); err != nil {
		slog.Error("failed to push report", "error", err)
		s.tracker.RestoreTraffic(traffic)
		s.tracker.RestoreAliveIPs(aliveIPs)
		s.pushBackoff.onFailure()
		return
	}

	s.pushBackoff.onSuccess()
	slog.Info("report pushed", "users", len(traffic))
}

// buildMetrics aggregates node-level metrics to be reported to the panel.
// This includes active connections, per-core CPU, GC stats, API call stats,
// WebSocket status, and limiter hit counts.
func (s *Service) buildMetrics(status monitor.Status) map[string]interface{} {
	m := make(map[string]interface{})

	m["uptime"] = status.Uptime

	// Active connections (last measured during tracker.Process()).
	m["active_connections"] = s.tracker.ActiveConnections()
	m["total_connections"] = s.tracker.TotalConnections()

	// Speed
	m["inbound_speed"] = s.tracker.InboundSpeed()
	m["outbound_speed"] = s.tracker.OutboundSpeed()

	// Per-core CPU usage (if available).
	if len(status.CPUPerCore) > 0 {
		m["cpu_per_core"] = status.CPUPerCore
	}

	// Speed Limiter metrics
	m["speed_limiter"] = map[string]interface{}{
		"has_limits":    s.speedTracker.HasLimits(),
		"limited_users": s.speedTracker.LimitedUserCount(),
	}

	// GC metrics.
	m["gc"] = map[string]interface{}{
		"num_gc":        status.NumGC,
		"last_pause_ms": status.LastPauseMS,
	}

	// API metrics.
	api := s.panel.SnapshotMetrics()
	m["api"] = map[string]interface{}{
		"success": api.Success,
		"failure": api.Failure,
	}

	// WebSocket status.
	wsEnabled := s.wsClient != nil
	wsConnected := wsEnabled && s.wsClient.IsConnected()
	m["ws"] = map[string]interface{}{
		"enabled":   wsEnabled,
		"connected": wsConnected,
	}

	// Limiter metrics.
	lm := s.limiter.SnapshotMetrics()
	m["limits"] = map[string]interface{}{
		"device_limit_events": lm.DeviceLimitEvents,
		"speed_limited_users": s.speedTracker.LimitedUserCount(),
	}

	return m
}

// computeConfigHash returns a SHA-256 hash of the kernel-relevant node config
// fields. BaseConfig (PushInterval/PullInterval) is excluded because it only
// affects service-layer polling intervals and must not trigger a kernel restart.
func computeConfigHash(cfg *panel.NodeConfig) string {
	if cfg == nil {
		return ""
	}
	tmp := *cfg
	tmp.BaseConfig = panel.BaseConfig{}
	data, err := json.Marshal(&tmp)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

// computeUserHash returns a deterministic hash of the user list for change detection.
// Uses direct byte encoding instead of binary.Write to avoid reflection overhead.
func computeUserHash(users []panel.User) string {
	sorted := make([]panel.User, len(users))
	copy(sorted, users)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	h := sha256.New()
	var buf [8]byte
	for _, u := range sorted {
		buf[0] = byte(u.ID)
		buf[1] = byte(u.ID >> 8)
		buf[2] = byte(u.ID >> 16)
		buf[3] = byte(u.ID >> 24)
		buf[4] = byte(u.ID >> 32)
		buf[5] = byte(u.ID >> 40)
		buf[6] = byte(u.ID >> 48)
		buf[7] = byte(u.ID >> 56)
		h.Write(buf[:])
		h.Write([]byte(u.UUID))
		buf[0] = byte(u.SpeedLimit)
		buf[1] = byte(u.SpeedLimit >> 8)
		buf[2] = byte(u.SpeedLimit >> 16)
		buf[3] = byte(u.SpeedLimit >> 24)
		buf[4] = byte(u.SpeedLimit >> 32)
		buf[5] = byte(u.SpeedLimit >> 40)
		buf[6] = byte(u.SpeedLimit >> 48)
		buf[7] = byte(u.SpeedLimit >> 56)
		h.Write(buf[:])
		buf[0] = byte(u.DeviceLimit)
		buf[1] = byte(u.DeviceLimit >> 8)
		buf[2] = byte(u.DeviceLimit >> 16)
		buf[3] = byte(u.DeviceLimit >> 24)
		buf[4] = byte(u.DeviceLimit >> 32)
		buf[5] = byte(u.DeviceLimit >> 40)
		buf[6] = byte(u.DeviceLimit >> 48)
		buf[7] = byte(u.DeviceLimit >> 56)
		h.Write(buf[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
