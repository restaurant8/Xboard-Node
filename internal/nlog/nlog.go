package nlog

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// ANSI color codes
const (
	ColorReset  = "\033[0m"
	ColorGray   = "\033[90m" // 时间戳
	ColorWhite  = "\033[37m" // INFO
	ColorYellow = "\033[33m" // WARN
	ColorRed    = "\033[31m" // ERROR
	ColorCyan   = "\033[36m" // 节点前缀
	ColorGreen  = "\033[32m" // 调试信息
	ColorBlue   = "\033[34m" // core 前缀
)

// NodeLog provides structured logging with node context.
// Format: LEVEL [protocol:port] message
type NodeLog struct {
	prefix string // e.g., "shadowsocks:10005" or "trojan:10033"
}

// Global logger state
var (
	mu          sync.RWMutex
	defaultNode string
	nodeLoggers = make(map[string]*NodeLog)
)

// SetDefault sets the default node prefix for core-level logs.
func SetDefault(prefix string) {
	mu.Lock()
	defaultNode = prefix
	mu.Unlock()
}

// ForNode returns a NodeLog for the given protocol and port.
func ForNode(protocol string, port int) *NodeLog {
	key := fmt.Sprintf("%s:%d", normalizeProto(protocol), port)
	mu.RLock()
	if nl, ok := nodeLoggers[key]; ok {
		mu.RUnlock()
		return nl
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()
	// Double check
	if nl, ok := nodeLoggers[key]; ok {
		return nl
	}
	nl := &NodeLog{prefix: key}
	nodeLoggers[key] = nl
	return nl
}

// Core returns a NodeLog for core/system messages.
func Core() *NodeLog {
	mu.RLock()
	if nl, ok := nodeLoggers["core"]; ok {
		mu.RUnlock()
		return nl
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()
	if nl, ok := nodeLoggers["core"]; ok {
		return nl
	}
	nl := &NodeLog{prefix: "core"}
	nodeLoggers["core"] = nl
	return nl
}

// normalizeProto returns the full protocol name for display.
func normalizeProto(p string) string {
	switch strings.ToLower(p) {
	case "shadowsocks", "ss":
		return "shadowsocks"
	case "vmess":
		return "vmess"
	case "vless":
		return "vless"
	case "trojan":
		return "trojan"
	case "hysteria2", "hysteria":
		return "hysteria2"
	case "tuic":
		return "tuic"
	case "anytls":
		return "anytls"
	case "naive":
		return "naive"
	case "http":
		return "http"
	case "socks":
		return "socks"
	default:
		return p
	}
}

// ─── Logging Methods ────────────────────────────────────────────────────────

func (nl *NodeLog) Debug(msg string, args ...any) {
	logWithColor(slog.LevelDebug, nl.prefix, msg, args...)
}

func (nl *NodeLog) Info(msg string, args ...any) {
	logWithColor(slog.LevelInfo, nl.prefix, msg, args...)
}

func (nl *NodeLog) Warn(msg string, args ...any) {
	logWithColor(slog.LevelWarn, nl.prefix, msg, args...)
}

func (nl *NodeLog) Error(msg string, args ...any) {
	logWithColor(slog.LevelError, nl.prefix, msg, args...)
}

// logWithColor outputs a colored log line directly to stdout.
// Format: HH:MM:SS.mmm LEVEL [prefix] message
func logWithColor(level slog.Level, prefix, msg string, args ...any) {
	// Time
	now := time.Now().Format("15:04:05.000")

	// Level with color
	var levelStr, levelColor string
	switch level {
	case slog.LevelDebug:
		levelStr = "DEBUG"
		levelColor = ColorGreen
	case slog.LevelInfo:
		levelStr = "INFO "
		levelColor = ColorWhite
	case slog.LevelWarn:
		levelStr = "WARN "
		levelColor = ColorYellow
	case slog.LevelError:
		levelStr = "ERROR"
		levelColor = ColorRed
	default:
		levelStr = "?????"
		levelColor = ColorWhite
	}

	// Prefix with color
	prefixColor := ColorCyan
	if prefix == "core" {
		prefixColor = ColorBlue
	}

	// Format message
	var fullMsg string
	if len(args) == 0 {
		fullMsg = msg
	} else {
		fullMsg = fmt.Sprintf("%s %v", msg, args)
	}

	// Output colored line
	fmt.Fprintf(os.Stdout, "%s%s%s %s%s%s %s[%s]%s %s\n",
		ColorGray, now, ColorReset,
		levelColor, levelStr, ColorReset,
		prefixColor, prefix, ColorReset,
		fullMsg,
	)
}

// ─── Startup Summary ────────────────────────────────────────────────────────

// StartupSummary logs a condensed startup summary.
type StartupSummary struct {
	mu    sync.Mutex
	nodes []nodeInfo
}

type nodeInfo struct {
	Protocol string
	Port     int
	Users    int
}

func NewStartupSummary() *StartupSummary {
	return &StartupSummary{}
}

func (s *StartupSummary) AddNode(protocol string, port, users int) {
	s.mu.Lock()
	s.nodes = append(s.nodes, nodeInfo{protocol, port, users})
	s.mu.Unlock()
}

func (s *StartupSummary) Print() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.nodes) == 0 {
		Core().Warn("no nodes configured")
		return
	}

	// Group by protocol
	byProto := make(map[string][]nodeInfo)
	for _, n := range s.nodes {
		proto := normalizeProto(n.Protocol)
		byProto[proto] = append(byProto[proto], n)
	}

	// Build summary line
	parts := make([]string, 0, len(byProto))
	for proto, nodes := range byProto {
		ports := make([]string, 0, len(nodes))
		for _, n := range nodes {
			ports = append(ports, fmt.Sprintf("%d", n.Port))
		}
		parts = append(parts, fmt.Sprintf("%s:%s", proto, strings.Join(ports, ",")))
	}

	Core().Info(fmt.Sprintf("started %d nodes: %s", len(s.nodes), strings.Join(parts, " | ")))
}

// ─── Helpers ────────────────────────────────────────────────────────────────

// ConfigUpdated logs a config update event.
func ConfigUpdated(nl *NodeLog, users int) {
	nl.Info(fmt.Sprintf("config updated, %d users", users))
}

// HotReload logs a hot-reload event (no restart).
func HotReload(nl *NodeLog, what string) {
	nl.Debug(fmt.Sprintf("hot-reload: %s", what))
}

// FullRestart logs a full kernel restart.
func FullRestart(nl *NodeLog, reason string) {
	nl.Info(fmt.Sprintf("kernel restart: %s", reason))
}

// ReportPushed logs a report push event.
func ReportPushed(users, online int) {
	Core().Info(fmt.Sprintf("report pushed: %d users, %d online", users, online))
}

// TrackerStats logs tracker statistics.
func TrackerStats(conns, users int) {
	Core().Debug(fmt.Sprintf("tracker: %d conns, %d users online", conns, users))
}

// Context-aware logging

type ctxKey struct{}

func WithNode(ctx context.Context, nl *NodeLog) context.Context {
	return context.WithValue(ctx, ctxKey{}, nl)
}

func FromContext(ctx context.Context) *NodeLog {
	if nl, ok := ctx.Value(ctxKey{}).(*NodeLog); ok {
		return nl
	}
	return Core()
}
