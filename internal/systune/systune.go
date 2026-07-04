// Package systune applies idempotent kernel network tuning at process startup:
// BBR congestion control (with fq qdisc) and enlarged TCP/UDP buffers. Living in
// the node binary — rather than install.sh — means already-deployed nodes pick
// up the tuning automatically after a panel-issued self-update, without anyone
// re-running the installer or SSH-ing into the machine.
//
// The tuning is written to /etc/sysctl.d/ (so it survives reboots) and applied
// immediately through /proc/sys. It only runs on Linux, as root, outside
// containers, and can be disabled with `runtime: sysctl_tune: false`.
package systune

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

// ConfPath is where the persistent tuning profile is written. Exported so
// xbctl uninstall can offer to remove it.
const ConfPath = "/etc/sysctl.d/99-xboard-node.conf"

// Overridable seams for tests (and for the cross-platform build: the package
// must compile everywhere, it just no-ops outside Linux).
var (
	confPath      = ConfPath
	procSysDir    = "/proc/sys"
	dockerEnvPath = "/.dockerenv"
	containerEnv  = "/run/.containerenv" // podman
	cgroupPath    = "/proc/1/cgroup"
	goosFn        = func() string { return runtime.GOOS }
	euidFn        = os.Geteuid
	modprobeBBR   = func() error { return exec.Command("modprobe", "tcp_bbr").Run() }
)

type setting struct {
	key   string // dotted sysctl name, e.g. "net.core.rmem_max"
	value string
}

// baseSettings are applied unconditionally. The 16MB socket buffer ceilings
// matter for QUIC-based protocols (Hysteria2/TUIC) and for TCP on high
// bandwidth-delay-product links; tcp_fastopen=3 enables TFO both ways.
var baseSettings = []setting{
	{"net.core.rmem_max", "16777216"},
	{"net.core.wmem_max", "16777216"},
	{"net.ipv4.tcp_rmem", "4096 87380 16777216"},
	{"net.ipv4.tcp_wmem", "4096 65536 16777216"},
	{"net.ipv4.tcp_fastopen", "3"},
}

// bbrSettings are added only when the kernel supports the bbr congestion
// control (fq pairs with bbr as the recommended qdisc).
var bbrSettings = []setting{
	{"net.core.default_qdisc", "fq"},
	{"net.ipv4.tcp_congestion_control", "bbr"},
}

// Apply performs the tuning. enabled comes from config (runtime.sysctl_tune,
// default true). Every failure is a warning at most — tuning must never block
// the node from starting.
func Apply(enabled bool) {
	if !enabled {
		nlog.Core().Info("systune: disabled by config (runtime.sysctl_tune=false)")
		return
	}
	if goosFn() != "linux" {
		return
	}
	if euidFn() != 0 {
		nlog.Core().Info("systune: not running as root, skipping kernel tuning")
		return
	}
	if inContainer() {
		nlog.Core().Info("systune: container environment detected, skipping kernel tuning (tune the host instead)")
		return
	}

	settings := make([]setting, 0, len(bbrSettings)+len(baseSettings))
	if bbrAvailable() {
		settings = append(settings, bbrSettings...)
	} else {
		nlog.Core().Warn("systune: kernel lacks bbr congestion control, skipping bbr/fq (buffers still applied)")
	}
	settings = append(settings, baseSettings...)

	writeConf(settings)
	applyRuntime(settings)
}

// writeConf persists the profile to sysctl.d so it survives reboots. The file
// is only rewritten when its content differs from the desired profile.
func writeConf(settings []setting) {
	var b strings.Builder
	b.WriteString("# Managed by xboard-node (systune). Delete this file to revert; set\n")
	b.WriteString("# `runtime: sysctl_tune: false` in config.yml to stop it being recreated.\n")
	for _, s := range settings {
		fmt.Fprintf(&b, "%s = %s\n", s.key, s.value)
	}
	desired := b.String()

	if cur, err := os.ReadFile(confPath); err == nil && string(cur) == desired {
		return
	}
	if err := os.WriteFile(confPath, []byte(desired), 0o644); err != nil {
		nlog.Core().Warn("systune: cannot write sysctl profile", "path", confPath, "error", err)
		return
	}
	nlog.Core().Info("systune: wrote kernel tuning profile", "path", confPath)
}

// applyRuntime pushes each value into the live kernel via /proc/sys. Values
// that already match are counted as applied; individual failures only warn.
func applyRuntime(settings []setting) {
	applied := 0
	for _, s := range settings {
		path := filepath.Join(procSysDir, strings.ReplaceAll(s.key, ".", "/"))
		if cur, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(cur)) == s.value {
			applied++
			continue
		}
		if err := os.WriteFile(path, []byte(s.value), 0o644); err != nil {
			nlog.Core().Warn("systune: cannot apply sysctl", "key", s.key, "error", err)
			continue
		}
		applied++
	}
	nlog.Core().Info("systune: kernel network tuning active", "applied", applied, "total", len(settings))
}

// bbrAvailable reports whether the running kernel offers bbr, loading the
// tcp_bbr module on demand when it is not already listed.
func bbrAvailable() bool {
	availPath := filepath.Join(procSysDir, "net/ipv4/tcp_available_congestion_control")
	if data, err := os.ReadFile(availPath); err == nil && strings.Contains(string(data), "bbr") {
		return true
	}
	if err := modprobeBBR(); err != nil {
		return false
	}
	data, err := os.ReadFile(availPath)
	return err == nil && strings.Contains(string(data), "bbr")
}

// inContainer detects docker/podman/k8s/lxc environments, where /proc/sys is
// namespaced or read-only and tuning belongs to the host.
func inContainer() bool {
	if _, err := os.Stat(dockerEnvPath); err == nil {
		return true
	}
	if _, err := os.Stat(containerEnv); err == nil {
		return true
	}
	data, err := os.ReadFile(cgroupPath)
	if err != nil {
		return false
	}
	s := string(data)
	return strings.Contains(s, "docker") || strings.Contains(s, "kubepods") || strings.Contains(s, "lxc")
}
