// Package selfupdate performs an in-place upgrade of the running xboard-node
// (and its xbctl companion) when the panel pushes a node.upgrade command over
// WebSocket.
//
// Mechanics: download the release artifacts for the host architecture, verify
// them (SHA256SUMS when available, plus a `-v` smoke test), atomically swap the
// binaries, then trigger a graceful shutdown. The systemd unit ships with
// Restart=always/RestartSec=5, so the service is relaunched from the freshly
// swapped binary. We deliberately do NOT call `systemctl restart` from inside
// the service: systemd would SIGTERM us mid-call, so the restart could never be
// observed (and rollback could not be driven). Letting the process exit and
// systemd relaunch is both simpler and race-free.
//
// A process-wide guard ensures only one upgrade runs at a time even when a
// single process hosts several node services that each receive the command.
package selfupdate

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cedar2025/xboard-node/internal/buildinfo"
	"github.com/cedar2025/xboard-node/internal/nlog"
)

const (
	// DefaultDownloadBase is the GitHub releases base used when the panel does
	// not supply an override (e.g. a self-hosted mirror for intranet nodes).
	DefaultDownloadBase = "https://github.com/restaurant8/Xboard-Node/releases"

	// WS event names used to report progress back to the panel.
	EventAck     = "upgrade.ack"
	EventResult  = "upgrade.result"
	EventRestart = "restart.result"
)

// Install paths — vars (not consts) so tests can redirect them to a temp dir.
var (
	binaryPath = "/usr/local/bin/xboard-node"
	cliPath    = "/usr/local/bin/xbctl"
)

// Sender is the subset of the WS client used to report upgrade progress.
type Sender interface {
	SendRaw(event string, data json.RawMessage)
}

// Command describes an upgrade request received from the panel.
type Command struct {
	Version      string `json:"version"`       // "latest" or a release tag; empty => latest
	DownloadBase string `json:"download_base"` // optional override; empty => DefaultDownloadBase
	RequestID    string `json:"request_id"`
}

// statusPayload is the JSON sent back to the panel for ack/result events.
type statusPayload struct {
	RequestID   string `json:"request_id,omitempty"`
	Status      string `json:"status"` // started | success | failed
	FromVersion string `json:"from_version,omitempty"`
	ToVersion   string `json:"to_version,omitempty"`
	Error       string `json:"error,omitempty"`
}

// ErrAlreadyUpToDate is returned by run when the resolved target version equals
// the currently running version, so the upgrade is skipped.
var ErrAlreadyUpToDate = errors.New("already up to date")

// inProgress guards against concurrent upgrades within a single process.
var inProgress atomic.Bool

// exitFn triggers a graceful shutdown so systemd relaunches the new binary.
// Overridable in tests. Uses os.Process.Signal (not syscall.Kill) so the
// package stays cross-platform compilable; the node only ever runs on Linux.
var exitFn = func() {
	if p, err := os.FindProcess(os.Getpid()); err == nil {
		_ = p.Signal(syscall.SIGTERM)
	}
}

// httpClient is used for downloads; overridable in tests.
var httpClient = &http.Client{Timeout: 120 * time.Second}

// Apply runs the full upgrade flow for a panel-issued command, reporting
// progress through sender. It is safe to call from multiple goroutines: only
// the first concurrent call proceeds; the rest are ignored.
//
// On success the process is terminated (systemd relaunches it); Apply does not
// return in that case. On failure it reports the error and returns.
func Apply(sender Sender, cmd Command) {
	if !inProgress.CompareAndSwap(false, true) {
		nlog.Core().Warn("upgrade already in progress, ignoring duplicate command", "request_id", cmd.RequestID)
		return
	}

	from := buildinfo.Version
	sendStatus(sender, statusPayload{RequestID: cmd.RequestID, Status: "started", FromVersion: from}, EventAck)

	newVersion, err := run(cmd)
	if errors.Is(err, ErrAlreadyUpToDate) {
		// Node is already on the target version — nothing to do.
		inProgress.Store(false)
		nlog.Core().Info("self-upgrade skipped, already up to date", "version", from, "request_id", cmd.RequestID)
		sendStatus(sender, statusPayload{RequestID: cmd.RequestID, Status: "skipped", FromVersion: from, ToVersion: from}, EventResult)
		return
	}
	if err != nil {
		// Reset the guard so the panel can retry after fixing the cause.
		inProgress.Store(false)
		nlog.Core().Error("self-upgrade failed", "error", err, "request_id", cmd.RequestID)
		sendStatus(sender, statusPayload{RequestID: cmd.RequestID, Status: "failed", FromVersion: from, Error: err.Error()}, EventResult)
		return
	}

	nlog.Core().Info("self-upgrade succeeded, restarting", "from", from, "to", newVersion, "request_id", cmd.RequestID)
	sendStatus(sender, statusPayload{RequestID: cmd.RequestID, Status: "success", FromVersion: from, ToVersion: newVersion}, EventResult)

	// Give the WS write loop a moment to flush the success message before the
	// process exits; the panel confirms the upgrade on reconnect via version.
	time.Sleep(1500 * time.Millisecond)
	exitFn()
}

// Restart triggers a graceful process restart (systemd relaunches the service
// via Restart=always/RestartSec=5). It reports a "restarting" status to the
// panel before exiting. Shares the inProgress guard with Apply so a restart and
// an upgrade can't run at the same time.
func Restart(sender Sender, requestID string) {
	if !inProgress.CompareAndSwap(false, true) {
		nlog.Core().Warn("upgrade/restart already in progress, ignoring restart command", "request_id", requestID)
		return
	}
	nlog.Core().Info("node.restart received, restarting process", "request_id", requestID)
	sendStatus(sender, statusPayload{RequestID: requestID, Status: "restarting", FromVersion: buildinfo.Version}, EventRestart)
	// Give the WS write loop a moment to flush before we exit.
	time.Sleep(1500 * time.Millisecond)
	exitFn()
}

func sendStatus(sender Sender, p statusPayload, event string) {
	if sender == nil {
		return
	}
	data, err := json.Marshal(p)
	if err != nil {
		return
	}
	sender.SendRaw(event, data)
}

// run downloads, verifies and swaps the binaries. It returns the new version
// string reported by the freshly installed binary.
func run(cmd Command) (string, error) {
	arch := runtime.GOARCH
	if arch != "amd64" && arch != "arm64" {
		return "", fmt.Errorf("unsupported architecture: %s", arch)
	}

	base := strings.TrimRight(cmd.DownloadBase, "/")
	if base == "" {
		base = DefaultDownloadBase
	}
	version := cmd.Version
	if version == "" {
		version = "latest"
	}

	// Version comparison: if we can positively resolve the concrete target
	// version (a fixed tag, or "latest" resolved via the releases redirect) and
	// it matches the running version, skip the upgrade entirely. When the latest
	// version cannot be resolved (network/offline), we proceed rather than skip.
	if target := resolveTargetVersion(base, version); target != "" && sameVersion(target, buildinfo.Version) {
		return buildinfo.Version, ErrAlreadyUpToDate
	}

	binaryArtifact := fmt.Sprintf("xboard-node-linux-%s", arch)
	cliArtifact := fmt.Sprintf("xbctl-linux-%s", arch)

	newBinary := filepath.Join(filepath.Dir(binaryPath), ".xboard-node.new")
	newCLI := filepath.Join(filepath.Dir(cliPath), ".xbctl.new")

	if err := download(downloadURL(base, version, binaryArtifact), newBinary); err != nil {
		return "", fmt.Errorf("download binary: %w", err)
	}
	if err := download(downloadURL(base, version, cliArtifact), newCLI); err != nil {
		os.Remove(newBinary)
		return "", fmt.Errorf("download xbctl: %w", err)
	}

	// Verify checksums when the release publishes a SHA256SUMS manifest. Older
	// releases without it fall back to the `-v` smoke test below.
	if sums, ok := fetchChecksums(downloadURL(base, version, "SHA256SUMS")); ok {
		if err := verifyChecksum(newBinary, binaryArtifact, sums); err != nil {
			return "", cleanup(newBinary, newCLI, err)
		}
		if err := verifyChecksum(newCLI, cliArtifact, sums); err != nil {
			return "", cleanup(newBinary, newCLI, err)
		}
	} else {
		nlog.Core().Warn("SHA256SUMS unavailable for release, skipping checksum verification", "version", version)
	}

	if err := os.Chmod(newBinary, 0o755); err != nil {
		return "", cleanup(newBinary, newCLI, fmt.Errorf("chmod binary: %w", err))
	}
	if err := os.Chmod(newCLI, 0o755); err != nil {
		return "", cleanup(newBinary, newCLI, fmt.Errorf("chmod xbctl: %w", err))
	}

	// Smoke test: the new binaries must at least run and report a version.
	if out, err := exec.Command(newBinary, "-v").CombinedOutput(); err != nil {
		return "", cleanup(newBinary, newCLI, fmt.Errorf("binary version check failed: %s", strings.TrimSpace(string(out))))
	}
	if out, err := exec.Command(newCLI, "version").CombinedOutput(); err != nil {
		return "", cleanup(newBinary, newCLI, fmt.Errorf("xbctl version check failed: %s", strings.TrimSpace(string(out))))
	}

	// Back up the current binaries so an operator can roll back manually if the
	// relaunched process misbehaves (it passed the smoke test, so this is rare).
	backupBinary := binaryPath + ".bak"
	backupCLI := cliPath + ".bak"
	if fileExists(binaryPath) {
		if err := copyFile(binaryPath, backupBinary); err != nil {
			return "", cleanup(newBinary, newCLI, fmt.Errorf("backup binary: %w", err))
		}
	}
	if fileExists(cliPath) {
		if err := copyFile(cliPath, backupCLI); err != nil {
			return "", cleanup(newBinary, newCLI, fmt.Errorf("backup xbctl: %w", err))
		}
	}

	// Atomic swap. Replacing a running binary via rename is safe on Linux: the
	// running process keeps the old inode until it exits.
	if err := os.Rename(newBinary, binaryPath); err != nil {
		return "", cleanup(newBinary, newCLI, fmt.Errorf("replace binary: %w", err))
	}
	if err := os.Rename(newCLI, cliPath); err != nil {
		if fileExists(backupBinary) {
			os.Rename(backupBinary, binaryPath)
		}
		os.Remove(newCLI)
		return "", fmt.Errorf("replace xbctl: %w", err)
	}

	// Recreate the /usr/bin/xbctl convenience symlink (best effort).
	os.Remove("/usr/bin/xbctl")
	os.Symlink(cliPath, "/usr/bin/xbctl")

	newVersion := "unknown"
	if out, err := exec.Command(binaryPath, "-v").CombinedOutput(); err == nil {
		newVersion = strings.TrimSpace(string(out))
	}
	return newVersion, nil
}

// resolveTargetVersion returns the concrete release tag the command targets.
// A fixed version is returned as-is. "latest" (or empty) is resolved by reading
// the Location header of the releases "/latest" redirect, e.g.
// .../releases/latest -> .../releases/tag/v1.2.3. Returns "" if it cannot be
// determined (caller then proceeds with the upgrade instead of skipping).
func resolveTargetVersion(base, version string) string {
	if version != "" && version != "latest" {
		return version
	}

	req, err := http.NewRequest(http.MethodGet, base+"/latest", nil)
	if err != nil {
		return ""
	}
	client := &http.Client{
		Timeout: 30 * time.Second,
		// Capture the redirect rather than following it.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	loc := resp.Header.Get("Location")
	if loc == "" {
		return ""
	}
	// Take the final path segment, e.g. ".../tag/v1.2.3" -> "v1.2.3".
	tag := path.Base(loc)
	if tag == "/" || tag == "." || tag == "latest" {
		return ""
	}
	return tag
}

// sameVersion compares two version strings, tolerating a leading "v" and
// surrounding whitespace. "dev"/empty never matches a real tag.
func sameVersion(a, b string) bool {
	na := strings.TrimPrefix(strings.TrimSpace(a), "v")
	nb := strings.TrimPrefix(strings.TrimSpace(b), "v")
	if na == "" || nb == "" || na == "dev" || nb == "dev" {
		return false
	}
	return na == nb
}

func downloadURL(base, version, artifact string) string {
	if version == "latest" {
		return base + "/latest/download/" + artifact
	}
	return base + "/download/" + version + "/" + artifact
}

func download(url, dest string) error {
	resp, err := httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

// fetchChecksums downloads and parses a SHA256SUMS manifest (lines of
// "<hex>  <filename>"). Returns ok=false when the manifest is unavailable.
func fetchChecksums(url string) (map[string]string, bool) {
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	sums := make(map[string]string)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 {
			sums[filepath.Base(fields[len(fields)-1])] = strings.ToLower(fields[0])
		}
	}
	if len(sums) == 0 {
		return nil, false
	}
	return sums, true
}

func verifyChecksum(path, artifact string, sums map[string]string) error {
	want, ok := sums[artifact]
	if !ok {
		// Manifest exists but lacks this artifact — treat as a hard failure to
		// avoid installing an unverified binary.
		return fmt.Errorf("checksum for %s missing from SHA256SUMS", artifact)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != want {
		return fmt.Errorf("checksum mismatch for %s: got %s want %s", artifact, got, want)
	}
	return nil
}

func cleanup(a, b string, err error) error {
	os.Remove(a)
	os.Remove(b)
	return err
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o755)
}
