package systune

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// setupFakeSystem redirects every seam to a temp directory that mimics a Linux
// host: a /proc/sys tree with writable files and no container markers.
// availCC is the content of tcp_available_congestion_control.
func setupFakeSystem(t *testing.T, availCC string) (procDir, conf string) {
	t.Helper()
	root := t.TempDir()

	procDir = filepath.Join(root, "proc", "sys")
	conf = filepath.Join(root, "etc", "99-xboard-node.conf")
	if err := os.MkdirAll(filepath.Dir(conf), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create every /proc/sys file the settings touch, plus the availability file.
	keys := make([]setting, 0, len(baseSettings)+len(bbrSettings))
	keys = append(keys, baseSettings...)
	keys = append(keys, bbrSettings...)
	for _, s := range keys {
		p := filepath.Join(procDir, strings.ReplaceAll(s.key, ".", "/"))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("0\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	availPath := filepath.Join(procDir, "net/ipv4/tcp_available_congestion_control")
	if err := os.WriteFile(availPath, []byte(availCC+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	origConf, origProc := confPath, procSysDir
	origDocker, origPodman, origCgroup := dockerEnvPath, containerEnv, cgroupPath
	origGoos, origEuid, origModprobe := goosFn, euidFn, modprobeBBR
	confPath = conf
	procSysDir = procDir
	dockerEnvPath = filepath.Join(root, "nonexistent-dockerenv")
	containerEnv = filepath.Join(root, "nonexistent-containerenv")
	cgroupPath = filepath.Join(root, "nonexistent-cgroup")
	goosFn = func() string { return "linux" }
	euidFn = func() int { return 0 }
	modprobeBBR = func() error { return os.ErrNotExist }
	t.Cleanup(func() {
		confPath, procSysDir = origConf, origProc
		dockerEnvPath, containerEnv, cgroupPath = origDocker, origPodman, origCgroup
		goosFn, euidFn, modprobeBBR = origGoos, origEuid, origModprobe
	})
	return procDir, conf
}

func readProc(t *testing.T, procDir, key string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(procDir, strings.ReplaceAll(key, ".", "/")))
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return strings.TrimSpace(string(data))
}

func TestApplyWithBBR(t *testing.T) {
	procDir, conf := setupFakeSystem(t, "reno cubic bbr")

	Apply(true)

	data, err := os.ReadFile(conf)
	if err != nil {
		t.Fatalf("profile not written: %v", err)
	}
	for _, want := range []string{"net.ipv4.tcp_congestion_control = bbr", "net.core.default_qdisc = fq", "net.core.rmem_max = 16777216"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("profile missing %q:\n%s", want, data)
		}
	}
	if got := readProc(t, procDir, "net.ipv4.tcp_congestion_control"); got != "bbr" {
		t.Errorf("tcp_congestion_control = %q, want bbr", got)
	}
	if got := readProc(t, procDir, "net.core.rmem_max"); got != "16777216" {
		t.Errorf("rmem_max = %q, want 16777216", got)
	}
}

func TestApplyWithoutBBRSkipsCongestionControl(t *testing.T) {
	procDir, conf := setupFakeSystem(t, "reno cubic")

	Apply(true)

	data, err := os.ReadFile(conf)
	if err != nil {
		t.Fatalf("profile not written: %v", err)
	}
	if strings.Contains(string(data), "bbr") {
		t.Errorf("profile should not mention bbr when unavailable:\n%s", data)
	}
	if !strings.Contains(string(data), "net.core.rmem_max = 16777216") {
		t.Errorf("buffer settings must still be applied:\n%s", data)
	}
	// The live congestion control value must be untouched.
	if got := readProc(t, procDir, "net.ipv4.tcp_congestion_control"); got != "0" {
		t.Errorf("tcp_congestion_control = %q, want untouched (0)", got)
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	_, conf := setupFakeSystem(t, "bbr")

	Apply(true)
	first, err := os.Stat(conf)
	if err != nil {
		t.Fatal(err)
	}
	// Make a rewrite detectable regardless of mtime resolution.
	if err := os.Chtimes(conf, first.ModTime(), first.ModTime()); err != nil {
		t.Fatal(err)
	}

	Apply(true)
	second, err := os.Stat(conf)
	if err != nil {
		t.Fatal(err)
	}
	if !second.ModTime().Equal(first.ModTime()) {
		t.Error("profile was rewritten although content is unchanged")
	}
}

func TestApplyDisabled(t *testing.T) {
	_, conf := setupFakeSystem(t, "bbr")

	Apply(false)

	if _, err := os.Stat(conf); !os.IsNotExist(err) {
		t.Error("profile must not be written when disabled")
	}
}

func TestApplySkipsInContainer(t *testing.T) {
	root := t.TempDir()
	_, conf := setupFakeSystem(t, "bbr")

	marker := filepath.Join(root, ".dockerenv")
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	orig := dockerEnvPath
	dockerEnvPath = marker
	defer func() { dockerEnvPath = orig }()

	Apply(true)

	if _, err := os.Stat(conf); !os.IsNotExist(err) {
		t.Error("profile must not be written inside a container")
	}
}

func TestApplySkipsNonRoot(t *testing.T) {
	_, conf := setupFakeSystem(t, "bbr")
	euidFn = func() int { return 1000 }

	Apply(true)

	if _, err := os.Stat(conf); !os.IsNotExist(err) {
		t.Error("profile must not be written without root")
	}
}

func TestBBRLoadedViaModprobe(t *testing.T) {
	procDir, conf := setupFakeSystem(t, "reno cubic")

	// modprobe succeeds and makes bbr appear in the availability list.
	modprobeBBR = func() error {
		availPath := filepath.Join(procDir, "net/ipv4/tcp_available_congestion_control")
		return os.WriteFile(availPath, []byte("reno cubic bbr\n"), 0o644)
	}

	Apply(true)

	data, err := os.ReadFile(conf)
	if err != nil {
		t.Fatalf("profile not written: %v", err)
	}
	if !strings.Contains(string(data), "net.ipv4.tcp_congestion_control = bbr") {
		t.Errorf("bbr should be enabled after successful modprobe:\n%s", data)
	}
}
