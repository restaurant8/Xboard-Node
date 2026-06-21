package selfupdate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadURL(t *testing.T) {
	base := "https://example.com/releases"
	cases := []struct {
		version  string
		artifact string
		want     string
	}{
		{"latest", "xboard-node-linux-amd64", "https://example.com/releases/latest/download/xboard-node-linux-amd64"},
		{"v1.2.3", "xbctl-linux-arm64", "https://example.com/releases/download/v1.2.3/xbctl-linux-arm64"},
	}
	for _, c := range cases {
		if got := downloadURL(base, c.version, c.artifact); got != c.want {
			t.Errorf("downloadURL(%q,%q) = %q, want %q", c.version, c.artifact, got, c.want)
		}
	}
}

func TestFetchChecksums(t *testing.T) {
	body := "abc123  xboard-node-linux-amd64\n" +
		"def456  ./dist/xbctl-linux-amd64\n" +
		"# a comment line that should be ignored without two fields\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	sums, ok := fetchChecksums(srv.URL)
	if !ok {
		t.Fatal("expected checksums to be parsed")
	}
	if sums["xboard-node-linux-amd64"] != "abc123" {
		t.Errorf("binary checksum = %q", sums["xboard-node-linux-amd64"])
	}
	// Basename of a path entry should be used as the key.
	if sums["xbctl-linux-amd64"] != "def456" {
		t.Errorf("cli checksum = %q", sums["xbctl-linux-amd64"])
	}
}

func TestFetchChecksumsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if _, ok := fetchChecksums(srv.URL); ok {
		t.Error("expected ok=false for 404 manifest")
	}
}

func TestVerifyChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "artifact.bin")
	content := []byte("hello world")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	hexSum := hex.EncodeToString(sum[:])

	// Correct checksum passes.
	if err := verifyChecksum(path, "artifact.bin", map[string]string{"artifact.bin": hexSum}); err != nil {
		t.Errorf("expected match, got %v", err)
	}
	// Wrong checksum fails.
	if err := verifyChecksum(path, "artifact.bin", map[string]string{"artifact.bin": "deadbeef"}); err == nil {
		t.Error("expected mismatch error")
	}
	// Missing entry fails (manifest present but artifact absent => never install unverified).
	if err := verifyChecksum(path, "other.bin", map[string]string{"artifact.bin": hexSum}); err == nil {
		t.Error("expected missing-checksum error")
	}
}

func TestSameVersion(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"v1.2.3", "1.2.3", true},
		{"v1.2.3", "v1.2.3", true},
		{"1.2.3", "1.2.4", false},
		{"dev", "v1.2.3", false},
		{"v1.2.3", "dev", false},
		{"", "v1.2.3", false},
		{"  v1.2.3  ", "v1.2.3", true},
	}
	for _, c := range cases {
		if got := sameVersion(c.a, c.b); got != c.want {
			t.Errorf("sameVersion(%q,%q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestResolveTargetVersion(t *testing.T) {
	// Fixed version returns as-is without any network call.
	if got := resolveTargetVersion("https://example.com/releases", "v2.0.0"); got != "v2.0.0" {
		t.Errorf("fixed version = %q, want v2.0.0", got)
	}

	// "latest" resolves via the /latest redirect Location header.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/releases/latest" {
			w.Header().Set("Location", "/releases/tag/v3.1.4")
			w.WriteHeader(http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if got := resolveTargetVersion(srv.URL+"/releases", "latest"); got != "v3.1.4" {
		t.Errorf("latest resolved = %q, want v3.1.4", got)
	}
	if got := resolveTargetVersion(srv.URL+"/releases", ""); got != "v3.1.4" {
		t.Errorf("empty resolved = %q, want v3.1.4", got)
	}
}

// TestApplyDuplicateGuard verifies that a second upgrade is rejected while one
// is already in progress.
func TestApplyDuplicateGuard(t *testing.T) {
	if !inProgress.CompareAndSwap(false, true) {
		t.Fatal("guard should start unset")
	}
	defer inProgress.Store(false)

	rec := &recordingSender{}
	// inProgress is already true, so Apply must return immediately without
	// sending any status or invoking exit.
	called := false
	old := exitFn
	exitFn = func() { called = true }
	defer func() { exitFn = old }()

	Apply(rec, Command{RequestID: "dup"})

	if len(rec.events) != 0 {
		t.Errorf("expected no events while guarded, got %v", rec.events)
	}
	if called {
		t.Error("exit must not be called when guarded")
	}
}

type recordingSender struct {
	events []string
}

func (r *recordingSender) SendRaw(event string, data json.RawMessage) {
	r.events = append(r.events, event)
}
