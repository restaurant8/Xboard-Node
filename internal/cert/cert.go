package cert

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/libdns/alidns"
	"github.com/libdns/cloudflare"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/nlog"
)

// Manager handles TLS certificate lifecycle.
//
// Supported modes (CertConfig.CertMode):
//   - "http"    — ACME HTTP-01 challenge via certmagic (needs port 80).
//   - "dns"     — ACME DNS-01 challenge via certmagic + libdns provider.
//   - "self"    — Self-signed certificate. Automatically generated and stored in CertDir.
//     Recommended for nodes behind a CDN or reverse proxy.
//   - "file"    — User-provided certificate and key file paths (CertFile/KeyFile).
//   - "content" — Certificate and key PEM content pushed from the panel.
//     Written to disk in CertDir for the kernel to consume.
//   - "none"    — No TLS. The node will handle plain connections.
//
// Priority Logic:
//
//	If CertMode is empty, the manager infers the mode in this order:
//	1. "http" (if auto_tls is true)
//	2. "content" (if both CertContent and KeyContent are provided)
//	3. "file" (if both CertFile and KeyFile paths are provided)
//	4. "none" (default)
type Manager struct {
	cfg      config.CertConfig
	certFile string
	keyFile  string

	magic       *certmagic.Config
	renewed     atomic.Bool
	acmeStarted bool // prevents double-init of certmagic background goroutines
}

// NewManager creates a certificate manager.
func NewManager(cfg config.CertConfig) *Manager {
	m := &Manager{cfg: cfg}
	m.setCertPaths()
	return m
}

// setCertPaths derives certFile / keyFile from the current m.cfg.
func (m *Manager) setCertPaths() {
	switch m.resolveMode() {
	case "file":
		m.certFile = m.cfg.CertFile
		m.keyFile = m.cfg.KeyFile
	case "http", "dns", "self", "content":
		dir := m.cfg.CertDir
		if dir == "" {
			dir = "/etc/xboard-node/certs"
		}
		domain := m.cfg.Domain
		if domain == "" {
			domain = "localhost"
		}
		m.certFile = filepath.Join(dir, domain+".crt")
		m.keyFile = filepath.Join(dir, domain+".key")
	default:
		m.certFile = ""
		m.keyFile = ""
	}
}

// Reconfigure applies a new cert configuration at runtime (e.g. from panel push).
// It is safe to call after Start(). For ACME modes that are already running this
// is a no-op — a process restart is required to change ACME settings.
// Returns true when the on-disk cert paths changed (caller should restart kernel).
func (m *Manager) Reconfigure(ctx context.Context, newCfg config.CertConfig) (bool, error) {
	// Preserve the storage directory so panel-pushed configs don't change
	// where certs live on disk (only the operator controls this path).
	if newCfg.CertDir == "" {
		newCfg.CertDir = m.cfg.CertDir
	}

	oldCertFile, oldKeyFile := m.certFile, m.keyFile
	m.cfg = newCfg
	m.setCertPaths()

	if err := m.Start(ctx); err != nil {
		return false, fmt.Errorf("cert reconfigure: %w", err)
	}

	return m.certFile != oldCertFile || m.keyFile != oldKeyFile, nil
}

// resolveMode returns the effective cert mode, handling backward compat for auto_tls.
func (m *Manager) resolveMode() string {
	mode := strings.ToLower(strings.TrimSpace(m.cfg.CertMode))
	if mode != "" {
		return mode
	}
	// Backward compat: auto_tls: true → "http"
	if m.cfg.AutoTLS {
		return "http"
	}
	// Inline PEM content provided → "content"
	if m.cfg.CertContent != "" && m.cfg.KeyContent != "" {
		return "content"
	}
	// If cert/key file paths are provided → "file"
	if m.cfg.CertFile != "" && m.cfg.KeyFile != "" {
		return "file"
	}
	return "none"
}

func (m *Manager) CertFile() string  { return m.certFile }
func (m *Manager) KeyFile() string   { return m.keyFile }
func (m *Manager) HasCert() bool     { return m.certFile != "" && m.keyFile != "" }
func (m *Manager) CertRenewed() bool { return m.renewed.Swap(false) }

// Start initializes the cert manager based on the resolved mode.
func (m *Manager) Start(ctx context.Context) error {
	mode := m.resolveMode()

	switch mode {
	case "none", "":
		return nil
	case "file":
		return m.startFile()
	case "self":
		return m.startSelfSigned()
	case "content":
		return m.startContent()
	case "http":
		return m.startACME(ctx, nil)
	case "dns":
		solver, err := m.buildDNSSolver()
		if err != nil {
			return fmt.Errorf("build dns solver: %w", err)
		}
		return m.startACME(ctx, solver)
	default:
		return fmt.Errorf("unknown cert_mode: %q (supported: http, dns, self, file, content, none)", mode)
	}
}

// Stop is a no-op: certmagic's background goroutine is cancelled via the
// context passed to ManageAsync.
func (m *Manager) Stop() {}

// ─── Mode: file ────────────────────────────────────────────────────────────

func (m *Manager) startFile() error {
	if _, err := os.Stat(m.cfg.CertFile); err != nil {
		return fmt.Errorf("cert file: %w", err)
	}
	if _, err := os.Stat(m.cfg.KeyFile); err != nil {
		return fmt.Errorf("key file: %w", err)
	}
	nlog.Core().Debug("using manual TLS certificates", "cert", m.certFile, "key", m.keyFile)
	return nil
}

// ─── Mode: self ────────────────────────────────────────────────────────────

func (m *Manager) startSelfSigned() error {
	// If files already exist and are valid, skip regeneration.
	if fileExists(m.certFile) && fileExists(m.keyFile) {
		nlog.Core().Debug("self-signed cert exists, reusing", "cert", m.certFile)
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(m.certFile), 0o755); err != nil {
		return fmt.Errorf("create cert dir: %w", err)
	}

	domain := m.cfg.Domain
	if domain == "" {
		domain = "localhost"
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate serial: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: domain},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour), // 10 years
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	// Add SANs
	if ip := net.ParseIP(domain); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{domain}
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if err := atomicWriteFile(m.certFile, certPEM, 0o644); err != nil {
		return fmt.Errorf("write cert: %w", err)
	}
	if err := atomicWriteFile(m.keyFile, keyPEM, 0o600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}

	nlog.Core().Info("self-signed certificate generated",
		"domain", domain, "cert", m.certFile, "valid_years", 10)
	return nil
}

// ─── Mode: content ────────────────────────────────────────────────────────

// startContent writes panel-supplied PEM strings to disk so the kernel can
// reference them as ordinary cert/key files.
func (m *Manager) startContent() error {
	if m.cfg.CertContent == "" || m.cfg.KeyContent == "" {
		return fmt.Errorf("cert_mode 'content' requires both cert_content and key_content")
	}
	if err := os.MkdirAll(filepath.Dir(m.certFile), 0o755); err != nil {
		return fmt.Errorf("create cert dir: %w", err)
	}
	if err := atomicWriteFile(m.certFile, []byte(m.cfg.CertContent), 0o644); err != nil {
		return fmt.Errorf("write cert content: %w", err)
	}
	if err := atomicWriteFile(m.keyFile, []byte(m.cfg.KeyContent), 0o600); err != nil {
		return fmt.Errorf("write key content: %w", err)
	}
	nlog.Core().Info("TLS certificate written from panel content", "cert", m.certFile)
	return nil
}

// ─── Mode: http / dns (ACME) ──────────────────────────────────────────────

func (m *Manager) startACME(ctx context.Context, dnsSolver *certmagic.DNS01Solver) error {
	// Guard: certmagic's background goroutines must only be started once.
	// A process restart is required to change ACME settings at runtime.
	if m.acmeStarted {
		nlog.Core().Info("cert: ACME already active, skipping re-init (restart to change ACME settings)")
		return nil
	}

	if m.cfg.Domain == "" {
		return fmt.Errorf("cert.domain is required for ACME modes (http/dns)")
	}

	if err := os.MkdirAll(m.cfg.CertDir, 0o755); err != nil {
		return fmt.Errorf("create cert dir: %w", err)
	}

	storage := &certmagic.FileStorage{Path: m.cfg.CertDir}

	var magic *certmagic.Config
	cache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert: func(_ certmagic.Certificate) (*certmagic.Config, error) {
			return magic, nil
		},
	})

	magic = certmagic.New(cache, certmagic.Config{
		Storage: storage,
		OnEvent: func(evtCtx context.Context, event string, data map[string]any) error {
			if event == "cert_obtained" {
				m.syncCertFiles(evtCtx, storage, data)
			}
			return nil
		},
	})

	issuer := certmagic.ACMEIssuer{
		CA:    certmagic.LetsEncryptProductionCA,
		Email: m.cfg.Email,
	}

	if dnsSolver != nil {
		// DNS-01 mode: no HTTP port needed, supports wildcards.
		issuer.DNS01Solver = dnsSolver
		issuer.DisableHTTPChallenge = true
		issuer.DisableTLSALPNChallenge = true
	} else {
		// HTTP-01 mode.
		httpPort := m.cfg.HTTPPort
		if httpPort == 0 {
			httpPort = 80
		}
		issuer.AltHTTPPort = httpPort
		issuer.DisableTLSALPNChallenge = true
	}

	magic.Issuers = []certmagic.Issuer{
		certmagic.NewACMEIssuer(magic, issuer),
	}
	m.magic = magic

	if err := magic.ObtainCertSync(ctx, m.cfg.Domain); err != nil {
		return fmt.Errorf("obtain certificate: %w", err)
	}

	if err := magic.ManageAsync(ctx, []string{m.cfg.Domain}); err != nil {
		return fmt.Errorf("start cert manager: %w", err)
	}

	m.acmeStarted = true
	return nil
}

// ─── DNS Provider Factory ──────────────────────────────────────────────────

// buildDNSSolver builds a certmagic DNS01Solver from the configured provider.
func (m *Manager) buildDNSSolver() (*certmagic.DNS01Solver, error) {
	provider, err := m.newDNSProvider()
	if err != nil {
		return nil, err
	}
	return &certmagic.DNS01Solver{DNSManager: certmagic.DNSManager{DNSProvider: provider}}, nil
}

func (m *Manager) newDNSProvider() (certmagic.DNSProvider, error) {
	name := strings.ToLower(strings.TrimSpace(m.cfg.DNSProvider))
	env := m.cfg.DNSEnv
	if env == nil {
		env = map[string]string{}
	}

	switch name {
	case "cloudflare", "cf":
		token := firstOf(env, "CF_API_TOKEN", "CLOUDFLARE_API_TOKEN")
		if token == "" {
			return nil, fmt.Errorf("cloudflare requires CF_API_TOKEN in dns_env")
		}
		return &cloudflare.Provider{APIToken: token}, nil

	case "alidns", "aliyun":
		keyID := firstOf(env, "ALICLOUD_ACCESS_KEY_ID", "ALI_ACCESS_KEY_ID")
		keySecret := firstOf(env, "ALICLOUD_ACCESS_KEY_SECRET", "ALI_ACCESS_KEY_SECRET")
		if keyID == "" || keySecret == "" {
			return nil, fmt.Errorf("alidns requires ALICLOUD_ACCESS_KEY_ID and ALICLOUD_ACCESS_KEY_SECRET in dns_env")
		}
		return &alidns.Provider{
			CredentialInfo: alidns.CredentialInfo{
				AccessKeyID:     keyID,
				AccessKeySecret: keySecret,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported dns_provider: %q (supported: cloudflare, alidns)", name)
	}
}

// ─── Helpers ───────────────────────────────────────────────────────────────

// syncCertFiles copies the cert and key from certmagic's storage to stable paths.
func (m *Manager) syncCertFiles(ctx context.Context, storage certmagic.Storage, data map[string]any) {
	certKey, _ := data["certificate_path"].(string)
	keyKey, _ := data["private_key_path"].(string)
	if certKey == "" || keyKey == "" {
		nlog.Core().Warn("cert_obtained event missing storage paths", "data", data)
		return
	}

	certPEM, err := storage.Load(ctx, certKey)
	if err != nil {
		nlog.Core().Error("failed to load cert from storage", "error", err)
		return
	}
	keyPEM, err := storage.Load(ctx, keyKey)
	if err != nil {
		nlog.Core().Error("failed to load key from storage", "error", err)
		return
	}

	if err := atomicWriteFile(m.certFile, certPEM, 0o644); err != nil {
		nlog.Core().Error("failed to write cert file", "path", m.certFile, "error", err)
		return
	}
	if err := atomicWriteFile(m.keyFile, keyPEM, 0o600); err != nil {
		nlog.Core().Error("failed to write key file", "path", m.keyFile, "error", err)
		return
	}

	renewal, _ := data["renewal"].(bool)
	nlog.Core().Info("TLS certificate synced", "domain", m.cfg.Domain, "renewal", renewal,
		"cert", m.certFile)
	m.renewed.Store(true)
}

func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// firstOf returns the first non-empty value for any of the given keys in the map.
func firstOf(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}
