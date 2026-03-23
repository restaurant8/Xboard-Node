package singbox

import (
	"encoding/json"
	"testing"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/panel"
)

var testUsers = []panel.User{
	{ID: 1, UUID: "aaaaaaaa-1111-2222-3333-444444444444", SpeedLimit: 0, DeviceLimit: 0},
	{ID: 2, UUID: "bbbbbbbb-5555-6666-7777-888888888888", SpeedLimit: 3, DeviceLimit: 2},
}

// --- Shadowsocks ---

func TestBuildInbound_Shadowsocks(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "shadowsocks",
		ServerPort: 111,
		Cipher:     "aes-128-gcm",
	}
	inbound := buildInbound(nc, testUsers, "", "")
	assertMapValue(t, inbound, "type", "shadowsocks")
	assertMapValue(t, inbound, "method", "aes-128-gcm")
	assertMapValue(t, inbound, "listen_port", 111)

	users := inbound["users"].([]M)
	if len(users) != 2 {
		t.Fatalf("users: got %d, want 2", len(users))
	}
	assertMapValue(t, users[0], "name", "aaaaaaaa-1111-2222-3333-444444444444")
	assertMapValue(t, users[0], "password", "aaaaaaaa-1111-2222-3333-444444444444")
}

func TestBuildInbound_Shadowsocks2022(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "shadowsocks",
		ServerPort: 222,
		Cipher:     "2022-blake3-aes-128-gcm",
		ServerKey:  "base64serverkey==",
	}
	inbound := buildInbound(nc, testUsers, "", "")
	assertMapValue(t, inbound, "method", "2022-blake3-aes-128-gcm")
	assertMapValue(t, inbound, "password", "base64serverkey==")
}

// --- VMess ---

func TestBuildInbound_VMess(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vmess",
		ServerPort: 443,
	}
	inbound := buildInbound(nc, testUsers, "", "")
	assertMapValue(t, inbound, "type", "vmess")
	assertMapValue(t, inbound, "tag", "vmess-in")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "uuid", "aaaaaaaa-1111-2222-3333-444444444444")
	assertMapValue(t, users[0], "alterId", 0)
}

func TestBuildInbound_VMess_WithTLS(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vmess",
		ServerPort: 443,
		TLS:        1,
		ServerName: "example.com",
	}
	inbound := buildInbound(nc, testUsers, "/path/cert.pem", "/path/key.pem")
	tls := inbound["tls"].(M)
	assertMapValue(t, tls, "enabled", true)
	assertMapValue(t, tls, "server_name", "example.com")
	assertMapValue(t, tls, "certificate_path", "/path/cert.pem")
	assertMapValue(t, tls, "key_path", "/path/key.pem")
}

func TestBuildInbound_VMess_NoTLS(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vmess",
		ServerPort: 80,
		TLS:        0,
	}
	inbound := buildInbound(nc, testUsers, "", "")
	if _, exists := inbound["tls"]; exists {
		t.Error("should not have TLS when tls=0")
	}
}

func TestBuildInbound_VMess_WithWebSocket(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vmess",
		ServerPort: 80,
		Network:    "ws",
		NetworkSettings: map[string]interface{}{
			"path": "/ws",
			"headers": map[string]interface{}{
				"Host": "example.com",
			},
		},
	}
	inbound := buildInbound(nc, testUsers, "", "")
	transport := inbound["transport"].(M)
	assertMapValue(t, transport, "type", "ws")
	assertMapValue(t, transport, "path", "/ws")
}

func TestBuildInbound_VMess_WithGRPC(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vmess",
		ServerPort: 443,
		Network:    "grpc",
		NetworkSettings: map[string]interface{}{
			"serviceName": "mygrpc",
		},
	}
	inbound := buildInbound(nc, testUsers, "", "")
	transport := inbound["transport"].(M)
	assertMapValue(t, transport, "type", "grpc")
	assertMapValue(t, transport, "service_name", "mygrpc")
}

func TestBuildInbound_VMess_WithH2(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vmess",
		ServerPort: 443,
		Network:    "h2",
		NetworkSettings: map[string]interface{}{
			"path": "/h2path",
			"host": "example.com",
		},
	}
	inbound := buildInbound(nc, testUsers, "", "")
	transport := inbound["transport"].(M)
	assertMapValue(t, transport, "type", "http")
	assertMapValue(t, transport, "path", "/h2path")
}

func TestBuildInbound_VMess_WithHTTPUpgrade(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vmess",
		ServerPort: 443,
		Network:    "httpupgrade",
		NetworkSettings: map[string]interface{}{
			"path": "/upgrade",
			"host": "example.com",
		},
	}
	inbound := buildInbound(nc, testUsers, "", "")
	transport := inbound["transport"].(M)
	assertMapValue(t, transport, "type", "httpupgrade")
	assertMapValue(t, transport, "path", "/upgrade")
	assertMapValue(t, transport, "host", "example.com")
}

// --- VLESS ---

func TestBuildInbound_VLESS(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vless",
		ServerPort: 443,
	}
	inbound := buildInbound(nc, testUsers, "", "")
	assertMapValue(t, inbound, "type", "vless")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "uuid", "aaaaaaaa-1111-2222-3333-444444444444")
}

func TestBuildInbound_VLESS_WithFlow(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vless",
		ServerPort: 443,
		Flow:       "xtls-rprx-vision",
	}
	inbound := buildInbound(nc, testUsers, "", "")
	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "flow", "xtls-rprx-vision")
}

func TestBuildInbound_VLESS_Reality(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vless",
		ServerPort: 443,
		TLS:        2,
		TLSSettings: map[string]interface{}{
			"private_key": "test-private-key",
			"short_id":    "abc123",
			"dest":        "www.example.com:443",
			"server_name": "www.example.com",
		},
	}
	inbound := buildInbound(nc, testUsers, "", "")
	tls := inbound["tls"].(M)
	assertMapValue(t, tls, "enabled", true)

	reality := tls["reality"].(M)
	assertMapValue(t, reality, "enabled", true)
	assertMapValue(t, reality, "private_key", "test-private-key")

	handshake := reality["handshake"].(M)
	assertMapValue(t, handshake, "server", "www.example.com")
	assertMapValue(t, handshake, "server_port", 443)
}

func TestBuildInbound_VLESS_Reality_ShortIDArray(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "vless",
		ServerPort: 443,
		TLS:        2,
		TLSSettings: map[string]interface{}{
			"private_key": "pk",
			"short_id":    []interface{}{"id1", "id2"},
			"dest":        "example.com",
		},
	}
	inbound := buildInbound(nc, testUsers, "", "")
	reality := inbound["tls"].(M)["reality"].(M)
	ids := reality["short_id"].([]string)
	if len(ids) != 2 {
		t.Errorf("short_id: got %d entries, want 2", len(ids))
	}
}

// --- Trojan ---

func TestBuildInbound_Trojan(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "trojan",
		ServerPort: 443,
	}
	inbound := buildInbound(nc, testUsers, "", "")
	assertMapValue(t, inbound, "type", "trojan")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "password", "aaaaaaaa-1111-2222-3333-444444444444")
}

func TestBuildInbound_Trojan_NoTLS(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "trojan",
		ServerPort: 80,
		TLS:        0,
	}
	inbound := buildInbound(nc, testUsers, "", "")
	// Trojan requires TLS; buildTrojan should force-enable self-signed TLS
	// even when the panel sends tls=0.
	tls, exists := inbound["tls"].(M)
	if !exists {
		t.Fatal("trojan with tls=0 should still get TLS (force-enabled)")
	}
	assertMapValue(t, tls, "enabled", true)
}

func TestBuildInbound_Trojan_WithTLS(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "trojan",
		ServerPort: 443,
		TLS:        1,
		ServerName: "example.com",
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	tls := inbound["tls"].(M)
	assertMapValue(t, tls, "enabled", true)
}

// --- Hysteria ---

func TestBuildInbound_Hysteria2(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "hysteria",
		ServerPort: 444,
		Version:    2,
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	assertMapValue(t, inbound, "type", "hysteria2")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "password", "aaaaaaaa-1111-2222-3333-444444444444")

	tls := inbound["tls"].(M)
	assertMapValue(t, tls, "enabled", true)
}

func TestBuildInbound_Hysteria2_WithObfs(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:     "hysteria",
		ServerPort:   444,
		Version:      2,
		Obfs:         "salamander",
		ObfsPassword: "secret",
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	obfs := inbound["obfs"].(M)
	assertMapValue(t, obfs, "type", "salamander")
	assertMapValue(t, obfs, "password", "secret")
}

func TestBuildInbound_Hysteria1(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "hysteria",
		ServerPort: 444,
		Version:    0,
		UpMbps:     100,
		DownMbps:   200,
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	assertMapValue(t, inbound, "type", "hysteria")
	assertMapValue(t, inbound, "up_mbps", 100)
	assertMapValue(t, inbound, "down_mbps", 200)

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "auth_str", "aaaaaaaa-1111-2222-3333-444444444444")
}

// --- TUIC ---

func TestBuildInbound_TUIC(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:          "tuic",
		ServerPort:        555,
		CongestionControl: "bbr",
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	assertMapValue(t, inbound, "type", "tuic")
	assertMapValue(t, inbound, "congestion_control", "bbr")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "uuid", "aaaaaaaa-1111-2222-3333-444444444444")
	assertMapValue(t, users[0], "password", "aaaaaaaa-1111-2222-3333-444444444444")
}

// --- AnyTLS ---

func TestBuildInbound_AnyTLS(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:      "anytls",
		ServerPort:    443,
		PaddingScheme: "stop=8\n0=30-30",
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	assertMapValue(t, inbound, "type", "anytls")
	assertMapValue(t, inbound, "padding_scheme", "stop=8\n0=30-30")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "password", "aaaaaaaa-1111-2222-3333-444444444444")
}

func TestBuildInbound_AnyTLS_NoPaddingScheme(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "anytls",
		ServerPort: 443,
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	if _, exists := inbound["padding_scheme"]; exists {
		t.Error("should not have padding_scheme when empty")
	}
}

// --- Naive ---

func TestBuildInbound_Naive(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "naive",
		ServerPort: 443,
		TLS:        1,
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	assertMapValue(t, inbound, "type", "naive")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "username", "1")
	assertMapValue(t, users[0], "password", "aaaaaaaa-1111-2222-3333-444444444444")

	tls := inbound["tls"].(M)
	assertMapValue(t, tls, "enabled", true)
}

// --- Socks ---

func TestBuildInbound_Socks(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "socks",
		ServerPort: 1080,
	}
	inbound := buildInbound(nc, testUsers, "", "")
	assertMapValue(t, inbound, "type", "socks")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "username", "aaaaaaaa-1111-2222-3333-444444444444")
	assertMapValue(t, users[0], "password", "aaaaaaaa-1111-2222-3333-444444444444")

	if _, exists := inbound["tls"]; exists {
		t.Error("socks should not have TLS")
	}
}

// --- HTTP ---

func TestBuildInbound_HTTP(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "http",
		ServerPort: 8080,
	}
	inbound := buildInbound(nc, testUsers, "", "")
	assertMapValue(t, inbound, "type", "http")

	users := inbound["users"].([]M)
	assertMapValue(t, users[0], "username", "aaaaaaaa-1111-2222-3333-444444444444")
}

func TestBuildInbound_HTTP_WithTLS(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "http",
		ServerPort: 443,
		TLS:        1,
	}
	inbound := buildInbound(nc, testUsers, "/c.pem", "/k.pem")
	tls := inbound["tls"].(M)
	assertMapValue(t, tls, "enabled", true)
}

// --- Unknown protocol ---

func TestBuildInbound_Unknown(t *testing.T) {
	nc := &panel.NodeConfig{
		Protocol:   "unknown-proto",
		ServerPort: 999,
	}
	inbound := buildInbound(nc, testUsers, "", "")
	if inbound != nil {
		t.Errorf("unknown protocol should return nil, got %v", inbound)
	}
}

// --- buildConfig ---

func TestBuildConfig(t *testing.T) {
	kcfg := config.KernelConfig{LogLevel: "info"}
	nc := &panel.NodeConfig{
		Protocol:   "shadowsocks",
		ServerPort: 111,
		Cipher:     "aes-128-gcm",
	}
	cfg := buildConfig(kcfg, nc, testUsers, "", "")

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if len(data) == 0 {
		t.Error("empty JSON output")
	}

	logCfg := cfg["log"].(M)
	assertMapValue(t, logCfg, "level", "info")
	assertMapValue(t, logCfg, "timestamp", true)

	outbounds := cfg["outbounds"].([]M)
	if len(outbounds) != 2 {
		t.Errorf("outbounds: got %d, want 2", len(outbounds))
	}
	assertMapValue(t, outbounds[0], "type", "direct")
	assertMapValue(t, outbounds[1], "type", "block")

	inbounds := cfg["inbounds"].([]M)
	if len(inbounds) != 1 {
		t.Fatalf("inbounds: got %d, want 1", len(inbounds))
	}
	assertMapValue(t, inbounds[0], "type", "shadowsocks")
}

func TestBuildConfig_OutboundPriority(t *testing.T) {
	kcfg := config.KernelConfig{
		LogLevel: "info",
		CustomOutbound: []map[string]any{
			{"tag": "block", "type": "dns"}, // Local static override
		},
	}
	nc := &panel.NodeConfig{
		Protocol:   "shadowsocks",
		ServerPort: 111,
		Cipher:     "aes-128-gcm",
		CustomOutbounds: []panel.OutboundConfig{
			{Tag: "direct", Protocol: "socks", Settings: map[string]any{"address": "1.2.3.4"}}, // Panel override
		},
	}

	cfg := buildConfig(kcfg, nc, testUsers, "", "")
	outbounds := cfg["outbounds"].([]M)

	// We have 2 overrides in input, so we should have exactly 2 outbounds total
	// (no auto-generated defaults for these tags)
	if len(outbounds) != 2 {
		t.Errorf("outbounds: got %d, want 2", len(outbounds))
	}

	foundDirect := false
	foundBlock := false

	for _, o := range outbounds {
		tag := o["tag"].(string)
		if tag == "direct" {
			foundDirect = true
			if o["type"] != "socks" {
				t.Errorf("expected 'direct' outbound to be type 'socks' (panel priority), got %v", o["type"])
			}
		}
		if tag == "block" {
			foundBlock = true
			if o["type"] != "dns" {
				t.Errorf("expected 'block' outbound to be type 'dns' (static config priority), got %v", o["type"])
			}
		}
	}

	if !foundDirect || !foundBlock {
		t.Errorf("missing outbounds: direct=%v, block=%v", foundDirect, foundBlock)
	}
}

func TestBuildConfig_AllProtocols_ValidJSON(t *testing.T) {
	protocols := []struct {
		name string
		nc   *panel.NodeConfig
	}{
		{"shadowsocks", &panel.NodeConfig{Protocol: "shadowsocks", ServerPort: 111, Cipher: "aes-128-gcm"}},
		{"vmess", &panel.NodeConfig{Protocol: "vmess", ServerPort: 443}},
		{"vless", &panel.NodeConfig{Protocol: "vless", ServerPort: 443}},
		{"trojan", &panel.NodeConfig{Protocol: "trojan", ServerPort: 443}},
		{"hysteria2", &panel.NodeConfig{Protocol: "hysteria", ServerPort: 444, Version: 2}},
		{"hysteria1", &panel.NodeConfig{Protocol: "hysteria", ServerPort: 444, Version: 1}},
		{"tuic", &panel.NodeConfig{Protocol: "tuic", ServerPort: 555}},
		{"anytls", &panel.NodeConfig{Protocol: "anytls", ServerPort: 443}},
		{"naive", &panel.NodeConfig{Protocol: "naive", ServerPort: 443}},
		{"socks", &panel.NodeConfig{Protocol: "socks", ServerPort: 1080}},
		{"http", &panel.NodeConfig{Protocol: "http", ServerPort: 8080}},
	}

	for _, tc := range protocols {
		t.Run(tc.name, func(t *testing.T) {
			cfg := buildConfig(config.KernelConfig{LogLevel: "warn"}, tc.nc, testUsers, "/c.pem", "/k.pem")
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatalf("marshal %s: %v", tc.name, err)
			}
			var parsed map[string]interface{}
			if err := json.Unmarshal(data, &parsed); err != nil {
				t.Fatalf("round-trip %s: %v", tc.name, err)
			}
		})
	}
}

// --- Routes ---

func TestBuildRoutes_Default(t *testing.T) {
	route := buildRoutes(nil, nil)
	assertMapValue(t, route, "final", "direct")

	rules := route["rules"].([]M)
	if len(rules) < 2 {
		t.Fatalf("expected at least 2 default rules, got %d", len(rules))
	}
	assertMapValue(t, rules[0], "outbound", "block")
	assertMapValue(t, rules[1], "outbound", "block")
}

func TestBuildRoutes_WithCustomRules(t *testing.T) {
	rules := []panel.RouteRule{
		{ID: 1, Match: []string{"blocked.com"}, Action: "block"},
		{ID: 2, Match: []string{"10.0.0.0/8"}, Action: "block"},
		{ID: 3, Match: []string{"allowed.com"}, Action: "direct"},
	}
	route := buildRoutes(rules, nil)
	allRules := route["rules"].([]M)

	if len(allRules) != 5 {
		t.Fatalf("rules count: got %d, want 5", len(allRules))
	}

	assertMapValue(t, allRules[2], "outbound", "block")
	if _, ok := allRules[2]["domain_suffix"]; !ok {
		t.Error("domain rule should use domain_suffix")
	}

	assertMapValue(t, allRules[3], "outbound", "block")
	if _, ok := allRules[3]["ip_cidr"]; !ok {
		t.Error("IP rule should use ip_cidr")
	}
}

func TestBuildRoutes_MultiMatch(t *testing.T) {
	// A single route with mixed domain + CIDR matches should produce separate rules
	// Wildcard *.evil.com should become evil.com for domain_suffix
	rules := []panel.RouteRule{
		{ID: 1, Match: []string{"*.evil.com", "bad.org", "192.168.1.0/24"}, Action: "block"},
		{ID: 2, Match: []string{"*.bypass.com"}, Action: "direct"},
	}
	route := buildRoutes(rules, nil)
	allRules := route["rules"].([]M)

	// 2 default private-IP rules + 1 domain rule + 1 CIDR rule + 1 domain rule = 5
	if len(allRules) != 5 {
		t.Fatalf("rules count: got %d, want 5", len(allRules))
	}

	// Rule #2 (index 2): domains from first route (wildcards stripped)
	domains := allRules[2]["domain_suffix"].([]string)
	if len(domains) != 2 || domains[0] != "evil.com" || domains[1] != "bad.org" {
		t.Errorf("domain_suffix: got %v, want [evil.com bad.org]", domains)
	}
	assertMapValue(t, allRules[2], "outbound", "block")

	// Rule #3 (index 3): CIDRs from first route
	cidrs := allRules[3]["ip_cidr"].([]string)
	if len(cidrs) != 1 || cidrs[0] != "192.168.1.0/24" {
		t.Errorf("ip_cidr: got %v, want [192.168.1.0/24]", cidrs)
	}
	assertMapValue(t, allRules[3], "outbound", "block")

	// Rule #4 (index 4): direct rule (wildcard stripped)
	directDomains := allRules[4]["domain_suffix"].([]string)
	if len(directDomains) != 1 || directDomains[0] != "bypass.com" {
		t.Errorf("direct domain_suffix: got %v, want [bypass.com]", directDomains)
	}
	assertMapValue(t, allRules[4], "outbound", "direct")
}

// --- TLS Config ---

func TestBuildTLSConfig_WithCert(t *testing.T) {
	nc := &panel.NodeConfig{ServerName: "example.com"}
	tls := buildTLSConfig(nc, "/cert.pem", "/key.pem")
	assertMapValue(t, tls, "enabled", true)
	assertMapValue(t, tls, "server_name", "example.com")
	assertMapValue(t, tls, "certificate_path", "/cert.pem")
	assertMapValue(t, tls, "key_path", "/key.pem")
}

func TestBuildTLSConfig_NoCert(t *testing.T) {
	nc := &panel.NodeConfig{ServerName: "example.com"}
	tls := buildTLSConfig(nc, "", "")
	assertMapValue(t, tls, "enabled", true)
	// Now we fallback to self-signed certificates when both cert and key are empty
	assertMapValue(t, tls, "certificate_path", "self-signed")
}

func TestBuildTLSConfig_FallbackToHost(t *testing.T) {
	nc := &panel.NodeConfig{Host: "fallback.com"}
	tls := buildTLSConfig(nc, "", "")
	assertMapValue(t, tls, "server_name", "fallback.com")
}

func TestBuildTLSConfig_TLSSettingsOverride(t *testing.T) {
	nc := &panel.NodeConfig{
		ServerName: "original.com",
		TLSSettings: map[string]interface{}{
			"server_name": "override.com",
		},
	}
	tls := buildTLSConfig(nc, "", "")
	assertMapValue(t, tls, "server_name", "override.com")
}

// --- Transport ---

func TestApplyTransport_TCP(t *testing.T) {
	base := M{}
	nc := &panel.NodeConfig{Network: "tcp"}
	applyTransport(base, nc)
	if _, exists := base["transport"]; exists {
		t.Error("tcp should not add transport")
	}
}

func TestApplyTransport_Empty(t *testing.T) {
	base := M{}
	nc := &panel.NodeConfig{Network: ""}
	applyTransport(base, nc)
	if _, exists := base["transport"]; exists {
		t.Error("empty network should not add transport")
	}
}

func TestApplyTransport_WS_MaxEarlyData(t *testing.T) {
	base := M{}
	nc := &panel.NodeConfig{
		Network: "ws",
		NetworkSettings: map[string]interface{}{
			"path":                   "/ws",
			"max_early_data":         2048,
			"early_data_header_name": "Sec-WebSocket-Protocol",
		},
	}
	applyTransport(base, nc)
	transport := base["transport"].(M)
	assertMapValue(t, transport, "max_early_data", 2048)
	assertMapValue(t, transport, "early_data_header_name", "Sec-WebSocket-Protocol")
}

// --- helpers ---

func assertMapValue(t *testing.T, m M, key string, expected interface{}) {
	t.Helper()
	val, ok := m[key]
	if !ok {
		t.Errorf("key %q not found in map", key)
		return
	}
	switch e := expected.(type) {
	case int:
		switch v := val.(type) {
		case int:
			if v != e {
				t.Errorf("%s: got %d, want %d", key, v, e)
			}
		case float64:
			if int(v) != e {
				t.Errorf("%s: got %v, want %d", key, v, e)
			}
		default:
			t.Errorf("%s: got %v (type %T), want %d", key, val, val, e)
		}
	default:
		if val != expected {
			t.Errorf("%s: got %v, want %v", key, val, expected)
		}
	}
}
