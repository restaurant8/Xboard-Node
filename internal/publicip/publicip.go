// Package publicip detects the node's public IPv4 and IPv6 addresses so the
// panel can display dual-stack nodes with both IPs. Detection runs in the
// background; Get() never blocks (it returns the last cached result).
package publicip

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const refreshInterval = 10 * time.Minute

type endpoint struct {
	network string // "tcp4" or "tcp6" — forces the address family
	url     string
}

// Echo services that return the caller's public IP as plain text.
var endpoints = []endpoint{
	{"tcp4", "https://api.ipify.org"},
	{"tcp6", "https://api6.ipify.org"},
}

var (
	mu        sync.RWMutex
	cached    []string
	startOnce sync.Once
)

// Get returns the detected public IPs (0–2). The first call starts a background
// refresher; it returns immediately (empty until the first detection lands).
func Get() []string {
	startOnce.Do(func() {
		go func() {
			refresh()
			t := time.NewTicker(refreshInterval)
			defer t.Stop()
			for range t.C {
				refresh()
			}
		}()
	})
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, len(cached))
	copy(out, cached)
	return out
}

func refresh() {
	var ips []string
	seen := map[string]bool{}
	for _, ep := range endpoints {
		ip := detect(ep.network, ep.url)
		if ip != "" && !seen[ip] {
			seen[ip] = true
			ips = append(ips, ip)
		}
	}
	if len(ips) == 0 {
		return // keep the previous cache rather than clobbering it on a transient failure
	}
	mu.Lock()
	cached = ips
	mu.Unlock()
}

func detect(network, url string) string {
	dialer := &net.Dialer{Timeout: 8 * time.Second}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			// Force the address family regardless of what the URL resolves to.
			return dialer.DialContext(ctx, network, addr)
		},
		DisableKeepAlives: true,
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: transport}

	resp, err := client.Get(url)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return ""
	}
	ip := strings.TrimSpace(string(body))
	if net.ParseIP(ip) == nil {
		return ""
	}
	return ip
}
