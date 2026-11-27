package xray

import (
	"sync"
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/transport"
)

func newTestDispatcher() *LimitDispatcher {
	return &LimitDispatcher{
		limitedIPs: make(map[string]map[string]int),
	}
}

type nopReader struct{}

func (nopReader) ReadMultiBuffer() (buf.MultiBuffer, error) { return nil, nil }

func TestLimitDispatcher_DeviceLimitCheck(t *testing.T) {
	ld := newTestDispatcher()

	users := []model.UserSpec{
		{ID: 1, UUID: "uuid-1", DeviceLimit: 2, SpeedLimit: 0},
		{ID: 2, UUID: "uuid-2", DeviceLimit: 0, SpeedLimit: 10},
	}

	emailToUID := make(map[string]int)
	deviceLimits := make(map[string]int)
	speedLimits := make(map[string]int)
	for _, u := range users {
		email := userEmail(u.ID)
		emailToUID[email] = u.ID
		if u.DeviceLimit > 0 {
			deviceLimits[email] = u.DeviceLimit
		}
		if u.SpeedLimit > 0 {
			speedLimits[email] = u.SpeedLimit
		}
	}
	ld.UpdateLimits(emailToUID, deviceLimits, speedLimits)

	email1 := userEmail(1)

	// First IP should be allowed
	if ld.checkDeviceLimit(email1, "1.1.1.1", true) {
		t.Error("first IP should be allowed")
	}

	// Second IP should be allowed (limit=2)
	if ld.checkDeviceLimit(email1, "2.2.2.2", true) {
		t.Error("second IP should be allowed")
	}

	// Third unique IP should be rejected
	if !ld.checkDeviceLimit(email1, "3.3.3.3", true) {
		t.Error("third IP should be rejected (limit=2)")
	}

	// Same IP as first should be allowed (already connected)
	if ld.checkDeviceLimit(email1, "1.1.1.1", true) {
		t.Error("same IP should always be allowed")
	}

	// User 2 has no device limit — should always be allowed
	email2 := userEmail(2)
	for i := 0; i < 10; i++ {
		ip := "10.0.0." + string(rune('0'+i))
		if ld.checkDeviceLimit(email2, ip, true) {
			t.Errorf("user with no device limit should always be allowed (ip=%s)", ip)
		}
	}
}

func TestLimitDispatcher_DelConn(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	deviceLimits := map[string]int{email: 2}
	ld.UpdateLimits(map[string]int{email: 1}, deviceLimits, nil)

	// Add 2 IPs
	ld.checkDeviceLimit(email, "1.1.1.1", true)
	ld.checkDeviceLimit(email, "2.2.2.2", true)

	// Third should be rejected
	if !ld.checkDeviceLimit(email, "3.3.3.3", true) {
		t.Error("third IP should be rejected")
	}

	// Remove first IP
	ld.delConn(email, "1.1.1.1")

	// Now third IP should be allowed
	if ld.checkDeviceLimit(email, "3.3.3.3", true) {
		t.Error("after deleting one IP, new IP should be allowed")
	}
}

func TestLimitDispatcher_GetConnectionState(t *testing.T) {
	ld := newTestDispatcher()

	email1 := userEmail(1)
	email2 := userEmail(2)
	ld.UpdateLimits(map[string]int{email1: 1, email2: 2}, nil, nil)

	ld.addUnlimitedIP(email1, "1.1.1.1")
	ld.addUnlimitedIP(email1, "2.2.2.2")
	ld.addUnlimitedIP(email2, "3.3.3.3")

	ld.incUserConn(email1)
	ld.incUserConn(email1)
	ld.incUserConn(email1)
	ld.incUserConn(email2)
	ld.incUserConn(email2)
	ld.connCount.Store(5)

	aliveIPs, userConns, connCount := ld.GetConnectionState()

	if connCount != 5 {
		t.Errorf("expected connCount=5, got %d", connCount)
	}
	if userConns[1] != 3 || userConns[2] != 2 {
		t.Errorf("userConns = %#v, want map[1:3 2:2]", userConns)
	}
	if len(aliveIPs[1]) != 2 {
		t.Errorf("user 1 IPs: got %d, want 2", len(aliveIPs[1]))
	}
	if len(aliveIPs[2]) != 1 {
		t.Errorf("user 2 IPs: got %d, want 1", len(aliveIPs[2]))
	}
}

func TestLimitDispatcher_PerUserTCPAndUDPConcurrencyLifecycle(t *testing.T) {
	ld := newTestDispatcher()
	email := userEmail(42)
	ld.UpdateLimits(map[string]int{email: 42}, nil, nil)

	// Same IP opens 40 TCP connections + 10 UDP sessions => 1 IP, 50 concurrent connections.
	tcpLinks := make([]*transport.Link, 0, 40)
	for i := 0; i < 40; i++ {
		if ld.checkDeviceLimit(email, "198.51.100.7", true) {
			t.Fatalf("unexpected TCP reject at %d", i)
		}
		link := &transport.Link{Writer: &noopBufWriter{}}
		ld.trackLink(link, email, "198.51.100.7", true)
		tcpLinks = append(tcpLinks, link)
	}
	udpLinks := make([]*transport.Link, 0, 10)
	for i := 0; i < 10; i++ {
		link := &transport.Link{Writer: &noopBufWriter{}}
		ld.trackLink(link, email, "198.51.100.7", false)
		udpLinks = append(udpLinks, link)
	}

	aliveIPs, userConns, connCount := ld.GetConnectionState()
	if len(aliveIPs[42]) != 1 || !aliveIPs[42]["198.51.100.7"] {
		t.Fatalf("aliveIPs[42] = %#v, want 1 IP", aliveIPs[42])
	}
	if userConns[42] != 50 || connCount != 50 {
		t.Fatalf("got userConns=%d connCount=%d, want 50/50", userConns[42], connCount)
	}

	// Close 20 TCP connections => 30 concurrent connections, IP still 1.
	for i := 0; i < 20; i++ {
		_ = common.Close(tcpLinks[i].Writer)
	}
	aliveIPs, userConns, connCount = ld.GetConnectionState()
	if len(aliveIPs[42]) != 1 {
		t.Fatalf("aliveIPs[42] after closing 20 TCP = %#v, want 1", aliveIPs[42])
	}
	if userConns[42] != 30 || connCount != 30 {
		t.Fatalf("after closing 20 TCP got userConns=%d connCount=%d, want 30/30", userConns[42], connCount)
	}

	// Close remaining 20 TCP connections => 10 UDP sessions still keep IP=1 and conns=10.
	for i := 20; i < 40; i++ {
		_ = common.Close(tcpLinks[i].Writer)
	}
	aliveIPs, userConns, connCount = ld.GetConnectionState()
	if len(aliveIPs[42]) != 1 || userConns[42] != 10 || connCount != 10 {
		t.Fatalf("with 10 UDP left got ips=%#v userConns=%d connCount=%d", aliveIPs[42], userConns[42], connCount)
	}

	// Close all UDP sessions => drains to 0 and cleans up map keys (no memory retention).
	for _, l := range udpLinks {
		_ = common.Close(l.Writer)
	}
	aliveIPs, userConns, connCount = ld.GetConnectionState()
	if len(aliveIPs[42]) != 0 || userConns[42] != 0 || connCount != 0 {
		t.Fatalf("after closing all got ips=%#v userConns=%d connCount=%d, want 0", aliveIPs[42], userConns[42], connCount)
	}
	ld.connStatsMu.Lock()
	remainingUserConns := len(ld.userConns)
	remainingUDPIPs := len(ld.udpIPs)
	ld.connStatsMu.Unlock()
	if remainingUserConns != 0 || remainingUDPIPs != 0 {
		t.Fatalf("stale keys retained after drain: userConns=%d udpIPs=%d", remainingUserConns, remainingUDPIPs)
	}
}

type noopBufWriter struct{}

func (noopBufWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	buf.ReleaseMulti(mb)
	return nil
}

func TestLimitDispatcher_ResetConns(t *testing.T) {
	ld := newTestDispatcher()

	ld.mu.Lock()
	ld.limitedIPs["user@1"] = map[string]int{"1.1.1.1": 1}
	ld.mu.Unlock()
	ld.connCount.Store(3)

	ld.ResetConns()

	ld.mu.RLock()
	ipCount := len(ld.limitedIPs)
	ld.mu.RUnlock()
	if ipCount != 0 {
		t.Error("limitedIPs should be empty after reset")
	}

	if ld.connCount.Load() != 0 {
		t.Error("connCount should be 0 after reset")
	}
}

func TestLimitDispatcher_UnlimitedUserFastPath(t *testing.T) {
	ld := newTestDispatcher()

	email := userEmail(1)
	// No device limit set for this user
	ld.UpdateLimits(map[string]int{email: 1}, nil, nil)

	// Should use fast path (sync.Map), no lock needed
	for i := 0; i < 100; i++ {
		ip := "10.0.0." + string(rune('0'+i%10))
		if ld.checkDeviceLimit(email, ip, true) {
			t.Errorf("unlimited user should always be allowed (ip=%s)", ip)
		}
	}

	// Verify IPs are tracked in unlimitedIPs
	aliveIPs, _, _ := ld.GetConnectionState()
	if len(aliveIPs[1]) == 0 {
		t.Error("should have tracked some IPs")
	}
}

// TestLimitDispatcher_UnlimitedTCPHandoverNoIPLoss verifies that rapid concurrent
// close-old / open-new TCP handovers on the same IP for an unlimited user never
// lose the active source IP (regression for the sync.Map LoadOrStore/Delete race).
func TestLimitDispatcher_UnlimitedTCPHandoverNoIPLoss(t *testing.T) {
	ld := newTestDispatcher()
	email := userEmail(99)
	ld.UpdateLimits(map[string]int{email: 99}, nil, nil)
	const ip = "198.51.100.99"

	for round := 0; round < 5; round++ {
		if ld.checkDeviceLimit(email, ip, true) {
			t.Fatalf("round %d: initial TCP rejected", round)
		}
		cur := &transport.Link{Writer: &noopBufWriter{}}
		ld.trackLink(cur, email, ip, true)

		for step := 0; step < 200; step++ {
			prev := cur
			next := &transport.Link{Writer: &noopBufWriter{}}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				_ = common.Close(prev.Writer)
			}()
			go func() {
				defer wg.Done()
				ld.checkDeviceLimit(email, ip, true)
				ld.trackLink(next, email, ip, true)
			}()
			wg.Wait()
			cur = next

			aliveIPs, userConns, _ := ld.GetConnectionState()
			if userConns[99] != 1 {
				t.Fatalf("round %d step %d: userConns=%d, want 1", round, step, userConns[99])
			}
			if !aliveIPs[99][ip] {
				t.Fatalf("round %d step %d: active TCP connection present (userConns=1) but IP %s missing from aliveIPs=%#v", round, step, ip, aliveIPs[99])
			}
		}

		_ = common.Close(cur.Writer)
		aliveIPs, userConns, _ := ld.GetConnectionState()
		if len(aliveIPs[99]) != 0 || userConns[99] != 0 {
			t.Fatalf("round %d: expected clean drain after final close, got ips=%#v conns=%d", round, aliveIPs[99], userConns[99])
		}
	}

	ld.connStatsMu.Lock()
	remUnlimited := len(ld.unlimitedIPs)
	remConns := len(ld.userConns)
	ld.connStatsMu.Unlock()
	if remUnlimited != 0 || remConns != 0 {
		t.Fatalf("expected zero retained keys after drain, got unlimitedIPs=%d userConns=%d", remUnlimited, remConns)
	}
}


func TestLimitDispatcher_TrackLinkPreservesReader(t *testing.T) {
	ld := newTestDispatcher()
	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 1}, nil)

	origReader := nopReader{}
	origWriter := &closeTrackingWriter{Writer: buf.Discard, onClose: func() {}}
	link := &transport.Link{Reader: origReader, Writer: origWriter}

	ld.trackLink(link, email, "1.1.1.1", true)

	if link.Reader != origReader {
		t.Fatal("trackLink must not replace link.Reader")
	}
	if link.Writer == origWriter {
		t.Fatal("trackLink should wrap link.Writer for lifecycle callbacks")
	}
}

func TestLimitDispatcher_CloseTrackingWriterReleasesConn(t *testing.T) {
	ld := newTestDispatcher()
	email := userEmail(1)
	ld.UpdateLimits(map[string]int{email: 1}, map[string]int{email: 1}, nil)
	if ld.checkDeviceLimit(email, "1.1.1.1", true) {
		t.Fatal("first connection should be allowed")
	}

	link := &transport.Link{Reader: nopReader{}, Writer: buf.Discard}
	ld.trackLink(link, email, "1.1.1.1", true)

	if got := ld.connCount.Load(); got != 1 {
		t.Fatalf("expected connCount=1 after tracking, got %d", got)
	}
	cw, ok := link.Writer.(*closeTrackingWriter)
	if !ok {
		t.Fatal("expected closeTrackingWriter wrapper")
	}
	if err := cw.Close(); err != nil {
		t.Fatalf("closeTrackingWriter.Close() error = %v", err)
	}
	if got := ld.connCount.Load(); got != 0 {
		t.Fatalf("expected connCount=0 after close, got %d", got)
	}
	if ld.checkDeviceLimit(email, "2.2.2.2", true) {
		t.Fatal("device slot should be released after writer close")
	}
}
