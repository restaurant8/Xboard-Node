package xray

import (
	"sync/atomic"
	"testing"

	"github.com/cedar2025/xboard-node/internal/panel"
)

func newTestDispatcher() *LimitDispatcher {
	return &LimitDispatcher{
		limitedIPs: make(map[string]map[string]int),
	}
}

func TestLimitDispatcher_DeviceLimitCheck(t *testing.T) {
	ld := newTestDispatcher()

	users := []panel.User{
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

func TestLimitDispatcher_SpeedBucket(t *testing.T) {
	ld := newTestDispatcher()

	email1 := userEmail(1)
	email2 := userEmail(2)

	speedLimits := map[string]int{email1: 10} // 10 Mbps
	ld.UpdateLimits(map[string]int{email1: 1, email2: 2}, nil, speedLimits)

	// User 1 has speed limit — should get a limiter
	limiter := ld.getBucket(email1)
	if limiter == nil {
		t.Error("user with speed limit should get a limiter")
	}

	// Same user should get the same limiter (cached)
	limiter2 := ld.getBucket(email1)
	if limiter != limiter2 {
		t.Error("same user should get cached limiter")
	}

	// User 2 has no speed limit — should get nil
	limiter3 := ld.getBucket(email2)
	if limiter3 != nil {
		t.Error("user without speed limit should get nil")
	}
}

func TestLimitDispatcher_GetUserTraffic(t *testing.T) {
	ld := newTestDispatcher()

	email1 := userEmail(1)
	email2 := userEmail(2)
	ld.UpdateLimits(map[string]int{email1: 1, email2: 2}, nil, nil)

	// Simulate traffic by storing counters directly
	tc1 := &userTrafficCounter{}
	tc1.upload.Store(1000)
	tc1.download.Store(2000)
	ld.userTraffic.Store(email1, tc1)

	tc2 := &userTrafficCounter{}
	tc2.upload.Store(500)
	tc2.download.Store(800)
	ld.userTraffic.Store(email2, tc2)

	// Simulate IPs (use unlimitedIPs for users without device limit)
	ic1 := &ipCounter{}
	ic1.ips.Store("1.1.1.1", &atomic.Int64{})
	ic1.ips.Store("2.2.2.2", &atomic.Int64{})
	ld.unlimitedIPs.Store(email1, ic1)

	ic2 := &ipCounter{}
	ic2.ips.Store("3.3.3.3", &atomic.Int64{})
	ld.unlimitedIPs.Store(email2, ic2)

	ld.connCount.Store(5)

	traffic, aliveIPs, connCount := ld.GetUserTraffic()

	if connCount != 5 {
		t.Errorf("expected connCount=5, got %d", connCount)
	}
	if traffic[1] != [2]int64{1000, 2000} {
		t.Errorf("user 1 traffic: got %v", traffic[1])
	}
	if traffic[2] != [2]int64{500, 800} {
		t.Errorf("user 2 traffic: got %v", traffic[2])
	}
	if len(aliveIPs[1]) != 2 {
		t.Errorf("user 1 IPs: got %d, want 2", len(aliveIPs[1]))
	}
	if len(aliveIPs[2]) != 1 {
		t.Errorf("user 2 IPs: got %d, want 1", len(aliveIPs[2]))
	}
}

func TestLimitDispatcher_ResetConns(t *testing.T) {
	ld := newTestDispatcher()

	// Add some state
	ld.mu.Lock()
	ld.limitedIPs["user@1"] = map[string]int{"1.1.1.1": 1}
	ld.mu.Unlock()
	ld.userTraffic.Store("user@1", &userTrafficCounter{})
	ld.connCount.Store(3)

	ld.ResetConns()

	ld.mu.RLock()
	ipCount := len(ld.limitedIPs)
	ld.mu.RUnlock()
	if ipCount != 0 {
		t.Error("limitedIPs should be empty after reset")
	}

	// Verify userTraffic is cleared
	count := 0
	ld.userTraffic.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Error("userTraffic should be empty after reset")
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
	v, ok := ld.unlimitedIPs.Load(email)
	if !ok {
		t.Error("unlimited user should have entry in unlimitedIPs")
	}
	ic := v.(*ipCounter)
	ips := ic.aliveIPs()
	if len(ips) == 0 {
		t.Error("should have tracked some IPs")
	}
}
