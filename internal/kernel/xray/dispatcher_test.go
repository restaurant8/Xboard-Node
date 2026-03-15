package xray

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/panel"
)

func TestLimitDispatcher_DeviceLimitCheck(t *testing.T) {
	ld := &LimitDispatcher{
		conns:   make(map[string]*dispatchedConn),
		userIPs: make(map[string]map[string]int),
	}

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
	ld := &LimitDispatcher{
		conns:   make(map[string]*dispatchedConn),
		userIPs: make(map[string]map[string]int),
	}

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
	ld := &LimitDispatcher{
		conns:   make(map[string]*dispatchedConn),
		userIPs: make(map[string]map[string]int),
	}

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

func TestLimitDispatcher_Snapshot(t *testing.T) {
	ld := &LimitDispatcher{
		conns:   make(map[string]*dispatchedConn),
		userIPs: make(map[string]map[string]int),
	}

	// Add some connections
	ld.conns["conn-1"] = &dispatchedConn{
		id: "conn-1", email: "user@1", sourceIP: "1.1.1.1", userID: 1,
	}
	ld.conns["conn-2"] = &dispatchedConn{
		id: "conn-2", email: "user@2", sourceIP: "2.2.2.2", userID: 2,
	}
	ld.conns["conn-1"].upload.Store(1000)
	ld.conns["conn-1"].download.Store(2000)

	snapshot := ld.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 connections, got %d", len(snapshot))
	}

	found := false
	for _, c := range snapshot {
		if c.ID == "conn-1" {
			found = true
			if c.UserID != 1 || c.SourceIP != "1.1.1.1" || c.Upload != 1000 || c.Download != 2000 {
				t.Errorf("unexpected connection data: %+v", c)
			}
		}
	}
	if !found {
		t.Error("conn-1 not found in snapshot")
	}
}

func TestLimitDispatcher_CloseConn(t *testing.T) {
	ld := &LimitDispatcher{
		conns:   make(map[string]*dispatchedConn),
		userIPs: make(map[string]map[string]int),
	}

	email := userEmail(1)
	ld.userIPs[email] = map[string]int{"1.1.1.1": 1}
	ld.conns["conn-1"] = &dispatchedConn{
		id: "conn-1", email: email, sourceIP: "1.1.1.1", userID: 1,
	}

	ok := ld.CloseConn("conn-1")
	if !ok {
		t.Error("CloseConn should return true for existing connection")
	}

	if len(ld.conns) != 0 {
		t.Error("connection should be removed after close")
	}

	ok = ld.CloseConn("nonexistent")
	if ok {
		t.Error("CloseConn should return false for nonexistent connection")
	}
}

func TestLimitDispatcher_ResetConns(t *testing.T) {
	ld := &LimitDispatcher{
		conns:   make(map[string]*dispatchedConn),
		userIPs: make(map[string]map[string]int),
	}
	ld.conns["c1"] = &dispatchedConn{id: "c1"}
	ld.userIPs["user@1"] = map[string]int{"1.1.1.1": 1}

	ld.ResetConns()

	if len(ld.conns) != 0 {
		t.Error("conns should be empty after reset")
	}
	if len(ld.userIPs) != 0 {
		t.Error("userIPs should be empty after reset")
	}
}
