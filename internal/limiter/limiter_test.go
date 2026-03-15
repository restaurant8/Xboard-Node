package limiter

import (
	"sort"
	"testing"

	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/panel"
)

func TestNew(t *testing.T) {
	l := New()
	if l == nil {
		t.Fatal("New returned nil")
	}
}

func TestUpdateUsers(t *testing.T) {
	l := New()
	users := []panel.User{
		{ID: 1, UUID: "u1", SpeedLimit: 3, DeviceLimit: 2},
		{ID: 2, UUID: "u2", SpeedLimit: 0, DeviceLimit: 0},
	}
	removed := l.UpdateUsers(users)

	if len(removed) != 0 {
		t.Errorf("first update should have no removed users, got %v", removed)
	}

	// Verify by running a check
	conns := []kernel.Connection{{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"}}
	kicks := l.Check(conns)
	if len(kicks) != 0 {
		t.Errorf("expected no kicks, got %d", len(kicks))
	}
}

func TestUpdateUsers_DetectsRemoved(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, UUID: "u1"},
		{ID: 2, UUID: "u2"},
		{ID: 3, UUID: "u3"},
	})

	// Remove user 2
	removed := l.UpdateUsers([]panel.User{
		{ID: 1, UUID: "u1"},
		{ID: 3, UUID: "u3"},
	})

	if len(removed) != 1 || removed[0] != 2 {
		t.Errorf("expected removed=[2], got %v", removed)
	}
}

func TestUpdateUsers_DetectsMultipleRemoved(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, UUID: "u1"},
		{ID: 2, UUID: "u2"},
		{ID: 3, UUID: "u3"},
	})

	// Remove users 1 and 3
	removed := l.UpdateUsers([]panel.User{
		{ID: 2, UUID: "u2"},
	})

	sort.Ints(removed)
	if len(removed) != 2 || removed[0] != 1 || removed[1] != 3 {
		t.Errorf("expected removed=[1,3], got %v", removed)
	}
}

func TestUpdateUsers_NoRemovals(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, UUID: "u1"},
	})

	// Add user, keep existing
	removed := l.UpdateUsers([]panel.User{
		{ID: 1, UUID: "u1"},
		{ID: 2, UUID: "u2"},
	})

	if len(removed) != 0 {
		t.Errorf("expected no removals, got %v", removed)
	}
}

func TestUpdateUsers_ReplacesAll(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{{ID: 1, DeviceLimit: 1}})

	// Replace with different users
	removed := l.UpdateUsers([]panel.User{{ID: 2, DeviceLimit: 1}})

	if len(removed) != 1 || removed[0] != 1 {
		t.Errorf("expected removed=[1], got %v", removed)
	}
}

func TestDeviceLimit_NoKick_UnderLimit(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, DeviceLimit: 3},
	})

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c2", UserID: 1, SourceIP: "2.2.2.2"},
	}
	kicks := l.Check(conns)

	if len(kicks) != 0 {
		t.Errorf("2 devices under limit 3, got %d kicks", len(kicks))
	}
}

func TestDeviceLimit_Kick_OverLimit(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, DeviceLimit: 2},
	})

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c2", UserID: 1, SourceIP: "2.2.2.2"},
		{ID: "c3", UserID: 1, SourceIP: "3.3.3.3"},
	}
	kicks := l.Check(conns)

	if len(kicks) == 0 {
		t.Fatal("expected kicks for exceeding device limit")
	}
	if len(kicks) < 1 {
		t.Errorf("expected at least 1 kick, got %d", len(kicks))
	}
}

func TestDeviceLimit_ExactlyAtLimit(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, DeviceLimit: 2},
	})

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c2", UserID: 1, SourceIP: "2.2.2.2"},
	}
	kicks := l.Check(conns)

	if len(kicks) != 0 {
		t.Errorf("exactly at limit should not kick, got %d", len(kicks))
	}
}

func TestDeviceLimit_UnlimitedDevices(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, DeviceLimit: 0}, // 0 = unlimited
	})

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c2", UserID: 1, SourceIP: "2.2.2.2"},
		{ID: "c3", UserID: 1, SourceIP: "3.3.3.3"},
		{ID: "c4", UserID: 1, SourceIP: "4.4.4.4"},
		{ID: "c5", UserID: 1, SourceIP: "5.5.5.5"},
	}
	kicks := l.Check(conns)

	if len(kicks) != 0 {
		t.Errorf("unlimited devices should not be kicked, got %d", len(kicks))
	}
}

func TestDeviceLimit_SameIPMultipleConnections(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, DeviceLimit: 1},
	})

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c2", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c3", UserID: 1, SourceIP: "1.1.1.1"},
	}
	kicks := l.Check(conns)

	if len(kicks) != 0 {
		t.Errorf("same IP multiple conns should count as 1 device, got %d kicks", len(kicks))
	}
}

func TestDeviceLimit_EmptySourceIP(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, DeviceLimit: 1},
	})

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: ""},
		{ID: "c2", UserID: 1, SourceIP: ""},
	}
	kicks := l.Check(conns)

	if len(kicks) != 0 {
		t.Errorf("empty source IP should be ignored, got %d kicks", len(kicks))
	}
}

func TestMultipleUsers_DeviceLimits(t *testing.T) {
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, DeviceLimit: 1},
		{ID: 2, DeviceLimit: 1},
	})

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c2", UserID: 2, SourceIP: "2.2.2.2"},
		{ID: "c3", UserID: 2, SourceIP: "3.3.3.3"},
	}
	kicks := l.Check(conns)

	deviceKicks := 0
	for _, k := range kicks {
		if k.UserID == 2 {
			deviceKicks++
		}
	}
	if deviceKicks == 0 {
		t.Error("expected device kick for user 2")
	}

	for _, k := range kicks {
		if k.UserID == 1 {
			t.Error("user 1 should not be kicked")
		}
	}
}

func TestNoSpeedKick(t *testing.T) {
	// Speed limits should NOT cause kicks — only device limits and user removal
	l := New()
	l.UpdateUsers([]panel.User{
		{ID: 1, SpeedLimit: 1, DeviceLimit: 0},
	})

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
	}
	kicks := l.Check(conns)
	if len(kicks) != 0 {
		t.Errorf("speed limits should not cause kicks, got %d", len(kicks))
	}
}

func TestKickUsers_Basic(t *testing.T) {
	l := New()

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c2", UserID: 2, SourceIP: "2.2.2.2"},
		{ID: "c3", UserID: 1, SourceIP: "1.1.1.2"},
		{ID: "c4", UserID: 3, SourceIP: "3.3.3.3"},
	}

	kicks := l.KickUsers(conns, []int{1})

	if len(kicks) != 2 {
		t.Fatalf("expected 2 kicks for user 1, got %d", len(kicks))
	}
	for _, k := range kicks {
		if k.UserID != 1 {
			t.Errorf("expected kick for user 1, got user %d", k.UserID)
		}
		if k.Reason != "user removed from panel" {
			t.Errorf("unexpected reason: %s", k.Reason)
		}
	}
}

func TestKickUsers_MultipleUsers(t *testing.T) {
	l := New()

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
		{ID: "c2", UserID: 2, SourceIP: "2.2.2.2"},
		{ID: "c3", UserID: 3, SourceIP: "3.3.3.3"},
	}

	kicks := l.KickUsers(conns, []int{1, 3})

	if len(kicks) != 2 {
		t.Fatalf("expected 2 kicks, got %d", len(kicks))
	}
	kickedUsers := map[int]bool{}
	for _, k := range kicks {
		kickedUsers[k.UserID] = true
	}
	if !kickedUsers[1] || !kickedUsers[3] {
		t.Errorf("expected users 1 and 3 kicked, got %v", kickedUsers)
	}
}

func TestKickUsers_NoUsers(t *testing.T) {
	l := New()

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
	}

	kicks := l.KickUsers(conns, nil)
	if len(kicks) != 0 {
		t.Errorf("expected no kicks for nil user list, got %d", len(kicks))
	}

	kicks = l.KickUsers(conns, []int{})
	if len(kicks) != 0 {
		t.Errorf("expected no kicks for empty user list, got %d", len(kicks))
	}
}

func TestKickUsers_NoMatchingConnections(t *testing.T) {
	l := New()

	conns := []kernel.Connection{
		{ID: "c1", UserID: 1, SourceIP: "1.1.1.1"},
	}

	kicks := l.KickUsers(conns, []int{99})
	if len(kicks) != 0 {
		t.Errorf("expected no kicks for non-existent user, got %d", len(kicks))
	}
}
