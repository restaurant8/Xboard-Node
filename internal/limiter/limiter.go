package limiter

import (
	"fmt"
	"log/slog"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/panel"
)

// KickAction represents a connection that should be closed.
type KickAction struct {
	ConnID string
	UserID int
	Reason string
}

// Limiter enforces per-user device limits and detects removed users.
type Limiter struct {
	mu    sync.RWMutex
	users map[int]panel.User

	deviceLimitEvents atomic.Uint64
}

func New() *Limiter {
	return &Limiter{
		users: make(map[int]panel.User),
	}
}

// UpdateUsers refreshes the user limit configuration and returns
// the IDs of users that were present before but are now removed.
func (l *Limiter) UpdateUsers(users []panel.User) []int {
	l.mu.Lock()
	defer l.mu.Unlock()

	newUsers := make(map[int]panel.User, len(users))
	for _, u := range users {
		newUsers[u.ID] = u
	}

	// Find removed users
	var removed []int
	for id := range l.users {
		if _, ok := newUsers[id]; !ok {
			removed = append(removed, id)
		}
	}

	l.users = newUsers
	return removed
}

// Check inspects connections against device limits.
// Returns a list of connections that should be closed.
func (l *Limiter) Check(conns []kernel.Connection) []KickAction {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var kicks []KickAction

	// Device limit check
	userIPs := make(map[int]map[string]bool)
	userConnsByIP := make(map[int]map[string][]string) // user_id -> ip -> []conn_ids
	for _, conn := range conns {
		if conn.UserID == 0 || conn.SourceIP == "" {
			continue
		}
		if userIPs[conn.UserID] == nil {
			userIPs[conn.UserID] = make(map[string]bool)
			userConnsByIP[conn.UserID] = make(map[string][]string)
		}
		userIPs[conn.UserID][conn.SourceIP] = true
		userConnsByIP[conn.UserID][conn.SourceIP] = append(
			userConnsByIP[conn.UserID][conn.SourceIP], conn.ID,
		)
	}

	for uid, ips := range userIPs {
		user, ok := l.users[uid]
		if !ok || user.DeviceLimit == 0 {
			continue
		}
		if len(ips) <= user.DeviceLimit {
			continue
		}

		// Count a device-limit violation event for metrics reporting.
		l.deviceLimitEvents.Add(1)

		slog.Warn("user exceeded device limit",
			"user_id", uid,
			"devices", len(ips),
			"limit", user.DeviceLimit,
		)

		// Sort IPs by numeric address so the "allowed" set is deterministic
		// across calls. netip.Addr comparison avoids the lexicographic ordering
		// quirks of string sort (e.g. "10.x" < "192.x", IPv6 always last).
		// Parse all addresses up front to avoid O(n log n) repeated parsing.
		type parsedIP struct {
			raw  string
			addr netip.Addr
		}
		ipList := make([]parsedIP, 0, len(ips))
		for ip := range ips {
			a, _ := netip.ParseAddr(ip)
			ipList = append(ipList, parsedIP{raw: ip, addr: a})
		}
		sort.Slice(ipList, func(i, j int) bool {
			return ipList[i].addr.Compare(ipList[j].addr) < 0
		})

		allowed := make(map[string]bool, user.DeviceLimit)
		for i := 0; i < user.DeviceLimit && i < len(ipList); i++ {
			allowed[ipList[i].raw] = true
		}

		for ip, connIDs := range userConnsByIP[uid] {
			if allowed[ip] {
				continue
			}
			for _, connID := range connIDs {
				kicks = append(kicks, KickAction{
					ConnID: connID,
					UserID: uid,
					Reason: fmt.Sprintf("devices %d > limit %d", len(ips), user.DeviceLimit),
				})
			}
		}
	}

	return kicks
}

// LimiterMetrics holds aggregated limiter statistics.
type LimiterMetrics struct {
	// DeviceLimitEvents is the total number of times a user exceeded
	// the configured device limit (since process start).
	DeviceLimitEvents uint64
}

// SnapshotMetrics returns a snapshot of limiter metrics.
func (l *Limiter) SnapshotMetrics() LimiterMetrics {
	return LimiterMetrics{
		DeviceLimitEvents: l.deviceLimitEvents.Load(),
	}
}

// KickUsers returns KickActions for all connections belonging to the given user IDs.
func (l *Limiter) KickUsers(conns []kernel.Connection, userIDs []int) []KickAction {
	if len(userIDs) == 0 {
		return nil
	}

	remove := make(map[int]bool, len(userIDs))
	for _, id := range userIDs {
		remove[id] = true
	}

	var kicks []KickAction
	for _, conn := range conns {
		if remove[conn.UserID] {
			kicks = append(kicks, KickAction{
				ConnID: conn.ID,
				UserID: conn.UserID,
				Reason: "user removed from panel",
			})
		}
	}
	return kicks
}
