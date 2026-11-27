package tracker

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

// defaultAliveRefreshInterval forces a periodic resend of non-empty alive IPs
// even when the IP set has not changed, keeping panel-side 300s device state
// fresh for long-lived connections.
const (
	defaultAliveRefreshInterval = 120 * time.Second
	dirtyAliveIPsHash           = "__dirty__"
)

// snapshot is an immutable point-in-time view of tracker state.
// It is swapped atomically so readers never block writers.
type snapshot struct {
	traffic   map[int][2]int64        // userID → [upload, download] delta
	aliveIPs  map[int]map[string]bool // userID → set of source IPs
	online    map[int]int             // userID → distinct IP count (legacy)
	userConns map[int]int             // userID → active TCP connections + UDP sessions (nil = unsupported)
	connCount int
	inSpeed   int64
	outSpeed  int64
}

// Tracker computes per-user traffic deltas from cumulative counters
// provided by the kernel, and accumulates totals for panel reporting.
//
// Architecture: the kernel maintains per-user atomic counters and IP sets.
// Each tick, the service calls Process() with cumulative per-user traffic.
// Tracker computes deltas against the previous cycle's values — O(users),
// not O(connections).
//
// Thread safety: Process() acquires mu to update internal state, then
// atomically publishes a new snapshot. All read methods (Flush*,
// CurrentOnline, LogStats, *Speed) read the snapshot lock-free.
// This eliminates contention between the 10s Process tick and the 60s
// flush/push tick.
type Tracker struct {
	mu sync.Mutex

	// lastSeen stores the cumulative traffic from the previous Process() call.
	// Protected by mu — only written by Process.
	lastSeen map[int][2]int64 // userID → [upload, download] cumulative

	// pending accumulates deltas between flushes.
	// Protected by mu — written by Process, drained by FlushTraffic.
	pendingTraffic map[int][2]int64

	// live holds the current snapshot, swapped atomically.
	// Readers load this pointer without any lock.
	live atomic.Pointer[snapshot]

	// aliveIPsBuf is a reusable buffer for FlushAliveIPs output.
	// Avoids allocating a new map+slice every 60s.
	aliveIPsBuf map[int][]string

	// lastAliveIPsHash detects changes to avoid duplicate reports.
	lastAliveIPsHash string

	// lastAliveFlushAt tracks when alive IPs were last emitted so non-empty
	// snapshots are periodically re-sent before panel-side TTL (300s) expires.
	lastAliveFlushAt     time.Time
	aliveRefreshInterval time.Duration
	nowFunc              func() time.Time

	// intervalSec is the Process() tick interval in seconds, used to convert
	// per-cycle byte deltas into bytes/second. Defaults to 10; override with
	// SetIntervalSeconds to match the configured track_interval.
	intervalSec int64
}

func New() *Tracker {
	t := &Tracker{
		lastSeen:             make(map[int][2]int64),
		pendingTraffic:       make(map[int][2]int64),
		aliveIPsBuf:          make(map[int][]string),
		aliveRefreshInterval: defaultAliveRefreshInterval,
		nowFunc:              time.Now,
		intervalSec:          10,
	}
	// Publish initial empty snapshot.
	t.live.Store(&snapshot{
		traffic:  make(map[int][2]int64),
		aliveIPs: make(map[int]map[string]bool),
		online:   make(map[int]int),
	})
	return t
}

// SetIntervalSeconds sets the Process() tick interval (seconds) used to derive
// speed (bytes/second) from per-cycle byte deltas. Values <= 0 are ignored so
// the default of 10s remains in effect.
func (t *Tracker) SetIntervalSeconds(sec int) {
	if sec <= 0 {
		return
	}
	atomic.StoreInt64(&t.intervalSec, int64(sec))
}

// Process computes per-user traffic deltas from cumulative kernel counters.
// Also stores alive IPs and connection count. O(users).
//
// Kept for callers/tests that do not supply per-user concurrency counts;
// production callers should use ProcessWithUserConns.
func (t *Tracker) Process(
	cumTraffic map[int][2]int64,
	kernelAliveIPs map[int]map[string]bool,
	connCount int,
) {
	t.ProcessWithUserConns(cumTraffic, kernelAliveIPs, nil, connCount)
}

// ProcessWithUserConns computes per-user traffic deltas and atomically
// publishes alive IPs, legacy distinct-IP online counts, and per-user
// active TCP+UDP connection counts.
func (t *Tracker) ProcessWithUserConns(
	cumTraffic map[int][2]int64,
	kernelAliveIPs map[int]map[string]bool,
	kernelUserConns map[int]int,
	connCount int,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var cycleIn, cycleOut int64

	for uid, cum := range cumTraffic {
		prev := t.lastSeen[uid]
		deltaUp := cum[0] - prev[0]
		deltaDown := cum[1] - prev[1]

		// Guard against counter reset (kernel restart).
		if deltaUp < 0 {
			deltaUp = cum[0]
		}
		if deltaDown < 0 {
			deltaDown = cum[1]
		}

		t.lastSeen[uid] = cum

		if deltaUp > 0 || deltaDown > 0 {
			cur := t.pendingTraffic[uid]
			cur[0] += deltaUp
			cur[1] += deltaDown
			t.pendingTraffic[uid] = cur

			cycleOut += deltaUp
			cycleIn += deltaDown
		}
	}

	// Prune bookkeeping for users the kernel no longer reports (removed users).
	// This is safe: a still-active user always reports a non-zero cumulative and
	// therefore stays in cumTraffic, so it is never pruned here; a user that is
	// removed and later re-added gets a fresh kernel counter, and the
	// negative-delta reset guard above handles the restart-from-zero case.
	if len(t.lastSeen) > len(cumTraffic) {
		for uid := range t.lastSeen {
			if _, ok := cumTraffic[uid]; !ok {
				delete(t.lastSeen, uid)
			}
		}
	}

	// Compute legacy online from alive IPs (distinct IP count).
	online := make(map[int]int, len(kernelAliveIPs))
	for uid, ips := range kernelAliveIPs {
		online[uid] = len(ips)
	}

	// Preserve non-nil empty map when kernel explicitly reported 0 active user connections
	// so the panel can immediately zero out the node instance instead of waiting for TTL.
	var userConns map[int]int
	if kernelUserConns != nil {
		userConns = make(map[int]int, len(kernelUserConns))
		for uid, count := range kernelUserConns {
			if count > 0 {
				userConns[uid] = count
			}
		}
	}

	// Publish new snapshot (readers will see this atomically).
	t.live.Store(&snapshot{
		traffic:   copyTrafficMap(t.pendingTraffic),
		aliveIPs:  kernelAliveIPs, // kernel provides fresh copy each tick
		online:    online,
		userConns: userConns,
		connCount: connCount,
		inSpeed:   cycleIn,
		outSpeed:  cycleOut,
	})
}

// FlushTraffic returns accumulated per-user traffic and resets the pending buffer.
// Lock-free: reads from live snapshot, then acquires mu only to drain.
func (t *Tracker) FlushTraffic() map[int][2]int64 {
	t.mu.Lock()
	data := t.pendingTraffic
	t.pendingTraffic = make(map[int][2]int64, len(data))
	t.mu.Unlock()
	return data
}

// RestoreTraffic adds traffic back (used when push to panel fails).
func (t *Tracker) RestoreTraffic(data map[int][2]int64) {
	t.mu.Lock()
	for uid, d := range data {
		cur := t.pendingTraffic[uid]
		cur[0] += d[0]
		cur[1] += d[1]
		t.pendingTraffic[uid] = cur
	}
	t.mu.Unlock()
}

// HasTraffic returns true if there is accumulated traffic to report.
func (t *Tracker) HasTraffic() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pendingTraffic) > 0
}

// FlushAliveIPs returns per-user alive IPs.
// Reuses internal buffer. Returns nil when unchanged, except that non-empty
// active IP sets are periodically re-emitted (every 120s by default) so
// long-lived connections do not expire from the panel's 300s device cache.
func (t *Tracker) FlushAliveIPs() map[int][]string {
	s := t.live.Load()

	t.mu.Lock()
	defer t.mu.Unlock()

	nowFn := t.nowFunc
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	refreshInterval := t.aliveRefreshInterval
	if refreshInterval <= 0 {
		refreshInterval = defaultAliveRefreshInterval
	}

	// Calculate hash of current aliveIPs
	currentHash := calcAliveIPsHash(s.aliveIPs)

	// If no changes, only emit when non-empty and refresh interval has elapsed.
	if currentHash == t.lastAliveIPsHash {
		if len(s.aliveIPs) == 0 || t.lastAliveFlushAt.IsZero() || now.Sub(t.lastAliveFlushAt) < refreshInterval {
			return nil
		}
	}

	t.lastAliveIPsHash = currentHash
	t.lastAliveFlushAt = now

	// Clear old buffer entries.
	for k := range t.aliveIPsBuf {
		delete(t.aliveIPsBuf, k)
	}

	// Fill buffer from snapshot.
	for uid, ips := range s.aliveIPs {
		buf := t.aliveIPsBuf[uid]
		if buf == nil {
			buf = make([]string, 0, len(ips))
		}
		buf = buf[:0]
		for ip := range ips {
			buf = append(buf, ip)
		}
		t.aliveIPsBuf[uid] = buf
	}

	// Return a deep copy rather than the internal buffer. The caller may hand
	// this map to a background goroutine (the report push), and a subsequent
	// FlushAliveIPs/RestoreAliveIPs call on the main goroutine would otherwise
	// mutate the same maps/slices concurrently — a data race.
	out := make(map[int][]string, len(t.aliveIPsBuf))
	for uid, ips := range t.aliveIPsBuf {
		cp := make([]string, len(ips))
		copy(cp, ips)
		out[uid] = cp
	}
	return out
}

// calcAliveIPsHash computes a deterministic hash for change detection.
func calcAliveIPsHash(aliveIPs map[int]map[string]bool) string {
	if len(aliveIPs) == 0 {
		return ""
	}

	h := sha256.New()

	// Sort user IDs for consistent hashing
	userIDs := make([]int, 0, len(aliveIPs))
	for uid := range aliveIPs {
		userIDs = append(userIDs, uid)
	}
	sort.Ints(userIDs)

	for _, uid := range userIDs {
		ips := aliveIPs[uid]
		// Sort IPs for consistent hashing
		ipList := make([]string, 0, len(ips))
		for ip := range ips {
			ipList = append(ipList, ip)
		}
		sort.Strings(ipList)

		// Write user ID and IPs to hash
		h.Write([]byte{byte(uid >> 24), byte(uid >> 16), byte(uid >> 8), byte(uid)})
		for _, ip := range ipList {
			h.Write([]byte(ip))
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}

// CurrentOnline returns a snapshot copy of user_id → device count (distinct IPs).
// Lock-free: reads from live snapshot.
func (t *Tracker) CurrentOnline() map[int]int {
	s := t.live.Load()
	cp := make(map[int]int, len(s.online))
	for k, v := range s.online {
		cp[k] = v
	}
	return cp
}

// CurrentUserConns returns a snapshot copy of user_id → active TCP+UDP connections.
// Returns nil when the underlying kernel has not produced a per-user connection
// snapshot; returns a non-nil empty map when all connections have drained to zero.
func (t *Tracker) CurrentUserConns() map[int]int {
	s := t.live.Load()
	if s.userConns == nil {
		return nil
	}
	cp := make(map[int]int, len(s.userConns))
	for k, v := range s.userConns {
		if v > 0 {
			cp[k] = v
		}
	}
	return cp
}

// MarkAliveDirty forces the next FlushAliveIPs call to emit the current
// alive IP snapshot (including an empty drain map) even if s.aliveIPs has not
// changed since the failed attempt.
func (t *Tracker) MarkAliveDirty() {
	t.mu.Lock()
	t.lastAliveIPsHash = dirtyAliveIPsHash
	t.mu.Unlock()
}

// RestoreAliveIPs merges alive IPs back in and marks the alive snapshot dirty
// so the next report / WS flush retries sending the latest state (including an
// empty drain map when all connections have closed).
func (t *Tracker) RestoreAliveIPs(data map[int][]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastAliveIPsHash = dirtyAliveIPsHash
	for uid, ipList := range data {
		ips := t.aliveIPsBuf[uid]
		if ips == nil {
			ips = make([]string, 0, len(ipList))
		}
		// Use map for O(n) dedup instead of O(n²) linear search
		existMap := make(map[string]struct{}, len(ips)+len(ipList))
		for _, existing := range ips {
			existMap[existing] = struct{}{}
		}
		for _, ip := range ipList {
			if _, exists := existMap[ip]; !exists {
				ips = append(ips, ip)
				existMap[ip] = struct{}{}
			}
		}
		t.aliveIPsBuf[uid] = ips
	}
}

// LogStats logs current tracking statistics.
// Lock-free: reads from live snapshot.
func (t *Tracker) LogStats() {
	s := t.live.Load()
	nlog.TrackerStats(s.connCount, len(s.online))
}

// ActiveConnections returns the last observed active connection count.
// Lock-free: reads from live snapshot.
func (t *Tracker) ActiveConnections() int {
	return t.live.Load().connCount
}

// TotalConnections is deprecated — no longer tracked per-connection.
// Returns 0 for backward compatibility.
func (t *Tracker) TotalConnections() int64 {
	return 0
}

// InboundSpeed returns the last observed inbound (download) speed in bytes/second.
// Lock-free: reads from live snapshot.
func (t *Tracker) InboundSpeed() int64 {
	return t.live.Load().inSpeed / t.intervalSeconds()
}

// OutboundSpeed returns the last observed outbound (upload) speed in bytes/second.
// Lock-free: reads from live snapshot.
func (t *Tracker) OutboundSpeed() int64 {
	return t.live.Load().outSpeed / t.intervalSeconds()
}

// intervalSeconds returns the configured Process() tick interval, never 0.
func (t *Tracker) intervalSeconds() int64 {
	if sec := atomic.LoadInt64(&t.intervalSec); sec > 0 {
		return sec
	}
	return 10
}

// copyTrafficMap creates a shallow copy of the traffic map.
func copyTrafficMap(src map[int][2]int64) map[int][2]int64 {
	dst := make(map[int][2]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
