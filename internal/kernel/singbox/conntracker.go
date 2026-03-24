package singbox

import (
	"context"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	singM "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/time/rate"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

const rateLimitThreshold = 64 * 1024

// ─── Per-user statistics ────────────────────────────────────────────────────

// userStats holds per-user traffic counters and alive IP tracking.
// Traffic counters are lock-free atomics (read on every packet).
// IP tracking uses a lightweight mutex (only touched at connect/disconnect).
type userStats struct {
	upload   atomic.Int64
	download atomic.Int64

	mu        sync.Mutex
	ips       map[string]int // sourceIP → refcount (number of active conns from that IP)
	connCount int            // total active connections
}

// addConn registers a new connection from sourceIP.
func (u *userStats) addConn(sourceIP string) {
	u.mu.Lock()
	u.connCount++
	u.ips[sourceIP]++
	u.mu.Unlock()
}

// removeConn unregisters a connection from sourceIP.
func (u *userStats) removeConn(sourceIP string) {
	u.mu.Lock()
	u.connCount--
	u.ips[sourceIP]--
	if u.ips[sourceIP] <= 0 {
		delete(u.ips, sourceIP)
	}
	u.mu.Unlock()
}

// distinctIPs returns the number of distinct IPs currently connected.
func (u *userStats) distinctIPs() int {
	u.mu.Lock()
	n := len(u.ips)
	u.mu.Unlock()
	return n
}

// ipSnapshot returns a copy of the current IP set (for device gate-keeping).
func (u *userStats) ipSnapshot() map[string]struct{} {
	u.mu.Lock()
	snapshot := make(map[string]struct{}, len(u.ips))
	for ip := range u.ips {
		snapshot[ip] = struct{}{}
	}
	u.mu.Unlock()
	return snapshot
}

// aliveIPList returns the set of alive IPs as a boolean map (for reporting).
func (u *userStats) aliveIPList() map[string]bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.ips) == 0 {
		return nil
	}
	result := make(map[string]bool, len(u.ips))
	for ip := range u.ips {
		result[ip] = true
	}
	return result
}

// ─── ConnTracker ────────────────────────────────────────────────────────────

// ConnTracker provides per-user traffic counting, alive IP tracking, device
// limit gate-keeping, and per-user speed limiting for the sing-box kernel.
//
// Architecture: instead of maintaining a per-connection map (O(connections)),
// it maintains per-user atomic counters and IP refcount sets (O(users)).
// This means:
//   - GetUserTraffic is O(users), not O(connections)
//   - No Snapshot/pending buffer needed
//   - No per-connection map lock contention
//   - Bytes are counted directly into per-user atomics via CountFunc callbacks
//
// The per-connection wrapper (trackedConn) is still needed for:
//   - Hooking into sing's ReadCounter/WriteCounter for zero-copy byte counting
//   - Close callback to decrement IP refcounts
//   - Per-connection rate limit token accumulation (amortized WaitN)
type ConnTracker struct {
	usersMu sync.RWMutex
	users   map[int]*userStats  // userID → stats
	uuidMap map[string]int      // UUID → userID (for lookup in RoutedConnection)
	connMap map[string]net.Conn // connID → conn (only for force-close support)

	idCounter atomic.Int64

	// speedLimitFunc resolves a user UUID to a *rate.Limiter.
	speedLimitFunc atomic.Pointer[func(uuid string) *rate.Limiter]

	// deviceLimitFunc resolves a user UUID to their device limit.
	deviceLimitFunc atomic.Pointer[func(uuid string) (int, bool)]
}

// NewConnTracker creates a tracker.
func NewConnTracker(_ int) *ConnTracker {
	return &ConnTracker{
		users:   make(map[int]*userStats),
		uuidMap: make(map[string]int),
		connMap: make(map[string]net.Conn),
	}
}

// SetSpeedLimitFunc configures the per-user speed limit lookup.
func (t *ConnTracker) SetSpeedLimitFunc(fn func(uuid string) *rate.Limiter) {
	t.speedLimitFunc.Store(&fn)
}

// SetDeviceLimitFunc configures the per-user device limit lookup for gate-keeping.
func (t *ConnTracker) SetDeviceLimitFunc(fn func(uuid string) (int, bool)) {
	t.deviceLimitFunc.Store(&fn)
}

// SetUserMap replaces the UUID→userID mapping and ensures per-user stats
// structs exist for all users. Old users that are no longer present keep
// their stats until their connections drain.
func (t *ConnTracker) SetUserMap(m map[string]int) {
	t.usersMu.Lock()
	t.uuidMap = m
	for _, uid := range m {
		if _, ok := t.users[uid]; !ok {
			t.users[uid] = &userStats{ips: make(map[string]int)}
		}
	}
	t.usersMu.Unlock()
}

// ─── adapter.ConnectionTracker ──────────────────────────────────────────────

// RoutedConnection wraps a TCP conn to count bytes per-user, track IPs,
// and optionally rate-limit. Gate-keeps device limits at connection time.
func (t *ConnTracker) RoutedConnection(
	ctx context.Context, conn net.Conn,
	metadata adapter.InboundContext,
	_ adapter.Rule, _ adapter.Outbound,
) net.Conn {
	uuid := metadata.User
	sourceIP := metadata.Source.Addr.String()

	t.usersMu.RLock()
	uid := t.uuidMap[uuid]
	us := t.users[uid]
	t.usersMu.RUnlock()

	// Gate-keep device limit
	if dlf := t.deviceLimitFunc.Load(); dlf != nil {
		if limit, hasLimit := (*dlf)(uuid); hasLimit {
			if t.checkDeviceGate(us, sourceIP, limit) {
				nlog.Core().Info("singbox: device limit gate-keep, rejecting connection",
					"user", uuid, "ip", sourceIP, "limit", limit)
				conn.Close()
				return conn
			}
		}
	}

	// Register connection
	if us != nil {
		us.addConn(sourceIP)
	}

	connID := t.nextID()

	// Store conn reference for force-close support
	t.usersMu.Lock()
	t.connMap[connID] = conn
	t.usersMu.Unlock()

	var lim *rate.Limiter
	if slf := t.speedLimitFunc.Load(); slf != nil {
		lim = (*slf)(uuid)
	}

	return &trackedConn{
		Conn:     conn,
		tracker:  t,
		us:       us,
		connID:   connID,
		sourceIP: sourceIP,
		limiter:  lim,
		ctx:      ctx,
	}
}

// RoutedPacketConnection wraps UDP with per-user counting (UDP not in connMap).
func (t *ConnTracker) RoutedPacketConnection(
	ctx context.Context, conn N.PacketConn,
	metadata adapter.InboundContext,
	_ adapter.Rule, _ adapter.Outbound,
) N.PacketConn {
	uuid := metadata.User
	sourceIP := metadata.Source.Addr.String()

	t.usersMu.RLock()
	uid := t.uuidMap[uuid]
	us := t.users[uid]
	t.usersMu.RUnlock()

	if dlf := t.deviceLimitFunc.Load(); dlf != nil {
		if limit, hasLimit := (*dlf)(uuid); hasLimit {
			if t.checkDeviceGate(us, sourceIP, limit) {
				nlog.Core().Info("singbox: device limit gate-keep, rejecting UDP connection",
					"user", uuid, "ip", sourceIP, "limit", limit)
				conn.Close()
				return conn
			}
		}
	}

	if us != nil {
		us.addConn(sourceIP)
	}

	connID := t.nextID()

	var lim *rate.Limiter
	if slf := t.speedLimitFunc.Load(); slf != nil {
		lim = (*slf)(uuid)
	}

	return &trackedPacketConn{
		PacketConn: conn,
		tracker:    t,
		us:         us,
		connID:     connID,
		sourceIP:   sourceIP,
		limiter:    lim,
		ctx:        ctx,
	}
}

// checkDeviceGate returns true if the connection should be rejected.
// Uses the deterministic lowest-IP algorithm.
func (t *ConnTracker) checkDeviceGate(us *userStats, sourceIP string, limit int) bool {
	if us == nil {
		return false
	}

	knownIPs := us.ipSnapshot()

	// Already known IP — always allow
	if _, exists := knownIPs[sourceIP]; exists {
		return false
	}

	// Under limit — allow
	if len(knownIPs) < limit {
		return false
	}

	// Over limit — deterministic: allow lowest IPs lexicographically
	ipList := make([]string, 0, len(knownIPs)+1)
	for ip := range knownIPs {
		ipList = append(ipList, ip)
	}
	ipList = append(ipList, sourceIP)
	sort.Strings(ipList)

	for i := 0; i < limit && i < len(ipList); i++ {
		if ipList[i] == sourceIP {
			return false
		}
	}
	return true
}

func (t *ConnTracker) nextID() string {
	return "sb-" + formatInt36(t.idCounter.Add(1))
}

func formatInt36(n int64) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	if n == 0 {
		return "0"
	}
	var buf [13]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%36]
		n /= 36
	}
	return string(buf[i:])
}

// ─── Public API ─────────────────────────────────────────────────────────────

// GetUserTraffic returns per-user cumulative traffic and alive IPs.
// This is O(users), not O(connections).
func (t *ConnTracker) GetUserTraffic() (traffic map[int][2]int64, aliveIPs map[int]map[string]bool, connCount int) {
	t.usersMu.RLock()
	traffic = make(map[int][2]int64, len(t.users))
	aliveIPs = make(map[int]map[string]bool, len(t.users))
	for uid, us := range t.users {
		up := us.upload.Load()
		down := us.download.Load()
		if up > 0 || down > 0 {
			traffic[uid] = [2]int64{up, down}
		}

		ips := us.aliveIPList()
		if ips != nil {
			aliveIPs[uid] = ips
		}

		us.mu.Lock()
		connCount += us.connCount
		us.mu.Unlock()
	}
	t.usersMu.RUnlock()
	return
}

// CloseByID force-closes a connection by its ID.
func (t *ConnTracker) CloseByID(id string) bool {
	t.usersMu.RLock()
	conn, ok := t.connMap[id]
	t.usersMu.RUnlock()
	if !ok {
		return false
	}
	if conn != nil {
		conn.Close()
	}
	return true
}

// CloseByUUID force-closes ALL connections for a given user UUID.
func (t *ConnTracker) CloseByUUID(uuid string) int {
	// This is a no-op for now — sing-box doesn't expose per-user connection
	// kill easily. The kernel's RemoveUsers removes the inbound user which
	// prevents new connections, and existing connections will fail on next I/O.
	return 0
}

// ActiveCount returns the total number of active connections.
func (t *ConnTracker) ActiveCount() int {
	t.usersMu.RLock()
	total := 0
	for _, us := range t.users {
		us.mu.Lock()
		total += us.connCount
		us.mu.Unlock()
	}
	t.usersMu.RUnlock()
	return total
}

// removeConnRef removes the connection reference from connMap on close.
func (t *ConnTracker) removeConnRef(connID string) {
	t.usersMu.Lock()
	delete(t.connMap, connID)
	t.usersMu.Unlock()
}

// ─── trackedConn (TCP) ──────────────────────────────────────────────────────

type trackedConn struct {
	net.Conn
	tracker       *ConnTracker
	us            *userStats // per-user stats (upload/download atomics + IP tracking)
	connID        string
	sourceIP      string
	limiter       *rate.Limiter
	ctx           context.Context
	pendingTokens atomic.Int64
	closed        atomic.Bool
}

func (c *trackedConn) Read(b []byte) (int, error) {
	if c.limiter != nil {
		if burst := c.limiter.Burst(); len(b) > burst {
			b = b[:burst]
		}
	}
	n, err := c.Conn.Read(b)
	if n > 0 {
		if c.us != nil {
			c.us.download.Add(int64(n))
		}
		if c.limiter != nil {
			if pending := c.pendingTokens.Add(int64(n)); pending >= rateLimitThreshold {
				tokens := int(c.pendingTokens.Swap(0))
				if burst := c.limiter.Burst(); tokens > burst {
					tokens = burst
				}
				resv := c.limiter.ReserveN(time.Now(), tokens)
				if delay := resv.Delay(); delay > 0 {
					time.Sleep(delay)
				}
			}
		}
	}
	return n, err
}

func (c *trackedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 {
		if c.us != nil {
			c.us.upload.Add(int64(n))
		}
		if c.limiter != nil {
			if pending := c.pendingTokens.Add(int64(n)); pending >= rateLimitThreshold {
				tokens := int(c.pendingTokens.Swap(0))
				if burst := c.limiter.Burst(); tokens > burst {
					tokens = burst
				}
				resv := c.limiter.ReserveN(time.Now(), tokens)
				if delay := resv.Delay(); delay > 0 {
					time.Sleep(delay)
				}
			}
		}
	}
	return n, err
}

func (c *trackedConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		if c.us != nil {
			c.us.removeConn(c.sourceIP)
		}
		c.tracker.removeConnRef(c.connID)
	}
	return c.Conn.Close()
}

// makeCountFunc builds a CountFunc for zero-copy byte counting via sing's
// ReadCounter/WriteCounter unwrap interfaces.
func (c *trackedConn) makeCountFunc(counter *atomic.Int64) N.CountFunc {
	if c.limiter == nil {
		return func(n int64) { counter.Add(n) }
	}
	return func(n int64) {
		counter.Add(n)
		if pending := c.pendingTokens.Add(n); pending >= rateLimitThreshold {
			tokens := int(c.pendingTokens.Swap(0))
			if burst := c.limiter.Burst(); tokens > burst {
				tokens = burst
			}
			resv := c.limiter.ReserveN(time.Now(), tokens)
			if delay := resv.Delay(); delay > 0 {
				t := time.NewTimer(delay)
				select {
				case <-t.C:
				case <-c.ctx.Done():
					t.Stop()
					resv.Cancel()
				}
			}
		}
	}
}

func (c *trackedConn) UnwrapReader() (io.Reader, []N.CountFunc) {
	if c.us == nil {
		return c.Conn, nil
	}
	return c.Conn, []N.CountFunc{c.makeCountFunc(&c.us.download)}
}

func (c *trackedConn) UnwrapWriter() (io.Writer, []N.CountFunc) {
	if c.us == nil {
		return c.Conn, nil
	}
	return c.Conn, []N.CountFunc{c.makeCountFunc(&c.us.upload)}
}

func (c *trackedConn) Upstream() any           { return c.Conn }
func (c *trackedConn) ReaderReplaceable() bool { return true }
func (c *trackedConn) WriterReplaceable() bool { return true }

// ─── trackedPacketConn (UDP / QUIC) ─────────────────────────────────────────

type trackedPacketConn struct {
	N.PacketConn
	tracker       *ConnTracker
	us            *userStats
	connID        string
	sourceIP      string
	limiter       *rate.Limiter
	ctx           context.Context
	pendingTokens atomic.Int64
	closed        atomic.Bool
}

func (c *trackedPacketConn) ReadPacket(buffer *buf.Buffer) (singM.Socksaddr, error) {
	dest, err := c.PacketConn.ReadPacket(buffer)
	if err == nil {
		n := int64(buffer.Len())
		if c.us != nil {
			c.us.download.Add(n)
		}
		if c.limiter != nil {
			if pending := c.pendingTokens.Add(n); pending >= rateLimitThreshold {
				tokens := int(c.pendingTokens.Swap(0))
				if burst := c.limiter.Burst(); tokens > burst {
					tokens = burst
				}
				resv := c.limiter.ReserveN(time.Now(), tokens)
				if delay := resv.Delay(); delay > 0 {
					time.Sleep(delay)
				}
			}
		}
	}
	return dest, err
}

func (c *trackedPacketConn) WritePacket(buffer *buf.Buffer, dest singM.Socksaddr) error {
	n := int64(buffer.Len())
	err := c.PacketConn.WritePacket(buffer, dest)
	if err == nil {
		if c.us != nil {
			c.us.upload.Add(n)
		}
		if c.limiter != nil {
			if pending := c.pendingTokens.Add(n); pending >= rateLimitThreshold {
				tokens := int(c.pendingTokens.Swap(0))
				if burst := c.limiter.Burst(); tokens > burst {
					tokens = burst
				}
				resv := c.limiter.ReserveN(time.Now(), tokens)
				if delay := resv.Delay(); delay > 0 {
					time.Sleep(delay)
				}
			}
		}
	}
	return err
}

func (c *trackedPacketConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		if c.us != nil {
			c.us.removeConn(c.sourceIP)
		}
		c.tracker.removeConnRef(c.connID)
	}
	return c.PacketConn.Close()
}

func (c *trackedPacketConn) makeCountFunc(counter *atomic.Int64) N.CountFunc {
	if c.limiter == nil {
		return func(n int64) { counter.Add(n) }
	}
	return func(n int64) {
		counter.Add(n)
		if pending := c.pendingTokens.Add(n); pending >= rateLimitThreshold {
			tokens := int(c.pendingTokens.Swap(0))
			if burst := c.limiter.Burst(); tokens > burst {
				tokens = burst
			}
			resv := c.limiter.ReserveN(time.Now(), tokens)
			if delay := resv.Delay(); delay > 0 {
				t := time.NewTimer(delay)
				select {
				case <-t.C:
				case <-c.ctx.Done():
					t.Stop()
					resv.Cancel()
				}
			}
		}
	}
}

func (c *trackedPacketConn) UnwrapPacketReader() (N.PacketReader, []N.CountFunc) {
	if c.us == nil {
		return c.PacketConn, nil
	}
	return c.PacketConn, []N.CountFunc{c.makeCountFunc(&c.us.download)}
}

func (c *trackedPacketConn) UnwrapPacketWriter() (N.PacketWriter, []N.CountFunc) {
	if c.us == nil {
		return c.PacketConn, nil
	}
	return c.PacketConn, []N.CountFunc{c.makeCountFunc(&c.us.upload)}
}

func (c *trackedPacketConn) Upstream() any           { return c.PacketConn }
func (c *trackedPacketConn) ReaderReplaceable() bool { return true }
func (c *trackedPacketConn) WriterReplaceable() bool { return true }
