package singbox

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	singM "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/time/rate"

	"github.com/cedar2025/xboard-node/internal/kernel"
)

// connRecord holds the minimal per-connection state we need.
// Each field is accessed by multiple goroutines; upload/download use atomics.
type connRecord struct {
	id       string
	userID   int
	sourceIP string
	upload   atomic.Int64
	download atomic.Int64
	conn     net.Conn // kept for force-close via CloseByID
}

func (r *connRecord) toConnection() kernel.Connection {
	return kernel.Connection{
		ID:       r.id,
		UserID:   r.userID,
		SourceIP: r.sourceIP,
		Upload:   r.upload.Load(),
		Download: r.download.Load(),
	}
}

// ConnTracker is a lightweight in-process connection tracker that replaces
// the Clash API TrafficManager. It handles:
//   - per-connection byte counting (upload / download)
//   - optional per-user speed limiting (when SetSpeedLimitFunc is configured)
//   - source IP tracking for device-limit enforcement
//   - force-close a connection by ID
//
// Both byte counting and speed limiting are implemented via sing's CountFunc
// callback mechanism (ReadCounter / WriteCounter interfaces). This allows
// sing's copy pipeline to use splice / ReadWaiter zero-copy paths while
// still counting bytes and enforcing bandwidth limits through post-transfer
// callbacks.
//
// Memory model:
//   - active: live connections, looked up by ID for snapshots and CloseByID
//   - pending: closed connections accumulated since the last Snapshot call,
//     drained atomically so the Tracker sees every connection's final bytes
//
// Thread safety: active is guarded by mu; pending by pendingMu.
type ConnTracker struct {
	mu     sync.RWMutex
	active map[string]*connRecord

	// idCounter generates unique connection IDs without crypto/rand.
	idCounter atomic.Int64

	pendingMu  sync.Mutex
	pending    []*connRecord
	maxPending int

	userMapMu sync.RWMutex
	userMap   map[string]int // UUID → userID

	// speedLimitFunc resolves a user UUID to a *rate.Limiter. When set,
	// new connections for users with a speed limit are automatically
	// rate-limited without needing a second ConnectionTracker wrapper.
	// The function must be safe for concurrent calls.
	speedLimitFunc func(uuid string) *rate.Limiter
}

// NewConnTracker creates a tracker.
func NewConnTracker(maxPending int) *ConnTracker {
	if maxPending <= 0 {
		maxPending = 50000
	}
	return &ConnTracker{
		active:     make(map[string]*connRecord),
		maxPending: maxPending,
		userMap:    make(map[string]int),
	}
}

// SetSpeedLimitFunc configures the per-user speed limit lookup.
// fn is called for every new connection; it must return nil for unlimited users.
// Thread-safe: fn itself must be safe for concurrent calls.
func (t *ConnTracker) SetSpeedLimitFunc(fn func(uuid string) *rate.Limiter) {
	t.speedLimitFunc = fn
}

// SetUserMap replaces the UUID→userID mapping. Must be called whenever the
// user list changes so that new connections are attributed to the correct user.
func (t *ConnTracker) SetUserMap(m map[string]int) {
	t.userMapMu.Lock()
	t.userMap = m
	t.userMapMu.Unlock()
}

// ─── adapter.ConnectionTracker ───────────────────────────────────────────────

// RoutedConnection wraps a TCP conn to count bytes, track lifecycle,
// and optionally rate-limit per-user bandwidth.
func (t *ConnTracker) RoutedConnection(
	ctx context.Context, conn net.Conn,
	metadata adapter.InboundContext,
	_ adapter.Rule, _ adapter.Outbound,
) net.Conn {
	rec := t.allocRecord(metadata, conn)
	var lim *rate.Limiter
	if t.speedLimitFunc != nil {
		lim = t.speedLimitFunc(metadata.User)
	}
	return &trackedConn{Conn: conn, rec: rec, tracker: t, limiter: lim, ctx: ctx}
}

// RoutedPacketConnection wraps a UDP PacketConn to count bytes, track lifecycle,
// and optionally rate-limit per-user bandwidth.
func (t *ConnTracker) RoutedPacketConnection(
	ctx context.Context, conn N.PacketConn,
	metadata adapter.InboundContext,
	_ adapter.Rule, _ adapter.Outbound,
) N.PacketConn {
	rec := t.allocRecord(metadata, nil)
	var lim *rate.Limiter
	if t.speedLimitFunc != nil {
		lim = t.speedLimitFunc(metadata.User)
	}
	return &trackedPacketConn{PacketConn: conn, rec: rec, tracker: t, limiter: lim, ctx: ctx}
}

// ─── Internal helpers ────────────────────────────────────────────────────────

func (t *ConnTracker) allocRecord(metadata adapter.InboundContext, conn net.Conn) *connRecord {
	// Use a fast atomic counter instead of crypto/rand UUID generation.
	// Under connection floods (thousands of concurrent handshakes), uuid.NewV4()
	// becomes a syscall bottleneck; a counter is allocation-free and lock-free.
	id := strconv.FormatInt(t.idCounter.Add(1), 36)
	t.userMapMu.RLock()
	uid := t.userMap[metadata.User]
	t.userMapMu.RUnlock()
	rec := &connRecord{
		id:       id,
		userID:   uid,
		sourceIP: metadata.Source.Addr.String(),
		conn:     conn,
	}
	t.mu.Lock()
	t.active[rec.id] = rec
	t.mu.Unlock()
	return rec
}

// closeRecord is idempotent: if the record has already been moved to pending,
// the second call is a no-op.
func (t *ConnTracker) closeRecord(rec *connRecord) {
	t.mu.Lock()
	_, present := t.active[rec.id]
	if present {
		delete(t.active, rec.id)
	}
	t.mu.Unlock()

	if !present {
		return
	}

	t.pendingMu.Lock()
	if len(t.pending) < t.maxPending {
		t.pending = append(t.pending, rec)
	} else {
		slog.Warn("conntracker: pending buffer full, dropping closed connection",
			"max_pending", t.maxPending, "user_id", rec.userID)
	}
	t.pendingMu.Unlock()
}

// ─── Public API ──────────────────────────────────────────────────────────────

// Snapshot returns a point-in-time view of all active connections plus all
// connections that closed since the previous Snapshot call. The pending
// (closed) list is drained atomically so callers never miss a connection.
func (t *ConnTracker) Snapshot() []kernel.Connection {
	// Drain pending under its own lock (hot path for closed connections).
	t.pendingMu.Lock()
	pending := t.pending
	t.pending = nil
	t.pendingMu.Unlock()

	// Two-phase read to minimise RLock hold time at high connection counts:
	//   Phase 1 (under RLock): collect record *pointers* — a pointer copy is
	//   ~8 bytes vs ~80 bytes for a full Connection struct, and no atomic loads.
	//   Phase 2 (lock-free):   call toConnection() which atomically reads the
	//   upload/download counters. Atomics are safe without any mutex.
	// This shrinks the RLock window proportionally as connection count grows,
	// reducing serialisation against concurrent allocRecord/closeRecord calls.
	t.mu.RLock()
	ptrs := make([]*connRecord, 0, len(t.active))
	for _, rec := range t.active {
		ptrs = append(ptrs, rec)
	}
	t.mu.RUnlock()

	result := make([]kernel.Connection, 0, len(ptrs)+len(pending))
	for _, rec := range ptrs {
		result = append(result, rec.toConnection())
	}
	for _, rec := range pending {
		result = append(result, rec.toConnection())
	}
	return result
}

// CloseByID force-closes a live connection by its ID.
// Returns true if the connection was found and closed.
func (t *ConnTracker) CloseByID(id string) bool {
	t.mu.RLock()
	rec, ok := t.active[id]
	t.mu.RUnlock()
	if !ok {
		return false
	}
	if rec.conn != nil {
		rec.conn.Close()
	}
	return true
}

// ActiveCount returns the number of currently tracked live connections.
func (t *ConnTracker) ActiveCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.active)
}

// ─── trackedConn (TCP) ───────────────────────────────────────────────────────

type trackedConn struct {
	net.Conn
	rec     *connRecord
	tracker *ConnTracker
	limiter *rate.Limiter   // nil = unlimited
	ctx     context.Context // for rate limiter WaitN cancellation
}

// Read is the fallback path used when the copy pipeline cannot unwrap this
// wrapper (should not happen with current code, but kept for robustness).
func (c *trackedConn) Read(b []byte) (int, error) {
	if c.limiter != nil {
		if burst := c.limiter.Burst(); len(b) > burst {
			b = b[:burst]
		}
	}
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.rec.download.Add(int64(n))
		if c.limiter != nil {
			waitN := min(n, c.limiter.Burst())
			_ = c.limiter.WaitN(c.ctx, waitN)
		}
	}
	return n, err
}

// Write is the fallback path.
func (c *trackedConn) Write(b []byte) (int, error) {
	if c.limiter != nil {
		burst := c.limiter.Burst()
		var n int
		for n < len(b) {
			chunk := min(len(b)-n, burst)
			_ = c.limiter.WaitN(c.ctx, chunk)
			written, writeErr := c.Conn.Write(b[n : n+chunk])
			n += written
			c.rec.upload.Add(int64(written))
			if writeErr != nil {
				return n, writeErr
			}
		}
		return n, nil
	}
	n, err := c.Conn.Write(b)
	if n > 0 {
		c.rec.upload.Add(int64(n))
	}
	return n, err
}

func (c *trackedConn) Close() error {
	c.tracker.closeRecord(c.rec)
	return c.Conn.Close()
}

// makeCountFunc builds a CountFunc that does byte accounting and optional
// rate limiting in a single callback. The direction is selected by the
// counter pointer (download or upload).
func (c *trackedConn) makeCountFunc(counter *atomic.Int64) N.CountFunc {
	if c.limiter == nil {
		// Fast path: no rate limiting, just atomic add.
		return func(n int64) { counter.Add(n) }
	}
	return func(n int64) {
		counter.Add(n)
		waitN := int(n)
		if burst := c.limiter.Burst(); waitN > burst {
			waitN = burst
		}
		_ = c.limiter.WaitN(c.ctx, waitN)
	}
}

// UnwrapReader implements N.ReadCounter — lets the copy pipeline strip this
// wrapper, collect a counter callback, and reach the inner conn for
// splice / ReadWaiter zero-copy transfer.
func (c *trackedConn) UnwrapReader() (io.Reader, []N.CountFunc) {
	return c.Conn, []N.CountFunc{c.makeCountFunc(&c.rec.download)}
}

// UnwrapWriter implements N.WriteCounter.
func (c *trackedConn) UnwrapWriter() (io.Writer, []N.CountFunc) {
	return c.Conn, []N.CountFunc{c.makeCountFunc(&c.rec.upload)}
}

func (c *trackedConn) Upstream() any           { return c.Conn }
func (c *trackedConn) ReaderReplaceable() bool { return true }
func (c *trackedConn) WriterReplaceable() bool { return true }

// ─── trackedPacketConn (UDP / QUIC) ──────────────────────────────────────────

type trackedPacketConn struct {
	N.PacketConn
	rec     *connRecord
	tracker *ConnTracker
	limiter *rate.Limiter
	ctx     context.Context
}

func (c *trackedPacketConn) ReadPacket(buffer *buf.Buffer) (singM.Socksaddr, error) {
	dest, err := c.PacketConn.ReadPacket(buffer)
	if err == nil {
		n := int64(buffer.Len())
		c.rec.download.Add(n)
		if c.limiter != nil {
			waitN := int(min(n, int64(c.limiter.Burst())))
			_ = c.limiter.WaitN(c.ctx, waitN)
		}
	}
	return dest, err
}

func (c *trackedPacketConn) WritePacket(buffer *buf.Buffer, dest singM.Socksaddr) error {
	n := int64(buffer.Len())
	err := c.PacketConn.WritePacket(buffer, dest)
	if err == nil {
		c.rec.upload.Add(n)
		if c.limiter != nil {
			waitN := int(min(n, int64(c.limiter.Burst())))
			_ = c.limiter.WaitN(c.ctx, waitN)
		}
	}
	return err
}

func (c *trackedPacketConn) Close() error {
	c.tracker.closeRecord(c.rec)
	return c.PacketConn.Close()
}

func (c *trackedPacketConn) makeCountFunc(counter *atomic.Int64) N.CountFunc {
	if c.limiter == nil {
		return func(n int64) { counter.Add(n) }
	}
	return func(n int64) {
		counter.Add(n)
		waitN := int(n)
		if burst := c.limiter.Burst(); waitN > burst {
			waitN = burst
		}
		_ = c.limiter.WaitN(c.ctx, waitN)
	}
}

func (c *trackedPacketConn) UnwrapPacketReader() (N.PacketReader, []N.CountFunc) {
	return c.PacketConn, []N.CountFunc{c.makeCountFunc(&c.rec.download)}
}

func (c *trackedPacketConn) UnwrapPacketWriter() (N.PacketWriter, []N.CountFunc) {
	return c.PacketConn, []N.CountFunc{c.makeCountFunc(&c.rec.upload)}
}

func (c *trackedPacketConn) Upstream() any           { return c.PacketConn }
func (c *trackedPacketConn) ReaderReplaceable() bool { return true }
func (c *trackedPacketConn) WriterReplaceable() bool { return true }
