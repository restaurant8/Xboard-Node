package xray

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	_ "unsafe"

	xrayDispatcher "github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

// Access xray's internal config creator registry so we can replace the
// default dispatcher factory with ours. This runs AFTER xray's init()
// functions because our package imports xray (dependency order guarantee).
//
//go:linkname typeCreatorRegistry github.com/xtls/xray-core/common.typeCreatorRegistry
var typeCreatorRegistry map[reflect.Type]common.ConfigCreator

var origDispatcherFactory common.ConfigCreator

// globalLimitDispatcher is set when the factory creates a LimitDispatcher.
// The Xray kernel reads it to configure limits and get connections.
var globalLimitDispatcher atomic.Pointer[LimitDispatcher]

func init() {
	configType := reflect.TypeOf((*xrayDispatcher.Config)(nil))
	origDispatcherFactory = typeCreatorRegistry[configType]
	typeCreatorRegistry[configType] = limitDispatcherFactory
}

func limitDispatcherFactory(ctx context.Context, config interface{}) (interface{}, error) {
	orig, err := origDispatcherFactory(ctx, config)
	if err != nil {
		return nil, err
	}
	inner, ok := orig.(routing.Dispatcher)
	if !ok {
		return orig, nil
	}
	ld := &LimitDispatcher{
		inner:        orig,
		innerDisp:    inner,
		limitedIPs:   make(map[string]map[string]int),
		unlimitedIPs: make(map[string]map[string]int),
		userConns:    make(map[string]int),
		udpIPs:       make(map[string]map[string]int),
	}
	globalLimitDispatcher.Store(ld)
	nlog.Core().Debug("xray: limit dispatcher installed")
	return ld, nil
}

// LimitDispatcher wraps xray's DefaultDispatcher to enforce per-user
// admission checks before a request is dispatched into xray-core.
//
// It intentionally does NOT mutate transport.Link.Reader/Writer. Xray's
// mux/XUDP close path requires the original concrete *pipe.Reader to remain
// intact, so the dispatcher is limited to gate-keeping and safe connection
// lifecycle bookkeeping.
type LimitDispatcher struct {
	inner     interface{}        // original DefaultDispatcher (Feature + Dispatcher)
	innerDisp routing.Dispatcher // same object, typed as Dispatcher

	// limitedUsers: users with device limit > 0, protected by mu.
	// Needs deterministic IP ordering for kick decisions.
	mu           sync.RWMutex
	limitedIPs   map[string]map[string]int // email → sourceIP → refcount
	deviceLimits map[string]int            // email → max devices
	emailToUID   map[string]int            // email → panel user ID

	// connStatsMu protects per-user active TCP+UDP counts, unlimited-user TCP IPs,
	// and UDP active IPs. Never nested with mu to avoid deadlock and eliminate
	// sync.Map LoadOrStore/Delete races during rapid TCP/UDP connection handovers;
	// zero-count entries are deleted immediately to prevent unbounded memory growth.
	connStatsMu  sync.Mutex
	unlimitedIPs map[string]map[string]int // email → sourceIP → active TCP refcount (no device limit)
	udpIPs       map[string]map[string]int // email → sourceIP → active UDP refcount
	userConns    map[string]int            // email → active TCP+UDP connections

	connCount atomic.Int64 // total active connections tracked by dispatcher
}

// ─── routing.Dispatcher ──────────────────────────────────────────────────────

func (d *LimitDispatcher) Dispatch(ctx context.Context, dest net.Destination) (*transport.Link, error) {
	email, sourceIP, isTCP, err := d.identifyAndCheck(ctx, dest)
	if err != nil {
		return nil, err
	}

	link, err := d.innerDisp.Dispatch(ctx, dest)
	if err != nil {
		if email != "" && isTCP {
			d.delConn(email, sourceIP)
		}
		return nil, err
	}

	if email != "" {
		d.trackLink(link, email, sourceIP, isTCP)
	}
	return link, nil
}

func (d *LimitDispatcher) DispatchLink(ctx context.Context, dest net.Destination, link *transport.Link) error {
	email, sourceIP, isTCP, err := d.identifyAndCheck(ctx, dest)
	if err != nil {
		return err
	}

	var release func()
	if email != "" {
		release = d.trackLink(link, email, sourceIP, isTCP)
	}
	err = d.innerDisp.DispatchLink(ctx, dest, link)
	if err != nil && release != nil {
		release()
	}
	return err
}

// identifyAndCheck extracts user identity from the session context, enforces
// device limits, and returns the user's email, source IP, and TCP flag.
// Returns a non-nil error only when the connection should be rejected.
func (d *LimitDispatcher) identifyAndCheck(ctx context.Context, dest net.Destination) (email, sourceIP string, isTCP bool, err error) {
	si := session.InboundFromContext(ctx)
	if si == nil || si.User == nil || len(si.User.Email) == 0 {
		return "", "", false, nil
	}
	email = si.User.Email
	sourceIP = si.Source.Address.IP().String()
	isTCP = dest.Network == net.Network_TCP

	if d.checkDeviceLimit(email, sourceIP, isTCP) {
		nlog.Core().Debug("xray: device limit exceeded", "email", email, "ip", sourceIP)
		return "", "", false, errors.New("device limit exceeded for " + email)
	}
	return email, sourceIP, isTCP, nil
}

// trackLink records connection lifecycle without mutating xray-core owned
// transport primitives. This keeps mux/XUDP compatible while still allowing
// the dispatcher to release device-limit and per-user concurrency state when
// the link closes.
func (d *LimitDispatcher) trackLink(link *transport.Link, email, sourceIP string, isTCP bool) func() {
	d.connCount.Add(1)
	d.incUserConn(email)
	if !isTCP && sourceIP != "" {
		d.addUDPIP(email, sourceIP)
	}

	onClose := func() {
		if isTCP {
			d.delConn(email, sourceIP)
		} else if sourceIP != "" {
			d.delUDPIP(email, sourceIP)
		}
		d.decUserConn(email)
		d.connCount.Add(-1)
	}

	w := &closeTrackingWriter{
		Writer:  link.Writer,
		onClose: onClose,
	}
	link.Writer = w
	return w.release
}

// ─── features.Feature (delegated) ───────────────────────────────────────────

func (d *LimitDispatcher) Type() interface{} { return routing.DispatcherType() }

func (d *LimitDispatcher) Start() error {
	if s, ok := d.inner.(interface{ Start() error }); ok {
		return s.Start()
	}
	return nil
}

func (d *LimitDispatcher) Close() error {
	if c, ok := d.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// ─── Limit management (called by Xray kernel) ──────────────────────────────

func (d *LimitDispatcher) UpdateLimits(emailToUID map[string]int, deviceLimits, _ map[string]int) {
	d.mu.Lock()
	d.emailToUID = emailToUID
	d.deviceLimits = deviceLimits
	d.mu.Unlock()

}

func (d *LimitDispatcher) ResetConns() {
	d.mu.Lock()
	d.limitedIPs = make(map[string]map[string]int)
	d.mu.Unlock()

	// Clear unlimited TCP IPs, UDP IPs, and per-user connection counters atomically
	d.connStatsMu.Lock()
	d.unlimitedIPs = make(map[string]map[string]int)
	d.udpIPs = make(map[string]map[string]int)
	d.userConns = make(map[string]int)
	d.connStatsMu.Unlock()

	d.connCount.Store(0)
}

// GetConnectionState returns dispatcher-tracked alive IPs, per-user active
// TCP+UDP connection counts, and total connection count.
// Traffic bytes are intentionally left to xray's built-in stats pipeline.
func (d *LimitDispatcher) GetConnectionState() (aliveIPs map[int]map[string]bool, userConns map[int]int, connCount int) {
	aliveIPs = make(map[int]map[string]bool)
	userConns = make(map[int]int)

	// Collect IPs from limited users while holding the read lock. limitedIPs and
	// its inner maps are mutated in place by checkDeviceLimit/delConn under the
	// write lock, so we must copy them out here rather than iterate after
	// unlocking — otherwise a concurrent map read/write triggers a fatal error.
	d.mu.RLock()
	for email, ipsMap := range d.limitedIPs {
		uid := d.emailToUID[email]
		if uid == 0 {
			continue
		}
		ipSet := make(map[string]bool, len(ipsMap))
		for ip := range ipsMap {
			ipSet[ip] = true
		}
		if len(ipSet) > 0 {
			aliveIPs[uid] = ipSet
		}
	}
	// emailToUID is only ever replaced wholesale (UpdateLimits), never mutated
	// in place, so the captured reference stays safe to read after unlocking.
	emailToUID := d.emailToUID
	d.mu.RUnlock()

	// Collect unlimited TCP IPs, UDP active IPs, and per-user active TCP+UDP
	// connection counts together under connStatsMu so rapid TCP/UDP handovers
	// never observe active connections with a missing source IP.
	d.connStatsMu.Lock()
	for email, ips := range d.unlimitedIPs {
		uid := emailToUID[email]
		if uid == 0 || len(ips) == 0 {
			continue
		}
		existing := aliveIPs[uid]
		if existing == nil {
			existing = make(map[string]bool, len(ips))
			aliveIPs[uid] = existing
		}
		for ip, ref := range ips {
			if ref > 0 {
				existing[ip] = true
			}
		}
	}
	for email, ips := range d.udpIPs {
		uid := emailToUID[email]
		if uid == 0 || len(ips) == 0 {
			continue
		}
		existing := aliveIPs[uid]
		if existing == nil {
			existing = make(map[string]bool, len(ips))
			aliveIPs[uid] = existing
		}
		for ip, ref := range ips {
			if ref > 0 {
				existing[ip] = true
			}
		}
	}
	for email, cnt := range d.userConns {
		uid := emailToUID[email]
		if uid == 0 || cnt <= 0 {
			continue
		}
		userConns[uid] += cnt
	}
	d.connStatsMu.Unlock()

	connCount = int(d.connCount.Load())
	return
}

func (d *LimitDispatcher) incUserConn(email string) {
	d.connStatsMu.Lock()
	if d.userConns == nil {
		d.userConns = make(map[string]int)
	}
	d.userConns[email]++
	d.connStatsMu.Unlock()
}

func (d *LimitDispatcher) decUserConn(email string) {
	d.connStatsMu.Lock()
	if d.userConns != nil {
		if n := d.userConns[email] - 1; n > 0 {
			d.userConns[email] = n
		} else {
			delete(d.userConns, email)
		}
	}
	d.connStatsMu.Unlock()
}

func (d *LimitDispatcher) addUnlimitedIP(email, sourceIP string) {
	d.connStatsMu.Lock()
	if d.unlimitedIPs == nil {
		d.unlimitedIPs = make(map[string]map[string]int)
	}
	m := d.unlimitedIPs[email]
	if m == nil {
		m = make(map[string]int)
		d.unlimitedIPs[email] = m
	}
	m[sourceIP]++
	d.connStatsMu.Unlock()
}

func (d *LimitDispatcher) delUnlimitedIP(email, sourceIP string) bool {
	d.connStatsMu.Lock()
	defer d.connStatsMu.Unlock()
	m := d.unlimitedIPs[email]
	if m == nil {
		return false
	}
	if _, exists := m[sourceIP]; !exists {
		return false
	}
	if n := m[sourceIP] - 1; n > 0 {
		m[sourceIP] = n
	} else {
		delete(m, sourceIP)
	}
	if len(m) == 0 {
		delete(d.unlimitedIPs, email)
	}
	return true
}

func (d *LimitDispatcher) addUDPIP(email, sourceIP string) {
	d.connStatsMu.Lock()
	if d.udpIPs == nil {
		d.udpIPs = make(map[string]map[string]int)
	}
	m := d.udpIPs[email]
	if m == nil {
		m = make(map[string]int)
		d.udpIPs[email] = m
	}
	m[sourceIP]++
	d.connStatsMu.Unlock()
}

func (d *LimitDispatcher) delUDPIP(email, sourceIP string) {
	d.connStatsMu.Lock()
	if m := d.udpIPs[email]; m != nil {
		if n := m[sourceIP] - 1; n > 0 {
			m[sourceIP] = n
		} else {
			delete(m, sourceIP)
		}
		if len(m) == 0 {
			delete(d.udpIPs, email)
		}
	}
	d.connStatsMu.Unlock()
}

// ─── Internal helpers ───────────────────────────────────────────────────────

// checkDeviceLimit enforces per-user device limits.
// Fast path: unlimited users use lock-free sync.Map.
// Slow path: limited users use RWMutex with deterministic IP ordering.
func (d *LimitDispatcher) checkDeviceLimit(email, sourceIP string, isTCP bool) bool {
	d.mu.RLock()
	limit, hasLimit := d.deviceLimits[email]
	d.mu.RUnlock()

	// Fast path: no device limit — guarded by connStatsMu (race-free on TCP handover).
	if !hasLimit || limit <= 0 {
		if isTCP {
			d.addUnlimitedIP(email, sourceIP)
		}
		return false
	}

	// Slow path: user has device limit — need deterministic ordering.
	d.mu.RLock()
	ips := d.limitedIPs[email]
	if ips != nil && ips[sourceIP] > 0 {
		d.mu.RUnlock()
		if isTCP {
			d.mu.Lock()
			// The inner map may have been deleted by delConn between releasing
			// the read lock and taking the write lock, so recreate it if needed
			// rather than writing into a nil map (which panics).
			m := d.limitedIPs[email]
			if m == nil {
				m = make(map[string]int)
				d.limitedIPs[email] = m
			}
			m[sourceIP]++
			d.mu.Unlock()
		}
		return false
	}

	if ips != nil && len(ips) < limit {
		d.mu.RUnlock()
		if isTCP {
			d.mu.Lock()
			if d.limitedIPs[email] == nil {
				d.limitedIPs[email] = make(map[string]int)
			}
			d.limitedIPs[email][sourceIP]++
			d.mu.Unlock()
		}
		return false
	}
	d.mu.RUnlock()

	// Over limit — need write lock for deterministic check.
	d.mu.Lock()
	defer d.mu.Unlock()

	// Re-check under write lock.
	ips = d.limitedIPs[email]
	if ips == nil {
		ips = make(map[string]int)
		d.limitedIPs[email] = ips
	}

	if ips[sourceIP] > 0 {
		if isTCP {
			ips[sourceIP]++
		}
		return false
	}

	if len(ips) < limit {
		if isTCP {
			ips[sourceIP]++
		}
		return false
	}

	// Over limit — deterministic: allow lowest IPs lexicographically.
	ipList := make([]string, 0, len(ips)+1)
	for ip := range ips {
		ipList = append(ipList, ip)
	}
	ipList = append(ipList, sourceIP)
	sort.Strings(ipList)

	for i := 0; i < limit && i < len(ipList); i++ {
		if ipList[i] == sourceIP {
			if isTCP {
				ips[sourceIP]++
			}
			return false
		}
	}
	return true
}

// delConn decrements the IP refcount when a connection closes.
func (d *LimitDispatcher) delConn(email, sourceIP string) {
	// Check unlimited TCP tracking first (atomic decrement+delete under connStatsMu).
	if d.delUnlimitedIP(email, sourceIP) {
		return
	}

	// Limited user — use write lock.
	d.mu.Lock()
	defer d.mu.Unlock()
	if ips, ok := d.limitedIPs[email]; ok {
		ips[sourceIP]--
		if ips[sourceIP] <= 0 {
			delete(ips, sourceIP)
		}
		if len(ips) == 0 {
			delete(d.limitedIPs, email)
		}
	}
}

type closeTrackingWriter struct {
	buf.Writer
	onClose func()
	closed  atomic.Bool
}

func (w *closeTrackingWriter) release() {
	if w.closed.CompareAndSwap(false, true) {
		w.onClose()
	}
}

func (w *closeTrackingWriter) Close() error {
	w.release()
	return common.Close(w.Writer)
}

func (w *closeTrackingWriter) Interrupt() {
	w.release()
	common.Interrupt(w.Writer)
}
