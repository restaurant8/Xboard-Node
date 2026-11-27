package singbox

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/sagernet/sing-box/adapter"
	singM "github.com/sagernet/sing/common/metadata"
	"golang.org/x/time/rate"
)

func TestSingBoxCapabilities(t *testing.T) {
	s := New(config.KernelConfig{Type: "sing-box"})
	caps := s.Capabilities()
	if !caps.PerUserSpeedLimit || !caps.DeviceLimit || !caps.AliveIPTracking || !caps.ForceCloseUser {
		t.Fatalf("unexpected sing-box capabilities: %+v", caps)
	}
	if caps.BuiltInTrafficStats || caps.ForceCloseConnection {
		t.Fatalf("unexpected sing-box capabilities: %+v", caps)
	}
	protocols := s.Protocols()
	found := false
	for _, protocol := range protocols {
		if protocol == "hysteria" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Protocols() missing hysteria: %v", protocols)
	}
}



type testConn struct {
	closed bool
	reads  [][]byte
	writes [][]byte
}

func (c *testConn) Read(b []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, errors.New("eof")
	}
	chunk := c.reads[0]
	c.reads = c.reads[1:]
	n := copy(b, chunk)
	return n, nil
}

func (c *testConn) Write(b []byte) (int, error) {
	cp := append([]byte(nil), b...)
	c.writes = append(c.writes, cp)
	return len(b), nil
}

func (c *testConn) Close() error { c.closed = true; return nil }
func (c *testConn) LocalAddr() net.Addr { return &net.TCPAddr{} }
func (c *testConn) RemoteAddr() net.Addr { return &net.TCPAddr{} }
func (c *testConn) SetDeadline(time.Time) error { return nil }
func (c *testConn) SetReadDeadline(time.Time) error { return nil }
func (c *testConn) SetWriteDeadline(time.Time) error { return nil }

func testInboundContext(uuid, ip string) adapter.InboundContext {
	return adapter.InboundContext{
		User:   uuid,
		Source: singM.Socksaddr{Addr: netip.MustParseAddr(ip)},
	}
}

func TestConnTrackerRoutedConnectionTracksTrafficAndAliveIPs(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{reads: [][]byte{[]byte("hello")}}

	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)

	buf := make([]byte, 16)
	n, err := wrapped.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if n != 5 {
		t.Fatalf("Read() bytes = %d, want 5", n)
	}
	if _, err := wrapped.Write([]byte("bye")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	traffic, aliveIPs, userConns, connCount := tracker.GetUserTraffic()
	if got := traffic[1]; got != [2]int64{5, 3} {
		t.Fatalf("traffic[1] = %v, want [5 3]", got)
	}
	if !aliveIPs[1]["1.1.1.1"] {
		t.Fatal("expected alive IP to include source address")
	}
	if userConns[1] != 1 {
		t.Fatalf("userConns[1] = %d, want 1", userConns[1])
	}
	if connCount != 1 {
		t.Fatalf("connCount = %d, want 1", connCount)
	}

	if err := wrapped.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	_, aliveIPs, userConns, connCount = tracker.GetUserTraffic()
	if aliveIPs[1] != nil {
		t.Fatalf("aliveIPs after close = %v, want nil", aliveIPs[1])
	}
	if userConns[1] != 0 {
		t.Fatalf("userConns[1] after close = %d, want 0", userConns[1])
	}
	if connCount != 0 {
		t.Fatalf("connCount after close = %d, want 0", connCount)
	}
}

func TestConnTrackerRoutedConnectionRejectsWhenDeviceLimitExceeded(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	tracker.SetDeviceLimitFunc(func(uuid string) (int, bool) {
		if uuid != "uuid-1" {
			return 0, false
		}
		return 1, true
	})

	first := &testConn{}
	wrapped1 := tracker.RoutedConnection(context.Background(), first, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	if wrapped1 == first {
		t.Fatal("expected first connection to be wrapped")
	}

	second := &testConn{}
	wrapped2 := tracker.RoutedConnection(context.Background(), second, testInboundContext("uuid-1", "2.2.2.2"), nil, nil)
	if wrapped2 != second {
		t.Fatal("expected rejected connection to be returned unwrapped")
	}
	if !second.closed {
		t.Fatal("expected rejected connection to be closed")
	}
}

func TestConnTrackerCheckDeviceGateMergesFreshGlobalDevices(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	us := tracker.users[1]
	us.addConn("1.1.1.1")
	tracker.UpdateGlobalDevices(map[int][]string{1: {"9.9.9.9"}})

	if tracker.checkDeviceGate(us, 1, "2.2.2.2", 2) {
		t.Fatal("expected lexicographically earlier candidate to remain allowed")
	}
	if !tracker.checkDeviceGate(us, 1, "99.99.99.99", 2) {
		t.Fatal("expected merged local+global device state to reject lexicographically later third device")
	}
	if tracker.checkDeviceGate(us, 1, "1.1.1.1", 2) {
		t.Fatal("existing local IP should still be allowed")
	}
	if tracker.checkDeviceGate(us, 1, "9.9.9.9", 2) {
		t.Fatal("existing global IP should still be allowed")
	}
}

func TestConnTrackerCloseByIDClosesTrackedConnection(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	base := &testConn{}
	wrapped := tracker.RoutedConnection(context.Background(), base, testInboundContext("uuid-1", "1.1.1.1"), nil, nil)
	tracked, ok := wrapped.(*trackedConn)
	if !ok {
		t.Fatalf("wrapped type = %T, want *trackedConn", wrapped)
	}
	if !tracker.CloseByID(tracked.connID) {
		t.Fatal("CloseByID() = false, want true")
	}
	if !base.closed {
		t.Fatal("expected underlying connection to be closed")
	}
}

func TestTrackedPacketConnUnwrapDirection(t *testing.T) {
	// Reading a packet from the inbound = user upload; writing a packet to the
	// inbound = user download. Regression guard for the previously-swapped
	// UnwrapPacketReader/UnwrapPacketWriter counters.
	tracker := NewConnTracker(0)
	us := &userStats{ips: make(map[string]int)}
	c := &trackedPacketConn{tracker: tracker, us: us, ctx: context.Background()}

	_, readFns := c.UnwrapPacketReader()
	if len(readFns) != 1 {
		t.Fatalf("UnwrapPacketReader funcs = %d, want 1", len(readFns))
	}
	readFns[0](100)
	if got := us.upload.Load(); got != 100 {
		t.Fatalf("after packet read, upload = %d, want 100", got)
	}
	if got := us.download.Load(); got != 0 {
		t.Fatalf("after packet read, download = %d, want 0", got)
	}

	_, writeFns := c.UnwrapPacketWriter()
	if len(writeFns) != 1 {
		t.Fatalf("UnwrapPacketWriter funcs = %d, want 1", len(writeFns))
	}
	writeFns[0](50)
	if got := us.download.Load(); got != 50 {
		t.Fatalf("after packet write, download = %d, want 50", got)
	}
	if got := us.upload.Load(); got != 100 {
		t.Fatalf("upload changed unexpectedly = %d, want 100", got)
	}
}

func TestWaitLimitDoesNotHangWhenAmountExceedsBurst(t *testing.T) {
	// ReserveN with n > burst returns an infinite delay; waitLimit must chunk
	// the amount so it never blocks forever.
	lim := rate.NewLimiter(rate.Limit(1<<30), 64*1024) // 1 GiB/s, 64 KiB burst
	done := make(chan struct{})
	go func() {
		waitLimit(context.Background(), lim, 4<<20) // 4 MiB, far exceeds burst
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("waitLimit hung on an amount larger than the burst")
	}
}

func TestWaitLimitReturnsOnContextCancel(t *testing.T) {
	lim := rate.NewLimiter(rate.Limit(1), 1) // 1 B/s: any real wait is long
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		waitLimit(ctx, lim, 1<<20)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("waitLimit did not return promptly after context cancel")
	}
}

func TestTrafficStatsBoundedUnderKeyExplosion(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetTrafficStatsEnabled(true)

	const n = 200000
	var total int64
	for i := 0; i < n; i++ {
		key := trafficStatKey{
			userID:      1,
			sourceIP:    "1.1.1.1",
			destination: "host-" + strconv.Itoa(i) + ".example.com",
			category:    "domain",
		}
		tracker.addTrafficStat(key, 1, 0)
		total++
	}

	retained := 0
	for i := range tracker.trafficStatShards {
		retained += len(tracker.trafficStatShards[i].items)
	}
	if retained >= n {
		t.Fatalf("retained %d keys, expected bounded folding well below %d", retained, n)
	}
	maxExpected := trafficStatShardCount * (maxTrafficStatEntriesPerShard + 1)
	if retained > maxExpected {
		t.Fatalf("retained %d keys exceeds cap %d", retained, maxExpected)
	}

	// Folding into the per-user "other" bucket must preserve totals.
	stats := tracker.FlushTrafficStats()
	var sum int64
	for _, s := range stats {
		sum += s.U
	}
	if sum != total {
		t.Fatalf("flushed upload sum = %d, want %d (folding must preserve totals)", sum, total)
	}
}

func TestConnTrackerRateLimitHonorsContextCancellation(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"uuid-1": 1})
	tracker.SetSpeedLimitFunc(func(uuid string) *rate.Limiter {
		if uuid != "uuid-1" {
			return nil
		}
		return rate.NewLimiter(rate.Limit(1), 1)
	})

	readCtx, cancelRead := context.WithCancel(context.Background())
	readBase := &testConn{reads: [][]byte{[]byte("a"), []byte("a")}}
	readTracked := tracker.RoutedConnection(readCtx, readBase, testInboundContext("uuid-1", "1.1.1.1"), nil, nil).(*trackedConn)
	buf := make([]byte, 8)
	if n, err := readTracked.Read(buf); err != nil || n != 1 {
		t.Fatalf("first Read() = (%d, %v), want (1, nil)", n, err)
	}
	cancelRead()
	if n, err := readTracked.Read(buf); !errors.Is(err, context.Canceled) || n != 1 {
		t.Fatalf("second Read() = (%d, %v), want (1, context canceled)", n, err)
	}

	writeCtx, cancelWrite := context.WithCancel(context.Background())
	writeBase := &testConn{}
	writeTracked := tracker.RoutedConnection(writeCtx, writeBase, testInboundContext("uuid-1", "1.1.1.2"), nil, nil).(*trackedConn)
	if n, err := writeTracked.Write([]byte("a")); err != nil || n != 1 {
		t.Fatalf("first Write() = (%d, %v), want (1, nil)", n, err)
	}
	cancelWrite()
	if n, err := writeTracked.Write([]byte("a")); !errors.Is(err, context.Canceled) || n != 0 {
		t.Fatalf("second Write() = (%d, %v), want (0, context canceled)", n, err)
	}
}
