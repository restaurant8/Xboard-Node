package limiter

import (
	"sync"
	"sync/atomic"

	"golang.org/x/time/rate"
)

// SpeedTracker manages per-user token-bucket rate limiters.
// It does NOT wrap connections itself — instead, ConnTracker consults it
// via GetLimiter to embed rate limiting in the same tracked connection
// wrapper that does byte counting.
type SpeedTracker struct {
	limiter *Limiter
	mu      sync.RWMutex
	buckets map[int]*rate.Limiter // userID → shared rate limiter
	uuidMap map[string]int        // UUID → userID

	// Fast-path: when no users have a speed limit, GetLimiter returns nil
	// immediately without any map lookup.
	hasLimits atomic.Bool
}

// NewSpeedTracker creates a bucket manager for per-user bandwidth throttling.
func NewSpeedTracker(l *Limiter) *SpeedTracker {
	return &SpeedTracker{
		limiter: l,
		buckets: make(map[int]*rate.Limiter),
		uuidMap: make(map[string]int),
	}
}

// UpdateBuckets rebuilds rate limiter buckets based on current user config.
// Call this whenever the user list changes.
func (t *SpeedTracker) UpdateBuckets() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.limiter.mu.RLock()
	defer t.limiter.mu.RUnlock()

	newBuckets := make(map[int]*rate.Limiter, len(t.limiter.users))
	newUUIDMap := make(map[string]int, len(t.limiter.users))
	for uid, user := range t.limiter.users {
		if user.UUID != "" {
			newUUIDMap[user.UUID] = uid
		}
		if user.SpeedLimit <= 0 {
			continue
		}
		bytesPerSec := int(user.SpeedLimit) * 1_000_000 / 8 // Mbps → bytes/s
		// Burst = 1 second of data, floored at 64 KiB, capped at 4 s.
		// A generous burst ensures downloads ramp up quickly (good UX) while
		// the token bucket still converges to the target rate within seconds.
		// The 4 s cap prevents excessive burst at very high limits.
		burst := bytesPerSec
		if burst < 64*1024 {
			burst = 64 * 1024
		}
		if cap4s := bytesPerSec * 4; cap4s > 64*1024 && burst > cap4s {
			burst = cap4s
		}
		if existing, ok := t.buckets[uid]; ok {
			existing.SetLimit(rate.Limit(bytesPerSec))
			existing.SetBurst(burst)
			newBuckets[uid] = existing
		} else {
			newBuckets[uid] = rate.NewLimiter(rate.Limit(bytesPerSec), burst)
		}
	}
	t.buckets = newBuckets
	t.uuidMap = newUUIDMap
	t.hasLimits.Store(len(newBuckets) > 0)
}

// GetLimiter returns the rate limiter for the given user UUID, or nil if
// no limit applies. This is called by ConnTracker for every new connection.
// Thread-safe.
func (t *SpeedTracker) GetLimiter(user string) *rate.Limiter {
	if !t.hasLimits.Load() {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.buckets[t.uuidMap[user]]
}

// HasLimits returns true if any user currently has a speed limit configured.
func (t *SpeedTracker) HasLimits() bool {
	return t.hasLimits.Load()
}

// LimitedUserCount returns the number of users with active speed limits.
func (t *SpeedTracker) LimitedUserCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.buckets)
}
