package limiter

import (
	"sync"
	"sync/atomic"

	"github.com/cedar2025/xboard-node/internal/panel"
	"golang.org/x/time/rate"
)

// SpeedTrackerLogCallback is called when bucket updates occur.
type SpeedTrackerLogCallback func(msg string)

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

	// Optional callback for logging
	logFunc SpeedTrackerLogCallback
}

// NewSpeedTracker creates a bucket manager for per-user bandwidth throttling.
func NewSpeedTracker(l *Limiter) *SpeedTracker {
	return &SpeedTracker{
		limiter: l,
		buckets: make(map[int]*rate.Limiter),
		uuidMap: make(map[string]int),
	}
}

// SetLogCallback sets the logging callback.
func (t *SpeedTracker) SetLogCallback(f SpeedTrackerLogCallback) {
	t.logFunc = f
}

// UpdateBuckets performs an incremental update of rate limiter buckets.
// It reuses existing rate.Limiter instances to avoid burst resets and
// minimizes memory allocations.
func (t *SpeedTracker) UpdateBuckets() {
	currentUsers := make([]panel.User, 0, 32)
	t.limiter.mu.RLock()
	for _, u := range t.limiter.users {
		currentUsers = append(currentUsers, u)
	}
	t.limiter.mu.RUnlock()

	func() {
		t.mu.Lock()
		defer t.mu.Unlock()

		newUUIDMap := make(map[string]int, len(currentUsers))
		activeIDs := make(map[int]struct{}, len(currentUsers))

		for _, user := range currentUsers {
			activeIDs[user.ID] = struct{}{}
			if user.UUID != "" {
				newUUIDMap[user.UUID] = user.ID
			}

			if user.SpeedLimit <= 0 {
				delete(t.buckets, user.ID)
				continue
			}

			bytesPerSec := int(user.SpeedLimit) * 1_000_000 / 8
			burst := bytesPerSec
			if burst < 64*1024 {
				burst = 64 * 1024
			}
			if cap4s := bytesPerSec * 4; cap4s > 64*1024 && burst > cap4s {
				burst = cap4s
			}

			if existing, ok := t.buckets[user.ID]; ok {
				existing.SetLimit(rate.Limit(bytesPerSec))
				existing.SetBurst(burst)
			} else {
				t.buckets[user.ID] = rate.NewLimiter(rate.Limit(bytesPerSec), burst)
			}
		}

		for id := range t.buckets {
			if _, ok := activeIDs[id]; !ok {
				delete(t.buckets, id)
			}
		}

		t.uuidMap = newUUIDMap
		t.hasLimits.Store(len(t.buckets) > 0)
	}()

	if t.logFunc != nil {
		t.logFunc("buckets updated")
	}
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
