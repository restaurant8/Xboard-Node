package monitor

import (
	"runtime"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

var startTime = time.Now()

func init() {
	// Warm up the CPU sampler. The first cpu.Percent call with interval=0
	// always returns 0% because it has no prior sample. This throwaway call
	// seeds the baseline so subsequent Collect() calls return real values.
	cpu.Percent(500*time.Millisecond, false)
}

// Status holds system resource metrics
type Status struct {
	Uptime     uint64
	CPU        float64
	CPUPerCore []float64
	Load1      float64
	Load5      float64
	Load15     float64
	MemTotal   uint64
	MemUsed    uint64
	SwapTotal  uint64
	SwapUsed   uint64
	DiskTotal  uint64
	DiskUsed   uint64
	Goroutines int

	// GC metrics (process-wide)
	NumGC       uint32
	LastPauseMS float64
}

// Collect gathers current system metrics
func Collect() Status {
	var s Status

	s.Uptime = uint64(time.Since(startTime).Seconds())

	if cpuPercent, err := cpu.Percent(0, false); err == nil && len(cpuPercent) > 0 {
		s.CPU = cpuPercent[0]
	} else if err != nil {
		nlog.Core().Debug("failed to get CPU usage", "error", err)
	}

	// Per-core CPU usage (best-effort; safe if it fails).
	if perCore, err := cpu.Percent(0, true); err == nil && len(perCore) > 0 {
		s.CPUPerCore = perCore
	}

	if loadAvg, err := load.Avg(); err == nil {
		s.Load1 = loadAvg.Load1
		s.Load5 = loadAvg.Load5
		s.Load15 = loadAvg.Load15
	}

	if vmStat, err := mem.VirtualMemory(); err == nil {
		s.MemTotal = vmStat.Total
		s.MemUsed = vmStat.Used
	}

	if swapStat, err := mem.SwapMemory(); err == nil {
		s.SwapTotal = swapStat.Total
		s.SwapUsed = swapStat.Used
	}

	if diskStat, err := disk.Usage("/"); err == nil {
		s.DiskTotal = diskStat.Total
		s.DiskUsed = diskStat.Used
	}

	// GC metrics
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.Goroutines = runtime.NumGoroutine()
	s.NumGC = ms.NumGC
	if ms.NumGC > 0 {
		// PauseNs is a ring buffer of the most recent GC pause times.
		idx := (ms.NumGC - 1) % uint32(len(ms.PauseNs))
		s.LastPauseMS = float64(ms.PauseNs[idx]) / 1e6
	}

	return s
}
