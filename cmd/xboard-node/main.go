package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/cedar2025/xboard-node/internal/service"
)

var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	configPath := flag.String("c", "config.yml", "config file path")
	showVersion := flag.Bool("v", false, "show version")
	flag.Parse()

	if *showVersion {
		fmt.Printf("xboard-node %s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	config.InitLogger(cfg.Log)

	// Apply runtime memory tuning before anything else allocates.
	applyRuntimeConfig(cfg.Runtime)

	nodes := cfg.ExpandNodes()
	nlog.Core().Info(fmt.Sprintf("xboard-node %s starting, %d nodes", version, len(nodes)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		nlog.Core().Info(fmt.Sprintf("received %v, shutting down...", sig))
		cancel()

		// Force-exit on second signal or after timeout.
		select {
		case sig = <-sigCh:
			slog.Warn("received second signal, forcing exit", "signal", sig)
			os.Exit(1)
		case <-time.After(15 * time.Second):
			slog.Error("shutdown timed out after 15s, forcing exit")
			os.Exit(2)
		}
	}()

	// Optional health-check endpoint for container orchestrators.
	if cfg.HealthPort > 0 {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.HealthPort))
		if err != nil {
			slog.Error("failed to start health check listener", "port", cfg.HealthPort, "error", err)
			os.Exit(1)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		})
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			nlog.Core().Debug(fmt.Sprintf("health check listening on :%d", cfg.HealthPort))
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				slog.Warn("health check server stopped", "error", err)
			}
		}()
		go func() {
			<-ctx.Done()
			srv.Close()
		}()
	}

	// Run one service per node. If any node fails, cancel all others.
	errCh := make(chan error, len(nodes))
	var wg sync.WaitGroup
	for _, nodeCfg := range nodes {
		nodeCfg := nodeCfg // capture
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc := service.New(nodeCfg)
			if err := svc.Run(ctx); err != nil {
				slog.Error("node service exited with error",
					"node_id", nodeCfg.Panel.NodeID, "error", err)
				errCh <- err
				cancel() // bring down all other nodes
			}
		}()
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		os.Exit(1)
	}
	nlog.Core().Info("stopped")
}

// applyRuntimeConfig wires up Go runtime memory limits from the config file.
// Both settings can also be overridden by environment variables (GOMEMLIMIT /
// GOGC) — the env vars take precedence because Go's runtime reads them before
// we can call these functions, but we set them here for completeness and so
// the values are logged.
func applyRuntimeConfig(rt config.RuntimeConfig) {
	// GOGC
	if rt.GoGCPercent > 0 {
		prev := debug.SetGCPercent(rt.GoGCPercent)
		slog.Info("runtime: GOGC set", "gogc", rt.GoGCPercent, "prev", prev)
	}

	// GOMEMLIMIT — parse human-readable size string (e.g. "30MiB")
	if rt.GoMemLimit != "" {
		limit, err := parseMemLimit(rt.GoMemLimit)
		if err != nil {
			slog.Warn("runtime: invalid gomemlimit, ignoring", "value", rt.GoMemLimit, "error", err)
		} else {
			prev := debug.SetMemoryLimit(limit)
			slog.Info("runtime: GOMEMLIMIT set",
				"limit", rt.GoMemLimit,
				"bytes", limit,
				"prev_bytes", prev,
			)
		}
	}
}

// parseMemLimit converts a human-readable size string to bytes.
// Supported suffixes: B, KiB, MiB, GiB, TiB (case-insensitive).
func parseMemLimit(s string) (int64, error) {
	s = strings.TrimSpace(s)
	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"TiB", 1 << 40},
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"B", 1},
	}
	upper := strings.ToUpper(s)
	for _, sf := range suffixes {
		if strings.HasSuffix(upper, strings.ToUpper(sf.suffix)) {
			numStr := strings.TrimSuffix(upper, strings.ToUpper(sf.suffix))
			numStr = strings.TrimSpace(numStr)
			var n int64
			if _, err := fmt.Sscanf(numStr, "%d", &n); err != nil {
				return 0, fmt.Errorf("parse number %q: %w", numStr, err)
			}
			return n * sf.mult, nil
		}
	}
	// No suffix: treat as raw bytes.
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("unrecognised size format %q", s)
	}
	return n, nil
}
