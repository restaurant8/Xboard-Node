package controlplane

import (
	"context"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/model"
)

type LocalControlPlane struct {
	cfg *config.Config
}

func NewLocalControlPlane(cfg *config.Config) *LocalControlPlane { return &LocalControlPlane{cfg: cfg} }
func (l *LocalControlPlane) SupportsPolling() bool               { return false }
func (l *LocalControlPlane) SupportsDiscovery() bool             { return false }
func (l *LocalControlPlane) SupportsReporting() bool             { return false }
func (l *LocalControlPlane) SupportsDeviceReports() bool         { return false }

func (l *LocalControlPlane) Initial(ctx context.Context, _ func() map[string]interface{}, _ chan<- Event, _ chan<- StatusChange) (Bootstrap, error) {
	select {
	case <-ctx.Done():
		return Bootstrap{}, ctx.Err()
	default:
	}
	return Bootstrap{
		PushInterval: l.cfg.Node.PushInterval,
		PullInterval: l.cfg.Node.PullInterval,
		Config:       model.NodeSpecFromStandalone(l.cfg),
		Users:        model.UserSpecsFromStandalone(l.cfg),
	}, nil
}

func (l *LocalControlPlane) Poll(ctx context.Context) (Snapshot, error) {
	select {
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	default:
		return Snapshot{}, nil
	}
}

func (l *LocalControlPlane) Discover(ctx context.Context, _ func() map[string]interface{}, _ chan<- Event, _ chan<- StatusChange) (PushClient, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, nil
	}
}

func (l *LocalControlPlane) Report(payload ReportPayload) error { _ = payload; return nil }
func (l *LocalControlPlane) ReportDevices(push PushClient, devices map[int][]string) {
	_, _ = push, devices
}
func (l *LocalControlPlane) Metrics() APIMetrics { return APIMetrics{} }
