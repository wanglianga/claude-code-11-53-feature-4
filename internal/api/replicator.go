package api

import (
	"context"
	"log/slog"
	"time"

	"agrisync/internal/service"
)

// Replicator runs policy-filtered replication on a timer. It is best-effort
// and entirely separate from the region write path: when the control plane is
// unreachable or a link is offline, local ingest keeps committing and the
// cursor simply stays where it was.
type Replicator struct {
	svc      *service.Service
	log      *slog.Logger
	interval time.Duration
}

func NewReplicator(svc *service.Service, log *slog.Logger, interval time.Duration) *Replicator {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Replicator{svc: svc, log: log, interval: interval}
}

func (r *Replicator) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

func (r *Replicator) tick(ctx context.Context) {
	// Serialise the sweep so batches never overlap on the same cursor.
	sweepCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	runs, err := r.svc.ReplicateAll(sweepCtx, nil, "")
	if err != nil {
		r.log.Debug("replication sweep", "error", err)
	}
	copied := 0
	for _, run := range runs {
		for _, n := range run.CopiedByCategory {
			copied += n
		}
	}
	if copied > 0 {
		r.log.Info("replication sweep", "copied_events", copied)
	}
}
