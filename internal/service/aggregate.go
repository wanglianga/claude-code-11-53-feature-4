package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"agrisync/internal/model"
)

// AggregateRun computes in-region aggregate metrics, replicates only the
// aggregate snapshot, and reports which regions were actually included in the
// group-visible result. Region lag is explicit: a region whose snapshot has
// not been confirmed is excluded and listed with its last confirmed cutoff.
func (s *Service) ComputeAggregate(ctx context.Context, approvalID string, from, to time.Time, metricKeys []string, actor, role string) (model.AggregateRun, error) {
	if !to.After(from) {
		return model.AggregateRun{}, fmt.Errorf("%w: period_to must be after period_from", ErrInvalid)
	}
	metrics, err := normalizeMetrics(metricKeys)
	if err != nil {
		return model.AggregateRun{}, err
	}
	approval, err := s.GetApproval(ctx, approvalID)
	if err != nil {
		return model.AggregateRun{}, err
	}
	if approval.Status != "approved" {
		return model.AggregateRun{}, fmt.Errorf("%w: approval %s is %s, not approved", ErrForbidden, approval.ID, approval.Status)
	}
	now := s.now().UTC()
	if approval.ValidFrom != nil && now.Before(*approval.ValidFrom) {
		return model.AggregateRun{}, fmt.Errorf("%w: approval not yet valid", ErrForbidden)
	}
	if approval.ValidUntil != nil && now.After(*approval.ValidUntil) {
		return model.AggregateRun{}, fmt.Errorf("%w: approval expired", ErrForbidden)
	}
	scopeRegions := approval.ScopeRegions
	if len(scopeRegions) == 0 {
		return model.AggregateRun{}, fmt.Errorf("%w: approval has empty region scope", ErrInvalid)
	}
	missing := []string{}
	for _, m := range metrics {
		if !containsString(approval.ScopeMetrics, m) {
			missing = append(missing, m)
		}
	}
	if len(missing) > 0 {
		return model.AggregateRun{}, fmt.Errorf("%w: metrics outside approval scope: %v", ErrForbidden, missing)
	}

	runID := randID("run")
	coverage := model.Coverage{Regions: []model.RegionMetric{}, IncludedRegions: []string{}, ExcludedRegions: []string{}, StaleRegions: []string{}}

	for _, region := range scopeRegions {
		// In-region computation touches only the regional database.
		snap, err := s.computeRegionSnapshot(ctx, region, runID, from, to, metrics)
		if err != nil {
			return model.AggregateRun{}, err
		}
		rm := model.RegionMetric{RegionCode: region, SnapshotID: snap.SnapshotID, DataCutoff: snap.DataCutoff, Metrics: snap.Metrics}
		coverage.Regions = append(coverage.Regions, rm)
	}

	// Policy-filtered async replication of the aggregate snapshots only.
	for _, region := range scopeRegions {
		if _, err := s.Replicate(ctx, region, ""); err != nil {
			// Keep going: an offline region becomes an explicitly excluded
			// (stale) region rather than failing the whole group report.
			if !errors.Is(err, ErrUnavailable) {
				return model.AggregateRun{}, err
			}
		}
	}

	total := model.Metrics{}
	var earliestCutoff time.Time
	for i := range coverage.Regions {
		rm := &coverage.Regions[i]
		var confirmed model.Metrics
		var confirmedCutoff time.Time
		var snapID string
		var sourceSeq int64
		err := s.db.Control().QueryRow(ctx, `
			select snapshot_id, data_cutoff, metrics, source_seq from replica_metrics
			where source_region=$1 and snapshot_id=$2 and status='confirmed'`,
			rm.RegionCode, rm.SnapshotID).Scan(&snapID, &confirmedCutoff, &confirmed, &sourceSeq)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return model.AggregateRun{}, err
			}
			// Not confirmed: report the last confirmed snapshot's cutoff so
			// the group can see exactly how far behind the region is.
			var lastCutoff *time.Time
			_ = s.db.Control().QueryRow(ctx, `
				select max(data_cutoff) from replica_metrics
				where source_region=$1 and status='confirmed'`, rm.RegionCode).Scan(&lastCutoff)
			rm.Included = false
			rm.Reason = "replication_not_confirmed"
			if lastCutoff != nil {
				rm.DataCutoff = *lastCutoff
			}
			rm.LagSeconds = int64(now.Sub(rm.DataCutoff).Seconds())
			if rm.LagSeconds < 0 {
				rm.LagSeconds = 0
			}
			coverage.ExcludedRegions = append(coverage.ExcludedRegions, rm.RegionCode)
			coverage.StaleRegions = append(coverage.StaleRegions, rm.RegionCode)
			continue
		}
		rm.Included = true
		rm.DataCutoff = confirmedCutoff
		rm.Metrics = confirmed
		// The exact replication version the group result is based on.
		rm.ReplicationSeq = sourceSeq
		rm.LagSeconds = int64(now.Sub(confirmedCutoff).Seconds())
		if rm.LagSeconds < 0 {
			rm.LagSeconds = 0
		}
		coverage.IncludedRegions = append(coverage.IncludedRegions, rm.RegionCode)
		if earliestCutoff.IsZero() || confirmedCutoff.Before(earliestCutoff) {
			earliestCutoff = confirmedCutoff
		}
		total.OperationCount += confirmed.OperationCount
		total.CompletedCount += confirmed.CompletedCount
		total.AreaHa += confirmed.AreaHa
		total.FuelL += confirmed.FuelL
		total.DurationHours += confirmed.DurationHours
	}
	if total.OperationCount > 0 {
		total.CompletionRate = round2(float64(total.CompletedCount) / float64(total.OperationCount) * 100)
	}
	total.AreaHa = round2(total.AreaHa)
	total.FuelL = round2(total.FuelL)
	total.DurationHours = round2(total.DurationHours)
	if earliestCutoff.IsZero() {
		earliestCutoff = now
	}
	coverage.DataCutoff = earliestCutoff
	coverage.GeneratedAt = now

	run := model.AggregateRun{
		ID: runID, ApprovalID: approval.ID, ApprovalVersion: approval.Version,
		PeriodFrom: from.UTC(), PeriodTo: to.UTC(), MetricKeys: metrics, Status: "computed",
		ScopeSnapshot: scopeRegions, Coverage: coverage, Result: total,
		DataCutoff: earliestCutoff, RequestedBy: actor, CreatedAt: now,
	}
	tx, err := s.db.Control().Begin(ctx)
	if err != nil {
		return model.AggregateRun{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		insert into aggregate_runs (id, approval_id, period_from, period_to, metric_keys, status,
			scope_snapshot, coverage, result, data_cutoff, approval_version, requested_by, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		run.ID, run.ApprovalID, run.PeriodFrom, run.PeriodTo, run.MetricKeys, run.Status,
		jsonBytes(run.ScopeSnapshot), jsonBytes(run.Coverage), jsonBytes(run.Result),
		run.DataCutoff, run.ApprovalVersion, run.RequestedBy, run.CreatedAt); err != nil {
		return model.AggregateRun{}, err
	}
	for _, rm := range coverage.Regions {
		if _, err := tx.Exec(ctx, `
			insert into aggregate_run_regions (run_id, region_code, included, exclusion_reason,
				replication_seq, snapshot_id, data_cutoff, metrics)
			values ($1,$2,$3,$4,$5,$6,$7,$8)`,
			run.ID, rm.RegionCode, rm.Included, nullStr(rm.Reason), rm.ReplicationSeq,
			rm.SnapshotID, rm.DataCutoff, jsonBytes(rm.Metrics)); err != nil {
			return model.AggregateRun{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return model.AggregateRun{}, err
	}
	_, err = s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: "aggregate.compute", SubjectType: "aggregate_run",
		SubjectID: run.ID, Decision: "allow",
		Detail: map[string]any{
			"approval_id": approval.ID, "approval_version": approval.Version,
			"included_regions": coverage.IncludedRegions, "excluded_regions": coverage.ExcludedRegions,
			"data_cutoff": earliestCutoff, "metrics": metrics,
		},
	})
	if err != nil {
		return model.AggregateRun{}, err
	}
	return run, nil
}

func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

type regionSnapshot struct {
	SnapshotID string
	DataCutoff time.Time
	Metrics    model.Metrics
}

// computeRegionSnapshot reads the region's own authoritative records and
// writes an aggregate snapshot into the regional database plus one
// aggregate_metric outbox event. No raw point ever leaves the region here.
func (s *Service) computeRegionSnapshot(ctx context.Context, region, runID string, from, to time.Time, keys []string) (regionSnapshot, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return regionSnapshot{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	var out regionSnapshot
	var m model.Metrics
	var cutoff time.Time
	err := pool.QueryRow(ctx, `
		select count(*),
		       count(*) filter (where completed),
		       coalesce(sum(area_ha),0),
		       coalesce(sum(fuel_l),0),
		       coalesce(sum(duration_s),0),
		       coalesce(max(received_at), now())
		from operation_events where started_at >= $1 and started_at < $2`,
		from.UTC(), to.UTC()).
		Scan(&m.OperationCount, &m.CompletedCount, &m.AreaHa, &m.FuelL, &m.DurationHours, &cutoff)
	if err != nil {
		return out, err
	}
	m.AreaHa = round2(m.AreaHa)
	m.FuelL = round2(m.FuelL)
	m.DurationHours = round2(m.DurationHours / 3600)
	if m.OperationCount > 0 {
		m.CompletionRate = round2(float64(m.CompletedCount) / float64(m.OperationCount) * 100)
	}
	out.SnapshotID = fmt.Sprintf("snap_%s_%s", runID, sanitize(region))
	out.DataCutoff = cutoff.UTC()
	out.Metrics = m

	tx, err := pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	var node string
	var epoch, nextOutbox int64
	if err := tx.QueryRow(ctx, `
		select active_node, local_epoch, next_outbox_seq from region_state where id for update`).
		Scan(&node, &epoch, &nextOutbox); err != nil {
		return out, err
	}
	payload := map[string]any{
		"snapshot_id": out.SnapshotID, "period_from": from.UTC(), "period_to": to.UTC(),
		"data_cutoff": out.DataCutoff, "computed_at": s.now().UTC(), "metrics": m,
	}
	if _, err := tx.Exec(ctx, `
		insert into metric_snapshots (snapshot_id, period_from, period_to, data_cutoff,
			computed_at, metrics, source_node, source_epoch, outbox_seq)
		values ($1,$2,$3,$4,now(),$5,$6,$7,$8)
		on conflict (snapshot_id) do nothing`,
		out.SnapshotID, from.UTC(), to.UTC(), out.DataCutoff, jsonBytes(m), node, epoch, nextOutbox); err != nil {
		return out, err
	}
	if err := insertOutbox(ctx, tx, nextOutbox, "metric_snapshot", out.SnapshotID,
		"aggregate_metric", payload, node, epoch); err != nil {
		return out, err
	}
	if _, err := tx.Exec(ctx, `update region_state set next_outbox_seq=$1, updated_at=now() where id`,
		nextOutbox+1); err != nil {
		return out, err
	}
	if err := tx.Commit(ctx); err != nil {
		return out, err
	}
	return out, nil
}

func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+32)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

func (s *Service) GetAggregate(ctx context.Context, id string) (model.AggregateRun, error) {
	var run model.AggregateRun
	var scope, coverage, result []byte
	err := s.db.Control().QueryRow(ctx, `
		select id, approval_id, approval_version, period_from, period_to, metric_keys, status,
		       scope_snapshot, coverage, result, data_cutoff, requested_by, created_at
		from aggregate_runs where id=$1`, id).
		Scan(&run.ID, &run.ApprovalID, &run.ApprovalVersion, &run.PeriodFrom, &run.PeriodTo,
			&run.MetricKeys, &run.Status, &scope, &coverage, &result, &run.DataCutoff,
			&run.RequestedBy, &run.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return run, fmt.Errorf("%w: aggregate run %s", ErrNotFound, id)
	}
	if err != nil {
		return run, err
	}
	_ = json.Unmarshal(scope, &run.ScopeSnapshot)
	_ = json.Unmarshal(coverage, &run.Coverage)
	_ = json.Unmarshal(result, &run.Result)
	dels, err := s.listDeliveries(ctx, id)
	if err != nil {
		return run, err
	}
	run.Deliveries = dels
	return run, nil
}

func (s *Service) ListAggregates(ctx context.Context, limit int) ([]model.AggregateRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.Control().Query(ctx, `
		select id from aggregate_runs order by created_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	out := []model.AggregateRun{}
	for _, id := range ids {
		run, err := s.GetAggregate(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, nil
}

// CountGroupReplicas is a diagnostic for the verification script: it shows how
// many raw trajectory replicas exist and for which regions.
type ReplicaStats struct {
	Summaries    map[string]int `json:"operation_summaries_by_region"`
	Trajectories map[string]int `json:"raw_trajectories_by_region"`
	Alarms       map[string]int `json:"device_alarms_by_region"`
	Metrics      map[string]int `json:"aggregate_metrics_by_region"`
	Unconfirmed  map[string]int `json:"unconfirmed_by_region"`
}

func (s *Service) ReplicaStats(ctx context.Context) (ReplicaStats, error) {
	out := ReplicaStats{
		Summaries: map[string]int{}, Trajectories: map[string]int{}, Alarms: map[string]int{},
		Metrics: map[string]int{}, Unconfirmed: map[string]int{},
	}
	queries := []struct {
		q  string
		to map[string]int
	}{
		{`select source_region, count(*) from replica_op_summaries group by 1`, out.Summaries},
		{`select source_region, count(*) from replica_trajectories group by 1`, out.Trajectories},
		{`select source_region, count(*) from replica_alarms group by 1`, out.Alarms},
		{`select source_region, count(*) from replica_metrics group by 1`, out.Metrics},
		{`select source_region, count(*) from (
		   select source_region from replica_op_summaries where status='unconfirmed'
		   union all select source_region from replica_trajectories where status='unconfirmed'
		   union all select source_region from replica_metrics where status='unconfirmed') x group by 1`, out.Unconfirmed},
	}
	for _, q := range queries {
		rows, err := s.db.Control().Query(ctx, q.q)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var r string
			var n int
			if err := rows.Scan(&r, &n); err != nil {
				rows.Close()
				return out, err
			}
			q.to[r] = n
		}
		rows.Close()
	}
	return out, nil
}
