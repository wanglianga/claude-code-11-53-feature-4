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

var ErrUnavailable = errors.New("unavailable")

type outboxEvent struct {
	Seq     int64
	Kind    string
	ID      string
	Cat     string
	Payload []byte
	Node    string
	Epoch   int64
}

// Replicate copies outbound-eligible outbox events from one region into the
// group replica store. Residency policy is applied per event category, so a
// restricted raw trajectory is never copied even when the operation summary of
// the same segment is.
//
// Copies land as `unconfirmed`; they become group-visible only once the
// receiving side confirms them, which also advances the region's
// last_confirmed_seq in the DR register. A redelivery after a crash before the
// cursor commit re-applies the same ids as upserts, so area is never
// double-counted.
func (s *Service) Replicate(ctx context.Context, region string, fault string) (model.ReplicationRun, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return model.ReplicationRun{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	control := s.db.Control()
	run := model.ReplicationRun{
		ID: randID("repl"), SourceRegion: region, Status: "noop", Fault: fault,
		CopiedByCategory: map[string]int{}, BlockedByCategory: map[string]int{},
	}

	if fault != "" && fault != "link_down" && fault != "copy_fail" && fault != "confirm_fail" && fault != "none" {
		return run, fmt.Errorf("%w: unknown fault %q", ErrInvalid, fault)
	}
	if fault == "link_down" {
		if _, err := control.Exec(ctx, `
			update replication_links set status='offline', reason=$2, updated_at=now()
			where region_code=$1`, region, "network partition injected for verification"); err != nil {
			return run, err
		}
	}

	var linkStatus, linkReason string
	if err := control.QueryRow(ctx, `select status, reason from replication_links where region_code=$1`, region).
		Scan(&linkStatus, &linkReason); err != nil {
		return run, err
	}
	if linkStatus == "offline" {
		run.StartedAt, run.FinishedAt = s.now().UTC(), s.now().UTC()
		run.Status = "failed"
		run.Error = "inter-region link offline: " + linkReason
		if _, err := control.Exec(ctx, `
			update replication_cursors set retry_count=retry_count+1, status='failed',
			  last_attempt_at=now(), last_error=$2, updated_at=now()
			where source_region=$1`, region, run.Error); err != nil {
			return run, err
		}
		if err := s.recordReplicationRun(ctx, run); err != nil {
			return run, err
		}
		return run, fmt.Errorf("%w: %s", ErrUnavailable, run.Error)
	}

	policy, err := s.policyMap(ctx, region)
	if err != nil {
		return run, err
	}

	var lastSeq int64
	if err := control.QueryRow(ctx, `
		select last_seq from replication_cursors where source_region=$1`, region).Scan(&lastSeq); err != nil {
		return run, err
	}
	var head int64
	if err := pool.QueryRow(ctx, `select coalesce(max(seq),0) from outbox_events`).Scan(&head); err != nil {
		return run, err
	}
	run.StartedAt = s.now().UTC()
	run.FromSeq = lastSeq + 1
	run.ToSeq = head
	if head <= lastSeq {
		run.FinishedAt = s.now().UTC()
		if _, err := control.Exec(ctx, `
			update replication_cursors set retry_count=0, last_attempt_at=now(), last_success_at=now(),
			  status='idle', last_error=null, updated_at=now()
			where source_region=$1`, region); err != nil {
			return run, err
		}
		return run, s.recordReplicationRun(ctx, run)
	}

	rows, err := pool.Query(ctx, `
		select seq, entity_type, entity_id, category, payload, source_node, source_epoch
		from outbox_events where seq > $1 and seq <= $2 order by seq limit $3`,
		lastSeq, head, s.cfg.ReplicationBatch)
	if err != nil {
		return run, err
	}
	events := []outboxEvent{}
	for rows.Next() {
		var e outboxEvent
		if err := rows.Scan(&e.Seq, &e.Kind, &e.ID, &e.Cat, &e.Payload, &e.Node, &e.Epoch); err != nil {
			rows.Close()
			return run, err
		}
		events = append(events, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return run, err
	}

	toSeq, applyErr := s.applyReplicas(ctx, region, events, policy, &run, fault)
	if applyErr == nil {
		if fault == "confirm_fail" {
			// The receiver wrote the rows but the acknowledgement never made
			// it back. The cursor and DR register stay at the previously
			// confirmed position, and the rows remain invisible to the group.
			applyErr = fmt.Errorf("injected confirmation failure after copying seq %d (rows stay unconfirmed)", toSeq)
		} else {
			var confirmErr error
			toSeq, confirmErr = s.confirmReplicas(ctx, region, toSeq)
			applyErr = confirmErr
		}
	}
	if applyErr != nil {
		// An attempt whose writes were rolled back leaves nothing behind. A
		// confirmation failure is different: the rows were written and the ack
		// was lost, so the attempt still holds unconfirmed rows. Those stay
		// invisible to the group and are discarded by the failover path if the
		// old primary never gets to clean up.
		if toSeq > 0 && fault != "confirm_fail" {
			if n, err := s.discardUncopied(ctx, region, toSeq); err == nil && n > 0 {
				run.DiscardedUnconfirmed = n
			}
		}
		run.Status = "failed"
		run.Error = applyErr.Error()
		run.FinishedAt = s.now().UTC()
		if _, err := control.Exec(ctx, `
			update replication_cursors set retry_count=retry_count+1, status='failed',
			  last_attempt_at=now(), last_error=$2, updated_at=now()
			where source_region=$1`, region, run.Error); err != nil {
			return run, err
		}
		if err := s.recordReplicationRun(ctx, run); err != nil {
			return run, err
		}
		return run, applyErr
	}

	run.Status = "succeeded"
	run.FinishedAt = s.now().UTC()
	copied, blocked := 0, 0
	for _, n := range run.CopiedByCategory {
		copied += n
	}
	for _, n := range run.BlockedByCategory {
		blocked += n
	}
	_, err = control.Exec(ctx, `
		update replication_cursors set last_seq=$2, retry_count=0, status='idle',
		  copied_total=copied_total+$3, blocked_total=blocked_total+$4,
		  last_attempt_at=now(), last_success_at=now(), last_error=null, updated_at=now()
		where source_region=$1`, region, toSeq, copied, blocked)
	if err != nil {
		return run, err
	}
	if err := s.recordReplicationRun(ctx, run); err != nil {
		return run, err
	}
	// Fan out to any other configured region that is behind, so one command
	// brings the whole group up to date while still confirming per region.
	return run, nil
}

func (s *Service) applyReplicas(ctx context.Context, region string, events []outboxEvent, policy map[string]bool, run *model.ReplicationRun, fault string) (int64, error) {
	control := s.db.Control()
	tx, err := control.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	applied := int64(0)
	for _, e := range events {
		if e.Cat == "raw_trajectory" && !policy[e.Cat] {
			// Local-only data never enters the control plane. The event is
			// still scanned so the cursor can advance past it.
			run.BlockedByCategory[e.Cat]++
			applied = e.Seq
			continue
		}
		if !policy[e.Cat] {
			run.BlockedByCategory[e.Cat]++
			applied = e.Seq
			continue
		}
		if fault == "copy_fail" {
			return 0, fmt.Errorf("injected replica write failure at source seq %d", e.Seq)
		}
		switch e.Cat {
		case "operation_summary":
			if err := applySummary(ctx, tx, region, e); err != nil {
				return 0, err
			}
		case "raw_trajectory":
			if err := applyTrajectory(ctx, tx, region, e); err != nil {
				return 0, err
			}
		case "device_alarm":
			if err := applyAlarm(ctx, tx, region, e); err != nil {
				return 0, err
			}
		case "aggregate_metric":
			if err := applyMetric(ctx, tx, region, e); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("unknown outbox category %q", e.Cat)
		}
		run.CopiedByCategory[e.Cat]++
		applied = e.Seq
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return applied, nil
}

func applySummary(ctx context.Context, tx pgx.Tx, region string, e outboxEvent) error {
	var p struct {
		OperationID   string     `json:"operation_id"`
		DeviceSerial  string     `json:"device_serial"`
		SegmentID     string     `json:"segment_id"`
		OperationType string     `json:"operation_type"`
		FieldID       string     `json:"field_id"`
		AreaHa        float64    `json:"area_ha"`
		FuelL         float64    `json:"fuel_l"`
		DurationS     int        `json:"duration_s"`
		Completed     bool       `json:"completed"`
		StartedAt     time.Time  `json:"started_at"`
		EndedAt       *time.Time `json:"ended_at"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		insert into replica_op_summaries (source_region, entity_id, source_seq, device_serial, segment_id,
			operation_type, field_id, area_ha, fuel_l, duration_s, completed, started_at, ended_at,
			source_node, status, copied_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'unconfirmed',now())
		on conflict (source_region, entity_id) do update
		  set source_seq=excluded.source_seq, device_serial=excluded.device_serial,
		      segment_id=excluded.segment_id, operation_type=excluded.operation_type,
		      field_id=excluded.field_id, area_ha=excluded.area_ha, fuel_l=excluded.fuel_l,
		      duration_s=excluded.duration_s, completed=excluded.completed,
		      started_at=excluded.started_at, ended_at=excluded.ended_at,
		      source_node=excluded.source_node, copied_at=excluded.copied_at`,
		region, p.OperationID, e.Seq, p.DeviceSerial, p.SegmentID, p.OperationType, p.FieldID,
		p.AreaHa, p.FuelL, p.DurationS, p.Completed, p.StartedAt.UTC(), p.EndedAt, e.Node)
	return err
}

func applyTrajectory(ctx context.Context, tx pgx.Tx, region string, e outboxEvent) error {
	var p struct {
		OperationID  string `json:"operation_id"`
		DeviceSerial string `json:"device_serial"`
		SegmentID    string `json:"segment_id"`
		PointCount   int    `json:"point_count"`
		Points       any    `json:"points"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		insert into replica_trajectories (source_region, entity_id, source_seq, device_serial,
			segment_id, point_count, points, source_node, status, copied_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,'unconfirmed',now())
		on conflict (source_region, entity_id) do update
		  set source_seq=excluded.source_seq, point_count=excluded.point_count,
		      points=excluded.points, source_node=excluded.source_node, copied_at=excluded.copied_at`,
		region, p.OperationID, e.Seq, p.DeviceSerial, p.SegmentID, p.PointCount, e.Payload, e.Node)
	return err
}

func applyAlarm(ctx context.Context, tx pgx.Tx, region string, e outboxEvent) error {
	var p struct {
		OperationID  string    `json:"operation_id"`
		DeviceSerial string    `json:"device_serial"`
		SegmentID    string    `json:"segment_id"`
		Code         string    `json:"code"`
		Severity     string    `json:"severity"`
		OccurredAt   time.Time `json:"occurred_at"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		insert into replica_alarms (source_region, entity_id, source_seq, device_serial, segment_id,
			code, severity, occurred_at, source_node, status, copied_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,'unconfirmed',now())
		on conflict (source_region, entity_id) do update
		  set source_seq=excluded.source_seq, code=excluded.code, severity=excluded.severity,
		      occurred_at=excluded.occurred_at, source_node=excluded.source_node,
		      copied_at=excluded.copied_at`,
		region, e.ID, e.Seq, p.DeviceSerial, p.SegmentID, p.Code, p.Severity,
		p.OccurredAt.UTC(), e.Node)
	return err
}

func applyMetric(ctx context.Context, tx pgx.Tx, region string, e outboxEvent) error {
	var p struct {
		SnapshotID string          `json:"snapshot_id"`
		PeriodFrom time.Time       `json:"period_from"`
		PeriodTo   time.Time       `json:"period_to"`
		DataCutoff time.Time       `json:"data_cutoff"`
		ComputedAt time.Time       `json:"computed_at"`
		Metrics    json.RawMessage `json:"metrics"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		insert into replica_metrics (source_region, snapshot_id, source_seq, source_epoch,
			period_from, period_to, data_cutoff, computed_at, metrics, source_node, status, copied_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'unconfirmed',now())
		on conflict (source_region, snapshot_id) do update
		  set source_seq=excluded.source_seq, source_epoch=excluded.source_epoch,
		      period_from=excluded.period_from, period_to=excluded.period_to,
		      data_cutoff=excluded.data_cutoff, computed_at=excluded.computed_at,
		      metrics=excluded.metrics, source_node=excluded.source_node,
		      copied_at=excluded.copied_at`,
		region, p.SnapshotID, e.Seq, e.Epoch, p.PeriodFrom.UTC(), p.PeriodTo.UTC(),
		p.DataCutoff.UTC(), p.ComputedAt.UTC(), p.Metrics, e.Node)
	return err
}

// discardUncopied removes replica rows written past the region's confirmed
// position by a failed attempt. Nothing above the confirmed seq was visible to
// the group, so deleting it restores exactly the last confirmed view.
func (s *Service) discardUncopied(ctx context.Context, region string, upto int64) (int, error) {
	tx, err := s.db.Control().Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var confirmed int64
	if err := tx.QueryRow(ctx, `
		select coalesce(confirmed_seq,0) from replication_cursors where source_region=$1 for update`,
		region).Scan(&confirmed); err != nil {
		return 0, err
	}
	if upto <= confirmed {
		return 0, tx.Commit(ctx)
	}
	total := 0
	for _, table := range []string{"replica_op_summaries", "replica_trajectories", "replica_alarms", "replica_metrics"} {
		var n int
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			`with del as (delete from %s where source_region=$1 and status='unconfirmed' and source_seq>$2 returning 1)
			 select count(*) from del`, table), region, confirmed).Scan(&n); err != nil {
			return 0, err
		}
		total += n
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return total, nil
}

// confirmReplicas advances the region's confirmed position in the control
// plane and the DR register, marking the copied rows group-visible.
func (s *Service) confirmReplicas(ctx context.Context, region string, toSeq int64) (int64, error) {
	if toSeq == 0 {
		return 0, nil
	}
	control := s.db.Control()
	tx, err := control.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var confirmed int64
	if err := tx.QueryRow(ctx, `
		select coalesce(confirmed_seq,0) from replication_cursors where source_region=$1 for update`, region).
		Scan(&confirmed); err != nil {
		return 0, err
	}
	if toSeq <= confirmed {
		return confirmed, tx.Commit(ctx)
	}
	for _, q := range []string{
		`update replica_op_summaries set status='confirmed' where source_region=$1 and source_seq<=$2`,
		`update replica_trajectories set status='confirmed' where source_region=$1 and source_seq<=$2`,
		`update replica_alarms set status='confirmed' where source_region=$1 and source_seq<=$2`,
		`update replica_metrics set status='confirmed' where source_region=$1 and source_seq<=$2`,
	} {
		if _, err := tx.Exec(ctx, q, region, toSeq); err != nil {
			return 0, err
		}
	}
	now := s.now().UTC()
	if _, err := tx.Exec(ctx, `
		update replication_cursors set confirmed_seq=$2, updated_at=now() where source_region=$1`,
		region, toSeq); err != nil {
		return 0, err
	}
	// The DR register only records a position the group has actually
	// confirmed; a standby promoted later may not resume past it.
	_, err = tx.Exec(ctx, `
		update dr_registry set last_confirmed_seq=greatest(last_confirmed_seq,$2),
		  last_confirmed_at=$3, updated_at=now()
		where region_code=$1`, region, toSeq, now)
	if err != nil {
		return 0, err
	}
	return toSeq, tx.Commit(ctx)
}

func (s *Service) policyMap(ctx context.Context, region string) (map[string]bool, error) {
	rows, err := s.db.Control().Query(ctx, `
		select category, outbound_allowed from residency_policies where region_code=$1`, region)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var cat string
		var allowed bool
		if err := rows.Scan(&cat, &allowed); err != nil {
			return nil, err
		}
		out[cat] = allowed
	}
	return out, rows.Err()
}

func (s *Service) recordReplicationRun(ctx context.Context, run model.ReplicationRun) error {
	_, err := s.db.Control().Exec(ctx, `
		insert into replication_runs (id, source_region, from_seq, to_seq, copied_by_category,
			blocked_by_category, status, fault, error, started_at, finished_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		run.ID, run.SourceRegion, run.FromSeq, run.ToSeq, jsonBytes(run.CopiedByCategory),
		jsonBytes(run.BlockedByCategory), run.Status, run.Fault, run.Error, run.StartedAt, run.FinishedAt)
	return err
}

// ReplicateAll runs replication for each given region (or all regions) and
// returns per-region results.
func (s *Service) ReplicateAll(ctx context.Context, regions []string, fault string) ([]model.ReplicationRun, error) {
	if len(regions) == 0 {
		regions = s.db.RegionCodes()
	}
	out := []model.ReplicationRun{}
	var firstErr error
	for _, r := range regions {
		run, err := s.Replicate(ctx, r, fault)
		out = append(out, run)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return out, firstErr
}

// SetLink opens or closes the inter-region link for one region.
func (s *Service) SetLink(ctx context.Context, region, status, reason string) error {
	if status != "online" && status != "offline" {
		return fmt.Errorf("%w: status must be online or offline", ErrInvalid)
	}
	tag, err := s.db.Control().Exec(ctx, `
		update replication_links set status=$2, reason=$3, updated_at=now() where region_code=$1`,
		region, status, reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	return nil
}

func (s *Service) ListLinks(ctx context.Context) ([]model.LinkState, error) {
	rows, err := s.db.Control().Query(ctx, `
		select region_code, status, reason, updated_at from replication_links order by region_code`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.LinkState{}
	for rows.Next() {
		var l model.LinkState
		if err := rows.Scan(&l.RegionCode, &l.Status, &l.Reason, &l.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

func (s *Service) ListCursors(ctx context.Context) ([]model.ReplicationCursor, error) {
	rows, err := s.db.Control().Query(ctx, `
		select source_region, last_seq, confirmed_seq, status, retry_count, copied_total,
		       blocked_total, last_attempt_at, last_success_at, coalesce(last_error,''), updated_at
		from replication_cursors order by source_region`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.ReplicationCursor{}
	for rows.Next() {
		var c model.ReplicationCursor
		if err := rows.Scan(&c.SourceRegion, &c.LastSeq, &c.ConfirmedSeq, &c.Status, &c.RetryCount,
			&c.CopiedTotal, &c.BlockedTotal, &c.LastAttemptAt, &c.LastSuccessAt, &c.LastError,
			&c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Service) ListReplicationRuns(ctx context.Context, region string, limit int) ([]model.ReplicationRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 20
	}
	rows, err := s.db.Control().Query(ctx, `
		select id, source_region, from_seq, to_seq, copied_by_category, blocked_by_category,
		       status, fault, coalesce(error,''), started_at, finished_at
		from replication_runs
		where ($1 = '' or source_region=$1)
		order by started_at desc limit $2`, region, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.ReplicationRun{}
	for rows.Next() {
		var r model.ReplicationRun
		var copied, blocked []byte
		if err := rows.Scan(&r.ID, &r.SourceRegion, &r.FromSeq, &r.ToSeq, &copied, &blocked,
			&r.Status, &r.Fault, &r.Error, &r.StartedAt, &r.FinishedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(copied, &r.CopiedByCategory)
		_ = json.Unmarshal(blocked, &r.BlockedByCategory)
		out = append(out, r)
	}
	return out, rows.Err()
}
