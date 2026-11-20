package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"agrisync/internal/model"
)

// Failover promotes a region's standby node. The new primary may only resume
// from the position already confirmed in the DR register:
//   - replica rows that were copied but never confirmed are discarded, because
//     the group must never treat the old primary's unconfirmed cache as
//     published data;
//   - the replication cursor is rewound to last_confirmed_seq so every outbox
//     event after that point is re-sent by the new primary (idempotent upserts
//     make the re-send safe);
//   - the region's local journal and outbox are NOT truncated: local farm
//     accounting continues, it simply is not group-visible until re-confirmed.
func (s *Service) Failover(ctx context.Context, region string, reconcile bool, actor, role string) (model.FailoverResult, error) {
	if s.db.Region(region) == nil {
		return model.FailoverResult{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	now := s.now().UTC()
	tx, err := s.db.Control().Begin(ctx)
	if err != nil {
		return model.FailoverResult{}, err
	}
	defer tx.Rollback(ctx)

	var reg model.DRRegistry
	if err := tx.QueryRow(ctx, `
		select region_code, primary_node, standby_node, active_node, epoch, last_confirmed_seq,
		       status from dr_registry where region_code=$1 for update`, region).
		Scan(&reg.RegionCode, &reg.PrimaryNode, &reg.StandbyNode, &reg.ActiveNode, &reg.Epoch,
			&reg.LastConfirmedSeq, &reg.Status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.FailoverResult{}, fmt.Errorf("%w: region %s has no DR entry", ErrNotFound, region)
		}
		return model.FailoverResult{}, err
	}
	if reg.ActiveNode != reg.PrimaryNode {
		return model.FailoverResult{}, fmt.Errorf("%w: region %s already failed over to %s; run failback first",
			ErrConflict, region, reg.ActiveNode)
	}

	discarded := map[string]int{}
	for _, table := range []string{"replica_op_summaries", "replica_trajectories", "replica_alarms", "replica_metrics"} {
		var n int
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			`with del as (delete from %s where source_region=$1 and status='unconfirmed' returning 1)
			 select count(*) from del`, table), region).Scan(&n); err != nil {
			return model.FailoverResult{}, err
		}
		discarded[table] = n
	}

	if _, err := tx.Exec(ctx, `
		update replication_cursors set last_seq=$2, confirmed_seq=$2, status='pending_reconcile',
		  retry_count=0, last_error=null, updated_at=now()
		where source_region=$1`, region, reg.LastConfirmedSeq); err != nil {
		return model.FailoverResult{}, err
	}
	newEpoch := reg.Epoch + 1
	if _, err := tx.Exec(ctx, `
		update dr_registry set active_node=$2, epoch=$3, status='failed_over', updated_at=now()
		where region_code=$1`, region, reg.StandbyNode, newEpoch); err != nil {
		return model.FailoverResult{}, err
	}
	if _, err := tx.Exec(ctx, `
		insert into dr_events (region_code, event_type, from_node, to_node, confirmed_seq, epoch, detail)
		values ($1,'failover',$2,$3,$4,$5,$6)`,
		region, reg.ActiveNode, reg.StandbyNode, reg.LastConfirmedSeq, newEpoch,
		jsonBytes(map[string]any{"discarded_unconfirmed": discarded,
			"policy": "resume only from DR-confirmed position"})); err != nil {
		return model.FailoverResult{}, err
	}
	// The region's local write path continues immediately on the new primary.
	if _, err := s.db.Region(region).Exec(ctx, `
		update region_state set active_node=$1, local_epoch=$2, updated_at=now() where id`,
		reg.StandbyNode, newEpoch); err != nil {
		return model.FailoverResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.FailoverResult{}, err
	}

	res := model.FailoverResult{
		RegionCode: region, FromNode: reg.ActiveNode, ToNode: reg.StandbyNode, Epoch: newEpoch,
		ConfirmedSeq: reg.LastConfirmedSeq, DiscardedRows: discarded,
		ReplicationReset: true, ResumedFromSeq: reg.LastConfirmedSeq,
		NewPrimaryState: "accepting_local_writes; group replication restarts at confirmed position",
		OccurredAt:      now,
	}
	_, err = s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: "failover.promote", SubjectType: "region", SubjectID: region,
		RegionCode: region, Decision: "allow",
		Detail: map[string]any{
			"from_node": res.FromNode, "to_node": res.ToNode, "epoch": newEpoch,
			"confirmed_seq": reg.LastConfirmedSeq, "discarded_unconfirmed": discarded,
		},
	})
	if err != nil {
		return res, err
	}
	if reconcile {
		_, replErr := s.Replicate(ctx, region, "")
		if replErr != nil && !errors.Is(replErr, ErrUnavailable) {
			return res, replErr
		}
		if _, err := s.db.Control().Exec(ctx, `
			update dr_registry set status='healthy', updated_at=now() where region_code=$1`, region); err != nil {
			return res, err
		}
	}
	return res, nil
}

// Failback returns a failed-over region to its primary node while keeping the
// DR-confirmed position as the replication restart point.
func (s *Service) Failback(ctx context.Context, region string, actor, role string) (model.FailoverResult, error) {
	if s.db.Region(region) == nil {
		return model.FailoverResult{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	now := s.now().UTC()
	tx, err := s.db.Control().Begin(ctx)
	if err != nil {
		return model.FailoverResult{}, err
	}
	defer tx.Rollback(ctx)
	var reg model.DRRegistry
	if err := tx.QueryRow(ctx, `
		select region_code, primary_node, standby_node, active_node, epoch, last_confirmed_seq, status
		from dr_registry where region_code=$1 for update`, region).
		Scan(&reg.RegionCode, &reg.PrimaryNode, &reg.StandbyNode, &reg.ActiveNode, &reg.Epoch,
			&reg.LastConfirmedSeq, &reg.Status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.FailoverResult{}, fmt.Errorf("%w: region %s has no DR entry", ErrNotFound, region)
		}
		return model.FailoverResult{}, err
	}
	if reg.ActiveNode != reg.StandbyNode {
		return model.FailoverResult{}, fmt.Errorf("%w: region %s is not failed over", ErrConflict, region)
	}
	newEpoch := reg.Epoch + 1
	if _, err := tx.Exec(ctx, `
		update dr_registry set active_node=primary_node, epoch=$2, status='healthy', updated_at=now()
		where region_code=$1`, region, newEpoch); err != nil {
		return model.FailoverResult{}, err
	}
	if _, err := tx.Exec(ctx, `
		insert into dr_events (region_code, event_type, from_node, to_node, confirmed_seq, epoch, detail)
		values ($1,'failback',$2,$3,$4,$5,'{}'::jsonb)`,
		region, reg.ActiveNode, reg.PrimaryNode, reg.LastConfirmedSeq, newEpoch); err != nil {
		return model.FailoverResult{}, err
	}
	if _, err := s.db.Region(region).Exec(ctx, `
		update region_state set active_node=$1, local_epoch=$2, updated_at=now() where id`,
		reg.PrimaryNode, newEpoch); err != nil {
		return model.FailoverResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.FailoverResult{}, err
	}
	res := model.FailoverResult{
		RegionCode: region, FromNode: reg.ActiveNode, ToNode: reg.PrimaryNode, Epoch: newEpoch,
		ConfirmedSeq: reg.LastConfirmedSeq, DiscardedRows: map[string]int{},
		ReplicationReset: true, ResumedFromSeq: reg.LastConfirmedSeq,
		NewPrimaryState: "primary restored", OccurredAt: now,
	}
	if _, err := s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: "failover.failback", SubjectType: "region", SubjectID: region,
		RegionCode: region, Decision: "allow",
		Detail: map[string]any{"from_node": res.FromNode, "to_node": res.ToNode, "epoch": newEpoch},
	}); err != nil {
		return res, err
	}
	return res, nil
}

func (s *Service) GetDR(ctx context.Context, region string) (model.DRRegistry, []map[string]any, error) {
	var r model.DRRegistry
	err := s.db.Control().QueryRow(ctx, `
		select region_code, primary_node, standby_node, active_node, epoch, last_confirmed_seq,
		       last_confirmed_at, status, updated_at from dr_registry where region_code=$1`, region).
		Scan(&r.RegionCode, &r.PrimaryNode, &r.StandbyNode, &r.ActiveNode, &r.Epoch,
			&r.LastConfirmedSeq, &r.LastConfirmedAt, &r.Status, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, nil, fmt.Errorf("%w: region %s", ErrNotFound, region)
	}
	if err != nil {
		return r, nil, err
	}
	rows, err := s.db.Control().Query(ctx, `
		select event_type, coalesce(from_node,''), coalesce(to_node,''), confirmed_seq, epoch, detail, created_at
		from dr_events where region_code=$1 order by id desc limit 20`, region)
	if err != nil {
		return r, nil, err
	}
	defer rows.Close()
	events := []map[string]any{}
	for rows.Next() {
		var typ, from, to string
		var seq, epoch int64
		var detail []byte
		var at any
		if err := rows.Scan(&typ, &from, &to, &seq, &epoch, &detail, &at); err != nil {
			return r, nil, err
		}
		var d any
		_ = json.Unmarshal(detail, &d)
		events = append(events, map[string]any{"event_type": typ, "from_node": from, "to_node": to,
			"confirmed_seq": seq, "epoch": epoch, "detail": d, "created_at": at})
	}
	return r, events, rows.Err()
}

func (s *Service) ListDR(ctx context.Context) ([]model.DRRegistry, error) {
	rows, err := s.db.Control().Query(ctx, `
		select region_code, primary_node, standby_node, active_node, epoch, last_confirmed_seq,
		       last_confirmed_at, status, updated_at from dr_registry order by region_code`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.DRRegistry{}
	for rows.Next() {
		var r model.DRRegistry
		if err := rows.Scan(&r.RegionCode, &r.PrimaryNode, &r.StandbyNode, &r.ActiveNode, &r.Epoch,
			&r.LastConfirmedSeq, &r.LastConfirmedAt, &r.Status, &r.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
