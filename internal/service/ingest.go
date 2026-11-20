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

const maxPointsPerSegment = 5000

// IngestBatch writes a batch of work segments into a region's local database.
// The write path never talks to the control plane: a region keeps recording
// while the inter-region link is down. Idempotency is two-layered:
//   - whole-batch replay: (batch_id, content hash) returns the original receipt
//   - segment dedupe: unique (device_serial, segment_id) within the region
func (s *Service) IngestBatch(ctx context.Context, region string, batch model.IngestBatch, actor string) (model.IngestResult, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return model.IngestResult{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if batch.BatchID == "" {
		return model.IngestResult{}, fmt.Errorf("%w: batch_id is required", ErrInvalid)
	}
	if len(batch.Operations) == 0 {
		return model.IngestResult{}, fmt.Errorf("%w: operations is empty", ErrInvalid)
	}
	if len(batch.Operations) > 1000 {
		return model.IngestResult{}, fmt.Errorf("%w: at most 1000 operations per batch", ErrInvalid)
	}
	contentHash := sha256Hex(jsonBytes(batch.Operations))

	res := model.IngestResult{Region: region, BatchID: batch.BatchID, LocalCommit: "committed_locally"}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback(ctx)

	// Serialise every write in this region so journal/outbox sequence
	// allocation is gapless.
	var activeNode string
	var epoch, nextJournal, nextOutbox int64
	if err := tx.QueryRow(ctx, `
		select active_node, local_epoch, next_journal_seq, next_outbox_seq
		from region_state where id for update`).Scan(&activeNode, &epoch, &nextJournal, &nextOutbox); err != nil {
		return res, fmt.Errorf("region state unavailable: %w", err)
	}
	res.ActiveNode = activeNode

	// Replay detection.
	var prevHash string
	var prevAccepted, prevDup int
	var prevFirst, prevLast *int64
	err = tx.QueryRow(ctx, `
		select content_sha256, accepted, duplicates, first_journal, last_journal
		from ingest_receipts where batch_id=$1`, batch.BatchID).
		Scan(&prevHash, &prevAccepted, &prevDup, &prevFirst, &prevLast)
	if err == nil {
		if prevHash != contentHash {
			return res, fmt.Errorf("%w: batch_id %s was already used with different content",
				ErrConflict, batch.BatchID)
		}
		res.Accepted, res.Duplicates, res.Replayed = prevAccepted, prevDup, true
		if prevFirst != nil {
			res.FirstSeq = *prevFirst
		}
		if prevLast != nil {
			res.LastSeq = *prevLast
		}
		return res, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return res, err
	}

	now := s.now().UTC()
	var firstSeq, lastSeq int64
	for _, op := range batch.Operations {
		if err := validateOperation(op, now); err != nil {
			res.Rejected++
			res.Errors = append(res.Errors, fmt.Sprintf("%s/%s: %v", op.DeviceSerial, op.SegmentID, err))
			continue
		}
		opID := fmt.Sprintf("%s:%s", op.DeviceSerial, op.SegmentID)
		journalSeq := nextJournal
		pointsJSON := jsonBytes(op.Points)
		alarmsJSON := jsonBytes(op.Alarms)
		tag, err := tx.Exec(ctx, `
			insert into operation_events (id, journal_seq, device_serial, segment_id, operation_type,
				field_id, area_ha, fuel_l, duration_s, completed, started_at, ended_at, point_count,
				points, alarms, upload_batch, source_node, source_epoch, received_at)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
			on conflict (device_serial, segment_id) do nothing`,
			opID+"@"+activeNode, journalSeq, op.DeviceSerial, op.SegmentID, op.OperationType,
			op.FieldID, op.AreaHa, op.FuelL, op.DurationS, op.Completed, op.StartedAt.UTC(),
			op.EndedAt, len(op.Points), pointsJSON, alarmsJSON, batch.BatchID, activeNode, epoch, now)
		if err != nil {
			return res, err
		}
		if tag.RowsAffected() == 0 {
			res.Duplicates++
			continue
		}
		nextJournal++
		if err := insertJournal(ctx, tx, journalSeq, "operation", opID, activeNode); err != nil {
			return res, err
		}
		if _, err := tx.Exec(ctx, `
			insert into devices (device_serial, device_type, first_seen_node)
			values ($1,$2,$3) on conflict (device_serial) do nothing`,
			op.DeviceSerial, op.OperationType, activeNode); err != nil {
			return res, err
		}

		summary := map[string]any{
			"operation_id": opID, "device_serial": op.DeviceSerial, "segment_id": op.SegmentID,
			"operation_type": op.OperationType, "field_id": op.FieldID, "area_ha": op.AreaHa,
			"fuel_l": op.FuelL, "duration_s": op.DurationS, "completed": op.Completed,
			"started_at": op.StartedAt.UTC(), "ended_at": op.EndedAt,
		}
		if err := insertOutbox(ctx, tx, nextOutbox, "operation", opID, "operation_summary",
			summary, activeNode, epoch); err != nil {
			return res, err
		}
		nextOutbox++

		if len(op.Points) > 0 {
			traj := map[string]any{
				"operation_id": opID, "device_serial": op.DeviceSerial, "segment_id": op.SegmentID,
				"point_count": len(op.Points), "points": op.Points,
			}
			if err := insertOutbox(ctx, tx, nextOutbox, "operation", opID+"#trajectory", "raw_trajectory",
				traj, activeNode, epoch); err != nil {
				return res, err
			}
			nextOutbox++
		}
		for i, alarm := range op.Alarms {
			alarmPayload := map[string]any{
				"operation_id": opID, "device_serial": op.DeviceSerial, "segment_id": op.SegmentID,
				"code": alarm.Code, "severity": alarm.Severity, "occurred_at": alarm.OccurredAt.UTC(),
			}
			alarmEntity := fmt.Sprintf("%s#alarm-%d", opID, i+1)
			if err := insertOutbox(ctx, tx, nextOutbox, "operation", alarmEntity, "device_alarm",
				alarmPayload, activeNode, epoch); err != nil {
				return res, err
			}
			nextOutbox++
		}

		res.Accepted++
		if firstSeq == 0 {
			firstSeq = journalSeq
		}
		lastSeq = journalSeq
	}

	if _, err := tx.Exec(ctx, `
		update region_state set next_journal_seq=$1, next_outbox_seq=$2, updated_at=now()
		where id`, nextJournal, nextOutbox); err != nil {
		return res, err
	}
	if _, err := tx.Exec(ctx, `
		insert into ingest_receipts (batch_id, content_sha256, accepted, duplicates, first_journal, last_journal)
		values ($1,$2,$3,$4,$5,$6)`,
		batch.BatchID, contentHash, res.Accepted, res.Duplicates, nullableSeq(firstSeq), nullableSeq(lastSeq)); err != nil {
		return res, err
	}
	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	res.FirstSeq = firstSeq
	res.LastSeq = lastSeq
	return res, nil
}

func nullableSeq(v int64) *int64 {
	if v == 0 {
		return nil
	}
	return &v
}

func validateOperation(op model.OperationInput, now time.Time) error {
	if op.DeviceSerial == "" || op.SegmentID == "" {
		return fmt.Errorf("device_serial and segment_id are required")
	}
	switch op.OperationType {
	case "seeding", "harvesting", "fertilizing":
	default:
		return fmt.Errorf("unknown operation_type %q", op.OperationType)
	}
	if op.AreaHa < 0 || op.FuelL < 0 || op.DurationS < 0 {
		return fmt.Errorf("area, fuel and duration must be non-negative")
	}
	if op.StartedAt.IsZero() {
		return fmt.Errorf("started_at is required")
	}
	if op.StartedAt.After(now.Add(24 * time.Hour)) {
		return fmt.Errorf("started_at is implausibly in the future")
	}
	if op.EndedAt != nil && op.EndedAt.Before(op.StartedAt) {
		return fmt.Errorf("ended_at precedes started_at")
	}
	if len(op.Points) > maxPointsPerSegment {
		return fmt.Errorf("too many trajectory points (max %d)", maxPointsPerSegment)
	}
	for _, p := range op.Points {
		if p.Lat < -90 || p.Lat > 90 || p.Lon < -180 || p.Lon > 180 {
			return fmt.Errorf("trajectory point out of range")
		}
	}
	for _, a := range op.Alarms {
		if a.Code == "" {
			return fmt.Errorf("alarm code is required")
		}
		switch a.Severity {
		case "info", "warning", "critical":
		default:
			return fmt.Errorf("unknown alarm severity %q", a.Severity)
		}
	}
	return nil
}

func insertJournal(ctx context.Context, tx pgx.Tx, seq int64, entityType, entityID, node string) error {
	_, err := tx.Exec(ctx, `
		insert into journal_entries (seq, entity_type, entity_id, source_node)
		values ($1,$2,$3,$4)`, seq, entityType, entityID, node)
	return err
}

func insertOutbox(ctx context.Context, tx pgx.Tx, seq int64, entityType, entityID, category string, payload map[string]any, node string, epoch int64) error {
	_, err := tx.Exec(ctx, `
		insert into outbox_events (seq, entity_type, entity_id, category, payload, source_node, source_epoch)
		values ($1,$2,$3,$4,$5,$6,$7)`, seq, entityType, entityID, category, jsonBytes(payload), node, epoch)
	return err
}

// RegionStatus is the operator's low-latency local view.
type RegionStatus struct {
	RegionCode     string    `json:"region_code"`
	ActiveNode     string    `json:"active_node"`
	Epoch          int64     `json:"epoch"`
	JournalHead    int64     `json:"journal_head"`
	JournalEntries int64     `json:"journal_entries"`
	OperationCount int64     `json:"operation_count"`
	OutboxHead     int64     `json:"outbox_head"`
	PendingOutbox  int64     `json:"pending_outbox"`
	DataCutoff     time.Time `json:"data_cutoff"`
	LinkStatus     string    `json:"replication_link"`
	HandoverReady  bool      `json:"handover_ready"`
}

func (s *Service) RegionStatus(ctx context.Context, region string) (RegionStatus, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return RegionStatus{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	out := RegionStatus{RegionCode: region}
	var outboxHead int64
	err := pool.QueryRow(ctx, `
		select region_code, active_node, local_epoch, next_journal_seq-1, next_outbox_seq-1
		from region_state where id`).Scan(&out.RegionCode, &out.ActiveNode, &out.Epoch, &out.JournalHead, &outboxHead)
	if err != nil {
		return out, err
	}
	out.OutboxHead = outboxHead
	if err := pool.QueryRow(ctx, `select count(*), coalesce(max(received_at), now()) from operation_events`).
		Scan(&out.OperationCount, &out.DataCutoff); err != nil {
		return out, err
	}
	if err := pool.QueryRow(ctx, `select count(*) from journal_entries`).Scan(&out.JournalEntries); err != nil {
		return out, err
	}
	var cursorSeq int64
	if err := s.db.Control().QueryRow(ctx, `
		select coalesce(c.confirmed_seq,0), coalesce(l.status,'online')
		from replication_cursors c
		left join replication_links l on l.region_code=c.source_region
		where c.source_region=$1`, region).Scan(&cursorSeq, &out.LinkStatus); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return out, err
		}
		out.LinkStatus = "unknown"
	}
	out.PendingOutbox = outboxHead - cursorSeq
	if out.PendingOutbox < 0 {
		out.PendingOutbox = 0
	}
	out.HandoverReady = out.JournalEntries == out.JournalHead
	return out, nil
}

func (s *Service) ListOperations(ctx context.Context, region string, limit int, opType string) ([]model.OperationRecord, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return nil, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	q := `select id, journal_seq, device_serial, segment_id, operation_type, field_id, area_ha,
	             fuel_l, duration_s, completed, started_at, ended_at, point_count, alarms,
	             source_node, received_at
	      from operation_events`
	args := []any{}
	if opType != "" {
		q += ` where operation_type=$1`
		args = append(args, opType)
	}
	q += fmt.Sprintf(` order by journal_seq desc limit %d`, limit)
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.OperationRecord{}
	for rows.Next() {
		var r model.OperationRecord
		var alarms []byte
		if err := rows.Scan(&r.ID, &r.JournalSeq, &r.DeviceSerial, &r.SegmentID, &r.OperationType,
			&r.FieldID, &r.AreaHa, &r.FuelL, &r.DurationS, &r.Completed, &r.StartedAt, &r.EndedAt,
			&r.PointCount, &alarms, &r.SourceNode, &r.ReceivedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(alarms, &r.Alarms)
		out = append(out, r)
	}
	return out, rows.Err()
}

// VerifyJournal proves the local journal is gapless: the number of entries
// matches the head sequence and no sequence is missing.
type JournalVerification struct {
	Region    string    `json:"region"`
	Gapless   bool      `json:"gapless"`
	Head      int64     `json:"head_seq"`
	Entries   int64     `json:"entries"`
	Missing   []int64   `json:"missing_seqs,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
}

func (s *Service) VerifyJournal(ctx context.Context, region string) (JournalVerification, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return JournalVerification{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	out := JournalVerification{Region: region, CheckedAt: s.now().UTC()}
	var count, minSeq, maxSeq int64
	if err := pool.QueryRow(ctx, `select count(*), coalesce(min(seq),0), coalesce(max(seq),0) from journal_entries`).
		Scan(&count, &minSeq, &maxSeq); err != nil {
		return out, err
	}
	out.Entries = count
	out.Head = maxSeq
	if count == 0 {
		out.Gapless = true
		return out, nil
	}
	rows, err := pool.Query(ctx, `select g from generate_series($1::bigint,$2::bigint) g
		left join journal_entries j on j.seq=g where j.seq is null order by g limit 20`, minSeq, maxSeq)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return out, err
		}
		out.Missing = append(out.Missing, v)
	}
	out.Gapless = len(out.Missing) == 0 && minSeq == 1
	return out, rows.Err()
}
