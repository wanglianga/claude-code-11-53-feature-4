package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"agrisync/internal/model"
)

// persistReview freezes a full review result inside the regional database.
func (s *Service) persistReview(ctx context.Context, tx pgx.Tx, d model.SequenceReviewDetail) error {
	r := d.Run
	if _, err := tx.Exec(ctx, `
		insert into sequence_review_runs
		  (run_id, region_code, status, period_from, period_to, fsm_rule_set_id, po_rule_set_id,
		   fact_version, attribution_version, version_bundle, fact_cutoff, note, created_by, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		r.RunID, r.RegionCode, r.Status, r.PeriodFrom, r.PeriodTo, r.FSMRuleSetID, r.PORuleSetID,
		r.FactVersion, r.AttributionVersion, jsonBytes(r.VersionBundle), r.FactCutoff, r.Note,
		r.CreatedBy, r.CreatedAt); err != nil {
		return err
	}
	for _, f := range d.Fields {
		if _, err := tx.Exec(ctx, `
			insert into sequence_review_fields
			  (run_id, season_id, field_id, crop_code, attribution_status, rule_applicability,
			   fsm_state, fsm_status, po_status, fsm_confirmed_stages, po_confirmed_stages,
			   missing_prerequisites, allowed_paths, pending_ranges, summary)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			f.RunID, f.SeasonID, f.FieldID, f.CropCode, f.AttributionStatus, f.RuleApplicability,
			f.FSMState, f.FSMStatus, f.POStatus, textArray(f.FSMConfirmedStages), textArray(f.POConfirmedStages),
			jsonBytes(f.MissingPrerequisites), jsonBytes(f.AllowedPaths), jsonBytes(f.PendingRanges),
			jsonBytes(f.Summary)); err != nil {
			return err
		}
	}
	for _, e := range d.Events {
		e.RunID = r.RunID
		if _, err := tx.Exec(ctx, `
			insert into sequence_review_events
			  (run_id, fact_id, season_id, engine, operation_type, event_at, received_at, interpretation, detail)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			on conflict do nothing`,
			e.RunID, e.FactID, e.SeasonID, e.Engine, e.OperationType, e.EventAt, e.ReceivedAt,
			e.Interpretation, jsonBytes(e.Detail)); err != nil {
			return err
		}
	}
	for _, is := range d.Issues {
		is.RunID = r.RunID
		if _, err := tx.Exec(ctx, `
			insert into sequence_review_issues
			  (run_id, season_id, engine, severity, code, message, evidence)
			values ($1,$2,$3,$4,$5,$6,$7)`,
			is.RunID, is.SeasonID, is.Engine, is.Severity, is.Code, is.Message, jsonBytes(is.Evidence)); err != nil {
			return err
		}
	}
	for _, g := range d.Engines {
		if _, err := tx.Exec(ctx, `
			insert into sequence_review_engines
			  (run_id, engine, status, field_count, conflict_count, pending_count, confirmed_stage_counts)
			values ($1,$2,$3,$4,$5,$6,$7)`,
			g.RunID, g.Engine, g.Status, g.FieldCount, g.ConflictCount, g.PendingCount,
			jsonBytes(g.ConfirmedStageCounts)); err != nil {
			return err
		}
	}
	return nil
}

func textArray(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

// ListSequenceReviews returns the run headers for one region, newest first.
func (s *Service) ListSequenceReviews(ctx context.Context, region string, limit int) ([]model.SequenceReviewRun, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return nil, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := pool.Query(ctx, `
		select run_id, region_code, status, period_from, period_to, fsm_rule_set_id, po_rule_set_id,
		       fact_version, attribution_version, version_bundle, fact_cutoff, note, created_by, created_at
		from sequence_review_runs order by created_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.SequenceReviewRun{}
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRun(sc rowScanner) (model.SequenceReviewRun, error) {
	var r model.SequenceReviewRun
	var bundle []byte
	err := sc.Scan(&r.RunID, &r.RegionCode, &r.Status, &r.PeriodFrom, &r.PeriodTo, &r.FSMRuleSetID,
		&r.PORuleSetID, &r.FactVersion, &r.AttributionVersion, &bundle, &r.FactCutoff, &r.Note,
		&r.CreatedBy, &r.CreatedAt)
	if err != nil {
		return r, err
	}
	_ = json.Unmarshal(bundle, &r.VersionBundle)
	return r, nil
}

// GetSequenceReview replays one stored run in full: fields, per-fact
// interpretations, issues and engine rollups. Because every input version is
// frozen on the run, an old run returns the same result after later
// corrections.
func (s *Service) GetSequenceReview(ctx context.Context, region, runID string) (model.SequenceReviewDetail, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return model.SequenceReviewDetail{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	var d model.SequenceReviewDetail
	err := pool.QueryRow(ctx, `
		select run_id, region_code, status, period_from, period_to, fsm_rule_set_id, po_rule_set_id,
		       fact_version, attribution_version, version_bundle, fact_cutoff, note, created_by, created_at
		from sequence_review_runs where run_id=$1`, runID).Scan(
		&d.Run.RunID, &d.Run.RegionCode, &d.Run.Status, &d.Run.PeriodFrom, &d.Run.PeriodTo,
		&d.Run.FSMRuleSetID, &d.Run.PORuleSetID, &d.Run.FactVersion, &d.Run.AttributionVersion,
		&jsonWrap{&d.Run.VersionBundle}, &d.Run.FactCutoff, &d.Run.Note, &d.Run.CreatedBy, &d.Run.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, fmt.Errorf("%w: sequence review run %s", ErrNotFound, runID)
	}
	if err != nil {
		return d, err
	}

	fieldRows, err := pool.Query(ctx, `
		select run_id, season_id, field_id, crop_code, attribution_status, rule_applicability,
		       fsm_state, fsm_status, po_status, fsm_confirmed_stages, po_confirmed_stages,
		       missing_prerequisites, allowed_paths, pending_ranges, summary
		from sequence_review_fields where run_id=$1 order by field_id, season_id`, runID)
	if err != nil {
		return d, err
	}
	d.Fields = []model.SequenceReviewField{}
	for fieldRows.Next() {
		var f model.SequenceReviewField
		var fsmStages, poStages []string
		if err := fieldRows.Scan(&f.RunID, &f.SeasonID, &f.FieldID, &f.CropCode, &f.AttributionStatus,
			&f.RuleApplicability, &f.FSMState, &f.FSMStatus, &f.POStatus, &fsmStages, &poStages,
			&jsonWrap{&f.MissingPrerequisites}, &jsonWrap{&f.AllowedPaths},
			&jsonWrap{&f.PendingRanges}, &jsonWrap{&f.Summary}); err != nil {
			fieldRows.Close()
			return d, err
		}
		f.FSMConfirmedStages = fsmStages
		f.POConfirmedStages = poStages
		d.Fields = append(d.Fields, f)
	}
	fieldRows.Close()
	if err := fieldRows.Err(); err != nil {
		return d, err
	}

	evRows, qerr := pool.Query(ctx, `
		select run_id, fact_id, season_id, engine, operation_type, event_at, received_at,
		       interpretation, detail
		from sequence_review_events where run_id=$1
		order by engine, event_at, fact_id`, runID)
	if qerr != nil {
		return d, qerr
	}
	d.Events = []model.SequenceReviewEvent{}
	for evRows.Next() {
		var e model.SequenceReviewEvent
		if err := evRows.Scan(&e.RunID, &e.FactID, &e.SeasonID, &e.Engine, &e.OperationType,
			&e.EventAt, &e.ReceivedAt, &e.Interpretation, &jsonWrap{&e.Detail}); err != nil {
			evRows.Close()
			return d, err
		}
		d.Events = append(d.Events, e)
	}
	evRows.Close()
	if err := evRows.Err(); err != nil {
		return d, err
	}

	isRows, err := pool.Query(ctx, `
		select id, run_id, season_id, engine, severity, code, message, evidence
		from sequence_review_issues where run_id=$1 order by engine, severity desc, id`, runID)
	if err != nil {
		return d, err
	}
	d.Issues = []model.SequenceReviewIssue{}
	for isRows.Next() {
		var is model.SequenceReviewIssue
		if err := isRows.Scan(&is.ID, &is.RunID, &is.SeasonID, &is.Engine, &is.Severity, &is.Code,
			&is.Message, &jsonWrap{&is.Evidence}); err != nil {
			isRows.Close()
			return d, err
		}
		d.Issues = append(d.Issues, is)
	}
	isRows.Close()
	if err := isRows.Err(); err != nil {
		return d, err
	}

	engRows, err := pool.Query(ctx, `
		select run_id, engine, status, field_count, conflict_count, pending_count, confirmed_stage_counts
		from sequence_review_engines where run_id=$1 order by engine`, runID)
	if err != nil {
		return d, err
	}
	d.Engines = []model.SequenceReviewEngineRollup{}
	for engRows.Next() {
		var g model.SequenceReviewEngineRollup
		if err := engRows.Scan(&g.RunID, &g.Engine, &g.Status, &g.FieldCount, &g.ConflictCount,
			&g.PendingCount, &jsonWrap{&g.ConfirmedStageCounts}); err != nil {
			engRows.Close()
			return d, err
		}
		d.Engines = append(d.Engines, g)
	}
	engRows.Close()
	return d, engRows.Err()
}

// jsonWrap scans a jsonb column into an arbitrary json-decodable target.
type jsonWrap struct{ target any }

func (w *jsonWrap) Scan(src any) error {
	if src == nil {
		return nil
	}
	var raw []byte
	switch v := src.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		return fmt.Errorf("unsupported jsonb scan type %T", src)
	}
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, w.target)
}

// --- data loading helpers ----------------------------------------------

func (s *Service) listSeasonsForPeriod(ctx context.Context, region string, from, to time.Time) ([]model.FieldSeason, error) {
	pool := s.db.Region(region)
	rows, err := pool.Query(ctx, `
		select season_id, field_id, crop_code, planned_from, planned_to, status, note, updated_at
		from field_seasons
		where planned_from < $2 and planned_to > $1
		order by field_id, planned_from`, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.FieldSeason{}
	for rows.Next() {
		var v model.FieldSeason
		if err := rows.Scan(&v.SeasonID, &v.FieldID, &v.CropCode, &v.PlannedFrom, &v.PlannedTo,
			&v.Status, &v.Note, &v.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Service) factsForPeriod(ctx context.Context, region string, from, to time.Time) ([]model.SequenceFact, error) {
	pool := s.db.Region(region)
	rows, err := pool.Query(ctx, `
		select fact_id, source, device_serial, segment_id, operation_type, field_id,
		       event_at, received_at, completed, batch_id, detail, created_at
		from sequence_facts
		where event_at >= $1 and event_at < $2
		order by event_at, received_at, fact_id`, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.SequenceFact{}
	for rows.Next() {
		var f model.SequenceFact
		var detail []byte
		if err := rows.Scan(&f.FactID, &f.Source, &f.DeviceSerial, &f.SegmentID, &f.OperationType,
			&f.FieldID, &f.EventAt, &f.ReceivedAt, &f.Completed, &f.BatchID, &detail, &f.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(detail, &f.Detail)
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *Service) allBoundaries(ctx context.Context, region string) ([]model.BoundaryRevision, error) {
	pool := s.db.Region(region)
	rows, err := pool.Query(ctx, `
		select revision_id, field_id, valid_from, valid_until, boundary_label, geometry,
		       superseded_by, note, created_at
		from field_boundary_revisions order by field_id, valid_from`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.BoundaryRevision{}
	for rows.Next() {
		var v model.BoundaryRevision
		var geo []byte
		if err := rows.Scan(&v.RevisionID, &v.FieldID, &v.ValidFrom, &v.ValidUntil, &v.BoundaryLabel,
			&geo, &v.SupersededBy, &v.Note, &v.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(geo, &v.Geometry)
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Service) allCalendars(ctx context.Context, region string) ([]model.CropCalendar, error) {
	pool := s.db.Region(region)
	rows, err := pool.Query(ctx, `
		select calendar_id, field_id, season_id, crop_code, valid_from, valid_until, note, created_at
		from crop_calendars order by field_id, valid_from`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.CropCalendar{}
	for rows.Next() {
		var v model.CropCalendar
		if err := rows.Scan(&v.CalendarID, &v.FieldID, &v.SeasonID, &v.CropCode, &v.ValidFrom,
			&v.ValidUntil, &v.Note, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// =====================================================================
// Region -> group publication (approved granularity only)
// =====================================================================

// PublishSequenceCoverage releases one region's stage coverage for a stored
// review run into the control plane. Only per-stage counts cross the
// boundary: no field id, fact id, path or evidence is included. The release
// is gated on the aggregate_metric residency policy, mirroring the
// policy-filtered replica path used by ordinary aggregates.
func (s *Service) PublishSequenceCoverage(ctx context.Context, region, runID, actor, role string) ([]model.StageCoveragePublication, error) {
	detail, err := s.GetSequenceReview(ctx, region, runID)
	if err != nil {
		return nil, err
	}
	allowed, err := s.categoryAllowed(ctx, region, "aggregate_metric")
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("%w: region %s residency policy does not allow aggregate release", ErrForbidden, region)
	}
	out := []model.StageCoveragePublication{}
	for _, g := range detail.Engines {
		pub := model.StageCoveragePublication{
			ID: randID("covpub"), SourceRegion: region, RunID: runID,
			VersionBundle: detail.Run.VersionBundle, FactCutoff: detail.Run.FactCutoff,
			Engine: g.Engine, FieldSeasonCount: g.FieldCount, ConflictCount: g.ConflictCount,
			PendingCount: g.PendingCount, StageCounts: g.ConfirmedStageCounts,
			Status: "confirmed", CopiedAt: s.now().UTC(),
		}
		if pub.StageCounts == nil {
			pub.StageCounts = map[string]int{}
		}
		if _, err := s.db.Control().Exec(ctx, `
			insert into sequence_coverage_publications
			  (id, source_region, run_id, version_bundle, fact_cutoff, engine,
			   field_season_count, conflict_count, pending_count, stage_counts, status)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'confirmed')
			on conflict (source_region, run_id, engine) do update
			  set version_bundle=excluded.version_bundle, fact_cutoff=excluded.fact_cutoff,
			      field_season_count=excluded.field_season_count,
			      conflict_count=excluded.conflict_count, pending_count=excluded.pending_count,
			      stage_counts=excluded.stage_counts, status='confirmed'`,
			pub.ID, pub.SourceRegion, pub.RunID, jsonBytes(pub.VersionBundle), pub.FactCutoff,
			pub.Engine, pub.FieldSeasonCount, pub.ConflictCount, pub.PendingCount,
			jsonBytes(pub.StageCounts)); err != nil {
			return nil, err
		}
		out = append(out, pub)
	}
	_, _ = s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: "sequence_coverage.publish",
		SubjectType: "sequence_review_run", SubjectID: runID, RegionCode: region, Decision: "allow",
		Detail: map[string]any{
			"engines": []string{ruleEngineFSM, ruleEnginePO}, "granularity": "region_stage_counts",
			"field_level_data_released": false,
		},
	})
	return out, nil
}

func (s *Service) categoryAllowed(ctx context.Context, region, category string) (bool, error) {
	var allowed bool
	err := s.db.Control().QueryRow(ctx, `
		select outbound_allowed from residency_policies
		where region_code=$1 and category=$2`, region, category).Scan(&allowed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("%w: no residency policy for %s/%s", ErrNotFound, region, category)
	}
	return allowed, err
}

// ListCoveragePublications is the control-plane view of released coverage.
// It contains counts only; the regional detail stays in the region database.
func (s *Service) ListCoveragePublications(ctx context.Context, region string) ([]model.StageCoveragePublication, error) {
	q := `select id, source_region, run_id, version_bundle, fact_cutoff, engine,
	             field_season_count, conflict_count, pending_count, stage_counts, status, copied_at
	      from sequence_coverage_publications`
	args := []any{}
	if region != "" {
		q += ` where source_region=$1`
		args = append(args, region)
	}
	q += ` order by copied_at desc, source_region, engine`
	rows, err := s.db.Control().Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.StageCoveragePublication{}
	for rows.Next() {
		var p model.StageCoveragePublication
		if err := rows.Scan(&p.ID, &p.SourceRegion, &p.RunID, &jsonWrap{&p.VersionBundle}, &p.FactCutoff,
			&p.Engine, &p.FieldSeasonCount, &p.ConflictCount, &p.PendingCount,
			&jsonWrap{&p.StageCounts}, &p.Status, &p.CopiedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// RollupSequenceCoverage builds the group-visible rollup across confirmed
// publications only. A region without a confirmed publication for the
// requested engine is excluded and named, never silently counted as zero or
// back-filled from another run.
func (s *Service) RollupSequenceCoverage(ctx context.Context, engine string, from, to time.Time,
	scopeRegions []string, actor string) (model.StageCoverageRollup, error) {
	if engine != ruleEngineFSM && engine != ruleEnginePO {
		return model.StageCoverageRollup{}, fmt.Errorf("%w: engine must be fsm or partial_order", ErrInvalid)
	}
	if !to.After(from) {
		return model.StageCoverageRollup{}, fmt.Errorf("%w: period_to must be after period_from", ErrInvalid)
	}
	regions, err := normalizeRegions(scopeRegions, s.cfg)
	if err != nil {
		return model.StageCoverageRollup{}, err
	}
	now := s.now().UTC()
	rollup := model.StageCoverageRollup{
		ID: randID("covroll"), PeriodFrom: from.UTC(), PeriodTo: to.UTC(), Engine: engine,
		IncludedRegions: []string{}, ExcludedRegions: []string{}, VersionBundles: map[string]any{},
		StageTotals:     map[string]int{},
		RegionBreakdown: []model.StageCoverageRegion{},
		RequestedBy:     actor, CreatedAt: now,
	}
	var earliestCutoff time.Time
	for _, region := range regions {
		var p model.StageCoveragePublication
		var bundle, counts []byte
		err := s.db.Control().QueryRow(ctx, `
			select id, source_region, run_id, version_bundle, fact_cutoff, engine,
			       field_season_count, conflict_count, pending_count, stage_counts, status, copied_at
			from sequence_coverage_publications
			where source_region=$1 and engine=$2 and status='confirmed'
			  and fact_cutoff >= $3 and fact_cutoff <= $4
			order by fact_cutoff desc, copied_at desc limit 1`,
			region, engine, from.UTC(), to.UTC()).Scan(
			&p.ID, &p.SourceRegion, &p.RunID, &bundle, &p.FactCutoff, &p.Engine, &p.FieldSeasonCount,
			&p.ConflictCount, &p.PendingCount, &counts, &p.Status, &p.CopiedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			rollup.ExcludedRegions = append(rollup.ExcludedRegions, region)
			rollup.RegionBreakdown = append(rollup.RegionBreakdown, model.StageCoverageRegion{
				RegionCode: region, Included: false, ExclusionReason: "no_confirmed_coverage_publication",
				StageCounts: map[string]int{},
			})
			continue
		}
		if err != nil {
			return rollup, err
		}
		_ = json.Unmarshal(bundle, &p.VersionBundle)
		_ = json.Unmarshal(counts, &p.StageCounts)
		if p.StageCounts == nil {
			p.StageCounts = map[string]int{}
		}
		cutoff := p.FactCutoff
		rollup.IncludedRegions = append(rollup.IncludedRegions, region)
		rollup.VersionBundles[region] = p.VersionBundle
		rollup.RegionBreakdown = append(rollup.RegionBreakdown, model.StageCoverageRegion{
			RegionCode: region, Included: true, RunID: p.RunID, FieldSeasonCount: p.FieldSeasonCount,
			ConflictCount: p.ConflictCount, PendingCount: p.PendingCount, StageCounts: p.StageCounts,
			VersionBundle: p.VersionBundle, FactCutoff: &cutoff,
		})
		for stage, n := range p.StageCounts {
			rollup.StageTotals[stage] += n
		}
		if earliestCutoff.IsZero() || p.FactCutoff.Before(earliestCutoff) {
			earliestCutoff = p.FactCutoff
		}
	}
	if earliestCutoff.IsZero() {
		earliestCutoff = now
	}
	rollup.DataCutoff = earliestCutoff
	sort.Strings(rollup.IncludedRegions)
	sort.Strings(rollup.ExcludedRegions)

	if _, err := s.db.Control().Exec(ctx, `
		insert into sequence_coverage_rollups
		  (id, period_from, period_to, engine, included_regions, excluded_regions, version_bundles,
		   stage_totals, region_breakdown, data_cutoff, requested_by, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		rollup.ID, rollup.PeriodFrom, rollup.PeriodTo, rollup.Engine,
		rollup.IncludedRegions, rollup.ExcludedRegions, jsonBytes(rollup.VersionBundles),
		jsonBytes(rollup.StageTotals), jsonBytes(rollup.RegionBreakdown), rollup.DataCutoff,
		rollup.RequestedBy, rollup.CreatedAt); err != nil {
		return rollup, err
	}
	_, _ = s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: "group_analyst", Action: "sequence_coverage.rollup",
		SubjectType: "sequence_coverage_rollup", SubjectID: rollup.ID, Decision: "allow",
		Detail: map[string]any{
			"engine": engine, "included_regions": rollup.IncludedRegions,
			"excluded_regions": rollup.ExcludedRegions, "granularity": "region_stage_counts",
		},
	})
	return rollup, nil
}

// ListCoverageRollups returns stored group rollups.
func (s *Service) ListCoverageRollups(ctx context.Context, limit int) ([]model.StageCoverageRollup, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.Control().Query(ctx, `
		select id, period_from, period_to, engine, included_regions, excluded_regions,
		       version_bundles, stage_totals, region_breakdown, data_cutoff, requested_by, created_at
		from sequence_coverage_rollups order by created_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.StageCoverageRollup{}
	for rows.Next() {
		var r model.StageCoverageRollup
		var included, excluded []string
		if err := rows.Scan(&r.ID, &r.PeriodFrom, &r.PeriodTo, &r.Engine, &included, &excluded,
			&jsonWrap{&r.VersionBundles}, &jsonWrap{&r.StageTotals}, &jsonWrap{&r.RegionBreakdown},
			&r.DataCutoff, &r.RequestedBy, &r.CreatedAt); err != nil {
			return nil, err
		}
		r.IncludedRegions, r.ExcludedRegions = included, excluded
		out = append(out, r)
	}
	return out, rows.Err()
}
