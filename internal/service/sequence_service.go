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

// =====================================================================
// Registration inputs
// =====================================================================

type FactInput struct {
	FactID        string         `json:"fact_id"`
	DeviceSerial  string         `json:"device_serial"`
	SegmentID     string         `json:"segment_id"`
	OperationType string         `json:"operation_type"`
	FieldID       string         `json:"field_id"`
	EventAt       time.Time      `json:"event_at"`
	ReceivedAt    *time.Time     `json:"received_at"`
	Completed     *bool          `json:"completed"`
	BatchID       string         `json:"batch_id"`
	Detail        map[string]any `json:"detail"`
}

type FieldSeasonInput struct {
	SeasonID    string    `json:"season_id"`
	FieldID     string    `json:"field_id"`
	CropCode    string    `json:"crop_code"`
	PlannedFrom time.Time `json:"planned_from"`
	PlannedTo   time.Time `json:"planned_to"`
	Status      string    `json:"status"`
	Note        string    `json:"note"`
}

type BoundaryInput struct {
	RevisionID    string         `json:"revision_id"`
	FieldID       string         `json:"field_id"`
	ValidFrom     time.Time      `json:"valid_from"`
	ValidUntil    *time.Time     `json:"valid_until"`
	BoundaryLabel string         `json:"boundary_label"`
	Geometry      map[string]any `json:"geometry"`
	SupersededBy  string         `json:"superseded_by"`
	Note          string         `json:"note"`
}

type CropCalendarInput struct {
	CalendarID string     `json:"calendar_id"`
	FieldID    string     `json:"field_id"`
	SeasonID   string     `json:"season_id"`
	CropCode   string     `json:"crop_code"`
	ValidFrom  time.Time  `json:"valid_from"`
	ValidUntil *time.Time `json:"valid_until"`
	Note       string     `json:"note"`
}

type RuleSetInput struct {
	RuleSetID string         `json:"rule_set_id"`
	Version   string         `json:"version"`
	Engine    string         `json:"engine"`
	CropCode  string         `json:"crop_code"`
	RuleDoc   map[string]any `json:"rule_doc"`
	Status    string         `json:"status"`
}

type ReviewRunInput struct {
	PeriodFrom   time.Time `json:"period_from"`
	PeriodTo     time.Time `json:"period_to"`
	FSMRuleSetID string    `json:"fsm_rule_set_id"`
	PORuleSetID  string    `json:"po_rule_set_id"`
	Note         string    `json:"note"`
}

func validateFact(in FactInput, now time.Time) error {
	if in.FactID == "" || in.DeviceSerial == "" || in.SegmentID == "" {
		return fmt.Errorf("%w: fact_id, device_serial and segment_id are required", ErrInvalid)
	}
	switch in.OperationType {
	case "seeding", "fertilizing", "harvesting", "other":
	default:
		return fmt.Errorf("%w: unknown operation_type %q", ErrInvalid, in.OperationType)
	}
	if in.FieldID == "" {
		return fmt.Errorf("%w: field_id is required", ErrInvalid)
	}
	if in.EventAt.IsZero() {
		return fmt.Errorf("%w: event_at is required", ErrInvalid)
	}
	if in.EventAt.After(now.Add(24 * time.Hour)) {
		return fmt.Errorf("%w: event_at is implausibly in the future", ErrInvalid)
	}
	return nil
}

// RegisterFacts stores standardised operation facts. Upserts key on
// (device_serial, segment_id) so a corrected re-submission replaces the
// prior row; review runs keep their own snapshots, so old runs are unaffected.
func (s *Service) RegisterFacts(ctx context.Context, region string, in []FactInput, actor string) (int, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return 0, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if len(in) == 0 {
		return 0, fmt.Errorf("%w: facts is empty", ErrInvalid)
	}
	if len(in) > 1000 {
		return 0, fmt.Errorf("%w: at most 1000 facts per request", ErrInvalid)
	}
	now := s.now().UTC()
	written := 0
	for _, f := range in {
		if err := validateFact(f, now); err != nil {
			return written, err
		}
		received := now
		if f.ReceivedAt != nil {
			received = f.ReceivedAt.UTC()
		}
		completed := true
		if f.Completed != nil {
			completed = *f.Completed
		}
		detail := f.Detail
		if detail == nil {
			detail = map[string]any{}
		}
		tag, err := pool.Exec(ctx, `
			insert into sequence_facts (fact_id, source, device_serial, segment_id, operation_type,
				field_id, event_at, received_at, completed, batch_id, detail)
			values ($1,'imported',$2,$3,$4,$5,$6,$7,$8,$9,$10)
			on conflict (device_serial, segment_id) do update
			  set operation_type=excluded.operation_type, field_id=excluded.field_id,
			      event_at=excluded.event_at, received_at=excluded.received_at,
			      completed=excluded.completed, batch_id=excluded.batch_id, detail=excluded.detail`,
			f.FactID, f.DeviceSerial, f.SegmentID, f.OperationType, f.FieldID,
			f.EventAt.UTC(), received, completed, f.BatchID, jsonBytes(detail))
		if err != nil {
			return written, err
		}
		written += int(tag.RowsAffected())
	}
	return written, nil
}

// ImportFactsFromOperations projects already-uploaded operation_events into
// standardised sequence facts without any manual re-entry.
func (s *Service) ImportFactsFromOperations(ctx context.Context, region string) (int, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return 0, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	rows, err := pool.Query(ctx, `
		select device_serial, segment_id, operation_type, field_id, started_at, received_at,
		       completed, upload_batch
		from operation_events order by journal_seq`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	type opRow struct {
		device, segment, opType, field, batch string
		started, received                     time.Time
		completed                             bool
	}
	ops := []opRow{}
	for rows.Next() {
		var o opRow
		if err := rows.Scan(&o.device, &o.segment, &o.opType, &o.field, &o.started,
			&o.received, &o.completed, &o.batch); err != nil {
			return 0, err
		}
		ops = append(ops, o)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	written := 0
	for _, o := range ops {
		factID := "fact-" + o.device + "-" + o.segment
		tag, err := pool.Exec(ctx, `
			insert into sequence_facts (fact_id, source, device_serial, segment_id, operation_type,
				field_id, event_at, received_at, completed, batch_id, detail)
			values ($1,'uploaded',$2,$3,$4,$5,$6,$7,$8,$9,'{}'::jsonb)
			on conflict (device_serial, segment_id) do nothing`,
			factID, o.device, o.segment, o.opType, o.field, o.started.UTC(),
			o.received.UTC(), o.completed, o.batch)
		if err != nil {
			return written, err
		}
		written += int(tag.RowsAffected())
	}
	return written, nil
}

func (s *Service) ListFacts(ctx context.Context, region string, fieldID string, limit int) ([]model.SequenceFact, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return nil, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	q := `select fact_id, source, device_serial, segment_id, operation_type, field_id,
	             event_at, received_at, completed, batch_id, detail, created_at
	      from sequence_facts`
	args := []any{}
	if fieldID != "" {
		q += ` where field_id=$1`
		args = append(args, fieldID)
	}
	q += fmt.Sprintf(` order by event_at, received_at, fact_id limit %d`, limit)
	rows, err := pool.Query(ctx, q, args...)
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

// --- field seasons ------------------------------------------------------

func (s *Service) UpsertFieldSeason(ctx context.Context, region string, in FieldSeasonInput) error {
	pool := s.db.Region(region)
	if pool == nil {
		return fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if in.SeasonID == "" || in.FieldID == "" || in.CropCode == "" {
		return fmt.Errorf("%w: season_id, field_id and crop_code are required", ErrInvalid)
	}
	if !in.PlannedTo.After(in.PlannedFrom) {
		return fmt.Errorf("%w: planned_to must be after planned_from", ErrInvalid)
	}
	status := in.Status
	if status == "" {
		status = "active"
	}
	switch status {
	case "planned", "active", "closed", "disputed":
	default:
		return fmt.Errorf("%w: unknown season status %q", ErrInvalid, status)
	}
	_, err := pool.Exec(ctx, `
		insert into field_seasons (season_id, field_id, crop_code, planned_from, planned_to, status, note)
		values ($1,$2,$3,$4,$5,$6,$7)
		on conflict (season_id) do update
		  set field_id=excluded.field_id, crop_code=excluded.crop_code,
		      planned_from=excluded.planned_from, planned_to=excluded.planned_to,
		      status=excluded.status, note=excluded.note, updated_at=now()`,
		in.SeasonID, in.FieldID, in.CropCode, in.PlannedFrom.UTC(), in.PlannedTo.UTC(), status, in.Note)
	return err
}

func (s *Service) ListFieldSeasons(ctx context.Context, region string) ([]model.FieldSeason, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return nil, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	rows, err := pool.Query(ctx, `
		select season_id, field_id, crop_code, planned_from, planned_to, status, note, updated_at
		from field_seasons order by field_id, planned_from`)
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

// --- boundaries ---------------------------------------------------------

func (s *Service) UpsertBoundary(ctx context.Context, region string, in BoundaryInput) error {
	pool := s.db.Region(region)
	if pool == nil {
		return fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if in.RevisionID == "" || in.FieldID == "" || in.ValidFrom.IsZero() {
		return fmt.Errorf("%w: revision_id, field_id and valid_from are required", ErrInvalid)
	}
	if in.ValidUntil != nil && !in.ValidUntil.After(in.ValidFrom) {
		return fmt.Errorf("%w: valid_until must be after valid_from", ErrInvalid)
	}
	geo := in.Geometry
	if geo == nil {
		geo = map[string]any{}
	}
	_, err := pool.Exec(ctx, `
		insert into field_boundary_revisions
		  (revision_id, field_id, valid_from, valid_until, boundary_label, geometry, superseded_by, note)
		values ($1,$2,$3,$4,$5,$6,$7,$8)
		on conflict (revision_id) do update
		  set field_id=excluded.field_id, valid_from=excluded.valid_from,
		      valid_until=excluded.valid_until, boundary_label=excluded.boundary_label,
		      geometry=excluded.geometry, superseded_by=excluded.superseded_by, note=excluded.note`,
		in.RevisionID, in.FieldID, in.ValidFrom.UTC(), nullableTime(in.ValidUntil), in.BoundaryLabel,
		jsonBytes(geo), in.SupersededBy, in.Note)
	return err
}

func (s *Service) ListBoundaries(ctx context.Context, region string) ([]model.BoundaryRevision, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return nil, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
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

// --- crop calendars -----------------------------------------------------

func (s *Service) UpsertCropCalendar(ctx context.Context, region string, in CropCalendarInput) error {
	pool := s.db.Region(region)
	if pool == nil {
		return fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if in.CalendarID == "" || in.FieldID == "" || in.SeasonID == "" || in.CropCode == "" || in.ValidFrom.IsZero() {
		return fmt.Errorf("%w: calendar_id, field_id, season_id, crop_code and valid_from are required", ErrInvalid)
	}
	if in.ValidUntil != nil && !in.ValidUntil.After(in.ValidFrom) {
		return fmt.Errorf("%w: valid_until must be after valid_from", ErrInvalid)
	}
	_, err := pool.Exec(ctx, `
		insert into crop_calendars (calendar_id, field_id, season_id, crop_code, valid_from, valid_until, note)
		values ($1,$2,$3,$4,$5,$6,$7)
		on conflict (calendar_id) do update
		  set field_id=excluded.field_id, season_id=excluded.season_id, crop_code=excluded.crop_code,
		      valid_from=excluded.valid_from, valid_until=excluded.valid_until, note=excluded.note`,
		in.CalendarID, in.FieldID, in.SeasonID, in.CropCode, in.ValidFrom.UTC(),
		nullableTime(in.ValidUntil), in.Note)
	return err
}

func (s *Service) ListCropCalendars(ctx context.Context, region string) ([]model.CropCalendar, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return nil, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
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

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

// --- rule sets ----------------------------------------------------------

// UpsertRuleSet validates the rule document against its declared engine and
// stores it verbatim.
func (s *Service) UpsertRuleSet(ctx context.Context, region string, in RuleSetInput) error {
	pool := s.db.Region(region)
	if pool == nil {
		return fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if in.RuleSetID == "" || in.Version == "" {
		return fmt.Errorf("%w: rule_set_id and version are required", ErrInvalid)
	}
	status := in.Status
	if status == "" {
		status = "active"
	}
	switch status {
	case "draft", "active", "retired":
	default:
		return fmt.Errorf("%w: unknown rule set status %q", ErrInvalid, status)
	}
	switch in.Engine {
	case ruleEngineFSM:
		var doc FSMRuleDoc
		if err := decodeMap(in.RuleDoc, &doc); err != nil {
			return err
		}
		if err := validateFSMRuleDoc(doc); err != nil {
			return err
		}
	case ruleEnginePO:
		var doc PORuleDoc
		if err := decodeMap(in.RuleDoc, &doc); err != nil {
			return err
		}
		if err := validatePORuleDoc(doc); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: engine must be fsm or partial_order", ErrInvalid)
	}
	_, err := pool.Exec(ctx, `
		insert into agronomy_rule_sets (rule_set_id, version, engine, crop_code, rule_doc, status)
		values ($1,$2,$3,$4,$5,$6)
		on conflict (rule_set_id) do update
		  set version=excluded.version, engine=excluded.engine, crop_code=excluded.crop_code,
		      rule_doc=excluded.rule_doc, status=excluded.status`,
		in.RuleSetID, in.Version, in.Engine, in.CropCode, jsonBytes(in.RuleDoc), status)
	return err
}

func decodeMap(m map[string]any, dst any) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("%w: malformed rule document: %v", ErrInvalid, err)
	}
	return nil
}

func (s *Service) ListRuleSets(ctx context.Context, region string) ([]model.RuleSet, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return nil, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	rows, err := pool.Query(ctx, `
		select rule_set_id, version, engine, crop_code, rule_doc, status, created_at
		from agronomy_rule_sets order by engine, crop_code, version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.RuleSet{}
	for rows.Next() {
		var v model.RuleSet
		var doc []byte
		if err := rows.Scan(&v.RuleSetID, &v.Version, &v.Engine, &v.CropCode, &doc, &v.Status, &v.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(doc, &v.RuleDoc)
		out = append(out, v)
	}
	return out, rows.Err()
}

// SeedDefaultRules makes every region converge on the generic v1 rule sets.
// Idempotent: active defaults already present are left untouched.
func (s *Service) SeedDefaultRules(ctx context.Context, region string) error {
	pool := s.db.Region(region)
	if pool == nil {
		return fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	specs := []struct {
		id     string
		engine string
		doc    any
	}{
		{"rules-fsm-generic-v1", ruleEngineFSM, DefaultFSMRules()},
		{"rules-po-generic-v1", ruleEnginePO, DefaultPORules()},
	}
	for _, sp := range specs {
		var exists string
		err := pool.QueryRow(ctx, `select rule_set_id from agronomy_rule_sets where rule_set_id=$1`, sp.id).Scan(&exists)
		if err == nil {
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if _, err := pool.Exec(ctx, `
			insert into agronomy_rule_sets (rule_set_id, version, engine, crop_code, rule_doc, status)
			values ($1,$2,$3,'',$4,'active')`,
			sp.id, ruleVersionV1, sp.engine, jsonBytes(sp.doc)); err != nil {
			return err
		}
	}
	return nil
}

// =====================================================================
// Review run
// =====================================================================

type storedRuleSet struct {
	id       string
	version  string
	engine   string
	cropCode string
	doc      map[string]any
}

func (s *Service) loadRuleSet(ctx context.Context, region, id string) (storedRuleSet, error) {
	pool := s.db.Region(region)
	var rs storedRuleSet
	var doc []byte
	err := pool.QueryRow(ctx, `
		select rule_set_id, version, engine, crop_code, rule_doc from agronomy_rule_sets where rule_set_id=$1`, id).
		Scan(&rs.id, &rs.version, &rs.engine, &rs.cropCode, &doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return rs, fmt.Errorf("%w: rule set %s", ErrNotFound, id)
	}
	if err != nil {
		return rs, err
	}
	_ = json.Unmarshal(doc, &rs.doc)
	return rs, nil
}

func (s *Service) activeRuleSet(ctx context.Context, region, engine, cropCode string) (storedRuleSet, error) {
	pool := s.db.Region(region)
	// A crop-specific rule version, even a retired one, means the crop is
	// governed by its own version line. When the newest crop-specific row is
	// not active there is no applicable replacement, so the generic rule set
	// must NOT silently take over: the variety stays not-applicable.
	var cropStatus string
	err := pool.QueryRow(ctx, `
		select status from agronomy_rule_sets
		where engine=$1 and crop_code=$2
		order by created_at desc limit 1`, engine, cropCode).Scan(&cropStatus)
	switch {
	case err == nil:
		if cropStatus != "active" {
			return storedRuleSet{}, fmt.Errorf("%w: %s rule version for crop %q is %s",
				ErrNotFound, engine, cropCode, cropStatus)
		}
	case errors.Is(err, pgx.ErrNoRows):
		// No crop-specific line: the generic set is allowed to apply.
	default:
		return storedRuleSet{}, err
	}
	var rs storedRuleSet
	var doc []byte
	err = pool.QueryRow(ctx, `
		select rule_set_id, version, engine, crop_code, rule_doc
		from agronomy_rule_sets
		where engine=$1 and status='active' and (crop_code=$2 or crop_code='')
		order by case when crop_code=$2 then 0 else 1 end, created_at
		limit 1`, engine, cropCode).Scan(&rs.id, &rs.version, &rs.engine, &rs.cropCode, &doc)
	if errors.Is(err, pgx.ErrNoRows) {
		return rs, fmt.Errorf("%w: no active %s rule set for crop %q", ErrNotFound, engine, cropCode)
	}
	if err != nil {
		return rs, err
	}
	_ = json.Unmarshal(doc, &rs.doc)
	return rs, nil
}

// CreateSequenceReview runs both engines over every field season whose
// planning window intersects the review period, then freezes the whole
// result under one version bundle.
func (s *Service) CreateSequenceReview(ctx context.Context, region string, in ReviewRunInput, actor, role string) (model.SequenceReviewDetail, error) {
	pool := s.db.Region(region)
	if pool == nil {
		return model.SequenceReviewDetail{}, fmt.Errorf("%w: unknown region %s", ErrNotFound, region)
	}
	if !in.PeriodTo.After(in.PeriodFrom) {
		return model.SequenceReviewDetail{}, fmt.Errorf("%w: period_to must be after period_from", ErrInvalid)
	}
	now := s.now().UTC()
	cutoff := now

	seasons, err := s.listSeasonsForPeriod(ctx, region, in.PeriodFrom, in.PeriodTo)
	if err != nil {
		return model.SequenceReviewDetail{}, err
	}
	allFacts, err := s.factsForPeriod(ctx, region, in.PeriodFrom, in.PeriodTo)
	if err != nil {
		return model.SequenceReviewDetail{}, err
	}
	boundaries, err := s.allBoundaries(ctx, region)
	if err != nil {
		return model.SequenceReviewDetail{}, err
	}
	calendars, err := s.allCalendars(ctx, region)
	if err != nil {
		return model.SequenceReviewDetail{}, err
	}

	factVersion := "fv_" + sha256Hex(jsonBytes(factVersionPayload(allFacts, cutoff)))[:12]
	attrVersion := "av_" + sha256Hex(jsonBytes(attributionVersionPayload(boundaries, calendars)))[:12]

	runID := randID("seqrun")
	tx, err := pool.Begin(ctx)
	if err != nil {
		return model.SequenceReviewDetail{}, err
	}
	defer tx.Rollback(ctx)

	detail := model.SequenceReviewDetail{
		Run: model.SequenceReviewRun{
			RunID: runID, RegionCode: region, Status: "completed",
			PeriodFrom: in.PeriodFrom.UTC(), PeriodTo: in.PeriodTo.UTC(),
			FactVersion: factVersion, AttributionVersion: attrVersion,
			FactCutoff: cutoff, Note: in.Note, CreatedBy: actor, CreatedAt: now,
		},
		Engines: []model.SequenceReviewEngineRollup{},
		Fields:  []model.SequenceReviewField{},
		Events:  []model.SequenceReviewEvent{},
		Issues:  []model.SequenceReviewIssue{},
	}

	// Distinct rule sets actually used, keyed engine -> rule_set_id. A run
	// spanning several crops can therefore carry several rule versions in one
	// fixed bundle instead of having later crops overwrite earlier ones.
	usedRules := map[string]map[string]storedRuleSet{
		ruleEngineFSM: {}, ruleEnginePO: {},
	}
	rollups := map[string]*model.SequenceReviewEngineRollup{
		ruleEngineFSM: {RunID: runID, Engine: ruleEngineFSM, ConfirmedStageCounts: map[string]int{}},
		ruleEnginePO:  {RunID: runID, Engine: ruleEnginePO, ConfirmedStageCounts: map[string]int{}},
	}

	coveredFacts := map[string]bool{}
	for _, season := range seasons {
		fd := buildFieldDetail(runID, season, allFacts, boundaries, calendars, coveredFacts)

		// Resolve the rule set that applies to this crop.
		fsmRS, poRS := s.resolveRules(ctx, region, season.CropCode, in)
		if fsmRS.id == "" || poRS.id == "" {
			// The version combination is fixed: if either engine has no rule
			// version for this crop, the season is not reviewable under this
			// bundle. Do not run the other engine either — mixing an
			// applicable engine with a non-applicable one would let reviewers
			// pick whichever path produces a cleaner completion rate.
			fd.AttributionStatus = keepWorse(fd.AttributionStatus, "pending_review")
			fd.RuleApplicability = "not_applicable"
			fd.FSMStatus, fd.POStatus = "pending_review", "pending_review"
			fd.Summary["rule_applicability"] = "rule_version_not_applicable"
			missing := []string{}
			if fsmRS.id == "" {
				missing = append(missing, ruleEngineFSM)
			}
			if poRS.id == "" {
				missing = append(missing, ruleEnginePO)
			}
			// The fixed bundle is incomplete for both engines, so both
			// engine rollups carry the not-applicable trace.
			for _, eng := range []string{ruleEngineFSM, ruleEnginePO} {
				fd.addEngineIssue(eng, "pending_review", "rule_version_not_applicable",
					fmt.Sprintf("fixed version bundle is incomplete for crop %s: missing %v",
						season.CropCode, missing),
					map[string]any{"crop_code": season.CropCode, "season_id": season.SeasonID,
						"missing_engines": missing})
			}
		} else {
			fd.RuleApplicability = "applicable"
			usedRules[ruleEngineFSM][fsmRS.id] = fsmRS
			usedRules[ruleEnginePO][poRS.id] = poRS
			// Record which exact versions explain this field season.
			fd.Summary["fsm_rule_set_id"] = fsmRS.id
			fd.Summary["po_rule_set_id"] = poRS.id
			var fsmDoc FSMRuleDoc
			var poDoc PORuleDoc
			if err := decodeMap(fsmRS.doc, &fsmDoc); err != nil {
				return detail, err
			}
			if err := decodeMap(poRS.doc, &poDoc); err != nil {
				return detail, err
			}
			facts := fd.engineFacts
			markLateArrivals(facts)
			fsmRes := runFSM(season.SeasonID, season, facts, fsmDoc)
			poRes := runPartialOrder(season.SeasonID, season, facts, poDoc)
			fd.applyEngineResult(fsmRes)
			fd.applyEngineResult(poRes)
		}

		detail.Fields = append(detail.Fields, fd.SequenceReviewField)
		detail.Events = append(detail.Events, fd.events...)
		detail.Issues = append(detail.Issues, fd.issues...)
	}

	// Facts that no season covers cannot be attributed: keep them pending,
	// never drop them and never guess a season for them.
	for _, f := range allFacts {
		if coveredFacts[f.FactID] {
			continue
		}
		for _, eng := range []string{ruleEngineFSM, ruleEnginePO} {
			detail.Issues = append(detail.Issues, model.SequenceReviewIssue{
				RunID: runID, SeasonID: "", Engine: eng, Severity: "pending_review",
				Code:     "fact_without_season_attribution",
				Message:  fmt.Sprintf("fact %s on field %s has no covering field season", f.FactID, f.FieldID),
				Evidence: map[string]any{"fact_id": f.FactID, "field_id": f.FieldID, "event_at": f.EventAt.UTC()},
			})
		}
	}

	// Finalize engine rollups. Each field-season contributes exactly once;
	// issues that belong to no season (facts without an attribution) are
	// counted separately as pending so they are not lost but never doubled.
	orphanPending := map[string]int{ruleEngineFSM: 0, ruleEnginePO: 0}
	for _, iss := range detail.Issues {
		if iss.SeasonID != "" {
			continue
		}
		engines := []string{iss.Engine}
		if iss.Engine == "both" {
			engines = []string{ruleEngineFSM, ruleEnginePO}
		}
		for _, eng := range engines {
			orphanPending[eng]++
		}
	}
	for _, frow := range detail.Fields {
		for eng, roll := range rollups {
			roll.FieldCount++
			status := frow.FSMStatus
			confirmed := frow.FSMConfirmedStages
			if eng == ruleEnginePO {
				status = frow.POStatus
				confirmed = frow.POConfirmedStages
			}
			switch status {
			case "conflict":
				roll.ConflictCount++
			case "pending_review":
				roll.PendingCount++
			}
			for _, st := range confirmed {
				roll.ConfirmedStageCounts[st]++
			}
		}
	}
	for _, eng := range []string{ruleEngineFSM, ruleEnginePO} {
		roll := rollups[eng]
		roll.PendingCount += orphanPending[eng]
		roll.Status = "ok"
		for _, frow := range detail.Fields {
			st := frow.FSMStatus
			if eng == ruleEnginePO {
				st = frow.POStatus
			}
			if st == "conflict" {
				roll.Status = "conflict"
			} else if st == "pending_review" && roll.Status == "ok" {
				roll.Status = "pending_review"
			}
		}
		if orphanPending[eng] > 0 && roll.Status == "ok" {
			roll.Status = "pending_review"
		}
		detail.Engines = append(detail.Engines, *roll)
	}

	// Version bundle: fixed combination of semantic facts, attribution and
	// every rule version this run actually used. The header columns keep a
	// single id only when the whole run used one set per engine; the full
	// list always lives in version_bundle.rule_sets.
	detail.Run.FSMRuleSetID = soleRuleID(usedRules[ruleEngineFSM])
	detail.Run.PORuleSetID = soleRuleID(usedRules[ruleEnginePO])
	detail.Run.VersionBundle = map[string]any{
		"fact_version":        factVersion,
		"attribution_version": attrVersion,
		"fact_cutoff":         cutoff,
		"rule_sets":           buildBundleRules(usedRules),
	}

	if err := s.persistReview(ctx, tx, detail); err != nil {
		return detail, err
	}
	if err := tx.Commit(ctx); err != nil {
		return detail, err
	}
	_, _ = s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: "sequence_review.create",
		SubjectType: "sequence_review_run", SubjectID: runID, RegionCode: region, Decision: "allow",
		Detail: map[string]any{
			"field_seasons": len(detail.Fields), "version_bundle": detail.Run.VersionBundle,
		},
	})
	return detail, nil
}

func (s *Service) resolveRules(ctx context.Context, region, crop string, in ReviewRunInput) (storedRuleSet, storedRuleSet) {
	pick := func(engine, explicit string) storedRuleSet {
		if explicit != "" {
			rs, err := s.loadRuleSet(ctx, region, explicit)
			if err != nil || rs.engine != engine {
				return storedRuleSet{}
			}
			if rs.cropCode != "" && rs.cropCode != crop {
				return storedRuleSet{}
			}
			return rs
		}
		rs, err := s.activeRuleSet(ctx, region, engine, crop)
		if err != nil {
			return storedRuleSet{}
		}
		return rs
	}
	return pick(ruleEngineFSM, in.FSMRuleSetID), pick(ruleEnginePO, in.PORuleSetID)
}

func ruleBundleEntry(rs storedRuleSet) map[string]any {
	return map[string]any{
		"rule_set_id": rs.id, "version": rs.version, "engine": rs.engine,
		"crop_code": rs.cropCode, "content_sha256": sha256Hex(jsonBytes(rs.doc)),
	}
}

func soleRuleID(m map[string]storedRuleSet) string {
	if len(m) == 1 {
		for id := range m {
			return id
		}
	}
	return ""
}

func buildBundleRules(byEngine map[string]map[string]storedRuleSet) map[string]any {
	out := map[string]any{}
	engines := make([]string, 0, len(byEngine))
	for eng := range byEngine {
		engines = append(engines, eng)
	}
	sort.Strings(engines)
	for _, eng := range engines {
		ids := make([]string, 0, len(byEngine[eng]))
		for id := range byEngine[eng] {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		entries := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			entries = append(entries, ruleBundleEntry(byEngine[eng][id]))
		}
		out[eng] = entries
	}
	return out
}

// fieldDraft accumulates one field-season row plus its raw engine outputs.
type fieldDraft struct {
	model.SequenceReviewField
	engineFacts []engineFact
	events      []model.SequenceReviewEvent
	issues      []model.SequenceReviewIssue
}

func buildFieldDetail(runID string, season model.FieldSeason, allFacts []model.SequenceFact,
	boundaries []model.BoundaryRevision, calendars []model.CropCalendar, covered map[string]bool) *fieldDraft {
	fd := &fieldDraft{
		SequenceReviewField: model.SequenceReviewField{
			RunID: runID, SeasonID: season.SeasonID, FieldID: season.FieldID, CropCode: season.CropCode,
			AttributionStatus: "ok", FSMStatus: "ok", POStatus: "ok",
			FSMConfirmedStages: []string{}, POConfirmedStages: []string{},
			MissingPrerequisites: []map[string]any{}, AllowedPaths: []map[string]any{},
			PendingRanges: []map[string]any{}, Summary: map[string]any{},
		},
		engineFacts: []engineFact{},
		events:      []model.SequenceReviewEvent{},
		issues:      []model.SequenceReviewIssue{},
	}
	for _, f := range allFacts {
		if f.FieldID != season.FieldID || f.EventAt.Before(season.PlannedFrom) || !f.EventAt.Before(season.PlannedTo) {
			continue
		}
		covered[f.FactID] = true
		fd.engineFacts = append(fd.engineFacts, engineFact{SequenceFact: f})
	}

	// Boundary revision overlap over the season window. A revision that was
	// explicitly superseded is a correction record, not a competing current
	// boundary: corrections are reviewed through a new run rather than
	// reported as an overlap against the revision that replaced them.
	fieldBounds := []model.BoundaryRevision{}
	for _, b := range boundaries {
		if b.FieldID == season.FieldID && b.SupersededBy == "" &&
			windowsOverlap(b.ValidFrom, b.ValidUntil, season.PlannedFrom, &season.PlannedTo) {
			fieldBounds = append(fieldBounds, b)
		}
	}
	for i := 0; i < len(fieldBounds); i++ {
		for j := i + 1; j < len(fieldBounds); j++ {
			a, b := fieldBounds[i], fieldBounds[j]
			if windowsOverlap(a.ValidFrom, a.ValidUntil, b.ValidFrom, b.ValidUntil) {
				fd.AttributionStatus = keepWorse(fd.AttributionStatus, "conflict")
				fd.addBothIssue("conflict", "boundary_overlap",
					fmt.Sprintf("boundary revisions %s and %s overlap on field %s", a.RevisionID, b.RevisionID, season.FieldID),
					map[string]any{
						"revision_a": a.RevisionID, "revision_b": b.RevisionID, "field_id": season.FieldID,
						"a_valid_from": a.ValidFrom.UTC(), "a_valid_until": a.ValidUntil,
						"b_valid_from": b.ValidFrom.UTC(), "b_valid_until": b.ValidUntil,
					})
			}
		}
	}

	// Mutually exclusive crops valid at the same instant.
	fieldCals := []model.CropCalendar{}
	for _, c := range calendars {
		if c.FieldID == season.FieldID {
			fieldCals = append(fieldCals, c)
		}
	}
	for _, f := range fd.engineFacts {
		crops := map[string]string{}
		for _, c := range fieldCals {
			if windowContains(c.ValidFrom, c.ValidUntil, f.EventAt) {
				crops[c.CropCode] = c.CalendarID
			}
		}
		if len(crops) > 1 {
			fd.AttributionStatus = keepWorse(fd.AttributionStatus, "conflict")
			keys := make([]string, 0, len(crops))
			for k := range crops {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			fd.addBothIssue("conflict", "mutex_crop_same_window",
				fmt.Sprintf("mutually exclusive crops %v declared on field %s at %s",
					keys, season.FieldID, f.EventAt.UTC().Format(time.RFC3339)),
				map[string]any{
					"fact_id": f.FactID, "field_id": season.FieldID, "crops": keys,
					"event_at": f.EventAt.UTC(),
				})
		}
	}
	fd.Summary["fact_count"] = len(fd.engineFacts)
	return fd
}

func (fd *fieldDraft) addBothIssue(severity, code, msg string, evidence map[string]any) {
	for _, eng := range []string{ruleEngineFSM, ruleEnginePO} {
		fd.issues = append(fd.issues, model.SequenceReviewIssue{
			SeasonID: fd.SeasonID, Engine: eng, Severity: severity, Code: code,
			Message: msg, Evidence: evidence,
		})
	}
	if severity == "conflict" {
		fd.FSMStatus, fd.POStatus = "conflict", "conflict"
	} else {
		if fd.FSMStatus == "ok" {
			fd.FSMStatus = "pending_review"
		}
		if fd.POStatus == "ok" {
			fd.POStatus = "pending_review"
		}
	}
}

func (fd *fieldDraft) addEngineIssue(engine, severity, code, msg string, evidence map[string]any) {
	fd.issues = append(fd.issues, model.SequenceReviewIssue{
		SeasonID: fd.SeasonID, Engine: engine, Severity: severity, Code: code,
		Message: msg, Evidence: evidence,
	})
	if engine == ruleEngineFSM {
		fd.FSMStatus = keepWorse(fd.FSMStatus, severity)
	} else {
		fd.POStatus = keepWorse(fd.POStatus, severity)
	}
}

func (fd *fieldDraft) applyEngineResult(r *fieldEngineResult) {
	for i := range r.events {
		r.events[i].RunID = fd.RunID
	}
	for i := range r.issues {
		r.issues[i].RunID = fd.RunID
	}
	fd.events = append(fd.events, r.events...)
	fd.issues = append(fd.issues, r.issues...)
	if r.engine == ruleEngineFSM {
		fd.FSMState = r.state
		fd.FSMConfirmedStages = r.confirmedStages
		fd.FSMStatus = keepWorse(fd.FSMStatus, r.status)
	} else {
		fd.POConfirmedStages = r.confirmedStages
		fd.POStatus = keepWorse(fd.POStatus, r.status)
	}
	fd.MissingPrerequisites = append(fd.MissingPrerequisites, r.missing...)
	fd.AllowedPaths = append(fd.AllowedPaths, r.allowedPaths...)
	fd.PendingRanges = append(fd.PendingRanges, r.pendingRanges...)
}

func keepWorse(current, candidate string) string {
	rank := map[string]int{"ok": 0, "pending_review": 1, "conflict": 2}
	if rank[candidate] > rank[current] {
		return candidate
	}
	return current
}

func windowsOverlap(fromA time.Time, untilA *time.Time, fromB time.Time, untilB *time.Time) bool {
	endA := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	if untilA != nil {
		endA = *untilA
	}
	endB := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	if untilB != nil {
		endB = *untilB
	}
	return fromA.Before(endB) && fromB.Before(endA)
}

func windowContains(from time.Time, until *time.Time, t time.Time) bool {
	if t.Before(from) {
		return false
	}
	if until != nil && !t.Before(*until) {
		return false
	}
	return true
}

func factVersionPayload(facts []model.SequenceFact, cutoff time.Time) any {
	type item struct {
		FactID string `json:"fact_id"`
		Event  string `json:"event_at"`
		Recv   string `json:"received_at"`
		Op     string `json:"operation_type"`
		Field  string `json:"field_id"`
	}
	items := make([]item, 0, len(facts))
	for _, f := range facts {
		items = append(items, item{
			f.FactID, f.EventAt.UTC().Format(time.RFC3339Nano),
			f.ReceivedAt.UTC().Format(time.RFC3339Nano), f.OperationType, f.FieldID,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].FactID < items[j].FactID })
	return map[string]any{"cutoff": cutoff.UTC(), "facts": items}
}

func attributionVersionPayload(b []model.BoundaryRevision, c []model.CropCalendar) any {
	return map[string]any{
		"boundary_revision_ids": idsOf(b),
		"calendar_ids":          calIDs(c),
	}
}

func idsOf(b []model.BoundaryRevision) []string {
	out := make([]string, 0, len(b))
	for _, v := range b {
		out = append(out, v.RevisionID)
	}
	sort.Strings(out)
	return out
}

func calIDs(c []model.CropCalendar) []string {
	out := make([]string, 0, len(c))
	for _, v := range c {
		out = append(out, v.CalendarID)
	}
	sort.Strings(out)
	return out
}
