package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"agrisync/internal/model"
)

type ApprovalInput struct {
	Purpose      string     `json:"purpose"`
	Recipient    string     `json:"recipient"`
	ScopeRegions []string   `json:"scope_regions"`
	ScopeMetrics []string   `json:"scope_metrics"`
	ValidFrom    *time.Time `json:"valid_from"`
	ValidUntil   *time.Time `json:"valid_until"`
}

func (s *Service) CreateApproval(ctx context.Context, in ApprovalInput, actor, role string) (model.OutboundRequest, error) {
	if strings.TrimSpace(in.Purpose) == "" || strings.TrimSpace(in.Recipient) == "" {
		return model.OutboundRequest{}, fmt.Errorf("%w: purpose and recipient are required", ErrInvalid)
	}
	regions, err := normalizeRegions(in.ScopeRegions, s.cfg)
	if err != nil {
		return model.OutboundRequest{}, err
	}
	metrics, err := normalizeMetrics(in.ScopeMetrics)
	if err != nil {
		return model.OutboundRequest{}, err
	}
	if in.ValidFrom != nil && in.ValidUntil != nil && !in.ValidUntil.After(*in.ValidFrom) {
		return model.OutboundRequest{}, fmt.Errorf("%w: valid_until must be after valid_from", ErrInvalid)
	}
	id := randID("apr")
	now := s.now().UTC()
	_, err = s.db.Control().Exec(ctx, `
		insert into outbound_requests (id, purpose, recipient, scope_regions, scope_metrics, status,
			version, valid_from, valid_until, requested_by, created_at, updated_at)
		values ($1,$2,$3,$4,$5,'pending',1,$6,$7,$8,$9,$9)`,
		id, strings.TrimSpace(in.Purpose), strings.TrimSpace(in.Recipient), regions, metrics,
		in.ValidFrom, in.ValidUntil, actor, now)
	if err != nil {
		return model.OutboundRequest{}, err
	}
	if _, err := s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: "approval.request", SubjectType: "outbound_request",
		SubjectID: id, Decision: "pending",
		Detail: map[string]any{"purpose": in.Purpose, "recipient": in.Recipient,
			"scope_regions": regions, "scope_metrics": metrics},
	}); err != nil {
		return model.OutboundRequest{}, err
	}
	return s.GetApproval(ctx, id)
}

func (s *Service) DecideApproval(ctx context.Context, id, decision, note, actor, role string) (model.OutboundRequest, error) {
	if decision != "approved" && decision != "rejected" {
		return model.OutboundRequest{}, fmt.Errorf("%w: decision must be approved or rejected", ErrInvalid)
	}
	now := s.now().UTC()
	tx, err := s.db.Control().Begin(ctx)
	if err != nil {
		return model.OutboundRequest{}, err
	}
	defer tx.Rollback(ctx)
	var status string
	if err := tx.QueryRow(ctx, `select status from outbound_requests where id=$1 for update`, id).
		Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.OutboundRequest{}, fmt.Errorf("%w: approval %s", ErrNotFound, id)
		}
		return model.OutboundRequest{}, err
	}
	if status != "pending" {
		return model.OutboundRequest{}, fmt.Errorf("%w: approval %s is already %s", ErrConflict, id, status)
	}
	if _, err := tx.Exec(ctx, `
		update outbound_requests set status=$2, decided_by=$3, decided_at=$4, decision_note=$5,
		  version=version+1, updated_at=$4 where id=$1`, id, decision, actor, now, note); err != nil {
		return model.OutboundRequest{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.OutboundRequest{}, err
	}
	if _, err := s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: "approval." + decision, SubjectType: "outbound_request",
		SubjectID: id, Decision: "allow",
		Detail: map[string]any{"note": note, "decided_by": actor},
	}); err != nil {
		return model.OutboundRequest{}, err
	}
	return s.GetApproval(ctx, id)
}

// RevokeApproval withdraws an approved request. A delivery that already
// started must re-read the approval before it sends; see Deliver.
func (s *Service) RevokeApproval(ctx context.Context, id, reason, actor, role string) (model.OutboundRequest, error) {
	if strings.TrimSpace(reason) == "" {
		return model.OutboundRequest{}, fmt.Errorf("%w: revoke reason is required", ErrInvalid)
	}
	now := s.now().UTC()
	tx, err := s.db.Control().Begin(ctx)
	if err != nil {
		return model.OutboundRequest{}, err
	}
	defer tx.Rollback(ctx)
	var status string
	if err := tx.QueryRow(ctx, `select status from outbound_requests where id=$1 for update`, id).
		Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.OutboundRequest{}, fmt.Errorf("%w: approval %s", ErrNotFound, id)
		}
		return model.OutboundRequest{}, err
	}
	if status != "approved" {
		return model.OutboundRequest{}, fmt.Errorf("%w: only approved requests can be revoked (current: %s)", ErrConflict, status)
	}
	if _, err := tx.Exec(ctx, `
		update outbound_requests set status='revoked', revoked_by=$2, revoked_at=$3,
		  revoke_reason=$4, version=version+1, updated_at=$3 where id=$1`,
		id, actor, now, reason); err != nil {
		return model.OutboundRequest{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.OutboundRequest{}, err
	}
	if _, err := s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: "approval.revoke", SubjectType: "outbound_request",
		SubjectID: id, Decision: "allow",
		Detail: map[string]any{"reason": reason},
	}); err != nil {
		return model.OutboundRequest{}, err
	}
	return s.GetApproval(ctx, id)
}

func (s *Service) GetApproval(ctx context.Context, id string) (model.OutboundRequest, error) {
	var r model.OutboundRequest
	err := s.db.Control().QueryRow(ctx, `
		select id, purpose, recipient, scope_regions, scope_metrics, status, version,
		       valid_from, valid_until, requested_by, coalesce(decided_by,''), decided_at,
		       coalesce(decision_note,''), coalesce(revoked_by,''), revoked_at,
		       coalesce(revoke_reason,''), created_at, updated_at
		from outbound_requests where id=$1`, id).
		Scan(&r.ID, &r.Purpose, &r.Recipient, &r.ScopeRegions, &r.ScopeMetrics, &r.Status, &r.Version,
			&r.ValidFrom, &r.ValidUntil, &r.RequestedBy, &r.DecidedBy, &r.DecidedAt,
			&r.DecisionNote, &r.RevokedBy, &r.RevokedAt, &r.RevokeReason, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, fmt.Errorf("%w: approval %s", ErrNotFound, id)
	}
	return r, err
}

func (s *Service) ListApprovals(ctx context.Context, limit int) ([]model.OutboundRequest, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.Control().Query(ctx, `select id from outbound_requests order by created_at desc limit $1`, limit)
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
	out := []model.OutboundRequest{}
	for _, id := range ids {
		r, err := s.GetApproval(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// Deliver re-validates authorization immediately before sending. It never
// relies on the scope captured when the aggregate run started: if the
// approval was revoked, narrowed, or expired in the meantime, the delivery is
// blocked and recorded as blocked (with the reason) instead of sent.
//
// This is the "approval revoked mid-run" guarantee: the checks below run
// after ComputeAggregate returns, and the scope used for the outbound artifact
// is re-read from the current approval row.
func (s *Service) Deliver(ctx context.Context, runID, actor, role string) (model.Delivery, error) {
	run, err := s.GetAggregate(ctx, runID)
	if err != nil {
		return model.Delivery{}, err
	}
	if run.Status == "delivered" {
		return model.Delivery{}, fmt.Errorf("%w: aggregate run %s was already delivered; a new delivery needs a new run",
			ErrConflict, run.ID)
	}
	approval, err := s.GetApproval(ctx, run.ApprovalID)
	if err != nil {
		return model.Delivery{}, err
	}
	now := s.now().UTC()
	d := model.Delivery{
		ID: randID("dlv"), RunID: run.ID, ApprovalID: approval.ID, Channel: s.cfg.DeliveryChannel,
		RequestedBy: actor, CreatedAt: now, Status: "blocked", Checks: []model.Check{},
	}

	addCheck := func(name string, passed bool, detail string) {
		d.Checks = append(d.Checks, model.Check{Name: name, Passed: passed, Detail: detail})
	}

	addCheck("approval_status", approval.Status == "approved",
		"approval status is "+approval.Status)
	addCheck("approval_version_current", approval.Version == run.ApprovalVersion,
		fmt.Sprintf("run computed under version %d, current version %d", run.ApprovalVersion, approval.Version))
	inValidity := true
	if approval.ValidFrom != nil && now.Before(*approval.ValidFrom) {
		inValidity = false
	}
	if approval.ValidUntil != nil && now.After(*approval.ValidUntil) {
		inValidity = false
	}
	addCheck("approval_validity_window", inValidity, "checked against current approval validity window")

	// Re-read current scope and intersect with what the run actually
	// aggregated. A narrowed scope drops regions/metrics instead of exporting
	// what was authorized at start.
	runRegions := run.ScopeSnapshot
	staleScope := false
	for _, r := range runRegions {
		if !containsString(approval.ScopeRegions, r) {
			staleScope = true
		}
	}
	addCheck("region_scope_current", !staleScope,
		"run scope "+strings.Join(runRegions, ",")+" vs current approval "+strings.Join(approval.ScopeRegions, ","))

	currentMetrics := approval.ScopeMetrics
	staleMetrics := false
	for _, m := range run.MetricKeys {
		if !containsString(currentMetrics, m) {
			staleMetrics = true
		}
	}
	addCheck("metric_scope_current", !staleMetrics,
		"run metrics "+strings.Join(run.MetricKeys, ",")+" vs current approval "+strings.Join(currentMetrics, ","))

	// Every included region must still be in the current approval scope.
	stillIncluded := []string{}
	for _, r := range run.Coverage.IncludedRegions {
		if !containsString(approval.ScopeRegions, r) {
			stillIncluded = append(stillIncluded, r)
		}
	}
	addCheck("included_regions_authorized", len(stillIncluded) == 0,
		"included regions outside current scope: "+strings.Join(stillIncluded, ","))

	// Residency check: no restricted region's raw points may be present in the
	// group replica store, let alone in the artifact. This is read from the
	// policy table at delivery time, not from a startup snapshot.
	restrictedRegions, err := s.restrictedRegions(ctx)
	if err != nil {
		return d, err
	}
	leaked := []string{}
	for _, r := range restrictedRegions {
		var n int
		if err := s.db.Control().QueryRow(ctx, `
			select count(*) from replica_trajectories where source_region=$1`, r).Scan(&n); err != nil {
			return d, err
		}
		if n > 0 {
			leaked = append(leaked, fmt.Sprintf("%s:%d", r, n))
		}
	}
	addCheck("restricted_raw_never_in_group_store", len(leaked) == 0,
		"restricted raw trajectory replicas found: "+strings.Join(leaked, ","))

	effectiveRegions := intersect(approval.ScopeRegions, run.Coverage.IncludedRegions)
	effectiveMetrics := intersect(approval.ScopeMetrics, run.MetricKeys)
	artifact := s.buildArtifact(run, approval, effectiveRegions, effectiveMetrics, now)
	artifactJSON := jsonBytes(artifact)

	// Structural scan of the exact bytes that would be sent. The outbound
	// schema is aggregate-only, and this proves it rather than asserting it.
	forbidden := scanForRestrictedFields(artifact, "")
	addCheck("artifact_has_no_coordinates", len(forbidden) == 0,
		"restricted field paths found in artifact: "+strings.Join(forbidden, ","))
	addCheck("artifact_contains_only_aggregates", !artifact.ContainsRawPoints,
		"artifact declares contains_raw_trajectories="+fmt.Sprint(artifact.ContainsRawPoints))

	allPassed := true
	for _, c := range d.Checks {
		if !c.Passed {
			allPassed = false
			if d.BlockedReason == "" {
				d.BlockedReason = c.Name + ": " + c.Detail
			}
		}
	}

	tx, err := s.db.Control().Begin(ctx)
	if err != nil {
		return d, err
	}
	defer tx.Rollback(ctx)
	var storedArtifact any
	if allPassed {
		d.Status = "sent"
		sentAt := now
		d.SentAt = &sentAt
		d.ArtifactSHA = sha256Hex(artifactJSON)
		d.Artifact = artifact
		storedArtifact = artifactJSON
		if _, err := tx.Exec(ctx, `
			update aggregate_runs set status='delivered' where id=$1`, run.ID); err != nil {
			return d, err
		}
	} else {
		// A blocked delivery exposes no aggregate payload at all: the caller
		// sees which checks failed, never the data that failed the checks.
		d.Artifact = nil
		d.ArtifactSHA = ""
		storedArtifact = nil
		if _, err := tx.Exec(ctx, `
			update aggregate_runs set status='blocked' where id=$1 and status<>'delivered'`, run.ID); err != nil {
			return d, err
		}
	}
	// The delivery row is written first so the audit chain can reference it.
	if _, err := tx.Exec(ctx, `
		insert into deliveries (id, run_id, approval_id, status, channel, checks, blocked_reason,
			artifact, artifact_sha256, requested_by, created_at, sent_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		d.ID, d.RunID, d.ApprovalID, d.Status, d.Channel, jsonBytes(d.Checks), nullStr(d.BlockedReason),
		storedArtifact, nullStr(d.ArtifactSHA), d.RequestedBy, d.CreatedAt, d.SentAt); err != nil {
		return d, err
	}
	if err := tx.Commit(ctx); err != nil {
		return d, err
	}
	decision := "allow"
	action := "delivery.sent"
	if !allPassed {
		decision = "deny"
		action = "delivery.blocked"
	}
	auditSeq, err := s.AppendAudit(ctx, model.AuditEvent{
		Actor: actor, Role: role, Action: action, SubjectType: "delivery", SubjectID: d.ID,
		Decision: decision,
		Detail: map[string]any{
			"run_id": run.ID, "approval_id": approval.ID, "approval_version": approval.Version,
			"approval_status": approval.Status, "channel": d.Channel,
			"included_regions": run.Coverage.IncludedRegions,
			"excluded_regions": run.Coverage.ExcludedRegions,
			"data_cutoff":      run.DataCutoff, "checks": d.Checks,
			"artifact_sha256": d.ArtifactSHA, "blocked_reason": d.BlockedReason,
		},
	})
	if err != nil {
		return d, err
	}
	d.AuditSeq = auditSeq
	_, _ = s.db.Control().Exec(ctx, `update deliveries set audit_seq=$2 where id=$1`, d.ID, auditSeq)
	return d, nil
}

type artifactRegion struct {
	RegionCode     string        `json:"region_code"`
	Included       bool          `json:"included"`
	Reason         string        `json:"exclusion_reason,omitempty"`
	DataCutoff     time.Time     `json:"data_cutoff"`
	LagSeconds     int64         `json:"lag_seconds"`
	ReplicationSeq int64         `json:"replication_seq"`
	Metrics        model.Metrics `json:"metrics"`
}

type artifactDoc struct {
	Schema            string           `json:"schema"`
	RunID             string           `json:"run_id"`
	ApprovalID        string           `json:"approval_id"`
	ApprovalVersion   int              `json:"approval_version"`
	Purpose           string           `json:"purpose"`
	Recipient         string           `json:"recipient"`
	PeriodFrom        time.Time        `json:"period_from"`
	PeriodTo          time.Time        `json:"period_to"`
	MetricKeys        []string         `json:"metric_keys"`
	IncludedRegions   []string         `json:"included_regions"`
	ExcludedRegions   []string         `json:"excluded_regions"`
	DelayedRegions    []string         `json:"delayed_regions"`
	DataCutoff        time.Time        `json:"data_cutoff"`
	GeneratedAt       time.Time        `json:"generated_at"`
	Regions           []artifactRegion `json:"regions"`
	Totals            model.Metrics    `json:"totals"`
	ContainsRawPoints bool             `json:"contains_raw_trajectories"`
	Notes             []string         `json:"notes"`
}

// buildArtifact assembles the outbound document strictly from the aggregate
// run plus the *current* approval. Restricted regions contribute coverage
// metadata only; their raw points never appear.
func (s *Service) buildArtifact(run model.AggregateRun, approval model.OutboundRequest, regions, metrics []string, now time.Time) artifactDoc {
	doc := artifactDoc{
		Schema: "agrisync.outbound_aggregate/v1", RunID: run.ID, ApprovalID: approval.ID,
		ApprovalVersion: approval.Version, Purpose: approval.Purpose, Recipient: approval.Recipient,
		PeriodFrom: run.PeriodFrom, PeriodTo: run.PeriodTo, MetricKeys: metrics,
		IncludedRegions: []string{}, ExcludedRegions: []string{}, DelayedRegions: []string{},
		DataCutoff: run.DataCutoff, GeneratedAt: now, Regions: []artifactRegion{},
		ContainsRawPoints: false,
		Notes: []string{
			"Aggregates only: no GPS coordinates or per-segment raw trajectories are included.",
			"Regions listed in excluded_regions were not confirmed at delivery time and are omitted from totals.",
		},
	}
	total := model.Metrics{}
	for _, rm := range run.Coverage.Regions {
		// regions is the intersection of what the run aggregated with the
		// approval as it stands right now; anything outside it is dropped
		// rather than exported under an authorization that no longer covers it.
		if !containsString(regions, rm.RegionCode) {
			continue
		}
		ar := artifactRegion{
			RegionCode: rm.RegionCode, Included: rm.Included, Reason: rm.Reason,
			DataCutoff: rm.DataCutoff, LagSeconds: rm.LagSeconds, ReplicationSeq: rm.ReplicationSeq,
			Metrics: projectMetrics(rm.Metrics, metrics),
		}
		doc.Regions = append(doc.Regions, ar)
		if rm.Included {
			doc.IncludedRegions = append(doc.IncludedRegions, rm.RegionCode)
			total.OperationCount += rm.Metrics.OperationCount
			total.CompletedCount += rm.Metrics.CompletedCount
			total.AreaHa += rm.Metrics.AreaHa
			total.FuelL += rm.Metrics.FuelL
			total.DurationHours += rm.Metrics.DurationHours
		} else {
			doc.ExcludedRegions = append(doc.ExcludedRegions, rm.RegionCode)
			doc.DelayedRegions = append(doc.DelayedRegions, rm.RegionCode)
		}
	}
	if total.OperationCount > 0 {
		total.CompletionRate = round2(float64(total.CompletedCount) / float64(total.OperationCount) * 100)
	}
	total.AreaHa = round2(total.AreaHa)
	total.FuelL = round2(total.FuelL)
	total.DurationHours = round2(total.DurationHours)
	doc.Totals = projectMetrics(total, metrics)
	return doc
}

// projectMetrics zeroes out metrics that are not in the approved scope, so a
// narrowed approval cannot leak extra fields.
func projectMetrics(m model.Metrics, keys []string) model.Metrics {
	out := model.Metrics{}
	for _, k := range keys {
		switch k {
		case "operation_count":
			out.OperationCount = m.OperationCount
		case "completed_count":
			out.CompletedCount = m.CompletedCount
		case "completion_rate":
			out.CompletionRate = m.CompletionRate
		case "area_ha":
			out.AreaHa = m.AreaHa
		case "fuel_l":
			out.FuelL = m.FuelL
		case "duration_hours":
			out.DurationHours = m.DurationHours
		}
	}
	return out
}

func intersect(a, b []string) []string {
	out := []string{}
	for _, v := range a {
		if containsString(b, v) {
			out = append(out, v)
		}
	}
	return out
}

func (s *Service) listDeliveries(ctx context.Context, runID string) ([]model.Delivery, error) {
	rows, err := s.db.Control().Query(ctx, `
		select id, run_id, approval_id, status, channel, checks, coalesce(blocked_reason,''),
		       artifact, coalesce(artifact_sha256,''), coalesce(audit_seq,0), requested_by,
		       created_at, sent_at
		from deliveries where run_id=$1 order by created_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Delivery{}
	for rows.Next() {
		var d model.Delivery
		var checks, artifact []byte
		if err := rows.Scan(&d.ID, &d.RunID, &d.ApprovalID, &d.Status, &d.Channel, &checks,
			&d.BlockedReason, &artifact, &d.ArtifactSHA, &d.AuditSeq, &d.RequestedBy,
			&d.CreatedAt, &d.SentAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(checks, &d.Checks)
		_ = json.Unmarshal(artifact, &d.Artifact)
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Service) GetDelivery(ctx context.Context, id string) (model.Delivery, error) {
	var d model.Delivery
	var checks, artifact []byte
	err := s.db.Control().QueryRow(ctx, `
		select id, run_id, approval_id, status, channel, checks, coalesce(blocked_reason,''),
		       artifact, coalesce(artifact_sha256,''), coalesce(audit_seq,0), requested_by,
		       created_at, sent_at
		from deliveries where id=$1`, id).
		Scan(&d.ID, &d.RunID, &d.ApprovalID, &d.Status, &d.Channel, &checks, &d.BlockedReason,
			&artifact, &d.ArtifactSHA, &d.AuditSeq, &d.RequestedBy, &d.CreatedAt, &d.SentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, fmt.Errorf("%w: delivery %s", ErrNotFound, id)
	}
	if err != nil {
		return d, err
	}
	_ = json.Unmarshal(checks, &d.Checks)
	_ = json.Unmarshal(artifact, &d.Artifact)
	return d, nil
}

// restrictedRegions returns the regions whose residency policy forbids raw
// trajectory egress. It is read fresh on every delivery.
func (s *Service) restrictedRegions(ctx context.Context) ([]string, error) {
	rows, err := s.db.Control().Query(ctx, `
		select region_code from residency_policies
		where category='raw_trajectory' and outbound_allowed=false order by region_code`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// restrictedFieldNames are the JSON keys that would indicate raw location
// data. Their presence in an outbound artifact fails delivery.
var restrictedFieldNames = map[string]bool{
	"lat": true, "lon": true, "latitude": true, "longitude": true,
	"points": true, "trajectory": true, "trajectories": true,
	"geo": true, "coordinates": true, "coords": true,
}

// scanForRestrictedFields walks the encoded artifact and returns the JSON
// paths of any restricted field.
func scanForRestrictedFields(v any, path string) []string {
	found := []string{}
	switch node := v.(type) {
	case map[string]any:
		for k, child := range node {
			p := path + "/" + k
			if restrictedFieldNames[strings.ToLower(k)] {
				found = append(found, p)
			}
			found = append(found, scanForRestrictedFields(child, p)...)
		}
	case []any:
		for i, child := range node {
			found = append(found, scanForRestrictedFields(child, fmt.Sprintf("%s[%d]", path, i))...)
		}
	}
	return found
}

// ArtifactVerification re-checks a stored delivery: the stored artifact must
// hash to the recorded SHA-256, contain no restricted fields, and the audit
// ledger row that recorded the delivery must still validate.
type ArtifactVerification struct {
	DeliveryID     string    `json:"delivery_id"`
	Status         string    `json:"status"`
	RecomputedSHA  string    `json:"recomputed_sha256,omitempty"`
	RecordedSHA    string    `json:"recorded_sha256,omitempty"`
	SHA256Matches  bool      `json:"sha256_matches"`
	RestrictedHits []string  `json:"restricted_fields,omitempty"`
	AuditSeq       int64     `json:"audit_seq,omitempty"`
	AuditValid     bool      `json:"audit_entry_valid"`
	AuditVerified  bool      `json:"audit_chain_valid"`
	ChainHead      string    `json:"audit_chain_head,omitempty"`
	VerifiedAt     time.Time `json:"verified_at"`
	ArtifactSize   int       `json:"artifact_bytes"`
}

func (s *Service) VerifyArtifact(ctx context.Context, deliveryID string) (ArtifactVerification, error) {
	d, err := s.GetDelivery(ctx, deliveryID)
	if err != nil {
		return ArtifactVerification{}, err
	}
	out := ArtifactVerification{DeliveryID: d.ID, Status: d.Status, RecordedSHA: d.ArtifactSHA,
		AuditSeq: d.AuditSeq, VerifiedAt: s.now().UTC()}
	var raw []byte
	if err := s.db.Control().QueryRow(ctx, `select artifact from deliveries where id=$1`, deliveryID).Scan(&raw); err != nil {
		return out, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		out.SHA256Matches = d.Status != "sent"
		out.ArtifactSize = len(raw)
	} else {
		out.ArtifactSize = len(raw)
		out.RecomputedSHA = sha256Hex(raw)
		out.SHA256Matches = out.RecomputedSHA == d.ArtifactSHA
		var generic any
		if err := json.Unmarshal(raw, &generic); err != nil {
			return out, err
		}
		out.RestrictedHits = scanForRestrictedFields(generic, "")
		if hits := scanForRestrictedFields(generic, ""); len(hits) > 0 {
			out.SHA256Matches = false
		}
	}
	// The audit entry for this delivery must exist, be a delivery.* action,
	// and the whole ledger must still verify.
	events, err := s.AuditFor(ctx, d.ID)
	if err != nil {
		return out, err
	}
	for _, e := range events {
		if e.SubjectID != d.ID {
			continue
		}
		if e.Action != "delivery.sent" && e.Action != "delivery.blocked" {
			continue
		}
		if v, ok := e.Detail["artifact_sha256"].(string); ok && v == d.ArtifactSHA {
			out.AuditValid = true
		}
	}
	chain, err := s.VerifyAudit(ctx)
	if err != nil {
		return out, err
	}
	out.AuditVerified = chain.Valid
	out.ChainHead = chain.Head
	return out, nil
}
