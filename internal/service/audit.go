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

var (
	ErrNotFound  = errors.New("not found")
	ErrConflict  = errors.New("conflict")
	ErrInvalid   = errors.New("invalid request")
	ErrForbidden = errors.New("forbidden")
)

// AppendAudit records a tamper-evident entry. Each hash covers the previous
// hash, so any rewritten row breaks the chain and is detected by VerifyAudit.
func (s *Service) AppendAudit(ctx context.Context, e model.AuditEvent) (int64, error) {
	if e.OccurredAt.IsZero() {
		e.OccurredAt = s.now().UTC()
	}
	// timestamptz stores microseconds and jsonb re-renders numbers and nested
	// values. Hash the canonical forms of both, so re-reading a row and
	// re-hashing it reproduces the same digest.
	e.OccurredAt = e.OccurredAt.UTC().Truncate(time.Microsecond)
	detail := e.Detail
	if detail == nil {
		detail = map[string]any{}
	}
	detail, err := canonicalDetail(detail)
	if err != nil {
		return 0, err
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.Control().Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// Serialise writers so prev_hash always belongs to the immediately
	// preceding committed entry.
	if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock(87232001)`); err != nil {
		return 0, err
	}
	var prevHash string
	var prevSeq int64
	err = tx.QueryRow(ctx, `select seq, hash from audit_events order by seq desc limit 1`).Scan(&prevSeq, &prevHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		prevHash = strings.Repeat("0", 64)
	}
	payload := auditPayload{
		Seq: prevSeq + 1, OccurredAt: e.OccurredAt, Actor: e.Actor, Role: e.Role,
		Action: e.Action, SubjectType: e.SubjectType, SubjectID: e.SubjectID,
		RegionCode: e.RegionCode, Decision: e.Decision, Detail: detail, PrevHash: prevHash,
	}
	hash := sha256Hex(jsonBytes(payload))
	var seq int64
	err = tx.QueryRow(ctx, `
		insert into audit_events (occurred_at, actor, role, action, subject_type, subject_id,
			region_code, decision, detail, prev_hash, hash)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) returning seq`,
		payload.OccurredAt, e.Actor, e.Role, e.Action, e.SubjectType, e.SubjectID,
		e.RegionCode, e.Decision, detailJSON, prevHash, hash).Scan(&seq)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return seq, nil
}

// canonicalDetail round-trips a detail map through JSON so that the value
// hashed at write time is identical to the value decoded from jsonb later.
func canonicalDetail(d map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(d)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

// auditPayload is the canonical object that gets hashed. Field order is the
// struct order, so the same row always re-hashes identically.
type auditPayload struct {
	Seq         int64          `json:"seq"`
	OccurredAt  time.Time      `json:"occurred_at"`
	Actor       string         `json:"actor"`
	Role        string         `json:"role"`
	Action      string         `json:"action"`
	SubjectType string         `json:"subject_type"`
	SubjectID   string         `json:"subject_id"`
	RegionCode  string         `json:"region_code"`
	Decision    string         `json:"decision"`
	Detail      map[string]any `json:"detail"`
	PrevHash    string         `json:"prev_hash"`
}

type AuditVerification struct {
	Valid      bool      `json:"valid"`
	Checked    int       `json:"checked"`
	BrokenAt   int64     `json:"broken_at,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Head       string    `json:"head_hash"`
	HeadSeq    int64     `json:"head_seq"`
	VerifiedAt time.Time `json:"verified_at"`
}

// VerifyAudit re-derives every hash and reports the first row that does not
// match the chain.
func (s *Service) VerifyAudit(ctx context.Context) (AuditVerification, error) {
	rows, err := s.db.Control().Query(ctx, `
		select seq, occurred_at, actor, role, action, subject_type, subject_id, region_code,
		       decision, detail, prev_hash, hash
		from audit_events order by seq`)
	if err != nil {
		return AuditVerification{}, err
	}
	defer rows.Close()
	out := AuditVerification{VerifiedAt: s.now().UTC()}
	prev := strings.Repeat("0", 64)
	for rows.Next() {
		var e model.AuditEvent
		var detail []byte
		if err := rows.Scan(&e.Seq, &e.OccurredAt, &e.Actor, &e.Role, &e.Action, &e.SubjectType,
			&e.SubjectID, &e.RegionCode, &e.Decision, &detail, &e.PrevHash, &e.Hash); err != nil {
			return AuditVerification{}, err
		}
		if err := json.Unmarshal(detail, &e.Detail); err != nil {
			return AuditVerification{}, err
		}
		payload := auditPayload{
			Seq: e.Seq, OccurredAt: e.OccurredAt.UTC().Truncate(time.Microsecond), Actor: e.Actor, Role: e.Role,
			Action: e.Action, SubjectType: e.SubjectType, SubjectID: e.SubjectID,
			RegionCode: e.RegionCode, Decision: e.Decision, Detail: e.Detail, PrevHash: e.PrevHash,
		}
		expect := sha256Hex(jsonBytes(payload))
		out.Checked++
		if e.PrevHash != prev {
			out.BrokenAt = e.Seq
			out.Reason = "prev_hash does not match previous entry"
			return out, nil
		}
		if expect != e.Hash {
			out.BrokenAt = e.Seq
			out.Reason = "hash mismatch: entry content was modified"
			return out, nil
		}
		prev = e.Hash
		out.Head = e.Hash
		out.HeadSeq = e.Seq
	}
	if err := rows.Err(); err != nil {
		return AuditVerification{}, err
	}
	out.Valid = true
	return out, nil
}

func (s *Service) ListAudit(ctx context.Context, limit int) ([]model.AuditEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.Control().Query(ctx, `
		select seq, occurred_at, actor, role, action, subject_type, subject_id, region_code,
		       decision, detail, prev_hash, hash
		from audit_events order by seq desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.AuditEvent{}
	for rows.Next() {
		var e model.AuditEvent
		var detail []byte
		if err := rows.Scan(&e.Seq, &e.OccurredAt, &e.Actor, &e.Role, &e.Action, &e.SubjectType,
			&e.SubjectID, &e.RegionCode, &e.Decision, &detail, &e.PrevHash, &e.Hash); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(detail, &e.Detail)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Service) AuditFor(ctx context.Context, subjectID string) ([]model.AuditEvent, error) {
	rows, err := s.db.Control().Query(ctx, `
		select seq, occurred_at, actor, role, action, subject_type, subject_id, region_code,
		       decision, detail, prev_hash, hash
		from audit_events where subject_id=$1 order by seq`, subjectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.AuditEvent{}
	for rows.Next() {
		var e model.AuditEvent
		var detail []byte
		if err := rows.Scan(&e.Seq, &e.OccurredAt, &e.Actor, &e.Role, &e.Action, &e.SubjectType,
			&e.SubjectID, &e.RegionCode, &e.Decision, &detail, &e.PrevHash, &e.Hash); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(detail, &e.Detail)
		out = append(out, e)
	}
	return out, rows.Err()
}

func validateRegionCode(code string) error {
	if !strings.Contains(code, "-") {
		return fmt.Errorf("%w: region code must look like DE-BB", ErrInvalid)
	}
	return nil
}
