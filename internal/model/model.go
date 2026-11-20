package model

import (
	"time"
)

// GeoPoint is one sampled position of a machine. Trajectory points are the
// restricted category: they only leave a region when residency policy allows.
type GeoPoint struct {
	Lat float64   `json:"lat"`
	Lon float64   `json:"lon"`
	At  time.Time `json:"at"`
}

type Alarm struct {
	Code       string    `json:"code"`
	Severity   string    `json:"severity"`
	OccurredAt time.Time `json:"occurred_at"`
}

// OperationInput is one uploaded work segment. Deduplication key is
// (device_serial, segment_id) scoped to the receiving region.
type OperationInput struct {
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
	Points        []GeoPoint `json:"points"`
	Alarms        []Alarm    `json:"alarms"`
}

type IngestBatch struct {
	BatchID    string           `json:"batch_id"`
	Operations []OperationInput `json:"operations"`
}

type IngestResult struct {
	Region      string   `json:"region"`
	BatchID     string   `json:"batch_id"`
	Accepted    int      `json:"accepted"`
	Duplicates  int      `json:"duplicates"`
	Rejected    int      `json:"rejected"`
	Errors      []string `json:"errors,omitempty"`
	FirstSeq    int64    `json:"first_journal_seq"`
	LastSeq     int64    `json:"last_journal_seq"`
	ActiveNode  string   `json:"active_node"`
	LocalCommit string   `json:"local_commit"`
	Replayed    bool     `json:"replayed_batch"`
}

// OperationRecord is returned by region-side queries and never leaves the
// region through the group API.
type OperationRecord struct {
	ID            string     `json:"id"`
	JournalSeq    int64      `json:"journal_seq"`
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
	PointCount    int        `json:"point_count"`
	Alarms        []Alarm    `json:"alarms"`
	SourceNode    string     `json:"source_node"`
	ReceivedAt    time.Time  `json:"received_at"`
}

type RegionMetric struct {
	RegionCode     string    `json:"region_code"`
	Included       bool      `json:"included"`
	Reason         string    `json:"exclusion_reason,omitempty"`
	ReplicationSeq int64     `json:"replication_seq"`
	SnapshotID     string    `json:"snapshot_id,omitempty"`
	DataCutoff     time.Time `json:"data_cutoff,omitempty"`
	LagSeconds     int64     `json:"lag_seconds"`
	Metrics        Metrics   `json:"metrics"`
}

type Metrics struct {
	OperationCount int     `json:"operation_count"`
	CompletedCount int     `json:"completed_count"`
	CompletionRate float64 `json:"completion_rate"`
	AreaHa         float64 `json:"area_ha"`
	FuelL          float64 `json:"fuel_l"`
	DurationHours  float64 `json:"duration_hours"`
}

type Coverage struct {
	IncludedRegions []string       `json:"included_regions"`
	ExcludedRegions []string       `json:"excluded_regions"`
	Regions         []RegionMetric `json:"regions"`
	DataCutoff      time.Time      `json:"data_cutoff"`
	GeneratedAt     time.Time      `json:"generated_at"`
	StaleRegions    []string       `json:"stale_regions"`
}

type OutboundRequest struct {
	ID           string     `json:"id"`
	Purpose      string     `json:"purpose"`
	Recipient    string     `json:"recipient"`
	ScopeRegions []string   `json:"scope_regions"`
	ScopeMetrics []string   `json:"scope_metrics"`
	Status       string     `json:"status"`
	Version      int        `json:"version"`
	ValidFrom    *time.Time `json:"valid_from,omitempty"`
	ValidUntil   *time.Time `json:"valid_until,omitempty"`
	RequestedBy  string     `json:"requested_by"`
	DecidedBy    string     `json:"decided_by,omitempty"`
	DecidedAt    *time.Time `json:"decided_at,omitempty"`
	DecisionNote string     `json:"decision_note,omitempty"`
	RevokedBy    string     `json:"revoked_by,omitempty"`
	RevokedAt    *time.Time `json:"revoked_at,omitempty"`
	RevokeReason string     `json:"revoke_reason,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type AggregateRun struct {
	ID              string     `json:"id"`
	ApprovalID      string     `json:"approval_id"`
	ApprovalVersion int        `json:"approval_version"`
	PeriodFrom      time.Time  `json:"period_from"`
	PeriodTo        time.Time  `json:"period_to"`
	MetricKeys      []string   `json:"metric_keys"`
	Status          string     `json:"status"`
	ScopeSnapshot   []string   `json:"scope_snapshot"`
	Coverage        Coverage   `json:"coverage"`
	Result          Metrics    `json:"result"`
	DataCutoff      time.Time  `json:"data_cutoff"`
	RequestedBy     string     `json:"requested_by"`
	CreatedAt       time.Time  `json:"created_at"`
	Deliveries      []Delivery `json:"deliveries,omitempty"`
}

type Delivery struct {
	ID            string     `json:"id"`
	RunID         string     `json:"run_id"`
	ApprovalID    string     `json:"approval_id"`
	Status        string     `json:"status"`
	Channel       string     `json:"channel"`
	Checks        []Check    `json:"checks"`
	BlockedReason string     `json:"blocked_reason,omitempty"`
	Artifact      any        `json:"artifact,omitempty"`
	ArtifactSHA   string     `json:"artifact_sha256,omitempty"`
	AuditSeq      int64      `json:"audit_seq,omitempty"`
	RequestedBy   string     `json:"requested_by"`
	CreatedAt     time.Time  `json:"created_at"`
	SentAt        *time.Time `json:"sent_at,omitempty"`
}

type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

type DRRegistry struct {
	RegionCode       string     `json:"region_code"`
	PrimaryNode      string     `json:"primary_node"`
	StandbyNode      string     `json:"standby_node"`
	ActiveNode       string     `json:"active_node"`
	Epoch            int64      `json:"epoch"`
	LastConfirmedSeq int64      `json:"last_confirmed_seq"`
	LastConfirmedAt  *time.Time `json:"last_confirmed_at,omitempty"`
	Status           string     `json:"status"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

type ReplicationCursor struct {
	SourceRegion  string     `json:"source_region"`
	LastSeq       int64      `json:"scanned_seq"`
	ConfirmedSeq  int64      `json:"confirmed_seq"`
	Status        string     `json:"status"`
	RetryCount    int        `json:"retry_count"`
	CopiedTotal   int64      `json:"copied_total"`
	BlockedTotal  int64      `json:"blocked_total"`
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type ReplicationRun struct {
	ID                string         `json:"id"`
	SourceRegion      string         `json:"source_region"`
	FromSeq           int64          `json:"from_seq"`
	ToSeq             int64          `json:"to_seq"`
	CopiedByCategory  map[string]int `json:"copied_by_category"`
	BlockedByCategory map[string]int `json:"blocked_by_category"`
	Status            string         `json:"status"`
	Fault             string         `json:"fault"`
	Error             string         `json:"error,omitempty"`
	// DiscardedUnconfirmed counts replica rows written by a failed attempt
	// that were rolled back to the last confirmed position.
	DiscardedUnconfirmed int       `json:"discarded_unconfirmed,omitempty"`
	StartedAt            time.Time `json:"started_at"`
	FinishedAt           time.Time `json:"finished_at"`
}

type AuditEvent struct {
	Seq         int64          `json:"seq"`
	OccurredAt  time.Time      `json:"occurred_at"`
	Actor       string         `json:"actor"`
	Role        string         `json:"role"`
	Action      string         `json:"action"`
	SubjectType string         `json:"subject_type"`
	SubjectID   string         `json:"subject_id"`
	RegionCode  string         `json:"region_code,omitempty"`
	Decision    string         `json:"decision"`
	Detail      map[string]any `json:"detail"`
	PrevHash    string         `json:"prev_hash"`
	Hash        string         `json:"hash"`
}

type FailoverResult struct {
	RegionCode       string         `json:"region_code"`
	FromNode         string         `json:"from_node"`
	ToNode           string         `json:"to_node"`
	Epoch            int64          `json:"epoch"`
	ConfirmedSeq     int64          `json:"confirmed_seq"`
	DiscardedRows    map[string]int `json:"discarded_unconfirmed_rows"`
	ReplicationReset bool           `json:"replication_reset_to_confirmed"`
	ResumedFromSeq   int64          `json:"resumed_from_seq"`
	NewPrimaryState  string         `json:"new_primary_state"`
	OccurredAt       time.Time      `json:"occurred_at"`
}

type Policy struct {
	RegionCode      string    `json:"region_code"`
	Category        string    `json:"category"`
	OutboundAllowed bool      `json:"outbound_allowed"`
	Rationale       string    `json:"rationale"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type LinkState struct {
	RegionCode string    `json:"region_code"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type APIErrorBody struct {
	Error APIError `json:"error"`
}

type APIError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}
