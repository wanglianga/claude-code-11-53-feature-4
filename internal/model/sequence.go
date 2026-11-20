package model

import "time"

// SequenceFact is a standardised operation fact used by the field operation
// sequence integrity product. event_at is when the work happened; received_at
// is when the region learned about it, so offline backfill is ordered by
// event time rather than arrival order.
type SequenceFact struct {
	FactID        string         `json:"fact_id"`
	Source        string         `json:"source"`
	DeviceSerial  string         `json:"device_serial"`
	SegmentID     string         `json:"segment_id"`
	OperationType string         `json:"operation_type"`
	FieldID       string         `json:"field_id"`
	EventAt       time.Time      `json:"event_at"`
	ReceivedAt    time.Time      `json:"received_at"`
	Completed     bool           `json:"completed"`
	BatchID       string         `json:"batch_id"`
	Detail        map[string]any `json:"detail,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
}

// FieldSeason is one review unit: a field, a crop, a season window.
type FieldSeason struct {
	SeasonID    string    `json:"season_id"`
	FieldID     string    `json:"field_id"`
	CropCode    string    `json:"crop_code"`
	PlannedFrom time.Time `json:"planned_from"`
	PlannedTo   time.Time `json:"planned_to"`
	Status      string    `json:"status"`
	Note        string    `json:"note"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// BoundaryRevision is a time-versioned field boundary. Overlapping windows
// on the same field are an attribution conflict.
type BoundaryRevision struct {
	RevisionID    string         `json:"revision_id"`
	FieldID       string         `json:"field_id"`
	ValidFrom     time.Time      `json:"valid_from"`
	ValidUntil    *time.Time     `json:"valid_until,omitempty"`
	BoundaryLabel string         `json:"boundary_label"`
	Geometry      map[string]any `json:"geometry,omitempty"`
	SupersededBy  string         `json:"superseded_by,omitempty"`
	Note          string         `json:"note"`
	CreatedAt     time.Time      `json:"created_at"`
}

// CropCalendar is a time-versioned crop declaration on a field season. Two
// crops valid at the same instant on one field are mutually exclusive.
type CropCalendar struct {
	CalendarID string     `json:"calendar_id"`
	FieldID    string     `json:"field_id"`
	SeasonID   string     `json:"season_id"`
	CropCode   string     `json:"crop_code"`
	ValidFrom  time.Time  `json:"valid_from"`
	ValidUntil *time.Time `json:"valid_until,omitempty"`
	Note       string     `json:"note"`
	CreatedAt  time.Time  `json:"created_at"`
}

// RuleSet is a versioned agronomy rule definition for one engine.
type RuleSet struct {
	RuleSetID string         `json:"rule_set_id"`
	Version   string         `json:"version"`
	Engine    string         `json:"engine"`
	CropCode  string         `json:"crop_code"`
	RuleDoc   map[string]any `json:"rule_doc"`
	Status    string         `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
}

// SequenceReviewRun freezes one version combination of facts, attribution
// and both rule engines. Corrections produce a new run; old runs stay
// replayable because the inputs that produced them are never rewritten.
type SequenceReviewRun struct {
	RunID              string         `json:"run_id"`
	RegionCode         string         `json:"region_code"`
	Status             string         `json:"status"`
	PeriodFrom         time.Time      `json:"period_from"`
	PeriodTo           time.Time      `json:"period_to"`
	FSMRuleSetID       string         `json:"fsm_rule_set_id"`
	PORuleSetID        string         `json:"po_rule_set_id"`
	FactVersion        string         `json:"fact_version"`
	AttributionVersion string         `json:"attribution_version"`
	VersionBundle      map[string]any `json:"version_bundle"`
	FactCutoff         time.Time      `json:"fact_cutoff"`
	Note               string         `json:"note"`
	CreatedBy          string         `json:"created_by"`
	CreatedAt          time.Time      `json:"created_at"`
}

// SequenceReviewField is one field-season result inside a run.
type SequenceReviewField struct {
	RunID                string           `json:"run_id"`
	SeasonID             string           `json:"season_id"`
	FieldID              string           `json:"field_id"`
	CropCode             string           `json:"crop_code"`
	AttributionStatus    string           `json:"attribution_status"`
	RuleApplicability    string           `json:"rule_applicability"`
	FSMState             string           `json:"fsm_state"`
	FSMStatus            string           `json:"fsm_status"`
	POStatus             string           `json:"po_status"`
	FSMConfirmedStages   []string         `json:"fsm_confirmed_stages"`
	POConfirmedStages    []string         `json:"po_confirmed_stages"`
	MissingPrerequisites []map[string]any `json:"missing_prerequisites"`
	AllowedPaths         []map[string]any `json:"allowed_paths"`
	PendingRanges        []map[string]any `json:"pending_ranges"`
	Summary              map[string]any   `json:"summary"`
}

// SequenceReviewEvent is one fact's interpretation under one engine.
type SequenceReviewEvent struct {
	RunID          string         `json:"run_id"`
	FactID         string         `json:"fact_id"`
	SeasonID       string         `json:"season_id"`
	Engine         string         `json:"engine"`
	OperationType  string         `json:"operation_type"`
	EventAt        time.Time      `json:"event_at"`
	ReceivedAt     time.Time      `json:"received_at"`
	Interpretation string         `json:"interpretation"`
	Detail         map[string]any `json:"detail,omitempty"`
}

// SequenceReviewIssue is a conflict or pending-review scope with evidence.
type SequenceReviewIssue struct {
	ID       int64          `json:"id"`
	RunID    string         `json:"run_id"`
	SeasonID string         `json:"season_id"`
	Engine   string         `json:"engine"`
	Severity string         `json:"severity"`
	Code     string         `json:"code"`
	Message  string         `json:"message"`
	Evidence map[string]any `json:"evidence"`
}

// SequenceReviewEngineRollup is an engine-level summary inside a run.
type SequenceReviewEngineRollup struct {
	RunID                string         `json:"run_id"`
	Engine               string         `json:"engine"`
	Status               string         `json:"status"`
	FieldCount           int            `json:"field_count"`
	ConflictCount        int            `json:"conflict_count"`
	PendingCount         int            `json:"pending_count"`
	ConfirmedStageCounts map[string]int `json:"confirmed_stage_counts"`
}

// SequenceReviewDetail is the full in-region view of one run.
type SequenceReviewDetail struct {
	Run     SequenceReviewRun            `json:"run"`
	Engines []SequenceReviewEngineRollup `json:"engines"`
	Fields  []SequenceReviewField        `json:"fields"`
	Events  []SequenceReviewEvent        `json:"events"`
	Issues  []SequenceReviewIssue        `json:"issues"`
}

// StageCoveragePublication is the only sequence product allowed to leave a
// region: per-region stage counts, no field ids.
type StageCoveragePublication struct {
	ID               string         `json:"id"`
	SourceRegion     string         `json:"source_region"`
	RunID            string         `json:"run_id"`
	VersionBundle    map[string]any `json:"version_bundle"`
	FactCutoff       time.Time      `json:"fact_cutoff"`
	Engine           string         `json:"engine"`
	FieldSeasonCount int            `json:"field_season_count"`
	ConflictCount    int            `json:"conflict_count"`
	PendingCount     int            `json:"pending_count"`
	StageCounts      map[string]int `json:"stage_counts"`
	Status           string         `json:"status"`
	CopiedAt         time.Time      `json:"copied_at"`
}

// StageCoverageRollup is the group-visible cross-region rollup. Its finest
// granularity is (region, engine, stage); no field-level data exists here.
type StageCoverageRollup struct {
	ID              string                `json:"id"`
	PeriodFrom      time.Time             `json:"period_from"`
	PeriodTo        time.Time             `json:"period_to"`
	Engine          string                `json:"engine"`
	IncludedRegions []string              `json:"included_regions"`
	ExcludedRegions []string              `json:"excluded_regions"`
	VersionBundles  map[string]any        `json:"version_bundles"`
	StageTotals     map[string]int        `json:"stage_totals"`
	RegionBreakdown []StageCoverageRegion `json:"region_breakdown"`
	DataCutoff      time.Time             `json:"data_cutoff"`
	RequestedBy     string                `json:"requested_by"`
	CreatedAt       time.Time             `json:"created_at"`
}

// StageCoverageRegion is one region's line in a group rollup.
type StageCoverageRegion struct {
	RegionCode       string         `json:"region_code"`
	Included         bool           `json:"included"`
	ExclusionReason  string         `json:"exclusion_reason,omitempty"`
	RunID            string         `json:"run_id,omitempty"`
	FieldSeasonCount int            `json:"field_season_count"`
	ConflictCount    int            `json:"conflict_count"`
	PendingCount     int            `json:"pending_count"`
	StageCounts      map[string]int `json:"stage_counts"`
	VersionBundle    map[string]any `json:"version_bundle,omitempty"`
	FactCutoff       *time.Time     `json:"fact_cutoff,omitempty"`
}
