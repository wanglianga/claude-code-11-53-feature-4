package service

import (
	"testing"
	"time"

	"agrisync/internal/model"
)

func seqTestTime(day int, hour int) time.Time {
	return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC)
}

func seqTestFact(id, op string, eventAt, receivedAt time.Time, field string) engineFact {
	return engineFact{SequenceFact: model.SequenceFact{
		FactID: id, DeviceSerial: "dev-" + id, SegmentID: "seg-" + id,
		OperationType: op, FieldID: field, EventAt: eventAt, ReceivedAt: receivedAt,
		Completed: true,
	}}
}

func seqTestSeason(id, field string) model.FieldSeason {
	return model.FieldSeason{
		SeasonID: id, FieldID: field, CropCode: "maize",
		PlannedFrom: seqTestTime(1, 0), PlannedTo: seqTestTime(30, 0),
	}
}

func interpretations(events []model.SequenceReviewEvent) map[string]string {
	out := map[string]string{}
	for _, e := range events {
		out[e.FactID] = e.Interpretation
	}
	return out
}

// Reseeding is an allowed alternative; a second fertilizer batch is an
// allowed repeat. The linear FSM keeps advancing through the same stages.
func TestFSMReseedingAndTwoFertilizerBatches(t *testing.T) {
	season := seqTestSeason("s1", "F1")
	facts := []engineFact{
		seqTestFact("f1", "seeding", seqTestTime(2, 8), seqTestTime(2, 9), "F1"),
		seqTestFact("f2", "seeding", seqTestTime(5, 8), seqTestTime(5, 9), "F1"),
		seqTestFact("f3", "fertilizing", seqTestTime(8, 8), seqTestTime(8, 9), "F1"),
		seqTestFact("f4", "fertilizing", seqTestTime(15, 8), seqTestTime(15, 9), "F1"),
		seqTestFact("f5", "harvesting", seqTestTime(22, 8), seqTestTime(22, 9), "F1"),
	}
	r := runFSM(season.SeasonID, season, facts, DefaultFSMRules())
	if r.status != "ok" {
		t.Fatalf("expected ok, got %s: %+v", r.status, r.issues)
	}
	got := interpretations(r.events)
	want := map[string]string{
		"f1": "confirmed", "f2": "alternative", "f3": "confirmed",
		"f4": "duplicate", "f5": "confirmed",
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("fact %s: got %s want %s", id, got[id], w)
		}
	}
	if r.state != "harvested" {
		t.Fatalf("final state = %s, want harvested", r.state)
	}
	if len(r.confirmedStages) != 3 {
		t.Fatalf("confirmed stages = %v, want all three", r.confirmedStages)
	}
	if len(r.missing) != 0 {
		t.Fatalf("fertilizing is optional/repeated, no gaps expected: %v", r.missing)
	}
}

// Harvest earlier than the provable seeding is an order conflict in both
// engines; the later seeding still gets confirmed and never erases it.
func TestHarvestBeforeProvableSeedingConflicts(t *testing.T) {
	season := seqTestSeason("s2", "F2")
	facts := []engineFact{
		seqTestFact("h1", "harvesting", seqTestTime(3, 8), seqTestTime(3, 9), "F2"),
		seqTestFact("s1", "seeding", seqTestTime(8, 8), seqTestTime(8, 9), "F2"),
	}
	fsm := runFSM(season.SeasonID, season, facts, DefaultFSMRules())
	if fsm.status != "conflict" {
		t.Fatalf("fsm status = %s, want conflict", fsm.status)
	}
	if got := interpretations(fsm.events)["h1"]; got != "conflict" {
		t.Fatalf("fsm harvest interpretation = %s, want conflict", got)
	}
	if got := interpretations(fsm.events)["s1"]; got != "confirmed" {
		t.Fatalf("later seeding must stay confirmed, got %s", got)
	}

	po := runPartialOrder(season.SeasonID, season, facts, DefaultPORules())
	if po.status != "conflict" {
		t.Fatalf("po status = %s, want conflict", po.status)
	}
	if got := interpretations(po.events)["h1"]; got != "conflict" {
		t.Fatalf("po harvest interpretation = %s, want conflict", got)
	}
	found := false
	for _, is := range po.issues {
		if is.Code == "order_conflict" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected order_conflict issue, got %+v", po.issues)
	}
}

// An offline backfill arrives after a later event but happened earlier.
// Event-time ordering still produces a valid sequence; arrival order never
// turns it into a reverse-order conflict, and the late fact is flagged.
func TestOfflineLateArrivalUsesEventTime(t *testing.T) {
	season := seqTestSeason("s3", "F3")
	// Fertilizer arrives first; the seeding happened earlier but is backfilled.
	facts := []engineFact{
		seqTestFact("fert1", "fertilizing", seqTestTime(6, 8), seqTestTime(6, 10), "F3"),
		seqTestFact("seed1", "seeding", seqTestTime(2, 8), seqTestTime(10, 12), "F3"),
		seqTestFact("harv1", "harvesting", seqTestTime(20, 8), seqTestTime(20, 12), "F3"),
	}
	markLateArrivals(facts)
	if !facts[1].Late || facts[0].Late {
		t.Fatalf("late flag misplaced: %+v", facts)
	}
	r := runFSM(season.SeasonID, season, facts, DefaultFSMRules())
	if r.status != "ok" {
		t.Fatalf("event-time order should be valid, got %s: %+v", r.status, r.issues)
	}
	got := interpretations(r.events)
	if got["seed1"] != "confirmed" || got["fert1"] != "confirmed" {
		t.Fatalf("unexpected interpretations: %v", got)
	}
	// Events are emitted in event-time order, so the backfilled seed comes first.
	if r.events[0].FactID != "seed1" {
		t.Fatalf("events must be ordered by event time, got %s first", r.events[0].FactID)
	}
	if r.events[0].Detail["late_arrival"] != true {
		t.Fatal("backfilled fact should carry late_arrival detail")
	}
}

// The partial-order engine holds the fertilizer prerequisite open while the
// seeding is missing, then reports the gap explicitly.
func TestPartialOrderMissingPrerequisite(t *testing.T) {
	season := seqTestSeason("s4", "F4")
	facts := []engineFact{
		seqTestFact("fert1", "fertilizing", seqTestTime(6, 8), seqTestTime(6, 10), "F4"),
	}
	po := runPartialOrder(season.SeasonID, season, facts, DefaultPORules())
	if po.status != "conflict" {
		t.Fatalf("status = %s, want conflict", po.status)
	}
	var codes []string
	for _, is := range po.issues {
		codes = append(codes, is.Code)
	}
	if !containsString(codes, "missing_prerequisite_evidence") {
		t.Fatalf("expected missing_prerequisite_evidence, got %v", codes)
	}
	// Harvesting is required by the rules and has no evidence at all.
	var missingStage string
	for _, m := range po.missing {
		if m["stage"] == "harvested" {
			missingStage = "harvested"
		}
	}
	if missingStage == "" {
		t.Fatalf("harvested stage should be a missing prerequisite: %v", po.missing)
	}
}

// Optional fertilizer means a seed->harvest season is complete, not gappy.
func TestFSMOptionalFertilizerIsNotAGap(t *testing.T) {
	season := seqTestSeason("s5", "F5")
	facts := []engineFact{
		seqTestFact("s1", "seeding", seqTestTime(2, 8), seqTestTime(2, 9), "F5"),
		seqTestFact("h1", "harvesting", seqTestTime(20, 8), seqTestTime(20, 9), "F5"),
	}
	r := runFSM(season.SeasonID, season, facts, DefaultFSMRules())
	if r.status != "ok" {
		t.Fatalf("status = %s, want ok", r.status)
	}
	for _, m := range r.missing {
		if m["stage"] == "fertilized" {
			t.Fatal("optional fertilizing must not be reported missing")
		}
	}
}

// Unverified operation semantics keep the season pending, never guessed.
func TestUnknownSemanticsPending(t *testing.T) {
	season := seqTestSeason("s6", "F6")
	facts := []engineFact{
		seqTestFact("o1", "other", seqTestTime(2, 8), seqTestTime(2, 9), "F6"),
	}
	fsm := runFSM(season.SeasonID, season, facts, DefaultFSMRules())
	if fsm.status != "pending_review" {
		t.Fatalf("fsm status = %s, want pending_review", fsm.status)
	}
	po := runPartialOrder(season.SeasonID, season, facts, DefaultPORules())
	if po.status != "pending_review" {
		t.Fatalf("po status = %s, want pending_review", po.status)
	}
}

// Allowed paths are enumerated from the current FSM state, including the
// declared alternative and repeat moves.
func TestFSMAllowedPathsExplainAlternatives(t *testing.T) {
	doc := DefaultFSMRules()
	paths := fsmAllowedPaths(doc, "seeded")
	kinds := map[string]bool{}
	for _, p := range paths {
		kinds[p["kind"].(string)] = true
	}
	for _, want := range []string{"normal", "alternative"} {
		if !kinds[want] {
			t.Fatalf("from seeded, allowed paths missing kind %s: %v", want, paths)
		}
	}
	// Repeat fertilizer batches are legal once fertilized.
	fertPaths := fsmAllowedPaths(doc, "fertilized")
	hasRepeat := false
	for _, p := range fertPaths {
		if p["kind"] == "repeat" && p["operation"] == "fertilizing" {
			hasRepeat = true
		}
	}
	if !hasRepeat {
		t.Fatalf("fertilized state should allow repeat fertilizing: %v", fertPaths)
	}
}

// The partial-order engine returns several feasible stage orderings because
// fertilizer placement is optional relative to the harvest path.
func TestPOAllowedPathsAreMultiple(t *testing.T) {
	paths := poAllowedPaths(DefaultPORules())
	stagePaths := 0
	for _, p := range paths {
		if p["engine"] == ruleEnginePO && p["stage_path"] != nil {
			stagePaths++
		}
	}
	if stagePaths < 2 {
		t.Fatalf("expected multiple feasible stage paths, got %d: %v", stagePaths, paths)
	}
}

// Exceeding the declared operation count is a conflict, not a silent repeat.
func TestPOOperationCountLimit(t *testing.T) {
	season := seqTestSeason("s7", "F7")
	facts := []engineFact{
		seqTestFact("s1", "seeding", seqTestTime(1, 8), seqTestTime(1, 9), "F7"),
		seqTestFact("s2", "seeding", seqTestTime(3, 8), seqTestTime(3, 9), "F7"),
		seqTestFact("s3", "seeding", seqTestTime(6, 8), seqTestTime(6, 9), "F7"),
	}
	po := runPartialOrder(season.SeasonID, season, facts, DefaultPORules())
	if po.status != "conflict" {
		t.Fatalf("status = %s, want conflict", po.status)
	}
	found := false
	for _, is := range po.issues {
		if is.Code == "operation_count_exceeded" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected operation_count_exceeded, got %+v", po.issues)
	}
}
