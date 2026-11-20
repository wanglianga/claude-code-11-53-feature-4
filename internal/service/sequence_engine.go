package service

import (
	"fmt"
	"sort"
	"time"

	"agrisync/internal/model"
)

// Both engines reason strictly on event time. Facts are sorted by
// (event_at, received_at, fact_id): an offline backfill with an early
// event_at is evaluated before a later-arriving fact, so arrival order can
// never turn normal backfill into a reverse-order conflict.

type engineFact struct {
	model.SequenceFact
	Late bool // another fact arrived earlier but happened later
}

type fieldEngineResult struct {
	engine          string
	status          string
	state           string
	confirmedStages []string
	events          []model.SequenceReviewEvent
	issues          []model.SequenceReviewIssue
	missing         []map[string]any
	allowedPaths    []map[string]any
	pendingRanges   []map[string]any
}

func newFieldEngineResult(engine string) *fieldEngineResult {
	return &fieldEngineResult{
		engine:          engine,
		status:          "ok",
		confirmedStages: []string{},
		events:          []model.SequenceReviewEvent{},
		issues:          []model.SequenceReviewIssue{},
		missing:         []map[string]any{},
		allowedPaths:    []map[string]any{},
		pendingRanges:   []map[string]any{},
	}
}

func (r *fieldEngineResult) markConflict() {
	if r.status != "conflict" {
		r.status = "conflict"
	}
}

func (r *fieldEngineResult) markPending() {
	if r.status == "ok" {
		r.status = "pending_review"
	}
}

func (r *fieldEngineResult) addIssue(seasonID, severity, code, msg string, evidence map[string]any) {
	r.issues = append(r.issues, model.SequenceReviewIssue{
		RunID: "", SeasonID: seasonID, Engine: r.engine, Severity: severity,
		Code: code, Message: msg, Evidence: evidence,
	})
	if severity == "conflict" {
		r.markConflict()
	} else {
		r.markPending()
	}
}

func (r *fieldEngineResult) confirmStage(stage string) {
	for _, s := range r.confirmedStages {
		if s == stage {
			return
		}
	}
	r.confirmedStages = append(r.confirmedStages, stage)
}

func (r *fieldEngineResult) baseEvent(f engineFact, seasonID, interpretation string, detail map[string]any) model.SequenceReviewEvent {
	if detail == nil {
		detail = map[string]any{}
	}
	if f.Late {
		detail["late_arrival"] = true
	}
	return model.SequenceReviewEvent{
		FactID: f.FactID, SeasonID: seasonID, Engine: r.engine,
		OperationType: f.OperationType, EventAt: f.EventAt.UTC(), ReceivedAt: f.ReceivedAt.UTC(),
		Interpretation: interpretation, Detail: detail,
	}
}

func sortFacts(facts []engineFact) {
	sort.SliceStable(facts, func(i, j int) bool {
		if !facts[i].EventAt.Equal(facts[j].EventAt) {
			return facts[i].EventAt.Before(facts[j].EventAt)
		}
		if !facts[i].ReceivedAt.Equal(facts[j].ReceivedAt) {
			return facts[i].ReceivedAt.Before(facts[j].ReceivedAt)
		}
		return facts[i].FactID < facts[j].FactID
	})
}

// markLateArrivals flags facts whose arrival order contradicts their event
// order. They still participate in event-time reasoning; the flag only
// documents why a "new" fact explains an older window.
func markLateArrivals(facts []engineFact) {
	for i := range facts {
		for j := range facts {
			if i == j {
				continue
			}
			if facts[j].ReceivedAt.Before(facts[i].ReceivedAt) && facts[j].EventAt.After(facts[i].EventAt) {
				facts[i].Late = true
				break
			}
		}
	}
}

// =====================================================================
// Finite-state-machine engine
// =====================================================================

func runFSM(seasonID string, season model.FieldSeason, facts []engineFact, doc FSMRuleDoc) *fieldEngineResult {
	r := newFieldEngineResult(ruleEngineFSM)
	state := doc.InitialState
	counts := map[string]int{}
	seenOps := map[string]bool{}
	order := append([]engineFact(nil), facts...)
	sortFacts(order)

	for _, f := range order {
		seenOps[f.OperationType] = true
		if f.OperationType == "other" {
			r.events = append(r.events, r.baseEvent(f, seasonID, "pending_review",
				map[string]any{"reason": "operation_semantics_unverified", "state": state}))
			r.addIssue(seasonID, "pending_review", "operation_semantics_pending",
				fmt.Sprintf("operation %s semantics need verification", f.FactID),
				map[string]any{"fact_id": f.FactID, "operation_type": f.OperationType,
					"from": f.EventAt.UTC(), "to": f.EventAt.UTC()})
			continue
		}
		var match *FSMTransition
		for i := range doc.Transitions {
			t := &doc.Transitions[i]
			if t.From == state && t.On == f.OperationType {
				match = t
				break
			}
		}
		if match == nil {
			// No legal move from the current stage: an order conflict, e.g.
			// harvesting before any provable seeding. The fact is recorded as
			// evidence but cannot advance the machine.
			r.events = append(r.events, r.baseEvent(f, seasonID, "conflict",
				map[string]any{"reason": "no_transition_from_state", "state": state}))
			r.addIssue(seasonID, "conflict", "order_conflict",
				fmt.Sprintf("%s is not allowed from state %s in season %s", f.OperationType, state, seasonID),
				map[string]any{"fact_id": f.FactID, "operation_type": f.OperationType,
					"state": state, "event_at": f.EventAt.UTC()})
			continue
		}
		counts[f.OperationType]++
		interpretation := "confirmed"
		detail := map[string]any{"from_state": state, "to_stage": match.To}
		switch {
		case match.Alternative:
			interpretation = "alternative"
			detail["allowed_as"] = "alternative_operation"
		case match.Repeatable:
			interpretation = "duplicate"
			detail["allowed_as"] = "repeat_operation"
			detail["repeat_count"] = counts[f.OperationType]
		}
		r.events = append(r.events, r.baseEvent(f, seasonID, interpretation, detail))
		state = match.To
		if stage, ok := doc.StageOf[f.OperationType]; ok {
			r.confirmStage(stage)
		}
	}

	// Missing prerequisite evidence: stages the rules require but no fact
	// ever confirmed. Optional operations never produce a gap.
	optionalStages := map[string]bool{}
	for _, op := range doc.OptionalOperations {
		if st, ok := doc.StageOf[op]; ok {
			optionalStages[st] = true
		}
	}
	confirmed := map[string]bool{}
	for _, st := range r.confirmedStages {
		confirmed[st] = true
	}
	for _, st := range doc.Stages {
		op := operationForStage(doc.StageOf, st)
		_, hasFact := seenOps[op]
		if !confirmed[st] && !optionalStages[st] && (op == "" || !hasFact) {
			r.missing = append(r.missing, map[string]any{
				"engine": r.engine, "stage": st, "required_operation": op,
				"reason": "no_evidence_for_required_stage",
			})
			r.markPending()
		}
	}

	r.allowedPaths = fsmAllowedPaths(doc, state)
	r.pendingRanges = fsmPendingRanges(seasonID, season, facts, r)
	r.state = state
	return r
}

func operationForStage(stageOf map[string]string, stage string) string {
	for op, st := range stageOf {
		if st == stage {
			return op
		}
	}
	return ""
}

// fsmAllowedPaths explains, from the current state, which operations are
// legal next, and lists every declared alternative/repeat operation.
func fsmAllowedPaths(doc FSMRuleDoc, state string) []map[string]any {
	out := []map[string]any{}
	for _, t := range doc.Transitions {
		if t.From != state {
			continue
		}
		kind := "normal"
		if t.Alternative {
			kind = "alternative"
		} else if t.Repeatable {
			kind = "repeat"
		}
		out = append(out, map[string]any{
			"engine": ruleEngineFSM, "from_state": t.From, "operation": t.On,
			"to_stage": t.To, "kind": kind,
		})
	}
	return out
}

func fsmPendingRanges(seasonID string, season model.FieldSeason, facts []engineFact, r *fieldEngineResult) []map[string]any {
	out := []map[string]any{}
	for _, is := range r.issues {
		if is.Severity == "pending_review" {
			out = append(out, map[string]any{
				"engine": r.engine, "reason": is.Code, "from": is.Evidence["from"],
				"to": is.Evidence["to"], "fact_id": is.Evidence["fact_id"],
			})
		}
	}
	for _, m := range r.missing {
		out = append(out, map[string]any{
			"engine": r.engine, "reason": m["reason"], "stage": m["stage"],
			"from": season.PlannedFrom.UTC(), "to": season.PlannedTo.UTC(), "season_id": seasonID,
		})
	}
	if len(facts) == 0 {
		out = append(out, map[string]any{
			"engine": r.engine, "reason": "no_operation_evidence",
			"from": season.PlannedFrom.UTC(), "to": season.PlannedTo.UTC(), "season_id": seasonID,
		})
	}
	return out
}

// =====================================================================
// Partial-order engine
// =====================================================================

func runPartialOrder(seasonID string, season model.FieldSeason, facts []engineFact, doc PORuleDoc) *fieldEngineResult {
	r := newFieldEngineResult(ruleEnginePO)
	opByName := map[string]POOperation{}
	stageByOp := map[string]string{}
	for _, o := range doc.Operations {
		opByName[o.Name] = o
		stageByOp[o.Name] = o.Stage
	}
	requires := map[string][]POConstraint{} // stage -> incoming requires edges
	mutex := map[string][]POConstraint{}
	for _, c := range doc.Constraints {
		switch c.Type {
		case "requires":
			requires[c.To] = append(requires[c.To], c)
		case "mutex":
			mutex[c.From] = append(mutex[c.From], c)
		}
	}

	order := append([]engineFact(nil), facts...)
	sortFacts(order)
	confirmedStages := map[string]bool{}
	opCounts := map[string]int{}
	lastOpAt := map[string]time.Time{}
	type pendingFact struct {
		fact    engineFact
		stage   string
		missing []string
	}
	pending := []pendingFact{}

	stageBlocked := func(stage string) []string {
		missing := []string{}
		for _, c := range requires[stage] {
			if !confirmedStages[c.From] {
				missing = append(missing, c.From)
			}
		}
		return missing
	}

	for _, f := range order {
		op, known := opByName[f.OperationType]
		if !known {
			r.events = append(r.events, r.baseEvent(f, seasonID, "pending_review",
				map[string]any{"reason": "operation_semantics_unverified"}))
			r.addIssue(seasonID, "pending_review", "operation_semantics_pending",
				fmt.Sprintf("operation %s semantics need verification", f.FactID),
				map[string]any{"fact_id": f.FactID, "operation_type": f.OperationType,
					"from": f.EventAt.UTC(), "to": f.EventAt.UTC()})
			continue
		}
		// Count and gap limits.
		opCounts[f.OperationType]++
		if op.MaxCount > 0 && opCounts[f.OperationType] > op.MaxCount {
			r.events = append(r.events, r.baseEvent(f, seasonID, "conflict",
				map[string]any{"reason": "operation_count_exceeded", "max_count": op.MaxCount}))
			r.addIssue(seasonID, "conflict", "operation_count_exceeded",
				fmt.Sprintf("%s occurred %d times, rule allows %d", f.OperationType, opCounts[f.OperationType], op.MaxCount),
				map[string]any{"fact_id": f.FactID, "operation_type": f.OperationType, "max_count": op.MaxCount})
			continue
		}
		if prev, ok := lastOpAt[f.OperationType]; ok && op.MinGapHours > 0 {
			gap := f.EventAt.Sub(prev).Hours()
			if gap < float64(op.MinGapHours) {
				r.events = append(r.events, r.baseEvent(f, seasonID, "conflict",
					map[string]any{"reason": "min_gap_violation", "gap_hours": gap, "min_gap_hours": op.MinGapHours}))
				r.addIssue(seasonID, "conflict", "min_gap_violation",
					fmt.Sprintf("%s events %.1fh apart, rule requires %dh", f.OperationType, gap, op.MinGapHours),
					map[string]any{"fact_id": f.FactID, "gap_hours": gap, "min_gap_hours": op.MinGapHours})
			}
		}
		lastOpAt[f.OperationType] = f.EventAt

		// Mutex between stages.
		for _, c := range mutex[op.Stage] {
			if confirmedStages[c.To] {
				r.events = append(r.events, r.baseEvent(f, seasonID, "conflict",
					map[string]any{"reason": "mutex_stage_confirmed", "other_stage": c.To}))
				r.addIssue(seasonID, "conflict", "mutex_stage_conflict",
					fmt.Sprintf("stage %s cannot coexist with %s", op.Stage, c.To),
					map[string]any{"fact_id": f.FactID, "stage": op.Stage, "other_stage": c.To})
			}
		}

		// Partial-order prerequisites. While evidence is missing the fact
		// stays a feasible interpretation; finalize decides whether it can be
		// confirmed or remains an order conflict.
		missing := stageBlocked(op.Stage)
		if len(missing) > 0 {
			r.events = append(r.events, r.baseEvent(f, seasonID, "pending_review",
				map[string]any{"reason": "prerequisite_not_evidence_yet", "missing_stages": missing}))
			pending = append(pending, pendingFact{fact: f, stage: op.Stage, missing: missing})
			continue
		}

		interpretation := "confirmed"
		if opCounts[f.OperationType] > 1 {
			if op.Optional {
				interpretation = "duplicate"
			} else {
				interpretation = "alternative"
			}
		}
		r.events = append(r.events, r.baseEvent(f, seasonID, interpretation,
			map[string]any{"stage": op.Stage, "occurrence": opCounts[f.OperationType]}))
		confirmedStages[op.Stage] = true
		r.confirmStage(op.Stage)
	}

	// Finalize. Facts are processed in event-time order, so a fact that was
	// blocked when it happened can never become a valid ordering: if its
	// prerequisite stage is still unconfirmed it has no evidence at all; if
	// the prerequisite only appears later, the temporal order itself is
	// inverted (e.g. harvest earlier than the provable seeding). Both are
	// conflicts, distinguished by reason for the reviewer.
	for _, pf := range pending {
		missing := stageBlocked(pf.stage)
		for i := range r.events {
			if r.events[i].FactID != pf.fact.FactID {
				continue
			}
			if len(missing) > 0 {
				r.events[i].Interpretation = "conflict"
				r.events[i].Detail["final_reason"] = "prerequisite_never_evidenced"
				r.addIssue(seasonID, "conflict", "missing_prerequisite_evidence",
					fmt.Sprintf("%s at %s lacks prerequisite stage evidence %v",
						pf.fact.OperationType, pf.fact.EventAt.UTC().Format(time.RFC3339), missing),
					map[string]any{"fact_id": pf.fact.FactID, "operation_type": pf.fact.OperationType,
						"stage": pf.stage, "missing_stages": missing, "event_at": pf.fact.EventAt.UTC()})
			} else {
				r.events[i].Interpretation = "conflict"
				r.events[i].Detail["final_reason"] = "prerequisite_evidenced_after_fact"
				r.addIssue(seasonID, "conflict", "order_conflict",
					fmt.Sprintf("%s at %s happened before its prerequisite stage %v was evidenced",
						pf.fact.OperationType, pf.fact.EventAt.UTC().Format(time.RFC3339), pf.missing),
					map[string]any{"fact_id": pf.fact.FactID, "operation_type": pf.fact.OperationType,
						"stage": pf.stage, "required_before": pf.missing, "event_at": pf.fact.EventAt.UTC()})
			}
		}
	}

	// Required stages without any evidence are reported as missing
	// prerequisites rather than guessed.
	for _, o := range doc.Operations {
		if o.Optional || confirmedStages[o.Stage] {
			continue
		}
		if opCounts[o.Name] == 0 {
			r.missing = append(r.missing, map[string]any{
				"engine": r.engine, "stage": o.Stage, "required_operation": o.Name,
				"reason": "no_evidence_for_required_operation",
			})
			r.markPending()
		}
	}

	r.allowedPaths = poAllowedPaths(doc)
	r.pendingRanges = poPendingRanges(seasonID, season, facts, r)
	return r
}

// poAllowedPaths enumerates the feasible topological orderings of stages
// under the requires edges, keeping optional stages optional. This is the
// property that lets the partial-order engine hold several feasible stages
// open at once instead of forcing one linear path.
func poAllowedPaths(doc PORuleDoc) []map[string]any {
	prereq := map[string]map[string]bool{}
	optionalStage := map[string]bool{}
	for _, o := range doc.Operations {
		if prereq[o.Stage] == nil {
			prereq[o.Stage] = map[string]bool{}
		}
		if o.Optional {
			optionalStage[o.Stage] = true
		}
	}
	for _, c := range doc.Constraints {
		if c.Type == "requires" {
			prereq[c.To][c.From] = true
		}
	}
	stages := append([]string(nil), doc.Stages...)
	var dfs func(have map[string]bool, remaining []string, path []string)
	paths := [][]string{}
	dfs = func(have map[string]bool, remaining []string, path []string) {
		if len(remaining) == 0 {
			paths = append(paths, append([]string(nil), path...))
			return
		}
		moved := false
		for i, st := range remaining {
			ok := true
			for need := range prereq[st] {
				if !have[need] {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
			moved = true
			next := append([]string(nil), remaining[:i]...)
			next = append(next, remaining[i+1:]...)
			nextHave := map[string]bool{}
			for k, v := range have {
				nextHave[k] = v
			}
			nextHave[st] = true
			dfs(nextHave, next, append(path, st))
		}
		if !moved {
			// Unsatisfiable remainder: keep the partial path for explainability.
			paths = append(paths, append([]string(nil), path...))
		}
	}
	dfs(map[string]bool{}, stages, []string{})

	seen := map[string]bool{}
	out := []map[string]any{}
	for _, p := range paths {
		key := fmt.Sprint(p)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, map[string]any{
			"engine":          ruleEnginePO,
			"stage_path":      p,
			"optional_stages": sortedKeys(optionalStage),
		})
	}
	// Repeat allowances, e.g. multiple fertilizer batches and reseeding.
	for _, o := range doc.Operations {
		maxCount := "unlimited"
		if o.MaxCount > 0 {
			maxCount = fmt.Sprintf("%d", o.MaxCount)
		}
		out = append(out, map[string]any{
			"engine": ruleEnginePO, "operation": o.Name, "stage": o.Stage,
			"max_count": maxCount, "optional": o.Optional, "min_gap_hours": o.MinGapHours,
			"kind": "operation_rule",
		})
	}
	return out
}

func poPendingRanges(seasonID string, season model.FieldSeason, facts []engineFact, r *fieldEngineResult) []map[string]any {
	out := []map[string]any{}
	for _, is := range r.issues {
		if is.Severity == "pending_review" {
			out = append(out, map[string]any{
				"engine": r.engine, "reason": is.Code, "from": is.Evidence["from"],
				"to": is.Evidence["to"], "fact_id": is.Evidence["fact_id"],
			})
		}
	}
	for _, m := range r.missing {
		out = append(out, map[string]any{
			"engine": r.engine, "reason": m["reason"], "stage": m["stage"],
			"from": season.PlannedFrom.UTC(), "to": season.PlannedTo.UTC(), "season_id": seasonID,
		})
	}
	if len(facts) == 0 {
		out = append(out, map[string]any{
			"engine": r.engine, "reason": "no_operation_evidence",
			"from": season.PlannedFrom.UTC(), "to": season.PlannedTo.UTC(), "season_id": seasonID,
		})
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
