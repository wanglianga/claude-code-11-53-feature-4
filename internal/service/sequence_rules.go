package service

import (
	"fmt"
	"sort"
)

// Rule document structures. A rule set is versioned, engine-specific and
// optionally crop-specific (crop_code == "" means a generic rule set that
// applies to every crop). The documents are stored verbatim as jsonb, so an
// old review run always keeps the exact rules that produced it.

// FSMTransition moves the finite-state machine from one stage on an observed
// operation. Repeatable transitions model allowed repeats (fertilizer
// batches); alternative transitions model substitute operations such as
// reseeding after the first sowing.
type FSMTransition struct {
	From        string `json:"from"`
	On          string `json:"on"`
	To          string `json:"to"`
	Repeatable  bool   `json:"repeatable"`
	Alternative bool   `json:"alternative"`
}

// FSMRuleDoc is the declarative finite-state-machine definition.
type FSMRuleDoc struct {
	Engine       string          `json:"engine"`
	CropCode     string          `json:"crop_code"`
	InitialState string          `json:"initial_state"`
	Stages       []string        `json:"stages"`
	Transitions  []FSMTransition `json:"transitions"`
	// StageOf maps an operation type to the stage its evidence confirms.
	StageOf map[string]string `json:"stage_of"`
	// OptionalOperations never generate a missing-prerequisite issue.
	OptionalOperations []string `json:"optional_operations"`
}

// POOperation declares one operation node of the partial-order graph.
// MaxCount 0 means unlimited repeats. MinGapHours constrains consecutive
// events of the same operation.
type POOperation struct {
	Name        string `json:"name"`
	Stage       string `json:"stage"`
	Optional    bool   `json:"optional"`
	MaxCount    int    `json:"max_count"`
	MinGapHours int    `json:"min_gap_hours"`
}

// POConstraint is a directed edge. "requires" means To may only be confirmed
// once From is confirmed; "mutex" means the two stages cannot both hold.
type POConstraint struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Type        string `json:"type"`
	MaxLagHours int    `json:"max_lag_hours"`
}

// PORuleDoc is the declarative partial-order definition.
type PORuleDoc struct {
	Engine      string         `json:"engine"`
	CropCode    string         `json:"crop_code"`
	Stages      []string       `json:"stages"`
	Operations  []POOperation  `json:"operations"`
	Constraints []POConstraint `json:"constraints"`
}

const (
	ruleEngineFSM = "fsm"
	ruleEnginePO  = "partial_order"
	ruleVersionV1 = "rules-v1"
)

// DefaultFSMRules is the generic, crop-agnostic FSM shipped with every
// region. Fertilizing is optional and repeatable; a second seeding is an
// allowed alternative (reseeding); harvesting without a prior seeding has
// no transition and therefore remains an order conflict.
func DefaultFSMRules() FSMRuleDoc {
	return FSMRuleDoc{
		Engine:       ruleEngineFSM,
		CropCode:     "",
		InitialState: "unstarted",
		Stages:       []string{"seeded", "fertilized", "harvested"},
		StageOf: map[string]string{
			"seeding": "seeded", "fertilizing": "fertilized", "harvesting": "harvested",
		},
		OptionalOperations: []string{"fertilizing"},
		Transitions: []FSMTransition{
			{From: "unstarted", On: "seeding", To: "seeded"},
			{From: "seeded", On: "seeding", To: "seeded", Repeatable: true, Alternative: true},
			{From: "seeded", On: "fertilizing", To: "fertilized"},
			{From: "fertilized", On: "fertilizing", To: "fertilized", Repeatable: true},
			{From: "seeded", On: "harvesting", To: "harvested"},
			{From: "fertilized", On: "harvesting", To: "harvested"},
			{From: "harvested", On: "harvesting", To: "harvested", Repeatable: true, Alternative: true},
		},
	}
}

// DefaultPORules is the matching generic partial-order definition. It can
// express several fertilizer batches and parallel preparation work, and it
// keeps several feasible stages open while prerequisite evidence is missing.
func DefaultPORules() PORuleDoc {
	return PORuleDoc{
		Engine:   ruleEnginePO,
		CropCode: "",
		Stages:   []string{"seeded", "fertilized", "harvested"},
		Operations: []POOperation{
			{Name: "seeding", Stage: "seeded", MaxCount: 2, MinGapHours: 24},
			{Name: "fertilizing", Stage: "fertilized", Optional: true, MaxCount: 0, MinGapHours: 12},
			{Name: "harvesting", Stage: "harvested", MaxCount: 1},
		},
		Constraints: []POConstraint{
			{From: "seeded", To: "fertilized", Type: "requires"},
			{From: "seeded", To: "harvested", Type: "requires"},
		},
	}
}

func validateFSMRuleDoc(d FSMRuleDoc) error {
	if d.Engine != ruleEngineFSM {
		return fmt.Errorf("%w: fsm rule doc must declare engine=fsm", ErrInvalid)
	}
	if d.InitialState == "" || len(d.Transitions) == 0 {
		return fmt.Errorf("%w: fsm rule doc needs initial_state and transitions", ErrInvalid)
	}
	known := map[string]bool{d.InitialState: true}
	for _, st := range d.Stages {
		known[st] = true
	}
	for _, t := range d.Transitions {
		if !known[t.From] || !known[t.To] {
			return fmt.Errorf("%w: transition %s--%s-->%s references unknown state", ErrInvalid, t.From, t.On, t.To)
		}
		if t.On == "" {
			return fmt.Errorf("%w: transition without operation", ErrInvalid)
		}
	}
	for op := range d.StageOf {
		if op == "" {
			return fmt.Errorf("%w: empty operation in stage_of", ErrInvalid)
		}
	}
	return nil
}

func validatePORuleDoc(d PORuleDoc) error {
	if d.Engine != ruleEnginePO {
		return fmt.Errorf("%w: partial_order rule doc must declare engine=partial_order", ErrInvalid)
	}
	if len(d.Operations) == 0 {
		return fmt.Errorf("%w: partial_order rule doc needs operations", ErrInvalid)
	}
	stages := map[string]bool{}
	for _, st := range d.Stages {
		stages[st] = true
	}
	ops := map[string]bool{}
	for _, o := range d.Operations {
		if o.Name == "" || o.Stage == "" {
			return fmt.Errorf("%w: operation needs name and stage", ErrInvalid)
		}
		if !stages[o.Stage] {
			return fmt.Errorf("%w: operation %s references unknown stage %s", ErrInvalid, o.Name, o.Stage)
		}
		ops[o.Name] = true
	}
	for _, c := range d.Constraints {
		if c.Type != "requires" && c.Type != "mutex" {
			return fmt.Errorf("%w: unknown constraint type %q", ErrInvalid, c.Type)
		}
		if !stages[c.From] || !stages[c.To] {
			return fmt.Errorf("%w: constraint references unknown stage", ErrInvalid)
		}
	}
	return nil
}

// sortedStages returns stages in their declared rule order, falling back to
// lexical order for stages the rule doc omitted.
func sortedStages(declared []string, found map[string]bool) []string {
	out := make([]string, 0, len(found))
	seen := map[string]bool{}
	for _, st := range declared {
		if found[st] && !seen[st] {
			out = append(out, st)
			seen[st] = true
		}
	}
	extra := make([]string, 0)
	for st := range found {
		if !seen[st] {
			extra = append(extra, st)
		}
	}
	sort.Strings(extra)
	return append(out, extra...)
}
