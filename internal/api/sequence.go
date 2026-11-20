package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"agrisync/internal/service"
)

// Field operation sequence integrity HTTP surface.
//
// Regional endpoints live under /regions/{region}/sequence and are subject to
// the same region boundary as every other regional read: a group analyst has
// no path to field-level evidence. The only group-facing surface is
// /sequence/coverage, which carries region-level stage counts.
//
// Every POST accepts either a bare object or a plural wrapper such as
// {"facts":[...]} so the sample payloads stay readable and hand-crafted
// single-object calls still work.

func readBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: unreadable body", service.ErrInvalid)
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil, fmt.Errorf("%w: empty body", service.ErrInvalid)
	}
	return body, nil
}

// decodeBulk returns a JSON array: either the plural wrapper value or the
// request body itself wrapped when it is a bare object.
func decodeBulk(body []byte, plural string) ([]byte, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, fmt.Errorf("%w: malformed JSON: %v", service.ErrInvalid, err)
	}
	if raw, ok := probe[plural]; ok {
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, fmt.Errorf("%w: %s must be an array", service.ErrInvalid, plural)
		}
		return raw, nil
	}
	wrapped, err := json.Marshal([]json.RawMessage{body})
	if err != nil {
		return nil, err
	}
	return wrapped, nil
}

func (s *Server) requireRegionalWrite(w http.ResponseWriter, r *http.Request, region string) bool {
	p := principalFrom(r.Context())
	if !p.inAny("region_operator") {
		s.fail(w, service.ErrForbidden)
		return false
	}
	if err := s.requireRegion(p, region); err != nil {
		s.fail(w, err)
		return false
	}
	return true
}

func (s *Server) handleSequenceFacts(w http.ResponseWriter, r *http.Request) {
	region := pathValue(r, "region")
	p := principalFrom(r.Context())
	switch r.Method {
	case http.MethodGet:
		if err := s.requireRegion(p, region); err != nil {
			s.fail(w, err)
			return
		}
		facts, err := s.svc.ListFacts(r.Context(), region, r.URL.Query().Get("field_id"), queryInt(r, "limit", 200))
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"region": region, "facts": facts})
	case http.MethodPost:
		if !s.requireRegionalWrite(w, r, region) {
			return
		}
		body, err := readBody(r)
		if err != nil {
			s.fail(w, err)
			return
		}
		raw, err := decodeBulk(body, "facts")
		if err != nil {
			s.fail(w, err)
			return
		}
		var items []service.FactInput
		if err := json.Unmarshal(raw, &items); err != nil {
			s.fail(w, fmt.Errorf("%w: %v", service.ErrInvalid, err))
			return
		}
		n, err := s.svc.RegisterFacts(r.Context(), region, items, p.Actor)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"region": region, "written": n})
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}

func (s *Server) handleSequenceFactImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	region := pathValue(r, "region")
	if !s.requireRegionalWrite(w, r, region) {
		return
	}
	n, err := s.svc.ImportFactsFromOperations(r.Context(), region)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"region": region, "imported": n})
}

func (s *Server) handleSequenceSeasons(w http.ResponseWriter, r *http.Request) {
	region := pathValue(r, "region")
	p := principalFrom(r.Context())
	switch r.Method {
	case http.MethodGet:
		if err := s.requireRegion(p, region); err != nil {
			s.fail(w, err)
			return
		}
		seasons, err := s.svc.ListFieldSeasons(r.Context(), region)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"region": region, "seasons": seasons})
	case http.MethodPost:
		if !s.requireRegionalWrite(w, r, region) {
			return
		}
		body, err := readBody(r)
		if err != nil {
			s.fail(w, err)
			return
		}
		raw, err := decodeBulk(body, "seasons")
		if err != nil {
			s.fail(w, err)
			return
		}
		var items []service.FieldSeasonInput
		if err := json.Unmarshal(raw, &items); err != nil {
			s.fail(w, fmt.Errorf("%w: %v", service.ErrInvalid, err))
			return
		}
		for _, in := range items {
			if err := s.svc.UpsertFieldSeason(r.Context(), region, in); err != nil {
				s.fail(w, err)
				return
			}
		}
		writeJSON(w, 200, map[string]any{"region": region, "stored": len(items)})
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}

func (s *Server) handleSequenceBoundaries(w http.ResponseWriter, r *http.Request) {
	region := pathValue(r, "region")
	p := principalFrom(r.Context())
	switch r.Method {
	case http.MethodGet:
		if err := s.requireRegion(p, region); err != nil {
			s.fail(w, err)
			return
		}
		rows, err := s.svc.ListBoundaries(r.Context(), region)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"region": region, "boundary_revisions": rows})
	case http.MethodPost:
		if !s.requireRegionalWrite(w, r, region) {
			return
		}
		body, err := readBody(r)
		if err != nil {
			s.fail(w, err)
			return
		}
		raw, err := decodeBulk(body, "boundaries")
		if err != nil {
			s.fail(w, err)
			return
		}
		var items []service.BoundaryInput
		if err := json.Unmarshal(raw, &items); err != nil {
			s.fail(w, fmt.Errorf("%w: %v", service.ErrInvalid, err))
			return
		}
		for _, in := range items {
			if err := s.svc.UpsertBoundary(r.Context(), region, in); err != nil {
				s.fail(w, err)
				return
			}
		}
		writeJSON(w, 200, map[string]any{"region": region, "stored": len(items)})
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}

func (s *Server) handleSequenceCalendars(w http.ResponseWriter, r *http.Request) {
	region := pathValue(r, "region")
	p := principalFrom(r.Context())
	switch r.Method {
	case http.MethodGet:
		if err := s.requireRegion(p, region); err != nil {
			s.fail(w, err)
			return
		}
		rows, err := s.svc.ListCropCalendars(r.Context(), region)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"region": region, "crop_calendars": rows})
	case http.MethodPost:
		if !s.requireRegionalWrite(w, r, region) {
			return
		}
		body, err := readBody(r)
		if err != nil {
			s.fail(w, err)
			return
		}
		raw, err := decodeBulk(body, "calendars")
		if err != nil {
			s.fail(w, err)
			return
		}
		var items []service.CropCalendarInput
		if err := json.Unmarshal(raw, &items); err != nil {
			s.fail(w, fmt.Errorf("%w: %v", service.ErrInvalid, err))
			return
		}
		for _, in := range items {
			if err := s.svc.UpsertCropCalendar(r.Context(), region, in); err != nil {
				s.fail(w, err)
				return
			}
		}
		writeJSON(w, 200, map[string]any{"region": region, "stored": len(items)})
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}

func (s *Server) handleSequenceRules(w http.ResponseWriter, r *http.Request) {
	region := pathValue(r, "region")
	p := principalFrom(r.Context())
	switch r.Method {
	case http.MethodGet:
		if err := s.requireRegion(p, region); err != nil {
			s.fail(w, err)
			return
		}
		rows, err := s.svc.ListRuleSets(r.Context(), region)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"region": region, "rule_sets": rows})
	case http.MethodPost:
		if !s.requireRegionalWrite(w, r, region) {
			return
		}
		body, err := readBody(r)
		if err != nil {
			s.fail(w, err)
			return
		}
		raw, err := decodeBulk(body, "rules")
		if err != nil {
			s.fail(w, err)
			return
		}
		var items []service.RuleSetInput
		if err := json.Unmarshal(raw, &items); err != nil {
			s.fail(w, fmt.Errorf("%w: %v", service.ErrInvalid, err))
			return
		}
		for _, in := range items {
			if err := s.svc.UpsertRuleSet(r.Context(), region, in); err != nil {
				s.fail(w, err)
				return
			}
		}
		writeJSON(w, 200, map[string]any{"region": region, "stored": len(items)})
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}

func (s *Server) handleSequenceReviews(w http.ResponseWriter, r *http.Request) {
	region := pathValue(r, "region")
	p := principalFrom(r.Context())
	switch r.Method {
	case http.MethodGet:
		if err := s.requireRegion(p, region); err != nil {
			s.fail(w, err)
			return
		}
		runs, err := s.svc.ListSequenceReviews(r.Context(), region, queryInt(r, "limit", 50))
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"region": region, "runs": runs})
	case http.MethodPost:
		if !s.requireRegionalWrite(w, r, region) {
			return
		}
		var in service.ReviewRunInput
		if err := decodeJSON(r, &in); err != nil {
			s.fail(w, err)
			return
		}
		detail, err := s.svc.CreateSequenceReview(r.Context(), region, in, p.Actor, p.Role)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 201, detail)
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}

func (s *Server) handleSequenceReview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	region := pathValue(r, "region")
	if err := s.requireRegion(principalFrom(r.Context()), region); err != nil {
		s.fail(w, err)
		return
	}
	detail, err := s.svc.GetSequenceReview(r.Context(), region, pathValue(r, "id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, detail)
}

func (s *Server) handleSequenceReviewPublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	region := pathValue(r, "region")
	p := principalFrom(r.Context())
	if !p.inAny("region_operator") {
		s.fail(w, service.ErrForbidden)
		return
	}
	if err := s.requireRegion(p, region); err != nil {
		s.fail(w, err)
		return
	}
	pubs, err := s.svc.PublishSequenceCoverage(r.Context(), region, pathValue(r, "id"), p.Actor, p.Role)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"region": region, "granularity": "region_stage_counts", "publications": pubs,
		"notice": "stage coverage counts only; field-level evidence remains in the region",
	})
}

// --- group-facing coverage (no field-level data) ------------------------

func (s *Server) handleCoveragePublications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	p := principalFrom(r.Context())
	if !p.inAny("group_analyst", "auditor") {
		s.fail(w, service.ErrForbidden)
		return
	}
	pubs, err := s.svc.ListCoveragePublications(r.Context(), r.URL.Query().Get("region"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"publications": pubs})
}

type rollupRequest struct {
	Engine       string    `json:"engine"`
	PeriodFrom   time.Time `json:"period_from"`
	PeriodTo     time.Time `json:"period_to"`
	ScopeRegions []string  `json:"scope_regions"`
}

func (s *Server) handleCoverageRollups(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	switch r.Method {
	case http.MethodGet:
		if !p.inAny("group_analyst", "auditor") {
			s.fail(w, service.ErrForbidden)
			return
		}
		rollups, err := s.svc.ListCoverageRollups(r.Context(), queryInt(r, "limit", 50))
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"rollups": rollups})
	case http.MethodPost:
		if !p.inAny("group_analyst") {
			s.fail(w, service.ErrForbidden)
			return
		}
		var in rollupRequest
		if err := decodeJSON(r, &in); err != nil {
			s.fail(w, err)
			return
		}
		rollup, err := s.svc.RollupSequenceCoverage(r.Context(), in.Engine, in.PeriodFrom, in.PeriodTo,
			in.ScopeRegions, p.Actor)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 201, rollup)
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}
