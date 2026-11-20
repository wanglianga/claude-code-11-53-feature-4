package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"agrisync/internal/config"
	"agrisync/internal/db"
	"agrisync/internal/model"
	"agrisync/internal/service"
)

//go:embed openapi.json
var openAPISpec []byte

// Server wires the HTTP surface. It is a backend service: every operation the
// platform supports is reachable through the documented REST API, and the
// OpenAPI 3 document below is served with Swagger UI at /docs.
type Server struct {
	cfg    *config.Config
	svc    *service.Service
	store  *db.Manager
	log    *slog.Logger
	static http.FileSystem
	mux    *http.ServeMux
}

func New(cfg *config.Config, store *db.Manager, svc *service.Service, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, svc: svc, store: store, log: log, mux: http.NewServeMux()}
	s.routes()
	return s
}

// UseStatic serves the vendored Swagger UI assets from the given filesystem.
func (s *Server) UseStatic(fsys http.FileSystem) {
	s.static = fsys
	s.routes()
}

func (s *Server) Handler() http.Handler {
	// Documentation and health are public; every business endpoint requires a
	// bearer token.
	public := http.NewServeMux()
	public.HandleFunc("/healthz", s.handleHealth)
	public.HandleFunc("/openapi.json", s.handleOpenAPI)
	public.HandleFunc("/docs", s.handleDocs)
	public.HandleFunc("/docs/", s.handleDocs)
	if s.static != nil {
		public.Handle("/swagger-ui/", http.StripPrefix("/swagger-ui/", http.FileServer(s.static)))
	}
	root := http.NewServeMux()
	root.Handle("/", public)
	root.Handle("/regions", s.auth(s.mux))
	root.Handle("/regions/", s.auth(s.mux))
	root.Handle("/replication/", s.auth(s.mux))
	root.Handle("/outbound/", s.auth(s.mux))
	root.Handle("/aggregates", s.auth(s.mux))
	root.Handle("/aggregates/", s.auth(s.mux))
	root.Handle("/deliveries/", s.auth(s.mux))
	root.Handle("/dr", s.auth(s.mux))
	root.Handle("/dr/", s.auth(s.mux))
	root.Handle("/audit", s.auth(s.mux))
	root.Handle("/audit/", s.auth(s.mux))
	root.Handle("/sequence/", s.auth(s.mux))
	root.Handle("/demo/", s.auth(s.mux))
	return s.recoverPanic(s.logRequests(root))
}

func (s *Server) routes() {
	m := http.NewServeMux()
	// Region write path + local reads.
	m.HandleFunc("/regions", s.handleRegions)
	m.HandleFunc("/regions/{region}/status", s.handleRegionStatus)
	m.HandleFunc("/regions/{region}/operations", s.handleOperations)
	m.HandleFunc("/regions/{region}/ingest", s.handleIngest)
	m.HandleFunc("/regions/{region}/journal/verify", s.handleVerifyJournal)

	// Field operation sequence integrity (in-region product).
	m.HandleFunc("/regions/{region}/sequence/facts", s.handleSequenceFacts)
	m.HandleFunc("/regions/{region}/sequence/facts/import", s.handleSequenceFactImport)
	m.HandleFunc("/regions/{region}/sequence/seasons", s.handleSequenceSeasons)
	m.HandleFunc("/regions/{region}/sequence/boundaries", s.handleSequenceBoundaries)
	m.HandleFunc("/regions/{region}/sequence/calendars", s.handleSequenceCalendars)
	m.HandleFunc("/regions/{region}/sequence/rules", s.handleSequenceRules)
	m.HandleFunc("/regions/{region}/sequence/reviews", s.handleSequenceReviews)
	m.HandleFunc("/regions/{region}/sequence/reviews/{id}", s.handleSequenceReview)
	m.HandleFunc("/regions/{region}/sequence/reviews/{id}/publish", s.handleSequenceReviewPublish)

	// Group-facing stage coverage: counts at approved granularity only.
	m.HandleFunc("/sequence/coverage/publications", s.handleCoveragePublications)
	m.HandleFunc("/sequence/coverage/rollups", s.handleCoverageRollups)

	// Replication control plane.
	m.HandleFunc("/replication/links", s.handleLinks)
	m.HandleFunc("/replication/links/{region}", s.handleLink)
	m.HandleFunc("/replication/cursors", s.handleCursors)
	m.HandleFunc("/replication/runs", s.handleReplicationRuns)
	m.HandleFunc("/replication/run", s.handleRunReplication)
	m.HandleFunc("/replication/batch", s.handleBatchReplication)
	m.HandleFunc("/replication/policies", s.handlePolicies)
	m.HandleFunc("/replication/replicas", s.handleReplicaStats)

	// Aggregation and cross-border delivery.
	m.HandleFunc("/outbound/requests", s.handleApprovals)
	m.HandleFunc("/outbound/requests/{id}", s.handleApproval)
	m.HandleFunc("/outbound/requests/{id}/decide", s.handleDecide)
	m.HandleFunc("/outbound/requests/{id}/revoke", s.handleRevoke)
	m.HandleFunc("/aggregates", s.handleAggregates)
	m.HandleFunc("/aggregates/{id}", s.handleAggregate)
	m.HandleFunc("/aggregates/{id}/deliver", s.handleDeliver)
	m.HandleFunc("/deliveries/{id}", s.handleDelivery)
	m.HandleFunc("/deliveries/{id}/verify", s.handleVerifyDelivery)

	// DR register.
	m.HandleFunc("/dr", s.handleDRList)
	m.HandleFunc("/dr/{region}", s.handleDR)
	m.HandleFunc("/dr/{region}/failover", s.handleFailover)
	m.HandleFunc("/dr/{region}/failback", s.handleFailback)

	// Audit ledger.
	m.HandleFunc("/audit", s.handleAudit)
	m.HandleFunc("/audit/verify", s.handleAuditVerify)
	m.HandleFunc("/audit/{subject}", s.handleAuditSubject)

	// Demo utilities.
	m.HandleFunc("/demo/reset", s.handleReset)
	m.HandleFunc("/demo/summary", s.handleSummary)

	s.mux = m
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		s.log.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.status,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic", "path", r.URL.Path, "error", fmt.Sprint(rec))
				writeError(w, http.StatusInternalServerError, "internal_error", "unexpected server error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// --- helpers ------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, code int, slug, msg string, details map[string]any) {
	writeJSON(w, code, model.APIErrorBody{Error: model.APIError{Code: slug, Message: msg, Details: details}})
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	code, slug := statusForError(err)
	writeError(w, code, slug, err.Error(), nil)
}

func decodeJSON(r *http.Request, dst any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("%w: unreadable body", service.ErrInvalid)
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return fmt.Errorf("%w: empty body", service.ErrInvalid)
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: malformed JSON: %v", service.ErrInvalid, err)
	}
	return nil
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func pathValue(r *http.Request, key string) string {
	return r.PathValue(key)
}

// --- infra endpoints ----------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 3*time.Second)
	defer cancel()
	status := "ok"
	if err := s.store.Control().Ping(ctx); err != nil {
		status = "degraded"
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": status, "service": "agrisync",
		"regions": s.store.RegionCodes(), "time": time.Now().UTC()})
}

func (s *Server) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(openAPISpec)
}

func (s *Server) handleDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, swaggerPage)
}

const swaggerPage = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>AgriSync Cross-Region API</title>
  <link rel="stylesheet" href="/swagger-ui/swagger-ui.css"/>
  <style>body{margin:0;background:#fafafa}</style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="/swagger-ui/swagger-ui-bundle.js"></script>
<script src="/swagger-ui/swagger-ui-standalone-preset.js"></script>
<script>
window.onload = function () {
  window.ui = SwaggerUIBundle({
    url: "/openapi.json",
    dom_id: "#swagger-ui",
    deepLinking: true,
    persistAuthorization: true,
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
    layout: "BaseLayout"
  });
};
</script>
</body>
</html>
`

// --- region endpoints ---------------------------------------------------

func (s *Server) handleRegions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	type regionView struct {
		Code        string `json:"code"`
		Name        string `json:"name"`
		Country     string `json:"country"`
		Timezone    string `json:"timezone"`
		PrimaryNode string `json:"primary_node"`
		StandbyNode string `json:"standby_node"`
	}
	out := []regionView{}
	for _, reg := range s.cfg.Regions {
		out = append(out, regionView{reg.Code, reg.Name, reg.Country, reg.Timezone, reg.PrimaryNode, reg.StandbyNode})
	}
	writeJSON(w, 200, map[string]any{"regions": out})
}

func (s *Server) handleRegionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	region := pathValue(r, "region")
	if err := s.requireRegion(principalFrom(r.Context()), region); err != nil {
		s.fail(w, err)
		return
	}
	if !s.regionKnown(region) {
		s.fail(w, fmt.Errorf("%w: unknown region %s", service.ErrNotFound, region))
		return
	}
	st, err := s.svc.RegionStatus(r.Context(), region)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, st)
}

func (s *Server) handleOperations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	region := pathValue(r, "region")
	if err := s.requireRegion(principalFrom(r.Context()), region); err != nil {
		s.fail(w, err)
		return
	}
	if !s.regionKnown(region) {
		s.fail(w, fmt.Errorf("%w: unknown region %s", service.ErrNotFound, region))
		return
	}
	ops, err := s.svc.ListOperations(r.Context(), region, queryInt(r, "limit", 100), r.URL.Query().Get("operation_type"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"region": region, "operations": ops})
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
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
	var batch model.IngestBatch
	if err := decodeJSON(r, &batch); err != nil {
		s.fail(w, err)
		return
	}
	res, err := s.svc.IngestBatch(r.Context(), region, batch, p.Actor)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, res)
}

func (s *Server) handleVerifyJournal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	region := pathValue(r, "region")
	if err := s.requireRegion(principalFrom(r.Context()), region); err != nil {
		s.fail(w, err)
		return
	}
	if !s.regionKnown(region) {
		s.fail(w, fmt.Errorf("%w: unknown region %s", service.ErrNotFound, region))
		return
	}
	v, err := s.svc.VerifyJournal(r.Context(), region)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, v)
}

// --- replication endpoints ----------------------------------------------

func (s *Server) handleLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	links, err := s.svc.ListLinks(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"links": links})
}

type linkRequest struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

func (s *Server) handleLink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	if !principalFrom(r.Context()).inAny("admin") {
		s.fail(w, service.ErrForbidden)
		return
	}
	region := pathValue(r, "region")
	if !s.regionKnown(region) {
		s.fail(w, fmt.Errorf("%w: unknown region %s", service.ErrNotFound, region))
		return
	}
	var req linkRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, err)
		return
	}
	if err := s.svc.SetLink(r.Context(), region, req.Status, req.Reason); err != nil {
		s.fail(w, err)
		return
	}
	links, _ := s.svc.ListLinks(r.Context())
	writeJSON(w, 200, map[string]any{"links": links})
}

func (s *Server) handleCursors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	cursors, err := s.svc.ListCursors(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"cursors": cursors})
}

func (s *Server) handleReplicationRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	runs, err := s.svc.ListReplicationRuns(r.Context(), r.URL.Query().Get("region"), queryInt(r, "limit", 20))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"runs": runs})
}

type replicateRequest struct {
	Region string `json:"region"`
	Fault  string `json:"fault"`
}

func (s *Server) handleRunReplication(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	p := principalFrom(r.Context())
	if !p.inAny("admin", "region_operator") {
		s.fail(w, service.ErrForbidden)
		return
	}
	var req replicateRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, err)
		return
	}
	if req.Region == "" {
		writeError(w, 400, "invalid_request", "region is required", nil)
		return
	}
	if p.Role == "region_operator" {
		if err := s.requireRegion(p, req.Region); err != nil {
			s.fail(w, err)
			return
		}
	}
	run, err := s.svc.Replicate(r.Context(), strings.ToUpper(req.Region), req.Fault)
	if err != nil {
		// A failed replication is a normal, recorded outcome: return the run
		// detail alongside the HTTP error so operators see the retry count.
		code, slug := statusForError(err)
		writeJSON(w, code, map[string]any{"error": model.APIError{Code: slug, Message: err.Error()}, "run": run})
		return
	}
	writeJSON(w, 200, run)
}

func (s *Server) handleBatchReplication(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	if !principalFrom(r.Context()).inAny("admin") {
		s.fail(w, service.ErrForbidden)
		return
	}
	var req replicateRequest
	_ = decodeJSON(r, &req)
	runs, err := s.svc.ReplicateAll(r.Context(), nil, req.Fault)
	if err != nil && !errors.Is(err, service.ErrUnavailable) {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"runs": runs})
}

func (s *Server) handlePolicies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	policies, err := s.svc.ListPolicies(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"policies": policies})
}

func (s *Server) handleReplicaStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	stats, err := s.svc.ReplicaStats(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, stats)
}

// --- approval endpoints -------------------------------------------------

type approvalRequest struct {
	Purpose      string     `json:"purpose"`
	Recipient    string     `json:"recipient"`
	ScopeRegions []string   `json:"scope_regions"`
	ScopeMetrics []string   `json:"scope_metrics"`
	ValidFrom    *time.Time `json:"valid_from"`
	ValidUntil   *time.Time `json:"valid_until"`
}

func (s *Server) handleApprovals(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	switch r.Method {
	case http.MethodGet:
		list, err := s.svc.ListApprovals(r.Context(), queryInt(r, "limit", 50))
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"requests": list})
	case http.MethodPost:
		if !p.inAny("group_analyst") {
			s.fail(w, service.ErrForbidden)
			return
		}
		var req approvalRequest
		if err := decodeJSON(r, &req); err != nil {
			s.fail(w, err)
			return
		}
		a, err := s.svc.CreateApproval(r.Context(), service.ApprovalInput{
			Purpose: req.Purpose, Recipient: req.Recipient, ScopeRegions: req.ScopeRegions,
			ScopeMetrics: req.ScopeMetrics, ValidFrom: req.ValidFrom, ValidUntil: req.ValidUntil,
		}, p.Actor, p.Role)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 201, a)
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}

func (s *Server) handleApproval(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	a, err := s.svc.GetApproval(r.Context(), pathValue(r, "id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, a)
}

type decisionRequest struct {
	Decision string `json:"decision"`
	Note     string `json:"note"`
}

func (s *Server) handleDecide(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	p := principalFrom(r.Context())
	if !p.inAny("approver") {
		s.fail(w, service.ErrForbidden)
		return
	}
	var req decisionRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, err)
		return
	}
	a, err := s.svc.DecideApproval(r.Context(), pathValue(r, "id"), req.Decision, req.Note, p.Actor, p.Role)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, a)
}

type revokeRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) handleRevoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	p := principalFrom(r.Context())
	if !p.inAny("approver") {
		s.fail(w, service.ErrForbidden)
		return
	}
	var req revokeRequest
	if err := decodeJSON(r, &req); err != nil {
		s.fail(w, err)
		return
	}
	a, err := s.svc.RevokeApproval(r.Context(), pathValue(r, "id"), req.Reason, p.Actor, p.Role)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, a)
}

// --- aggregate endpoints ------------------------------------------------

type aggregateRequest struct {
	ApprovalID string    `json:"approval_id"`
	PeriodFrom time.Time `json:"period_from"`
	PeriodTo   time.Time `json:"period_to"`
	MetricKeys []string  `json:"metric_keys"`
}

func (s *Server) handleAggregates(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		list, err := s.svc.ListAggregates(r.Context(), queryInt(r, "limit", 50))
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"runs": list})
	case http.MethodPost:
		p := principalFrom(r.Context())
		if !p.inAny("group_analyst") {
			s.fail(w, service.ErrForbidden)
			return
		}
		var req aggregateRequest
		if err := decodeJSON(r, &req); err != nil {
			s.fail(w, err)
			return
		}
		run, err := s.svc.ComputeAggregate(r.Context(), req.ApprovalID, req.PeriodFrom, req.PeriodTo, req.MetricKeys, p.Actor, p.Role)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 201, run)
	default:
		writeError(w, 405, "method_not_allowed", "GET or POST only", nil)
	}
}

func (s *Server) handleAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	run, err := s.svc.GetAggregate(r.Context(), pathValue(r, "id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, run)
}

func (s *Server) handleDeliver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	p := principalFrom(r.Context())
	if !p.inAny("group_analyst") {
		s.fail(w, service.ErrForbidden)
		return
	}
	d, err := s.svc.Deliver(r.Context(), pathValue(r, "id"), p.Actor, p.Role)
	if err != nil {
		s.fail(w, err)
		return
	}
	// A blocked delivery is a first-class, successful API outcome: the caller
	// gets the checks that failed and the audit sequence that recorded it.
	code := http.StatusOK
	if d.Status != "sent" {
		code = http.StatusConflict
	}
	writeJSON(w, code, d)
}

func (s *Server) handleDelivery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	d, err := s.svc.GetDelivery(r.Context(), pathValue(r, "id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, d)
}

// --- DR endpoints -------------------------------------------------------

func (s *Server) handleDRList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	list, err := s.svc.ListDR(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"regions": list})
}

func (s *Server) handleDR(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	reg, events, err := s.svc.GetDR(r.Context(), pathValue(r, "region"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"registry": reg, "events": events})
}

type failoverRequest struct {
	Reconcile bool `json:"reconcile"`
}

func (s *Server) handleFailover(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	p := principalFrom(r.Context())
	if !p.inAny("admin") {
		s.fail(w, service.ErrForbidden)
		return
	}
	var req failoverRequest
	_ = decodeJSON(r, &req)
	res, err := s.svc.Failover(r.Context(), pathValue(r, "region"), req.Reconcile, p.Actor, p.Role)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, res)
}

func (s *Server) handleFailback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	p := principalFrom(r.Context())
	if !p.inAny("admin") {
		s.fail(w, service.ErrForbidden)
		return
	}
	res, err := s.svc.Failback(r.Context(), pathValue(r, "region"), p.Actor, p.Role)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, res)
}

// --- audit endpoints ----------------------------------------------------

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	events, err := s.svc.ListAudit(r.Context(), queryInt(r, "limit", 100))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"events": events})
}

func (s *Server) handleAuditVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	v, err := s.svc.VerifyAudit(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	code := http.StatusOK
	if !v.Valid {
		code = http.StatusConflict
	}
	writeJSON(w, code, v)
}

func (s *Server) handleAuditSubject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	events, err := s.svc.AuditFor(r.Context(), pathValue(r, "subject"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"events": events})
}

// --- demo endpoints -----------------------------------------------------

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method_not_allowed", "POST only", nil)
		return
	}
	p := principalFrom(r.Context())
	if !p.inAny("admin") {
		s.fail(w, service.ErrForbidden)
		return
	}
	if os.Getenv("DEMO_MODE") == "false" {
		writeError(w, 403, "forbidden", "demo reset is disabled (DEMO_MODE=false)", nil)
		return
	}
	if err := s.store.Reset(r.Context()); err != nil {
		s.fail(w, err)
		return
	}
	for _, code := range s.store.RegionCodes() {
		if err := s.store.SeedRegionState(r.Context(), code); err != nil {
			s.fail(w, err)
			return
		}
		if err := s.svc.SeedDefaultRules(r.Context(), code); err != nil {
			s.fail(w, err)
			return
		}
	}
	_, _ = s.svc.AppendAudit(r.Context(), model.AuditEvent{
		Actor: p.Actor, Role: p.Role, Action: "demo.reset", Decision: "allow",
		Detail: map[string]any{"note": "demo state truncated"},
	})
	writeJSON(w, 200, map[string]any{"status": "reset"})
}

// handleSummary gives the verification script one call to assert group state.
func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	cursors, err := s.svc.ListCursors(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	links, err := s.svc.ListLinks(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	stats, err := s.svc.ReplicaStats(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	dr, err := s.svc.ListDR(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	policies, err := s.svc.ListPolicies(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	regions := []map[string]any{}
	for _, code := range s.store.RegionCodes() {
		st, err := s.svc.RegionStatus(r.Context(), code)
		if err != nil {
			s.fail(w, err)
			return
		}
		regions = append(regions, map[string]any{
			"region_code": st.RegionCode, "active_node": st.ActiveNode, "epoch": st.Epoch,
			"journal_head": st.JournalHead, "journal_entries": st.JournalEntries,
			"operation_count": st.OperationCount, "pending_outbox": st.PendingOutbox,
			"replication_link": st.LinkStatus, "handover_ready": st.HandoverReady,
		})
	}
	writeJSON(w, 200, map[string]any{
		"regions": regions, "cursors": cursors, "links": links, "replicas": stats,
		"dr": dr, "policies": policies,
	})
}

func contextWithTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}
