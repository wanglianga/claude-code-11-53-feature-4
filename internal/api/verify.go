package api

import "net/http"

// handleVerifyDelivery re-checks an already-recorded delivery: stored artifact
// hash, absence of restricted fields, and the audit ledger entry that
// recorded it. This is the "prove the report never contained restricted raw
// trajectories" endpoint.
func (s *Server) handleVerifyDelivery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, 405, "method_not_allowed", "GET only", nil)
		return
	}
	v, err := s.svc.VerifyArtifact(r.Context(), pathValue(r, "id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	code := http.StatusOK
	if v.Status == "sent" && (!v.SHA256Matches || len(v.RestrictedHits) > 0 || !v.AuditValid || !v.AuditVerified) {
		code = http.StatusConflict
	}
	writeJSON(w, code, v)
}
