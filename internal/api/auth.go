package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"agrisync/internal/service"
)

type ctxKey string

const principalKey ctxKey = "principal"

// Principal is the authenticated caller.
type Principal struct {
	Actor  string
	Role   string
	Region string
}

func (p Principal) isAdmin() bool { return p.Role == "admin" }

func (p Principal) inAny(roles ...string) bool {
	if p.isAdmin() {
		return true
	}
	for _, r := range roles {
		if p.Role == r {
			return true
		}
	}
	return false
}

func principalFrom(ctx context.Context) Principal {
	p, _ := ctx.Value(principalKey).(Principal)
	return p
}

// auth resolves the bearer token against the configured credential table.
// Tokens are only compared for equality; there is no anonymous access.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		token := ""
		if strings.HasPrefix(h, "Bearer ") {
			token = strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
		}
		if token == "" {
			token = r.Header.Get("X-Demo-Token")
		}
		for _, t := range s.cfg.Tokens {
			if t.Secret == token && token != "" {
				p := Principal{Actor: t.Actor, Role: t.Role, Region: t.Region}
				writeCORS(w)
				if r.Method == http.MethodOptions {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey, p)))
				return
			}
		}
		writeCORS(w)
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token", nil)
	})
}

func writeCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Demo-Token")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
}

// requireRegion enforces that regional data is only readable by the region's
// own operator, an auditor, or the platform admin. Group analysts deliberately
// have no path to regional records: the only group-facing data is the
// approved aggregate produced by the aggregation service.
func (s *Server) requireRegion(p Principal, region string) error {
	if p.isAdmin() || p.Role == "auditor" {
		return nil
	}
	if p.Role == "region_operator" && p.Region == region {
		return nil
	}
	return service.ErrForbidden
}

// regionKnown reports whether the requested region is configured, so callers
// that are allowed to see the region can get an honest 404 instead of a
// data-dependent error further down the stack.
func (s *Server) regionKnown(region string) bool {
	_, ok := s.cfg.RegionByCode(region)
	return ok
}

func statusForError(err error) (int, string) {
	switch {
	case errors.Is(err, service.ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, service.ErrConflict):
		return http.StatusConflict, "conflict"
	case errors.Is(err, service.ErrInvalid):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, service.ErrForbidden):
		return http.StatusForbidden, "forbidden"
	case errors.Is(err, service.ErrUnavailable):
		return http.StatusServiceUnavailable, "unavailable"
	default:
		return http.StatusInternalServerError, "internal_error"
	}
}
