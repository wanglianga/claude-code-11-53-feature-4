package service

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"agrisync/internal/config"
	"agrisync/internal/db"
)

// Service implements the regional write path, the replication control plane,
// in-region aggregation, the cross-border approval gate and the DR register.
type Service struct {
	cfg *config.Config
	db  *db.Manager
	now func() time.Time
}

func New(cfg *config.Config, manager *db.Manager) *Service {
	return &Service{cfg: cfg, db: manager, now: time.Now}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func randID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

func jsonBytes(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func normalizeMetrics(metrics []string) ([]string, error) {
	allowed := map[string]bool{
		"operation_count": true, "completed_count": true, "completion_rate": true,
		"area_ha": true, "fuel_l": true, "duration_hours": true,
	}
	if len(metrics) == 0 {
		return nil, fmt.Errorf("%w: scope_metrics is required", ErrInvalid)
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(metrics))
	for _, m := range metrics {
		m = strings.TrimSpace(m)
		if !allowed[m] {
			return nil, fmt.Errorf("%w: unknown metric %q", ErrInvalid, m)
		}
		if !seen[m] {
			seen[m] = true
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out, nil
}

func normalizeRegions(codes []string, cfg *config.Config) ([]string, error) {
	if len(codes) == 0 {
		return nil, fmt.Errorf("%w: scope_regions is required", ErrInvalid)
	}
	seen := map[string]bool{}
	out := []string{}
	for _, c := range codes {
		c = strings.ToUpper(strings.TrimSpace(c))
		if _, ok := cfg.RegionByCode(c); !ok {
			return nil, fmt.Errorf("%w: unknown region %q", ErrInvalid, c)
		}
		if !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out, nil
}

// sameSet reports whether two string slices contain the same elements.
func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	return reflect.DeepEqual(x, y)
}

func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}
