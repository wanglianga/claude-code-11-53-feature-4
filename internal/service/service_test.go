package service

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"agrisync/internal/config"
	"agrisync/internal/model"
)

func TestValidateOperation(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	end := now.Add(-time.Minute)
	good := model.OperationInput{
		DeviceSerial: "DE-SEED-1", SegmentID: "seg-1", OperationType: "seeding",
		AreaHa: 12.5, FuelL: 30, DurationS: 600, StartedAt: now.Add(-time.Hour),
		EndedAt: &end, Points: []model.GeoPoint{{Lat: 52.4, Lon: 13.2, At: now}},
		Alarms: []model.Alarm{{Code: "X", Severity: "warning", OccurredAt: now}},
	}
	if err := validateOperation(good, now); err != nil {
		t.Fatalf("well-formed operation rejected: %v", err)
	}

	cases := map[string]func(*model.OperationInput){
		"missing device":      func(o *model.OperationInput) { o.DeviceSerial = "" },
		"missing segment":     func(o *model.OperationInput) { o.SegmentID = "" },
		"bad type":            func(o *model.OperationInput) { o.OperationType = "spraying" },
		"negative area":       func(o *model.OperationInput) { o.AreaHa = -1 },
		"negative fuel":       func(o *model.OperationInput) { o.FuelL = -1 },
		"negative duration":   func(o *model.OperationInput) { o.DurationS = -1 },
		"missing start":       func(o *model.OperationInput) { o.StartedAt = time.Time{} },
		"start in far future": func(o *model.OperationInput) { o.StartedAt = now.Add(48 * time.Hour) },
		"end before start":    func(o *model.OperationInput) { e := o.StartedAt.Add(-time.Hour); o.EndedAt = &e },
		"lat out of range":    func(o *model.OperationInput) { o.Points = []model.GeoPoint{{Lat: 120, Lon: 0}} },
		"lon out of range":    func(o *model.OperationInput) { o.Points = []model.GeoPoint{{Lat: 0, Lon: 200}} },
		"alarm without code":  func(o *model.OperationInput) { o.Alarms = []model.Alarm{{Severity: "info"}} },
		"alarm bad severity":  func(o *model.OperationInput) { o.Alarms = []model.Alarm{{Code: "A", Severity: "fatal"}} },
	}
	for name, mutate := range cases {
		op := good
		op.Points = append([]model.GeoPoint(nil), good.Points...)
		op.Alarms = append([]model.Alarm(nil), good.Alarms...)
		mutate(&op)
		if err := validateOperation(op, now); err == nil {
			t.Errorf("%s: expected rejection, got none", name)
		}
	}
}

func TestNormalizeMetricsAndRegions(t *testing.T) {
	m, err := normalizeMetrics([]string{" area_ha", "area_ha", "operation_count"})
	if err != nil {
		t.Fatalf("normalize metrics: %v", err)
	}
	if strings.Join(m, ",") != "area_ha,operation_count" {
		t.Fatalf("unexpected order/dedupe: %v", m)
	}
	if _, err := normalizeMetrics(nil); err == nil {
		t.Fatal("empty metrics should be rejected")
	}
	if _, err := normalizeMetrics([]string{"gps_lat"}); err == nil {
		t.Fatal("unknown metric should be rejected")
	}

	cfg := &config.Config{Regions: []config.Region{{Code: "DE-BB"}, {Code: "BR-MT"}}}
	r, err := normalizeRegions([]string{"de-bb", "BR-MT", "DE-BB"}, cfg)
	if err != nil {
		t.Fatalf("normalize regions: %v", err)
	}
	if strings.Join(r, ",") != "BR-MT,DE-BB" {
		t.Fatalf("unexpected regions: %v", r)
	}
	if _, err := normalizeRegions([]string{"XX-YY"}, cfg); err == nil {
		t.Fatal("unknown region should be rejected")
	}
	if _, err := normalizeRegions(nil, cfg); err == nil {
		t.Fatal("empty scope should be rejected")
	}
}

func TestScanForRestrictedFields(t *testing.T) {
	clean := map[string]any{
		"regions": []any{map[string]any{"region_code": "DE-BB", "metrics": map[string]any{"area_ha": 12.5}}},
		"totals":  map[string]any{"operation_count": 4},
	}
	if hits := scanForRestrictedFields(clean, ""); len(hits) != 0 {
		t.Fatalf("clean artifact flagged: %v", hits)
	}
	dirty := map[string]any{
		"regions": []any{map[string]any{"region_code": "DE-BB", "points": []any{
			map[string]any{"lat": 52.4, "lon": 13.2},
		}}},
	}
	hits := scanForRestrictedFields(dirty, "")
	joined := strings.Join(hits, ",")
	for _, want := range []string{"/regions[0]/points", "/regions[0]/points[0]/lat", "/regions[0]/points[0]/lon"} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected hit %s, got %v", want, hits)
		}
	}
	// Field-name matching must be case-insensitive.
	upper := map[string]any{"LATITUDE": 1.0}
	if len(scanForRestrictedFields(upper, "")) != 1 {
		t.Fatal("uppercase restricted key not detected")
	}
}

func TestProjectMetricsDropsOutOfScopeKeys(t *testing.T) {
	full := model.Metrics{OperationCount: 10, CompletedCount: 8, CompletionRate: 80, AreaHa: 120.5, FuelL: 900, DurationHours: 22.25}
	got := projectMetrics(full, []string{"area_ha", "completion_rate"})
	if got.AreaHa != 120.5 || got.CompletionRate != 80 {
		t.Fatalf("in-scope metrics lost: %+v", got)
	}
	if got.OperationCount != 0 || got.FuelL != 0 || got.DurationHours != 0 || got.CompletedCount != 0 {
		t.Fatalf("out-of-scope metrics leaked: %+v", got)
	}
}

func TestCanonicalDetailIsStableAcrossJSONRoundTrip(t *testing.T) {
	in := map[string]any{
		"regions": []any{"DE-BB", "BR-MT"},
		"count":   float64(3),
		"nested":  map[string]any{"a": true, "b": nil},
	}
	first, err := canonicalDetail(in)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(first)
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	second, err := canonicalDetail(decoded)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if string(a) != string(b) {
		t.Fatalf("canonical detail changed across round trip:\n%s\n%s", a, b)
	}
}

func TestIntersectAndSameSet(t *testing.T) {
	if got := intersect([]string{"a", "b", "c"}, []string{"c", "a", "z"}); strings.Join(got, ",") != "a,c" {
		t.Fatalf("intersect: %v", got)
	}
	if !sameSet([]string{"a", "b"}, []string{"b", "a"}) {
		t.Fatal("sameSet should ignore order")
	}
	if sameSet([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("sameSet should compare lengths")
	}
}

func TestRound2(t *testing.T) {
	if got := round2(1.005); got != 1.01 && got != 1.0 {
		// Floating point: just require two-decimal granularity.
		t.Fatalf("round2 granularity: %v", got)
	}
	if got := round2(2.3449); got != 2.34 {
		t.Fatalf("round2(2.3449)=%v", got)
	}
}
