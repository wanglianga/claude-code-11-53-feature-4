package config

import (
	"os"
	"testing"
)

func TestLoadDefaultRegionsAndTokens(t *testing.T) {
	for _, k := range []string{"REGIONS", "PGHOST"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Regions) != 4 {
		t.Fatalf("expected 4 regions, got %d", len(cfg.Regions))
	}
	/* every role must have exactly one distinct secret */
	seen := map[string]int{}
	for _, tok := range cfg.Tokens {
		seen[tok.Secret]++
	}
	for s, n := range seen {
		if n != 1 {
			t.Fatalf("token %q configured %d times", s, n)
		}
	}
	want := map[string]bool{"admin": true, "group_analyst": true, "approver": true, "auditor": true, "region_operator": true}
	for _, tok := range cfg.Tokens {
		delete(want, tok.Role)
	}
	if len(want) != 0 {
		t.Fatalf("roles without tokens: %v", want)
	}
	// region operators are scoped to exactly one region each
	ops := 0
	for _, tok := range cfg.Tokens {
		if tok.Role == "region_operator" {
			ops++
			if tok.Region == "" {
				t.Fatalf("operator %s has no region scope", tok.Actor)
			}
		}
	}
	if ops != 4 {
		t.Fatalf("expected 4 region operators, got %d", ops)
	}
}

func TestLoadRejectsUnknownRegion(t *testing.T) {
	t.Setenv("REGIONS", "DE-BB,XX-YY")
	if _, err := Load(); err == nil {
		t.Fatal("unknown region should be rejected")
	}
	t.Setenv("REGIONS", "DE-BB,DE-BB")
	if _, err := Load(); err == nil {
		t.Fatal("duplicate region should be rejected")
	}
}

func TestRegionDBName(t *testing.T) {
	cfg := &Config{RegionDBFmt: "agrisync_region_"}
	if got := cfg.RegionDBName("BR-MT"); got != "agrisync_region_br_mt" {
		t.Fatalf("RegionDBName: %s", got)
	}
	dsn := cfg.DSN("x")
	if dsn == "" {
		t.Fatal("empty DSN")
	}
}
