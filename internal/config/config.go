package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Region describes one operating region of the group. Each region owns a
// dedicated PostgreSQL database so that local-only data can never leak through
// a group-level query.
type Region struct {
	Code        string
	Name        string
	Country     string
	Timezone    string
	PrimaryNode string
	StandbyNode string
}

// Token is a bearer credential used by the demo deployment. Roles are:
// admin, region_operator, group_analyst, approver, auditor.
type Token struct {
	Secret string
	Actor  string
	Role   string
	Region string
}

type Config struct {
	Addr string

	PGHost      string
	PGPort      int
	PGUser      string
	PGPassword  string
	PGSSLMode   string
	AdminURL    string
	AdminDB     string
	ControlDB   string
	RegionDBFmt string // prefix, sanitized region code is appended

	Regions []Region
	Tokens  []Token

	ReplicationBatch int
	DeliveryChannel  string

	// ReplicationInterval enables the background replication sweep. Zero
	// (the default) keeps replication purely API-driven so replay scripts are
	// deterministic; production deployments set e.g. 5s.
	ReplicationInterval time.Duration
}

var KnownRegions = map[string]Region{
	"DE-BB": {
		Code: "DE-BB", Name: "Germany - Brandenburg", Country: "DE",
		Timezone: "Europe/Berlin", PrimaryNode: "de-bb-node-a", StandbyNode: "de-bb-node-b",
	},
	"BR-MT": {
		Code: "BR-MT", Name: "Brazil - Mato Grosso", Country: "BR",
		Timezone: "America/Cuiaba", PrimaryNode: "br-mt-node-a", StandbyNode: "br-mt-node-b",
	},
	"US-IA": {
		Code: "US-IA", Name: "United States - Iowa", Country: "US",
		Timezone: "America/Chicago", PrimaryNode: "us-ia-node-a", StandbyNode: "us-ia-node-b",
	},
	"FR-NOR": {
		Code: "FR-NOR", Name: "France - Normandie", Country: "FR",
		Timezone: "Europe/Paris", PrimaryNode: "fr-nor-node-a", StandbyNode: "fr-nor-node-b",
	},
}

func DefaultRegions() []string { return []string{"DE-BB", "BR-MT", "US-IA", "FR-NOR"} }

func Load() (*Config, error) {
	c := &Config{
		Addr:             env("HTTP_ADDR", ":8080"),
		PGHost:           env("PGHOST", "127.0.0.1"),
		PGUser:           env("PGUSER", "ccdev"),
		PGPassword:       env("PGPASSWORD", "ccdev"),
		PGSSLMode:        env("PGSSLMODE", "disable"),
		AdminURL:         os.Getenv("PG_ADMIN_URL"),
		AdminDB:          env("PG_ADMIN_DB", "postgres"),
		ControlDB:        env("CONTROL_DB", "agrisync_control"),
		RegionDBFmt:      env("REGION_DB_PREFIX", "agrisync_region_"),
		ReplicationBatch: 500,
		DeliveryChannel:  env("DELIVERY_CHANNEL", "secure_store"),
	}
	port, err := strconv.Atoi(env("PGPORT", "5432"))
	if err != nil {
		return nil, fmt.Errorf("invalid PGPORT: %w", err)
	}
	c.PGPort = port
	if v := os.Getenv("REPLICATION_BATCH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 5000 {
			return nil, fmt.Errorf("invalid REPLICATION_BATCH %q", v)
		}
		c.ReplicationBatch = n
	}

	if v := os.Getenv("REPLICATION_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 || d > time.Hour {
			return nil, fmt.Errorf("invalid REPLICATION_INTERVAL %q", v)
		}
		c.ReplicationInterval = d
	}

	codes := DefaultRegions()
	if v := strings.TrimSpace(os.Getenv("REGIONS")); v != "" {
		codes = nil
		for _, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				codes = append(codes, part)
			}
		}
	}
	if len(codes) == 0 {
		return nil, fmt.Errorf("REGIONS is empty")
	}
	seen := map[string]bool{}
	for _, code := range codes {
		code = strings.ToUpper(strings.TrimSpace(code))
		r, ok := KnownRegions[code]
		if !ok {
			return nil, fmt.Errorf("unknown region %q (known: DE-BB, BR-MT, US-IA, FR-NOR)", code)
		}
		if seen[code] {
			return nil, fmt.Errorf("duplicate region %q", code)
		}
		seen[code] = true
		c.Regions = append(c.Regions, r)
	}

	c.Tokens = []Token{
		{Secret: env("TOKEN_ADMIN", "demo-admin-token"), Actor: "platform-admin", Role: "admin"},
		{Secret: env("TOKEN_ANALYST", "demo-analyst-token"), Actor: "group-analyst", Role: "group_analyst"},
		{Secret: env("TOKEN_APPROVER", "demo-approver-token"), Actor: "data-steward", Role: "approver"},
		{Secret: env("TOKEN_AUDITOR", "demo-auditor-token"), Actor: "auditor", Role: "auditor"},
	}
	for _, r := range c.Regions {
		envKey := "TOKEN_OPERATOR_" + strings.ToUpper(strings.ReplaceAll(r.Code, "-", "_"))
		def := "demo-operator-" + strings.ToLower(r.Code)
		c.Tokens = append(c.Tokens, Token{
			Secret: env(envKey, def),
			Actor:  "operator-" + strings.ToLower(r.Code),
			Role:   "region_operator",
			Region: r.Code,
		})
	}
	seenSecret := map[string]bool{}
	for _, t := range c.Tokens {
		if t.Secret == "" {
			return nil, fmt.Errorf("empty token for role %s", t.Role)
		}
		if seenSecret[t.Secret] {
			return nil, fmt.Errorf("duplicate token configured")
		}
		seenSecret[t.Secret] = true
	}
	return c, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// RegionDBName maps a region code to its dedicated database name.
func (c *Config) RegionDBName(code string) string {
	return c.RegionDBFmt + strings.ToLower(strings.ReplaceAll(code, "-", "_"))
}

func (c *Config) AdminDSN() string {
	if c.AdminURL != "" {
		return c.AdminURL
	}
	return c.DSN(c.AdminDB)
}

func (c *Config) DSN(dbName string) string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.PGUser, c.PGPassword),
		Host:   fmt.Sprintf("%s:%d", c.PGHost, c.PGPort),
		Path:   "/" + dbName,
	}
	q := u.Query()
	q.Set("sslmode", c.PGSSLMode)
	q.Set("timezone", "UTC")
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *Config) RegionByCode(code string) (Region, bool) {
	for _, r := range c.Regions {
		if r.Code == code {
			return r, true
		}
	}
	return Region{}, false
}
