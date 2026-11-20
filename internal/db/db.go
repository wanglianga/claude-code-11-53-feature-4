package db

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"agrisync/internal/config"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Manager owns one connection pool per database: the group control plane plus
// one operational database per region. Region databases are physically
// separate so that a group-level query cannot read a restricted local table.
type Manager struct {
	cfg     *config.Config
	control *pgxpool.Pool

	mu      sync.RWMutex
	regions map[string]*pgxpool.Pool
}

func Open(ctx context.Context, cfg *config.Config) (*Manager, error) {
	m := &Manager{cfg: cfg, regions: map[string]*pgxpool.Pool{}}
	if err := ensureDatabase(ctx, cfg, cfg.ControlDB); err != nil {
		return nil, err
	}
	pool, err := newPool(ctx, cfg.DSN(cfg.ControlDB))
	if err != nil {
		return nil, fmt.Errorf("connect control db: %w", err)
	}
	m.control = pool
	if err := m.ensureSchema(ctx, pool, "migrations/control.sql"); err != nil {
		return nil, fmt.Errorf("control schema: %w", err)
	}
	for _, r := range cfg.Regions {
		dbName := cfg.RegionDBName(r.Code)
		if err := ensureDatabase(ctx, cfg, dbName); err != nil {
			return nil, err
		}
		rp, err := newPool(ctx, cfg.DSN(dbName))
		if err != nil {
			return nil, fmt.Errorf("connect region %s: %w", r.Code, err)
		}
		if err := m.ensureSchema(ctx, rp, "migrations/region.sql"); err != nil {
			rp.Close()
			return nil, fmt.Errorf("region schema %s: %w", r.Code, err)
		}
		m.regions[r.Code] = rp
	}
	if err := m.seedRegistry(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

func newPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = 8
	pc.MinConns = 1
	pc.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func (m *Manager) ensureSchema(ctx context.Context, pool *pgxpool.Pool, path string) error {
	body, err := migrations.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, string(body))
	return err
}

// ensureDatabase creates the database if it does not exist. It connects with
// the admin DSN (default: the `postgres` maintenance database).
func ensureDatabase(ctx context.Context, cfg *config.Config, dbName string) error {
	admin, err := pgx.Connect(ctx, cfg.AdminDSN())
	if err != nil {
		return fmt.Errorf("connect admin db: %w", err)
	}
	defer admin.Close(ctx)
	var exists bool
	if err := admin.QueryRow(ctx, `select exists(select 1 from pg_database where datname=$1)`, dbName).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	ident := pgx.Identifier{dbName}.Sanitize()
	if _, err := admin.Exec(ctx, `create database `+ident); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P04" { // duplicate_database
			return nil
		}
		return fmt.Errorf("create database %s: %w", dbName, err)
	}
	return nil
}

// seedRegistry makes the region registry, residency policy and DR register
// converge on the configured regions. It is idempotent and safe on restart.
func (m *Manager) seedRegistry(ctx context.Context) error {
	tx, err := m.control.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Policies per region. Restricted raw trajectory: only US-IA has an
	// explicit outbound authorization for raw traces in the demo dataset;
	// DE-BB, BR-MT and FR-NOR keep raw points in region and share aggregates.
	policies := map[string]bool{
		"DE-BB":  false,
		"BR-MT":  false,
		"US-IA":  true,
		"FR-NOR": false,
	}
	for _, r := range m.cfg.Regions {
		if _, err := tx.Exec(ctx, `
			insert into regions (code, name, country, timezone, db_name)
			values ($1,$2,$3,$4,$5)
			on conflict (code) do update
			  set name=excluded.name, country=excluded.country,
			      timezone=excluded.timezone, db_name=excluded.db_name`,
			r.Code, r.Name, r.Country, r.Timezone, m.cfg.RegionDBName(r.Code)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			insert into dr_registry (region_code, primary_node, standby_node, active_node, status)
			values ($1,$2,$3,$4,'healthy')
			on conflict (region_code) do update
			  set primary_node=excluded.primary_node, standby_node=excluded.standby_node`,
			r.Code, r.PrimaryNode, r.StandbyNode, r.PrimaryNode); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			insert into replication_links (region_code, status, reason)
			values ($1,'online','')
			on conflict (region_code) do nothing`, r.Code); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			insert into replication_cursors (source_region) values ($1)
			on conflict (source_region) do nothing`, r.Code); err != nil {
			return err
		}
		rawAllowed := policies[r.Code]
		rows := []struct {
			category string
			allowed  bool
			why      string
		}{
			{"raw_trajectory", rawAllowed, rawRationale(rawAllowed)},
			{"operation_summary", true, "allowed: aggregated work record without coordinates"},
			{"device_alarm", true, "allowed: alarm codes without coordinates"},
			{"aggregate_metric", true, "allowed: in-region aggregate approved for group reporting"},
		}
		for _, row := range rows {
			if _, err := tx.Exec(ctx, `
				insert into residency_policies (region_code, category, outbound_allowed, rationale)
				values ($1,$2,$3,$4)
				on conflict (region_code, category) do update
				  set outbound_allowed=excluded.outbound_allowed, rationale=excluded.rationale`,
				r.Code, row.category, row.allowed, row.why); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

// SeedRegionState initialises the local region_state row. Existing state is
// preserved so restarts and failovers do not lose the active node pointer.
func (m *Manager) SeedRegionState(ctx context.Context, code string) error {
	r, ok := m.cfg.RegionByCode(code)
	if !ok {
		return fmt.Errorf("unknown region %s", code)
	}
	pool := m.Region(code)
	if pool == nil {
		return fmt.Errorf("region %s not open", code)
	}
	_, err := pool.Exec(ctx, `
		insert into region_state (id, region_code, active_node) values (true,$1,$2)
		on conflict (id) do nothing`, code, r.PrimaryNode)
	return err
}

func (m *Manager) Control() *pgxpool.Pool { return m.control }

func (m *Manager) Region(code string) *pgxpool.Pool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.regions[code]
}

func (m *Manager) RegionCodes() []string {
	out := make([]string, 0, len(m.regions))
	for _, r := range m.cfg.Regions {
		out = append(out, r.Code)
	}
	return out
}

func (m *Manager) Close() {
	for _, p := range m.regions {
		p.Close()
	}
	if m.control != nil {
		m.control.Close()
	}
}

// Reset drops all data in the control plane and every region database. Used by
// the demo reset endpoint and the verification script; refused in production
// mode by the HTTP layer.
func (m *Manager) Reset(ctx context.Context) error {
	tables := []string{
		"deliveries", "aggregate_run_regions", "aggregate_runs", "outbound_requests",
		"audit_events", "replication_runs", "replica_op_summaries", "replica_trajectories",
		"replica_alarms", "replica_metrics",
		"sequence_coverage_rollups", "sequence_coverage_publications",
	}
	for _, t := range tables {
		if _, err := m.control.Exec(ctx, "truncate table "+t+" restart identity cascade"); err != nil {
			return err
		}
	}
	// reset cursors and DR register but keep region/policy/link rows
	for _, q := range []string{
		`update replication_cursors set last_seq=0, confirmed_seq=0, status='idle', retry_count=0,
		   copied_total=0, blocked_total=0, last_attempt_at=null, last_success_at=null, last_error=null, updated_at=now()`,
		`update dr_registry set epoch=1, last_confirmed_seq=0, last_confirmed_at=null,
		   status='healthy', active_node=primary_node, updated_at=now()`,
		`update replication_links set status='online', reason='', updated_at=now()`,
		`delete from dr_events`,
	} {
		if _, err := m.control.Exec(ctx, q); err != nil {
			return err
		}
	}
	for _, code := range m.RegionCodes() {
		reg, _ := m.cfg.RegionByCode(code)
		for _, q := range []string{
			`truncate operation_events, journal_entries, outbox_events, metric_snapshots, ingest_receipts, devices,
			   sequence_review_issues, sequence_review_events, sequence_review_fields, sequence_review_engines,
			   sequence_review_runs, sequence_facts, crop_calendars, field_boundary_revisions, field_seasons,
			   agronomy_rule_sets`,
			fmt.Sprintf(`update region_state set local_epoch=1, next_journal_seq=1, next_outbox_seq=1,
			   active_node=%s, updated_at=now()`, quoteLiteral(reg.PrimaryNode)),
		} {
			if _, err := m.Region(code).Exec(ctx, q); err != nil {
				return err
			}
		}
	}
	return nil
}

func quoteLiteral(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

// SanitizeIdentifier is exported for tests.
func SanitizeIdentifier(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "-", "_"))
}

func rawRationale(allowed bool) string {
	if allowed {
		return "outbound authorized by regional data steward: raw trajectory may be copied"
	}
	return "restricted: raw GPS trajectories stay in the region"
}
