-- Per-region operational schema. Each region has its own PostgreSQL database;
-- raw trajectories written here are never copied out unless residency policy
-- explicitly allows the raw_trajectory category to leave the region.
-- Local writes commit locally even while the inter-region link is down.

create table if not exists region_state (
  id                boolean primary key default true check (id),
  region_code       text not null,
  active_node       text not null,
  local_epoch       bigint not null default 1,
  next_journal_seq  bigint not null default 1,
  next_outbox_seq   bigint not null default 1,
  updated_at        timestamptz not null default now()
);

create table if not exists devices (
  device_serial  text primary key,
  device_type    text not null default 'seeder',
  model          text not null default '',
  first_seen_node text not null,
  created_at     timestamptz not null default now()
);

-- Local, authoritative operation records. Deduplication key is the device
-- serial plus the region that sourced the segment; the region is implicit in
-- the database boundary and explicit in every cross-region replica.
create table if not exists operation_events (
  id            text primary key,
  journal_seq   bigint not null unique,
  device_serial text not null,
  segment_id    text not null,
  operation_type text not null check (operation_type in ('seeding','harvesting','fertilizing')),
  field_id      text not null default '',
  area_ha       double precision not null check (area_ha >= 0),
  fuel_l        double precision not null default 0 check (fuel_l >= 0),
  duration_s    integer not null default 0 check (duration_s >= 0),
  completed     boolean not null default false,
  started_at    timestamptz not null,
  ended_at      timestamptz,
  point_count   integer not null default 0,
  points        jsonb not null default '[]'::jsonb,
  alarms        jsonb not null default '[]'::jsonb,
  upload_batch  text not null default '',
  source_node   text not null,
  source_epoch  bigint not null default 1,
  received_at   timestamptz not null default now(),
  unique (device_serial, segment_id)
);
create index if not exists operation_events_started_idx on operation_events (started_at);
create index if not exists operation_events_type_idx on operation_events (operation_type);

-- Gapless local journal. The next sequence is allocated under a row lock in
-- the same transaction as the operation insert, so a region that keeps
-- recording through an outage has a contiguous, provable local history.
create table if not exists journal_entries (
  seq          bigint primary key,
  entity_type  text not null,
  entity_id    text not null,
  source_node  text not null,
  created_at   timestamptz not null default now()
);

-- Transactional outbox. Sequence is gapless and allocated under the same row
-- lock as the journal entry. One operation fans out into a summary entry, a
-- raw trajectory entry and one entry per device alarm; replication filters
-- each category against residency policy.
create table if not exists outbox_events (
  seq          bigint primary key,
  entity_type  text not null,
  entity_id    text not null,
  category     text not null check (category in ('raw_trajectory','operation_summary','device_alarm','aggregate_metric')),
  payload      jsonb not null,
  source_node  text not null,
  source_epoch bigint not null,
  created_at   timestamptz not null default now()
);
create index if not exists outbox_events_category_idx on outbox_events (category, seq);

-- In-region aggregate snapshots. Only these aggregates, never raw points, are
-- eligible for outbound reports.
create table if not exists metric_snapshots (
  snapshot_id  text primary key,
  period_from  timestamptz not null,
  period_to    timestamptz not null,
  data_cutoff  timestamptz not null,
  computed_at  timestamptz not null,
  metrics      jsonb not null,
  source_node  text not null,
  source_epoch bigint not null,
  outbox_seq   bigint not null
);
create index if not exists metric_snapshots_period_idx on metric_snapshots (period_from, period_to);

create table if not exists ingest_receipts (
  batch_id       text primary key,
  content_sha256 text not null,
  accepted       integer not null,
  duplicates     integer not null,
  first_journal  bigint,
  last_journal   bigint,
  created_at     timestamptz not null default now()
);

-- =====================================================================
-- Field operation sequence integrity data product (in-region only).
-- Every table below stays inside the regional database: field-level
-- trajectories of evidence never leave the region. Only stage-coverage
-- counts, derived inside a review run, are eligible for publication.
-- =====================================================================

-- Standardised operation facts used by sequence review. A fact is either
-- linked to an uploaded operation_events segment (source='uploaded') or
-- registered out-of-band (source='imported', e.g. a manual agronomy note).
-- event_at is when the work happened; received_at is when the region
-- learned about it. Late, offline backfill therefore has event_at earlier
-- than received_at and must never be treated as a reverse-order event.
create table if not exists sequence_facts (
  fact_id        text primary key,
  source         text not null check (source in ('uploaded','imported')),
  device_serial  text not null,
  segment_id     text not null,
  operation_type text not null check (operation_type in ('seeding','fertilizing','harvesting','other')),
  field_id       text not null default '',
  event_at       timestamptz not null,
  received_at    timestamptz not null default now(),
  completed      boolean not null default true,
  batch_id       text not null default '',
  detail         jsonb not null default '{}'::jsonb,
  created_at     timestamptz not null default now(),
  unique (device_serial, segment_id)
);
create index if not exists sequence_facts_event_idx on sequence_facts (event_at);
create index if not exists sequence_facts_field_idx on sequence_facts (field_id, event_at);

-- Field/crop-season attribution, versioned over time. A field season is the
-- review unit: one field, one crop, one season.
create table if not exists field_seasons (
  season_id    text primary key,
  field_id     text not null,
  crop_code    text not null,
  planned_from timestamptz not null,
  planned_to   timestamptz not null,
  status       text not null default 'active'
                 check (status in ('planned','active','closed','disputed')),
  note         text not null default '',
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now(),
  unique (field_id, season_id)
);
create index if not exists field_seasons_field_idx on field_seasons (field_id, planned_from);

-- Boundary revisions. Overlapping valid_from/valid_until windows for the
-- same field are an attribution conflict the review must surface, never
-- silently resolve.
create table if not exists field_boundary_revisions (
  revision_id   text primary key,
  field_id      text not null,
  valid_from    timestamptz not null,
  valid_until   timestamptz,
  boundary_label text not null default '',
  geometry      jsonb not null default '{}'::jsonb,
  superseded_by text not null default '',
  note          text not null default '',
  created_at    timestamptz not null default now()
);
create index if not exists field_boundary_revisions_field_idx
  on field_boundary_revisions (field_id, valid_from);

-- Crop calendar declarations: which crop was declared on a field during a
-- season, themselves versioned. Two crops valid at the same instant on the
-- same field are a mutually-exclusive-crop conflict.
create table if not exists crop_calendars (
  calendar_id text primary key,
  field_id    text not null,
  season_id   text not null references field_seasons(season_id) on delete cascade,
  crop_code   text not null,
  valid_from  timestamptz not null,
  valid_until timestamptz,
  note        text not null default '',
  created_at  timestamptz not null default now()
);
create index if not exists crop_calendars_field_idx on crop_calendars (field_id, valid_from);

-- Agronomy rule sets, versioned and engine-specific. rule_doc is the full
-- machine-readable rule definition (states/transitions for the FSM engine,
-- operations/preconditions/time constraints for the partial-order engine).
create table if not exists agronomy_rule_sets (
  rule_set_id text primary key,
  version     text not null,
  engine      text not null check (engine in ('fsm','partial_order')),
  crop_code   text not null default '',
  rule_doc    jsonb not null,
  status      text not null default 'active'
                check (status in ('draft','active','retired')),
  created_at  timestamptz not null default now(),
  unique (version, engine, crop_code)
);

-- One review run freezes one version combination: facts as of fact_cutoff,
-- the attribution tables and both rule sets at run time. Corrections create
-- a new run; old rows are never rewritten, so old summaries stay replayable.
create table if not exists sequence_review_runs (
  run_id              text primary key,
  region_code         text not null,
  status              text not null default 'completed',
  period_from         timestamptz not null,
  period_to           timestamptz not null,
  fsm_rule_set_id     text not null,
  po_rule_set_id      text not null,
  fact_version        text not null,
  attribution_version text not null,
  version_bundle      jsonb not null,
  fact_cutoff         timestamptz not null,
  note                text not null default '',
  created_by          text not null,
  created_at          timestamptz not null default now()
);
create index if not exists sequence_review_runs_created_idx
  on sequence_review_runs (created_at desc);

-- Per-field-season result of one run, for both engines.
create table if not exists sequence_review_fields (
  run_id               text not null references sequence_review_runs(run_id) on delete cascade,
  season_id            text not null,
  field_id             text not null,
  crop_code            text not null,
  attribution_status   text not null check (attribution_status in ('ok','conflict','pending_review')),
  rule_applicability   text not null check (rule_applicability in ('applicable','not_applicable')),
  fsm_state            text not null default '',
  fsm_status           text not null check (fsm_status in ('ok','conflict','pending_review')),
  po_status            text not null check (po_status in ('ok','conflict','pending_review')),
  fsm_confirmed_stages text[] not null default '{}',
  po_confirmed_stages  text[] not null default '{}',
  missing_prerequisites jsonb not null default '[]'::jsonb,
  allowed_paths        jsonb not null default '[]'::jsonb,
  pending_ranges       jsonb not null default '[]'::jsonb,
  summary              jsonb not null default '{}'::jsonb,
  primary key (run_id, season_id)
);
create index if not exists sequence_review_fields_field_idx
  on sequence_review_fields (run_id, field_id);

-- Per-fact interpretation inside a run, per engine. Late facts add new
-- interpretations; they never delete or negate an earlier confirmed one.
create table if not exists sequence_review_events (
  run_id          text not null references sequence_review_runs(run_id) on delete cascade,
  fact_id         text not null,
  season_id       text not null,
  engine          text not null check (engine in ('fsm','partial_order')),
  operation_type  text not null,
  event_at        timestamptz not null,
  received_at     timestamptz not null,
  interpretation  text not null check (interpretation in
                    ('confirmed','alternative','duplicate','conflict','pending_review','excluded')),
  detail          jsonb not null default '{}'::jsonb,
  primary key (run_id, fact_id, engine)
);
create index if not exists sequence_review_events_season_idx
  on sequence_review_events (run_id, season_id, engine);

-- Conflicts and pending-review scopes, per engine, with evidence.
create table if not exists sequence_review_issues (
  id         bigserial primary key,
  run_id     text not null references sequence_review_runs(run_id) on delete cascade,
  season_id  text not null,
  engine     text not null check (engine in ('fsm','partial_order','both')),
  severity   text not null check (severity in ('conflict','pending_review')),
  code       text not null,
  message    text not null,
  evidence   jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);
create index if not exists sequence_review_issues_run_idx
  on sequence_review_issues (run_id, engine, severity);

-- Engine-level rollup inside a run.
create table if not exists sequence_review_engines (
  run_id               text not null references sequence_review_runs(run_id) on delete cascade,
  engine               text not null check (engine in ('fsm','partial_order')),
  status               text not null check (status in ('ok','conflict','pending_review')),
  field_count          integer not null,
  conflict_count       integer not null,
  pending_count        integer not null,
  confirmed_stage_counts jsonb not null default '{}'::jsonb,
  primary key (run_id, engine)
);
