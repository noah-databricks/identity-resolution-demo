CREATE SCHEMA IF NOT EXISTS steward;

CREATE TABLE IF NOT EXISTS steward.review_cases (
  case_id TEXT PRIMARY KEY,
  source_run_id TEXT NOT NULL,
  version BIGINT NOT NULL CHECK (version > 0),
  status TEXT NOT NULL DEFAULT 'REVIEW' CHECK (status IN ('REVIEW','RESOLVED','DEFERRED')),
  payload JSONB NOT NULL,
  search_text TEXT NOT NULL,
  pipeline_confidence DOUBLE PRECISION,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS review_queue_age ON steward.review_cases(status, imported_at, case_id);

CREATE TABLE IF NOT EXISTS steward.proposals (
  proposal_id TEXT PRIMARY KEY,
  case_id TEXT NOT NULL REFERENCES steward.review_cases(case_id),
  payload JSONB NOT NULL,
  confirmation_digest TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS steward.decisions (
  decision_id TEXT PRIMARY KEY,
  request_id TEXT UNIQUE NOT NULL,
  proposal_id TEXT UNIQUE NOT NULL REFERENCES steward.proposals(proposal_id),
  case_id TEXT NOT NULL REFERENCES steward.review_cases(case_id),
  actor TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('manual','confirm','auto')),
  payload JSONB NOT NULL,
  previous_groups JSONB NOT NULL,
  applied_case_version BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS steward.outbox (
  event_id TEXT PRIMARY KEY REFERENCES steward.decisions(decision_id),
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS steward.agent_runs (
  run_id TEXT PRIMARY KEY,
  case_id TEXT NOT NULL REFERENCES steward.review_cases(case_id),
  actor TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('confirm','auto')),
  status TEXT NOT NULL CHECK (status IN ('QUEUED','RUNNING','COMPLETED','FAILED')),
  trace_id TEXT,
  error_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS steward.agent_events (
  sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES steward.agent_runs(run_id),
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS agent_event_cursor ON steward.agent_events(run_id, sequence);

-- Additive migrations preserve existing requests and review history.
ALTER TABLE steward.agent_runs ADD COLUMN IF NOT EXISTS request_id TEXT;
ALTER TABLE steward.agent_runs ADD COLUMN IF NOT EXISTS case_snapshot JSONB;
ALTER TABLE steward.agent_runs ADD COLUMN IF NOT EXISTS message TEXT;
ALTER TABLE steward.agent_runs ADD COLUMN IF NOT EXISTS proposal_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS agent_request_idempotency
  ON steward.agent_runs(actor, request_id) WHERE request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS agent_queue ON steward.agent_runs(status, created_at);

-- Imported case payload, kept so a demo reset can restore it exactly.
ALTER TABLE steward.review_cases ADD COLUMN IF NOT EXISTS baseline_payload JSONB;

CREATE TABLE IF NOT EXISTS steward.import_batches (
  source_run_id TEXT PRIMARY KEY,
  content_digest TEXT NOT NULL,
  release_manifest JSONB NOT NULL,
  summary JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Golden-field survivorship: tracks which source record was chosen for each field
-- in each resulting guest (steward-identified customer).
CREATE TABLE IF NOT EXISTS steward.reviewed_golden_fields (
  decision_id TEXT NOT NULL REFERENCES steward.decisions(decision_id),
  case_id TEXT NOT NULL REFERENCES steward.review_cases(case_id),
  source_run_id TEXT NOT NULL,
  applied_case_version BIGINT NOT NULL,
  guest_identity TEXT NOT NULL,  -- steward-<digest> customer_id
  field_name TEXT NOT NULL,  -- e.g., 'name', 'email', 'date_of_birth'
  chosen_record_key TEXT NOT NULL,  -- which source record the value comes from
  field_value TEXT NOT NULL,  -- the actual value chosen
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (decision_id, guest_identity, field_name)
);

-- Release reconciliation (import_release.py --replace-release). Exactly one
-- active release; a replaced release keeps its ledger row, marked inactive.
ALTER TABLE steward.import_batches ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE steward.import_batches ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMPTZ;
ALTER TABLE steward.import_batches ADD COLUMN IF NOT EXISTS superseded_by_run_id TEXT;
ALTER TABLE steward.import_batches ADD COLUMN IF NOT EXISTS replaces_run_id TEXT;
ALTER TABLE steward.import_batches ADD COLUMN IF NOT EXISTS archive_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS one_active_release
  ON steward.import_batches (active) WHERE active;

-- One batch per replaced release: the prior ledger row plus the row count and
-- content digest of every archived table, proven equal to the live rows.
CREATE TABLE IF NOT EXISTS steward.release_archives (
  archive_id TEXT PRIMARY KEY,
  source_run_id TEXT NOT NULL UNIQUE,
  superseded_by_run_id TEXT NOT NULL,
  prior_ledger JSONB NOT NULL,
  row_counts JSONB NOT NULL,
  row_digests JSONB NOT NULL,
  archived_by TEXT NOT NULL DEFAULT current_user,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Every archived row, verbatim (to_jsonb of the live row), so later additive
-- migrations never break the archive. Query with row_data->>'column'.
CREATE TABLE IF NOT EXISTS steward.release_archive_rows (
  archive_id TEXT NOT NULL REFERENCES steward.release_archives(archive_id),
  table_name TEXT NOT NULL CHECK (table_name IN ('review_cases','proposals','decisions','outbox',
    'reviewed_golden_fields','agent_runs','agent_events')),
  row_key TEXT NOT NULL,
  row_data JSONB NOT NULL,
  PRIMARY KEY (archive_id, table_name, row_key)
);
CREATE INDEX IF NOT EXISTS release_archive_rows_case
  ON steward.release_archive_rows ((row_data->>'case_id'));

-- Queue ordering by score for larger releases.
CREATE INDEX IF NOT EXISTS review_queue_confidence
  ON steward.review_cases(status, pipeline_confidence DESC NULLS LAST, case_id);

-- Why each case is in review: release metadata derived read-only from the
-- resolver's frozen tables by import_review_context.py (graph-withheld links and
-- reason codes for queue tags). Not case state: reset and decisions leave it alone,
-- and it is keyed by release so a replaced release keeps its own rows.
CREATE TABLE IF NOT EXISTS steward.case_review_context (
  source_run_id TEXT NOT NULL,
  case_id TEXT NOT NULL,
  graph_links JSONB NOT NULL DEFAULT '[]'::jsonb,
  reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
  provenance JSONB NOT NULL,
  derived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_run_id, case_id)
);

-- Tables created by this principal join the shared owner role, so the operator
-- and the app principal stay co-owners (see regrant_app_principal.py).
DO $$
DECLARE t record;
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'steward_owner')
     AND pg_has_role(current_user, 'steward_owner', 'MEMBER') THEN
    FOR t IN SELECT tablename FROM pg_tables
             WHERE schemaname = 'steward' AND tableowner = current_user LOOP
      EXECUTE format('ALTER TABLE steward.%I OWNER TO steward_owner', t.tablename);
    END LOOP;
  END IF;
END $$;
