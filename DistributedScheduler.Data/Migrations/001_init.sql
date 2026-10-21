CREATE TABLE jobs (
  id uuid PRIMARY KEY,
  type text NOT NULL,
  payload jsonb,
  queue text NOT NULL DEFAULT 'default',
  status smallint NOT NULL DEFAULT 0,
  priority int NOT NULL DEFAULT 0,
  run_at timestamptz NOT NULL DEFAULT now(),
  attempts int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 5,
  locked_by text,
  locked_until timestamptz,
  last_error text,
  idempotency_key text UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_jobs_claim ON jobs (priority DESC, run_at) WHERE status = 0;

CREATE TABLE job_effects (
  job_id uuid NOT NULL,
  handler text NOT NULL,
  executed_at timestamptz NOT NULL DEFAULT now()
);