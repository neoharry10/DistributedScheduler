ALTER TABLE jobs
  ADD COLUMN recurring_id text,
  ADD COLUMN scheduled_for timestamptz;

-- NULLs are distinct in unique indexes, so normal jobs are unaffected.
CREATE UNIQUE INDEX ux_jobs_recurring_slot ON jobs (recurring_id, scheduled_for);

CREATE TABLE recurring_jobs (
  id text PRIMARY KEY,
  type text NOT NULL,
  payload jsonb,
  queue text NOT NULL DEFAULT 'default',
  cron text NOT NULL,
  time_zone text NOT NULL DEFAULT 'UTC',
  enabled boolean NOT NULL DEFAULT true,
  next_run_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ix_recurring_due ON recurring_jobs (next_run_at) WHERE enabled;