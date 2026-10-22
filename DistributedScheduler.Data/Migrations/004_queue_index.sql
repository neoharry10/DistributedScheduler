CREATE INDEX ix_jobs_claim_queue ON jobs (queue, priority DESC, run_at) WHERE status = 0;
DROP INDEX ix_jobs_claim;