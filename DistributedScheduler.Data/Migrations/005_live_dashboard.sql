ALTER TABLE jobs ADD COLUMN finished_at timestamptz;
CREATE INDEX ix_jobs_finished ON jobs (finished_at) WHERE finished_at IS NOT NULL;

CREATE FUNCTION set_finished_at() RETURNS trigger AS $$
BEGIN
  IF NEW.status IN (2, 3, 4, 5) THEN NEW.finished_at := now();
  ELSE NEW.finished_at := NULL;     -- requeued or retried: not finished anymore
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_finished_at
  BEFORE UPDATE OF status ON jobs
  FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status)
  EXECUTE FUNCTION set_finished_at();

-- The payload is empty on purpose: the UI just reloads. Identical notifications inside one
-- transaction are merged, so a 100-row UPDATE sends one notification.
CREATE FUNCTION notify_jobs_changed() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('jobs_changed', '');
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_insert
  AFTER INSERT ON jobs
  FOR EACH ROW EXECUTE FUNCTION notify_jobs_changed();

CREATE TRIGGER trg_jobs_status
  AFTER UPDATE OF status ON jobs
  FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status)
  EXECUTE FUNCTION notify_jobs_changed();