-- 002 resumable runs: progress, pause/resume fencing and persisted replicates.
-- Fresh deployments already get this shape from SQLAlchemy metadata; apply these
-- statements only to a database created before this change.
ALTER TABLE diversity_runs ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE diversity_runs ADD COLUMN completed_replicates INTEGER NOT NULL DEFAULT 0;
ALTER TABLE diversity_runs ADD COLUMN total_replicates INTEGER NOT NULL DEFAULT 0;
ALTER TABLE diversity_runs ADD COLUMN params_fingerprint VARCHAR(64);
CREATE TABLE run_samples (
  run_id INTEGER NOT NULL REFERENCES diversity_runs(id),
  transect_id INTEGER NOT NULL,
  replicate_index INTEGER NOT NULL,
  shannon DOUBLE PRECISION NOT NULL,
  simpson DOUBLE PRECISION NOT NULL,
  rarefied DOUBLE PRECISION NOT NULL,
  CONSTRAINT run_samples_pkey PRIMARY KEY (run_id, transect_id, replicate_index)
);
