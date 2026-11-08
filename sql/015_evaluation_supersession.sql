\set ON_ERROR_STOP on
SET ROLE movie_app;

ALTER TABLE evaluation.dataset
    DROP CONSTRAINT IF EXISTS dataset_status_check;
ALTER TABLE evaluation.dataset
    ADD CONSTRAINT dataset_status_check CHECK (
        status IN ('draft','frozen','labeling','labeled','evaluated','superseded')
    );
ALTER TABLE evaluation.dataset
    ADD COLUMN IF NOT EXISTS superseded_by_dataset_id bigint REFERENCES evaluation.dataset(evaluation_dataset_id),
    ADD COLUMN IF NOT EXISTS superseded_reason text;

COMMENT ON COLUMN evaluation.dataset.superseded_by_dataset_id IS
    'Replacement frozen dataset. Superseded rows and predictions remain immutable history and must not be labeled or scored.';

RESET ROLE;
