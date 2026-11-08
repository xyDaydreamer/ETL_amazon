#!/usr/bin/env bash
set -euo pipefail

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set airflow_password="$AIRFLOW_DB_PASSWORD" \
  --set movie_password="$MOVIE_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE airflow_app LOGIN PASSWORD %L', :'airflow_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_app')\gexec

SELECT format('CREATE ROLE movie_app LOGIN PASSWORD %L', :'movie_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'movie_app')\gexec

SELECT 'CREATE DATABASE airflow OWNER airflow_app'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE amazon_movies OWNER movie_app'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'amazon_movies')\gexec
SQL

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname amazon_movies \
  --file /opt/project/sql/001_core_schema.sql

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname amazon_movies \
  --file /opt/project/sql/002_stage1_review_ingest.sql

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname amazon_movies \
  --file /opt/project/sql/003_stage1_versioned_reparse.sql

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname amazon_movies \
  --file /opt/project/sql/004_stage2_full_ingest.sql

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname amazon_movies \
  --file /opt/project/sql/005_stage3_product_metadata.sql

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname amazon_movies \
  --file /opt/project/sql/006_stage4_first_movie_candidates.sql

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname amazon_movies \
  --file /opt/project/sql/007_stage4_cached_candidate_reevaluation.sql

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname amazon_movies \
  --file /opt/project/sql/008_stage4_v3_evidence_and_labels.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/009_stage4_human_decision_views.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/010_stage4_unified_mapping_adoption.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/011_stage5_imdb_snapshot_and_evaluation.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/012_stage5_selective_imdb_integration.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/013_stage5_scope_product_mappings.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/014_stage6_candidate_range.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/015_evaluation_supersession.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/016_stage7_query_views.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/017_collection_review_semantics.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/018_logical_review_deduplication.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/019_final_field_lineage_audit.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/020_p2_movie_expansion.sql
psql --username "$POSTGRES_USER" --dbname amazon_movies --set ON_ERROR_STOP=1 \
  --file /opt/project/sql/021_product_page_collection.sql

