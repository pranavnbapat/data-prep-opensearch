# db_bootstrap.py

from __future__ import annotations

import os
import logging

import pymysql

logger = logging.getLogger(__name__)

# --- Tables DDL (idempotent via IF NOT EXISTS) ---

DDL_PIPELINE_RUNS = r"""
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id            VARCHAR(32)  NOT NULL,
  env_mode          VARCHAR(16)  NOT NULL,
  stage             VARCHAR(32)  NOT NULL,
  created_at        DATETIME(6)  NOT NULL,

  emitted           INT          NOT NULL DEFAULT 0,
  dropped           INT          NOT NULL DEFAULT 0,
  changed           INT          NOT NULL DEFAULT 0,
  unchanged_reused  INT          NOT NULL DEFAULT 0,
  url_tasks         INT          NOT NULL DEFAULT 0,
  media_tasks       INT          NOT NULL DEFAULT 0,
  elapsed_sec       DECIMAL(10,3) NOT NULL DEFAULT 0.000,

  stats_json        JSON         NULL,

  inserted_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id),
  KEY idx_runs_env_stage_created (env_mode, stage, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

DDL_KNOWLEDGE_OBJECTS = r"""
CREATE TABLE IF NOT EXISTS knowledge_objects (
  id                 BIGINT       NOT NULL AUTO_INCREMENT,

  mongo_id            VARCHAR(24)   NULL,
  at_id               VARCHAR(2048) NOT NULL,

  title               TEXT          NULL,
  title_llm           TEXT          NULL,

  subtitle            TEXT          NULL,
  subtitle_llm        TEXT          NULL,

  description         TEXT          NULL,
  description_llm     TEXT          NULL,

  category            VARCHAR(64)   NULL,
  category_llm        VARCHAR(64)   NULL,

  license             VARCHAR(64)   NULL,

  project_id          CHAR(64)      NULL,
  project_name        TEXT          NULL,
  project_acronym     VARCHAR(128)  NULL,
  project_url         VARCHAR(2048) NULL,
  project_doi         VARCHAR(255)  NULL,
  project_type        VARCHAR(128)  NULL,

  ko_object_name      TEXT          NULL,
  ko_object_extension VARCHAR(32)   NULL,
  ko_object_size      BIGINT        NULL,
  ko_object_mimetype  VARCHAR(128)  NULL,
  ko_upload_source    VARCHAR(64)   NULL,
  ko_file_id          VARCHAR(2048) NULL,
  ko_is_hosted        TINYINT(1)    NULL,

  date_of_completion  DATE          NULL,
  ko_created_at       DATETIME(6)   NULL,
  ko_updated_at       DATETIME(6)   NULL,
  proj_created_at     DATETIME(6)   NULL,
  proj_updated_at     DATETIME(6)   NULL,

  keywords            JSON          NULL,
  keywords_llm        JSON          NULL,

  creators            JSON          NULL,
  languages           JSON          NULL,

  intended_purposes   JSON          NULL,
  intended_purposes_llm JSON        NULL,

  locations           JSON          NULL,
  locations_flat      JSON          NULL,

  topics              JSON          NULL,
  topics_llm          JSON          NULL,

  themes              JSON          NULL,
  themes_llm          JSON          NULL,

  subcategories       JSON          NULL,
  subcategories_llm   JSON          NULL,

  ko_content_flat     LONGTEXT      NULL,
  ko_content_flat_summarised LONGTEXT NULL,

  enriched            TINYINT(1)    NOT NULL DEFAULT 0,
  improved            TINYINT(1)    NOT NULL DEFAULT 0,

  dl_fp               CHAR(64)      NULL,
  enricher_fp         CHAR(64)      NULL,
  improver_fp         CHAR(64)      NULL,
  fields_fp           CHAR(64)      NULL,
  field_hashes        JSON          NULL,

  first_seen_run_id   VARCHAR(32)   NULL,
  last_seen_run_id    VARCHAR(32)   NULL,

  inserted_at         TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at          TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (id),

  UNIQUE KEY uq_at_id (at_id(512)),
  KEY idx_project_id (project_id),
  KEY idx_category (category),
  KEY idx_mimetype (ko_object_mimetype),
  KEY idx_fields_fp (fields_fp),
  KEY idx_last_seen_run (last_seen_run_id),

  CONSTRAINT fk_first_seen_run
    FOREIGN KEY (first_seen_run_id) REFERENCES pipeline_runs(run_id)
    ON UPDATE CASCADE ON DELETE SET NULL,

  CONSTRAINT fk_last_seen_run
    FOREIGN KEY (last_seen_run_id) REFERENCES pipeline_runs(run_id)
    ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

def ensure_tables_exist() -> None:
    """
    Connects to MySQL and creates tables if missing.
    Safe to call on every container start (DDL is IF NOT EXISTS).
    """
    host = os.getenv("MYSQL_HOST", "data-prep-opensearch-mysql")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    db = os.getenv("MYSQL_DATABASE", "").strip()
    user = os.getenv("MYSQL_USER", "").strip()
    pwd = os.getenv("MYSQL_PASSWORD", "")

    if not db or not user or not pwd:
        raise RuntimeError("Missing MYSQL_DATABASE / MYSQL_USER / MYSQL_PASSWORD in environment.")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=pwd,
        database=db,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(DDL_PIPELINE_RUNS)
            cur.execute(DDL_KNOWLEDGE_OBJECTS)
        logger.info("MySQL tables ensured (pipeline_runs, knowledge_objects).")
    finally:
        conn.close()
