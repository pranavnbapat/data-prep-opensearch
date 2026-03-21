# MySQL Control Plane

Related reference:

- [mysql_queries.md](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/docs/mysql_queries.md)

This is an optional layer on top of the existing file-based pipeline.

It adds four operational capabilities:

1. sync backend-core records into local MySQL
2. run a fast pipeline from MySQL-backed source records
3. defer expensive PDFs above a configured page threshold
4. export a fresh final-improved snapshot from MySQL

## Intended Flow

1. `POST /sync/backend-core`
2. `POST /pipeline/fast`
3. `POST /pipeline/deferred`
4. `POST /exports/final-improved`

## Endpoints

- `POST /sync/backend-core`
  - runs downloader against backend-core
  - stores normalized source docs in MySQL
  - marks hosted PDFs with page count above `FAST_PIPELINE_PDF_MAX_PAGES` as deferred
  - request fields:
    - `page_size`: downloader page size for backend-core metadata paging; `0` or omitted uses the default effective page size
    - `env_mode`: target environment bucket, usually `DEV` or `PRD`
    - `sort_criteria`: backend-core metadata API sort mode
    - `dl_workers`: downloader per-page processing worker count; `0` or omitted uses the configured default

- `GET /sync/backend-core/status`
  - returns recent sync jobs

- `POST /pipeline/fast`
  - selects non-deferred MySQL records
  - runs enricher and improver on that subset
  - writes processed docs back to MySQL

- `GET /pipeline/fast/status`
  - returns recent fast-path jobs

- `POST /pipeline/deferred`
  - selects deferred MySQL records
  - runs enricher and improver on that subset
  - writes processed docs back to MySQL

- `GET /pipeline/deferred/status`
  - returns recent deferred jobs

- `POST /records/{id}/reprocess`
  - forces a single record through the fast path

- `GET /records/{id}`
  - returns current MySQL state for one record

- `POST /exports/final-improved`
  - exports current MySQL state to `final_improved_mysql_export_<run_id>.json`

- `GET /exports/final-improved/latest`
  - returns the latest export path

## Key Environment Variables

- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `FAST_PIPELINE_PDF_MAX_PAGES`
- `MYSQL_SYNC_PDFINFO_TIMEOUT`

## Notes

- This does not replace the original file-based pipeline yet.
- The original `/run-pipeline` endpoint still works.
- MySQL endpoints require `PyMySQL` in the runtime environment.
- Stage artifacts are still written under `output/`.
- Schema bootstrap is automatic when the FastAPI app starts and MySQL is configured.
- Tables are created with `CREATE TABLE IF NOT EXISTS`, so existing tables are not recreated.
- The current design uses:
  - `ko_records` as the current-state table
  - `ko_records_history` as the history table
- Duplicate rows are prevented by the `(llid, env_mode)` primary key.
- If the incoming source fingerprint is unchanged, sync updates status/timestamps only and does not create a duplicate history entry.
- If the incoming processed fingerprint is unchanged, processed state updates status/timestamps only and does not create a duplicate history entry.
- When source or processed state changes, the previous row state is archived into `ko_records_history` before the current row is updated.
