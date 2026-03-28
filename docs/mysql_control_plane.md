# MySQL Control Plane

Related reference:

- [mysql_queries.md](mysql_queries.md)

This is an optional layer on top of the existing file-based pipeline.

It adds five operational capabilities:

1. sync backend-core records into local MySQL
2. run a fast pipeline from MySQL-backed source records
3. defer expensive PDFs above a configured page threshold
4. export a fresh final-improved snapshot from MySQL
5. keep ingest-ineligible resources in MySQL while excluding them from processing

## Intended Flow

1. `POST /sync/backend-core`
2. `POST /pipeline/fast`
3. `POST /pipeline/deferred`
4. `POST /exports/final-improved`

## Full Workflow

```text
                                   MYSQL CONTROL PLANE
                                   ===================

  ┌──────────────────────────────┐
  │ POST /sync/backend-core      │
  └──────────────┬───────────────┘
                 │
                 v
      ┌──────────────────────────────┐
      │ Downloader reads backend-core│
      │ page by page                 │
      └──────────────┬───────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ For each record                                              │
      │ - normalize metadata                                         │
      │ - keep backend-provided fields like license/title/etc        │
      │ - fetch ko_content_flat from backend content API             │
      │ - set enrich_via route family based on KO type               │
      │ - compute source_fp / content_fp / field hashes              │
      └──────────────┬───────────────────────────────────────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ If hosted PDF                                                │
      │ - try to fetch PDF bytes                                     │
      │ - try to compute pdf_page_count                              │
      │ - apply processing eligibility checks                        │
      │   * all files <= 1 GiB                                       │
      │   * PDFs <= 100 pages                                        │
      │   * Office docs <= 100 converted pages/slides                │
      │   * text files <= 5 MB and <= 500000 chars extracted         │
      │   * images <= 10000 px on either side                        │
      │   * audio/video <= 3000 s                                    │
      │ - if page_count > FAST_PIPELINE_PDF_MAX_PAGES                │
      │     => is_deferred = 1                                       │
      │ - else                                                       │
      │     => is_deferred = 0                                       │
      │ - if any eligibility check fails                             │
      │     => processing_eligible = 0                               │
      │        processing_ineligible_reason = <reason>               │
      │ If pdf probe fails                                           │
      │ - record is still stored                                     │
      │ - pdf_page_count = NULL                                      │
      │ - is_deferred = 0                                            │
      └──────────────┬───────────────────────────────────────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ Upsert into ko_records                                       │
      │ - source_doc_json written incrementally page by page         │
      │ - current_doc_json preserved if already present              │
      │ - source changes archived into ko_records_history            │
      └──────────────────────────────────────────────────────────────┘


  ┌──────────────────────────────┐
  │ POST /pipeline/fast          │
  └──────────────┬───────────────┘
                 │
                 v
      ┌──────────────────────────────────────────────────────────────┐
      │ Select rows from ko_records where                            │
      │ - env_mode matches                                           │
      │ - is_deferred = 0                                            │
      │ - processing_eligible = 1                                    │
      │ - optional llids/max_docs filter                             │
      └──────────────┬───────────────────────────────────────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ For each selected row                                        │
      │ - load source_doc_json                                       │
      │ - load current_doc_json as previous state                    │
      │ - if current_doc_json already has                            │
      │     improved = 1 and non-empty ko_content_flat_summarised    │
      │     and source/enrichment fingerprint unchanged              │
      │     => skip whole record                                     │
      └──────────────┬───────────────────────────────────────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ Enricher phase (per record)                                  │
      │                                                              │
      │ URL/page KO                                                  │
      │ - pagesense                                                  │
      │ - writes ko_content_flat                                     │
      │ - enrich_via / ko_content_source = pagesense                 │
      │                                                              │
      │ Hosted PDF/document KO                                       │
      │ - starts in pagesense route family                           │
      │ - may prefer vision path                                     │
      │ - if vision succeeds                                         │
      │     writes ko_content_flat_vision                            │
      │     ko_content_is_summary = 1                                │
      │     enrich_via / ko_content_source = vision_model            │
      │ - if pagesense wins                                          │
      │     writes ko_content_flat                                   │
      │     enrich_via / ko_content_source = pagesense               │
      │ - if upstream text is kept                                   │
      │     enrich_via / ko_content_source = upstream_preserved      │
      │                                                              │
      │ Image KO                                                     │
      │ - vision model path                                          │
      │ - enrich_via / ko_content_source = vision_model              │
      │                                                              │
      │ Audio/video KO                                               │
      │ - api_transcribe or custom_transcribe                        │
      │ - writes ko_content_flat                                     │
      │ - enrich_via / ko_content_source = api_transcribe            │
      │   or custom_transcribe                                       │
      └──────────────┬───────────────────────────────────────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ Write enriched state immediately to ko_records.current_doc_json│
      │ - stage = enriched                                           │
      │ - enriched_at updated                                        │
      │ - history row created if current state changed               │
      └──────────────┬───────────────────────────────────────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ Improver phase (per record)                                  │
      │ - if ko_content_is_summary = 1 and ko_content_flat_vision    │
      │     => light polish from vision summary                      │
      │ - else if ko_content_flat exists                             │
      │     => summarise ko_content_flat                             │
      │ - then generate title_llm / subtitle_llm /                   │
      │   description_llm / keywords_llm when summary is substantive │
      └──────────────┬───────────────────────────────────────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ Write improved state immediately to ko_records.current_doc_json│
      │ - ko_content_flat_summarised                                 │
      │ - ko_content_flat_summarised_stats                           │
      │ - *_llm metadata fields                                      │
      │ - improved = 1                                               │
      │ - improved_at updated                                        │
      │ - fast_pipeline_status = success                             │
      └──────────────────────────────────────────────────────────────┘


  ┌──────────────────────────────┐
  │ POST /pipeline/deferred      │
  └──────────────┬───────────────┘
                 │
                 v
      ┌──────────────────────────────────────────────────────────────┐
      │ Same per-record logic as /pipeline/fast, but selection is:   │
      │ - is_deferred = 1                                            │
      │ - processing_eligible = 1                                    │
      │ Typically these are hosted PDFs above                        │
      │ FAST_PIPELINE_PDF_MAX_PAGES                                  │
      └──────────────┬───────────────────────────────────────────────┘
                     │
                     v
      ┌──────────────────────────────────────────────────────────────┐
      │ Deferred rows usually take the expensive PDF vision path     │
      │ When improved write succeeds:                                │
      │ - deferred_pipeline_status = success                         │
      │ - background_completed_at updated                            │
      └──────────────────────────────────────────────────────────────┘


  ┌──────────────────────────────┐
  │ POST /pipeline/improver-fallback │
  └──────────────┬──────────────────┘
                 │
                 v
      ┌──────────────────────────────────────────────────────────────┐
      │ Select rows (intended for deferred rows) where               │
      │ current_doc_json lacks ko_content_flat_summarised            │
      │                                                              │
      │ For each row                                                 │
      │ - start from source_doc_json                                 │
      │ - overlay current_doc_json if present                        │
      │ - if ko_content_flat missing in current state                │
      │     fall back to source_doc_json.ko_content_flat             │
      │ - run improver only                                          │
      │ - write improved result to current_doc_json                  │
      └──────────────────────────────────────────────────────────────┘


  ┌──────────────────────────────┐
  │ POST /exports/final-improved │
  └──────────────┬───────────────┘
                 │
                 v
      ┌──────────────────────────────────────────────────────────────┐
      │ Export builder reads ko_records                              │
      │ - if current_doc_json exists => export that                  │
      │ - else => fall back to source_doc_json                       │
      │ - write final_improved_mysql_export_<run_id>.json            │
      └──────────────────────────────────────────────────────────────┘


  ┌──────────────────────────────────────────────────────────────────┐
  │ Resulting operational model                                      │
  │ - sync stores all rows                                           │
  │ - fast processes non-deferred rows                               │
  │ - deferred processes expensive PDF rows later                    │
  │ - improver-fallback can create interim summaries from source text│
  │ - export always produces one full snapshot of the corpus         │
  └──────────────────────────────────────────────────────────────────┘
```

## Step-by-Step Notes

### Fast Pipeline

1. Select only `is_deferred = 0` rows.
2. Select only `processing_eligible = 1` rows.
3. Skip rows that already have a completed unchanged summary.
4. Enrich each remaining row.
5. Write enriched `current_doc_json` immediately.
6. Improve the same row.
7. Write improved `current_doc_json` immediately.

### Deferred Pipeline

1. Select only `is_deferred = 1` rows.
2. Select only `processing_eligible = 1` rows.
3. Apply the same skip logic for already completed unchanged rows.
4. Enrich each remaining row, usually using the heavier PDF vision path.
5. Write enriched `current_doc_json` immediately.
6. Improve the same row.
7. Write improved `current_doc_json` immediately and mark background completion.

### Export

1. Read all rows from `ko_records`, or only processed rows if `processed_only = true`.
2. Prefer `current_doc_json`.
3. Fall back to `source_doc_json` if `current_doc_json` is absent.
4. Write one new export file under `output/<ENV>/<YYYY>/<MM>/`.

## Endpoints

- `POST /sync/backend-core`
  - runs downloader against backend-core
  - stores normalized source docs in MySQL
  - marks hosted PDFs with page count above `FAST_PIPELINE_PDF_MAX_PAGES` as deferred
  - computes `processing_eligible` and `processing_ineligible_reason` using ingestion-aligned limits
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

- `POST /pipeline/improver-fallback`
  - processes records through improver only, without running enricher first
  - if `current_doc_json` already has `ko_content_flat_summarised`, the record is skipped
  - falls back to `source_doc_json.ko_content_flat` when needed
  - intended for deferred records

- `GET /pipeline/improver-fallback/status`
  - returns recent improver-fallback jobs

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
- `MYSQL_CONNECT_TIMEOUT`
- `PROCESSING_MAX_FILE_SIZE_BYTES`
- `PROCESSING_MAX_PDF_PAGES`
- `PROCESSING_MAX_OFFICE_PAGES`
- `PROCESSING_MAX_TEXT_FILE_BYTES`
- `PROCESSING_MAX_TEXT_CHARS`
- `PROCESSING_MAX_IMAGE_DIM_PX`
- `PROCESSING_MAX_MEDIA_DURATION_SEC`
- `PROCESSING_PROBE_TIMEOUT`
- `PROCESSING_PROBE_RETRIES`
- `PROCESSING_PROBE_BACKOFF_SEC`

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
- The MySQL current-state table also stores:
  - `processing_eligible`
  - `processing_ineligible_reason`
- Duplicate rows are prevented by the `(llid, env_mode)` primary key.
- Ingest-ineligible rows are still synced into MySQL, but are excluded from fast/deferred/improver-fallback processing.
- If the incoming source fingerprint is unchanged, sync updates status/timestamps only and does not create a duplicate history entry.
- If the incoming processed fingerprint is unchanged, processed state updates status/timestamps only and does not create a duplicate history entry.
- When source or processed state changes, the previous row state is archived into `ko_records_history` before the current row is updated.
