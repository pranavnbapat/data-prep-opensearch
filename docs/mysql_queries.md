# MySQL Queries

Useful ad hoc queries for inspecting `ko_records` and `ko_records_history`.

## Top 5

1. Records processed per day:

```sql
SELECT
  DATE(improved_at) AS processed_date,
  COUNT(*) AS records_processed
FROM ko_records
WHERE improved_at IS NOT NULL
GROUP BY DATE(improved_at)
ORDER BY processed_date DESC;
```

2. Fast vs deferred processed per day:

```sql
SELECT
  DATE(improved_at) AS processed_date,
  SUM(CASE WHEN is_deferred = 0 THEN 1 ELSE 0 END) AS fast_records,
  SUM(CASE WHEN is_deferred = 1 THEN 1 ELSE 0 END) AS deferred_records,
  COUNT(*) AS total_records
FROM ko_records
WHERE improved_at IS NOT NULL
GROUP BY DATE(improved_at)
ORDER BY processed_date DESC;
```

3. Rows still missing processed state:

```sql
SELECT
  llid,
  env_mode,
  is_deferred,
  sync_status,
  fast_pipeline_status,
  deferred_pipeline_status
FROM ko_records
WHERE current_doc_json IS NULL
ORDER BY synced_at ASC;
```

4. Rows with current state but still missing summary:

```sql
SELECT
  llid,
  env_mode,
  is_deferred,
  improved_at
FROM ko_records
WHERE current_doc_json IS NOT NULL
  AND JSON_UNQUOTE(JSON_EXTRACT(current_doc_json, '$.ko_content_flat_summarised')) IS NULL
ORDER BY updated_at DESC;
```

5. Deferred queue still pending:

```sql
SELECT
  llid,
  pdf_page_count,
  deferred_reason,
  deferred_pipeline_status,
  background_completed_at
FROM ko_records
WHERE is_deferred = 1
  AND (deferred_pipeline_status IS NULL OR deferred_pipeline_status <> 'success')
ORDER BY pdf_page_count DESC, synced_at ASC;
```

## Records Processed Per Day

Processed per day using `improved_at`:

```sql
SELECT
  DATE(improved_at) AS processed_date,
  COUNT(*) AS records_processed
FROM ko_records
WHERE improved_at IS NOT NULL
GROUP BY DATE(improved_at)
ORDER BY processed_date DESC;
```

Fast vs deferred processed per day:

```sql
SELECT
  DATE(improved_at) AS processed_date,
  SUM(CASE WHEN is_deferred = 0 THEN 1 ELSE 0 END) AS fast_records,
  SUM(CASE WHEN is_deferred = 1 THEN 1 ELSE 0 END) AS deferred_records,
  COUNT(*) AS total_records
FROM ko_records
WHERE improved_at IS NOT NULL
GROUP BY DATE(improved_at)
ORDER BY processed_date DESC;
```

Enriched per day using `enriched_at`:

```sql
SELECT
  DATE(enriched_at) AS enriched_date,
  COUNT(*) AS records_enriched
FROM ko_records
WHERE enriched_at IS NOT NULL
GROUP BY DATE(enriched_at)
ORDER BY enriched_date DESC;
```

Deferred/background completions per day:

```sql
SELECT
  DATE(background_completed_at) AS background_date,
  COUNT(*) AS deferred_completed
FROM ko_records
WHERE background_completed_at IS NOT NULL
GROUP BY DATE(background_completed_at)
ORDER BY background_date DESC;
```

## Current Status Overview

Overall counts:

```sql
SELECT
  COUNT(*) AS total_records,
  SUM(CASE WHEN is_deferred = 1 THEN 1 ELSE 0 END) AS deferred_records,
  SUM(CASE WHEN current_doc_json IS NOT NULL THEN 1 ELSE 0 END) AS current_docs_present,
  SUM(CASE WHEN improved_at IS NOT NULL THEN 1 ELSE 0 END) AS improved_records
FROM ko_records;
```

By pipeline status:

```sql
SELECT
  fast_pipeline_status,
  deferred_pipeline_status,
  COUNT(*) AS records_count
FROM ko_records
GROUP BY fast_pipeline_status, deferred_pipeline_status
ORDER BY records_count DESC;
```

Fast vs deferred split:

```sql
SELECT
  is_deferred,
  COUNT(*) AS records_count
FROM ko_records
GROUP BY is_deferred;
```

## Incomplete Records

Rows with no processed state yet:

```sql
SELECT
  llid,
  env_mode,
  is_deferred,
  sync_status,
  fast_pipeline_status,
  deferred_pipeline_status
FROM ko_records
WHERE current_doc_json IS NULL
ORDER BY synced_at ASC;
```

Rows with current state but missing summary:

```sql
SELECT
  llid,
  env_mode,
  is_deferred,
  improved_at
FROM ko_records
WHERE current_doc_json IS NOT NULL
  AND JSON_UNQUOTE(JSON_EXTRACT(current_doc_json, '$.ko_content_flat_summarised')) IS NULL
ORDER BY updated_at DESC;
```

Rows that have summary already:

```sql
SELECT
  llid,
  env_mode,
  is_deferred,
  improved_at
FROM ko_records
WHERE JSON_UNQUOTE(JSON_EXTRACT(current_doc_json, '$.ko_content_flat_summarised')) IS NOT NULL
ORDER BY improved_at DESC;
```

## Deferred Queue Inspection

Deferred rows still pending:

```sql
SELECT
  llid,
  pdf_page_count,
  deferred_reason,
  deferred_pipeline_status,
  background_completed_at
FROM ko_records
WHERE is_deferred = 1
  AND (deferred_pipeline_status IS NULL OR deferred_pipeline_status <> 'success')
ORDER BY pdf_page_count DESC, synced_at ASC;
```

Deferred rows completed:

```sql
SELECT
  llid,
  pdf_page_count,
  deferred_reason,
  background_completed_at
FROM ko_records
WHERE is_deferred = 1
  AND deferred_pipeline_status = 'success'
ORDER BY background_completed_at DESC;
```

Deferred rows grouped by page count:

```sql
SELECT
  pdf_page_count,
  COUNT(*) AS records_count
FROM ko_records
WHERE is_deferred = 1
GROUP BY pdf_page_count
ORDER BY pdf_page_count DESC;
```

## Source vs Current State

Rows where processed state exists:

```sql
SELECT
  llid,
  source_fp,
  current_fp,
  synced_at,
  updated_at
FROM ko_records
WHERE current_doc_json IS NOT NULL
ORDER BY updated_at DESC;
```

Rows where processed state falls back to source in export:

```sql
SELECT
  llid,
  is_deferred,
  synced_at
FROM ko_records
WHERE current_doc_json IS NULL
ORDER BY synced_at ASC;
```

## History Table

History rows per record:

```sql
SELECT
  llid,
  env_mode,
  COUNT(*) AS history_rows
FROM ko_records_history
GROUP BY llid, env_mode
ORDER BY history_rows DESC, llid ASC;
```

Recent history events:

```sql
SELECT
  llid,
  env_mode,
  history_kind,
  archived_at
FROM ko_records_history
ORDER BY archived_at DESC
LIMIT 100;
```

## Export Sanity Checks

How many rows would be exported with `processed_only = true`:

```sql
SELECT COUNT(*) AS processed_export_count
FROM ko_records
WHERE current_doc_json IS NOT NULL;
```

How many rows would be exported in full snapshot mode:

```sql
SELECT COUNT(*) AS full_export_count
FROM ko_records;
```
