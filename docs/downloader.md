# Downloader Stage

## Purpose

The downloader is the source-of-truth ingestion stage. It fetches knowledge objects from the backend API, normalizes them, drops records that are structurally unusable, and builds the current working snapshot for downstream stages.

## High-Level Flow

```text
+-------------------+
| Backend API       |
| documents/projects|
+---------+---------+
          |
          v
+-------------------+
| Fetch paginated   |
| KO metadata       |
+---------+---------+
          |
          v
+-------------------+
| Normalize fields  |
| clean metadata    |
+---------+---------+
          |
          v
+-------------------+
| Validate required |
| structural fields |
+----+---------+----+
     |         |
     | valid   | invalid
     v         v
+-------------------+    +-------------------+
| Compute source    |    | dropped_kos.json  |
| fingerprint       |    | with reason       |
+---------+---------+    +-------------------+
          |
          v
+-------------------+
| Compare with      |
| previous snapshot |
+----+---------+----+
     |         |
     | same    | new/updated
     v         v
+-------------------+
| reuse previous    |
| doc payload       |
+---------+---------+
          |
          v
+-------------------+
| Emit final_output |
| + latest pointer  |
+-------------------+
```

## Main Logic

1. Load backend credentials for `DEV` or `PRD`.
2. Fetch paginated KO metadata from the backend.
3. Normalize and clean fields into the project's internal document shape.
4. Apply hard validation for structurally required fields such as KO id and project id.
5. Normalize `date_of_completion`:
   - prefer `date_of_completion`
   - then `dateCreated`
   - then `date`
   - then created/updated timestamps
   - if still missing, keep the record and mark it as missing
6. Compute a stable source fingerprint per document.
7. Compare against the previous snapshot by logical id.
8. Split records into:
   - `new_added`
   - `updated`
   - `unchanged_reused`
   - `dropped`
   - `removed_from_source`
9. Write:
   - `final_output_<run_id>.json`
   - `latest_downloaded.json`

## Output Contract

The downloader emits a full snapshot, not just changed documents.

Important fields:
- `docs`
- `url_tasks`
- `media_tasks`
- stats:
  - `source_seen`
  - `emitted`
  - `new_added`
  - `updated`
  - `unchanged_reused`
  - `dropped`
  - `removed_from_source`

## Notes

- Unchanged documents are still emitted in the current snapshot.
- Reuse is based on fingerprint comparison, not timestamp comparison alone.
- `dropped` means "seen upstream but rejected locally", not "deleted from source".
- `removed_from_source` means the document existed in the previous snapshot but was not seen upstream in the current run.
