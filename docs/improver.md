# Improver Stage

## Purpose

The improver is the LLM post-processing stage. It reads already-enriched documents, generates improved summaries and metadata, and writes the final improved snapshot.

## High-Level Flow

```text
+----------------------+
| Enriched docs        |
+----------+-----------+
           |
           v
+----------------------+
| Load previous        |
| improved snapshot    |
+----------+-----------+
           |
           v
+----------------------+
| For each doc         |
| carry forward prior  |
| improvements         |
+----------+-----------+
           |
           v
+----------------------+
| Skip if previous    |
| improvement still   |
| applies             |
+----+-----------+----+
     |           |
   yes           no
     |           |
     v           v
+----------------------+   +----------------------+
| unchanged / already  |   | call vLLM-compatible |
| improved             |   | improver engine      |
+----------------------+   +----------+-----------+
                                      |
                                      v
                           +----------------------+
                           | parse model output   |
                           | patch doc in place   |
                           +----------+-----------+
                                      |
                                      v
                           +----------------------+
                           | write final_improved |
                           | + latest pointer     |
                           +----------------------+
```

## Main Logic

1. Load the input enriched docs.
   - The pipeline now passes the current run's enriched docs directly.
   - This avoids accidentally resolving and reusing an older enriched file.
2. Load the previous improved snapshot for reuse checks.
3. For each document:
   - compute `_improver_fp` from the current improver inputs
   - carry forward any previous improvements when valid
   - skip if the previous improvement is still applicable and `_improver_fp` matches
   - otherwise call the improver engine
4. The engine:
   - summarizes content
   - for hosted PDFs, prefers generating `ko_content_flat_summarised` from the file itself via the vision path
   - keeps `ko_content_flat` unchanged; only the summary field changes
   - derives improved metadata fields
   - mutates the document in place
5. Write:
   - `final_improved_<run_id>.json`
   - `latest_improved.json`

## LLM Parsing Logic

The improver expects structured model output, but the parser is defensive.

It now tolerates:
- fenced JSON
- prose wrapped around JSON
- truncated `{"summary": ...` style output
- bare `summary` fields
- plain-text fallback

This reduces failures caused by malformed model responses.

## Hosted PDF Summary Path

Current behavior for hosted PDFs:

- if `ko_is_hosted = true`
- and `ko_object_mimetype = application/pdf`
- and `ko_file_id` is present

then the improver prefers generating `ko_content_flat_summarised` directly from the PDF file via the vision helper.

That means:

- the improver does not rely only on upstream `ko_content_flat` for the summary field
- the raw content field is not overwritten
- the summary can use the PDF-aware vision map-reduce logic already implemented in the enricher vision module

If that file-based summary path fails, the improver falls back to the normal text-based summary flow from `ko_content_flat`.

## Output Contract

Important stats include:
- `attempted`
- `improved`
- `failed`
- `skipped_prev_done`
- `carry_forward_copied`
- `failure_reasons`

The improver writes the final document set used by the rest of the pipeline output.

## Notes

- A single model failure should not abort the full stage.
- Previous improvements are reused to avoid unnecessary LLM calls.
- Warm-up is cached per process per `(host, model)` pair.
- Upstream `524` proxy failures are external-service issues, not parser issues.
