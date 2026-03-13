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
+----+-------------+---+
     |             |
     | skip        | attempt
     v             v
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
   - carry forward any previous improvements when valid
   - skip if the previous improvement is still applicable
   - otherwise call the improver engine
4. The engine:
   - summarizes content
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
- Upstream `524` proxy failures are external-service issues, not parser issues.
