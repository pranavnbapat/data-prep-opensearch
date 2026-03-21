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
   - relies on the enricher-prepared content fields
   - stores summary diagnostics in `ko_content_flat_summarised_stats`
   - derives improved metadata fields
   - mutates the document in place
5. Write:
   - `final_improved_<run_id>.json`
   - `latest_improved.json`

Checkpointing:

- `latest_improved.json` is updated after each improver record that is actually processed.
- The same `final_improved_<run_id>.json` path is rewritten atomically during the run and finalized again when the stage completes.
- This allows reruns after cancellation to reuse the latest persisted improver work.

## LLM Parsing Logic

The improver expects structured model output, but the parser is defensive.

It now tolerates:
- fenced JSON
- prose wrapped around JSON
- truncated `{"summary": ...` style output
- bare `summary` fields
- plain-text fallback

This reduces failures caused by malformed model responses.

Retry behavior:

- summary generation retries up to `IMPROVER_MAX_ATTEMPTS`
- metadata field generation retries up to `IMPROVER_METADATA_MAX_ATTEMPTS`

These retries cover:

- malformed JSON
- empty text output
- unparsable keyword output
- transient model / proxy failures

## PDF Summary Path

Current behavior for PDFs:

- the enricher is responsible for PDF-aware file processing
- for summarised PDF VLM output, the enricher stores:
  - `ko_content_flat_vision`
  - `ko_content_is_summary = 1`
- the improver does not re-run hosted PDF vision extraction
- if `ko_content_is_summary = 1` and `ko_content_flat_vision` exists:
  - the improver copies that content into `ko_content_flat_summarised`
  - runs a light polish pass only
  - explicitly avoids aggressive compression / re-summarisation
- otherwise the improver follows the normal summary flow from `ko_content_flat`

## Output Contract

Important stats include:
- `attempted`
- `improved`
- `failed`
- `skipped_prev_done`
- `carry_forward_copied`
- `failure_reasons`

The improver writes the final document set used by the rest of the pipeline output.

Per-document summary outputs now include:

- `ko_content_flat_summarised`
- `ko_content_flat_summarised_stats`

`ko_content_flat_summarised_stats` contains practical diagnostics such as:

- `mode`
- `summary`
- `coverage_score`
- `density_score`
- `compression_judgement`
- `faithfulness_confidence`
- `notes`
- `source_word_count`
- `summary_word_count`
- `source_char_count`
- `summary_char_count`
- `compression_ratio`

Notes:

- model-reported scores are self-assessment signals, not ground truth
- deterministic counts and compression ratio are computed locally
- reference-based metrics such as ROUGE/BLEU/METEOR are not computed here because the pipeline does not have gold reference summaries

## Summary Quality Target

The goal is not aggressive compression. For this pipeline, summary quality should favor:

- faithfulness over polish
- coverage over brevity
- useful search context over short UI-style summaries

Practical guidance:

- `ko_content_flat_summarised` should usually preserve the main entities, topics, actions, and claims from the source
- long PDFs may still produce fairly long summaries; that is acceptable if the text remains grounded and useful
- avoid rewriting so aggressively that document-specific terminology, named entities, or procedural detail disappear
- avoid dumping raw chunk text without enough structure, because that increases noise downstream

Recommended evaluation dimensions:

- faithfulness:
  - summary claims should be supported by the source
- coverage:
  - key document content should remain present after summarisation
- usefulness:
  - the summary should still help retrieval, classification, and metadata generation
- compression:
  - shorter than the source, but not aggressively collapsed

Useful heuristic target ranges for document summarisation:

- coverage:
  - prefer medium-high coverage
  - rough target: `0.45` to `0.75`
- density:
  - prefer medium density
  - rough target: `1.5` to `4.0`
- compression:
  - prefer moderate compression
  - rough target: `5` to `20`

Interpretation:

- very low coverage or very low density:
  - often means the summary is too rewritten and may lose context
- very high density:
  - often means the summary is too extractive and noisy
- very high compression:
  - often means important detail has been discarded

These are heuristic bands, not hard gates. For this repo:

- faithfulness is more important than hitting a compression target
- coverage is more important than producing a very short summary
- longer PDF summaries are acceptable if they preserve useful content without obvious garbage

If summarisation quality is evaluated formally, prefer a mix of:

- semantic similarity
- faithfulness / consistency checks
- extractiveness measures such as coverage-density-compression
- small manual review on real long PDFs

## Notes

- A single model failure should not abort the full stage.
- Previous improvements are reused to avoid unnecessary LLM calls.
- Warm-up is cached per process per `(host, model)` pair.
- Upstream `524` proxy failures are external-service issues, not parser issues.
