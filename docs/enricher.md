# Enricher Stage

## Purpose

The enricher fills missing searchable text for documents that point to webpages, hosted media, or supported video platforms.

## High-Level Flow

```text
+----------------------+
| Downloader snapshot  |
+----------+-----------+
           |
           v
+----------------------+
| Select docs with     |
| enrich_via route     |
+----------+-----------+
           |
           v
+----------------------+
| Reuse previous       |
| enrichment if inputs |
| fingerprint matches  |
+----+-------------+---+
     |             |
     | reused      | needs work
     v             v
+----------------------+ 
| skip current call    |
+----------+-----------+
           |
           v
+----------------------+
| Route selection      |
| pagesense / api /    |
| custom transcribe    |
+----+-------------+---+
     |             |
     | page-like   | media-like
     v             v
+----------------------+   +----------------------+
| PageSense first      |   | Reachability probe   |
| or vision preferred  |   | and transcribe call  |
+----+-------------+---+   +----------+-----------+
     |             |                 |
     | empty       | text            | transcript
     v             v                 v
+----------------------+   +----------------------+
| Vision fallback      |   | Patch ko_content_*   |
| for image/pdf pages  |   | fields              |
+----------+-----------+   +----------------------+
           |
           v
+----------------------+
| Write final_enriched |
| + latest pointer     |
+----------------------+
```

## Main Logic

1. Load the latest downloader output.
2. Build an index by logical KO id.
3. For each document:
   - determine whether it has an enrichment route
   - compute `_enrich_inputs_fp`
   - try carry-forward from previous enriched output
   - skip if previous enrichment is still valid
4. Resolve the route:
   - `pagesense`
   - `api_transcribe`
   - `custom_transcribe`
5. Validate the target URL.
6. Optionally probe the URL before expensive work.
7. Execute route-specific logic:

### `pagesense`

- Used for text-oriented page URLs.
- For clearly visual resources, the stage prefers vision before PageSense:
  - direct image content
  - direct PDF content
  - embedded PDF found in HTML
- If PageSense returns empty text, the vision fallback may still try to extract text from an image/PDF target.

### `api_transcribe`

- Used for supported platform URLs handled by the deAPI transcription path.

### `custom_transcribe`

- Used for direct hosted media URLs.
- The stage now has a pre-flight guard for long videos:
  - probe content type
  - if it is `video/*`, probe duration via `ffprobe`
  - if duration exceeds `CUSTOM_TRANSCRIBE_MAX_DURATION_SEC` (default `3000`, 50 min), skip with `custom_transcribe:too_long`

## Visual Fallback Logic

```text
page URL
  |
  +--> direct image content ----------> vision
  |
  +--> direct PDF content ------------> render page 1 to PNG --> vision
  |
  +--> HTML page
         |
         +--> embedded PDF found -----> verify PDF --> render --> vision
         |
         +--> og:image / twitter:image -> vision
         |
         +--> single obvious <img> ----> vision
         |
         +--> otherwise ----------------> PageSense
```

## Output Contract

The enricher patches documents in place with:
- `ko_content_flat`
- `ko_content_source`
- `ko_content_url`
- `enriched = 1`

It writes:
- `final_enriched_<run_id>.json`
- `latest_enriched.json`

## Notes

- The enricher does not enrich every document, only candidates with a valid route.
- Previous successful enrichments are reused aggressively when the enrichment inputs did not change.
- Failures are recorded by route-specific tags such as:
  - `pagesense:empty`
  - `vision_preferred:*`
  - `custom_transcribe:proxy_timeout`
  - `custom_transcribe:too_long`
