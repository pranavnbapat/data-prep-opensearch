# Enricher Stage

## Purpose

The enricher fills `ko_content_flat` for documents that require external extraction or transcription. It works on the latest downloader snapshot and may reuse prior enrichment results when the enrichment inputs are unchanged.

## High-Level Flow

```text
+----------------------+
| latest_downloaded    |
| downloader snapshot  |
+----------+-----------+
           |
           v
+----------------------+
| load previous        |
| latest_enriched      |
+----------+-----------+
           |
           v
+----------------------+
| for each doc:        |
| compute enrich fp    |
+----------+-----------+
           |
           v
+----------------------+
| carry forward prior  |
| enrichment if any    |
+----------+-----------+
           |
           v
+----------------------+
| skip if prior        |
| enrichment is valid  |
+----------+-----------+
           |
           v
+----------------------+
| route selection via  |
| enrich_via           |
+----+-------------+---+
     |             |
 pagesense   api/custom transcribe
     |             |
     v             v
+----------------------+   +----------------------+
| visual detection      |   | direct transcribe    |
| + pagesense fallback  |   | path                 |
+----------+-----------+   +----------+-----------+
           |                          |
           v                          v
           +------------+-------------+
                        |
                        v
             +----------------------+
             | patch ko_content_*   |
             | on success           |
             +----------+-----------+
                        |
                        v
             +----------------------+
             | final_enriched_*     |
             | latest_enriched.json |
             +----------------------+
```

## Current Implemented Logic

### 1. Input loading

- Load the latest downloader output from `latest_downloaded.json` or the latest `final_output_*.json`.
- Load the previous enricher output from `latest_enriched.json` or the latest `final_enriched_*.json`.
- Build a previous-doc index by logical id (`_orig_id` or `_id`).

### 2. Enrichment fingerprint

Each document gets `_enrich_inputs_fp` before routing.

Current fingerprint regimes:

- URL-only, non-hosted:
  - `@id`
- Hosted media:
  - `@id`
  - `ko_file_id`
- Hosted non-media doc:
  - `@id`
  - `ko_file_id`
  - `ko_content_flat`

### 3. Carry-forward and skip

For each doc:

- If a previous enriched doc exists and has real enriched content, the enricher may copy forward:
  - `ko_content_flat`
  - `ko_content_source`
  - `ko_content_url`
  - `enriched = 1`
- After that, the enricher skips a fresh external call if:
  - previous doc exists
  - previous doc has actual enriched content
  - previous `_enrich_inputs_fp` matches current `_enrich_inputs_fp`

### 4. Route selection

The current route is stored in `enrich_via`.

Current implemented routing rules:

```text
if ko_is_hosted and mimetype is audio/* or video/*:
    enrich_via = custom_transcribe

elif ko_is_hosted and mimetype is image/* or application/pdf:
    enrich_via = pagesense

elif not ko_is_hosted:
    optionally resolve @id -> resolved_url

    if no mimetype and no ko_file_id:
        if @id is supported by deAPI:
            enrich_via = api_transcribe
        elif resolved_url is supported by deAPI:
            enrich_via = api_transcribe
        else:
            enrich_via = pagesense

    else:
        if @id is supported by deAPI:
            enrich_via = api_transcribe
        else:
            enrich_via = pagesense

else:
    no enrich_via
```

Important details of the current implementation:

- Hosted `audio/*` and `video/*` go to `custom_transcribe`.
- Hosted `image/*` and hosted `application/pdf` currently go to `pagesense`, but the execution path will usually prefer vision first because the target is the hosted file URL.
- Non-hosted URL-only records become `api_transcribe` only if `@id` or `resolved_url` is supported by deAPI.
- Non-hosted URLs may also get a stored `resolved_url` if redirect resolution succeeds.

### 5. Candidate filtering before execution

After `enrich_via` is set, the enricher filters candidates:

- skip if no route
- skip if route feature flag is disabled
- skip if target URL is missing
- skip if URL classification fails:
  - unsupported scheme
  - missing host
  - bad TLD
  - homepage / bare domain
- optionally probe target URL first:
  - `custom_transcribe` probes by default
  - `pagesense` probing is optional and off by default

### 6. Target URL resolution

Current target selection:

- `pagesense`
  - use `ko_file_id` for hosted docs when present
  - otherwise use `@id`
- `api_transcribe`
  - use `@id`
- `custom_transcribe`
  - if hosted `audio/*` or `video/*`, use `ko_file_id`
  - otherwise fall back to `@id`

### 7. Route execution

#### `pagesense`

Current behavior:

1. Try visual-target detection first.
2. Prefer vision immediately if the detected reason is one of:
   - `content_type_image`
   - `content_type_pdf`
   - `get_content_type_image`
   - `get_content_type_pdf`
   - `embedded_pdf`
   - `meta_image`
   - `single_img`
3. If vision succeeds:
   - patch with `ko_content_source = "vision_fallback"`
4. Otherwise run PageSense with retries and backoff.
5. If PageSense returns empty, try vision fallback again for any detected visual target.
6. If all fail:
   - tag as `pagesense:empty` or `vision_preferred:*`

#### `api_transcribe`

Current behavior:

- Only the deAPI path is used.
- The actual execution function only accepts:
  - YouTube
  - Twitter / X
  - Twitch
  - Kick
- Unsupported platform URLs fail as `api_transcribe:unsupported_url`.

#### `custom_transcribe`

Current behavior:

1. Send direct hosted media URL to the custom transcription endpoint.
2. Before doing that, if content type is `video/*`:
   - probe duration with `ffprobe`
   - if duration is greater than `CUSTOM_TRANSCRIBE_MAX_DURATION_SEC`:
     - skip with `custom_transcribe:too_long`
3. Retry on non-terminal failures with backoff.
4. Stop early for terminal tags such as:
   - `custom_transcribe:proxy_timeout`
   - `custom_transcribe:http_404`
   - `custom_transcribe:http_403`
   - `custom_transcribe:too_long`

## Visual Detection Logic

Current visual-target extraction works like this:

```text
target URL
  |
  +--> HEAD/GET says image/* -------------> use target directly
  |
  +--> HEAD/GET says application/pdf -----> use target directly
  |
  +--> HTML page
         |
         +--> embedded PDF URL found ------> verify PDF --> use PDF target
         |
         +--> og:image meta --------------> use image target
         |
         +--> twitter:image meta ---------> use image target
         |
         +--> exactly one <img src> ------> use image target
         |
         +--> otherwise ------------------> no visual target
```

Current vision rendering behavior:

- direct image URL:
  - send directly to vision model
- PDF URL:
  - download PDF
  - render page 1 to PNG with `pdftoppm`
  - send PNG as data URL
- SVG image:
  - download SVG
  - rasterize with ImageMagick `convert`
  - send PNG as data URL

## Success Patching

On successful enrichment, the stage patches:

- `ko_content_flat`
- `ko_content_source`
- `ko_content_url`
- `enriched = 1`
- `_enrich_inputs_fp` is recomputed

## Output Files

The enricher writes:

- `final_enriched_<run_id>.json`
- `latest_enriched.json`

If there are no patched docs and no counted carry-forward copies, it may keep the previous enriched file as-is.
After the recent fix, carried-forward enrichments are counted correctly.

## Current Failure / Skip Tags

Examples currently used in logs and stats:

- `pagesense:empty`
- `pagesense:failed`
- `vision_preferred:*`
- `api_transcribe:unsupported_url`
- `api_transcribe:failed`
- `custom_transcribe:proxy_timeout`
- `custom_transcribe:http_404`
- `custom_transcribe:http_403`
- `custom_transcribe:too_long`
- probe / URL validation skip reasons
