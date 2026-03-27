# Data Prep OpenSearch

FastAPI-based ETL service for preparing EU FarmBook knowledge objects for downstream indexing and search.

The pipeline has three stages:

- `downloader`: fetches and normalizes source knowledge objects
- `enricher`: fills `ko_content_flat` via extraction, transcription, and vision fallbacks
- `improver`: uses an LLM to improve summaries and metadata

## Repo Layout

```text
api/                  FastAPI app, job state, background runner
pipeline/             pipeline orchestration, latest-file helpers, locking
stages/downloader/    source ingestion and snapshot building
stages/enricher/      extraction, transcription, vision fallback
stages/improver/      LLM-based metadata improvement
common/               shared helpers
docs/                 per-stage markdown notes and flow diagrams
```

## High-Level Flow

```text
backend source
    |
    v
downloader
    |
    v
final_output_<run_id>.json
    |
    v
enricher
    |
    v
final_enriched_<run_id>.json
    |
    v
improver
    |
    v
final_improved_<run_id>.json
```

## What It Produces

Artifacts are written under:

```text
output/<ENV>/<YYYY>/<MM>/
```

Important files:

- `final_output_<run_id>.json`
- `final_enriched_<run_id>.json`
- `final_improved_<run_id>.json`
- `final_report_<run_id>.json`
- `logs.txt`
- `latest_downloaded.json`
- `latest_enriched.json`
- `latest_improved.json`
- `dropped_kos.json`
- `job-logs/<job_id>.log`

Checkpoint behavior:

- `latest_downloaded.json` is updated after each downloader page and finalized again at downloader completion.
- `latest_enriched.json` is updated after each enricher record and finalized again at enricher completion.
- `latest_improved.json` is updated after each improver record and finalized again at improver completion.
- This allows canceled runs to restart from the latest persisted stage state instead of waiting for full-stage completion.

## API

Main endpoints:

- `GET /healthz`
- `POST /run-pipeline`
- `POST /sync/backend-core`
- `GET /sync/backend-core/status`
- `POST /pipeline/fast`
- `GET /pipeline/fast/status`
- `POST /pipeline/deferred`
- `GET /pipeline/deferred/status`
- `POST /records/{id}/reprocess`
- `GET /records/{id}`
- `POST /exports/final-improved`
- `GET /exports/final-improved/latest`
- `GET /jobs`
- `GET /jobs/{job_id}`
- `POST /jobs/{job_id}/cancel`
- `GET /jobs/{job_id}/logs`

Run locally:

```bash
uvicorn api.app:app --host 0.0.0.0 --port 8000
```

Development with reload:

```bash
uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload
```

Example trigger:

```bash
curl -X POST http://127.0.0.1:8000/run-pipeline \
  -H "Content-Type: application/json" \
  -d '{"env_mode":"DEV","page_size":100}'
```

Example cancel:

```bash
curl -X POST http://127.0.0.1:8000/jobs/<job_id>/cancel
```

MySQL-backed control plane examples:

```bash
curl -X POST http://127.0.0.1:8000/sync/backend-core \
  -H "Content-Type: application/json" \
  -d '{"env_mode":"DEV","page_size":100}'

curl -X POST http://127.0.0.1:8000/pipeline/fast \
  -H "Content-Type: application/json" \
  -d '{"env_mode":"DEV","max_docs":500}'

curl -X POST http://127.0.0.1:8000/pipeline/deferred \
  -H "Content-Type: application/json" \
  -d '{"env_mode":"DEV","max_docs":50}'

curl -X POST http://127.0.0.1:8000/exports/final-improved \
  -H "Content-Type: application/json" \
  -d '{"env_mode":"DEV","processed_only":true}'
```

`POST /sync/backend-core` request fields:

- `page_size`
  - downloader page size used while fetching backend-core metadata
  - `0` or omitted means "use the configured/default effective page size"
- `env_mode`
  - which environment bucket to sync into, typically `DEV` or `PRD`
- `sort_criteria`
  - backend-core metadata API sort mode passed through to the downloader
- `dl_workers`
  - downloader per-page worker count during sync
  - `0` or omitted means "use the configured/default worker count"

Cancellation notes:

- Cancellation is cooperative, not a hard kill.
- A running job stops at safe checkpoints:
  - between downloader pages
  - between enricher records
  - between improver documents
  - between stage boundaries
- If a job is currently inside a long external call, the cancel request is recorded first and the job stops after that call returns or times out.
- A true hard kill is not implemented in the current in-process background-task design.
- The current manual hard stop is to restart or stop the app/container process.

Example filtered job list:

```bash
curl http://127.0.0.1:8000/jobs?env_mode=DEV
curl http://127.0.0.1:8000/jobs?env_mode=PRD
```

## Local Setup

Python version used by the container is `3.11`.

Typical local setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.sample .env
```

Then fill the required environment variables in `.env`.

## Important Environment Variables

The full sample is in `.env.sample`. These are the main ones by area.

API / runtime:

- `LOG_LEVEL`
- `TZ`
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `FAST_PIPELINE_PDF_MAX_PAGES`

Downloader:

- `BACKEND_CORE_HOST_DEV`
- `BACKEND_CORE_HOST_PRD`
- `BACKEND_CORE_USERNAME_DEV`
- `BACKEND_CORE_PASSWORD_DEV`
- `BACKEND_CORE_USERNAME_PRD`
- `BACKEND_CORE_PASSWORD_PRD`
- `DL_PAGE_SIZE`
- `DL_HTTP_TIMEOUT`
- `DL_MAX_WORKERS`

Enricher:

- `ENRICH_ENABLE_PAGESENSE`
- `ENRICH_ENABLE_API_TRANSCRIBE`
- `ENRICH_ENABLE_CUSTOM_TRANSCRIBE`
- `ENRICH_ENABLE_VISION_FALLBACK`
- `PAGESENSE_URL`
- `PAGESENSE_API_KEY`
- `DEAPI_URL`
- `DEAPI_API_KEY`
- `CUSTOM_TRANSCRIBE_URL`
- `CUSTOM_TRANSCRIBE_API_KEY`
- `CUSTOM_TRANSCRIBE_MAX_DURATION_SEC`
- `EUF_VISION_URL`
  - Vision endpoint base URL. Both forms are accepted:
  - `https://host:8000`
  - `https://host:8000/v1/chat/completions`
- `EUF_VISION_MODEL`
- `EUF_VISION_API_KEY`
- `EUF_VISION_RETRIES`
- `EUF_VISION_RETRY_BASE_SEC`
- `EUF_VISION_MIN_INTERVAL_SEC`
- `EUF_VISION_PDF_MAP_REDUCE_THRESHOLD`
- `EUF_VISION_PDF_CHUNK_PAGES`
- `EUF_VISION_PDF_MAX_PAGES`
- `EUF_VISION_REDUCE_PARTS_PER_PASS`

Improver:

- `VLLM_HOST`
- `VLLM_MODEL`
- `VLLM_API_KEY`
- `IMPROVER_MAX_ATTEMPTS`
- `IMPROVER_METADATA_MAX_ATTEMPTS`
- `IMPROVER_HTTP_TIMEOUT`
- `IMPROVER_MAX_INPUT_CHARS`

Notes:

- `DEV` and `PRD` backend credentials are separate.
- If a stage integration is disabled, its endpoint variables may be unused for that run.
- Keep `.env` out of the image; the Docker setup already expects it at runtime.

## Optional MySQL Control Plane

The original pipeline remains file-snapshot based. An optional MySQL layer can now be used to:

- sync backend-core records into a local MySQL table
- run a fast path against records stored in MySQL
- defer expensive PDFs above `FAST_PIPELINE_PDF_MAX_PAGES`
- export a fresh final-improved snapshot from MySQL state

Recommended flow:

1. `POST /sync/backend-core`
2. `POST /pipeline/fast`
3. `POST /pipeline/deferred`
4. `POST /exports/final-improved`

Current behavior:

- sync stores source docs in MySQL
- fast path processes non-deferred records
- deferred path processes records marked deferred during sync
- export writes `final_improved_mysql_export_<run_id>.json`
- schema bootstrap runs automatically on app startup when MySQL is configured
- current state is stored in `ko_records`
- previous row versions are archived in `ko_records_history`
- unchanged source/processed fingerprints do not create duplicate rows or duplicate history entries

Current limitations:

- this is an additive control plane, not a full replacement of the original downloader/orchestrator yet
- deferred classification currently focuses on hosted PDFs whose page count exceeds `FAST_PIPELINE_PDF_MAX_PAGES`
- the same enricher/improver stage logic is reused; stage artifacts are still written under `output/`

See also:

- [docs/mysql_control_plane.md](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/docs/mysql_control_plane.md)

## Docker

Build the app image:

```bash
docker build -t ghcr.io/pranavnbapat/data-prep-opensearch:latest .
```

Build the scheduler image:

```bash
docker build -f Dockerfile.scheduler -t ghcr.io/pranavnbapat/data-prep-opensearch-scheduler:latest .
```

Push:

```bash
docker push ghcr.io/pranavnbapat/data-prep-opensearch:latest
docker push ghcr.io/pranavnbapat/data-prep-opensearch-scheduler:latest
```

Convenience script for building and pushing both images:

```bash
sh build_and_push_images.sh
```

With an explicit tag:

```bash
sh build_and_push_images.sh 2026-03-14
```

This script builds and pushes:

- `ghcr.io/pranavnbapat/data-prep-opensearch:<tag>`
- `ghcr.io/pranavnbapat/data-prep-opensearch-scheduler:<tag>`

You can override the registry/image names with environment variables if needed:

```bash
REGISTRY=ghcr.io/pranavnbapat sh build_and_push_images.sh latest
```

Local compose:

```bash
docker compose up --build
```

Server-side pull and restart helper:

```bash
sh pull_and_restart.sh
```

This runs:

```bash
docker compose pull
docker compose up -d
```

Typical deployment flow from now on:

1. Local machine:

```bash
sh build_and_push_images.sh latest
```

2. Server:

```bash
sh pull_and_restart.sh
```

## Runtime Notes

- The pipeline is file-backed. Latest pointers are stored as JSON files in `output/`.
- Downloader emits a full snapshot, not just changed records.
- Enricher only attempts external work for candidate documents that need it.
- The data-prep vision path serializes requests and now uses a true PDF map-reduce flow for manageable PDFs.
- For hosted PDFs under the configured page cap, the VLM result is stored in `ko_content_flat_vision` and marked with `ko_content_is_summary = 1` while upstream `ko_content_flat` is preserved.
- Hosted PDFs above the configured page cap are skipped in the vision path and keep their existing upstream `ko_content_flat` when it is usable.
- When `ko_content_is_summary = 1`, the improver copies `ko_content_flat_vision` into `ko_content_flat_summarised` and only performs a light polish pass rather than re-summarising.
- The improver now also stores `ko_content_flat_summarised_stats` with summary diagnostics and deterministic compression statistics.
- Improver reuses prior LLM output when the improver fingerprint is unchanged.
- Long hosted videos can be skipped before custom transcription if they exceed the configured duration limit.
- Job cancellation is cooperative. A running job stops between pages, documents, and stage boundaries rather than being force-killed mid-write.
- A future hard-kill design would require each job to run in its own subprocess so the API can terminate that process explicitly.
- Live console output is also mirrored incrementally to `output/<ENV>/<YYYY>/<MM>/logs.txt` in addition to per-job logs under `job-logs/`.
- `logs.txt` is reset at the start of each new run, while `job-logs/<job_id>.log` remains the durable per-job history.
- The monthly `logs.txt` file includes explicit `RUN_START` and `RUN_END_*` markers so consecutive runs are easy to distinguish.
- Stage JSON snapshots are also checkpointed during execution:
  - downloader after each page
  - enricher after each record
  - improver after each record

## Stage Notes

More detailed stage logic and ASCII diagrams:

- [docs/downloader.md](docs/downloader.md)
- [docs/enricher.md](docs/enricher.md)
- [docs/improver.md](docs/improver.md)

The improver doc also includes the current summary-quality target for balancing:

- faithfulness
- coverage
- extractiveness
- compression

## Known External Dependencies

Depending on configuration, the service may talk to:

- backend-core API
- PageSense extractor
- deAPI for supported platform transcription
- custom transcription endpoint
- vLLM-compatible text model
- vision model endpoint

The app image also includes runtime tools used by the enricher:

- `ffprobe`
- `pdftoppm`
- `convert`

## RunPod Vision Config

Recommended RunPod config files for the shared InternVL vision pod are stored in:

- [runpod_vllm_config/internvl/README.md](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/runpod_vllm_config/internvl/README.md)
- [runpod_vllm_config/internvl/SETUP.md](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/runpod_vllm_config/internvl/SETUP.md)
- [runpod_vllm_config/internvl/TUNING.md](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/runpod_vllm_config/internvl/TUNING.md)
- [runpod_vllm_config/internvl/supervisord.conf](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/runpod_vllm_config/internvl/supervisord.conf)
- [runpod_vllm_config/internvl/traefik.yml](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/runpod_vllm_config/internvl/traefik.yml)
- [runpod_vllm_config/internvl/dynamic/vllm_vlm.yml](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/runpod_vllm_config/internvl/dynamic/vllm_vlm.yml)
- [docs/runpod_a100_sxm_setup.md](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/docs/runpod_a100_sxm_setup.md)

These are tuned for:

- `OpenGVLab/InternVL3_5-14B`
- `NVIDIA A40 48 GB`
- map-reduce style PDF/image workloads
- stability-first operation with `16K` context and `max-num-seqs=1`

For a bare RunPod A100 SXM pod, use the dedicated setup guide above. It includes troubleshooting for:

- `curl: (23) Failure writing output to destination` during Traefik tag lookup
- `tar: Cannot change ownership ... Operation not permitted` during Traefik extraction

## MySQL Workflow

For the full ASCII workflow of:

- `POST /sync/backend-core`
- `POST /pipeline/fast`
- `POST /pipeline/deferred`
- `POST /pipeline/improver-fallback`
- `POST /exports/final-improved`

see:

- [docs/mysql_control_plane.md](/home/pranav/PyCharm/EU-FarmBook/data-prep-opensearch/docs/mysql_control_plane.md)

## Current Design Notes

- Job execution currently runs in-process through FastAPI background tasks.
- Outputs are intended for downstream indexing / search preparation, not direct end-user serving.
- The repo contains stage docs for behavior, but the source code is the final authority.
