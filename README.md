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
- `latest_downloaded.json`
- `latest_enriched.json`
- `latest_improved.json`
- `dropped_kos.json`
- `job-logs/<job_id>.log`

## API

Main endpoints:

- `GET /healthz`
- `POST /run-pipeline`
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
- `EUF_VISION_MODEL`
- `EUF_VISION_API_KEY`
- `EUF_VISION_RETRIES`
- `EUF_VISION_RETRY_BASE_SEC`
- `EUF_VISION_MIN_INTERVAL_SEC`
- `EUF_VISION_PDF_MAP_REDUCE_THRESHOLD`
- `EUF_VISION_PDF_CHUNK_PAGES`

Improver:

- `VLLM_HOST`
- `VLLM_MODEL`
- `VLLM_API_KEY`
- `IMPROVER_HTTP_TIMEOUT`
- `IMPROVER_MAX_INPUT_CHARS`

Notes:

- `DEV` and `PRD` backend credentials are separate.
- If a stage integration is disabled, its endpoint variables may be unused for that run.
- Keep `.env` out of the image; the Docker setup already expects it at runtime.

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
- The data-prep vision path serializes requests and can use a light PDF map-reduce flow for longer PDFs.
- The improver can generate `ko_content_flat_summarised` directly from hosted PDFs without overwriting `ko_content_flat`.
- Improver reuses prior LLM output when the improver fingerprint is unchanged.
- Long hosted videos can be skipped before custom transcription if they exceed the configured duration limit.
- Job cancellation is cooperative. A running job stops between pages, documents, and stage boundaries rather than being force-killed mid-write.

## Stage Notes

More detailed stage logic and ASCII diagrams:

- [docs/downloader.md](docs/downloader.md)
- [docs/enricher.md](docs/enricher.md)
- [docs/improver.md](docs/improver.md)

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

## Current Design Notes

- Job execution currently runs in-process through FastAPI background tasks.
- Outputs are intended for downstream indexing / search preparation, not direct end-user serving.
- The repo contains stage docs for behavior, but the source code is the final authority.
