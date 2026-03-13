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

Local compose:

```bash
docker compose up --build
```

## Runtime Notes

- The pipeline is file-backed. Latest pointers are stored as JSON files in `output/`.
- Downloader emits a full snapshot, not just changed records.
- Enricher only attempts external work for candidate documents that need it.
- Improver reuses prior LLM output when the improver fingerprint is unchanged.
- Long hosted videos can be skipped before custom transcription if they exceed the configured duration limit.

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
