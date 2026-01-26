# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY \
  app.py pipeline.py db_bootstrap.py download_mongodb_data.py utils.py \
  downloader.py downloader_utils.py \
  enricher.py enricher_utils.py \
  improver.py improver_config.py improver_engine.py improver_extractors.py \
  improver_llm_client.py improver_prompts.py improver_text_utils.py improver_utils.py \
  io_helpers.py \
  deapi_transcribe.py \
  job_lock.py \
  ./


RUN mkdir -p /app/output \
 && useradd -m -u 10001 appuser \
 && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
