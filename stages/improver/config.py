# improver_config.py

from __future__ import annotations

import os

# vLLM / OpenAI-compatible endpoint
RUNPOD_VLLM_HOST = (os.getenv("RUNPOD_VLLM_HOST") or "").rstrip("/")
if not RUNPOD_VLLM_HOST:
    # Don't crash at import time; improver_engine will fail fast with a clearer error.
    RUNPOD_VLLM_HOST = ""

BASE_VLLM_HOST = RUNPOD_VLLM_HOST

VLLM_API_KEY = (os.getenv("VLLM_API_KEY") or "").strip()

PRIMARY_MODEL = (os.getenv("VLLM_MODEL") or "").strip()

# Context/limits
VLLM_MAX_MODEL_LEN = int(os.getenv("VLLM_MAX_MODEL_LEN", "131072"))
EXTREME_CTX_THRESHOLD_TOK = VLLM_MAX_MODEL_LEN - 3_000
NEAR_LIMIT_CTX_THRESHOLD_TOK = int(VLLM_MAX_MODEL_LEN * 0.85)

CHUNK_TARGET_TOK = int(os.getenv("IMPROVER_CHUNK_TARGET_TOK", "16000"))
CHUNK_OVERLAP_TOK = int(os.getenv("IMPROVER_CHUNK_OVERLAP_TOK", "400"))

DEFAULT_NUM_PREDICT = int(os.getenv("IMPROVER_DEFAULT_NUM_PREDICT", "3072"))
COMBINE_NUM_PREDICT = int(os.getenv("IMPROVER_COMBINE_NUM_PREDICT", "24576"))

SUMMARY_MAX_ATTEMPTS = int(os.getenv("IMPROVER_MAX_ATTEMPTS", "3"))

# Networking
PER_REQUEST_TIMEOUT = int(os.getenv("IMPROVER_PER_REQUEST_TIMEOUT", "600"))
