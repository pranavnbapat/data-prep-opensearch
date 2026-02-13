# improver_llm_client.py

from __future__ import annotations

import logging

from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter, Retry

from improver_config import BASE_VLLM_HOST, PER_REQUEST_TIMEOUT, VLLM_API_KEY


logger = logging.getLogger(__name__)
_session = requests.Session()
_retries = Retry(
    total=2,
    backoff_factor=0.7,
    # 404 is usually "real", but with RunPod proxy it can be transient. We'll treat it as retryable.
    status_forcelist=(404, 408, 409, 425, 429, 499, 500, 502, 503, 504, 524),
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retries))
_session.mount("http://", HTTPAdapter(max_retries=_retries))


def _auth_headers() -> Dict[str, str]:
    """
    vLLM (OpenAI-compatible) expects: Authorization: Bearer <key>
    """
    if not VLLM_API_KEY:
        return {}
    return {"Authorization": f"Bearer {VLLM_API_KEY}"}


def warm_up_model(model: str, base_url: Optional[str] = None) -> None:
    host = (base_url or BASE_VLLM_HOST).rstrip("/")
    if not host:
        return
    url = f"{host}/v1/chat/completions"
    try:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "Warm-up request."},
                {"role": "user", "content": "hi"},
            ],
            "max_tokens": 4,
            "temperature": 0.0,
        }

        resp = _session.post(url, json=payload, headers=_auth_headers(), timeout=(10, 30))

        if resp.status_code >= 400:
            logger.warning(
                "[ImproverWarmup] status=%s url=%s body=%s",
                resp.status_code, url, (resp.text or "")[:200]
            )

    except Exception:
        pass


def call_vllm_chat(
    *,
    model: str,
    prompt: str,
    content: str,
    options_override: Optional[Dict[str, Any]] = None,
    base_url: Optional[str] = None,
) -> str:
    host = (base_url or BASE_VLLM_HOST).rstrip("/")
    if not host:
        raise RuntimeError("Missing RUNPOD_VLLM_HOST (or BASE_VLLM_HOST) for improver")

    url = f"{host}/v1/chat/completions"

    full_prompt = (
        f"{prompt}\n\n"
        "-----\n"
        "FILE CONTENT START\n"
        f"{content}\n"
        "FILE CONTENT END\n"
        "-----"
    )

    payload: Dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Answer directly. Return only the required output."},
            {"role": "user", "content": full_prompt},
        ],
        "temperature": 0.2,
    }

    if options_override:
        if isinstance(options_override.get("max_tokens"), int):
            payload["max_tokens"] = options_override["max_tokens"]
        if isinstance(options_override.get("temperature"), (float, int)):
            payload["temperature"] = options_override["temperature"]

    # logger.warning("[ImproverHTTP] POST %r", url)
    logger.debug("[ImproverHTTP] request")

    r = _session.post(url, json=payload, headers=_auth_headers(), timeout=PER_REQUEST_TIMEOUT)

    if r.status_code >= 400:
        cf_ray = r.headers.get("cf-ray", "")
        logger.error(
            "[ImproverHTTP] status=%s url=%s cf_ray=%s body=%s",
            r.status_code, url, cf_ray, (r.text or "")[:500]
        )

        if r.status_code == 404:
            logger.error(
                "[ImproverHTTP] 404 usually means the server behind BASE_VLLM_HOST is not vLLM's OpenAI API "
                "(wrong port/service, vLLM not running, or proxy not pointing to vLLM). Try POST %s/v1/models to verify.",
                (host or "").rstrip("/")
            )
            # Quick health probe: if models works, the 404 was likely transient routing.
            try:
                probe = _session.get(f"{host}/v1/models", headers=_auth_headers(), timeout=(5, 10))

                logger.error("[ImproverHTTP] 404 probe_models_status=%s", probe.status_code)
            except Exception as e:
                logger.error("[ImproverHTTP] 404 probe_models_failed=%s", e)

        r.raise_for_status()

    data = r.json()

    try:
        text = data["choices"][0]["message"]["content"]
    except Exception:
        raise RuntimeError(f"Malformed vLLM response: {data!r}")

    text = (text or "").strip()
    if not text:
        raise RuntimeError("Empty response from vLLM")

    return text
