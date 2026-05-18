from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests

from stages.downloader.utils import api_base, get_session, load_backend_cfg
from stages.enricher.deapi_client import DEAPI_BASE_URL
from stages.improver.config import VLLM_API_KEY


def _is_enabled(name: str, default: str = "false") -> bool:
    return (os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"})


def _masked_headers(token: Optional[str] = None) -> Dict[str, str]:
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def _service_result(
    *,
    service: str,
    enabled: bool,
    status: str,
    endpoint: Optional[str] = None,
    status_code: Optional[int] = None,
    latency_ms: Optional[int] = None,
    error: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "service": service,
        "enabled": enabled,
        "status": status,
    }
    if endpoint:
        payload["endpoint"] = endpoint
    if status_code is not None:
        payload["status_code"] = status_code
    if latency_ms is not None:
        payload["latency_ms"] = latency_ms
    if error:
        payload["error"] = error
    if details:
        payload["details"] = details
    return payload


def _probe_get(
    *,
    service: str,
    endpoint: str,
    timeout_s: int,
    headers: Optional[Dict[str, str]] = None,
    ok_statuses: Optional[set[int]] = None,
    accept_lt_500: bool = False,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    try:
        resp = requests.get(endpoint, headers=headers or {}, timeout=timeout_s)
        latency_ms = int((time.perf_counter() - t0) * 1000)
        status_code = int(resp.status_code)
        if (ok_statuses and status_code in ok_statuses) or (accept_lt_500 and status_code < 500):
            return _service_result(
                service=service,
                enabled=True,
                status="ok",
                endpoint=endpoint,
                status_code=status_code,
                latency_ms=latency_ms,
            )
        return _service_result(
            service=service,
            enabled=True,
            status="error",
            endpoint=endpoint,
            status_code=status_code,
            latency_ms=latency_ms,
            error=(resp.text or "")[:300].strip() or f"Unexpected HTTP {status_code}",
        )
    except Exception as exc:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return _service_result(
            service=service,
            enabled=True,
            status="error",
            endpoint=endpoint,
            latency_ms=latency_ms,
            error=f"{exc.__class__.__name__}: {exc}",
        )


def _probe_backend_core(env_mode: str) -> Dict[str, Any]:
    try:
        cfg = load_backend_cfg(env_mode)
        endpoint = f"{api_base(cfg)}/api/logical_layer/documents"
        session = get_session(cfg, timeout=max(5, int(os.getenv("DL_HTTP_TIMEOUT", "30"))))
        t0 = time.perf_counter()
        resp = session.post(endpoint, params={"limit": 1, "page": 1, "sort_criteria": 1}, json={})
        latency_ms = int((time.perf_counter() - t0) * 1000)
        if resp.status_code == 200:
            return _service_result(
                service="backend_core",
                enabled=True,
                status="ok",
                endpoint=endpoint,
                status_code=resp.status_code,
                latency_ms=latency_ms,
            )
        return _service_result(
            service="backend_core",
            enabled=True,
            status="error",
            endpoint=endpoint,
            status_code=resp.status_code,
            latency_ms=latency_ms,
            error=(resp.text or "")[:300].strip() or f"Unexpected HTTP {resp.status_code}",
        )
    except Exception as exc:
        return _service_result(
            service="backend_core",
            enabled=True,
            status="error",
            error=f"{exc.__class__.__name__}: {exc}",
        )


def _probe_agri_gate() -> Dict[str, Any]:
    enabled = _is_enabled("AGRI_GATE_ENABLED", "true")
    base_url = (os.getenv("AGRI_GATE_BASE_URL") or "").strip().rstrip("/")
    if not enabled:
        return _service_result(service="agri_gate", enabled=False, status="skipped", details={"reason": "disabled"})
    if not base_url:
        return _service_result(service="agri_gate", enabled=True, status="error", error="Missing AGRI_GATE_BASE_URL")
    token = (os.getenv("AGRI_GATE_API_TOKEN") or "").strip()
    return _probe_get(
        service="agri_gate",
        endpoint=f"{base_url}/v1/health",
        timeout_s=max(5, int(os.getenv("AGRI_GATE_TIMEOUT", "60"))),
        headers=_masked_headers(token),
        ok_statuses={200},
    )


def _probe_pagesense() -> Dict[str, Any]:
    enabled = _is_enabled("ENRICH_ENABLE_URL_EXTRACT", "true")
    base_url = (os.getenv("URL_CONTENT_EXTRACTOR_BASE") or "").strip().rstrip("/")
    if not enabled:
        return _service_result(service="pagesense", enabled=False, status="skipped", details={"reason": "disabled"})
    if not base_url:
        return _service_result(service="pagesense", enabled=True, status="error", error="Missing URL_CONTENT_EXTRACTOR_BASE")
    return _probe_get(
        service="pagesense",
        endpoint=base_url,
        timeout_s=max(5, int(os.getenv("EXTRACTOR_HTTP_TIMEOUT", "30"))),
        accept_lt_500=True,
    )


def _probe_custom_transcribe() -> Dict[str, Any]:
    enabled = _is_enabled("ENRICH_ENABLE_CUSTOM_TRANSCRIBE", "false")
    endpoint = (os.getenv("CUSTOM_TRANSCRIBE_ENDPOINT") or "").strip().rstrip("/")
    if not enabled:
        return _service_result(service="custom_transcribe", enabled=False, status="skipped", details={"reason": "disabled"})
    if not endpoint:
        return _service_result(service="custom_transcribe", enabled=True, status="error", error="Missing CUSTOM_TRANSCRIBE_ENDPOINT")
    return _probe_get(
        service="custom_transcribe",
        endpoint=endpoint,
        timeout_s=max(5, int(os.getenv("CUSTOM_TRANSCRIBE_HTTP_TIMEOUT", "180"))),
        accept_lt_500=True,
    )


def _probe_deapi() -> Dict[str, Any]:
    enabled = _is_enabled("ENRICH_ENABLE_API_TRANSCRIBE", "false")
    api_key = (os.getenv("DEAPI_API_KEY") or "").strip()
    if not enabled:
        return _service_result(service="deapi", enabled=False, status="skipped", details={"reason": "disabled"})
    if not api_key:
        return _service_result(service="deapi", enabled=True, status="error", error="Missing DEAPI_API_KEY")
    return _probe_get(
        service="deapi",
        endpoint=DEAPI_BASE_URL,
        timeout_s=max(5, int(os.getenv("DEAPI_HTTP_TIMEOUT", "60"))),
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        accept_lt_500=True,
    )


def _probe_vision() -> Dict[str, Any]:
    base_url = (os.getenv("EUF_VISION_URL") or "").strip().rstrip("/")
    model = (os.getenv("EUF_VISION_MODEL") or "").strip()
    api_key = (os.getenv("EUF_VISION_API_KEY") or "").strip()
    if not base_url or not model or not api_key:
        return _service_result(service="vision", enabled=False, status="skipped", details={"reason": "not_configured"})
    return _probe_get(
        service="vision",
        endpoint=f"{base_url}/v1/models",
        timeout_s=max(5, int(os.getenv("EUF_VISION_TIMEOUT", "120"))),
        headers={"Authorization": f"Bearer {api_key}"},
        ok_statuses={200},
    )


def _probe_vllm() -> Dict[str, Any]:
    base_url = (os.getenv("RUNPOD_VLLM_HOST") or "").strip().rstrip("/")
    if not base_url:
        return _service_result(service="improver_vllm", enabled=False, status="skipped", details={"reason": "not_configured"})
    return _probe_get(
        service="improver_vllm",
        endpoint=f"{base_url}/v1/models",
        timeout_s=max(5, int(os.getenv("IMPROVER_PER_REQUEST_TIMEOUT", "600"))),
        headers=_masked_headers(VLLM_API_KEY),
        ok_statuses={200},
    )


def probe_dependencies(env_mode: str) -> Dict[str, Any]:
    env = (env_mode or os.getenv("ENV_MODE") or "DEV").strip().upper()
    checks = [
        _probe_backend_core(env),
        _probe_agri_gate(),
        _probe_pagesense(),
        _probe_custom_transcribe(),
        _probe_deapi(),
        _probe_vision(),
        _probe_vllm(),
    ]
    has_error = any(item.get("status") == "error" for item in checks if item.get("enabled"))
    overall = "error" if has_error else "ok"
    return {
        "ok": not has_error,
        "status": overall,
        "env_mode": env,
        "services": checks,
    }
