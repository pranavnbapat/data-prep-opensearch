# deapi_transcribe.py

from __future__ import annotations

import json
import os
import time

from dataclasses import dataclass
from typing import Optional, Union, Dict, Any

import requests


DEAPI_BASE_URL = "https://api.deapi.ai"


@dataclass(frozen=True)
class DeapiTranscriptResult:
    request_id: str
    status: str
    transcript_text: Optional[str] = None
    result_url: Optional[str] = None
    raw_status_payload: Optional[Dict[str, Any]] = None


class DeapiError(RuntimeError):
    pass


def _build_session(api_key: str, timeout_s: int = 60) -> requests.Session:
    """
    Builds a requests.Session with the required headers.
    """
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    })
    # Note: per-request timeout is passed on each request call.
    return s


def _is_probably_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


def _sleep_with_backoff(attempt: int, base_s: float = 1.0, cap_s: float = 20.0) -> None:
    """
    Exponential-ish backoff: 1s, 2s, 4s, 8s... capped.
    """
    delay = min(cap_s, base_s * (2 ** max(0, attempt - 1)))
    time.sleep(delay)


def _raise_for_deapi_error(resp: requests.Response) -> None:
    """
    deAPI uses a consistent error JSON structure; raise something readable.
    """
    try:
        payload = resp.json()
    except Exception:
        resp.raise_for_status()
        return

    message = payload.get("message") or f"HTTP {resp.status_code}"
    errors = payload.get("errors") or []
    details = ""

    # 422 validation errors are often per-field.
    if isinstance(errors, list) and errors:
        try:
            details = " | " + "; ".join(
                f"{e.get('field')}: {','.join(e.get('messages', []))}" if isinstance(e, dict) else str(e)
                for e in errors
            )
        except Exception:
            details = " | " + str(errors)

    raise DeapiError(f"deAPI error {resp.status_code}: {message}{details}")


def _http_request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout_s: int,
    max_attempts: int = 5,
    **kwargs: Any,
) -> requests.Response:
    """
    Retries on 429 and 5xx with backoff. For 429 respects Retry-After if present.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.request(method, url, timeout=timeout_s, **kwargs)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        time.sleep(int(retry_after))
                    except Exception:
                        _sleep_with_backoff(attempt)
                else:
                    _sleep_with_backoff(attempt)
                continue

            if 500 <= resp.status_code <= 599:
                _sleep_with_backoff(attempt)
                continue

            return resp

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            _sleep_with_backoff(attempt)

    raise DeapiError(f"Request failed after {max_attempts} attempts: {last_exc}")


def _poll_request_status(
    session: requests.Session,
    request_id: str,
    *,
    timeout_s: int,
    poll_interval_s: float = 2.0,
    max_wait_s: float = 10 * 60,
) -> Dict[str, Any]:
    """
    Polls /request-status/{request_id} until done/error or timeout.
    """
    status_url = f"{DEAPI_BASE_URL}/api/v1/client/request-status/{request_id}"

    start = time.time()
    while True:
        resp = _http_request_with_retry(session, "GET", status_url, timeout_s=timeout_s)
        if resp.status_code != 200:
            _raise_for_deapi_error(resp)

        payload = resp.json()
        data = (payload or {}).get("data") or {}
        status = (data.get("status") or "").lower()

        if status in {"done", "error"}:
            return payload

        if time.time() - start > max_wait_s:
            raise DeapiError(f"Timed out waiting for request_id={request_id} after {max_wait_s}s")

        time.sleep(poll_interval_s)


def _try_extract_transcript_text(status_payload: Dict[str, Any]) -> Optional[str]:
    """
    deAPI's status payload shows:
      data: { status, progress, result_url, result, preview }
    Sometimes transcript is in 'result' (if return_result_in_response was used),
    otherwise it's behind result_url.
    """
    data = (status_payload or {}).get("data") or {}
    result = data.get("result")

    # If they inline the transcript, it may be a string or a JSON object.
    if isinstance(result, str) and result.strip():
        return result

    if isinstance(result, dict):
        # Be permissive: common keys might be "text", "transcript", etc.
        for k in ("text", "transcript", "content"):
            v = result.get(k)
            if isinstance(v, str) and v.strip():
                return v

        # Last resort: stringify dict so caller can parse it later
        return json.dumps(result, ensure_ascii=False)

    return None


def transcribe_video(
    *,
    api_key: str,
    video: Union[str, os.PathLike],
    model: str = "WhisperLargeV3",
    include_timestamps: bool = False,
    return_result_in_response: bool = True,
    timeout_s: int = 60,
    poll_interval_s: float = 2.0,
    max_wait_s: float = 10 * 60,
) -> DeapiTranscriptResult:
    """
    One function:
      - If `video` is a URL -> uses /vid2txt
      - If `video` is a local file path -> uses /videofile2txt
    Returns transcript text when possible + request_id + result_url.

    Notes:
    - include_timestamps maps to 'include_ts' in deAPI.
    - return_result_in_response: if True, asks deAPI to inline transcript in the response
      (when supported), otherwise you'll typically get a result_url.
    """
    api_key = (api_key or "").strip()
    if not api_key:
        raise ValueError("api_key is empty")

    video_str = str(video)

    session = _build_session(api_key=api_key, timeout_s=timeout_s)

    if _is_probably_url(video_str):
        # URL workflow
        submit_url = f"{DEAPI_BASE_URL}/api/v1/client/vid2txt"
        payload = {
            "video_url": video_str,
            "include_ts": bool(include_timestamps),
            "model": model,
            "return_result_in_response": bool(return_result_in_response),
        }
        resp = _http_request_with_retry(session, "POST", submit_url, timeout_s=timeout_s, json=payload)
        if resp.status_code != 200:
            _raise_for_deapi_error(resp)

    else:
        # File workflow
        # deAPI docs show /api/v1/client/videofile2txt exists; typical upload is multipart/form-data.
        submit_url = f"{DEAPI_BASE_URL}/api/v1/client/videofile2txt"

        path = os.path.expanduser(video_str)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Video file not found: {path}")

        # Many APIs expect:
        # - multipart file field name like "file" or "video"
        # - plus form fields for options
        # deAPI's per-endpoint schema may define the exact field name; "file" is the common convention.
        # If deAPI uses a different field, change files={'file': ...} accordingly.
        with open(path, "rb") as f:
            files = {"file": (os.path.basename(path), f, "video/mp4")}
            data = {
                "include_ts": str(bool(include_timestamps)).lower(),
                "model": model,
                "return_result_in_response": str(bool(return_result_in_response)).lower(),
            }
            resp = _http_request_with_retry(session, "POST", submit_url, timeout_s=timeout_s, files=files, data=data)
            if resp.status_code != 200:
                _raise_for_deapi_error(resp)

    submit_payload = resp.json()
    request_id = ((submit_payload or {}).get("data") or {}).get("request_id")
    if not request_id:
        raise DeapiError(f"Missing request_id in response: {submit_payload}")

    status_payload = _poll_request_status(
        session,
        request_id,
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
        max_wait_s=max_wait_s,
    )

    data = (status_payload or {}).get("data") or {}
    status = (data.get("status") or "").lower()
    result_url = data.get("result_url")

    transcript_text = _try_extract_transcript_text(status_payload)

    # If transcript not inlined, fetch from result_url.
    if not transcript_text and result_url and isinstance(result_url, str) and result_url.startswith("http"):
        # Try to download. If it’s not plain text, we still return the raw body as text.
        dl = _http_request_with_retry(session, "GET", result_url, timeout_s=timeout_s)
        if dl.status_code == 200:
            # Could be text/plain or application/json; we store as text either way.
            transcript_text = dl.text.strip() or None

    return DeapiTranscriptResult(
        request_id=request_id,
        status=status,
        transcript_text=transcript_text,
        result_url=result_url,
        raw_status_payload=status_payload,
    )
