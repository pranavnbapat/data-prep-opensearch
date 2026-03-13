from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from stages.enricher.deapi_client import transcribe_video, DeapiError
from stages.enricher.utils import env_bool


logger = logging.getLogger(__name__)


def transcribe_with_deapi(*, video_url: Optional[str] = None, video_path: Optional[Path] = None) -> Optional[str]:
    api_key = (os.getenv("DEAPI_API_KEY") or "").strip()
    if not api_key:
        logger.error("[deAPI] Missing DEAPI_API_KEY env var")
        return None

    model = (os.getenv("DEAPI_TRANSCRIBE_MODEL") or "WhisperLargeV3").strip() or "WhisperLargeV3"
    include_ts = env_bool("DEAPI_INCLUDE_TIMESTAMPS", False)
    timeout_s = int(os.getenv("DEAPI_HTTP_TIMEOUT", "60"))
    poll_interval_s = float(os.getenv("DEAPI_POLL_INTERVAL_SEC", "2.0"))
    max_wait_s = int(os.getenv("DEAPI_MAX_WAIT_SEC", str(12 * 60)))

    try:
        if video_url is not None and isinstance(video_url, (bytes, bytearray)):
            video_url = video_url.decode("utf-8", "ignore")

        video_arg: Optional[str] = None
        if isinstance(video_url, str) and video_url.strip():
            video_arg = video_url.strip()
        elif video_path is not None:
            video_arg = str(video_path)

        if not video_arg:
            logger.error("[deAPI] Missing video source (both url and path empty)")
            return None

        result = transcribe_video(
            api_key=api_key,
            video=video_arg,
            model=model,
            include_timestamps=include_ts,
            return_result_in_response=True,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
            max_wait_s=max_wait_s,
        )

        text = (result.transcript_text or "").strip()
        if not text:
            logger.warning("[deAPI] Empty transcript. status=%s request_id=%s url_host=%s",
                           result.status, result.request_id, video_url)
            return None
        return text

    except DeapiError as e:
        logger.error("[deAPI] Transcription failed url_host=%s err=%s", video_url, e)
        return None
    except Exception as e:
        logger.exception("[deAPI] Unexpected error url_host=%s err=%s", video_url, e)
        return None
