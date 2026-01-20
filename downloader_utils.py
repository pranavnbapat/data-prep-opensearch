# downloader_utils.py

import threading
import logging
import os

from dataclasses import dataclass
from typing import TypedDict, Optional, Dict, Any, List, Tuple
from urllib3.util.retry import Retry

import requests

from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth


ENV_CHOICES = {"DEV", "PRD"}
logger = logging.getLogger(__name__)
KO_CONTENT_MODE = os.getenv("KO_CONTENT_MODE", "flat_only").strip() or "flat_only"

# ---------------- Thread-local session ----------------
_tls = threading.local()

class BackendCfg(TypedDict):
    host: str
    user: str
    pwd: str

@dataclass(frozen=True)
class DownloadResult:
    docs: List[Dict[str, Any]]
    url_tasks: List[Dict[str, Any]]
    media_tasks: List[Dict[str, Any]]
    stats: Dict[str, Any]

# ---------------- Backend config ----------------
def load_backend_cfg(env_mode: str) -> BackendCfg:
    """
    Loads backend config from environment variables.
    No globals; safe per-job.
    """
    mode = (env_mode or "").strip().upper()
    if mode not in ENV_CHOICES:
        logging.warning("Invalid ENV_MODE=%r; falling back to DEV", env_mode)
        mode = "DEV"

    if mode == "DEV":
        host = os.getenv("BACKEND_CORE_HOST_DEV", "")
        user = os.getenv("BACKEND_CORE_DEV_API_USERNAME", "")
        pwd  = os.getenv("BACKEND_CORE_DEV_API_PASSWORD", "")
    else:
        host = os.getenv("BACKEND_CORE_HOST_PRD", "")
        user = os.getenv("BACKEND_CORE_PRD_API_USERNAME", "")
        pwd  = os.getenv("BACKEND_CORE_PRD_API_PASSWORD", "")

    if not host or not user or not pwd:
        raise RuntimeError(
            f"Missing required env vars for {mode}. "
            f"Expected BACKEND_CORE_HOST_{mode}, BACKEND_CORE_{mode}_API_USERNAME, BACKEND_CORE_{mode}_API_PASSWORD"
        )

    return {"host": host.rstrip("/"), "user": user, "pwd": pwd}

def session_cache_key(backend_cfg: BackendCfg, timeout: int) -> str:
    # Key includes host + username + timeout; password changes should trigger new container redeploy anyway.
    return f"{backend_cfg['host']}|{backend_cfg['user']}|{timeout}"

def get_session(backend_cfg: BackendCfg, timeout: int = 15) -> requests.Session:
    """
    Return a thread-local pooled session. Recreates session when backend/timeout changes.
    Safe to call from threadpool workers.
    """
    key = session_cache_key(backend_cfg, timeout)
    sess = getattr(_tls, "session", None)
    sess_key = getattr(_tls, "session_key", None)
    if sess is None or sess_key != key:
        # Close old session if present
        try:
            if sess is not None:
                sess.close()
        except Exception:
            pass
        sess = build_session(backend_cfg, timeout=timeout)
        _tls.session = sess
        _tls.session_key = key
    return sess

def build_session(backend_cfg: BackendCfg, timeout: int = 15) -> requests.Session:
    """
    Create a pooled, retrying Requests Session with HTTP Basic Auth.
    Adds default timeout to all requests.
    """
    s = requests.Session()
    s.auth = HTTPBasicAuth(backend_cfg["user"], backend_cfg["pwd"])
    s.headers.update({
        "accept": "application/json",
        "Content-Type": "application/json",
    })

    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=50,
        pool_maxsize=50,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    # Enforce a default timeout transparently
    original = s.request
    def _with_timeout(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return original(method, url, **kw)
    s.request = _with_timeout  # type: ignore[attr-defined]

    return s

def api_base(backend_cfg: BackendCfg) -> str:
    base = backend_cfg["host"]
    if not base:
        raise RuntimeError("Backend host is empty.")
    return base

