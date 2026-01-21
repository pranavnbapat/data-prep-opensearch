# improver_utils.py

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from enricher_utils import load_stage_payload
from io_helpers import resolve_latest_pointer, find_latest_matching


# ---------------- Loading inputs ----------------

def load_latest_enricher_output(env_mode: str, output_root: str) -> Tuple[Path, List[Dict[str, Any]]]:
    """
    Prefer pointer file first: latest_enriched.json
    Else fall back to latest matching: final_enriched_*.json
    Supports both payload dict shape {"docs":[...]} or raw list shape.
    """
    p = resolve_latest_pointer(env_mode, output_root, "latest_enriched.json")
    if p is None:
        p = find_latest_matching(env_mode, output_root, "final_enriched_*.json")
    if p is None:
        raise RuntimeError(
            f"No enricher output found under {output_root}/{env_mode.upper()}/ "
            f"(expected latest_enriched.json or final_enriched_*.json)"
        )

    payload = load_stage_payload(p)

    if isinstance(payload, dict):
        docs = payload.get("docs") or []
    elif isinstance(payload, list):
        docs = payload
    else:
        raise RuntimeError(f"Unexpected payload shape in {p}: {type(payload)}")

    if not isinstance(docs, list):
        raise RuntimeError(f"Invalid docs in {p}")

    return p, docs

def load_latest_improver_output(env_mode: str, output_root: str) -> Tuple[Optional[Path], List[Dict[str, Any]]]:
    """
    Prefer pointer file first: latest_improved.json
    Else fall back to latest matching: final_improved_*.json
    """
    p = resolve_latest_pointer(env_mode, output_root, "latest_improved.json")
    if p is None:
        p = find_latest_matching(env_mode, output_root, "final_improved_*.json")
    if p is None:
        return None, []

    payload = load_stage_payload(p)
    if isinstance(payload, dict):
        docs = payload.get("docs") or []
    elif isinstance(payload, list):
        docs = payload
    else:
        return p, []

    if not isinstance(docs, list):
        return p, []

    return p, docs


# ---------------- Skip logic / idempotency ----------------
def should_skip_improve(doc: Dict[str, Any], prev: Optional[Dict[str, Any]]) -> bool:
    """
    Skip if previous snapshot already improved this doc AND the improver fingerprint matches.

    Uses:
      - doc["_improver_fp"] (computed in downloader)
      - prev["_improver_fp"]
      - prev["improved"] == 1
    """
    if not prev:
        return False

    prev_improved = int(prev.get("improved") or 0) == 1
    if not prev_improved:
        return False

    cur_fp = doc.get("_improver_fp")
    prev_fp = prev.get("_improver_fp")

    # If fingerprint missing anywhere, do not skip (safe default)
    if not isinstance(cur_fp, str) or not cur_fp.strip():
        return False
    if not isinstance(prev_fp, str) or not prev_fp.strip():
        return False

    return cur_fp == prev_fp

def classify_failure(reason: Optional[str]) -> str:
    s = (reason or "").strip()
    if not s:
        return "llm_failed"

    # Keep tags stable and countable
    if "404" in s and "/v1/chat/completions" in s:
        return "http_404_chat_completions"
    if "429" in s:
        return "http_429_rate_limited"
    if "502" in s or "503" in s or "504" in s:
        return "http_5xx_gateway"
    if "Read timed out" in s or "timeout" in s.lower():
        return "timeout"
    return "other_error"

def carry_forward_previous_improvements(doc: Dict[str, Any], prev: Optional[Dict[str, Any]]) -> bool:
    """
    If prev has improved output, copy its generated fields into the current doc.

    Returns True only if this actually changed the current doc (meaningful carry-forward),
    and False if it's a no-op (already identical / baseline reconstruction).
    """
    if not prev:
        return False

    if int(prev.get("improved") or 0) != 1:
        return False

    keys = ("ko_content_flat_summarised", "title_llm", "subtitle_llm", "description_llm", "keywords_llm")

    # --- No-op short-circuit: if doc already matches prev, don't count it ---
    # We treat "already identical" as NOT a meaningful change.
    already_identical = (
        int(doc.get("improved") or 0) == 1
        and all(doc.get(k) == prev.get(k) for k in keys)
    )
    if already_identical:
        return False

    changed = False

    # Copy values, but only mark as changed when we actually modify something.
    for k in keys:
        if prev.get(k) is None:
            continue

        # Only overwrite if different; avoids counting baseline reconstruction
        if doc.get(k) != prev.get(k):
            doc[k] = prev[k]
            changed = True

    # carry forward status too (optional, but keeps output self-describing)
    if changed:
        doc["improved"] = 1

        # If after copying we are identical to prev, this is baseline reconstruction:
        # treat as not meaningful (so it won't trigger a new snapshot).
        if (
            int(doc.get("improved") or 0) == int(prev.get("improved") or 0)
            and all(doc.get(k) == prev.get(k) for k in keys)
        ):
            return False

    return changed

