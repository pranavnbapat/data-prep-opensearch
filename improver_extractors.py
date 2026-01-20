# improver_extractors.py

from __future__ import annotations

import json

from typing import Any, Dict, List


def extract_summary_json(raw: str) -> Dict[str, Any]:
    s = (raw or "").strip()

    # Strip markdown fences if the model returns them
    if s.startswith("```"):
        s = s.split("```", 1)[-1].strip()
    if "```" in s:
        s = s.split("```", 1)[0].strip()

    obj = json.loads(s)

    if not isinstance(obj, dict) or "summary" not in obj or len(obj.keys()) != 1:
        raise ValueError("Summary output must be JSON: {'summary': '...'}")
    if not isinstance(obj["summary"], str) or not obj["summary"].strip():
        raise ValueError("Empty summary")
    return {"summary": obj["summary"].strip()}


def extract_metadata_text(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        raise ValueError("Empty metadata text")
    # tolerate accidental {"summary": "..."} wrapper
    try:
        maybe = json.loads(s)
        if isinstance(maybe, dict) and isinstance(maybe.get("summary"), str) and maybe["summary"].strip():
            return maybe["summary"].strip()
    except Exception:
        pass
    return s


def extract_metadata_keywords(raw: str) -> List[str]:
    s = (raw or "").strip()
    if not s:
        raise ValueError("Empty metadata keywords")

    # JSON list
    try:
        maybe = json.loads(s)
        if isinstance(maybe, list) and all(isinstance(x, str) for x in maybe):
            kws = [x.strip() for x in maybe if x.strip()]
            if kws:
                return kws
        if isinstance(maybe, dict) and isinstance(maybe.get("summary"), str):
            s = maybe["summary"].strip()
    except Exception:
        pass

    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        raise ValueError("Could not parse keywords")
    return parts
