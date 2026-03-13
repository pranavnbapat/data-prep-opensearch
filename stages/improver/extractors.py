# improver_extractors.py

from __future__ import annotations

import json
import re

from typing import Any, Dict, List


_SUMMARY_FIELD_RE = re.compile(r'"summary"\s*:\s*"', re.I)


def _strip_fences(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("```"):
        s = s.split("```", 1)[-1].strip()
        if s.lower().startswith("json"):
            s = s[4:].strip()
    if "```" in s:
        s = s.split("```", 1)[0].strip()
    return s


def _extract_summary_from_dict(obj: Any) -> Dict[str, Any]:
    if not isinstance(obj, dict) or "summary" not in obj:
        raise ValueError("Summary output must contain a summary field")
    summary = obj.get("summary")
    if not isinstance(summary, str) or not summary.strip():
        raise ValueError("Empty summary")
    return {"summary": summary.strip()}


def extract_summary_json(raw: str) -> Dict[str, Any]:
    s = _strip_fences(raw)

    # Best case: valid JSON object.
    try:
        return _extract_summary_from_dict(json.loads(s))
    except Exception:
        pass

    # Common case: extra prose before/after a JSON object.
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return _extract_summary_from_dict(json.loads(s[start:end + 1]))
        except Exception:
            pass

    # Salvage case: malformed JSON but visible `"summary": "..."` field.
    m = _SUMMARY_FIELD_RE.search(s)
    if m:
        tail = s[m.end():].lstrip()
        if tail.startswith('"'):
            tail = tail[1:]
        buf: list[str] = []
        escaped = False
        for ch in tail:
            if escaped:
                buf.append(ch)
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                buf.append(ch)
                continue
            if ch == '"':
                break
            buf.append(ch)
        candidate = "".join(buf).strip()
        if candidate:
            try:
                candidate = json.loads(f'"{candidate}"')
            except Exception:
                pass
            if isinstance(candidate, str) and candidate.strip():
                return {"summary": candidate.strip()}

    # Last resort: treat the raw output as plain summary text.
    plain = s.strip()
    if plain.startswith("{") and plain.endswith("}"):
        plain = plain[1:-1].strip()
    plain = _SUMMARY_FIELD_RE.sub("", plain).strip(" \n\r\t:,'\"")
    if plain:
        return {"summary": plain}
    raise ValueError("Empty summary")


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
