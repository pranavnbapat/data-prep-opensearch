# analyse_json_mime.py

"""
Analyse a JSON file to:
1) Count MIME types (group-by)
2) Find keys that appear only in some objects ("unique-ish" fields)
3) For those keys, report which MIME types tend to co-occur with them

Usage:
  python3 analyse_json_mime.py --input /path/to/file.json
  python3 analyse_json_mime.py --input file.json --topkey records
  python3 analyse_json_mime.py --input file.json --out_prefix report

Notes:
- Supports:
  * JSON list
  * JSON dict containing a list (auto-detected or via --topkey)
  * NDJSON (one JSON object per line)
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple


MIME_RE = re.compile(r"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$", re.IGNORECASE)


def _looks_like_mime(s: str) -> bool:
    s = s.strip()
    if ";" in s:
        s = s.split(";", 1)[0].strip()
    return bool(MIME_RE.match(s))


def _normalise_mime(s: str) -> str:
    s = s.strip()
    if ";" in s:
        s = s.split(";", 1)[0].strip()
    return s.lower()


def load_json_flexible(path: str) -> Any:
    """
    Load JSON that may be:
      - normal JSON (list/dict)
      - NDJSON (one object per line)
    """
    with open(path, "r", encoding="utf-8") as f:
        first = f.read(1)
        f.seek(0)

        # Quick heuristic: NDJSON often doesn't start with [ or { (but it can start with { too).
        # We try normal json.load first; if it fails, fall back to NDJSON.
        try:
            return json.load(f)
        except json.JSONDecodeError:
            f.seek(0)
            items = []
            for ln_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except json.JSONDecodeError as e:
                    raise SystemExit(f"Failed to parse NDJSON at line {ln_no}: {e}") from e
            return items


def find_first_list_container(obj: Any) -> Optional[Tuple[str, List[Any]]]:
    """
    If obj is a dict that contains a list-of-dicts somewhere at top-level,
    return (key, list_value) for the first plausible candidate.
    """
    if not isinstance(obj, dict):
        return None
    best_key = None
    best_list = None
    best_len = 0

    for k, v in obj.items():
        if isinstance(v, list) and v:
            # Prefer lists of dicts
            dictish = sum(1 for x in v if isinstance(x, dict))
            if dictish > 0 and len(v) > best_len:
                best_key, best_list, best_len = k, v, len(v)

    if best_key is None:
        return None
    return best_key, best_list  # type: ignore[return-value]


def extract_mimes_from_obj(o: Dict[str, Any]) -> List[str]:
    """
    Extract MIME types from common patterns in a record.
    Returns list because an object can contain multiple media items / attachments.
    """
    found: List[str] = []

    # Common direct fields
    # FarmBook-ish / KO-ish direct fields
    for key in (
            "ko_hosted_mime_type",
            "ko_object_mimetype",
            "ko_external_content_type",  # not a MIME, but useful fallback classification
    ):
        v = o.get(key)
        if isinstance(v, str):
            # ko_external_content_type is not a true MIME, so don't run MIME regex on it
            if key == "ko_external_content_type" and v.strip():
                # We'll treat it as a pseudo-mime bucket like "external/youtube"
                found.append(f"external/{v.strip().lower()}")
            elif _looks_like_mime(v):
                found.append(_normalise_mime(v))

    # HTTP-like headers
    headers = o.get("headers")
    if isinstance(headers, dict):
        for hk in ("content-type", "Content-Type", "CONTENT-TYPE"):
            hv = headers.get(hk)
            if isinstance(hv, str) and _looks_like_mime(hv):
                found.append(_normalise_mime(hv))

    # Nested common blobs: media / file / attachment(s)
    for container_key in ("media", "file", "files", "attachment", "attachments", "assets"):
        v = o.get(container_key)
        if isinstance(v, dict):
            found.extend(extract_mimes_from_nested(v))
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    found.extend(extract_mimes_from_nested(item))

    # Fallback buckets when MIME is genuinely absent
    if not found:
        if o.get("is_url_only") is True:
            # e.g. URL-only objects often don't have file MIME types
            ext = o.get("ko_external_content_type")
            if isinstance(ext, str) and ext.strip():
                found.append(f"external/{ext.strip().lower()}")
            else:
                found.append("url-only/(unknown)")
        else:
            # Non-url objects with no MIME is suspicious
            found.append("(missing)")

    # If MIME is generic, try to infer something more specific from extension
    if "application/octet-stream" in found:
        ext = o.get("ko_object_extension")
        if isinstance(ext, str) and ext.lower() == ".pdf":
            # prefer the inferred real type
            found = [m for m in found if m != "application/octet-stream"]
            found.insert(0, "application/pdf")

    # Deduplicate but keep stable-ish order
    deduped = []
    seen = set()
    for m in found:
        if m not in seen:
            deduped.append(m)
            seen.add(m)
    return deduped


def extract_mimes_from_nested(d: Dict[str, Any]) -> List[str]:
    found: List[str] = []
    for key in ("mime", "mimetype", "mime_type", "content_type", "contentType"):
        v = d.get(key)
        if isinstance(v, str) and _looks_like_mime(v):
            found.append(_normalise_mime(v))

    # Sometimes it’s stored as "format": "video/mp4"
    v = d.get("format")
    if isinstance(v, str) and _looks_like_mime(v):
        found.append(_normalise_mime(v))

    return found


def deep_scan_for_mimes(obj: Any, max_depth: int) -> List[str]:
    """
    Conservative deep scan: finds MIME-looking strings anywhere in structure.
    Limits recursion depth to avoid heavy scans.
    """
    found: List[str] = []

    def _walk(x: Any, depth: int) -> None:
        if depth > max_depth:
            return
        if isinstance(x, str):
            if _looks_like_mime(x):
                found.append(_normalise_mime(x))
            return
        if isinstance(x, dict):
            for vv in x.values():
                _walk(vv, depth + 1)
        elif isinstance(x, list):
            for vv in x:
                _walk(vv, depth + 1)

    _walk(obj, 0)
    return found


def flatten_keys(obj: Any, prefix: str = "", out: Optional[set] = None, max_depth: int = 6) -> set:
    """
    Flatten keys into dotted paths for dicts. For lists, we do not include indices
    (we treat list items as the same "shape").
    """
    if out is None:
        out = set()
    if max_depth < 0:
        return out

    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else str(k)
            out.add(path)
            flatten_keys(v, path, out, max_depth=max_depth - 1)
    elif isinstance(obj, list):
        for item in obj:
            flatten_keys(item, prefix, out, max_depth=max_depth - 1)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to JSON or NDJSON file")
    ap.add_argument("--topkey", default="", help="If root is a dict, use this key as the list container")
    ap.add_argument("--out_prefix", default="", help="Write CSV reports with this prefix (optional)")
    ap.add_argument("--max_key_depth", type=int, default=6, help="Max depth for key-flattening")
    args = ap.parse_args()

    raw = load_json_flexible(args.input)

    # Identify records list
    records: List[Dict[str, Any]]
    if isinstance(raw, list):
        records = [x for x in raw if isinstance(x, dict)]
    elif isinstance(raw, dict):
        if args.topkey:
            v = raw.get(args.topkey)
            if not isinstance(v, list):
                raise SystemExit(f"--topkey '{args.topkey}' is not a list in the JSON root.")
            records = [x for x in v if isinstance(x, dict)]
        else:
            guess = find_first_list_container(raw)
            if not guess:
                raise SystemExit("JSON root is a dict and I couldn't find a list-of-dicts. Use --topkey.")
            k, v = guess
            records = [x for x in v if isinstance(x, dict)]
            print(f"[info] Auto-detected records list under key: '{k}' (len={len(records)})")
    else:
        raise SystemExit("Unsupported JSON root type. Expected list or dict.")

    if not records:
        raise SystemExit("No dict records found to analyse.")

    # MIME group-by
    mime_counter = Counter()
    record_mimes: List[List[str]] = []
    for rec in records:
        mimes = extract_mimes_from_obj(rec)
        record_mimes.append(mimes)
        if not mimes:
            mime_counter["(missing)"] += 1
        elif len(mimes) == 1:
            mime_counter[mimes[0]] += 1
        else:
            # If multiple, count each, but also keep a combined label so you can spot multi-asset records
            for m in mimes:
                mime_counter[m] += 1
            mime_counter["(multiple_per_record)"] += 1

    print("\n=== MIME type counts ===")
    for mime, c in mime_counter.most_common():
        print(f"{mime}\t{c}")

    # Key presence frequencies (flattened paths)
    key_freq = Counter()
    keys_per_record: List[set] = []
    for rec in records:
        ks = flatten_keys(rec, max_depth=args.max_key_depth)
        keys_per_record.append(ks)
        key_freq.update(ks)

    n = len(records)

    # "Unique-ish" keys: appear in <100% of records.
    # You can tighten this further later, e.g., only keys that appear in <= 5 records.
    uniqueish = [(k, c) for k, c in key_freq.items() if c < n]
    uniqueish.sort(key=lambda x: (x[1], x[0]))

    print("\n=== Keys not present in all records (unique-ish fields) ===")
    # Print the rarest first
    for k, c in uniqueish[:200]:
        print(f"{k}\t{c}/{n}")

    # For each key, collect co-occurring MIME types (based on per-record extracted MIME list)
    key_to_mimes: Dict[str, Counter] = defaultdict(Counter)
    for idx, ks in enumerate(keys_per_record):
        mimes = record_mimes[idx]
        mime_bucket = mimes if mimes else ["(missing)"]
        for k in ks:
            key_to_mimes[k].update(mime_bucket)

    print("\n=== MIME types associated with rare keys (rarest 50) ===")
    for k, c in uniqueish[:50]:
        top = key_to_mimes[k].most_common(5)
        top_str = ", ".join([f"{m}:{cnt}" for m, cnt in top])

        total_for_key = sum(key_to_mimes[k].values())
        top_mime, top_cnt = key_to_mimes[k].most_common(1)[0]
        purity = top_cnt / total_for_key if total_for_key else 0.0

        print(f"{k}\t{c}/{n}\tpurity={purity:.2%}\t{top_str}")

    # Optional CSV output
    if args.out_prefix:
        import csv

        mime_path = f"{args.out_prefix}_mime_counts.csv"
        with open(mime_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["mime_type", "count"])
            for mime, c in mime_counter.most_common():
                w.writerow([mime, c])

        keys_path = f"{args.out_prefix}_key_frequency.csv"
        with open(keys_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["key_path", "present_in_records", "total_records"])
            for k, c in key_freq.most_common():
                w.writerow([k, c, n])

        rare_path = f"{args.out_prefix}_rare_keys_mime.csv"
        with open(rare_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["key_path", "present_in_records", "total_records", "top_mimes"])
            for k, c in uniqueish:
                top = key_to_mimes[k].most_common(10)
                top_str = "; ".join([f"{m}:{cnt}" for m, cnt in top])
                w.writerow([k, c, n, top_str])

        print(f"\n[info] Wrote:\n- {mime_path}\n- {keys_path}\n- {rare_path}")


if __name__ == "__main__":
    main()
