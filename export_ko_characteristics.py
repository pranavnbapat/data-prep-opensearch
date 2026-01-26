# export_ko_characteristics.py
from __future__ import annotations

import argparse
import csv
import json

from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests

from urllib.parse import urlparse


COLUMNS: List[str] = ["_id", "ko_is_hosted", "ko_object_mimetype", "ko_file_id", "@id", "@id_resolved", "@id_final_domain", "@id_redirect_hops", "enrich_via", "ko_type"]

def resolve_final_url(url: str, timeout: int = 10) -> tuple[str | None, int]:
    """
    Resolve HTTP redirects and return (final_url, hop_count).
    Uses HEAD first (cheap), falls back to GET if needed.
    """
    if not isinstance(url, str) or not url.strip():
        return None, 0

    u = url.strip()

    try:
        # HEAD is usually enough and cheap
        r = requests.head(u, allow_redirects=True, timeout=timeout)
        if r.status_code >= 400:
            # Some servers don’t support HEAD properly; fall back to GET
            r = requests.get(u, allow_redirects=True, timeout=timeout)

        final_url = r.url
        hops = len(getattr(r, "history", []) or [])
        return final_url, hops
    except Exception:
        return None, 0


def domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def iter_docs(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Yield doc dicts safely from the JSON payload."""
    docs = payload.get("docs", [])
    if isinstance(docs, list):
        for d in docs:
            if isinstance(d, dict):
                yield d

def is_media_mimetype(mimetype: str) -> bool:
    """True if mimetype starts with video/, audio/, or image/."""
    mt = (mimetype or "").strip().lower()
    return mt.startswith("video/") or mt.startswith("audio/") or mt.startswith("image/")


def looks_like_media_url(url: str) -> bool:
    """
    Heuristic for URL-only KOs: decide if URL likely needs transcription.
    - YouTube (youtube.com, youtu.be)
    - Common media file extensions
    - Some common media host patterns
    """
    u = (url or "").strip().lower()
    if not u:
        return False

    # YouTube
    if "youtube.com" in u or "youtu.be" in u:
        return True

    # Common media extensions
    media_exts = (
        ".mp4", ".mov", ".mkv", ".webm", ".avi",
        ".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg",
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".tiff",
    )
    if any(u.endswith(ext) for ext in media_exts):
        return True

    # Light host hints (optional, but useful)
    media_host_hints = ("vimeo.com", "soundcloud.com", "podcast", "audio", "video")
    if any(h in u for h in media_host_hints):
        return True

    return False


def derive_enrich_via_and_type(doc: Dict[str, Any]) -> Dict[str, str]:
    """
    Implements rules + a tie-breaker for the duplicated URL-only condition:
    - URL looks like media -> api_transcribe
    - otherwise -> pagesense
    """
    hosted = bool(doc.get("ko_is_hosted"))
    mimetype = (doc.get("ko_object_mimetype") or "").strip()
    file_id = (doc.get("ko_file_id") or "").strip()
    url = (doc.get("@id") or "").strip()

    ko_type = "file" if hosted else "URL"
    enrich_via = ""

    if hosted:
        # Hosted file KO
        if is_media_mimetype(mimetype):
            enrich_via = "custom_transcribe"
        else:
            enrich_via = ""
    else:
        # URL KO (not hosted)
        if mimetype == "" and file_id == "":
            # URL-only case: choose between pagesense vs api_transcribe
            enrich_via = "api_transcribe" if looks_like_media_url(url) else "pagesense"
        else:
            # If it has a media mimetype, assume transcription; else pagesense.
            enrich_via = "api_transcribe" if is_media_mimetype(mimetype) else "pagesense"

    return {"enrich_via": enrich_via, "ko_type": ko_type}


def to_cell(value: Any) -> str:
    """
    Convert values to CSV-friendly strings.
    - None / missing -> ""
    - bool -> "true"/"false" (more CSV-friendly than Python True/False)
    - everything else -> str(value)
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export EU-FarmBook KO characteristics from JSON to CSV."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the JSON file (downloader/enricher output).",
    )
    parser.add_argument(
        "--output",
        default="euf_ko_characteristics.csv",
        help="Path to output CSV file (default: euf_ko_characteristics.csv).",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    # Cache resolutions so repeated URLs don't cause repeated HTTP calls
    resolved_cache: Dict[str, tuple[str | None, int]] = {}

    # Read JSON
    with input_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    # Write CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for doc in iter_docs(payload):
            derived = derive_enrich_via_and_type(doc)

            # Start with the base columns pulled from the doc
            row = {col: to_cell(doc.get(col)) for col in COLUMNS}

            # Overwrite/insert derived fields (ensures they’re present even if missing in doc)
            row["enrich_via"] = derived["enrich_via"]
            row["ko_type"] = derived["ko_type"]

            raw_url = (doc.get("@id") or "").strip() if isinstance(doc.get("@id"), str) else ""
            if raw_url:
                if raw_url not in resolved_cache:
                    resolved_cache[raw_url] = resolve_final_url(raw_url, timeout=10)
                final_url, hops = resolved_cache[raw_url]
            else:
                final_url, hops = None, 0

            row["@id_resolved"] = to_cell(final_url)
            row["@id_redirect_hops"] = str(hops) if hops else ""
            row["@id_final_domain"] = domain(final_url) if final_url else ""

            writer.writerow(row)

    print(f"Wrote CSV: {output_path.resolve()}")


if __name__ == "__main__":
    main()
