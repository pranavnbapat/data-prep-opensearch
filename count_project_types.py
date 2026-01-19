#!/usr/bin/env python3
"""
Count occurrences of project_type in a JSON export like:
{
  "meta": {...},
  "stats": {...},
  "docs": [ { ... "project_type": "Horizon 2020" }, ... ]
}

Usage (Kubuntu):
  python3 scripts/count_project_types.py --input /path/to/file.json
  python3 scripts/count_project_types.py --input file.json --csv-out project_type_counts.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple


def iter_docs(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """Yield doc objects from payload['docs'], handling absent/non-list gracefully."""
    docs = payload.get("docs", [])
    if isinstance(docs, list):
        for d in docs:
            if isinstance(d, dict):
                yield d


def normalise_project_name(value: Any) -> str:
    """
    Normalise project_name for listing.
    Missing/None/"" -> "(missing)"
    Strips whitespace.
    """
    if value is None:
        return "(missing)"
    if isinstance(value, str):
        v = value.strip()
        return v if v else "(missing)"
    return str(value).strip() or "(missing)"


def normalise_project_acronym(value: Any) -> str:
    """
    Optional: normalise project_acronym for nicer display.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalise_project_type(value: Any) -> str:
    """
    Turn project_type into a stable key for counting.
    - Missing/None/"" -> "(missing)"
    - Non-string types -> JSON-ish string
    - Strips whitespace
    """
    if value is None:
        return "(missing)"
    if isinstance(value, str):
        v = value.strip()
        return v if v else "(missing)"
    # Fallback for unexpected types (rare, but keeps script safe)
    return str(value).strip() or "(missing)"


def collect_project_type_stats(
    docs: Iterable[Dict[str, Any]],
) -> tuple[Counter[str], dict[str, set[str]], dict[str, set[str]]]:
    """
    Returns:
      - counts: project_type -> count of docs
      - names_by_type: project_type -> set of project_name values seen
      - acronyms_by_type: project_type -> set of project_acronym values seen (optional)
    """
    counts: Counter[str] = Counter()
    names_by_type: dict[str, set[str]] = {}
    acronyms_by_type: dict[str, set[str]] = {}

    for d in docs:
        ptype = normalise_project_type(d.get("project_type"))
        pname = normalise_project_name(d.get("project_name"))
        pacr = normalise_project_acronym(d.get("project_acronym"))

        counts[ptype] += 1

        names_by_type.setdefault(ptype, set()).add(pname)
        if pacr:
            acronyms_by_type.setdefault(ptype, set()).add(pacr)

    return counts, names_by_type, acronyms_by_type


def sorted_items(counter: Counter[str]) -> Iterable[Tuple[str, int]]:
    """Sort by count descending, then key ascending for stable output."""
    return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))


def main() -> None:
    parser = argparse.ArgumentParser(description="Count project_type values in docs[]")
    parser.add_argument("--input", "-i", required=True, help="Path to JSON file")
    parser.add_argument("--csv-out", help="Optional: write results to CSV (columns: project_type,count)")
    parser.add_argument("--out", help="Optional: write the printed report to this file")
    args = parser.parse_args()

    out_fh = None

    def writeln(msg: str = "") -> None:
        """Write to stdout and optionally also to --out file."""
        print(msg)
        if out_fh is not None:
            out_fh.write(msg + "\n")

    try:
        if args.out:
            out_fh = open(args.out, "w", encoding="utf-8")

        in_path = Path(args.input)
        with in_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        counts, names_by_type, acronyms_by_type = collect_project_type_stats(iter_docs(payload))

        total_docs = sum(counts.values())
        writeln(f"Total docs counted: {total_docs}")
        writeln("=== project_type counts ===")
        for project_type, n in sorted_items(counts):
            writeln(f"{n:>6}  {project_type}")

        writeln()
        writeln("=== project names by project_type ===")
        for project_type, n in sorted_items(counts):
            # Sort names alphabetically; keep "(missing)" last for readability
            names = sorted(
                names_by_type.get(project_type, set()),
                key=lambda s: (s == "(missing)", s.lower()),
            )
            writeln(f"-- {project_type} ({len(names)} projects; {n} docs) --")
            for name in names:
                writeln(f"  - {name}")

            acronyms = sorted(acronyms_by_type.get(project_type, set()), key=lambda s: s.lower())
            if acronyms:
                writeln(f"  [Acronyms: {', '.join(acronyms)}]")
            writeln()  # blank line between groups

        if args.csv_out:
            out_path = Path(args.csv_out)
            with out_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["project_type", "count"])
                for project_type, n in sorted_items(counts):
                    writer.writerow([project_type, n])

            writeln(f"Wrote CSV: {out_path.resolve()}")

    finally:
        if out_fh is not None:
            out_fh.close()

if __name__ == "__main__":
    main()