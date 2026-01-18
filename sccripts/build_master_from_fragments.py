#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build consolidated master CSVs from g*/ or g*_SANITIZED fragments.

Outputs:
  data/master/asset.csv
  data/master/business_field.csv
  data/master/character.csv
  data/master/macro.csv
  data/master/theme.csv
  data/master/_build_report.json

Priority (winner when duplicate IDs):
  1) SANITIZED folders win over non-sanitized
  2) Higher g number wins (g10 > g9 > ... > g1)
  3) If still tied, later discovered file wins (stable within run)

Usage:
  python scripts/build_master_from_fragments.py
"""

from __future__ import annotations

import csv
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "master"

# master filename -> (id_column, expected_headers_optional)
MASTER_SPECS = {
    "asset.csv": ("asset_id", None),
    "business_field.csv": ("bf_id", None),
    "character.csv": ("character_id", None),
    "macro.csv": ("macro_id", None),
    "theme.csv": ("theme_id", None),
}

# Where to look for fragments (in priority order by folder type)
# We'll still apply numeric ordering later; this is just scanning patterns.
FRAGMENT_GLOBS = [
    "g*_SANITIZED",  # preferred
    "g*",            # fallback (includes g6 etc.)
]

# Ignore folders that are obviously not gNN or gNN_SANITIZED (we’ll filter strictly anyway)
G_DIR_RE = re.compile(r"^g(\d+)(?:_SANITIZED)?$", re.IGNORECASE)


def is_sanitized_dir(p: Path) -> bool:
    return p.name.lower().endswith("_sanitized")


def extract_g_num(p: Path) -> Optional[int]:
    m = G_DIR_RE.match(p.name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except:
        return None


def list_g_dirs(root: Path) -> List[Path]:
    dirs: List[Path] = []
    for pattern in FRAGMENT_GLOBS:
        for p in root.glob(pattern):
            if not p.is_dir():
                continue
            gnum = extract_g_num(p)
            if gnum is None:
                continue
            dirs.append(p)

    # Deduplicate same path
    dirs = sorted(set(dirs), key=lambda x: x.name.lower())

    # Sort by: sanitized first, then gnum desc
    def key(p: Path):
        gnum = extract_g_num(p) or -1
        return (0 if is_sanitized_dir(p) else 1, -gnum)

    dirs.sort(key=key)
    return dirs


def safe_read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Reads CSV with csv.DictReader.
    Returns (fieldnames, rows).
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return ([], [])
        fieldnames = [h.strip() for h in reader.fieldnames]
        rows: List[Dict[str, str]] = []
        for r in reader:
            # normalize keys
            norm = { (k or "").strip(): (v or "").strip() for k, v in r.items() }
            rows.append(norm)
        return (fieldnames, rows)


def write_csv(path: Path, headers: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({h: r.get(h, "") for h in headers})


def merge_headers(base: List[str], extra: List[str]) -> List[str]:
    seen = set(base)
    out = list(base)
    for h in extra:
        h2 = (h or "").strip()
        if not h2:
            continue
        if h2 not in seen:
            out.append(h2)
            seen.add(h2)
    return out


def build_one_master(
    g_dirs: List[Path],
    filename: str,
    id_col: str,
) -> Tuple[List[str], List[Dict[str, str]], Dict]:
    """
    Merge all fragments for a given master filename.
    """
    winners: Dict[str, Dict[str, str]] = {}
    winner_src: Dict[str, str] = {}
    headers: List[str] = [id_col]
    used_files: List[str] = []
    total_rows = 0
    kept_rows = 0
    skipped_no_id = 0

    # Scan in priority order. Because list_g_dirs already sorted:
    # sanitized first, then gnum desc.
    for gdir in g_dirs:
        f = gdir / filename
        if not f.exists():
            continue

        used_files.append(str(f.relative_to(ROOT)))
        fieldnames, rows = safe_read_csv(f)
        if not fieldnames or not rows:
            continue

        headers = merge_headers(headers, fieldnames)

        for r in rows:
            total_rows += 1
            rid = (r.get(id_col) or "").strip()
            if not rid:
                skipped_no_id += 1
                continue

            # Priority: because we iterate from highest priority to lower priority,
            # we keep the first seen and do NOT overwrite.
            if rid in winners:
                continue

            winners[rid] = r
            winner_src[rid] = str(f.relative_to(ROOT))
            kept_rows += 1

    # Stable ordering by ID (numeric aware for A_001 etc.)
    def sort_key(x: str):
        # split prefix + number if possible
        m = re.match(r"^([A-Za-z_]+)(\d+)$", x.replace("-", "_"))
        if m:
            return (m.group(1), int(m.group(2)))
        return (x, 10**18)

    merged_rows = [winners[k] for k in sorted(winners.keys(), key=sort_key)]

    report = {
        "filename": filename,
        "id_col": id_col,
        "used_files": used_files,
        "total_rows_scanned": total_rows,
        "rows_kept_unique": kept_rows,
        "rows_skipped_no_id": skipped_no_id,
        "unique_ids": len(winners),
        "sample_sources": dict(list(winner_src.items())[:5]),
    }
    return headers, merged_rows, report


def main():
    g_dirs = list_g_dirs(ROOT)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    build_report = {
        "root": str(ROOT),
        "g_dirs_priority_order": [str(p.relative_to(ROOT)) for p in g_dirs],
        "masters": {},
    }

    changed_any = False

    for out_name, (id_col, _headers_opt) in MASTER_SPECS.items():
        headers, rows, report = build_one_master(g_dirs, out_name, id_col)

        out_path = OUT_DIR / out_name
        prev_text = out_path.read_text(encoding="utf-8") if out_path.exists() else ""

        # Write to temp then compare
        tmp_path = OUT_DIR / (out_name + ".tmp")
        write_csv(tmp_path, headers, rows)
        new_text = tmp_path.read_text(encoding="utf-8")

        if new_text != prev_text:
            changed_any = True
            tmp_path.replace(out_path)
        else:
            tmp_path.unlink(missing_ok=True)

        build_report["masters"][out_name] = report

    # report json
    report_path = OUT_DIR / "_build_report.json"
    report_text = json.dumps(build_report, ensure_ascii=False, indent=2)

    prev_report = report_path.read_text(encoding="utf-8") if report_path.exists() else ""
    if report_text != prev_report:
        changed_any = True
        report_path.write_text(report_text, encoding="utf-8")

    print("[OK] master build completed.")
    print(f"- output dir: {OUT_DIR.relative_to(ROOT)}")
    print(f"- changed_any: {changed_any}")

    # exit code: 0 always (workflow should not fail just because no changes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
