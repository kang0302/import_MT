# scripts/build_master_from_fragments.py
# Build master CSVs by auto-merging all g*/g*_SANITIZED fragment CSVs
# Output:
#   data/master/asset.csv
#   data/master/business_field.csv
#   data/master/theme.csv
#   data/master/character.csv
#   data/master/macro.csv
#
# Rule:
# - Scan repo root for folders: g1, g2, ... and also g*_SANITIZED
# - If the same ID appears multiple times, keep the first seen (stable)
# - Header is taken from the first valid file found for that master type
# - Writes only if at least 1 row exists

from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]  # /import_MT
OUT_DIR = REPO_ROOT / "data" / "master"

# master_name -> (fragment_filename, id_column_candidates)
TARGETS = {
    "asset": ("asset.csv", ["asset_id", "id", "assetId"]),
    "business_field": ("business_field.csv", ["bf_id", "business_field_id", "id"]),
    "theme": ("theme.csv", ["theme_id", "id", "themeId"]),
    "character": ("character.csv", ["character_id", "id", "characterId"]),
    "macro": ("macro.csv", ["macro_id", "id", "macroId"]),
}

G_FOLDER_RE = re.compile(r"^g\d+$", re.IGNORECASE)
G_SAN_RE = re.compile(r"^g\d+_SANITIZED$", re.IGNORECASE)


def list_fragment_dirs(repo_root: Path) -> List[Path]:
    dirs = []
    for p in repo_root.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        if G_FOLDER_RE.match(name) or G_SAN_RE.match(name):
            dirs.append(p)
    # deterministic order: g1,g2,... then g*_SANITIZED also sorted
    dirs.sort(key=lambda x: x.name.lower())
    return dirs


def read_csv_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = []
        for r in reader:
            # normalize keys + values (strip)
            rr = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items() if k is not None}
            rows.append(rr)
        return header, rows


def detect_id_key(header: List[str], candidates: List[str]) -> Optional[str]:
    header_set = {h.strip() for h in header}
    for c in candidates:
        if c in header_set:
            return c
    # fallback: if exact not found, try case-insensitive match
    lower_map = {h.lower(): h for h in header}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def merge_master(master_name: str, fragment_filename: str, id_candidates: List[str], dirs: List[Path]) -> Tuple[List[str], List[Dict[str, str]]]:
    merged: Dict[str, Dict[str, str]] = {}
    out_header: List[str] = []
    id_key: Optional[str] = None
    files_used: List[str] = []

    # scan every dir
    for d in dirs:
        fp = d / fragment_filename
        if not fp.exists():
            continue

        try:
            header, rows = read_csv_rows(fp)
        except Exception as e:
            print(f"[WARN] failed to read {fp}: {e}")
            continue

        if not header or not rows:
            continue

        if not out_header:
            out_header = header[:]  # first header wins
            id_key = detect_id_key(out_header, id_candidates)
            if not id_key:
                print(f"[WARN] {master_name}: cannot detect id column from header: {out_header}")
                # cannot merge without ID column
                return [], []

        files_used.append(str(fp.relative_to(REPO_ROOT)))

        for r in rows:
            rid = (r.get(id_key, "") or "").strip()
            if not rid:
                continue
            # keep first seen
            if rid not in merged:
                # ensure all header columns exist
                normalized = {h: (r.get(h, "") or "").strip() for h in out_header}
                merged[rid] = normalized

    merged_rows = list(merged.values())
    print(f"[OK] {master_name}: rows={len(merged_rows)} files={len(files_used)}")
    if files_used:
        print("      used:", ", ".join(files_used[:8]) + (" ..." if len(files_used) > 8 else ""))
    return out_header, merged_rows


def write_csv(path: Path, header: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({h: r.get(h, "") for h in header})


def main():
    dirs = list_fragment_dirs(REPO_ROOT)
    if not dirs:
        raise SystemExit("[ERR] No g* folders found at repo root.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for master_name, (frag_file, id_candidates) in TARGETS.items():
        header, rows = merge_master(master_name, frag_file, id_candidates, dirs)
        if not header or not rows:
            print(f"[SKIP] {master_name}: no data")
            continue

        out_path = OUT_DIR / f"{master_name}.csv"
        write_csv(out_path, header, rows)
        print(f"[WRITE] {out_path.relative_to(REPO_ROOT)}")

    print("[DONE] master build complete.")


if __name__ == "__main__":
    main()
