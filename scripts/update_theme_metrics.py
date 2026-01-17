# scripts/build_master_json.py
# MoneyTree Master Builder (Assets + Business Fields)
# - Scan g*/asset.csv and g*/business_field.csv (also supports businessfield.csv)
# - Build one JSON: data/master/master.json
# - Policy: last write wins by folder order (g1 -> g2 -> ... -> g99). If same id repeats, later g overwrites.

import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = REPO_ROOT / "data" / "master" / "master.json"

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _read_csv_any_delim(path: Path) -> List[Dict[str, str]]:
    # supports comma or tab separated
    text = path.read_text(encoding="utf-8", errors="replace")
    sample = text[:4096]
    delim = "\t" if "\t" in sample and sample.count("\t") > sample.count(",") else ","
    rows: List[Dict[str, str]] = []
    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    for r in reader:
        if not r:
            continue
        rows.append({k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items() if k is not None})
    return rows

def _g_folder_key(p: Path) -> Tuple[int, str]:
    # sort by g-number: g4, g10 ...
    m = re.match(r"^g(\d+)$", p.name.strip())
    if m:
        return (int(m.group(1)), p.name)
    return (10**9, p.name)

def _pick(d: Dict[str, str], *keys: str) -> str:
    for k in keys:
        if k in d and d[k] is not None:
            return str(d[k]).strip()
    return ""

def build_assets(repo_root: Path) -> Dict[str, Any]:
    assets: Dict[str, Any] = {}

    g_dirs = [p for p in repo_root.iterdir() if p.is_dir() and re.match(r"^g\d+$", p.name)]
    g_dirs.sort(key=_g_folder_key)

    for gd in g_dirs:
        # asset file candidates
        cand = [gd / "asset.csv", gd / "assets.csv"]
        for ap in cand:
            if not ap.exists():
                continue
            rows = _read_csv_any_delim(ap)
            for r in rows:
                asset_id = _pick(r, "asset_id", "id", "assetId")
                if not asset_id:
                    continue

                # normalize fields (support multiple column spellings)
                rec = {
                    "asset_id": asset_id,
                    "asset_name_en": _pick(r, "asset_name_en", "name_en", "asset_name_eng"),
                    "asset_name_ko": _pick(r, "asset_name_ko", "name_ko", "asset_name_kor"),
                    "ticker": _pick(r, "ticker"),
                    "exchange": _pick(r, "exchange"),
                    "country": _pick(r, "country"),
                    "asset_type": _pick(r, "asset_type", "type"),
                    "_source": str(ap.relative_to(repo_root)).replace("\\", "/"),
                }

                # last write wins
                assets[asset_id] = rec

    return assets

def build_business_fields(repo_root: Path) -> Dict[str, Any]:
    bfs: Dict[str, Any] = {}

    g_dirs = [p for p in repo_root.iterdir() if p.is_dir() and re.match(r"^g\d+$", p.name)]
    g_dirs.sort(key=_g_folder_key)

    for gd in g_dirs:
        cand = [
            gd / "business_field.csv",
            gd / "businessfield.csv",
            gd / "business_fields.csv",
        ]
        for bp in cand:
            if not bp.exists():
                continue
            rows = _read_csv_any_delim(bp)
            for r in rows:
                bf_id = _pick(r, "bf_id", "business_field_id", "id")
                if not bf_id:
                    continue

                rec = {
                    "bf_id": bf_id,
                    "business_field_ko": _pick(r, "business_field_ko", "bf_ko", "name_ko"),
                    "business_field_en": _pick(r, "business_field_en", "bf_en", "name_en"),
                    "_source": str(bp.relative_to(repo_root)).replace("\\", "/"),
                }

                bfs[bf_id] = rec

    return bfs

def make_label_maps(assets: Dict[str, Any], bfs: Dict[str, Any]) -> Dict[str, str]:
    # labelById: id -> display label (KO preferred)
    out: Dict[str, str] = {}
    for aid, a in assets.items():
        label = (a.get("asset_name_ko") or "").strip() or (a.get("asset_name_en") or "").strip() or aid
        out[aid] = label
    for bid, b in bfs.items():
        label = (b.get("business_field_ko") or "").strip() or (b.get("business_field_en") or "").strip() or bid
        out[bid] = label
    return out

def main():
    assets = build_assets(REPO_ROOT)
    bfs = build_business_fields(REPO_ROOT)
    label_by_id = make_label_maps(assets, bfs)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generatedAtUTC": _now_utc_iso(),
        "counts": {
            "assets": len(assets),
            "businessFields": len(bfs),
            "labels": len(label_by_id),
        },
        "assets": assets,
        "businessFields": bfs,
        "labelById": label_by_id,
    }

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[OK] wrote:", str(OUT_PATH.relative_to(REPO_ROOT)))

if __name__ == "__main__":
    main()
