# scripts/build_master_json.py
# MoneyTree Master Builder (Assets + Business Fields)
#
# - Scan g*/asset.csv and g*/business_field.csv (also supports businessfield.csv)
# - Build one JSON: data/master/master.json
# - Policy: last write wins by folder order (g1 -> g2 -> ... -> g99).
#   If same id repeats, later g overwrites.
#
# Enhancements (final):
# - Robust CSV reading (comma/tab, BOM safe)
# - Stable output ordering (sorted keys) to reduce diff noise
# - Optional mirror output to public/data/master/master.json for web reflection
# - Overwrite logs to help trace duplicates

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]

OUT_PATH = REPO_ROOT / "data" / "master" / "master.json"
PUBLIC_OUT_PATH = REPO_ROOT / "public" / "data" / "master" / "master.json"


# -----------------------------
# helpers
# -----------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _read_csv_any_delim(path: Path) -> List[Dict[str, str]]:
    """
    Supports comma or tab separated.
    Also safe for UTF-8 BOM and weird encodings.
    """
    text = path.read_text(encoding="utf-8", errors="replace")

    # BOM 제거(헤더에 숨어들어가는 케이스 방지)
    if text.startswith("\ufeff"):
        text = text.lstrip("\ufeff")

    sample = text[:4096]
    delim = "\t" if ("\t" in sample and sample.count("\t") > sample.count(",")) else ","

    rows: List[Dict[str, str]] = []
    reader = csv.DictReader(text.splitlines(), delimiter=delim)

    for r in reader:
        if not r:
            continue

        # 완전 빈 행 스킵
        if all((v is None or str(v).strip() == "") for v in r.values()):
            continue

        cleaned = {}
        for k, v in r.items():
            if k is None:
                continue
            key = str(k).strip()
            val = (v.strip() if isinstance(v, str) else v)
            cleaned[key] = val
        rows.append(cleaned)

    return rows


def _list_g_dirs(repo_root: Path) -> List[Path]:
    g_dirs = [p for p in repo_root.iterdir() if p.is_dir() and re.match(r"^g\d+$", p.name)]
    g_dirs.sort(key=_g_folder_key)
    return g_dirs


# -----------------------------
# builders
# -----------------------------
def build_assets(repo_root: Path) -> Dict[str, Any]:
    assets: Dict[str, Any] = {}
    overwrite_log: List[str] = []

    for gd in _list_g_dirs(repo_root):
        cand = [gd / "asset.csv", gd / "assets.csv"]
        for ap in cand:
            if not ap.exists():
                continue

            rows = _read_csv_any_delim(ap)
            for r in rows:
                asset_id = _pick(r, "asset_id", "id", "assetId")
                if not asset_id:
                    continue

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

                if asset_id in assets:
                    overwrite_log.append(
                        f"[ASSET overwrite] {asset_id}: {assets[asset_id].get('_source')} -> {rec.get('_source')}"
                    )

                assets[asset_id] = rec

    if overwrite_log:
        print(f"[INFO] asset overwrites: {len(overwrite_log)}")
        # 너무 길면 부담이니 상위 30개만 출력
        for line in overwrite_log[:30]:
            print(" ", line)
        if len(overwrite_log) > 30:
            print("  ... (truncated)")

    return assets


def build_business_fields(repo_root: Path) -> Dict[str, Any]:
    bfs: Dict[str, Any] = {}
    overwrite_log: List[str] = []

    for gd in _list_g_dirs(repo_root):
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

                if bf_id in bfs:
                    overwrite_log.append(
                        f"[BF overwrite] {bf_id}: {bfs[bf_id].get('_source')} -> {rec.get('_source')}"
                    )

                bfs[bf_id] = rec

    if overwrite_log:
        print(f"[INFO] business field overwrites: {len(overwrite_log)}")
        for line in overwrite_log[:30]:
            print(" ", line)
        if len(overwrite_log) > 30:
            print("  ... (truncated)")

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


def _stable_sorted_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    # stable ordering for JSON (to reduce diff noise)
    return dict(sorted(d.items(), key=lambda kv: kv[0]))


# -----------------------------
# main
# -----------------------------
def main() -> None:
    assets = build_assets(REPO_ROOT)
    bfs = build_business_fields(REPO_ROOT)
    label_by_id = make_label_maps(assets, bfs)

    # Stable ordering (IDs sorted)
    assets_sorted = _stable_sorted_dict(assets)
    bfs_sorted = _stable_sorted_dict(bfs)
    label_sorted = _stable_sorted_dict(label_by_id)

    payload = {
        "generatedAtUTC": _now_utc_iso(),
        "counts": {
            "assets": len(assets_sorted),
            "businessFields": len(bfs_sorted),
            "labels": len(label_sorted),
        },
        "assets": assets_sorted,
        "businessFields": bfs_sorted,
        "labelById": label_sorted,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[OK] wrote:", str(OUT_PATH.relative_to(REPO_ROOT)).replace("\\", "/"))

    # Optional mirror for web reflection
    if PUBLIC_OUT_PATH.parent.exists():
        PUBLIC_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        PUBLIC_OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[OK] mirrored:", str(PUBLIC_OUT_PATH.relative_to(REPO_ROOT)).replace("\\", "/"))
    else:
        # public/ 폴더가 없는 레포에서도 에러 없이 넘어가도록
        print("[INFO] public/data/master not found. skip mirror.")


if __name__ == "__main__":
    main()