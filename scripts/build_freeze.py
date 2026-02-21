import json
import os
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
THEME_DIR = DATA_DIR / "theme"
ASSET_VAL_PATH = DATA_DIR / "valuation" / "kr_valuation.json"

SCHEMA_VERSION = "v5"


def read_json(path: Path):
    if not path.exists():
        raise SystemExit(f"File not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj):
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def load_themes():
    index_path = THEME_DIR / "index.json"
    idx = read_json(index_path)

    # ✅ index.json 포맷 호환
    # 1) {"themes":[...]}  (dict)
    # 2) [ {...}, {...} ]  (list)
    if isinstance(idx, dict):
        themes = idx.get("themes", []) or []
    elif isinstance(idx, list):
        themes = idx
    else:
        raise SystemExit(f"Unexpected index.json format: {type(idx)}")

    if not themes:
        raise SystemExit("index.json에 themes가 비어있습니다.")

    return themes


def load_kr_valuation():
    if not ASSET_VAL_PATH.exists():
        print("⚠ kr_valuation.json not found. Skipping KR metrics injection.")
        return {}

    return read_json(ASSET_VAL_PATH)


def inject_metrics_into_theme(theme_path: Path, kr_val_map: dict):
    theme_obj = read_json(theme_path)

    nodes = theme_obj.get("nodes", [])
    updated = 0

    for node in nodes:
        if node.get("type") != "ASSET":
            continue

        exposure = node.get("exposure", {})
        if exposure.get("country") != "KR":
            continue

        ticker = exposure.get("ticker")
        if not ticker:
            continue

        val = kr_val_map.get(ticker)
        if not val:
            continue

        metrics = node.setdefault("metrics", {})

        metrics["marketCap"] = val.get("marketCap")
        metrics["pe_ttm"] = val.get("pe_ttm")
        metrics["close"] = val.get("close")
        metrics["valuationAsOf"] = val.get("valuationAsOf")

        updated += 1

    write_json(theme_path, theme_obj)
    return updated


def rebuild_index(themes):
    index_path = THEME_DIR / "index.json"

    # index.json이 list 구조인 경우 그대로 유지
    if isinstance(themes, list):
        write_json(index_path, themes)
    else:
        write_json(index_path, {"themes": themes})


def main():
    print("=== Build Freeze Start ===")

    themes = load_themes()
    kr_val_map = load_kr_valuation()

    total_updated = 0

    for t in themes:
        theme_id = None

        if isinstance(t, dict):
            theme_id = t.get("themeId") or t.get("id")
        elif isinstance(t, str):
            theme_id = t

        if not theme_id:
            continue

        theme_path = THEME_DIR / f"{theme_id}.json"

        if not theme_path.exists():
            print(f"⚠ Theme file not found: {theme_path}")
            continue

        updated = inject_metrics_into_theme(theme_path, kr_val_map)
        total_updated += updated

    rebuild_index(themes)

    print(f"✅ KR metrics injected into {total_updated} asset nodes.")
    print("=== Build Freeze Completed ===")


if __name__ == "__main__":
    main()