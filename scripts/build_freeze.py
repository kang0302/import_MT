# import_MT/scripts/build_freeze.py
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
THEME_DIR = DATA_DIR / "theme"
VAL_PATH = DATA_DIR / "valuation" / "kr_valuation.json"


def read_json(path: Path):
    if not path.exists():
        raise SystemExit(f"File not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def load_themes():
    index_path = THEME_DIR / "index.json"
    idx = read_json(index_path)

    # index.json 포맷 호환: {"themes":[...]} 또는 [...]
    if isinstance(idx, dict):
        themes = idx.get("themes", []) or []
    elif isinstance(idx, list):
        themes = idx
    else:
        raise SystemExit(f"Unexpected index.json format: {type(idx)}")

    if not themes:
        raise SystemExit("index.json에 themes가 비어있습니다.")

    return themes


def load_kr_valuation_by_ticker():
    """
    kr_valuation.json 구조:
    {
      "asOf": "yyyy-mm-dd",
      "source": "PYKRX",
      "items": {
        "A_004": {"ticker":"005380","close":...,"marketCap":...,"pe_ttm":...}
      }
    }

    -> ticker 기준으로 빠르게 찾도록 ticker_map으로 변환
    """
    if not VAL_PATH.exists():
        print("⚠ kr_valuation.json not found. Skipping KR metrics injection.")
        return {}

    v = read_json(VAL_PATH)
    items = v.get("items", {}) if isinstance(v, dict) else {}
    ticker_map = {}

    for _, it in items.items():
        if not isinstance(it, dict):
            continue
        t = (it.get("ticker") or "").strip()
        if not t:
            continue
        t = t.zfill(6)
        ticker_map[t] = it

    return ticker_map


def inject_metrics_into_theme(theme_path: Path, kr_ticker_map: dict):
    theme_obj = read_json(theme_path)
    nodes = theme_obj.get("nodes", [])
    updated = 0

    for node in nodes:
        if node.get("type") != "ASSET":
            continue

        exposure = node.get("exposure", {}) or {}
        if (exposure.get("country") or "").upper() != "KR":
            continue

        ticker = (exposure.get("ticker") or "").strip()
        if not ticker:
            continue
        ticker = ticker.zfill(6)

        val = kr_ticker_map.get(ticker)
        if not val:
            continue

        metrics = node.setdefault("metrics", {})
        metrics["marketCap"] = val.get("marketCap")
        metrics["pe_ttm"] = val.get("pe_ttm")
        metrics["close"] = val.get("close")
        metrics["valuationAsOf"] = val.get("valuationAsOf")
        metrics["valuationSource"] = val.get("valuationSource", "PYKRX")

        updated += 1

    write_json(theme_path, theme_obj)
    return updated


def rebuild_index(themes):
    index_path = THEME_DIR / "index.json"
    # 원래 구조 유지
    idx = read_json(index_path)
    if isinstance(idx, dict):
        write_json(index_path, {"themes": themes})
    else:
        write_json(index_path, themes)


def main():
    print("=== Build Freeze Start ===")
    themes = load_themes()
    kr_ticker_map = load_kr_valuation_by_ticker()

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

        total_updated += inject_metrics_into_theme(theme_path, kr_ticker_map)

    rebuild_index(themes)

    print(f"✅ KR metrics injected into {total_updated} asset nodes.")
    print("=== Build Freeze Completed ===")


if __name__ == "__main__":
    main()