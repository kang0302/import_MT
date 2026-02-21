# import_MT/scripts/build_freeze.py
import json
from pathlib import Path
from typing import Any, Dict, List, Union

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
THEME_DIR = DATA_DIR / "theme"

# ✅ 현재 프로젝트 구조 기준: valuation 파일은 cache에 있음
VAL_PATH = DATA_DIR / "cache" / "valuation_kr.json"


def read_json(path: Path) -> Any:
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    # ✅ BOM(UTF-8 with BOM)도 안전하게 읽기
    txt = path.read_text(encoding="utf-8-sig")
    return json.loads(txt)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def load_themes() -> List[Union[Dict[str, Any], str]]:
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


def load_kr_valuation_by_ticker() -> Dict[str, Dict[str, Any]]:
    """
    valuation_kr.json 구조(현재 네가 보여준 형태 기준):
    {
      "asOf": "2026-02-21",
      "source": "PYKRX",
      "items": {
        "A_004": {"ticker":"005380","close":...,"marketCap":...,"pe_ttm":...},
        ...
      }
    }

    -> ticker(6자리) 기준으로 찾도록 ticker_map 반환
    -> 또한 (asOf, source) 메타를 함께 넘겨 주입 시 사용
    """
    if not VAL_PATH.exists():
        print(f"⚠ KR valuation file not found: {VAL_PATH}")
        print("   -> Skipping KR metrics injection.")
        return {}

    v = read_json(VAL_PATH)

    if not isinstance(v, dict):
        print("⚠ valuation_kr.json format is not a dict. Skipping.")
        return {}

    as_of = (v.get("asOf") or "").strip()
    source = (v.get("source") or "PYKRX").strip()

    items = v.get("items", {}) if isinstance(v.get("items", {}), dict) else {}
    ticker_map: Dict[str, Dict[str, Any]] = {}

    for _, it in items.items():
        if not isinstance(it, dict):
            continue
        t = (it.get("ticker") or "").strip()
        if not t:
            continue

        t = t.zfill(6)

        # ✅ 주입 편의를 위해 메타를 item에 같이 붙여둠
        it2 = dict(it)
        it2["valuationAsOf"] = as_of
        it2["valuationSource"] = source or "PYKRX"

        ticker_map[t] = it2

    print(f"✅ Loaded KR valuation: {len(ticker_map)} tickers (asOf={as_of}, source={source})")
    return ticker_map


def _is_meaningful_value(val: Dict[str, Any]) -> bool:
    """
    값이 전부 0/None이면 '수집 실패'로 보고 주입하지 않음.
    (네가 지금 겪는 케이스 방지)
    """
    close = val.get("close")
    mcap = val.get("marketCap")
    pe = val.get("pe_ttm")

    close_ok = close not in (None, 0, 0.0)
    mcap_ok = mcap not in (None, 0, 0.0)
    pe_ok = pe not in (None, 0, 0.0)  # 적자기업은 None 가능

    return close_ok or mcap_ok or pe_ok


def inject_metrics_into_theme(theme_path: Path, kr_ticker_map: Dict[str, Dict[str, Any]]) -> int:
    theme_obj = read_json(theme_path)
    nodes = theme_obj.get("nodes", [])
    if not isinstance(nodes, list):
        return 0

    updated = 0

    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("type") != "ASSET":
            continue

        exposure = node.get("exposure", {}) or {}
        if not isinstance(exposure, dict):
            continue

        if (exposure.get("country") or "").upper() != "KR":
            continue

        ticker = (exposure.get("ticker") or "").strip()
        if not ticker:
            continue
        ticker = ticker.zfill(6)

        val = kr_ticker_map.get(ticker)
        if not val:
            continue

        # ✅ 전부 0/None이면 주입 스킵 (수집 실패 방지)
        if not _is_meaningful_value(val):
            continue

        metrics = node.setdefault("metrics", {})

        # ✅ 주입 (None은 None으로 유지, 0은 그대로 들어갈 수 있으나 위에서 필터링)
        metrics["close"] = val.get("close")
        metrics["marketCap"] = val.get("marketCap")
        metrics["pe_ttm"] = val.get("pe_ttm")

        metrics["valuationAsOf"] = val.get("valuationAsOf")
        metrics["valuationSource"] = val.get("valuationSource", "PYKRX")

        updated += 1

    if updated > 0:
        write_json(theme_path, theme_obj)

    return updated


def rebuild_index(themes: List[Union[Dict[str, Any], str]]) -> None:
    index_path = THEME_DIR / "index.json"
    idx = read_json(index_path)

    # ✅ 기존 구조 최대한 유지
    if isinstance(idx, dict):
        idx2 = dict(idx)
        idx2["themes"] = themes
        write_json(index_path, idx2)
    else:
        write_json(index_path, themes)


def main() -> None:
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