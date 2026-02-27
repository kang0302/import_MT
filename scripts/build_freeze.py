# import_MT/scripts/build_freeze.py

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Union, Tuple

BASE_DIR = Path(__file__).resolve().parents[1]  # .../moneytree-web/import_MT
PROJECT_DIR = BASE_DIR.parent                   # .../moneytree-web
DATA_DIR = BASE_DIR / "data"
THEME_DIR = DATA_DIR / "theme"

# ✅ 웹이 실제로 읽는 폴더가 있으면 미러링 저장(없으면 무시)
THEME_PUBLIC_DIR = PROJECT_DIR / "public" / "data" / "theme"
HAS_PUBLIC_THEME_DIR = THEME_PUBLIC_DIR.exists()

# ========================
# caches
# ========================
VAL_KR_PATH = DATA_DIR / "cache" / "valuation_kr.json"
RET_KR_PATH = DATA_DIR / "cache" / "returns_kr.json"
VAL_FMP_PATH = DATA_DIR / "cache" / "valuation_fmp.json"
RET_ALL_PATH = DATA_DIR / "cache" / "returns_all.json"

RETURN_KEYS = ["return_3d", "return_7d", "return_1m", "return_ytd", "return_1y", "return_3y"]
VAL_KEYS = ["close", "marketCap", "pe_ttm", "valuationAsOf", "valuationSource"]
RET_META_KEYS = ["returnsAsOf", "returnsSource"]

# ------------------------
# IO (SAFE)
# ------------------------
def read_json(path: Path) -> Any:
    """
    ✅ 절대 build_freeze를 죽이지 않는 안전 read (필수 파일용)
    - 파일이 없으면 SystemExit
    - 내용이 비어있거나 JSON이 깨지면 {} 반환
    """
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    txt = path.read_text(encoding="utf-8-sig").strip()  # BOM + 공백 안전
    if not txt:
        return {}

    try:
        return json.loads(txt)
    except Exception:
        print(f"⚠ JSON parse failed. skip as empty: {path}")
        return {}


def read_json_optional(path: Path) -> Any:
    """
    ✅ 캐시파일 전용: 없거나 깨져도 {} 반환
    """
    if not path.exists():
        return {}
    txt = path.read_text(encoding="utf-8-sig").strip()
    if not txt:
        return {}
    try:
        return json.loads(txt)
    except Exception:
        print(f"⚠ JSON parse failed. cache ignored: {path}")
        return {}


def write_json(path: Path, obj: Any) -> None:
    """
    ✅ 원자적 저장(atomic): tmp에 먼저 쓰고 replace
    -> 파일이 빈 상태로 남는 사고 방지
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

# ------------------------
# index loader
# ------------------------
def load_themes() -> List[Union[Dict[str, Any], str]]:
    index_path = THEME_DIR / "index.json"
    idx = read_json(index_path)

    if isinstance(idx, dict):
        themes = idx.get("themes", []) or []
    elif isinstance(idx, list):
        themes = idx
    else:
        raise SystemExit(f"Unexpected index.json format: {type(idx)}")

    if not themes:
        raise SystemExit("index.json에 themes가 비어있습니다.")

    return themes

# ------------------------
# normalization (theme.json 구조 차이 흡수)
# ------------------------
def _normalize_theme_obj(theme_obj: Dict[str, Any]) -> Dict[str, Any]:
    # nodes
    nodes = theme_obj.get("nodes")
    if not isinstance(nodes, list):
        data_obj = theme_obj.get("data")
        graph_obj = theme_obj.get("graph")
        if isinstance(data_obj, dict) and isinstance(data_obj.get("nodes"), list):
            nodes = data_obj.get("nodes")
        elif isinstance(graph_obj, dict) and isinstance(graph_obj.get("nodes"), list):
            nodes = graph_obj.get("nodes")
        else:
            nodes = []
    theme_obj["nodes"] = nodes

    # links (edges -> links)
    links = theme_obj.get("links")
    if not isinstance(links, list):
        edges = theme_obj.get("edges")
        if isinstance(edges, list):
            links = edges
        else:
            data_obj = theme_obj.get("data")
            graph_obj = theme_obj.get("graph")
            if isinstance(data_obj, dict) and isinstance(data_obj.get("links"), list):
                links = data_obj.get("links")
            elif isinstance(data_obj, dict) and isinstance(data_obj.get("edges"), list):
                links = data_obj.get("edges")
            elif isinstance(graph_obj, dict) and isinstance(graph_obj.get("links"), list):
                links = graph_obj.get("links")
            elif isinstance(graph_obj, dict) and isinstance(graph_obj.get("edges"), list):
                links = graph_obj.get("edges")
            else:
                links = []
    theme_obj["links"] = links

    # normalize nodes (노드 단위 보정은 inject에서 "구조변경 감지"까지 함께 처리)
    for node in theme_obj["nodes"]:
        if not isinstance(node, dict):
            continue
        if node.get("type") != "ASSET":
            continue

        exposure = node.get("exposure")
        if not isinstance(exposure, dict):
            exposure = {}
            node["exposure"] = exposure

        # legacy: ticker/exchange/country at root
        if "ticker" in node and not exposure.get("ticker"):
            exposure["ticker"] = str(node.get("ticker") or "").strip()
        if "exchange" in node and not exposure.get("exchange"):
            exposure["exchange"] = str(node.get("exchange") or "").strip()
        if "country" in node and not exposure.get("country"):
            exposure["country"] = str(node.get("country") or "").strip()

        exposure.setdefault("ticker", "")
        exposure.setdefault("exchange", "")
        exposure.setdefault("country", "")

    return theme_obj


def _is_valid_kr_ticker(t: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", (t or "").strip()))


def _extract_items_any_shape(v: Any) -> Tuple[str, str, Dict[str, Any]]:
    """
    {asOf, source, items:{...}} 또는 legacy(root dict items)
    return: (asOf, source, items)
    """
    if not isinstance(v, dict):
        return ("", "", {})

    as_of = (v.get("asOf") or "").strip()
    source = (v.get("source") or "").strip()

    items = v.get("items")
    if isinstance(items, dict):
        return (as_of, source, items)

    meta_keys = {"asOf", "source", "items"}
    root_keys = set(v.keys())
    if root_keys and root_keys.issubset(meta_keys):
        return (as_of, source, {})

    # legacy: root dict 자체가 items
    return (as_of, source, v)

# ------------------------
# loaders
# ------------------------
def load_kr_valuation_by_ticker() -> Dict[str, Dict[str, Any]]:
    v = read_json_optional(VAL_KR_PATH)
    if not v:
        print(f"⚠ KR valuation file missing/empty: {VAL_KR_PATH}")
        return {}

    as_of, source, items = _extract_items_any_shape(v)
    source = source or "PYKRX"

    ticker_map: Dict[str, Dict[str, Any]] = {}
    if not isinstance(items, dict) or not items:
        print("⚠ valuation_kr.json has no usable items.")
        return {}

    for k, it in items.items():
        if not isinstance(it, dict):
            continue

        # items key는 asset_id일 가능성이 높고, ticker는 item에 존재
        t = (it.get("ticker") or "").strip()
        if not t and isinstance(k, str) and k.strip().isdigit():
            t = k.strip()

        if not t:
            continue

        t = t.zfill(6)
        if not _is_valid_kr_ticker(t):
            continue

        it2 = dict(it)
        it2["valuationAsOf"] = as_of
        it2["valuationSource"] = source
        ticker_map[t] = it2

    print(f"✅ Loaded KR valuation: {len(ticker_map)} tickers (asOf={as_of}, source={source})")
    return ticker_map


def load_returns_by_ticker(path: Path, default_source: str) -> Dict[str, Dict[str, Any]]:
    v = read_json_optional(path)
    if not v:
        print(f"⚠ returns file missing/empty: {path}")
        return {}

    as_of, source, items = _extract_items_any_shape(v)
    source = source or default_source

    if not isinstance(items, dict) or not items:
        print(f"⚠ returns has no usable items: {path}")
        return {}

    ticker_map: Dict[str, Dict[str, Any]] = {}
    for k, it in items.items():
        if not isinstance(it, dict):
            continue

        # ✅ key가 "005930" 처럼 ticker일 수도 있고, "A_088" 처럼 assetId일 수도 있음
        # -> assetId인 경우 item["ticker"]를 사용
        t = ""
        if isinstance(k, str) and k.strip().isdigit():
            t = k.strip().zfill(6)
        if not t:
            t = str(it.get("ticker") or "").strip().zfill(6)

        if not _is_valid_kr_ticker(t):
            continue

        it2 = dict(it)
        it2["returnsAsOf"] = as_of
        it2["returnsSource"] = source
        ticker_map[t] = it2

    print(f"✅ Loaded returns by ticker: {len(ticker_map)} (asOf={as_of}, source={source})")
    return ticker_map


def load_items_by_asset_id(path: Path, default_source: str) -> Dict[str, Dict[str, Any]]:
    """
    권장 포맷:
      { "asOf":"YYYY-MM-DD", "source":"FMP", "items": { "A_001": {...}, "A_002": {...} } }
    """
    v = read_json_optional(path)
    if not v:
        print(f"⚠ file missing/empty: {path}")
        return {}

    as_of, source, items = _extract_items_any_shape(v)
    source = source or default_source

    if not isinstance(items, dict) or not items:
        print(f"⚠ no usable items: {path}")
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for aid, it in items.items():
        if not isinstance(aid, str):
            continue
        if not isinstance(it, dict):
            continue

        it2 = dict(it)

        # asOf/source 메타 주입 (추적용)
        if ("close" in it2 or "marketCap" in it2 or "pe_ttm" in it2):
            it2.setdefault("valuationAsOf", as_of)
            it2.setdefault("valuationSource", source)

        if any(k in it2 for k in RETURN_KEYS):
            it2.setdefault("returnsAsOf", as_of)
            it2.setdefault("returnsSource", source)

        out[aid] = it2

    print(f"✅ Loaded asset-id items: {len(out)} from {path.name} (asOf={as_of}, source={source})")
    return out


def _is_meaningful_valuation(val: Dict[str, Any]) -> bool:
    close = val.get("close")
    mcap = val.get("marketCap")
    pe = val.get("pe_ttm")
    close_ok = close not in (None, 0, 0.0)
    mcap_ok = mcap not in (None, 0, 0.0)
    pe_ok = pe not in (None, 0, 0.0)
    return close_ok or mcap_ok or pe_ok

# ------------------------
# injection utils
# ------------------------
def _set_if_meaningful(metrics: Dict[str, Any], key: str, value: Any) -> bool:
    """
    ✅ None(또는 공백)는 기존 값을 덮어쓰지 않음
    """
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    if metrics.get(key) == value:
        return False
    metrics[key] = value
    return True


def _ensure_asset_metrics_shape(node: Dict[str, Any]) -> bool:
    """
    ✅ ASSET 노드의 metrics 구조를 강제(RETURN_KEYS 항상 존재)
    return: 구조 변경이 있었는지 여부
    """
    changed = False

    metrics = node.get("metrics")
    if not isinstance(metrics, dict):
        node["metrics"] = {}
        metrics = node["metrics"]
        changed = True

    for rk in RETURN_KEYS:
        if rk not in metrics:
            metrics[rk] = None
            changed = True

    return changed

# ------------------------
# injection
# ------------------------
def inject_metrics_into_theme(
    theme_path: Path,
    kr_val_by_ticker: Dict[str, Dict[str, Any]],
    kr_ret_by_ticker: Dict[str, Dict[str, Any]],
    fmp_val_by_aid: Dict[str, Dict[str, Any]],
    ret_all_by_aid: Dict[str, Dict[str, Any]],
) -> Tuple[int, int, bool, int]:
    """
    return: (updated_asset_nodes, scanned_asset_nodes, structural_changed, updated_us_asset_nodes)
    structural_changed: updated=0이라도 파일에 저장해야 하는 '스키마 보정' 발생 여부
    """
    theme_obj = read_json(theme_path)
    if not isinstance(theme_obj, dict):
        return (0, 0, False, 0)

    theme_obj = _normalize_theme_obj(theme_obj)

    nodes = theme_obj.get("nodes", [])
    if not isinstance(nodes, list):
        return (0, 0, False, 0)

    updated = 0
    scanned = 0
    updated_us = 0
    structural_changed = False  # ✅ 여기 핵심

    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("type") != "ASSET":
            continue

        scanned += 1
        structural_changed = _ensure_asset_metrics_shape(node) or structural_changed

        aid = (node.get("id") or "").strip()
        exposure = node.get("exposure", {}) or {}
        if not isinstance(exposure, dict):
            exposure = {}

        country = (exposure.get("country") or "").upper().strip()
        ticker = (exposure.get("ticker") or "").strip()

        metrics = node.setdefault("metrics", {})
        changed_any = False

        # ======================
        # (1) KR valuation/returns by ticker
        # ======================
        if country == "KR" and ticker:
            t6 = ticker.zfill(6)
            if _is_valid_kr_ticker(t6):
                val = kr_val_by_ticker.get(t6)
                if val and _is_meaningful_valuation(val):
                    for k in VAL_KEYS:
                        if k in val:
                            changed_any = _set_if_meaningful(metrics, k, val.get(k)) or changed_any

                ret = kr_ret_by_ticker.get(t6)
                if ret:
                    for rk in RETURN_KEYS:
                        changed_any = _set_if_meaningful(metrics, rk, ret.get(rk)) or changed_any
                    for k in RET_META_KEYS:
                        changed_any = _set_if_meaningful(metrics, k, ret.get(k)) or changed_any

        # ======================
        # (2) US valuation by asset_id (FMP: country=US only)
        # ✅ 핵심 변경: country != KR  ->  country == US
        # ======================
        if aid and country == "US":
            fmp = fmp_val_by_aid.get(aid)
            if fmp and _is_meaningful_valuation(fmp):
                for k in VAL_KEYS:
                    if k in fmp:
                        changed_any = _set_if_meaningful(metrics, k, fmp.get(k)) or changed_any
                if changed_any:
                    updated_us += 1

        # ======================
        # (3) Returns all by asset_id (optional, later)
        # ======================
        if aid:
            r = ret_all_by_aid.get(aid)
            if r:
                for rk in RETURN_KEYS:
                    changed_any = _set_if_meaningful(metrics, rk, r.get(rk)) or changed_any
                for k in RET_META_KEYS:
                    changed_any = _set_if_meaningful(metrics, k, r.get(k)) or changed_any

        if changed_any:
            updated += 1

    # ✅ 핵심: updated=0이어도 "구조 보정"이 있으면 저장해야 findstr/web에 키가 남는다
    if updated > 0 or structural_changed:
        write_json(theme_path, theme_obj)

        # ✅ public 폴더가 있으면 미러링(웹 반영 루프 방지)
        if HAS_PUBLIC_THEME_DIR:
            out_path = THEME_PUBLIC_DIR / theme_path.name
            write_json(out_path, theme_obj)

    return (updated, scanned, structural_changed, updated_us)


def rebuild_index(themes: List[Union[Dict[str, Any], str]]) -> None:
    index_path = THEME_DIR / "index.json"
    idx = read_json(index_path)

    if isinstance(idx, dict):
        idx2 = dict(idx)
        idx2["themes"] = themes
        write_json(index_path, idx2)
        if HAS_PUBLIC_THEME_DIR:
            write_json(THEME_PUBLIC_DIR / "index.json", idx2)
    else:
        write_json(index_path, themes)
        if HAS_PUBLIC_THEME_DIR:
            write_json(THEME_PUBLIC_DIR / "index.json", themes)


def main() -> None:
    print("=== Build Freeze Start ===")

    themes = load_themes()

    # KR
    kr_val_by_ticker = load_kr_valuation_by_ticker()
    kr_ret_by_ticker = load_returns_by_ticker(RET_KR_PATH, default_source="PYKRX")

    # US valuation (FMP: assetId map)
    fmp_val_by_aid = load_items_by_asset_id(VAL_FMP_PATH, default_source="FMP")

    # Returns all (optional, later)
    ret_all_by_aid = load_items_by_asset_id(RET_ALL_PATH, default_source="AUTO")

    total_updated = 0
    total_scanned = 0
    total_structural = 0
    total_updated_us = 0

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

        updated, scanned, structural_changed, updated_us = inject_metrics_into_theme(
            theme_path,
            kr_val_by_ticker,
            kr_ret_by_ticker,
            fmp_val_by_aid,
            ret_all_by_aid,
        )

        total_updated += updated
        total_scanned += scanned
        total_updated_us += updated_us
        if structural_changed:
            total_structural += 1

        # 테마별 로그(디버깅)
        if scanned > 0:
            extra = " +schema" if structural_changed and updated == 0 else ""
            us_extra = f", us+{updated_us}" if updated_us else ""
            print(f"  - {theme_id}: updated {updated}/{scanned} asset nodes{us_extra}{extra}")

    rebuild_index(themes)

    print(f"✅ Metrics injected into {total_updated} asset nodes (scanned={total_scanned}).")
    print(f"✅ US valuation injected into {total_updated_us} asset nodes (country=US, source=FMP).")
    if total_structural:
        print(f"✅ Schema-normalized themes written: {total_structural} (even if updated=0)")
    if HAS_PUBLIC_THEME_DIR:
        print(f"✅ Also mirrored to public: {THEME_PUBLIC_DIR}")
    print("=== Build Freeze Completed ===")


if __name__ == "__main__":
    main()