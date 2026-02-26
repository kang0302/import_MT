# import_MT/scripts/update_return_fmp.py
# MoneyTree - Overseas Returns Cache Builder (FMP)
#
# Output:
#   data/cache/returns_fmp.json
#
# Schema (KR returns_kr.json과 동일한 "assetId 키" 통일):
# {
#   "asOf": "YYYY-MM-DD",
#   "source": "FMP",
#   "items": {
#     "A_001": {
#       "ticker": "TSLA",
#       "return_3d": 1.23,
#       "return_7d": -2.34,
#       "return_1m": 0.12,
#       "return_ytd": 9.87,
#       "return_1y": 20.1,
#       "return_3y": 55.0
#     }
#   },
#   "updatedAt": "YYYY-MM-DD"
# }

import json
import os
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
THEME_DIR = DATA_DIR / "theme"
CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "returns_fmp.json"

FMP_BASE = "https://financialmodelingprep.com/api/v3"

RETURN_KEYS = ["return_3d", "return_7d", "return_1m", "return_ytd", "return_1y", "return_3y"]
HORIZON_TO_TRADING_DAYS = {
    "return_3d": 3,
    "return_7d": 7,
    "return_1m": 21,
    "return_1y": 252,
    "return_3y": 756,
}

# -----------------------------
# helpers
# -----------------------------
def read_json(path: Path) -> Any:
    txt = path.read_text(encoding="utf-8-sig")
    return json.loads(txt)

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def is_asset_id(s: str) -> bool:
    return bool(re.fullmatch(r"A_\d{3,}", (s or "").strip()))

def to_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def pct_return(last_close: Optional[float], prev_close: Optional[float]) -> Optional[float]:
    if last_close is None or prev_close is None:
        return None
    if prev_close == 0:
        return None
    return (last_close / prev_close - 1.0) * 100.0

def http_get_json(url: str, params: dict, retry: int = 5, sleep_base: float = 1.0):
    last_err = None
    for i in range(retry):
        try:
            resp = requests.get(url, params=params, timeout=25)
            if resp.status_code == 429:
                time.sleep(sleep_base * (2 ** i))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(sleep_base * (2 ** i))
    raise SystemExit(f"❌ HTTP failed: {url} err={last_err}")

# -----------------------------
# theme parsing (KR과 동일 구조)
# -----------------------------
def normalize_theme_obj(theme_obj: Dict[str, Any]) -> Dict[str, Any]:
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
    return theme_obj

def collect_overseas_assets_from_themes() -> Dict[str, str]:
    """
    return: assetId -> ticker(symbol)
    - KR 스크립트와 동일하게 theme JSON에서만 수집
    - country != KR 인 ASSET만
    """
    out: Dict[str, str] = {}
    if not THEME_DIR.exists():
        return out

    for p in sorted(THEME_DIR.glob("T_*.json")):
        if not p.is_file():
            continue
        try:
            obj = read_json(p)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue

        obj = normalize_theme_obj(obj)
        for n in obj.get("nodes", []):
            if not isinstance(n, dict):
                continue
            if (n.get("type") or "").upper() != "ASSET":
                continue

            asset_id = (n.get("id") or "").strip()
            if not is_asset_id(asset_id):
                continue

            exposure = n.get("exposure") if isinstance(n.get("exposure"), dict) else {}
            country = (exposure.get("country") or "").upper().strip()

            # ✅ 해외만
            if country == "KR":
                continue

            t = (exposure.get("ticker") or "").strip()
            if not t:
                continue

            out[asset_id] = t

    return out

# -----------------------------
# FMP fetch / compute
# -----------------------------
def fetch_history(symbol: str, api_key: str, timeseries: int = 1200) -> List[dict]:
    """
    /historical-price-full/{symbol}?timeseries=...
    반환: historical: [{date, close, ...}, ...]
    """
    url = f"{FMP_BASE}/historical-price-full/{symbol}"
    data = http_get_json(url, {"apikey": api_key, "timeseries": timeseries})

    hist = data.get("historical") if isinstance(data, dict) else None
    if not isinstance(hist, list):
        return []

    # close/date 있는 것만, 최신이 앞으로 오도록 정렬
    hist = [h for h in hist if isinstance(h, dict) and h.get("date") and h.get("close") is not None]
    hist.sort(key=lambda x: x["date"], reverse=True)
    return hist

def get_close_by_trading_offset(hist: List[dict], offset: int) -> Optional[float]:
    # hist[0]이 최신
    if len(hist) <= offset:
        return None
    return to_float_or_none(hist[offset].get("close"))

def get_close_ytd(hist: List[dict]) -> Optional[float]:
    if not hist:
        return None
    latest_date = str(hist[0].get("date"))
    if len(latest_date) < 4:
        return None
    year = latest_date[:4]
    same_year = [h for h in hist if str(h.get("date", "")).startswith(year)]
    if not same_year:
        return None
    # 최신->과거 정렬이므로, 마지막이 그 해 가장 초반에 가까움
    return to_float_or_none(same_year[-1].get("close"))

def compute_returns_for_symbol(symbol: str, api_key: str) -> Tuple[Optional[str], Dict[str, Optional[float]]]:
    hist = fetch_history(symbol, api_key=api_key, timeseries=1200)
    if not hist:
        return (None, {k: None for k in RETURN_KEYS})

    as_of = str(hist[0].get("date"))
    last_close = to_float_or_none(hist[0].get("close"))

    ret: Dict[str, Optional[float]] = {}

    for k, tdays in HORIZON_TO_TRADING_DAYS.items():
        prev_close = get_close_by_trading_offset(hist, tdays)
        ret[k] = pct_return(last_close, prev_close)

    # YTD
    first_close = get_close_ytd(hist)
    ret["return_ytd"] = pct_return(last_close, first_close)

    for k in RETURN_KEYS:
        ret.setdefault(k, None)

    return (as_of, ret)

def main() -> None:
    print("=== Update FMP Returns Start ===")

    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    asset_map = collect_overseas_assets_from_themes()
    print(f"✅ Collected Overseas assets from themes: {len(asset_map)} (assetId 기준)")

    items: Dict[str, Dict[str, Optional[float]]] = {}
    effective_as_of: Optional[str] = None

    # 심볼 중복 호출 줄이기: symbol -> (as_of, ret) 캐시
    memo: Dict[str, Tuple[Optional[str], Dict[str, Optional[float]]]] = {}

    for i, (asset_id, symbol) in enumerate(asset_map.items(), 1):
        if symbol in memo:
            as_of_iso, ret = memo[symbol]
        else:
            as_of_iso, ret = compute_returns_for_symbol(symbol, api_key=api_key)
            memo[symbol] = (as_of_iso, ret)

        items[asset_id] = {"ticker": symbol, **ret}

        if as_of_iso:
            if effective_as_of is None or as_of_iso > effective_as_of:
                effective_as_of = as_of_iso

        if i % 50 == 0:
            print(f"  ... computed {i}/{len(asset_map)}")

        time.sleep(0.10)  # FMP rate-limit 완화

    payload = {
        "asOf": effective_as_of or date.today().isoformat(),
        "source": "FMP",
        "items": items,
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
    }

    write_json(OUT_PATH, payload)
    print(f"✅ wrote: {OUT_PATH}")
    print(f"✅ items: {len(items)}")
    print("=== Update FMP Returns Completed ===")

if __name__ == "__main__":
    main()