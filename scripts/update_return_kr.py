# import_MT/scripts/update_return_kr.py
# MoneyTree - KR Returns Cache Builder (PYKRX)
#
# Output:
#   data/cache/returns_kr.json
#
# Schema (KR valuation_kr.json과 동일한 "assetId 키" 통일):
# {
#   "asOf": "YYYY-MM-DD",
#   "source": "PYKRX",
#   "items": {
#     "A_088": {
#       "ticker": "005930",
#       "return_3d": 1.23,
#       "return_7d": -2.34,
#       "return_1m": 0.12,
#       "return_ytd": 9.87,
#       "return_1y": 20.1,
#       "return_3y": 55.0
#     },
#     ...
#   },
#   "updatedAt": "YYYY-MM-DD"
# }

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pykrx import stock

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
THEME_DIR = DATA_DIR / "theme"
CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "returns_kr.json"

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

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def is_valid_kr_ticker(t: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", (t or "").strip()))

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

# -----------------------------
# theme parsing
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

def collect_kr_assets_from_themes() -> Dict[str, str]:
    """
    return: assetId -> ticker(6자리)
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
            if country != "KR":
                continue

            t = (exposure.get("ticker") or "").strip()
            if is_valid_kr_ticker(t):
                out[asset_id] = t

    return out

# -----------------------------
# PYKRX fetch / compute
# -----------------------------
def fetch_close_series(ticker: str, start: date, end: date) -> Optional[List[Tuple[date, float]]]:
    """
    Returns list of (trading_date, close) ascending
    """
    try:
        df = stock.get_market_ohlcv_by_date(yyyymmdd(start), yyyymmdd(end), ticker)
        if df is None or df.empty:
            return None

        out: List[Tuple[date, float]] = []
        for idx, row in df.iterrows():
            d = idx.date() if hasattr(idx, "date") else None
            close = to_float_or_none(row.get("종가"))
            if d is None or close is None:
                continue
            out.append((d, float(close)))

        out.sort(key=lambda x: x[0])
        return out if out else None
    except Exception:
        return None

def compute_returns_for_ticker(ticker: str, as_of: date) -> Tuple[str, Dict[str, Optional[float]]]:
    start = as_of - timedelta(days=1300)  # ~3y trading days buffer
    end = as_of

    closes = fetch_close_series(ticker, start, end)
    if not closes:
        return (as_of.isoformat(), {k: None for k in RETURN_KEYS})

    last_d, last_close = closes[-1]
    close_values = [c for _, c in closes]

    ret: Dict[str, Optional[float]] = {}

    for k, tdays in HORIZON_TO_TRADING_DAYS.items():
        if len(close_values) <= tdays:
            ret[k] = None
            continue
        prev_close = close_values[-(tdays + 1)]
        ret[k] = pct_return(last_close, prev_close)

    # YTD
    y = last_d.year
    ytd_rows = [(d, c) for (d, c) in closes if d.year == y]
    if not ytd_rows:
        ret["return_ytd"] = None
    else:
        first_close = ytd_rows[0][1]
        ret["return_ytd"] = pct_return(last_close, first_close)

    for k in RETURN_KEYS:
        ret.setdefault(k, None)

    return (last_d.isoformat(), ret)

def main() -> None:
    print("=== Update KR Returns Start ===")
    asset_map = collect_kr_assets_from_themes()
    print(f"✅ Collected KR assets from themes: {len(asset_map)} (assetId 기준)")

    today = date.today()
    items: Dict[str, Dict[str, Optional[float]]] = {}
    effective_as_of: Optional[str] = None

    for i, (asset_id, ticker) in enumerate(asset_map.items(), 1):
        as_of_iso, ret = compute_returns_for_ticker(ticker, today)
        items[asset_id] = {"ticker": ticker, **ret}

        if effective_as_of is None or as_of_iso > effective_as_of:
            effective_as_of = as_of_iso

        if i % 50 == 0:
            print(f"  ... computed {i}/{len(asset_map)}")

    payload = {
        "asOf": effective_as_of or today.isoformat(),
        "source": "PYKRX",
        "items": items,
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
    }

    write_json(OUT_PATH, payload)
    print(f"✅ wrote: {OUT_PATH}")
    print(f"✅ items: {len(items)}")
    print("=== Update KR Returns Completed ===")

if __name__ == "__main__":
    main()