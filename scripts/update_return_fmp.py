# scripts/update_return_fmp.py
# MoneyTree - Overseas Returns Cache Builder (FMP Stable)
#
# Output:
#   data/cache/returns_fmp.json
#
# Sentinel Raw Historical Save (only):
#   data/cache/fmp_historical_eod_full/{SYMBOL}.json
#
# Schema (KR returns_kr.json과 동일한 "assetId 키" 통일):
# {
#   "asOf": "YYYY-MM-DD",
#   "source": "FMP_STABLE",
#   "items": {
#     "A_001": {
#       "ticker": "AAPL",
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

import csv
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
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "returns_fmp.json"

# ✅ sentinel raw 저장 폴더 (운영상: 전 종목 저장 금지)
HIST_DIR = CACHE_DIR / "fmp_historical_eod_full"

# ✅ Stable only
FMP_STABLE_BASE = "https://financialmodelingprep.com/stable"

# ✅ sentinel (데이터 품질/포맷 감시용)
SENTINEL_SYMBOLS = ["AAPL", "MSFT", "SPY"]

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
def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def to_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            v = float(x)
            if v != v:
                return None
            return v
        s = str(x).strip().replace(",", "")
        if s in ("", "-", "N/A", "null", "None"):
            return None
        v = float(s)
        if v != v:
            return None
        return v
    except Exception:
        return None


def pct_return(last_close: Optional[float], prev_close: Optional[float]) -> Optional[float]:
    if last_close is None or prev_close is None:
        return None
    if prev_close == 0:
        return None
    return (last_close / prev_close - 1.0) * 100.0


def is_asset_id(s: str) -> bool:
    return bool(re.fullmatch(r"A_\d{3,}", (s or "").strip()))


def safe_symbol_filename(sym: str) -> str:
    sym = (sym or "").strip().upper()
    sym = re.sub(r"[^A-Z0-9._-]+", "_", sym)
    return sym


def http_get_json(url: str, params: dict, retry: int = 5, sleep_base: float = 1.0) -> Any:
    last_err = None
    for i in range(retry):
        try:
            resp = requests.get(url, params=params, timeout=30)
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
# SSOT parsing
# -----------------------------
def load_overseas_assets_from_ssot() -> Dict[str, str]:
    """
    return: assetId -> ticker(symbol)
    - country != KR
    - ticker 존재
    """
    if not SSOT_PATH.exists():
        raise SystemExit(f"❌ SSOT not found: {SSOT_PATH}")

    out: Dict[str, str] = {}
    with SSOT_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            aid = (r.get("asset_id") or "").strip()
            tkr = (r.get("ticker") or "").strip()
            country = (r.get("country") or "").strip().upper()

            if not aid or not is_asset_id(aid):
                continue
            if country == "KR":
                continue
            if not tkr:
                continue

            out[aid] = tkr.strip().upper()

    return out


# -----------------------------
# FMP Stable: historical EOD full
# -----------------------------
def fetch_eod_full(symbol: str, api_key: str) -> Any:
    """
    ✅ Stable endpoint ONLY
    GET /stable/historical-price-eod/full?symbol=XXX&apikey=...
    """
    url = f"{FMP_STABLE_BASE}/historical-price-eod/full"
    params = {"symbol": symbol, "apikey": api_key}
    return http_get_json(url, params=params, retry=5, sleep_base=1.0)


def save_sentinel_raw(symbol: str, raw_obj: Any) -> None:
    """
    ✅ sentinel만 원본 저장 (운영/용량 폭발 방지)
    """
    if symbol not in SENTINEL_SYMBOLS:
        return
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    path = HIST_DIR / (safe_symbol_filename(symbol) + ".json")
    path.write_text(json.dumps(raw_obj, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_eod_series(raw_obj: Any) -> Optional[List[Tuple[date, float]]]:
    """
    raw_obj에서 (date, close) 리스트를 뽑는다.
    stable 응답 구조가 변동될 수 있으므로 방어적으로 처리.
    """
    if raw_obj is None:
        return None

    rows = None
    if isinstance(raw_obj, dict):
        if isinstance(raw_obj.get("historical"), list):
            rows = raw_obj.get("historical")
        elif isinstance(raw_obj.get("data"), list):
            rows = raw_obj.get("data")
        elif isinstance(raw_obj.get("prices"), list):
            rows = raw_obj.get("prices")

    if not isinstance(rows, list) or not rows:
        if isinstance(raw_obj, list):
            rows = raw_obj
        else:
            return None

    out: List[Tuple[date, float]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue

        ds = r.get("date") or r.get("datetime") or r.get("time")
        # close 우선, 없으면 price
        c = r.get("close") if "close" in r else r.get("price")

        if not ds:
            continue

        try:
            d = datetime.strptime(str(ds)[:10], "%Y-%m-%d").date()
        except Exception:
            continue

        close = to_float_or_none(c)
        if close is None:
            continue

        out.append((d, float(close)))

    if not out:
        return None

    out.sort(key=lambda x: x[0])  # ascending
    return out


# -----------------------------
# return computation
# -----------------------------
def compute_returns_from_series(closes: List[Tuple[date, float]]) -> Tuple[str, Dict[str, Optional[float]]]:
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
    print("=== Update FMP Returns Start (Stable) ===")
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    asset_map = load_overseas_assets_from_ssot()
    print(f"✅ Collected overseas assets from SSOT: {len(asset_map)} (assetId 기준)")

    items: Dict[str, Dict[str, Optional[float]]] = {}
    effective_as_of: Optional[str] = None

    # ticker 중복 제거: ticker별로 historical 1회만 호출
    unique_symbols = sorted(list(set(asset_map.values())))
    series_cache: Dict[str, Optional[List[Tuple[date, float]]]] = {}

    # ✅ 먼저 ticker별로 시계열 확보
    for i, sym in enumerate(unique_symbols, 1):
        try:
            raw = fetch_eod_full(sym, api_key=api_key)
            save_sentinel_raw(sym, raw)
            closes = parse_eod_series(raw)
            series_cache[sym] = closes
        except Exception as e:
            print(f"❌ failed symbol={sym} err={e}")
            series_cache[sym] = None

        if i % 25 == 0:
            print(f"  ... fetched {i}/{len(unique_symbols)} symbols")

        # rate-limit 완화
        time.sleep(0.2)

    # ✅ assetId별로 returns 계산(시계열 재사용)
    for j, (asset_id, sym) in enumerate(asset_map.items(), 1):
        closes = series_cache.get(sym)

        if not closes:
            items[asset_id] = {"ticker": sym, **{k: None for k in RETURN_KEYS}}
            continue

        as_of_iso, ret = compute_returns_from_series(closes)
        items[asset_id] = {"ticker": sym, **ret}

        if effective_as_of is None or as_of_iso > effective_as_of:
            effective_as_of = as_of_iso

        if j % 100 == 0:
            print(f"  ... computed {j}/{len(asset_map)} assets")

    payload = {
        "asOf": effective_as_of or date.today().isoformat(),
        "source": "FMP_STABLE",
        "items": items,
        "updatedAt": datetime.utcnow().strftime("%Y-%m-%d"),
    }

    write_json(OUT_PATH, payload)
    print(f"✅ wrote: {OUT_PATH}")
    print(f"✅ items: {len(items)}")
    print(f"✅ sentinel raw saved to: {HIST_DIR} (only: {', '.join(SENTINEL_SYMBOLS)})")
    print("=== Update FMP Returns Completed ===")


if __name__ == "__main__":
    main()