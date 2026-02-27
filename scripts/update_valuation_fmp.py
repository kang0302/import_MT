# import_MT/scripts/update_valuation_fmp.py
# MoneyTree - Overseas Valuation Cache Builder (FMP - stable endpoints)
#
# Output:
#   data/cache/valuation_fmp.json
#
# Schema (KR valuation_kr.json과 동일한 "assetId 키" 통일):
# {
#   "asOf": "YYYY-MM-DD",
#   "source": "FMP",
#   "items": {
#     "A_001": { "ticker": "AAPL", "close": 123.45, "marketCap": 123456789, "pe_ttm": 28.3 },
#     ...
#   },
#   "updatedAt": "YYYY-MM-DD"
# }
#
# 핵심:
# - legacy(/api/v3/...) 호출은 "legacy only"로 막힐 수 있음
# - Starter에서 안정적으로 동작시키기 위해:
#   ✅ /stable/quote 는 "심볼 1개씩" 호출 (symbol=AAPL)
#   ❌ /stable/batch-quote 같은 batch 엔드포인트는 402(Payment Required)로 막힐 수 있음

import csv
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "valuation_fmp.json"

FMP_BASE = "https://financialmodelingprep.com"  # stable endpoints base

# 거래일/데이터 생존 체크용(단일 심볼 호출로 확인)
SENTINEL_SYMBOLS = ["AAPL", "MSFT", "SPY"]

# -----------------------------
# helpers
# -----------------------------
def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def to_float_or_none(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, str) and x.strip() in ("", "-", "N/A", "null"):
            return None
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def to_int_or_none(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        if isinstance(x, str) and x.strip() in ("", "-", "N/A", "null"):
            return None
        return int(float(x))
    except Exception:
        return None


def http_get_json(url: str, params: Dict[str, Any], retry: int = 5, sleep_base: float = 1.0) -> Any:
    last_err = None
    for i in range(retry):
        try:
            resp = requests.get(url, params=params, timeout=30)
            # Rate limit
            if resp.status_code == 429:
                time.sleep(sleep_base * (2 ** i))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(sleep_base * (2 ** i))
    raise SystemExit(f"❌ HTTP failed: {url} params={params} err={last_err}")


def load_overseas_assets_from_ssot() -> Dict[str, str]:
    """
    return: assetId -> ticker(symbol)
    - country != KR
    - ticker required
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

            if not aid:
                continue
            if country == "KR":
                continue
            if not tkr:
                continue

            # 혹시 SSOT에 "AAPL,MSFT" 같은 값이 들어있으면 방어적으로 첫 토큰만 사용
            if "," in tkr:
                tkr = tkr.split(",")[0].strip()

            out[aid] = tkr

    return out


# -----------------------------
# FMP stable quote (single symbol)
# -----------------------------
def fetch_quote_single(symbol: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    GET https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey=...
    Expected: list with 1 dict (commonly), but handle dict too.
    """
    symbol = (symbol or "").strip()
    if not symbol:
        return None
    if "," in symbol:
        # 절대 다중 심볼을 넣지 않는다(402 트리거 가능)
        symbol = symbol.split(",")[0].strip()

    url = f"{FMP_BASE}/stable/quote"
    data = http_get_json(url, {"symbol": symbol, "apikey": api_key})

    # FMP가 에러를 dict로 줄 때도 있음
    if isinstance(data, dict):
        # {"Error Message": "..."} 형태
        if data.get("Error Message"):
            return None
        # 혹시 단일 dict 반환이면
        if data.get("symbol") == symbol:
            return data
        return None

    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return data[0]

    return None


def is_data_alive(api_key: str) -> bool:
    ok = 0
    for sym in SENTINEL_SYMBOLS:
        q = fetch_quote_single(sym, api_key=api_key)
        if not q:
            continue
        price = to_float_or_none(q.get("price"))
        if price not in (None, 0, 0.0):
            ok += 1
    return ok >= 1


# -----------------------------
# main
# -----------------------------
def main() -> None:
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    # 아주 흔한 실수: secrets에 공백/줄바꿈 들어감
    api_key_clean = api_key.replace("\n", "").replace("\r", "").strip()
    if api_key_clean != api_key:
        api_key = api_key_clean

    print(f"✅ FMP_API_KEY length={len(api_key)} sha256={hashlib.sha256(api_key.encode()).hexdigest()[:12]}...")

    if not is_data_alive(api_key=api_key):
        raise SystemExit("❌ FMP stable/quote does not return valid sentinel data. Check API key / plan / permissions.")

    assets = load_overseas_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ Overseas assets not found in SSOT (country!=KR & ticker required)")

    # 중복 심볼은 1번만 호출하기 위해 심볼 캐시
    unique_symbols = sorted(set(assets.values()))
    print(f"✅ Overseas assets={len(assets)} uniqueSymbols={len(unique_symbols)}")

    quote_cache: Dict[str, Dict[str, Any]] = {}
    nonzero_count = 0

    # 1) 심볼별 quote 수집(단일 호출)
    for i, sym in enumerate(unique_symbols, 1):
        q = fetch_quote_single(sym, api_key=api_key)
        if q:
            quote_cache[sym] = q

        if i % 50 == 0:
            print(f"  ... fetched {i}/{len(unique_symbols)} quotes")

        # Starter/안정성: 너무 빨리 치지 않도록 약간의 텀
        time.sleep(0.25)

    # 2) assetId 기준으로 items 구성
    items: Dict[str, Dict[str, Any]] = {}
    max_ts: Optional[int] = None

    for asset_id, sym in assets.items():
        q = quote_cache.get(sym, {}) if isinstance(quote_cache, dict) else {}

        close = to_float_or_none(q.get("price"))
        market_cap = to_int_or_none(q.get("marketCap"))
        # stable/quote에는 "pe"가 없을 수도 있음(있으면 사용)
        pe_ttm = to_float_or_none(q.get("pe"))

        ts = to_int_or_none(q.get("timestamp"))
        if ts is not None:
            max_ts = ts if max_ts is None else max(max_ts, ts)

        if (close not in (None, 0, 0.0)) or (market_cap not in (None, 0)) or (pe_ttm not in (None, 0, 0.0)):
            nonzero_count += 1

        items[asset_id] = {
            "ticker": sym,
            "close": close if close is None else float(close),
            "marketCap": market_cap,
            "pe_ttm": pe_ttm,
        }

    if nonzero_count == 0:
        print("❌ All values are zero/None. Sample check:")
        for s in SENTINEL_SYMBOLS:
            qq = quote_cache.get(s, {})
            print(s, "price=", qq.get("price"), "mcap=", qq.get("marketCap"), "pe=", qq.get("pe"), "ts=", qq.get("timestamp"))
        raise SystemExit("❌ FMP stable quote returned no meaningful values. Failing workflow.")

    # asOf: quote timestamp가 있으면 그 날짜(UTC) 사용, 없으면 오늘(UTC)
    if max_ts is not None:
        as_of = datetime.utcfromtimestamp(max_ts).strftime("%Y-%m-%d")
    else:
        as_of = datetime.utcnow().strftime("%Y-%m-%d")

    out = {
        "asOf": as_of,
        "source": "FMP",
        "items": items,
        "updatedAt": datetime.utcnow().strftime("%Y-%m-%d"),
    }

    write_json(OUT_PATH, out)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero_count}, asOf={as_of})")


if __name__ == "__main__":
    main()