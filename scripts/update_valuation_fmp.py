# scripts/update_valuation_fmp.py
import csv
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

# ✅ Stable only
FMP_STABLE_BASE = "https://financialmodelingprep.com/stable"

# ✅ asOf 판별용 sentinel (stable quote에서 timestamp가 있으면 사용)
SENTINEL_SYMBOLS = ["AAPL", "MSFT", "SPY"]

# FMP batch-quote는 symbols 파라미터를 사용
# Endpoint: https://financialmodelingprep.com/stable/batch-quote?symbols=AAPL  :contentReference[oaicite:1]{index=1}


# -------------------------
# helpers
# -------------------------
def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def to_float_or_none(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, str) and x.strip() in ("", "-", "N/A", "null", "None"):
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
        if isinstance(x, str) and x.strip() in ("", "-", "N/A", "null", "None"):
            return None
        return int(float(x))
    except Exception:
        return None


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


# -------------------------
# SSOT
# -------------------------
def load_overseas_assets_from_ssot() -> Dict[str, str]:
    """
    return: {asset_id: ticker(symbol)}
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
            sym = (r.get("ticker") or "").strip()
            country = (r.get("country") or "").strip().upper()

            if not aid:
                continue
            if country == "KR":
                continue
            if not sym:
                continue

            out[aid] = sym.upper()

    return out


# -------------------------
# FMP Stable: batch quote
# -------------------------
def fetch_batch_quote(symbols, api_key: str) -> Dict[str, dict]:
    """
    Stable Batch Quote
    Endpoint: /stable/batch-quote?symbols=AAPL,MSFT,...  :contentReference[oaicite:2]{index=2}
    """
    url = f"{FMP_STABLE_BASE}/batch-quote"
    data = http_get_json(url, params={"symbols": ",".join(symbols), "apikey": api_key})

    out: Dict[str, dict] = {}
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                s = (item.get("symbol") or "").strip().upper()
                if s:
                    out[s] = item
    elif isinstance(data, dict):
        # 혹시 dict 형태로 오는 변형 방어
        s = (data.get("symbol") or "").strip().upper()
        if s:
            out[s] = data
    return out


def derive_asof_from_quotes(quote_map: Dict[str, dict]) -> str:
    """
    quote의 timestamp(유닉스 초)가 있으면 그 날짜(UTC)를 asOf로 사용.
    없으면 UTC 오늘.
    """
    for sym in SENTINEL_SYMBOLS:
        q = quote_map.get(sym, {})
        ts = q.get("timestamp")
        try:
            if ts is not None:
                ts_int = int(ts)
                return datetime.utcfromtimestamp(ts_int).strftime("%Y-%m-%d")
        except Exception:
            pass
    return datetime.utcnow().strftime("%Y-%m-%d")


def main() -> None:
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    assets = load_overseas_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ Overseas assets not found in SSOT (country!=KR & ticker required)")

    # ✅ asset + sentinel 합쳐서 quotes를 받는다
    symbols_all = sorted(list(set(list(assets.values()) + SENTINEL_SYMBOLS)))

    quote_map: Dict[str, dict] = {}
    # batch-quote는 여러 심볼을 콤마로 받을 수 있음 :contentReference[oaicite:3]{index=3}
    # 안전하게 100개 단위로 쪼갬
    for i in range(0, len(symbols_all), 100):
        chunk = symbols_all[i:i + 100]
        part = fetch_batch_quote(chunk, api_key=api_key)
        quote_map.update(part)
        time.sleep(0.25)

    as_of = derive_asof_from_quotes(quote_map)
    print(f"✅ Using asOf: {as_of}")

    items: Dict[str, dict] = {}
    nonzero_count = 0

    for asset_id, sym in assets.items():
        q = quote_map.get(sym, {}) if isinstance(quote_map, dict) else {}

        # 필드명 방어(Stable 응답에서 price/marketCap/pe/timestamp가 일반적)
        close = to_float_or_none(q.get("price"))
        market_cap = to_int_or_none(q.get("marketCap"))
        pe_ttm = to_float_or_none(q.get("pe"))

        if (close not in (None, 0, 0.0)) or (market_cap not in (None, 0)) or (pe_ttm not in (None, 0, 0.0)):
            nonzero_count += 1

        items[asset_id] = {
            "ticker": sym,
            "close": None if close is None else float(close),
            "marketCap": market_cap,
            "pe_ttm": pe_ttm,
        }

    if nonzero_count == 0:
        print("❌ All values are zero/None. Sample check (sentinels):")
        for s in SENTINEL_SYMBOLS:
            qq = quote_map.get(s, {})
            print(s, "price=", qq.get("price"), "mcap=", qq.get("marketCap"), "pe=", qq.get("pe"), "timestamp=", qq.get("timestamp"))
        raise SystemExit("❌ FMP stable batch-quote returned no meaningful values. Failing workflow.")

    out = {
        "asOf": as_of,
        "source": "FMP_STABLE",
        "items": items,
        "updatedAt": datetime.utcnow().strftime("%Y-%m-%d"),
    }

    write_json(OUT_PATH, out)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero_count})")


if __name__ == "__main__":
    main()