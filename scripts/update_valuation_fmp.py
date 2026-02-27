# import_MT/scripts/update_valuation_fmp.py
# MoneyTree - Overseas Valuation Cache Builder (FMP - Starter Safe, Single Symbol)

import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "valuation_fmp.json"

FMP_BASE = "https://financialmodelingprep.com"

SENTINEL_SYMBOLS = ["AAPL", "MSFT", "SPY"]

HTTP_TIMEOUT = 25
MAX_RETRY = 5
SLEEP_BASE = 1.0
PER_REQUEST_SLEEP = 0.20


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def to_float_or_none(x):
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


def to_int_or_none(x):
    if x is None:
        return None
    try:
        if isinstance(x, str) and x.strip() in ("", "-", "N/A", "null", "None"):
            return None
        return int(float(x))
    except Exception:
        return None


def http_get_json(url: str, params: dict):
    last_err = None
    for i in range(MAX_RETRY):
        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)

            if resp.status_code == 429:
                time.sleep(SLEEP_BASE * (2 ** i))
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            last_err = e
            time.sleep(SLEEP_BASE * (2 ** i))

    raise SystemExit(f"❌ HTTP failed: {url} err={last_err}")


def load_overseas_assets_from_ssot():
    if not SSOT_PATH.exists():
        raise SystemExit(f"❌ SSOT not found: {SSOT_PATH}")

    out = {}
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

            out[aid] = tkr
    return out


def fetch_quote_single(symbol: str, api_key: str):
    url = f"{FMP_BASE}/stable/quote"
    data = http_get_json(url, {"symbol": symbol, "apikey": api_key})

    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return data[0]
    if isinstance(data, dict) and data.get("symbol"):
        return data
    return None


def fetch_latest_date_from_quote(symbol: str, api_key: str):
    q = fetch_quote_single(symbol, api_key)
    if not q:
        return None
    ts = q.get("timestamp")
    try:
        if ts is None:
            return None
        ts = int(ts)
        dt = datetime.utcfromtimestamp(ts)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def find_asof(api_key: str):
    for sym in SENTINEL_SYMBOLS:
        try:
            ds = fetch_latest_date_from_quote(sym, api_key)
            if ds:
                return ds
        except Exception:
            pass
    return datetime.utcnow().strftime("%Y-%m-%d")


def main():
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    assets = load_overseas_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ Overseas assets not found in SSOT (country!=KR & ticker required)")

    as_of = find_asof(api_key)
    print(f"✅ Using asOf: {as_of}")

    items = {}
    nonzero_count = 0

    for i, (asset_id, sym) in enumerate(assets.items(), 1):
        q = fetch_quote_single(sym, api_key)

        close = None
        market_cap = None
        pe_ttm = None

        if q:
            close = to_float_or_none(q.get("price"))
            market_cap = to_int_or_none(q.get("marketCap"))
            pe_ttm = to_float_or_none(q.get("pe"))  # 있을 수도/없을 수도

        if (close not in (None, 0, 0.0)) or (market_cap not in (None, 0)) or (pe_ttm not in (None, 0, 0.0)):
            nonzero_count += 1

        items[asset_id] = {
            "ticker": sym,
            "close": close if close is None else float(close),
            "marketCap": market_cap,
            "pe_ttm": pe_ttm,
        }

        if i % 50 == 0:
            print(f"  ... fetched {i}/{len(assets)}")

        time.sleep(PER_REQUEST_SLEEP)

    if nonzero_count == 0:
        print("❌ All values are zero/None. Sample check:")
        for s in SENTINEL_SYMBOLS:
            try:
                qq = fetch_quote_single(s, api_key) or {}
                print(s, "price=", qq.get("price"), "mcap=", qq.get("marketCap"), "pe=", qq.get("pe"))
            except Exception:
                pass
        raise SystemExit("❌ FMP stable quote returned no meaningful values. Failing workflow.")

    out = {
        "asOf": as_of,
        "source": "FMP",
        "items": items,
    }

    write_json(OUT_PATH, out)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero_count})")


if __name__ == "__main__":
    main()