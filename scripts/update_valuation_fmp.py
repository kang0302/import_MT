# scripts/update_valuation_fmp.py
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import csv
import time
import requests

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "valuation_fmp.json"

FMP_BASE = "https://financialmodelingprep.com/api/v3"

# ✅ 거래일(=asOf) 판별용 sentinel
SENTINEL_SYMBOLS = ["AAPL", "MSFT", "SPY"]


# -------------------------
# helpers
# -------------------------
def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def to_float_or_none(x):
    if x is None:
        return None
    try:
        if isinstance(x, str) and x.strip() in ("", "-", "N/A"):
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
        if isinstance(x, str) and x.strip() in ("", "-", "N/A"):
            return None
        return int(float(x))
    except Exception:
        return None


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


# -------------------------
# SSOT
# -------------------------
def load_overseas_assets_from_ssot():
    """
    return: {asset_id: ticker(symbol)}
    - country != KR
    - ticker 존재
    """
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


# -------------------------
# FMP fetch
# -------------------------
def fetch_quote_batch(symbols, api_key: str):
    """
    /quote/{symbols} -> price, marketCap, pe(TTM)
    """
    out = {}
    for i in range(0, len(symbols), 100):
        group = symbols[i:i + 100]
        sym_str = ",".join(group)
        url = f"{FMP_BASE}/quote/{sym_str}"
        data = http_get_json(url, {"apikey": api_key})

        if isinstance(data, list):
            for item in data:
                s = (item.get("symbol") or "").strip()
                if s:
                    out[s] = item

        time.sleep(0.25)
    return out


def fetch_latest_close_date(symbol: str, api_key: str):
    """
    /historical-price-full/{symbol}?timeseries=10
    -> 가장 최신 date/close (sentinel 용도)
    """
    url = f"{FMP_BASE}/historical-price-full/{symbol}"
    data = http_get_json(url, {"apikey": api_key, "timeseries": 10})
    hist = data.get("historical") if isinstance(data, dict) else None
    if not isinstance(hist, list) or len(hist) == 0:
        return None, None

    hist = [h for h in hist if isinstance(h, dict) and h.get("date") and h.get("close") is not None]
    if not hist:
        return None, None

    hist.sort(key=lambda x: x["date"], reverse=True)
    latest = hist[0]
    return latest.get("date"), to_float_or_none(latest.get("close"))


def find_latest_market_day(api_key: str) -> str:
    """
    ✅ KR처럼 '거래일'을 강하게 찾고 싶지만,
    해외는 시장/휴장이 다르고, 우리가 daily snapshot만 필요하므로
    sentinel(미국 대표)의 최신 date를 asOf로 사용한다.

    - sentinel별 latest를 1번씩만 호출 (중복 호출 제거)
    """
    latest_dates = []
    for sym in SENTINEL_SYMBOLS:
        ds, close = fetch_latest_close_date(sym, api_key=api_key)
        if ds and close not in (None, 0, 0.0):
            latest_dates.append(ds)

    if not latest_dates:
        raise SystemExit("❌ FMP sentinel history returned no valid dates/closes")

    # 가장 최신(문자열 YYYY-MM-DD는 max가 최신)
    return max(latest_dates)


def main():
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    assets = load_overseas_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ Overseas assets not found in SSOT (country!=KR & ticker required)")

    as_of = find_latest_market_day(api_key=api_key)
    print(f"✅ Using market day(asOf): {as_of}")

    symbols = sorted(list(set(assets.values())))
    quote_map = fetch_quote_batch(symbols, api_key=api_key)

    items = {}
    nonzero_count = 0

    for asset_id, sym in assets.items():
        q = quote_map.get(sym, {}) if isinstance(quote_map, dict) else {}

        # KR과 같은 필드명으로 통일
        close = to_float_or_none(q.get("price"))
        market_cap = to_int_or_none(q.get("marketCap"))
        pe_ttm = to_float_or_none(q.get("pe"))  # FMP quote의 pe는 보통 TTM

        if (close not in (None, 0, 0.0)) or (market_cap not in (None, 0)) or (pe_ttm not in (None, 0, 0.0)):
            nonzero_count += 1

        items[asset_id] = {
            "ticker": sym,
            "close": None if close is None else float(close),
            "marketCap": market_cap,
            "pe_ttm": pe_ttm,
        }

    # ✅ KR과 동일: 의미 있는 값이 하나도 없으면 workflow fail
    if nonzero_count == 0:
        print("❌ All values are zero/None. Sample check (sentinels):")
        for s in SENTINEL_SYMBOLS:
            try:
                qq = quote_map.get(s, {})
                print(s, "price=", qq.get("price"), "mcap=", qq.get("marketCap"), "pe=", qq.get("pe"))
            except Exception:
                pass
        raise SystemExit("❌ FMP valuation fetch returned no meaningful values. Failing workflow.")

    out = {
        "asOf": as_of,
        "source": "FMP",
        "items": items,
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
    }

    write_json(OUT_PATH, out)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero_count})")


if __name__ == "__main__":
    main()