# import_MT/scripts/update_valuation_fmp.py
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

# ✅ 거래일 판별용 (미국 대표 2~3개로 충분)
# - 해외 전체를 "완벽히 같은 거래일"로 맞추는 건 불가능(시간대/휴장 다름)
# - 따라서 "FMP가 정상 동작하는 최신 날짜"를 대표로 잡는 용도
SENTINEL_SYMBOLS = ["AAPL", "MSFT", "SPY"]


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


def fetch_quote_batch(symbols, api_key: str):
    # /quote/{symbols} -> price, marketCap, pe(TTM)
    out = {}
    # FMP batch는 100개 단위가 안전
    for i in range(0, len(symbols), 100):
        group = symbols[i:i+100]
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
    # /historical-price-full/{symbol}?timeseries=5
    url = f"{FMP_BASE}/historical-price-full/{symbol}"
    data = http_get_json(url, {"apikey": api_key, "timeseries": 10})
    hist = data.get("historical") if isinstance(data, dict) else None
    if not isinstance(hist, list) or len(hist) == 0:
        return None, None

    # 최신 날짜 찾기 (정렬 방어)
    hist = [h for h in hist if isinstance(h, dict) and h.get("date") and h.get("close") is not None]
    if not hist:
        return None, None
    hist.sort(key=lambda x: x["date"], reverse=True)

    latest = hist[0]
    return latest.get("date"), to_float_or_none(latest.get("close"))


def is_valid_market_day(date_str: str, api_key: str) -> bool:
    # sentinel들의 close가 0/None이 아니면 "정상 거래일(데이터 존재)"로 판정
    ok = 0
    for sym in SENTINEL_SYMBOLS:
        d, close = fetch_latest_close_date(sym, api_key=api_key)
        if d == date_str and close not in (None, 0, 0.0):
            ok += 1
    return ok >= 1  # 1개만 잡혀도 데이터는 살아있다고 판단


def find_latest_market_day(max_back_days: int, api_key: str) -> str:
    # FMP는 "오늘"이 휴장이어도 직전 거래일 데이터를 준다.
    # 다만 안전하게 최근 N일 범위에서 유효한 날짜를 찾는다.
    d = datetime.utcnow()

    # 주말 보정(UTC 기준이긴 하지만, 대략적 안전장치)
    if d.weekday() == 5:   # 토
        d = d - timedelta(days=1)
    elif d.weekday() == 6: # 일
        d = d - timedelta(days=2)

    # 우선 sentinel 중 하나의 최신 날짜를 먼저 확보
    latest_dates = []
    for sym in SENTINEL_SYMBOLS:
        ds, _ = fetch_latest_close_date(sym, api_key=api_key)
        if ds:
            latest_dates.append(ds)
    if not latest_dates:
        raise SystemExit("❌ FMP sentinel history returned no dates")

    # 후보 시작점 = sentinel 최신일(가장 최근)
    cand0 = max(latest_dates)

    # cand0가 유효하면 끝. 아니면 뒤로 탐색.
    if is_valid_market_day(cand0, api_key=api_key):
        return cand0

    # cand0 기준 뒤로 max_back_days 탐색
    cand_dt = datetime.strptime(cand0, "%Y-%m-%d")
    for i in range(1, max_back_days + 1):
        ds = (cand_dt - timedelta(days=i)).strftime("%Y-%m-%d")
        if is_valid_market_day(ds, api_key=api_key):
            return ds

    raise SystemExit("❌ 최근 유효 market day를 찾지 못했습니다. (FMP/네트워크/휴장 가능)")


def main():
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    assets = load_overseas_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ Overseas assets not found in SSOT (country!=KR & ticker required)")

    as_of = find_latest_market_day(max_back_days=30, api_key=api_key)
    print(f"✅ Using market day: {as_of}")

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

        # close가 quote에서 없으면 history 최신 close로 보강(안전)
        if close in (None, 0, 0.0):
            ds, c = fetch_latest_close_date(sym, api_key=api_key)
            if ds == as_of and c not in (None, 0, 0.0):
                close = c

        if (close not in (None, 0, 0.0)) or (market_cap not in (None, 0)) or (pe_ttm not in (None, 0, 0.0)):
            nonzero_count += 1

        items[asset_id] = {
            "ticker": sym,
            "close": close if close is None else float(close),
            "marketCap": market_cap,
            "pe_ttm": pe_ttm,
        }

        time.sleep(0.05)

    if nonzero_count == 0:
        print("❌ All values are zero/None. Sample check:")
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
    }

    write_json(OUT_PATH, out)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero_count})")


if __name__ == "__main__":
    main()