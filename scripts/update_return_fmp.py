# import_MT/scripts/update_return_fmp.py
import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "returns_fmp.json"

FMP_BASE = "https://financialmodelingprep.com"

RETURN_KEYS = ["return_3d", "return_7d", "return_1m", "return_ytd", "return_1y", "return_3y"]


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def to_float_or_none(x) -> Optional[float]:
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


def safe_get(url: str, params: Dict[str, Any], timeout: int = 25, retry: int = 5, sleep_base: float = 1.0) -> Tuple[Optional[Any], Optional[int], Optional[str]]:
    """
    return: (json_or_none, status_code_or_none, error_tag)
      error_tag:
        - "PAYMENT_REQUIRED" (402)
        - "INVALID_API_KEY"
        - "HTTP_ERROR"
        - "EXCEPTION"
    """
    last_err = None
    for i in range(retry):
        try:
            resp = requests.get(url, params=params, timeout=timeout)

            if resp.status_code == 429:
                time.sleep(sleep_base * (2 ** i))
                continue

            if resp.status_code == 402:
                return None, 402, "PAYMENT_REQUIRED"

            if resp.status_code in (401, 403):
                # FMP는 메시지로 invalid key를 주기도 함
                try:
                    j = resp.json()
                    msg = (j.get("Error Message") or "").lower()
                    if "invalid api key" in msg:
                        return None, resp.status_code, "INVALID_API_KEY"
                except Exception:
                    pass
                return None, resp.status_code, "HTTP_ERROR"

            resp.raise_for_status()
            return resp.json(), resp.status_code, None

        except Exception as e:
            last_err = e
            time.sleep(sleep_base * (2 ** i))

    return None, None, f"EXCEPTION:{last_err}"


def load_us_assets_from_ssot() -> Dict[str, str]:
    if not SSOT_PATH.exists():
        raise SystemExit(f"❌ SSOT not found: {SSOT_PATH}")

    out: Dict[str, str] = {}
    with SSOT_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            aid = (r.get("asset_id") or "").strip()
            tkr = (r.get("ticker") or "").strip()
            country = (r.get("country") or "").strip().upper()

            if not aid or not tkr:
                continue

            # ✅ US만
            if country != "US":
                continue

            out[aid] = tkr

    return out


def ytd_anchor_year(ts_utc: datetime) -> int:
    return ts_utc.year


def compute_return(cur: Optional[float], past: Optional[float]) -> Optional[float]:
    if cur in (None, 0, 0.0) or past in (None, 0, 0.0):
        return None
    try:
        return (float(cur) / float(past) - 1.0) * 100.0
    except Exception:
        return None


def fetch_eod_series(symbol: str, api_key: str) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str], Optional[str]]:
    """
    stable endpoint (EOD):
      /stable/historical-price-eod/full?symbol=XXX&apikey=...
    return: (list_or_none, asof_date, error_tag)
    """
    url = f"{FMP_BASE}/stable/historical-price-eod/full"
    data, status, err = safe_get(url, {"symbol": symbol, "apikey": api_key})
    if err:
        return None, None, err

    # 보통 list 형태
    if isinstance(data, list):
        series = [x for x in data if isinstance(x, dict) and x.get("date") and x.get("close") is not None]
        if not series:
            return None, None, "HTTP_ERROR"
        # 최신일 기준 내림차순
        series.sort(key=lambda x: x["date"], reverse=True)
        return series, series[0].get("date"), None

    # dict로 내려오는 케이스 방어
    if isinstance(data, dict):
        # 일부 응답은 {"symbol": "...", "historical": [...]} 형태일 수 있음
        hist = data.get("historical") if isinstance(data.get("historical"), list) else None
        if isinstance(hist, list):
            series = [x for x in hist if isinstance(x, dict) and x.get("date") and x.get("close") is not None]
            if not series:
                return None, None, "HTTP_ERROR"
            series.sort(key=lambda x: x["date"], reverse=True)
            return series, series[0].get("date"), None

    return None, None, "HTTP_ERROR"


def pick_close_on_or_before(series: List[Dict[str, Any]], target_date: str) -> Optional[float]:
    # series는 date desc라고 가정
    for row in series:
        d = row.get("date")
        if not d:
            continue
        if d <= target_date:
            return to_float_or_none(row.get("close"))
    return None


def main() -> None:
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    assets = load_us_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ US assets not found in SSOT (country==US & ticker required)")

    symbols = sorted(list(set(assets.values())))
    print(f"US assets={len(assets)} uniqueSymbols={len(symbols)}")

    now_utc = datetime.utcnow()
    ytd_year = ytd_anchor_year(now_utc)

    items: Dict[str, Dict[str, Any]] = {}
    as_of_global = ""

    skipped_402 = 0
    ok_count = 0

    for aid, sym in assets.items():
        series, as_of, err = fetch_eod_series(sym, api_key=api_key)

        if err == "PAYMENT_REQUIRED":
            skipped_402 += 1
            # ✅ 402 심볼은 스킵하고 계속
            continue

        if err:
            # 기타 에러는 로그만 남기고 스킵
            print(f"⚠ skip symbol={sym} err={err}")
            continue

        if not series or not as_of:
            continue

        if not as_of_global:
            as_of_global = as_of

        # 현재 close
        cur = to_float_or_none(series[0].get("close"))

        # 과거 기준일들
        # 단순히 "거래일 기준 N일"은 정확 매칭이 어려워서:
        # - series의 index를 써서 근사치로 잡는다 (eod series가 trading days로만 구성되기 때문)
        def close_by_trading_index(idx: int) -> Optional[float]:
            if idx < 0 or idx >= len(series):
                return None
            return to_float_or_none(series[idx].get("close"))

        close_3d = close_by_trading_index(3)
        close_7d = close_by_trading_index(7)
        close_1m = close_by_trading_index(21)   # 대략 21 trading days
        close_1y = close_by_trading_index(252)  # 대략 252 trading days
        close_3y = close_by_trading_index(252 * 3)

        # YTD: 올해 1/1 이후 첫 거래일 close를 찾아서 사용
        ytd_target = f"{ytd_year}-01-01"
        ytd_close = pick_close_on_or_before(list(reversed(series)), ytd_target)  # asc로 만들어 탐색
        # reversed(series)은 asc가 아니므로 방어: 아래처럼 확실히 asc 정렬
        series_asc = list(series)
        series_asc.sort(key=lambda x: x["date"])
        ytd_close = None
        for row in series_asc:
            d = row.get("date")
            if d and d >= ytd_target:
                ytd_close = to_float_or_none(row.get("close"))
                break

        out = {
            "ticker": sym,
            "return_3d": compute_return(cur, close_3d),
            "return_7d": compute_return(cur, close_7d),
            "return_1m": compute_return(cur, close_1m),
            "return_ytd": compute_return(cur, ytd_close),
            "return_1y": compute_return(cur, close_1y),
            "return_3y": compute_return(cur, close_3y),
        }

        items[aid] = out
        ok_count += 1

        time.sleep(0.25)

    out_obj = {
        "asOf": as_of_global or now_utc.strftime("%Y-%m-%d"),
        "source": "FMP",
        "items": items,
        "skipped402": skipped_402,
        "ok": ok_count,
        "total": len(assets),
    }

    write_json(OUT_PATH, out_obj)
    print(f"✅ wrote: {OUT_PATH} items={len(items)} skipped402={skipped_402}")


if __name__ == "__main__":
    main()