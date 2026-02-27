# import_MT/scripts/update_valuation_fmp.py
import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "valuation_fmp.json"

FMP_BASE = "https://financialmodelingprep.com"


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


def to_int_or_none(x) -> Optional[int]:
    if x is None:
        return None
    try:
        if isinstance(x, str) and x.strip() in ("", "-", "N/A"):
            return None
        return int(float(x))
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
            exchange = (r.get("exchange") or "").strip().upper()

            if not aid or not tkr:
                continue

            # ✅ US만
            if country != "US":
                continue

            # (선택) OTC/기타를 배제하고 싶으면 여기서 필터
            if exchange and exchange not in ("NASDAQ", "NYSE", "AMEX"):
                continue

            out[aid] = tkr

    return out


# =========================
# FMP stable quote (single symbol)
# =========================
def fetch_quote_single(symbol: str, api_key: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    FMP stable quote endpoint (single symbol only):
      /stable/quote?symbol=XXX&apikey=...
    Expected response: list with 1 dict (commonly), but handle dict too.
    """
    url = f"{FMP_BASE}/stable/quote"
    data, status, err = safe_get(url, {"symbol": symbol, "apikey": api_key})
    if err:
        return None, err

    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return data[0], None

    if isinstance(data, dict) and data.get("symbol"):
        return data, None

    return None, "HTTP_ERROR"


def main() -> None:
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    assets = load_us_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ US assets not found in SSOT (country==US & ticker required)")

    symbols = sorted(list(set(assets.values())))
    print(f"FMP_API_KEY length={len(api_key)}")
    print(f"Overseas assets={len(assets)} uniqueSymbols={len(symbols)}")

    items: Dict[str, Dict[str, Any]] = {}
    as_of = datetime.utcnow().strftime("%Y-%m-%d")

    nonzero_count = 0
    skipped_402 = 0

    for aid, sym in assets.items():
        q, err = fetch_quote_single(sym, api_key=api_key)

        if err == "PAYMENT_REQUIRED":
            skipped_402 += 1
            continue

        if err:
            # ✅ 핵심: 에러는 스킵하고 계속
            continue

        close = to_float_or_none(q.get("price")) if q else None
        market_cap = to_int_or_none(q.get("marketCap")) if q else None
        pe_ttm = to_float_or_none(q.get("pe")) if q else None  # 보통 TTM

        if (close not in (None, 0, 0.0)) or (market_cap not in (None, 0)) or (pe_ttm not in (None, 0, 0.0)):
            nonzero_count += 1

        items[aid] = {
            "ticker": sym,
            "close": close,
            "marketCap": market_cap,
            "pe_ttm": pe_ttm,
            "valuationAsOf": as_of,
            "valuationSource": "FMP",
        }

        time.sleep(0.2)

    if nonzero_count == 0:
        raise SystemExit("❌ FMP stable quote returned no meaningful values. Failing workflow.")

    out = {
        "asOf": as_of,
        "source": "FMP",
        "items": items,
        "skipped402": skipped_402,
        "nonzero": nonzero_count,
        "total": len(assets),
    }

    write_json(OUT_PATH, out)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero_count}, skipped402={skipped_402})")


if __name__ == "__main__":
    main()