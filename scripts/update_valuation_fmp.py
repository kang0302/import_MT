# import_MT/scripts/update_valuation_fmp.py
# MoneyTree - US Valuation Cache Builder (FMP stable/quote, Starter-friendly)
#
# Output:
#   data/cache/valuation_fmp.json
#
# Schema (KR valuation_kr.json과 동일한 "assetId 키" 통일):
# {
#   "asOf": "YYYY-MM-DD",
#   "source": "FMP",
#   "scope": "US",
#   "items": {
#     "A_001": { "ticker": "AAPL", "close": 123.45, "marketCap": 123456789, "pe_ttm": 12.3 },
#     ...
#   },
#   "skipped": {
#     "PAYMENT_REQUIRED": ["BA.", ...],
#     "INVALID_SYMBOL": ["", ...],
#     "HTTP_OTHER": ["XXX", ...]
#   },
#   "updatedAt": "YYYY-MM-DD"
# }

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
QUOTE_ENDPOINT = f"{FMP_BASE}/stable/quote"  # ✅ Starter-friendly: single symbol quote

# 거래일(asOf) 판별용(US 대표)
SENTINEL_SYMBOLS = ["AAPL", "MSFT", "SPY"]

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
            if v != v:  # NaN
                return None
            return v
        s = str(x).strip()
        if s in ("", "-", "N/A", "None"):
            return None
        v = float(s)
        if v != v:
            return None
        return v
    except Exception:
        return None


def to_int_or_none(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            return int(x)
        s = str(x).strip()
        if s in ("", "-", "N/A", "None"):
            return None
        return int(float(s))
    except Exception:
        return None


def normalize_symbol(sym: str) -> str:
    """
    ✅ 운영 안정용 심볼 정규화
    - 공백 제거
    - 끝의 '.' 같은 불필요 문자를 1차 제거(BA. -> BA)
    - 대문자화
    주의: 해외(UK: VOD.L 등) 같은 접미사는 US-only 모드에서는 어차피 제외되지만,
         SSOT에 이상값(BA.) 등이 섞인 걸 방어하기 위한 최소 처리.
    """
    s = (sym or "").strip().upper()
    if s.endswith("."):
        s = s[:-1]
    return s


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 25) -> Tuple[int, Any]:
    """
    returns: (status_code, json_or_text)
    """
    resp = requests.get(url, params=params, timeout=timeout)
    ctype = (resp.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try:
            return resp.status_code, resp.json()
        except Exception:
            return resp.status_code, resp.text
    return resp.status_code, resp.text


# -----------------------------
# SSOT load (US only)
# -----------------------------
def load_us_assets_from_ssot() -> Dict[str, str]:
    """
    return: asset_id -> symbol (US only)
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
            if country != "US":  # ✅ 핵심: US only
                continue
            if not tkr:
                continue

            out[aid] = normalize_symbol(tkr)

    return out


# -----------------------------
# FMP stable quote (single symbol)
# -----------------------------
def fetch_quote_single(symbol: str, api_key: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Returns: (status_tag, quote_dict_or_none)
      status_tag:
        - "OK"
        - "PAYMENT_REQUIRED" (402)
        - "RATE_LIMIT" (429)
        - "UNAUTHORIZED" (401)
        - "FORBIDDEN" (403)
        - "NOT_FOUND" (404)
        - "HTTP_OTHER"
        - "BAD_JSON"
    """
    params = {"symbol": symbol, "apikey": api_key}
    code, data = http_get_json(QUOTE_ENDPOINT, params=params)

    if code == 200:
        # stable/quote는 보통 list 형태
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            return "OK", data[0]
        # 혹시 dict로 오는 케이스 방어
        if isinstance(data, dict) and data.get("symbol"):
            return "OK", data
        return "BAD_JSON", None

    if code == 402:
        return "PAYMENT_REQUIRED", None
    if code == 429:
        return "RATE_LIMIT", None
    if code == 401:
        return "UNAUTHORIZED", None
    if code == 403:
        return "FORBIDDEN", None
    if code == 404:
        return "NOT_FOUND", None
    return "HTTP_OTHER", None


def find_asof_from_sentinels(api_key: str) -> str:
    """
    asOf는 "엄밀한 거래일"을 전세계로 통일하는 게 불가능하니,
    US-only에서는 sentinel의 quote timestamp(Unix) 또는 서버시간 기반으로 "오늘"을 기록.
    - quote 응답에 'timestamp'가 있으면 그걸 UTC->date로 변환
    - 없으면 그냥 오늘(UTC 기준)로 기록
    """
    # fallback
    fallback = datetime.utcnow().strftime("%Y-%m-%d")

    for sym in SENTINEL_SYMBOLS:
        tag, q = fetch_quote_single(sym, api_key=api_key)
        if tag != "OK" or not isinstance(q, dict):
            continue
        ts = q.get("timestamp")
        try:
            ts_i = int(ts)
            d = datetime.utcfromtimestamp(ts_i).strftime("%Y-%m-%d")
            return d
        except Exception:
            pass

    return fallback


def main() -> None:
    api_key = (os.environ.get("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("❌ Missing env FMP_API_KEY")

    assets = load_us_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ US assets not found in SSOT (country=US & ticker required)")

    # asOf 결정 (US sentinel 기반)
    as_of = find_asof_from_sentinels(api_key=api_key)
    print(f"✅ Using asOf: {as_of}")

    # 스킵/에러 기록
    skipped = {
        "PAYMENT_REQUIRED": [],
        "INVALID_SYMBOL": [],
        "RATE_LIMIT": [],
        "UNAUTHORIZED": [],
        "FORBIDDEN": [],
        "NOT_FOUND": [],
        "HTTP_OTHER": [],
        "BAD_JSON": [],
    }

    items: Dict[str, Dict[str, Any]] = {}
    nonzero_count = 0

    # 진행 로그
    print(f"✅ US assets={len(assets)}")

    # 개별 호출(Starter-friendly). 429 방지 위해 약간 슬립.
    for i, (asset_id, raw_sym) in enumerate(assets.items(), 1):
        sym = normalize_symbol(raw_sym)
        if not sym:
            skipped["INVALID_SYMBOL"].append(raw_sym)
            continue

        # 429 방지: 심볼당 약간 쉬기
        time.sleep(0.12)

        tag, q = fetch_quote_single(sym, api_key=api_key)

        if tag != "OK" or not q:
            skipped.setdefault(tag, []).append(sym)
            # ✅ 핵심: 402 포함 어떤 에러든 "스킵하고 계속"
            if i % 25 == 0:
                print(f"  ... processed {i}/{len(assets)} (skipped so far={sum(len(v) for v in skipped.values())})")
            continue

        close = to_float_or_none(q.get("price"))
        market_cap = to_int_or_none(q.get("marketCap"))
        # stable/quote는 PER를 직접 안 줄 수 있어요. 있으면 쓰고 없으면 None.
        pe_ttm = to_float_or_none(q.get("pe")) if "pe" in q else None

        if (close not in (None, 0, 0.0)) or (market_cap not in (None, 0)) or (pe_ttm not in (None, 0, 0.0)):
            nonzero_count += 1

        items[asset_id] = {
            "ticker": sym,
            "close": close if close is None else float(close),
            "marketCap": market_cap,
            "pe_ttm": pe_ttm,
        }

        if i % 25 == 0:
            print(f"  ... processed {i}/{len(assets)} (ok={len(items)}, skipped={sum(len(v) for v in skipped.values())})")

    if nonzero_count == 0:
        # sentinel sanity
        print("❌ All values are zero/None. Sentinel check:")
        for s in SENTINEL_SYMBOLS:
            tag, q = fetch_quote_single(s, api_key=api_key)
            print("  ", s, "->", tag, ("price=" + str(q.get("price"))) if q else "")
        raise SystemExit("❌ FMP valuation fetch returned no meaningful values. Failing workflow.")

    payload = {
        "asOf": as_of,
        "source": "FMP",
        "scope": "US",
        "items": items,
        "skipped": skipped,
        "updatedAt": datetime.utcnow().strftime("%Y-%m-%d"),
    }

    write_json(OUT_PATH, payload)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero_count}, skipped={sum(len(v) for v in skipped.values())})")


if __name__ == "__main__":
    main()