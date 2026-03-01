import json
import os
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests


BASE_DIR = Path(__file__).resolve().parents[1]          # .../moneytree-web/import_MT
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"
CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "valuation_fmp.json"

FMP_BASE = "https://financialmodelingprep.com"
# Stable Quote endpoint (single symbol)
# https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey=...
# (Docs show same endpoint used widely)  :contentReference[oaicite:2]{index=2}
QUOTE_PATH = "/stable/quote"

# ---- behavior knobs
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS = 0.20  # Ultimate에서도 과호출 방지용(필요시 0.05까지 낮춰도 됨)
MAX_RETRIES = 2

# 심볼별로 402/403 등 뜨면 스킵하고 계속
SKIP_HTTP_STATUS = {401, 402, 403, 404}


# =========================
# helpers
# =========================
def now_kst_iso() -> str:
    # KST(UTC+9) 느낌으로 맞추고 싶으면 여기서 조정 가능.
    # 지금은 "asOf"를 YYYY-MM-DD로만 쓰므로 ISO는 로그용.
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def write_json_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def http_get_json(url: str, params: Dict[str, Any]) -> Any:
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def to_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def to_int_or_none(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() == "nan":
            return None
        return int(float(s))
    except Exception:
        return None


def normalize_symbol(sym: str) -> str:
    """
    - 공백 제거
    - 끝에 '.' 붙는 케이스(예: 'BA.') 제거
    - 그 외는 그대로(글로벌은 거래소 접미사/포맷이 다양해서 임의 변환 금지)
    """
    s = (sym or "").strip()
    while s.endswith("."):
        s = s[:-1]
    return s


def parse_quote_payload(payload: Any) -> Optional[Dict[str, Any]]:
    """
    stable/quote 응답은 보통 list[dict] 형태.
    예시(네가 브라우저에서 확인한 형태):
      [
        {
          "symbol":"AAPL", "price":274.23, "marketCap":..., "pe":..., "timestamp":...
        }
      ]
    """
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    if isinstance(payload, dict) and payload.get("symbol"):
        return payload
    return None


def load_overseas_assets_from_ssot() -> Dict[str, Dict[str, str]]:
    """
    SSOT에서 KR 제외(=해외/글로벌) 자산을 읽는다.
    반환: {asset_id: {"ticker": "...", "country": "...", "exchange": "..."} }
    """
    import csv

    if not SSOT_PATH.exists():
        raise SystemExit(f"SSOT not found: {SSOT_PATH}")

    out: Dict[str, Dict[str, str]] = {}
    with SSOT_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            aid = (row.get("asset_id") or "").strip()
            country = (row.get("country") or "").strip().upper()
            ticker = (row.get("ticker") or "").strip()
            exchange = (row.get("exchange") or "").strip()

            if not aid:
                continue
            if country == "KR":
                continue
            if not ticker:
                continue

            out[aid] = {
                "ticker": normalize_symbol(ticker),
                "country": country,
                "exchange": exchange,
            }

    return out


# =========================
# main logic
# =========================
def fetch_quote(symbol: str, api_key: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    return (quote_dict_or_none, error_string_or_none)
    """
    url = f"{FMP_BASE}{QUOTE_PATH}"
    params = {"symbol": symbol, "apikey": api_key}

    for attempt in range(MAX_RETRIES + 1):
        try:
            data = http_get_json(url, params)
            q = parse_quote_payload(data)
            if not q:
                return (None, "empty_quote_payload")
            return (q, None)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in SKIP_HTTP_STATUS:
                return (None, f"http_{status}")
            if attempt >= MAX_RETRIES:
                return (None, f"http_error:{status}")
            time.sleep(0.5 + attempt * 0.5)
        except Exception as e:
            if attempt >= MAX_RETRIES:
                return (None, f"error:{type(e).__name__}")
            time.sleep(0.5 + attempt * 0.5)

    return (None, "unknown")


def main() -> None:
    api_key = (os.getenv("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("FMP_API_KEY is missing (set env var)")

    # Debug trace (Actions에서 Key 자체는 노출 금지)
    sha = hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:12]
    print(f"✅ FMP_API_KEY length={len(api_key)} sha256={sha}...")

    assets = load_overseas_assets_from_ssot()
    print(f"✅ Overseas assets={len(assets)} (KR excluded)")

    items: Dict[str, Dict[str, Any]] = {}
    nonzero = 0
    skipped = 0

    for idx, (aid, meta) in enumerate(assets.items(), start=1):
        sym = meta["ticker"]
        q, err = fetch_quote(sym, api_key)

        if err:
            # 402/403/404 등은 글로벌이라도 심볼별로 뜰 수 있으니 스킵
            skipped += 1
            print(f"⚠ skip {aid} sym={sym} err={err}")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        close = to_float_or_none(q.get("price"))
        mcap = to_int_or_none(q.get("marketCap"))
        pe = to_float_or_none(q.get("pe"))

        items[aid] = {
            "ticker": sym,
            "exchange": meta.get("exchange", ""),
            "country": meta.get("country", ""),
            "close": close,
            "marketCap": mcap,
            "pe_ttm": pe,
        }

        if close not in (None, 0, 0.0) or mcap not in (None, 0) or pe not in (None, 0, 0.0):
            nonzero += 1

        if idx % 25 == 0:
            print(f"  ...progress {idx}/{len(assets)} (nonzero={nonzero}, skipped={skipped})")

        time.sleep(SLEEP_BETWEEN_CALLS)

    as_of = yyyymmdd(datetime.utcnow())
    out = {"asOf": as_of, "source": "FMP", "items": items}
    write_json_atomic(OUT_PATH, out)

    print(f"✅ wrote: {OUT_PATH} items={len(items)} nonzero={nonzero} skipped={skipped}")
    if nonzero == 0:
        raise SystemExit("❌ FMP stable quote returned no meaningful values. Check plan/key/symbol formats.")


if __name__ == "__main__":
    main()