import json
import os
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE_DIR = Path(__file__).resolve().parents[1]          # .../moneytree-web/import_MT
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"
CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "returns_fmp.json"

FMP_BASE = "https://financialmodelingprep.com"

# Stable EOD full history endpoint (docs show: /stable/historical-price-eod/full?symbol=...)
# :contentReference[oaicite:3]{index=3}
HIST_EOD_FULL_PATH = "/stable/historical-price-eod/full"

REQUEST_TIMEOUT = 25
SLEEP_BETWEEN_CALLS = 0.25
MAX_RETRIES = 2
SKIP_HTTP_STATUS = {401, 402, 403, 404}

RETURN_KEYS = ["return_3d", "return_7d", "return_1m", "return_ytd", "return_1y", "return_3y"]


# =========================
# helpers
# =========================
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


def normalize_symbol(sym: str) -> str:
    s = (sym or "").strip()
    while s.endswith("."):
        s = s[:-1]
    return s


def load_overseas_assets_from_ssot() -> Dict[str, Dict[str, str]]:
    """
    KR 제외(=해외/글로벌) 자산만 returns 대상으로.
    반환: {asset_id: {"ticker":"...", "country":"..", "exchange":".."}}
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


def parse_hist_payload(payload: Any) -> List[Dict[str, Any]]:
    """
    기대 형태(대부분):
      {"symbol":"AAPL", "historical":[{"date":"2026-02-26","close":...}, ...]}
    """
    if isinstance(payload, dict):
        hist = payload.get("historical")
        if isinstance(hist, list):
            return [x for x in hist if isinstance(x, dict)]
    return []


def compute_returns_from_closes(hist: List[Dict[str, Any]]) -> Tuple[Optional[str], Dict[str, Optional[float]]]:
    """
    hist: [{"date": "...", "close": ...}, ...]  (최신이 앞일 수도 뒤일 수도 있음)
    - trading day index 기반으로 3d/7d/1m/1y/3y 계산
    - ytd는 해당 연도 첫 거래일 close 기준
    """
    rows: List[Tuple[str, float]] = []
    for r in hist:
        d = (r.get("date") or "").strip()
        c = to_float_or_none(r.get("close"))
        if not d or c is None:
            continue
        rows.append((d, c))

    if not rows:
        return (None, {k: None for k in RETURN_KEYS})

    # 날짜 오름차순 정렬
    rows.sort(key=lambda x: x[0])

    as_of_date = rows[-1][0]
    last_close = rows[-1][1]

    def ret_at_back(trading_days_back: int) -> Optional[float]:
        i = len(rows) - 1 - trading_days_back
        if i < 0:
            return None
        base = rows[i][1]
        if base in (0, 0.0) or base is None:
            return None
        return (last_close / base - 1.0) * 100.0

    # YTD: 같은 연도의 첫 거래일
    y = as_of_date[:4]
    ytd_base = None
    for d, c in rows:
        if d.startswith(y):
            ytd_base = c
            break
    ytd_ret = None
    if ytd_base not in (None, 0, 0.0):
        ytd_ret = (last_close / ytd_base - 1.0) * 100.0

    out = {
        "return_3d": ret_at_back(3),
        "return_7d": ret_at_back(7),
        "return_1m": ret_at_back(21),
        "return_ytd": ytd_ret,
        "return_1y": ret_at_back(252),
        "return_3y": ret_at_back(756),
    }
    return (as_of_date, out)


def fetch_history(symbol: str, api_key: str, from_date: str, to_date: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    url = f"{FMP_BASE}{HIST_EOD_FULL_PATH}"
    params = {"symbol": symbol, "apikey": api_key, "from": from_date, "to": to_date}

    for attempt in range(MAX_RETRIES + 1):
        try:
            data = http_get_json(url, params)
            hist = parse_hist_payload(data)
            return (hist, None)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in SKIP_HTTP_STATUS:
                return ([], f"http_{status}")
            if attempt >= MAX_RETRIES:
                return ([], f"http_error:{status}")
            time.sleep(0.7 + attempt * 0.7)
        except Exception as e:
            if attempt >= MAX_RETRIES:
                return ([], f"error:{type(e).__name__}")
            time.sleep(0.7 + attempt * 0.7)

    return ([], "unknown")


def main() -> None:
    api_key = (os.getenv("FMP_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("FMP_API_KEY is missing (set env var)")

    sha = hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:12]
    print(f"✅ FMP_API_KEY length={len(api_key)} sha256={sha}...")

    assets = load_overseas_assets_from_ssot()
    unique_symbols = len(set(v["ticker"] for v in assets.values()))
    print(f"✅ Overseas assets={len(assets)} uniqueSymbols={unique_symbols} (KR excluded)")

    # 3y 계산하려면 3y+버퍼 만큼
    today = datetime.utcnow()
    from_dt = today - timedelta(days=365 * 3 + 30)
    from_date = yyyymmdd(from_dt)
    to_date = yyyymmdd(today)

    items: Dict[str, Dict[str, Any]] = {}
    ok = 0
    skipped = 0
    last_asof_seen: Optional[str] = None

    for idx, (aid, meta) in enumerate(assets.items(), start=1):
        sym = meta["ticker"]

        hist, err = fetch_history(sym, api_key, from_date, to_date)
        if err:
            skipped += 1
            print(f"⚠ skip {aid} sym={sym} err={err}")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        asof, rets = compute_returns_from_closes(hist)
        if asof:
            last_asof_seen = asof

        items[aid] = {
            "ticker": sym,
            "exchange": meta.get("exchange", ""),
            "country": meta.get("country", ""),
            **rets,
        }
        ok += 1

        if idx % 15 == 0:
            print(f"  ...progress {idx}/{len(assets)} ok={ok} skipped={skipped}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    out_asof = last_asof_seen or yyyymmdd(today)
    out = {"asOf": out_asof, "source": "FMP", "items": items}
    write_json_atomic(OUT_PATH, out)

    print(f"✅ wrote: {OUT_PATH} items={len(items)} ok={ok} skipped={skipped}")
    if ok == 0:
        raise SystemExit("❌ No returns computed (all skipped). Check plan/key/symbol formats.")


if __name__ == "__main__":
    main()