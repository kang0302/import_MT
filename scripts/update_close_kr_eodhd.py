# import_MT/scripts/update_close_kr_eodhd.py
# MoneyTree - KR Close + Returns Updater (EODHD)
# - EODHD에서 KR 종가/시계열을 한 번에 수집
# - data/cache/valuation_kr.json의 items[*].close 갱신 (marketCap/pe_ttm 등 보존)
# - data/cache/returns_kr.json도 동시에 갱신 (return_3d / 7d / 1m / ytd / 1y / 3y)
# - 휴장일이어도 "가장 최근 거래일" 데이터를 사용(실패로 떨어지지 않게)
# - PYKRX 의존 제거 (EODHD 단일 소스)

import csv
import io
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import requests

# Windows cp949 콘솔에서도 unicode 출력으로 죽지 않게 강제 utf-8 + replace.
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"
CACHE_DIR = DATA_DIR / "cache"
VAL_PATH = CACHE_DIR / "valuation_kr.json"
RET_PATH = CACHE_DIR / "returns_kr.json"

EOD_BASE = "https://eodhd.com/api/eod"

SLEEP_SEC = float(os.environ.get("EODHD_SLEEP_SEC", "0.15"))
RETRIES = int(os.environ.get("EODHD_RETRIES", "3"))
TIMEOUT_SEC = int(os.environ.get("EODHD_TIMEOUT_SEC", "30"))

# 3년 + 30일 버퍼: return_3y(거래일 ~756일 lookback) 안전하게 커버.
WINDOW_DAYS = int(os.environ.get("EODHD_WINDOW_DAYS", str(365 * 3 + 30)))

# 최신 거래일 판별용(삼성전자)
SENTINEL = os.environ.get("EODHD_SENTINEL", "005930.KO")

RETURN_KEYS = ["return_3d", "return_7d", "return_1m", "return_ytd", "return_1y", "return_3y"]


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_kr_assets_from_ssot() -> Dict[str, str]:
    """
    return: asset_id -> ticker(6자리 숫자)
    """
    if not SSOT_PATH.exists():
        raise SystemExit(f"❌ SSOT not found: {SSOT_PATH}")

    out: Dict[str, str] = {}
    with SSOT_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        need = {"asset_id", "ticker", "country"}
        if not need.issubset(set(r.fieldnames or [])):
            raise SystemExit(f"❌ asset_ssot.csv missing columns. need={need} got={r.fieldnames}")

        for row in r:
            if (row.get("country") or "").strip().upper() != "KR":
                continue
            aid = (row.get("asset_id") or "").strip()
            tkr = (row.get("ticker") or "").strip()
            if not aid or not tkr:
                continue

            if tkr.isdigit():
                tkr = tkr.zfill(6)

            # 숫자 6자리만 KR 대상으로 삼음(특수코드 제외)
            if not (tkr.isdigit() and len(tkr) == 6):
                continue

            out[aid] = tkr

    return out


def _eod_fetch(symbol: str, api_key: str, date_from: str, date_to: str) -> Optional[List[dict]]:
    url = f"{EOD_BASE}/{symbol}"
    params = {
        "api_token": api_key,
        "period": "d",
        "fmt": "json",
        "from": date_from,
        "to": date_to,
    }

    last_err = None
    for i in range(RETRIES):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT_SEC)
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
            else:
                data = r.json()
                if isinstance(data, list):
                    return data
                last_err = f"Invalid JSON type: {type(data)}"
        except Exception as e:
            last_err = repr(e)

        if i < RETRIES - 1:
            time.sleep(0.5 + i * 0.8)

    print(f"⚠️ EOD fetch failed for {symbol}: {last_err}")
    return None


def eod_history(symbol: str, api_key: str) -> Optional[List[Dict[str, Any]]]:
    """
    return: 날짜 오름차순 정렬된 [{"date":"...","close":...}, ...] 또는 None.
    - 3y+ 윈도로 한 번 가져와서 close/returns 둘 다에 재사용.
    - 빈 리스트면 None 반환(다음 거래소 fallback 트리거).
    """
    today = datetime.now().date()
    d_from = (today - timedelta(days=WINDOW_DAYS)).isoformat()
    d_to = today.isoformat()

    data = _eod_fetch(symbol, api_key, d_from, d_to)
    if not data:
        return None

    rows: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        d = row.get("date")
        c = row.get("close")
        ac = row.get("adjusted_close")
        # 수익률은 split/dividend 보정된 adjusted_close 우선
        c_use = ac if isinstance(ac, (int, float)) else c
        if not d or c_use is None:
            continue
        try:
            c_use = float(c_use)
            c_raw = float(c) if c is not None else c_use
        except Exception:
            continue
        if not (c_use > 0):
            continue
        rows.append({"date": str(d), "close": c_raw, "adj": c_use})

    if not rows:
        return None
    rows.sort(key=lambda r: r["date"])
    return rows


def latest_from_history(rows: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[float]]:
    if not rows:
        return (None, None)
    last = rows[-1]
    return (last["date"], last["close"])


def compute_returns_from_history(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """
    거래일 인덱스 기반으로 3d/7d/1m/1y/3y 계산. ytd는 해당 연도 첫 거래일 close 기준.
    수익률 계산엔 adj(분할/배당 보정) 사용.
    """
    out = {k: None for k in RETURN_KEYS}
    if not rows:
        return out

    last = rows[-1]
    last_adj = last["adj"]
    last_date = last["date"]

    def back(n: int) -> Optional[float]:
        i = len(rows) - 1 - n
        if i < 0:
            return None
        base = rows[i]["adj"]
        if not (base > 0):
            return None
        return (last_adj / base - 1.0) * 100.0

    out["return_3d"] = back(3)
    out["return_7d"] = back(7)
    out["return_1m"] = back(21)
    out["return_1y"] = back(252)
    out["return_3y"] = back(756)

    # YTD: 해당 연도의 첫 거래일
    y = last_date[:4]
    ytd_base = None
    for r in rows:
        if r["date"].startswith(y):
            ytd_base = r["adj"]
            break
    if ytd_base and ytd_base > 0:
        out["return_ytd"] = (last_adj / ytd_base - 1.0) * 100.0

    return out


def resolve_symbol_candidates(ticker6: str) -> Tuple[str, str]:
    return (f"{ticker6}.KO", f"{ticker6}.KQ")


def main() -> None:
    print("=== Update KR Close + Returns (EODHD) Start ===")
    api_key = (os.environ.get("EODHD_API_KEY") or "").strip()

    # 키 없으면 기존 캐시 유지하고 종료(파이프라인 끊김 방지)
    if not api_key:
        print("⚠️ EODHD_API_KEY missing. Keep existing cache and exit 0.")
        if VAL_PATH.exists() or RET_PATH.exists():
            print(f"✅ keep existing caches")
            return
        raise SystemExit("❌ No API key and no existing cache to fall back on.")

    assets = load_kr_assets_from_ssot()
    print(f"✅ KR assets from SSOT (6-digit only): {len(assets)}")

    # 기존 valuation_kr.json 로드(있으면 marketCap/pe_ttm 등 보존)
    base_val: Dict[str, Any]
    if VAL_PATH.exists():
        try:
            base_val = read_json(VAL_PATH)
            if not isinstance(base_val, dict):
                base_val = {}
        except Exception:
            base_val = {}
    else:
        base_val = {}

    val_items = base_val.get("items") if isinstance(base_val.get("items"), dict) else {}
    if not isinstance(val_items, dict):
        val_items = {}

    # Sentinel로 최근 거래일 기준 잡기 (close만 빠르게 한 번)
    sentinel_rows = eod_history(SENTINEL, api_key)
    sd, _sc = latest_from_history(sentinel_rows or [])
    if sd:
        as_of = sd
        print(f"✅ Sentinel OK: {SENTINEL} latest={sd}")
    else:
        as_of = datetime.now().strftime("%Y-%m-%d")
        print("⚠️ Sentinel failed. Continue anyway; asOf=today (best-effort).")

    ret_items: Dict[str, Dict[str, Any]] = {}
    val_updated = 0
    ret_updated = 0
    failed = 0
    last_ok_date = as_of

    for i, (aid, tkr) in enumerate(assets.items(), 1):
        sym1, sym2 = resolve_symbol_candidates(tkr)
        rows = eod_history(sym1, api_key)
        if not rows:
            rows = eod_history(sym2, api_key)

        cur = val_items.get(aid) if isinstance(val_items.get(aid), dict) else {}
        if not isinstance(cur, dict):
            cur = {}
        cur["ticker"] = tkr

        if not rows:
            failed += 1
            cur.setdefault("close", None)
            # returns_kr.items[aid]는 모두 None으로 채워두면 build_freeze가 의미 없는 값으로 기존 데이터 덮을 수 있어서 아예 생략
        else:
            d, c = latest_from_history(rows)
            if d and str(d) > str(last_ok_date):
                last_ok_date = str(d)
            if c is not None:
                cur["close"] = int(round(c))
                val_updated += 1

            rets = compute_returns_from_history(rows)
            # returns 항목 중 단 하나라도 valid 값이 있으면 cache에 등록
            if any(isinstance(v, (int, float)) for v in rets.values()):
                ret_items[aid] = {"ticker": tkr, **rets}
                ret_updated += 1

        val_items[aid] = cur

        if i % 100 == 0:
            print(f"  ... processed {i}/{len(assets)} (val_updated={val_updated}, ret_updated={ret_updated}, failed={failed})")

        time.sleep(SLEEP_SEC)

    # valuation_kr.json
    val_out = {
        "asOf": last_ok_date,
        "source": "EODHD",
        "items": val_items,
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
        "notes": "KR close via EODHD; marketCap/pe_ttm preserved if pre-existing",
    }
    write_json_atomic(VAL_PATH, val_out)
    print(f"✅ wrote: {VAL_PATH}  (close updated: {val_updated})")

    # returns_kr.json
    ret_out = {
        "asOf": last_ok_date,
        "source": "EODHD",
        "items": ret_items,
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
        "notes": "KR returns via EODHD adjusted_close (3d/7d/1m/ytd/1y/3y, trading-day index based)",
    }
    write_json_atomic(RET_PATH, ret_out)
    print(f"✅ wrote: {RET_PATH}  (assets with returns: {ret_updated})")

    print(f"✅ failed (no data on either .KO/.KQ): {failed}")
    print("=== Update KR Close + Returns (EODHD) Completed ===")


if __name__ == "__main__":
    main()