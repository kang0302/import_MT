# import_MT/scripts/update_valuation_kr.py
import csv
import json
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from pykrx import stock

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "valuation_kr.json"

SENTINEL_TICKERS = ["005930", "000660"]
LOOKBACK_DAYS = 14
FALLBACK_DAYS = 30


# -----------------------------
# helpers
# -----------------------------
def write_json_atomic(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def to_int_or_none(x):
    try:
        if x in (None, "", "-", "N/A"):
            return None
        return int(float(x))
    except Exception:
        return None


def to_float_or_none(x):
    try:
        if x in (None, "", "-", "N/A"):
            return None
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


# -----------------------------
# SSOT load (csv 안전 파싱)
# -----------------------------
def load_kr_assets_from_ssot() -> Dict[str, str]:
    if not SSOT_PATH.exists():
        raise SystemExit(f"❌ SSOT not found: {SSOT_PATH}")

    out: Dict[str, str] = {}
    with SSOT_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"asset_id", "ticker", "country"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise SystemExit(f"❌ asset_ssot.csv missing columns: {required} / got={reader.fieldnames}")

        for row in reader:
            country = (row.get("country") or "").strip().upper()
            if country != "KR":
                continue

            aid = (row.get("asset_id") or "").strip()
            tkr = (row.get("ticker") or "").strip()

            if not aid or not tkr:
                continue

            if tkr.isdigit() and len(tkr) <= 6:
                tkr = tkr.zfill(6)

            out[aid] = tkr

    return out


# -----------------------------
# trading day detect
# -----------------------------
def detect_latest_trading_day(today: date) -> Optional[date]:
    """
    휴장/주말이면 센티넬 OHLCV로 최근 거래일을 잡는다.
    """
    start = today - timedelta(days=LOOKBACK_DAYS)
    for t in SENTINEL_TICKERS:
        try:
            df = stock.get_market_ohlcv_by_date(yyyymmdd(start), yyyymmdd(today), t)
            if df is not None and not df.empty:
                return df.index[-1].date()
        except Exception:
            continue
    return None


def fetch_cap_only(ds: str):
    """
    ✅ CAP만 필수. (종가/시총)
    """
    try:
        cap_df = stock.get_market_cap_by_ticker(ds)
        if cap_df is None or cap_df.empty:
            return None
        return cap_df
    except Exception:
        return None


def fetch_fundamental_optional(ds: str):
    """
    ✅ PER는 optional. 실패/KeyError/빈 DF면 None 반환.
    """
    try:
        df = stock.get_market_fundamental_by_ticker(ds)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def find_valid_trading_day_for_cap(today: date) -> Tuple[str, str]:
    """
    cap_df가 정상으로 확보되는 날짜를 찾는다.
    """
    latest = detect_latest_trading_day(today)
    if latest is None:
        raise SystemExit("❌ 최근 거래일 탐지 실패 (네트워크/pykrx/KRX 이슈 가능)")

    for i in range(FALLBACK_DAYS + 1):
        cand = latest - timedelta(days=i)
        ds = yyyymmdd(cand)
        cap_df = fetch_cap_only(ds)
        if cap_df is not None:
            return ds, cand.strftime("%Y-%m-%d")

    raise SystemExit("❌ cap 데이터 확보 실패 (네트워크/pykrx/KRX 이슈 가능)")


def main():
    print("=== Update KR Valuation Start ===")

    assets = load_kr_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ KR assets not found in SSOT")

    today = date.today()
    trade_day, as_of = find_valid_trading_day_for_cap(today)
    print(f"✅ Using trading day: {trade_day} (asOf={as_of})")

    cap_df = fetch_cap_only(trade_day)
    if cap_df is None:
        raise SystemExit(f"❌ cap df empty for {trade_day} (unexpected after selection)")

    # fundamental은 optional
    fun_df = fetch_fundamental_optional(trade_day)
    if fun_df is None:
        print("⚠️ fundamental df unavailable. Proceeding with CAP-only (pe_ttm=None).")

    items: Dict[str, Dict[str, Any]] = {}
    nonzero = 0

    for aid, tkr in assets.items():
        close = None
        mcap = None
        pe = None

        if tkr in cap_df.index:
            r = cap_df.loc[tkr]
            close = to_int_or_none(r.get("종가"))
            mcap = to_int_or_none(r.get("시가총액"))

        if fun_df is not None and tkr in fun_df.index:
            r2 = fun_df.loc[tkr]
            pe = to_float_or_none(r2.get("PER"))
            if pe is not None and pe <= 0:
                pe = None

        if (close not in (None, 0)) or (mcap not in (None, 0)) or (pe is not None):
            nonzero += 1

        items[aid] = {
            "ticker": tkr,
            "close": close,
            "marketCap": mcap,
            "pe_ttm": pe,  # optional
        }

    if nonzero == 0:
        raise SystemExit("❌ 모든 값이 None/0 → cap 데이터 이상")

    out = {
        "asOf": as_of,
        "source": "PYKRX",
        "items": items,
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
    }

    write_json_atomic(OUT_PATH, out)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero})")
    print("=== Update KR Valuation Completed ===")


if __name__ == "__main__":
    main()