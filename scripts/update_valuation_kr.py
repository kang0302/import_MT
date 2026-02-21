# import_MT/scripts/update_valuation_kr.py
import json
from datetime import datetime, timedelta
from pathlib import Path

from pykrx import stock

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "valuation_kr.json"

SENTINEL_TICKERS = ["005930", "000660"]  # 거래일 판별용(삼성전자/하이닉스)


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


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


def load_kr_assets_from_ssot():
    if not SSOT_PATH.exists():
        raise SystemExit(f"❌ SSOT not found: {SSOT_PATH}")

    txt = SSOT_PATH.read_text(encoding="utf-8-sig").splitlines()
    if not txt:
        raise SystemExit("❌ asset_ssot.csv is empty")

    header = [h.strip() for h in txt[0].split(",")]
    idx_asset = header.index("asset_id")
    idx_ticker = header.index("ticker")
    idx_country = header.index("country")

    out = {}
    for line in txt[1:]:
        if not line.strip():
            continue
        cols = [c.strip() for c in line.split(",")]
        if len(cols) <= max(idx_asset, idx_ticker, idx_country):
            continue
        if (cols[idx_country] or "").upper() != "KR":
            continue
        aid = cols[idx_asset]
        t = (cols[idx_ticker] or "").strip()
        if not t:
            continue
        out[aid] = t.zfill(6)

    return out


def is_valid_trading_day(ds: str) -> bool:
    """
    ✅ 'len(df)>0' 같은 약한 판별이 아니라,
    대표 종목 종가/시총이 0이 아닌지로 거래일 판별.
    """
    try:
        cap_df = stock.get_market_cap_by_ticker(ds)
        if cap_df is None or len(cap_df) == 0:
            return False

        # cap_df 컬럼: 종가, 시가총액 ...
        for t in SENTINEL_TICKERS:
            if t in cap_df.index:
                r = cap_df.loc[t]
                close = r.get("종가", 0)
                mcap = r.get("시가총액", 0)
                try:
                    close = int(close)
                    mcap = int(mcap)
                except Exception:
                    close = 0
                    mcap = 0

                if close > 0 and mcap > 0:
                    return True

        return False
    except Exception:
        return False


def find_latest_trading_day(max_back_days: int = 30) -> str:
    """
    오늘부터 거꾸로 내려가며 '진짜 거래일'을 찾는다.
    """
    d = datetime.now()

    # 추가 안전: 주말이면 금요일부터 시작하도록 1~2일 미리 당김
    # (월=0 ... 일=6)
    if d.weekday() == 5:   # 토
        d = d - timedelta(days=1)
    elif d.weekday() == 6: # 일
        d = d - timedelta(days=2)

    for i in range(max_back_days + 1):
        cand = d - timedelta(days=i)
        ds = yyyymmdd(cand)
        if is_valid_trading_day(ds):
            return ds

    raise SystemExit("❌ 최근 거래일을 찾지 못했습니다. (휴장/네트워크/pykrx 이슈 가능)")


def main():
    assets = load_kr_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ KR assets not found in SSOT (country=KR & ticker required)")

    trade_day = find_latest_trading_day(max_back_days=30)
    as_of = datetime.strptime(trade_day, "%Y%m%d").strftime("%Y-%m-%d")
    print(f"✅ Using trading day: {trade_day} (asOf={as_of})")

    cap_df = stock.get_market_cap_by_ticker(trade_day)
    fun_df = stock.get_market_fundamental_by_ticker(trade_day)

    if cap_df is None or len(cap_df) == 0:
        raise SystemExit(f"❌ market cap df empty for {trade_day}")
    if fun_df is None or len(fun_df) == 0:
        raise SystemExit(f"❌ fundamental df empty for {trade_day}")

    items = {}
    nonzero_count = 0

    for asset_id, tkr in assets.items():
        close = None
        mcap = None
        pe = None

        if tkr in cap_df.index:
            row = cap_df.loc[tkr]
            close = row.get("종가", None)
            mcap = row.get("시가총액", None)

        if tkr in fun_df.index:
            row2 = fun_df.loc[tkr]
            pe = row2.get("PER", None)

        try:
            close = int(close) if close not in (None, "") else None
        except Exception:
            close = None

        try:
            mcap = int(mcap) if mcap not in (None, "") else None
        except Exception:
            mcap = None

        pe = to_float_or_none(pe)

        if (close not in (None, 0)) or (mcap not in (None, 0)) or (pe not in (None, 0, 0.0)):
            nonzero_count += 1

        items[asset_id] = {
            "ticker": tkr,
            "close": close,
            "marketCap": mcap,
            "pe_ttm": pe,
        }

    if nonzero_count == 0:
        print("❌ All values are zero/None. Sample check:")
        for s in SENTINEL_TICKERS + ["035420", "051910"]:
            try:
                if s in cap_df.index:
                    r = cap_df.loc[s]
                    print(s, "종가=", r.get("종가"), "시총=", r.get("시가총액"))
            except Exception:
                pass
        raise SystemExit("❌ KR valuation fetch returned no meaningful values. Failing workflow.")

    out = {
        "asOf": as_of,
        "source": "PYKRX",
        "items": items,
    }

    write_json(OUT_PATH, out)
    print(f"✅ wrote: {OUT_PATH} (items={len(items)}, nonzero={nonzero_count})")


if __name__ == "__main__":
    main()