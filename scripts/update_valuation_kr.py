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


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def find_latest_trading_day(max_back_days: int = 14) -> str:
    """
    오늘부터 거꾸로 내려가며,
    'get_market_cap_by_ticker'가 비어있지 않은 첫 날짜를 거래일로 간주.
    """
    today = datetime.now()
    for i in range(max_back_days + 1):
        d = today - timedelta(days=i)
        ds = yyyymmdd(d)
        try:
            cap_df = stock.get_market_cap_by_ticker(ds)
            if cap_df is not None and len(cap_df) > 0:
                return ds
        except Exception:
            pass

    raise SystemExit("❌ 최근 거래일을 찾지 못했습니다. (휴장/네트워크/pykrx 이슈 가능)")


def load_kr_assets_from_ssot():
    """
    SSOT에서 country=KR & ticker가 있는 항목만 읽어서
    {asset_id: ticker6} 반환
    """
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


def to_float_or_none(x):
    if x is None:
        return None
    try:
        # pykrx는 '-' 같은 값이 올 수 있음
        if isinstance(x, str) and x.strip() in ("", "-", "N/A"):
            return None
        return float(x)
    except Exception:
        return None


def main():
    assets = load_kr_assets_from_ssot()
    if not assets:
        raise SystemExit("❌ KR assets not found in SSOT (country=KR & ticker required)")

    trade_day = find_latest_trading_day(max_back_days=14)
    as_of = datetime.strptime(trade_day, "%Y%m%d").strftime("%Y-%m-%d")

    print(f"✅ Using trading day: {trade_day} (asOf={as_of})")

    # 1) 시총/종가
    cap_df = stock.get_market_cap_by_ticker(trade_day)
    # 2) PER 등 펀더멘털
    fun_df = stock.get_market_fundamental_by_ticker(trade_day)

    if cap_df is None or len(cap_df) == 0:
        raise SystemExit(f"❌ market cap df empty for {trade_day}")
    if fun_df is None or len(fun_df) == 0:
        raise SystemExit(f"❌ fundamental df empty for {trade_day}")

    # pykrx DF index는 보통 ticker(종목코드)
    # cap_df columns: 종가, 시가총액, ...
    # fun_df columns: PER, ...
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

        close = int(close) if close not in (None, "") else None
        mcap = int(mcap) if mcap not in (None, "") else None
        pe = to_float_or_none(pe)

        # ✅ 의미있는 값 카운트 (둘 중 하나라도 있으면 성공으로 봄)
        if (close not in (None, 0)) or (mcap not in (None, 0)) or (pe not in (None, 0, 0.0)):
            nonzero_count += 1

        items[asset_id] = {
            "ticker": tkr,
            "close": close if close is not None else None,
            "marketCap": mcap if mcap is not None else None,
            "pe_ttm": pe,
        }

    # ✅ 안전장치: 전부 0/None이면 “수집 실패”로 간주하고 Action 실패 처리
    if nonzero_count == 0:
        # 디버그용으로 대표 종목 몇개 찍기
        sample = ["005930", "000660", "035420", "051910"]
        print("❌ All values are zero/None. Sample check:")
        for s in sample:
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