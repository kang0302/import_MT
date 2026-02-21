# import_MT/scripts/update_valuation_kr.py
import json
from pathlib import Path
from datetime import datetime, timedelta

from pykrx import stock

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
SSOT_PATH = DATA_DIR / "ssot" / "asset_ssot.csv"

OUT_DIR = DATA_DIR / "valuation"
OUT_PATH = OUT_DIR / "kr_valuation.json"

SOURCE_NAME = "PYKRX"


def _today_kst_yyyymmdd():
    # GitHub Actions runner는 UTC일 수 있으니, "오늘"은 일단 로컬 날짜 기준으로 잡고
    # 아래에서 거래일 back-off로 보정한다.
    return datetime.now().strftime("%Y%m%d")


def _find_latest_trading_day(max_back_days: int = 14) -> str:
    """
    pykrx는 거래일이 아니면 데이터가 비거나(EMPTY DF) NaN이 많다.
    그래서 '가장 최근 거래일'을 찾는다.
    """
    d = datetime.now().date()
    for _ in range(max_back_days):
        ds = d.strftime("%Y%m%d")
        try:
            df = stock.get_market_cap_by_ticker(ds)
            if df is not None and len(df) > 0:
                return ds
        except Exception:
            pass
        d = d - timedelta(days=1)

    raise SystemExit("최근 거래일을 찾지 못했습니다. (pykrx 응답/네트워크/휴장기간 확인)")


def _read_asset_ssot_kr():
    """
    asset_ssot.csv에서 country=KR 종목만 읽는다.
    CSV 파서(표준 라이브러리)로 간단 처리. (쉼표 포함 이름 등은 SSOT 규칙상 없음이 전제)
    """
    import csv

    if not SSOT_PATH.exists():
        raise SystemExit(f"asset_ssot.csv not found: {SSOT_PATH}")

    rows = []
    with SSOT_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if (r.get("country") or "").strip().upper() != "KR":
                continue
            asset_id = (r.get("asset_id") or "").strip()
            ticker = (r.get("ticker") or "").strip()

            if not asset_id or not ticker:
                continue

            # pykrx는 6자리 문자열이 안전
            # (005930 같은 0 포함 케이스)
            ticker = ticker.zfill(6)

            rows.append({"asset_id": asset_id, "ticker": ticker})

    return rows


def _safe_float(x):
    try:
        if x is None:
            return None
        # pandas 값은 numpy 타입일 수 있음
        v = float(x)
        # NaN 처리
        if v != v:
            return None
        return v
    except Exception:
        return None


def _safe_int(x):
    try:
        if x is None:
            return None
        v = int(x)
        return v
    except Exception:
        # NaN/문자열 등
        try:
            v = float(x)
            if v != v:
                return None
            return int(v)
        except Exception:
            return None


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) 최근 거래일 찾기
    trade_day = _find_latest_trading_day()
    asof_iso = f"{trade_day[:4]}-{trade_day[4:6]}-{trade_day[6:8]}"

    print(f"[OK] valuation date (latest trading day): {trade_day} ({asof_iso})")

    # 2) pykrx에서 전체 테이블 한번에 뽑기(가장 빠르고 안정적)
    cap_df = stock.get_market_cap_by_ticker(trade_day)  # 시가총액
    fund_df = stock.get_market_fundamental_by_ticker(trade_day)  # PER 등
    ohlcv_df = stock.get_market_ohlcv_by_ticker(trade_day)  # 종가 포함

    # 3) SSOT의 KR 종목만 필터링
    kr_assets = _read_asset_ssot_kr()

    items = {}
    for r in kr_assets:
        aid = r["asset_id"]
        tic = r["ticker"]

        # DataFrame 인덱스가 ticker 문자열(예: '005930')일 때가 일반적
        # 혹시 숫자 인덱스로 들어오는 경우를 대비해 두 번 시도
        def _get_row(df):
            if df is None:
                return None
            if tic in df.index:
                return df.loc[tic]
            try:
                t2 = int(tic)
                if t2 in df.index:
                    return df.loc[t2]
            except Exception:
                pass
            return None

        cap_row = _get_row(cap_df)
        fund_row = _get_row(fund_df)
        ohlcv_row = _get_row(ohlcv_df)

        # pykrx 컬럼명(한글) 기준: 종가 / 시가총액 / PER
        close = _safe_int(ohlcv_row["종가"]) if ohlcv_row is not None and "종가" in ohlcv_row else None
        market_cap = _safe_int(cap_row["시가총액"]) if cap_row is not None and "시가총액" in cap_row else None

        pe_ttm = None
        if fund_row is not None:
            # fund_df는 보통 컬럼에 PER이 있음
            # 값이 '-' 또는 NaN일 수 있음
            if "PER" in fund_row:
                pe_ttm = _safe_float(fund_row["PER"])

        items[aid] = {
            "ticker": tic,
            "close": close,
            "marketCap": market_cap,
            "pe_ttm": pe_ttm,
            # 개별 항목에도 넣어두면 build_freeze에서 주입하기 편함
            "valuationAsOf": asof_iso,
            "valuationSource": SOURCE_NAME,
        }

    out = {
        "asOf": asof_iso,
        "source": SOURCE_NAME,
        "items": items,
    }

    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote: {OUT_PATH}")
    print(f"[OK] KR items: {len(items)}")


if __name__ == "__main__":
    main()