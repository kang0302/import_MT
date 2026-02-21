# import_MT/scripts/update_valuation_kr.py
# -*- coding: utf-8 -*-

import argparse
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

from pykrx import stock


REPO_ROOT = Path(__file__).resolve().parents[1]
SSOT_PATH = REPO_ROOT / "data" / "ssot" / "asset_ssot.csv"
OUT_PATH = REPO_ROOT / "data" / "cache" / "valuation_kr.json"


def ymd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def iso_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def find_latest_krx_date(start: datetime, max_back_days: int = 30) -> datetime:
    for i in range(max_back_days + 1):
        d = start - timedelta(days=i)
        date_str = ymd(d)
        try:
            df = stock.get_market_cap_by_ticker(date_str)
            if df is not None and len(df.index) > 0:
                return d
        except Exception:
            pass
    raise RuntimeError("최근 영업일(KRX 데이터 존재)을 찾지 못했습니다.")


def load_kr_assets_from_ssot(ssot_path: Path):
    items = []
    with ssot_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"asset_id", "ticker", "country"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"asset_ssot.csv 헤더 누락: {missing}")

        for row in reader:
            if (row.get("country") or "").strip() != "KR":
                continue
            asset_id = (row.get("asset_id") or "").strip()
            ticker = (row.get("ticker") or "").strip()

            if ticker.isdigit():
                ticker6 = ticker.zfill(6)
            else:
                continue

            if asset_id:
                items.append((asset_id, ticker6))
    return items


def safe_float(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def safe_int(x):
    if x is None:
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", help="기준일(YYYY-MM-DD)")
    args = ap.parse_args()

    if args.asof:
        start = datetime.strptime(args.asof, "%Y-%m-%d")
    else:
        start = datetime.now()

    if not SSOT_PATH.exists():
        raise RuntimeError(f"SSOT 파일이 없습니다: {SSOT_PATH}")

    kr_assets = load_kr_assets_from_ssot(SSOT_PATH)
    if not kr_assets:
        raise RuntimeError("KR 종목을 찾지 못했습니다.")

    asof_dt = find_latest_krx_date(start)
    asof_ymd = ymd(asof_dt)
    asof_iso = iso_date(asof_dt)

    cap_df = stock.get_market_cap_by_ticker(asof_ymd)
    fun_df = stock.get_market_fundamental_by_ticker(asof_ymd)

    out_items = {}

    for asset_id, ticker6 in kr_assets:
        market_cap = None
        close_price = None
        pe_ttm = None

        if cap_df is not None and ticker6 in cap_df.index:
            row = cap_df.loc[ticker6]
            market_cap = row.get("시가총액", None)
            close_price = row.get("종가", None)

        if fun_df is not None and ticker6 in fun_df.index:
            pe_ttm = fun_df.loc[ticker6].get("PER", None)

        out_items[asset_id] = {
            "ticker": ticker6,
            "close": safe_int(close_price),
            "marketCap": safe_int(market_cap),
            "pe_ttm": safe_float(pe_ttm),
        }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "asOf": asof_iso,
        "source": "PYKRX",
        "items": out_items,
    }

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote: {OUT_PATH}")
    print(f"      asOf={asof_iso}, KR assets={len(kr_assets)}")


if __name__ == "__main__":
    main()