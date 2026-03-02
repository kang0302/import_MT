# import_MT/scripts/update_return_kr.py
# MoneyTree - KR Returns Cache Builder (PYKRX)
#
# Output:
#   data/cache/returns_kr.json
#
# Schema (assetId 키):
# {
#   "asOf": "YYYY-MM-DD",
#   "source": "PYKRX",
#   "items": {
#     "A_088": {
#       "ticker": "005930",
#       "return_3d": 1.23,
#       "return_7d": -2.34,
#       "return_1m": 0.12,
#       "return_ytd": 9.87,
#       "return_1y": 20.1,
#       "return_3y": 55.0
#     }
#   },
#   "updatedAt": "YYYY-MM-DD"
# }

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pykrx import stock

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
THEME_DIR = DATA_DIR / "theme"
CACHE_DIR = DATA_DIR / "cache"
OUT_PATH = CACHE_DIR / "returns_kr.json"

RETURN_KEYS = ["return_3d", "return_7d", "return_1m", "return_ytd", "return_1y", "return_3y"]
HORIZON_TO_TRADING_DAYS = {
    "return_3d": 3,
    "return_7d": 7,
    "return_1m": 21,
    "return_1y": 252,
    "return_3y": 756,
}

# ✅ 최신 영업일 판별용 센티넬 (거래가 거의 항상 있는 대형주)
SENTINEL_TICKERS = ["005930", "000660"]  # 삼성전자, SK하이닉스


# -----------------------------
# helpers
# -----------------------------
def read_json(path: Path) -> Any:
    txt = path.read_text(encoding="utf-8-sig").strip()
    if not txt:
        raise ValueError(f"Empty JSON: {path}")
    return json.loads(txt)


def write_json_atomic(path: Path, obj: Any) -> None:
    """✅ tmp에 먼저 쓰고 replace: 파일이 빈 상태로 남는 사고 방지"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def is_valid_kr_ticker(t: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", (t or "").strip()))


def is_asset_id(s: str) -> bool:
    return bool(re.fullmatch(r"A_\d{3,}", (s or "").strip()))


def to_float_or_none(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def pct_return(last_close: Optional[float], prev_close: Optional[float]) -> Optional[float]:
    if last_close is None or prev_close is None:
        return None
    if prev_close == 0:
        return None
    return (last_close / prev_close - 1.0) * 100.0


# -----------------------------
# theme parsing (KR 자산 수집)
# -----------------------------
def normalize_theme_obj(theme_obj: Dict[str, Any]) -> Dict[str, Any]:
    """nodes 구조 정규화 + ASSET.exposure 정규화(legacy root -> exposure)"""
    nodes = theme_obj.get("nodes")
    if not isinstance(nodes, list):
        data_obj = theme_obj.get("data")
        graph_obj = theme_obj.get("graph")
        if isinstance(data_obj, dict) and isinstance(data_obj.get("nodes"), list):
            nodes = data_obj.get("nodes")
        elif isinstance(graph_obj, dict) and isinstance(graph_obj.get("nodes"), list):
            nodes = graph_obj.get("nodes")
        else:
            nodes = []
    theme_obj["nodes"] = nodes

    # ✅ exposure 정규화: root(ticker/country/exchange) -> exposure로 복사
    for n in nodes:
        if not isinstance(n, dict):
            continue
        if (n.get("type") or "").upper() != "ASSET":
            continue

        exposure = n.get("exposure")
        if not isinstance(exposure, dict):
            exposure = {}
            n["exposure"] = exposure

        # legacy root fields
        if "ticker" in n and not exposure.get("ticker"):
            exposure["ticker"] = str(n.get("ticker") or "").strip()
        if "country" in n and not exposure.get("country"):
            exposure["country"] = str(n.get("country") or "").strip()
        if "exchange" in n and not exposure.get("exchange"):
            exposure["exchange"] = str(n.get("exchange") or "").strip()

        # 기본 키 존재 보장
        exposure.setdefault("ticker", "")
        exposure.setdefault("country", "")
        exposure.setdefault("exchange", "")

    return theme_obj


def collect_kr_assets_from_themes() -> Dict[str, str]:
    """
    return: assetId -> ticker(6자리)
    ✅ 테마에 등장하는 KR 자산만 수집(선별 종목만 수집 원칙)
    """
    out: Dict[str, str] = {}
    if not THEME_DIR.exists():
        return out

    for p in sorted(THEME_DIR.glob("T_*.json")):
        if not p.is_file():
            continue

        try:
            obj = read_json(p)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue

        obj = normalize_theme_obj(obj)

        for n in obj.get("nodes", []):
            if not isinstance(n, dict):
                continue
            if (n.get("type") or "").upper() != "ASSET":
                continue

            asset_id = (n.get("id") or "").strip()
            if not is_asset_id(asset_id):
                continue

            exposure = n.get("exposure") if isinstance(n.get("exposure"), dict) else {}
            country = (exposure.get("country") or "").upper().strip()
            if country != "KR":
                continue

            t = (exposure.get("ticker") or "").strip()
            # ✅ 혹시 5930 같이 들어오면 6자리로 보정
            if t.isdigit() and len(t) <= 6:
                t = t.zfill(6)

            if is_valid_kr_ticker(t):
                out[asset_id] = t

    return out


# -----------------------------
# 최신 영업일 판별 (PYKRX 제공 기준)
# -----------------------------
def detect_latest_trading_day(today: date) -> Optional[date]:
    """
    ✅ '오늘'이 아니라 PYKRX가 실제로 제공하는 최신 거래일을 잡는다.
    - SENTINEL_TICKERS 중 하나라도 데이터가 잡히면 그 마지막 index 날짜를 사용.
    - 최근 10일 범위에서 탐색.
    """
    start = today - timedelta(days=10)
    end = today

    for t in SENTINEL_TICKERS:
        try:
            df = stock.get_market_ohlcv_by_date(yyyymmdd(start), yyyymmdd(end), t)
            if df is None or df.empty:
                continue
            idx = df.index[-1]
            d = idx.date() if hasattr(idx, "date") else None
            if d:
                return d
        except Exception:
            continue

    return None


# -----------------------------
# PYKRX fetch / compute
# -----------------------------
def fetch_close_series(ticker: str, start: date, end: date) -> Optional[List[Tuple[date, float]]]:
    """
    Returns list of (trading_date, close) ascending
    """
    try:
        df = stock.get_market_ohlcv_by_date(yyyymmdd(start), yyyymmdd(end), ticker)
        if df is None or df.empty:
            return None

        out: List[Tuple[date, float]] = []
        for idx, row in df.iterrows():
            d = idx.date() if hasattr(idx, "date") else None
            close = to_float_or_none(row.get("종가"))
            if d is None or close is None:
                continue
            out.append((d, float(close)))

        out.sort(key=lambda x: x[0])
        return out if out else None
    except Exception:
        return None


def compute_returns_for_ticker(ticker: str, as_of: date) -> Tuple[str, Dict[str, Optional[float]]]:
    start = as_of - timedelta(days=1300)  # ~3y trading days buffer
    end = as_of

    closes = fetch_close_series(ticker, start, end)
    if not closes:
        return (as_of.isoformat(), {k: None for k in RETURN_KEYS})

    last_d, last_close = closes[-1]
    close_values = [c for _, c in closes]

    ret: Dict[str, Optional[float]] = {}

    for k, tdays in HORIZON_TO_TRADING_DAYS.items():
        if len(close_values) <= tdays:
            ret[k] = None
            continue
        prev_close = close_values[-(tdays + 1)]
        ret[k] = pct_return(last_close, prev_close)

    # YTD
    y = last_d.year
    ytd_rows = [(d, c) for (d, c) in closes if d.year == y]
    if not ytd_rows:
        ret["return_ytd"] = None
    else:
        first_close = ytd_rows[0][1]
        ret["return_ytd"] = pct_return(last_close, first_close)

    for k in RETURN_KEYS:
        ret.setdefault(k, None)

    return (last_d.isoformat(), ret)


def main() -> None:
    print("=== Update KR Returns Start ===")

    asset_map = collect_kr_assets_from_themes()
    print(f"✅ Collected KR assets from themes: {len(asset_map)} (assetId 기준)")

    today = date.today()

    # ✅ 최신 영업일을 먼저 고정
    latest_td = detect_latest_trading_day(today)
    as_of_date = latest_td or today
    print(f"✅ Effective asOf date (latest trading day): {as_of_date.isoformat()}")

    items: Dict[str, Dict[str, Optional[float]]] = {}
    effective_as_of: Optional[str] = None
    fail_count = 0

    for i, (asset_id, ticker) in enumerate(asset_map.items(), 1):
        as_of_iso, ret = compute_returns_for_ticker(ticker, as_of_date)
        items[asset_id] = {"ticker": ticker, **ret}

        # effective_as_of는 실제 last_d 중 최신으로 유지
        if effective_as_of is None or as_of_iso > effective_as_of:
            effective_as_of = as_of_iso

        # 실패(전부 None) 간단 카운트
        if all(items[asset_id].get(k) is None for k in RETURN_KEYS):
            fail_count += 1

        if i % 50 == 0:
            print(f"  ... computed {i}/{len(asset_map)}")

    payload = {
        "asOf": effective_as_of or as_of_date.isoformat(),
        "source": "PYKRX",
        "items": items,
        "updatedAt": datetime.now().strftime("%Y-%m-%d"),
    }

    write_json_atomic(OUT_PATH, payload)
    print(f"✅ wrote: {OUT_PATH}")
    print(f"✅ items: {len(items)} (fails(all None): {fail_count})")
    print("=== Update KR Returns Completed ===")


if __name__ == "__main__":
    main()