# scripts/update_theme_metrics.py
# DAY56 (MoneyTree) - update returns into data/theme/T_009.json
# - US: Stooq (free)
# - KR: KRX via pykrx (free)
# - If a value can't be computed, keep the existing manual/test value (no deletion)

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import pandas as pd
import requests
from pykrx import stock


THEME_PATH = "data/theme/T_009.json"


@dataclass
class AssetSource:
    country: str  # "US" or "KR"
    # for US (stooq): symbol like "tsla.us", "sony.us"
    stooq_symbol: Optional[str] = None
    # for KR (pykrx): ticker like "066570"
    krx_ticker: Optional[str] = None


# ✅ T_009 자산 매핑 (필요시 여기만 추가/수정)
ASSET_SOURCES: Dict[str, AssetSource] = {
    "A_079": AssetSource(country="KR", krx_ticker="066570"),  # LG전자
    "A_080": AssetSource(country="KR", krx_ticker="011070"),  # LG이노텍
    "A_082": AssetSource(country="KR", krx_ticker="108490"),  # 로보티즈
    "A_083": AssetSource(country="KR", krx_ticker="090360"),  # 로보스타
    "A_084": AssetSource(country="KR", krx_ticker="455900"),  # 엔젤로보틱스
    "A_085": AssetSource(country="KR", krx_ticker="009150"),  # 삼성전기
    "A_086": AssetSource(country="US", stooq_symbol="sony.us"), # Sony ADR (NYSE: SONY)
    "A_087": AssetSource(country="US", stooq_symbol="tsla.us"), # Tesla (NASDAQ: TSLA)
    # A_081(베어로보틱스): 비상장/티커 없음 → 소스 미지정(기존값 유지)
}


def pct_return(latest: float, past: float) -> Optional[float]:
    if past is None or latest is None:
        return None
    if past == 0:
        return None
    return (latest / past - 1.0) * 100.0


def safe_float(x) -> Optional[float]:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def stooq_download_daily(symbol: str) -> pd.DataFrame:
    """
    Stooq CSV download format (works for e.g. tsla.us / sony.us).
    """
    url = f"https://stooq.com/q/q/l/?e=csv&f=sd2t2ohlcv&h=&s={symbol}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(pd.compat.StringIO(r.text))
    # expected columns: Date, Time, Open, High, Low, Close, Volume
    if "Date" not in df.columns or "Close" not in df.columns:
        raise ValueError(f"Unexpected stooq schema for {symbol}: {df.columns}")
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date")
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df = df.dropna(subset=["Close"])
    return df[["Date", "Close"]]


def krx_download_daily(ticker: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    KRX daily OHLCV via pykrx.
    """
    s = start.strftime("%Y%m%d")
    e = end.strftime("%Y%m%d")
    df = stock.get_market_ohlcv_by_date(s, e, ticker)
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date", "Close"])
    df = df.reset_index()
    # columns usually include: 날짜, 시가, 고가, 저가, 종가, 거래량 ...
    # After reset_index, "날짜" may be the date column name
    if "날짜" in df.columns:
        df["Date"] = pd.to_datetime(df["날짜"])
    else:
        # fallback: first column
        df["Date"] = pd.to_datetime(df.iloc[:, 0])
    # 종가 column
    close_col = "종가" if "종가" in df.columns else None
    if close_col is None:
        # fallback: try "Close"
        close_col = "Close" if "Close" in df.columns else None
    if close_col is None:
        return pd.DataFrame(columns=["Date", "Close"])

    df["Close"] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.dropna(subset=["Close"]).sort_values("Date")
    return df[["Date", "Close"]]


def compute_returns_from_close(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    Compute: 3D, 7D, 1M(=~21 trading days), YTD, 1Y(~252), 3Y(~756)
    """
    out = {"ret3d": None, "ret7d": None, "ret1m": None, "retYtd": None, "ret1y": None, "ret3y": None}
    if df is None or df.empty:
        return out

    closes = df["Close"].tolist()
    dates = df["Date"].tolist()
    latest = closes[-1]
    latest_date = dates[-1]

    def pick_n_trading_days_ago(n: int) -> Optional[float]:
        if len(closes) <= n:
            return None
        return closes[-1 - n]

    # 3D, 7D: trading days (3, 7)
    out["ret3d"] = pct_return(latest, pick_n_trading_days_ago(3))
    out["ret7d"] = pct_return(latest, pick_n_trading_days_ago(7))

    # 1M: ~21 trading days
    out["ret1m"] = pct_return(latest, pick_n_trading_days_ago(21))

    # 1Y: ~252 trading days
    out["ret1y"] = pct_return(latest, pick_n_trading_days_ago(252))

    # 3Y: ~756 trading days
    out["ret3y"] = pct_return(latest, pick_n_trading_days_ago(756))

    # YTD: first trading day in the same calendar year
    y = latest_date.year
    ytd_df = df[df["Date"].dt.year == y]
    if not ytd_df.empty:
        first_close = ytd_df["Close"].iloc[0]
        out["retYtd"] = pct_return(latest, first_close)

    # round for clean JSON
    for k, v in out.items():
        if v is not None:
            out[k] = float(round(v, 4))
    return out


def load_theme_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_theme_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    theme = load_theme_json(THEME_PATH)
    nodes = theme.get("nodes", [])
    now = datetime.utcnow()
    # 충분히 긴 기간 확보 (3Y 계산 위해 여유)
    start = now - timedelta(days=365 * 5)
    end = now + timedelta(days=2)

    for n in nodes:
        if n.get("type") != "ASSET":
            continue
        asset_id = n.get("id")
        src = ASSET_SOURCES.get(asset_id)
        if not src:
            # 소스 없는 자산은 "기존값 유지"
            continue

        df = None
        try:
            if src.country == "US" and src.stooq_symbol:
                df = stooq_download_daily(src.stooq_symbol)
            elif src.country == "KR" and src.krx_ticker:
                df = krx_download_daily(src.krx_ticker, start, end)
        except Exception as e:
            print(f"[WARN] fetch failed for {asset_id}: {e}")
            df = None

        returns = compute_returns_from_close(df) if df is not None else {}
        if not returns:
            continue

        metrics = n.get("metrics") or {}
        # ✅ 덮어쓰기 정책:
        # - 계산된 값이 있으면 갱신
        # - 계산 불가(None)는 기존값 유지(테스트값 삭제/초기화 금지)
        for k, v in returns.items():
            if v is not None:
                metrics[k] = v

        n["metrics"] = metrics

    save_theme_json(THEME_PATH, theme)
    print("[OK] Updated:", THEME_PATH)


if __name__ == "__main__":
    main()
