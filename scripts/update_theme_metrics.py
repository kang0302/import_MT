# scripts/update_theme_metrics.py
# DAY57 MULTI-THEME FINAL — MoneyTree Auto Update
#
# Features:
# - Multi-theme automation
# - KR prices: pykrx (free)
# - US prices: FMP stable endpoint (historical-price-eod/light)
# - KR PER / 12M FWD PER: FnGuide (best-effort, keep existing on fail)
# - US 12M FWD PER: FMP analyst estimates (eps) + latest price
#
# Policy:
# - NEVER delete existing values
# - Only overwrite when a new value is successfully computed

import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Tuple, List
from io import StringIO

import pandas as pd
import requests
from pykrx import stock


# =========================
# CONFIG
# =========================

TARGET_THEMES = [
    "T_006",
    "T_009",
]

THEME_DIR = "data/theme"

# =========================
# ASSET SOURCE MAP
# =========================

@dataclass
class AssetSource:
    country: str            # "KR" or "US"
    krx_ticker: Optional[str] = None
    us_symbol: Optional[str] = None


ASSET_SOURCES: Dict[str, AssetSource] = {
    # ---- KR ----
    "A_079": AssetSource(country="KR", krx_ticker="066570"),  # LG전자
    "A_080": AssetSource(country="KR", krx_ticker="011070"),  # LG이노텍
    "A_082": AssetSource(country="KR", krx_ticker="108490"),  # 로보티즈
    "A_083": AssetSource(country="KR", krx_ticker="090360"),  # 로보스타
    "A_084": AssetSource(country="KR", krx_ticker="455900"),  # 엔젤로보틱스
    "A_085": AssetSource(country="KR", krx_ticker="009150"),  # 삼성전기

    # ---- US ----
    "A_086": AssetSource(country="US", us_symbol="SONY"),     # Sony ADR
    "A_087": AssetSource(country="US", us_symbol="TSLA"),     # Tesla
}


# =========================
# UTIL
# =========================

def pick_num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        if math.isfinite(x):
            return x
    except Exception:
        pass
    return None


def normalize_pct(v: float) -> float:
    return v * 100.0 if abs(v) <= 1.5 else v


def pct_return(latest: float, past: float) -> Optional[float]:
    if latest is None or past is None or past == 0:
        return None
    return (latest / past - 1.0) * 100.0


def compute_returns_from_close(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out = {
        "ret3d": None,
        "ret7d": None,
        "ret1m": None,
        "retYtd": None,
        "ret1y": None,
        "ret3y": None,
    }
    if df is None or df.empty:
        return out

    closes = df["Close"].tolist()
    dates = df["Date"].tolist()
    latest = closes[-1]
    latest_date = dates[-1]

    def n_days_ago(n: int) -> Optional[float]:
        if len(closes) <= n:
            return None
        return closes[-1 - n]

    out["ret3d"] = pct_return(latest, n_days_ago(3))
    out["ret7d"] = pct_return(latest, n_days_ago(7))
    out["ret1m"] = pct_return(latest, n_days_ago(21))
    out["ret1y"] = pct_return(latest, n_days_ago(252))
    out["ret3y"] = pct_return(latest, n_days_ago(756))

    y = latest_date.year
    ytd_df = df[df["Date"].dt.year == y]
    if not ytd_df.empty:
        out["retYtd"] = pct_return(latest, ytd_df["Close"].iloc[0])

    for k, v in out.items():
        if v is not None:
            out[k] = round(normalize_pct(v), 4)

    return out


# =========================
# PRICE FETCH
# =========================

def krx_download_daily(ticker: str, start: datetime, end: datetime) -> pd.DataFrame:
    df = stock.get_market_ohlcv_by_date(
        start.strftime("%Y%m%d"),
        end.strftime("%Y%m%d"),
        ticker,
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date", "Close"])

    df = df.reset_index()
    df["Date"] = pd.to_datetime(df.iloc[:, 0], errors="coerce")
    df["Close"] = pd.to_numeric(df["종가"], errors="coerce")
    df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
    return df[["Date", "Close"]]


def fmp_historical_price_eod_light(symbol: str, api_key: str) -> pd.DataFrame:
    url = "https://financialmodelingprep.com/stable/historical-price-eod/light"
    params = {"symbol": symbol, "apikey": api_key}
    headers = {
        "User-Agent": "Mozilla/5.0 (MoneyTreeBot; GitHub Actions)",
        "Accept": "application/json",
    }

    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()

    if not isinstance(data, list) or not data:
        return pd.DataFrame(columns=["Date", "Close"])

    rows = []
    for it in data:
        d = pd.to_datetime(it.get("date"), errors="coerce")
        c = pick_num(it.get("close"))
        if pd.notna(d) and c is not None:
            rows.append((d, c))

    return pd.DataFrame(rows, columns=["Date", "Close"]).sort_values("Date")


# =========================
# PER FETCH
# =========================

def fn_guide_url(krx_ticker: str) -> str:
    return (
        "https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
        f"?pGB=1&gicode=A{krx_ticker}&MenuYn=Y&stkGb=701"
    )


def fetch_per_from_fnguide(krx_ticker: str) -> Tuple[Optional[float], Optional[float]]:
    url = fn_guide_url(krx_ticker)
    r = requests.get(url, timeout=20)
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    nums: List[float] = []

    for df in tables:
        for v in df.astype(str).values.flatten():
            nums += [pick_num(x.replace(",", "")) for x in re.findall(r"\d+\.?\d*", v)]

    nums = [n for n in nums if n and n > 0]
    if not nums:
        return None, None

    trailing = round(nums[0], 2)
    fwd12m = round(nums[1], 2) if len(nums) > 1 else None
    return trailing, fwd12m


def fetch_forward_eps_from_fmp(symbol: str, api_key: str) -> Optional[float]:
    url = "https://financialmodelingprep.com/stable/analyst-estimates"
    params = {
        "symbol": symbol,
        "period": "annual",
        "limit": 5,
        "apikey": api_key,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if not isinstance(data, list):
        return None

    for row in data:
        for k in row:
            if "eps" in k.lower():
                v = pick_num(row.get(k))
                if v and v != 0:
                    return v
    return None


# =========================
# MAIN
# =========================

def process_theme(theme_id: str, fmp_api_key: str):
    path = f"{THEME_DIR}/{theme_id}.json"
    with open(path, "r", encoding="utf-8") as f:
        theme = json.load(f)

    nodes = theme.get("nodes", [])
    now = datetime.utcnow()
    start = now - timedelta(days=365 * 5)

    for n in nodes:
        if n.get("type") != "ASSET":
            continue

        asset_id = n.get("id")
        src = ASSET_SOURCES.get(asset_id)
        if not src:
            continue

        metrics = n.get("metrics", {})
        df = None
        latest_price = None

        try:
            if src.country == "KR":
                df = krx_download_daily(src.krx_ticker, start, now)
            elif src.country == "US" and fmp_api_key:
                df = fmp_historical_price_eod_light(src.us_symbol, fmp_api_key)
        except Exception as e:
            print(f"[WARN] price fetch failed for {asset_id}: {e}")

        if df is not None and not df.empty:
            latest_price = pick_num(df["Close"].iloc[-1])
            for k, v in compute_returns_from_close(df).items():
                if v is not None:
                    metrics[k] = v

        try:
            if src.country == "KR":
                per, fwd = fetch_per_from_fnguide(src.krx_ticker)
                if per:
                    metrics["per"] = per
                if fwd:
                    metrics["perFwd12m"] = fwd
        except Exception as e:
            print(f"[WARN] FnGuide PER failed for {asset_id}: {e}")

        if src.country == "US" and fmp_api_key and latest_price:
            try:
                eps = fetch_forward_eps_from_fmp(src.us_symbol, fmp_api_key)
                if eps:
                    metrics["perFwd12m"] = round(latest_price / eps, 2)
            except Exception as e:
                print(f"[WARN] US forward PER failed for {asset_id}: {e}")

        n["metrics"] = metrics

    with open(path, "w", encoding="utf-8") as f:
        json.dump(theme, f, ensure_ascii=False, indent=2)

    print(f"[OK] Updated: {path}")


def main():
    fmp_api_key = os.getenv("FMP_API_KEY", "").strip()
    for theme_id in TARGET_THEMES:
        process_theme(theme_id, fmp_api_key)


if __name__ == "__main__":
    main()
