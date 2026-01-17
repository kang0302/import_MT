# scripts/update_theme_metrics.py
# DAY 58-2 FINAL — MoneyTree Multi-Theme Auto Update Engine
#
# Core principles
# - Source of truth: data/theme/index.json
# - NEVER delete existing values
# - Update metrics ONLY when a new value is successfully computed
# - One theme failure must NOT break the whole run
#
# Data sources
# - KR price: pykrx
# - US price: Financial Modeling Prep (FMP)
# - KR PER / 12M FWD PER: FnGuide (best-effort)
# - US 12M FWD PER: FMP analyst estimates
#
# Environment
# - Requires: FMP_API_KEY (for US assets)
#
# This file is COPY-PASTE SAFE.

import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Tuple, List

import pandas as pd
import requests
from pykrx import stock
from io import StringIO

# --------------------------------------------------
# Paths
# --------------------------------------------------

THEME_INDEX_PATH = "data/theme/index.json"
THEME_DIR = "data/theme"

# --------------------------------------------------
# Asset source mapping (GLOBAL)
# - Add / modify here only
# --------------------------------------------------

@dataclass
class AssetSource:
    country: str              # "KR" or "US"
    krx_ticker: Optional[str] = None
    us_symbol: Optional[str] = None


ASSET_SOURCES: Dict[str, AssetSource] = {
    # T_006 / T_009 예시
    "A_079": AssetSource(country="KR", krx_ticker="066570"),  # LG전자
    "A_080": AssetSource(country="KR", krx_ticker="011070"),  # LG이노텍
    "A_082": AssetSource(country="KR", krx_ticker="108490"),  # 로보티즈
    "A_083": AssetSource(country="KR", krx_ticker="090360"),  # 로보스타
    "A_084": AssetSource(country="KR", krx_ticker="455900"),  # 엔젤로보틱스
    "A_085": AssetSource(country="KR", krx_ticker="009150"),  # 삼성전기
    "A_086": AssetSource(country="US", us_symbol="SONY"),     # Sony ADR
    "A_087": AssetSource(country="US", us_symbol="TSLA"),     # Tesla
}

# --------------------------------------------------
# Utility helpers
# --------------------------------------------------

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
    if latest is None or past in (None, 0):
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

    def ago(n: int) -> Optional[float]:
        if len(closes) <= n:
            return None
        return closes[-1 - n]

    out["ret3d"] = pct_return(latest, ago(3))
    out["ret7d"] = pct_return(latest, ago(7))
    out["ret1m"] = pct_return(latest, ago(21))
    out["ret1y"] = pct_return(latest, ago(252))
    out["ret3y"] = pct_return(latest, ago(756))

    y = latest_date.year
    ytd_df = df[df["Date"].dt.year == y]
    if not ytd_df.empty:
        out["retYtd"] = pct_return(latest, ytd_df["Close"].iloc[0])

    for k, v in out.items():
        if v is not None:
            out[k] = round(normalize_pct(v), 4)

    return out

# --------------------------------------------------
# Price sources
# --------------------------------------------------

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
    close_col = "종가" if "종가" in df.columns else None
    if close_col is None:
        return pd.DataFrame(columns=["Date", "Close"])

    df["Close"] = pd.to_numeric(df[close_col], errors="coerce")
    return df[["Date", "Close"]].dropna().sort_values("Date")


def fmp_historical_price_full(symbol: str, api_key: str) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
    params = {"serietype": "line", "apikey": api_key}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    hist = r.json().get("historical", [])
    rows = []
    for it in hist:
        d = pd.to_datetime(it.get("date"), errors="coerce")
        c = pick_num(it.get("close"))
        if pd.notna(d) and c is not None:
            rows.append((d, c))

    return pd.DataFrame(rows, columns=["Date", "Close"]).sort_values("Date")

# --------------------------------------------------
# PER sources
# --------------------------------------------------

def fn_guide_url(krx_ticker: str) -> str:
    return (
        "https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
        f"?pGB=1&gicode=A{krx_ticker}&MenuYn=Y&stkGb=701"
    )


def fetch_per_from_fnguide(krx_ticker: str) -> Tuple[Optional[float], Optional[float]]:
    url = fn_guide_url(krx_ticker)
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    trailing = None
    fwd12m = None

    for df in tables:
        for col in df.columns:
            if "PER" in str(col):
                nums = pd.to_numeric(df[col], errors="coerce").dropna()
                if not nums.empty:
                    trailing = float(nums.iloc[0])
        for _, row in df.iterrows():
            row_str = " ".join(map(str, row))
            if "12" in row_str and "PER" in row_str:
                nums = re.findall(r"\d+\.?\d*", row_str)
                if nums:
                    fwd12m = float(nums[0])

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

    for item in data:
        for k in item:
            if "eps" in k.lower():
                v = pick_num(item.get(k))
                if v:
                    return v
    return None

# --------------------------------------------------
# Theme processing
# --------------------------------------------------

def load_theme_index() -> List[Dict[str, str]]:
    with open(THEME_INDEX_PATH, "r", encoding="utf-8") as f:
        return json.load(f).get("themes", [])


def load_theme_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_theme_json(path: str, data: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def update_single_theme(theme_path: str):
    fmp_api_key = os.getenv("FMP_API_KEY", "").strip()
    theme = load_theme_json(theme_path)

    now = datetime.utcnow()
    start = now - timedelta(days=365 * 5)
    end = now + timedelta(days=2)

    for node in theme.get("nodes", []):
        if node.get("type") != "ASSET":
            continue

        asset_id = node.get("id")
        src = ASSET_SOURCES.get(asset_id)
        if not src:
            continue

        metrics = node.get("metrics") or {}

        df = None
        latest_price = None

        try:
            if src.country == "KR" and src.krx_ticker:
                df = krx_download_daily(src.krx_ticker, start, end)
            elif src.country == "US" and src.us_symbol and fmp_api_key:
                df = fmp_historical_price_full(src.us_symbol, fmp_api_key)
        except Exception as e:
            print(f"[WARN] price fetch failed {asset_id}: {e}")

        if df is not None and not df.empty:
            latest_price = pick_num(df["Close"].iloc[-1])
            returns = compute_returns_from_close(df)
            for k, v in returns.items():
                if v is not None:
                    metrics[k] = v

        try:
            if src.country == "KR" and src.krx_ticker:
                per, fwd = fetch_per_from_fnguide(src.krx_ticker)
                if per:
                    metrics["per"] = round(per, 2)
                if fwd:
                    metrics["perFwd12m"] = round(fwd, 2)
        except Exception as e:
            print(f"[WARN] FnGuide PER failed {asset_id}: {e}")

        try:
            if src.country == "US" and latest_price and fmp_api_key:
                eps = fetch_forward_eps_from_fmp(src.us_symbol, fmp_api_key)
                if eps:
                    metrics["perFwd12m"] = round(latest_price / eps, 2)
        except Exception as e:
            print(f"[WARN] US PER failed {asset_id}: {e}")

        node["metrics"] = metrics

    save_theme_json(theme_path, theme)

# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    themes = load_theme_index()
    for t in themes:
        theme_id = t.get("themeId")
        if not theme_id:
            continue

        theme_path = f"{THEME_DIR}/{theme_id}.json"
        try:
            update_single_theme(theme_path)
            print(f"[OK] Updated: {theme_id}")
        except Exception as e:
            print(f"[WARN] Theme failed {theme_id}: {e}")


if __name__ == "__main__":
    main()

