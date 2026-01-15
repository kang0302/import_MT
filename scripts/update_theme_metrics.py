# scripts/update_theme_metrics.py
# DAY56-1 HOTFIX FINAL — MoneyTree Auto Update (T_009)
# FIX: Remove Stooq (404 issue). US price history from FMP instead.
#
# - KR prices: KRX via pykrx (free)
# - US prices: FMP historical-price-full (needs FMP_API_KEY)
# - KR PER/12M FWD PER: FnGuide (best-effort scrape; if fail -> keep existing)
# - US 12M FWD PER: FMP analyst estimates (eps) + latest price => forward P/E
#
# Policy:
# - NEVER delete existing manual/test values.
# - Only overwrite metrics when a new value is successfully computed.

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


THEME_PATH = "data/theme/T_009.json"


@dataclass
class AssetSource:
    country: str  # "US" or "KR"
    krx_ticker: Optional[str] = None   # KR: "066570"
    us_symbol: Optional[str] = None    # US: "TSLA", "SONY"


# ✅ T_009 자산 소스 매핑 (필요하면 여기만 추가/수정)
ASSET_SOURCES: Dict[str, AssetSource] = {
    "A_079": AssetSource(country="KR", krx_ticker="066570"),  # LG전자
    "A_080": AssetSource(country="KR", krx_ticker="011070"),  # LG이노텍
    "A_082": AssetSource(country="KR", krx_ticker="108490"),  # 로보티즈
    "A_083": AssetSource(country="KR", krx_ticker="090360"),  # 로보스타
    "A_084": AssetSource(country="KR", krx_ticker="455900"),  # 엔젤로보틱스
    "A_085": AssetSource(country="KR", krx_ticker="009150"),  # 삼성전기
    "A_086": AssetSource(country="US", us_symbol="SONY"),     # Sony ADR
    "A_087": AssetSource(country="US", us_symbol="TSLA"),     # Tesla
    # A_081(베어로보틱스): 비상장/티커 없음 → 소스 미지정(기존값 유지)
}


# -------------------------
# Helpers
# -------------------------

def pick_num(v: Any) -> Optional[float]:
    try:
        x = float(v)
        if math.isfinite(x):
            return x
    except Exception:
        pass
    return None


def normalize_pct(v: float) -> float:
    # 0.12(=12%) 같은 값이 들어오면 100배
    return v * 100.0 if abs(v) <= 1.5 else v


def pct_return(latest: float, past: float) -> Optional[float]:
    if latest is None or past is None:
        return None
    if past == 0:
        return None
    return (latest / past - 1.0) * 100.0


def compute_returns_from_close(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    Compute returns:
      ret3d, ret7d, ret1m(~21td), retYtd, ret1y(~252td), ret3y(~756td)
    """
    out = {"ret3d": None, "ret7d": None, "ret1m": None, "retYtd": None, "ret1y": None, "ret3y": None}
    if df is None or df.empty:
        return out

    closes = df["Close"].tolist()
    dates = df["Date"].tolist()
    latest = closes[-1]
    latest_date = dates[-1]

    def n_trading_days_ago(n: int) -> Optional[float]:
        if len(closes) <= n:
            return None
        return closes[-1 - n]

    out["ret3d"] = pct_return(latest, n_trading_days_ago(3))
    out["ret7d"] = pct_return(latest, n_trading_days_ago(7))
    out["ret1m"] = pct_return(latest, n_trading_days_ago(21))
    out["ret1y"] = pct_return(latest, n_trading_days_ago(252))
    out["ret3y"] = pct_return(latest, n_trading_days_ago(756))

    # YTD: first trading day in same calendar year
    y = latest_date.year
    ytd_df = df[df["Date"].dt.year == y]
    if not ytd_df.empty:
        first_close = ytd_df["Close"].iloc[0]
        out["retYtd"] = pct_return(latest, first_close)

    # normalize + round
    for k, v in list(out.items()):
        if v is not None:
            out[k] = float(round(normalize_pct(v), 4))
    return out


# -------------------------
# KR price (pykrx)
# -------------------------

def krx_download_daily(ticker: str, start: datetime, end: datetime) -> pd.DataFrame:
    s = start.strftime("%Y%m%d")
    e = end.strftime("%Y%m%d")
    df = stock.get_market_ohlcv_by_date(s, e, ticker)
    if df is None or df.empty:
        return pd.DataFrame(columns=["Date", "Close"])

    df = df.reset_index()
    if "날짜" in df.columns:
        df["Date"] = pd.to_datetime(df["날짜"], errors="coerce")
    else:
        df["Date"] = pd.to_datetime(df.iloc[:, 0], errors="coerce")

    close_col = "종가" if "종가" in df.columns else ("Close" if "Close" in df.columns else None)
    if close_col is None:
        return pd.DataFrame(columns=["Date", "Close"])

    df["Close"] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
    return df[["Date", "Close"]]


# -------------------------
# US price (FMP) — HOTFIX
# -------------------------

def fmp_historical_price_full(symbol: str, api_key: str) -> pd.DataFrame:
    """
    Fetch daily historical prices from FMP.
    Endpoint:
      https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?serietype=line&apikey=...
    Returns DataFrame with Date, Close sorted ascending.
    """
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
    params = {"serietype": "line", "apikey": api_key}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    hist = data.get("historical")
    if not isinstance(hist, list) or not hist:
        return pd.DataFrame(columns=["Date", "Close"])

    # historical item: {"date":"2025-01-02","close":123.45,...}
    rows = []
    for it in hist:
        if not isinstance(it, dict):
            continue
        d = it.get("date")
        c = it.get("close")
        dv = pd.to_datetime(d, errors="coerce")
        cv = pick_num(c)
        if pd.notna(dv) and cv is not None:
            rows.append((dv, cv))

    df = pd.DataFrame(rows, columns=["Date", "Close"]).sort_values("Date")
    return df


# -------------------------
# PER sources
# -------------------------

def fn_guide_url(krx_ticker: str) -> str:
    gicode = f"A{krx_ticker}"
    return (
        "https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
        f"?pGB=1&gicode={gicode}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
    )


def _extract_numbers_from_text(s: str) -> List[float]:
    nums = re.findall(r"-?\d[\d,]*\.?\d*", s)
    out: List[float] = []
    for x in nums:
        x = x.replace(",", "")
        v = pick_num(x)
        if v is not None:
            out.append(v)
    return out


def _best_effort_find_per_values_from_tables(tables: List[pd.DataFrame]) -> Tuple[Optional[float], Optional[float]]:
    trailing = None
    fwd12m = None

    for df in tables:
        try:
            df2 = df.copy().fillna("")
            grid = df2.astype(str).values.tolist()
        except Exception:
            continue

        # find 12M PER
        if fwd12m is None:
            for r, row in enumerate(grid):
                for c, cell in enumerate(row):
                    txt = (cell or "").strip()
                    if re.search(r"12\s*M\s*PER|12M\s*PER|12개월\s*PER", txt, re.IGNORECASE):
                        cand = None
                        if c + 1 < len(row):
                            ns = _extract_numbers_from_text(row[c + 1])
                            cand = ns[0] if ns else None
                        if cand is None and r + 1 < len(grid):
                            ns = _extract_numbers_from_text(grid[r + 1][c])
                            cand = ns[0] if ns else None
                        if cand is None:
                            ns = []
                            for x in row:
                                ns += _extract_numbers_from_text(str(x))
                            cand = ns[0] if ns else None
                        if cand is not None and cand > 0:
                            fwd12m = float(cand)
                            break
                if fwd12m is not None:
                    break

        # find trailing PER
        if trailing is None:
            for r, row in enumerate(grid):
                row_join = " | ".join([str(x) for x in row])
                if re.search(r"\bPER\b|PER\(", row_join, re.IGNORECASE):
                    if re.search(r"12\s*M|12M|12개월", row_join, re.IGNORECASE):
                        continue
                    nums = []
                    for x in row:
                        nums += _extract_numbers_from_text(str(x))
                    nums = [n for n in nums if 0 < n < 5000]
                    if nums:
                        trailing = float(nums[0])
                        break

        if trailing is not None and fwd12m is not None:
            break

    if trailing is not None and trailing <= 0:
        trailing = None
    if fwd12m is not None and fwd12m <= 0:
        fwd12m = None

    return trailing, fwd12m


def fetch_per_from_fnguide(krx_ticker: str) -> Tuple[Optional[float], Optional[float]]:
    """
    FnGuide best-effort scrape using pandas.read_html.
    Requires lxml installed in workflow.
    """
    url = fn_guide_url(krx_ticker)
    headers = {
        "User-Agent": "Mozilla/5.0 (MoneyTreeBot; +https://github.com/)",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    trailing, fwd12m = _best_effort_find_per_values_from_tables(tables)

    trailing = float(round(trailing, 2)) if trailing is not None else None
    fwd12m = float(round(fwd12m, 2)) if fwd12m is not None else None
    return trailing, fwd12m


def fetch_forward_eps_from_fmp(symbol: str, api_key: str) -> Optional[float]:
    """
    FMP stable endpoint:
      https://financialmodelingprep.com/stable/analyst-estimates?symbol=TSLA&period=annual&page=0&limit=10&apikey=...
    We pick the first available eps estimate key (best-effort).
    """
    url = "https://financialmodelingprep.com/stable/analyst-estimates"
    params = {"symbol": symbol, "period": "annual", "page": 0, "limit": 10, "apikey": api_key}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        return None

    key_candidates = [
        "epsEstimated",
        "epsEstimatedAvg",
        "estimatedEpsAvg",
        "epsAvg",
        "eps",
        "estimatedEPS",
    ]

    for item in data:
        if not isinstance(item, dict):
            continue
        for k in key_candidates:
            v = pick_num(item.get(k))
            if v is not None and v != 0:
                return float(v)

    for item in data:
        if not isinstance(item, dict):
            continue
        for k, val in item.items():
            kk = str(k).lower()
            if "eps" in kk and ("estim" in kk or "estimate" in kk or "forecast" in kk):
                v = pick_num(val)
                if v is not None and v != 0:
                    return float(v)

    return None


# -------------------------
# JSON I/O
# -------------------------

def load_theme_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_theme_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    fmp_api_key = os.getenv("FMP_API_KEY", "").strip()

    theme = load_theme_json(THEME_PATH)
    nodes = theme.get("nodes", [])

    now = datetime.utcnow()
    start = now - timedelta(days=365 * 5)
    end = now + timedelta(days=2)

    for n in nodes:
        if n.get("type") != "ASSET":
            continue

        asset_id = n.get("id")
        src = ASSET_SOURCES.get(asset_id)
        if not src:
            continue  # 소스 미정이면 기존값 유지

        metrics = n.get("metrics") or {}

        # -------------------------
        # 1) Price → Returns
        # -------------------------
        df = None
        latest_price = None

        try:
            if src.country == "KR" and src.krx_ticker:
                df = krx_download_daily(src.krx_ticker, start, end)

            elif src.country == "US" and src.us_symbol:
                if not fmp_api_key:
                    print("[INFO] FMP_API_KEY missing. US prices will not update.")
                    df = None
                else:
                    df = fmp_historical_price_full(src.us_symbol, fmp_api_key)

        except Exception as e:
            print(f"[WARN] price fetch failed for {asset_id}: {e}")
            df = None

        if df is not None and not df.empty:
            latest_price = pick_num(df["Close"].iloc[-1])

            returns = compute_returns_from_close(df)
            # ✅ 덮어쓰기: 계산된 값만 갱신 / None이면 기존 유지
            for k, v in returns.items():
                if v is not None:
                    metrics[k] = v

        # -------------------------
        # 2) PER update
        # -------------------------
        # KR: FnGuide (best-effort)
        if src.country == "KR" and src.krx_ticker:
            try:
                trailing_per, fwd12m_per = fetch_per_from_fnguide(src.krx_ticker)
                if trailing_per is not None:
                    metrics["per"] = trailing_per
                if fwd12m_per is not None:
                    metrics["perFwd12m"] = fwd12m_per
            except Exception as e:
                print(f"[WARN] FnGuide PER fetch failed for {asset_id}: {e}")

        # US: Forward PER from FMP (needs latest_price + eps_fwd)
        if src.country == "US" and src.us_symbol:
            if fmp_api_key and latest_price is not None:
                try:
                    eps_fwd = fetch_forward_eps_from_fmp(src.us_symbol, fmp_api_key)
                    if eps_fwd is not None and eps_fwd != 0:
                        fwd_per = float(round(latest_price / eps_fwd, 2))
                        if fwd_per > 0:
                            metrics["perFwd12m"] = fwd_per
                except Exception as e:
                    print(f"[WARN] FMP forward EPS fetch failed for {asset_id}: {e}")
            else:
                if not fmp_api_key:
                    print("[INFO] FMP_API_KEY missing. US forward PER will not update.")
                if latest_price is None:
                    print(f"[INFO] US latest price missing for {asset_id}. US forward PER will not update.")

        n["metrics"] = metrics

    save_theme_json(THEME_PATH, theme)
    print("[OK] Updated:", THEME_PATH)


if __name__ == "__main__":
    main()
