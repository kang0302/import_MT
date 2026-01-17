# scripts/update_theme_metrics.py
# DAY58-1 FINAL — MoneyTree Auto Update (Multi-Theme) + Blocked-Friendly Ops
#
# Goals:
# - Multi-theme update (TARGET_THEMES)
# - KR prices: pykrx (free)
# - US prices: Primary=FMP (stable), Backup=Alpha Vantage (TIME_SERIES_DAILY_ADJUSTED)
# - PER:
#   - KR trailing PER & 12M FWD PER: FnGuide best-effort (fail => keep existing)
#   - US 12M FWD PER: FMP analyst estimates (eps) + latest price (needs price)
# - Policy:
#   - NEVER delete existing manual/test values
#   - Only overwrite metrics when new values are successfully computed
# - DataHealth:
#   - Save update status per theme + per asset (OK/PARTIAL/FAIL)
#   - Save updatedAt + source + lastError (short)
#
# Env needed:
#   - FMP_API_KEY (recommended)
#   - ALPHAVANTAGE_API_KEY (backup)
#
# Notes:
# - Alpha Vantage free tier is rate-limited; we only call it when FMP fails.
# - If US sources are blocked, service still runs because JSON cache remains.

import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from typing import Dict, Optional, Any, Tuple, List

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

# US history lookback window (enough to compute 3Y with trading days)
US_LOOKBACK_DAYS = 365 * 5

# Alpha Vantage polite delay when used (helps avoid rate-limit)
ALPHAVANTAGE_MIN_DELAY_SEC = 13  # free tier: ~5 calls/min; keep buffer


# =========================
# ASSET SOURCE MAP
# =========================

@dataclass
class AssetSource:
    country: str  # "KR" or "US"
    krx_ticker: Optional[str] = None
    us_symbol: Optional[str] = None


# ✅ 여기만 추가/수정하면 됨
ASSET_SOURCES: Dict[str, AssetSource] = {
    # ---- KR (T_009) ----
    "A_079": AssetSource(country="KR", krx_ticker="066570"),  # LG전자
    "A_080": AssetSource(country="KR", krx_ticker="011070"),  # LG이노텍
    "A_082": AssetSource(country="KR", krx_ticker="108490"),  # 로보티즈
    "A_083": AssetSource(country="KR", krx_ticker="090360"),  # 로보스타
    "A_084": AssetSource(country="KR", krx_ticker="455900"),  # 엔젤로보틱스
    "A_085": AssetSource(country="KR", krx_ticker="009150"),  # 삼성전기

    # ---- US (T_009) ----
    "A_086": AssetSource(country="US", us_symbol="SONY"),     # Sony ADR (주의: 티커/거래소 이슈 가능)
    "A_087": AssetSource(country="US", us_symbol="TSLA"),     # Tesla
}


# =========================
# UTIL
# =========================

def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def short_err(e: Exception, max_len: int = 180) -> str:
    s = str(e).replace("\n", " ").strip()
    return s[:max_len]


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
    Returns:
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


# =========================
# KR PRICE (pykrx)
# =========================

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

    close_col = "종가" if "종가" in df.columns else None
    if close_col is None:
        return pd.DataFrame(columns=["Date", "Close"])

    df["Close"] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
    return df[["Date", "Close"]]


# =========================
# US PRICE (Primary: FMP)
# =========================

def fmp_historical_price_eod_light(symbol: str, api_key: str) -> pd.DataFrame:
    """
    FMP stable endpoint (recommended):
      https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=TSLA&apikey=...
    Returns list[{"date":"YYYY-MM-DD","close":...}, ...]
    """
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
        if not isinstance(it, dict):
            continue
        d = pd.to_datetime(it.get("date"), errors="coerce")
        c = pick_num(it.get("close"))
        if pd.notna(d) and c is not None:
            rows.append((d, c))

    df = pd.DataFrame(rows, columns=["Date", "Close"]).sort_values("Date")
    # keep last US_LOOKBACK_DAYS
    cutoff = datetime.utcnow() - timedelta(days=US_LOOKBACK_DAYS)
    df = df[df["Date"] >= pd.Timestamp(cutoff)]
    return df


# =========================
# US PRICE (Backup: Alpha Vantage)
# =========================

_LAST_AV_CALL_TS = 0.0

def _alpha_vantage_polite_delay():
    global _LAST_AV_CALL_TS
    now = time.time()
    gap = now - _LAST_AV_CALL_TS
    if gap < ALPHAVANTAGE_MIN_DELAY_SEC:
        time.sleep(ALPHAVANTAGE_MIN_DELAY_SEC - gap)
    _LAST_AV_CALL_TS = time.time()

def alphavantage_daily_adjusted(symbol: str, api_key: str) -> pd.DataFrame:
    """
    Alpha Vantage:
      https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=TSLA&outputsize=full&apikey=...
    Returns JSON with "Time Series (Daily)" dict.
    """
    _alpha_vantage_polite_delay()

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": "full",
        "apikey": api_key,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (MoneyTreeBot; GitHub Actions)",
        "Accept": "application/json",
    }
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()

    if not isinstance(data, dict):
        return pd.DataFrame(columns=["Date", "Close"])

    # rate limit / errors
    if "Note" in data:
        raise RuntimeError(f"AlphaVantage rate limit: {data.get('Note')}")
    if "Error Message" in data:
        raise RuntimeError(f"AlphaVantage error: {data.get('Error Message')}")

    ts = data.get("Time Series (Daily)")
    if not isinstance(ts, dict) or not ts:
        return pd.DataFrame(columns=["Date", "Close"])

    rows = []
    for dstr, row in ts.items():
        d = pd.to_datetime(dstr, errors="coerce")
        if pd.isna(d):
            continue
        # adjusted close preferred, else close
        c = pick_num(row.get("5. adjusted close"))
        if c is None:
            c = pick_num(row.get("4. close"))
        if c is None:
            continue
        rows.append((d, c))

    df = pd.DataFrame(rows, columns=["Date", "Close"]).sort_values("Date")
    cutoff = datetime.utcnow() - timedelta(days=US_LOOKBACK_DAYS)
    df = df[df["Date"] >= pd.Timestamp(cutoff)]
    return df


# =========================
# PER SOURCES
# =========================

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

        # 12M FWD PER
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
                        if cand is not None and cand > 0:
                            fwd12m = float(cand)
                            break
                if fwd12m is not None:
                    break

        # trailing PER
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
    FnGuide best-effort scrape using pandas.read_html
    - Uses StringIO to avoid FutureWarning
    - Can fail due to blocks; we treat as optional (keep existing).
    """
    url = fn_guide_url(krx_ticker)
    headers = {
        "User-Agent": "Mozilla/5.0 (MoneyTreeBot; GitHub Actions)",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()

    tables = pd.read_html(StringIO(r.text))
    trailing, fwd12m = _best_effort_find_per_values_from_tables(tables)

    trailing = float(round(trailing, 2)) if trailing is not None else None
    fwd12m = float(round(fwd12m, 2)) if fwd12m is not None else None
    return trailing, fwd12m

def fetch_forward_eps_from_fmp(symbol: str, api_key: str) -> Optional[float]:
    """
    FMP stable endpoint:
      https://financialmodelingprep.com/stable/analyst-estimates?symbol=TSLA&period=annual&limit=10&apikey=...
    We pick a usable eps estimate key (best-effort).
    """
    url = "https://financialmodelingprep.com/stable/analyst-estimates"
    params = {"symbol": symbol, "period": "annual", "page": 0, "limit": 10, "apikey": api_key}
    headers = {
        "User-Agent": "Mozilla/5.0 (MoneyTreeBot; GitHub Actions)",
        "Accept": "application/json",
    }
    r = requests.get(url, params=params, headers=headers, timeout=30)
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


# =========================
# JSON I/O
# =========================

def load_theme_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_theme_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def ensure_meta(theme: dict) -> dict:
    if not isinstance(theme.get("meta"), dict):
        theme["meta"] = {}
    return theme["meta"]


# =========================
# CORE: update one theme
# =========================

def update_one_theme(theme_id: str, fmp_api_key: str, av_api_key: str) -> Tuple[bool, str]:
    """
    Returns (success, message)
    success means: file saved without crashing.
    It can still be PARTIAL (some assets not updated).
    """
    theme_path = f"{THEME_DIR}/{theme_id}.json"
    theme = load_theme_json(theme_path)

    meta = ensure_meta(theme)
    meta["metricsUpdatedAtUTC"] = utc_now_iso()
    meta["metricsStatus"] = "OK"   # will downgrade to PARTIAL/FAIL
    meta["metricsLastError"] = ""

    nodes = theme.get("nodes", [])
    now = datetime.utcnow()
    start = now - timedelta(days=US_LOOKBACK_DAYS)
    end = now + timedelta(days=2)

    updated_assets = 0
    failed_assets = 0

    for n in nodes:
        if n.get("type") != "ASSET":
            continue

        asset_id = n.get("id")
        src = ASSET_SOURCES.get(asset_id)
        if not src:
            continue  # mapping 없는 자산은 “그대로 유지”

        metrics = n.get("metrics") or {}
        if not isinstance(metrics, dict):
            metrics = {}

        # per-asset meta
        asset_meta = n.get("metricsMeta")
        if not isinstance(asset_meta, dict):
            asset_meta = {}
        asset_meta["updatedAtUTC"] = utc_now_iso()
        asset_meta["status"] = "OK"
        asset_meta["sourcePrice"] = ""
        asset_meta["sourcePER"] = ""
        asset_meta["lastError"] = ""

        df = None
        latest_price = None

        # -------------------------
        # (1) PRICE -> RETURNS (KR/US)
        # -------------------------
        try:
            if src.country == "KR" and src.krx_ticker:
                df = krx_download_daily(src.krx_ticker, start, end)
                asset_meta["sourcePrice"] = "KRX(pykrx)"

            elif src.country == "US" and src.us_symbol:
                # Primary: FMP
                if fmp_api_key:
                    try:
                        df = fmp_historical_price_eod_light(src.us_symbol, fmp_api_key)
                        asset_meta["sourcePrice"] = "FMP(stable)"
                    except Exception as e_fmp:
                        # Only fallback for typical blocking / 403 / etc.
                        asset_meta["lastError"] = f"FMP price failed: {short_err(e_fmp)}"
                        df = None

                # Backup: Alpha Vantage (only if FMP missing/failed)
                if (df is None or df.empty) and av_api_key:
                    try:
                        df = alphavantage_daily_adjusted(src.us_symbol, av_api_key)
                        asset_meta["sourcePrice"] = "AlphaVantage(backup)"
                        asset_meta["lastError"] = ""  # backup succeeded
                    except Exception as e_av:
                        asset_meta["lastError"] = f"AV price failed: {short_err(e_av)}"
                        df = None

        except Exception as e:
            asset_meta["status"] = "PARTIAL"
            asset_meta["lastError"] = f"price error: {short_err(e)}"
            df = None

        if df is not None and not df.empty:
            latest_price = pick_num(df["Close"].iloc[-1])
            returns = compute_returns_from_close(df)
            wrote_any = False
            for k, v in returns.items():
                if v is not None:
                    metrics[k] = v
                    wrote_any = True
            if wrote_any:
                updated_assets += 1
            else:
                asset_meta["status"] = "PARTIAL"
        else:
            asset_meta["status"] = "PARTIAL"

        # -------------------------
        # (2) PER UPDATE
        # -------------------------
        # KR: FnGuide (best-effort)
        if src.country == "KR" and src.krx_ticker:
            try:
                trailing_per, fwd12m_per = fetch_per_from_fnguide(src.krx_ticker)
                if trailing_per is not None:
                    metrics["per"] = trailing_per
                    asset_meta["sourcePER"] = "FnGuide(best-effort)"
                if fwd12m_per is not None:
                    metrics["perFwd12m"] = fwd12m_per
                    asset_meta["sourcePER"] = "FnGuide(best-effort)"
            except Exception as e:
                # keep existing
                asset_meta["status"] = "PARTIAL"
                msg = short_err(e)
                if asset_meta["lastError"]:
                    asset_meta["lastError"] += f" | FnGuide PER failed: {msg}"
                else:
                    asset_meta["lastError"] = f"FnGuide PER failed: {msg}"

        # US: 12M FWD PER = price / forward EPS (FMP)
        if src.country == "US" and src.us_symbol:
            if fmp_api_key and latest_price is not None:
                try:
                    eps_fwd = fetch_forward_eps_from_fmp(src.us_symbol, fmp_api_key)
                    if eps_fwd is not None and eps_fwd != 0:
                        fwd_per = float(round(latest_price / eps_fwd, 2))
                        if fwd_per > 0:
                            metrics["perFwd12m"] = fwd_per
                            if asset_meta["sourcePER"]:
                                asset_meta["sourcePER"] += "+FMP(eps)"
                            else:
                                asset_meta["sourcePER"] = "FMP(eps)"
                    else:
                        asset_meta["status"] = "PARTIAL"
                except Exception as e:
                    asset_meta["status"] = "PARTIAL"
                    msg = short_err(e)
                    if asset_meta["lastError"]:
                        asset_meta["lastError"] += f" | US EPS failed: {msg}"
                    else:
                        asset_meta["lastError"] = f"US EPS failed: {msg}"
            else:
                # no crash, just partial
                asset_meta["status"] = "PARTIAL"
                if latest_price is None:
                    if asset_meta["lastError"]:
                        asset_meta["lastError"] += " | US latest price missing => no US FWD PER"
                    else:
                        asset_meta["lastError"] = "US latest price missing => no US FWD PER"
                if not fmp_api_key:
                    if asset_meta["lastError"]:
                        asset_meta["lastError"] += " | FMP_API_KEY missing => no US EPS"
                    else:
                        asset_meta["lastError"] = "FMP_API_KEY missing => no US EPS"

        # finalize node
        n["metrics"] = metrics
        n["metricsMeta"] = asset_meta

        if asset_meta["status"] != "OK":
            failed_assets += 1

    # theme status
    if failed_assets > 0 and updated_assets == 0:
        meta["metricsStatus"] = "FAIL"
        meta["metricsLastError"] = "All mapped assets failed to update (kept previous cache)."
    elif failed_assets > 0:
        meta["metricsStatus"] = "PARTIAL"
        meta["metricsLastError"] = f"Some assets not updated (failed_assets={failed_assets})."
    else:
        meta["metricsStatus"] = "OK"
        meta["metricsLastError"] = ""

    save_theme_json(theme_path, theme)
    return True, f"[OK] Updated: {theme_path} (status={meta['metricsStatus']})"


# =========================
# MAIN
# =========================

def main():
    fmp_api_key = os.getenv("FMP_API_KEY", "").strip()
    av_api_key = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()

    print("[INFO] TARGET_THEMES:", ", ".join(TARGET_THEMES))
    print("[INFO] FMP_API_KEY:", "SET" if fmp_api_key else "MISSING")
    print("[INFO] ALPHAVANTAGE_API_KEY:", "SET" if av_api_key else "MISSING")

    ok_all = True
    for theme_id in TARGET_THEMES:
        try:
            ok, msg = update_one_theme(theme_id, fmp_api_key, av_api_key)
            print(msg)
            if not ok:
                ok_all = False
        except Exception as e:
            ok_all = False
            print(f"[ERROR] Theme {theme_id} crashed: {short_err(e)}")

    if ok_all:
        print("[DONE] All themes processed.")
    else:
        # GitHub Actions: we still allow success if file write happened,
        # but if a theme crashed entirely we exit 1 to get signal.
        raise SystemExit(1)


if __name__ == "__main__":
    main()

