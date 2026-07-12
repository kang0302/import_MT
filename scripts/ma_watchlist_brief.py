# -*- coding: utf-8 -*-
"""
관심종목(워치리스트) 이동평균선 데일리 브리핑 생성기.

- 입력: data/watchlist.json  ({items:[{ticker,exchange,country,name}]})
- 종가 히스토리:
    US  → FMP /stable/historical-price-eod/full (env FMP_API_KEY)  또는 로컬 캐시 data/cache/fmp_historical_eod_full/{ticker}.json
    KR  → EODHD /api/eod/{ticker}.KO|.KQ (env EODHD_API_KEY)
- 계산: 전일 종가 vs SMA30/60/120 (상회▲/하회▼ + 이격도%), 정/역배열, 당일 돌파·이탈·골든/데드크로스
- 출력: data/ma_brief/latest.md, data/ma_brief/{YYYY-MM-DD}.md

로컬에서 API 키가 없으면 캐시가 있는 종목만 계산하고 나머지는 '데이터 없음'으로 표기한다(워크플로우에서 키로 채워짐).
"""
import io, sys, os, json, datetime
from pathlib import Path
try:
    import requests
except Exception:
    requests = None

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
BASE = Path(__file__).resolve().parents[1]
DATA = BASE / "data"
CACHE = DATA / "cache" / "fmp_historical_eod_full"
OUT = DATA / "ma_brief"
OUT.mkdir(parents=True, exist_ok=True)
FMP_KEY = (os.environ.get("FMP_API_KEY") or "").strip()
EODHD_KEY = (os.environ.get("EODHD_API_KEY") or "").strip()
FMP_BASE = "https://financialmodelingprep.com/stable/historical-price-eod/full"
EOD_BASE = "https://eodhd.com/api/eod"
TODAY = datetime.date.today()
FROM = (TODAY - datetime.timedelta(days=400)).isoformat()
TO = TODAY.isoformat()

def _get(url, params):
    if requests is None: return None
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200: return None
        return r.json()
    except Exception:
        return None

def hist_us(ticker):
    # 1) 로컬 캐시
    f = CACHE / f"{ticker}.json"
    if f.exists():
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
            if isinstance(d, dict): d = d.get("historical") or []
            if d: return d
        except Exception:
            pass
    # 2) FMP fetch
    if FMP_KEY:
        d = _get(FMP_BASE, {"symbol": ticker, "apikey": FMP_KEY, "from": FROM, "to": TO})
        if isinstance(d, dict): d = d.get("historical") or []
        if d: return d
    return None

def hist_kr(ticker):
    if not EODHD_KEY: return None
    for suf in (".KO", ".KQ"):
        d = _get(f"{EOD_BASE}/{ticker}{suf}", {"api_token": EODHD_KEY, "fmt": "json", "from": FROM, "to": TO})
        if isinstance(d, list) and len(d) > 30:
            return d
    return None

def closes_desc(rows):
    """[{date,close}, ...] → 최신순 종가 리스트 + 최신 날짜"""
    clean = [(r.get("date"), r.get("close")) for r in rows if r.get("date") and r.get("close") is not None]
    clean.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in clean], (clean[0][0] if clean else None)

def sma(vals, n, off=0):
    seg = vals[off:off+n]
    return sum(seg)/n if len(seg) == n else None

def arrow(close, m):
    if m is None: return "—"
    gap = (close/m - 1)*100
    return f"{'▲' if close>=m else '▼'} {gap:+.1f}%"

def signals(cl):
    """당일 돌파/이탈/골든·데드크로스 감지 (최신순 리스트 cl)"""
    sig = []
    if len(cl) < 121: return sig
    c0, c1 = cl[0], cl[1]
    for n in (30, 60, 120):
        m0, m1 = sma(cl, n, 0), sma(cl, n, 1)
        if m0 is None or m1 is None: continue
        if c1 < m1 and c0 >= m0: sig.append(f"{n}일선 상향돌파")
        elif c1 > m1 and c0 <= m0: sig.append(f"{n}일선 이탈")
    # MA 골든/데드 (30 vs 60)
    a0, a1 = sma(cl,30,0), sma(cl,30,1)
    b0, b1 = sma(cl,60,0), sma(cl,60,1)
    if None not in (a0,a1,b0,b1):
        if a1 <= b1 and a0 > b0: sig.append("골든크로스(30>60)")
        elif a1 >= b1 and a0 < b0: sig.append("데드크로스(30<60)")
    return sig

def main():
    wl = json.loads((DATA/"watchlist.json").read_text(encoding="utf-8"))
    items = wl.get("items", [])
    rows_out, asof = [], None
    n_up = n_dn = n_bull = n_bear = n_break = n_lose = 0
    missing = []
    for it in items:
        tk, co, name = it["ticker"], it.get("country","US"), it.get("name", it["ticker"])
        rows = hist_kr(tk) if co == "KR" else hist_us(tk)
        if not rows:
            rows_out.append(f"| {name} ({tk}) | 데이터 없음 | — | — | — | — | — |")
            missing.append(name); continue
        cl, d = closes_desc(rows)
        if d and (asof is None or d > asof): asof = d
        if len(cl) < 30:
            rows_out.append(f"| {name} ({tk}) | 데이터 부족 | — | — | — | — | — |")
            missing.append(name); continue
        c0 = cl[0]
        m30, m60, m120 = sma(cl,30), sma(cl,60), sma(cl,120)
        # 배열
        if None not in (m30,m60,m120) and m30>m60>m120: align="🟢 정배열"; n_bull+=1
        elif None not in (m30,m60,m120) and m30<m60<m120: align="🔴 역배열"; n_bear+=1
        else: align="⚪ 혼조"
        if m30 is not None: (n_up if c0>=m30 else n_dn);
        if m30 is not None and c0>=m30: n_up+=1
        elif m30 is not None: n_dn+=1
        sig = signals(cl)
        for s in sig:
            if "상향돌파" in s: n_break+=1
            if "이탈" in s: n_lose+=1
        sig_txt = " · ".join(sig) if sig else "—"
        rows_out.append(f"| {name} ({tk}) | {c0:,.2f} | {arrow(c0,m30)} | {arrow(c0,m60)} | {arrow(c0,m120)} | {align} | {sig_txt} |")
    asof = asof or TO
    md = []
    md.append(f"# 📈 관심종목 이동평균선 브리핑")
    md.append("")
    md.append(f"**기준일(전일 종가): {asof}** · 종목 {len(items)}개 · 생성 {TODAY.isoformat()}")
    md.append("")
    md.append(f"- 30일선 상회 **{n_up}** / 하회 **{n_dn}**  ·  정배열 **{n_bull}** / 역배열 **{n_bear}**")
    md.append(f"- 오늘 상향돌파 **{n_break}** · 이탈 **{n_lose}**" + (f"  ·  데이터 없음 {len(missing)}" if missing else ""))
    md.append("")
    md.append("| 종목 | 종가 | vs 30일선 | vs 60일선 | vs 120일선 | 배열 | 오늘 신호 |")
    md.append("| --- | --- | --- | --- | --- | --- | --- |")
    md += rows_out
    md.append("")
    md.append("> ▲ 상회 / ▼ 하회 (괄호=이격도%). 정배열=30>60>120일선, 역배열=반대. 신호는 전일 대비 당일 돌파·이탈·골든/데드크로스.")
    md.append("")
    text = "\n".join(md) + "\n"
    (OUT/"latest.md").write_text(text, encoding="utf-8")
    (OUT/f"{TODAY.isoformat()}.md").write_text(text, encoding="utf-8")
    print(f"✅ ma_brief 생성: asof={asof} 종목={len(items)} 데이터없음={len(missing)} {missing}")
    print(f"   정배열={n_bull} 역배열={n_bear} 30선상회={n_up} 하회={n_dn} 돌파={n_break} 이탈={n_lose}")

if __name__ == "__main__":
    main()
