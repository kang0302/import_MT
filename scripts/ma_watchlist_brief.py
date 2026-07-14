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

def hist_hk(ticker):
    # 홍콩(예: 2800.HK): EODHD 우선, 실패 시 FMP
    if EODHD_KEY:
        d = _get(f"{EOD_BASE}/{ticker}", {"api_token": EODHD_KEY, "fmt": "json", "from": FROM, "to": TO})
        if isinstance(d, list) and len(d) > 30:
            return d
    return hist_us(ticker)

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

def seq7(cl):
    """최근 7거래일 등락 시퀀스 (과거→최근). (기호문자열, 워딩문자열) 반환."""
    syms, words = [], []
    for i in range(min(7, len(cl)-1)):
        d = cl[i] - cl[i+1]
        if d > 0: syms.append("▲"); words.append("상승")
        elif d < 0: syms.append("▼"); words.append("하락")
        else: syms.append("-"); words.append("보합")
    syms = syms[::-1]; words = words[::-1]
    return "".join(syms) if syms else "—", "-".join(words) if words else "—"


def momentum_text(cl):
    """최근 7거래일 등락을 해석하는 문구."""
    ch = []
    for i in range(min(7, len(cl)-1)):
        d = cl[i] - cl[i+1]
        ch.append(1 if d > 0 else (-1 if d < 0 else 0))
    if not ch:
        return "최근 등락 데이터 부족"
    ups = ch.count(1); downs = ch.count(-1)
    s = ch[0]; streak = 1
    for x in ch[1:]:
        if x == s and x != 0: streak += 1
        else: break
    if s == 1 and streak >= 3:
        return f"최근 {streak}거래일 연속 상승세로 단기 매수 모멘텀 강함"
    if s == -1 and streak >= 3:
        return f"최근 {streak}거래일 연속 하락세로 단기 매도 압력 지속"
    if ups > downs:
        return f"최근 7거래일 중 {ups}일 상승({downs}일 하락)으로 단기 매수 우위"
    if downs > ups:
        return f"최근 7거래일 중 {downs}일 하락({ups}일 상승)으로 단기 매도 우위"
    return f"최근 7거래일 상승·하락 {ups}:{downs} 균형으로 방향성 중립"


def interpret(c0, m30, m60, m120, align_key, sig):
    """종목별 이평선 상황 해석 텍스트(규칙 기반)."""
    if None in (m30, m60, m120):
        return "120일 이동평균 계산에 필요한 데이터가 부족합니다."
    g30 = (c0/m30-1)*100; g60 = (c0/m60-1)*100; g120 = (c0/m120-1)*100
    above = [n for n, g in zip((30,60,120), (g30,g60,g120)) if g >= 0]
    parts = []
    if len(above) == 3:
        base = "단기·중기·장기(30·60·120일) 이평선을 모두 상회하는 상승추세"
    elif len(above) == 0:
        base = "세 이평선을 모두 하회하는 하락추세"
    else:
        up = "·".join(f"{n}일선" for n in above)
        base = f"{up}은 상회하나 나머지 이평선은 하회하는 혼조 국면"
    if align_key == "bull": base += " (이평선 정배열, 추세 견조)"
    elif align_key == "bear": base += " (역배열, 추세 약세)"
    parts.append(base)
    for x in sig:
        if "상향돌파" in x: parts.append(f"금일 {x.replace('일선 상향돌파','')}일선을 상향 돌파해 단기 반등 시도")
        elif "이탈" in x: parts.append(f"금일 {x.replace('일선 이탈','')}일선을 이탈해 단기 약세로 전환")
        elif "골든" in x: parts.append("골든크로스(30>60일선)로 추세 개선 신호")
        elif "데드" in x: parts.append("데드크로스(30<60일선)로 추세 악화 신호")
    near = [n for n, g in zip((30,60,120), (g30,g60,g120)) if abs(g) <= 2]
    if near and not sig:
        parts.append(f"{near[0]}일선 부근에서 지지·저항 공방")
    if g120 >= 25: parts.append("장기선 대비 이격이 커 단기 과열 구간")
    elif g120 <= -25: parts.append("장기선 대비 과대낙폭으로 기술적 반등 여지")
    return ". ".join(parts) + "."


def high_gap(cl):
    """최근 1년(약 252거래일) 최고 종가 대비 현재가 격차(%). (값, 표시문자열)."""
    seg = cl[:252] if len(cl) >= 1 else cl
    hi = max(seg) if seg else None
    if not hi:
        return None, "—"
    g = (cl[0]/hi - 1)*100
    return g, f"{g:+.1f}%"


def high_phrase(g):
    if g is None:
        return ""
    if g >= -3:
        return f"52주 신고가 부근(고점比 {g:+.1f}%)"
    if g <= -25:
        return f"52주 고점比 {g:+.1f}%로 깊은 조정"
    if g <= -10:
        return f"52주 고점比 {g:+.1f}% 조정 국면"
    return f"52주 고점比 {g:+.1f}%"


def main():
    wl = json.loads((DATA/"watchlist.json").read_text(encoding="utf-8"))
    items = wl.get("items", [])
    records, asof = [], None
    n_up = n_dn = n_bull = n_bear = n_break = n_lose = 0
    missing = []
    for it in items:
        tk, co, name = it["ticker"], it.get("country","US"), it.get("name", it["ticker"])
        # 종목명 링크: 국내=네이버금융, 해외=야후파이낸스
        link = (f"https://finance.naver.com/item/main.naver?code={tk}" if co == "KR"
                else f"https://finance.yahoo.com/quote/{tk}")
        sector = it.get("sector", "")
        label = f"{name} ({tk})"
        mdlabel = (f"`{sector}` " if sector else "") + f"[{label}]({link})"
        _badge = (f"<span style='display:inline-block;padding:0 5px;margin-right:4px;border-radius:4px;background:#eef2ff;color:#4338ca;font-size:11px;vertical-align:middle'>{sector}</span>" if sector else "")
        htmlabel_plain = f"<a href=\"{link}\" style=\"color:#2563eb;text-decoration:none\">{label}</a>"
        htmlabel = _badge + htmlabel_plain
        rows = hist_kr(tk) if co == "KR" else (hist_hk(tk) if co == "HK" else hist_us(tk))
        if not rows:
            records.append({"ak":"na","above":-1,"hg":None,"md":f"| {mdlabel} | 데이터 없음 | — | — | — | — | — | — | — | — | — |","cells":(htmlabel,"데이터 없음","—","—","—","—","—","—","—","—","—"),"il":(mdlabel,htmlabel,"—","데이터 없음(해석 불가).")}); missing.append(name); continue
        cl, d = closes_desc(rows)
        if d and (asof is None or d > asof): asof = d
        if len(cl) < 30:
            records.append({"ak":"na","above":-1,"hg":None,"md":f"| {mdlabel} | 데이터 부족 | — | — | — | — | — | — | — | — | — |","cells":(htmlabel,"데이터 부족","—","—","—","—","—","—","—","—","—"),"il":(mdlabel,htmlabel,"—","데이터 부족(해석 불가).")}); missing.append(name); continue
        c0 = cl[0]
        m5, m15 = sma(cl,5), sma(cl,15)
        m30, m60, m120 = sma(cl,30), sma(cl,60), sma(cl,120)
        # 배열
        if None not in (m30,m60,m120) and m30>m60>m120: align="🟢 정배열"; align_key="bull"; n_bull+=1
        elif None not in (m30,m60,m120) and m30<m60<m120: align="🔴 역배열"; align_key="bear"; n_bear+=1
        else: align="⚪ 혼조"; align_key="flat"
        sym7, words7 = seq7(cl)
        if m30 is not None and c0>=m30: n_up+=1
        elif m30 is not None: n_dn+=1
        sig = signals(cl)
        for s in sig:
            if "상향돌파" in s: n_break+=1
            if "이탈" in s: n_lose+=1
        sig_txt = " · ".join(sig) if sig else "—"
        hg, hg_str = high_gap(cl)
        above_ct = sum(1 for m in (m5,m15,m30,m60,m120) if m is not None and c0 >= m)
        hp = high_phrase(hg)
        interp_full = interpret(c0, m30, m60, m120, align_key, sig) + ((" " + hp + ".") if hp else "")
        mdrow = f"| {mdlabel} | {c0:,.2f} | {arrow(c0,m5)} | {arrow(c0,m15)} | {arrow(c0,m30)} | {arrow(c0,m60)} | {arrow(c0,m120)} | {hg_str} | {align} | {sym7} | {sig_txt} |"
        records.append({"ak":align_key,"above":above_ct,"hg":hg,"md":mdrow,
                        "cells":(htmlabel, f"{c0:,.2f}", arrow(c0,m5), arrow(c0,m15), arrow(c0,m30), arrow(c0,m60), arrow(c0,m120), hg_str, align, sym7, sig_txt),
                        "il":(mdlabel, htmlabel_plain, momentum_text(cl), interp_full)})
    asof = asof or TO
    md = []
    md.append(f"# 📈 관심종목 이동평균선 브리핑")
    md.append("")
    md.append(f"**기준일(전일 종가): {asof}** · 종목 {len(items)}개 · 생성 {TODAY.isoformat()}")
    md.append("")
    n_flat = len([r for r in records if r["ak"]=="flat"])
    md.append(f"- 🟢 정배열 **{n_bull}** · ⚪ 혼조 **{n_flat}** · 🔴 역배열 **{n_bear}**")
    md.append(f"- 30일선 상회 **{n_up}** / 하회 **{n_dn}** · 오늘 상향돌파 **{n_break}** · 이탈 **{n_lose}**" + (f" · 데이터 없음 {len(missing)}" if missing else ""))
    md.append("")
    GROUPS = [("bull","🟢 정배열"), ("flat","⚪ 혼조"), ("bear","🔴 역배열"), ("na","⚫ 데이터 없음")]
    def rank_grp(ak):
        # 그룹 내 우선순위: ①종가>이평선 개수(3>2>1>0) ②52주 신고가 근접(격차 작은 순)
        return sorted([r for r in records if r["ak"] == ak],
                      key=lambda r: (-(r.get("above", -1)), -(r["hg"] if r.get("hg") is not None else -999.0)))
    HDR = "| 종목 | 종가 | vs 5일선 | vs 15일선 | vs 30일선 | vs 60일선 | vs 120일선 | 52주高比 | 배열 | 최근7일 | 오늘 신호 |"
    SEP = "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    for ak, glabel in GROUPS:
        grp = rank_grp(ak)
        if not grp: continue
        md.append(f"## {glabel} ({len(grp)})")
        md.append("")
        md.append(HDR); md.append(SEP)
        for r in grp: md.append(r["md"])
        md.append("")
        for mdl, _, mom, txt in [r["il"] for r in grp]:
            md.append(f"- {mdl} — {txt} {mom}.")
        md.append("")
    md.append("> ▲(적) 상회·상승 / ▼(청) 하회·하락 (괄호=이격도%). 52주高比=최근1년 최고종가 대비 격차. 이평선=5·15·30·60·120일 · 정배열=30>60>120. 최근7일=과거→최근. 그룹 내 정렬=종가상회 이평선수↓ · 52주 신고가 근접순. 신호는 전일 대비 당일 돌파·이탈·골든/데드크로스.")
    md.append("")
    text = "\n".join(md) + "\n"
    (OUT/"latest.md").write_text(text, encoding="utf-8")
    (OUT/f"{TODAY.isoformat()}.md").write_text(text, encoding="utf-8")

    # HTML (이메일 본문용)
    def esc(s): return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    def cellcol(v):
        if v.startswith("▲"): return "#dc2626"   # 상승=적색
        if v.startswith("▼"): return "#2563eb"   # 하락=청색
        return "#334155"
    def highcol(v):
        try: g = float(str(v).replace("%","").replace("+",""))
        except Exception: return "#334155"
        if g >= -3: return "#dc2626"    # 고점 근접=적
        if g <= -20: return "#2563eb"   # 큰 낙폭=청
        return "#334155"
    head = "".join(f"<th>{h}</th>" for h in ["종목","종가","5일선","15일선","30일선","60일선","120일선","52주高比","배열","최근7일","오늘 신호"])
    def render_body(grp):
        out = ""
        for r in grp:
            tds = ""
            for i, v in enumerate(r["cells"]):
                if i == 9:  # 최근7일 시퀀스
                    cell = "".join(("<span style='color:#dc2626'>▲</span>" if c=="▲" else
                                    "<span style='color:#2563eb'>▼</span>" if c=="▼" else
                                    f"<span style='color:#94a3b8'>{esc(c)}</span>") for c in str(v))
                    col = "#0f172a"
                elif i == 7:  # 52주 고점比
                    col = highcol(v)
                    cell = esc(v)
                else:
                    col = cellcol(v) if i in (2,3,4,5,6) else "#0f172a"
                    cell = v if i == 0 else esc(v)
                st = f" style='color:{col}'" if col in ("#dc2626","#2563eb") else ""
                tds += f"<td{st}>{cell}</td>"
            out += f"<tr>{tds}</tr>"
        return out
    sections = ""
    for ak, glabel in GROUPS:
        grp = rank_grp(ak)
        if not grp: continue
        gbody = render_body(grp)
        lis = "".join(f"<li><b>{h}</b> — {esc(t)} {esc(mom)}.</li>" for _, h, mom, t in [r["il"] for r in grp])
        sections += (f"<h3>{glabel} ({len(grp)})</h3>"
                     f"<table><thead><tr>{head}</tr></thead><tbody>{gbody}</tbody></table>"
                     f"<ul>{lis}</ul>")
    html = f"""<div class="mb">
<style>
.mb{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#0f172a}}
.mb h2{{margin:0 0 6px}} .mb h3{{margin:16px 0 6px}}
.mb table{{border-collapse:collapse;font-size:13px}}
.mb td,.mb th{{padding:5px 8px;border:1px solid #e2e8f0;white-space:nowrap}}
.mb th{{background:#f1f5f9;text-align:left}}
.mb ul{{margin:6px 0 0;padding-left:18px;color:#334155;font-size:12.5px;line-height:1.5}}
</style>
<h2>📈 관심종목 이동평균선 브리핑</h2>
<p style="margin:0 0 4px;color:#475569">기준일(전일 종가): <b>{esc(asof)}</b> · 종목 {len(items)}개 · 생성 {TODAY.isoformat()}</p>
<p style="margin:0 0 6px;color:#475569">🟢 정배열 <b>{n_bull}</b> · ⚪ 혼조 <b>{n_flat}</b> · 🔴 역배열 <b>{n_bear}</b> · 30일선 상회 <b>{n_up}</b>/하회 <b>{n_dn}</b> · 상향돌파 <b>{n_break}</b>·이탈 <b>{n_lose}</b></p>
{sections}
<p style="margin:12px 0 0;color:#94a3b8;font-size:11px">▲(적) 상회·상승 / ▼(청) 하회·하락 (괄호=이격도%). 52주高比=최근1년 최고종가 대비 격차. 이평선=5·15·30·60·120일 · 정배열=30&gt;60&gt;120. 최근7일=과거→최근. 신호=전일 대비 당일 돌파·이탈·골든/데드크로스.</p>
</div>"""
    (OUT/"latest.html").write_text(html, encoding="utf-8")
    (OUT/f"{TODAY.isoformat()}.html").write_text(html, encoding="utf-8")

    # 아카이브 인덱스 (날짜 목록, 최신순) — 다시 찾아볼 수 있게 누적
    idx_path = OUT/"index.json"
    try:
        idx = json.loads(idx_path.read_text(encoding="utf-8")) if idx_path.exists() else []
    except Exception:
        idx = []
    idx = [e for e in idx if isinstance(e, dict) and e.get("date")]
    today_iso = TODAY.isoformat()
    found = False
    for e in idx:
        if e.get("date") == today_iso:
            e["asof"] = asof; e["bull"] = n_bull; e["bear"] = n_bear; found = True
    if not found:
        idx.append({"date": today_iso, "asof": asof, "bull": n_bull, "bear": n_bear})
    idx = sorted(idx, key=lambda e: e["date"], reverse=True)
    idx_path.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"✅ ma_brief 생성: asof={asof} 종목={len(items)} 데이터없음={len(missing)} {missing}")
    print(f"   정배열={n_bull} 역배열={n_bear} 30선상회={n_up} 하회={n_dn} 돌파={n_break} 이탈={n_lose}")

if __name__ == "__main__":
    main()
