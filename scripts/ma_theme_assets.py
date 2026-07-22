#!/usr/bin/env python3
# ma_theme_assets.py
# 전체 테마 ASSET(중복제거) + 벤치마크(SPY/DIA/QQQ)에 대해 ma_watchlist_brief.py 와
# 동일한 원리·데이터소스(FMP 미국 / EODHD 한국)로 이동평균선 시그널을 계산.
# 출력: data/ma_brief/assets.json  = {asof, generated, count, buckets, items:{TICKER: jrow}}
# UI(테마 브리핑)는 이 파일에서 테마 자산 + 벤치마크만 골라 렌더.
import os, sys, json, glob
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ma_watchlist_brief as mb  # 동일 로직 재사용 (hist_*, sma, seq7, signals, bucket_of, ...)

DATA = HERE.parent / "data"
THEME_DIR = DATA / "theme"
OUT = DATA / "ma_brief"
OUT.mkdir(exist_ok=True)

# 벤치마크: 항상 포함 (UI에서 표 맨 아래 고정)
BENCH = [
    {"ticker": "SPY", "exchange": "NYSEARCA", "country": "US", "name": "SPY", "sector": "벤치마크"},
    {"ticker": "DIA", "exchange": "NYSEARCA", "country": "US", "name": "DIA", "sector": "벤치마크"},
    {"ticker": "QQQ", "exchange": "NASDAQ",   "country": "US", "name": "QQQ", "sector": "벤치마크"},
    {"ticker": "SOXX", "exchange": "NASDAQ",  "country": "US", "name": "SOXX (필라델피아반도체)", "sector": "벤치마크"},
    {"ticker": "069500", "exchange": "KRX", "country": "KR", "name": "KODEX200", "sector": "벤치마크"},
    {"ticker": "229200", "exchange": "KRX", "country": "KR", "name": "KODEX코스닥150", "sector": "벤치마크"},
]

# JP FMP 접미사(간이) — 나머지 US는 티커 직접
FMP_SUFFIX = {"TSE": ".T", "TYO": ".T"}


def collect_assets():
    """전 테마 ASSET 중복제거 → {ticker: {ticker,exchange,country,name}}"""
    out = {}
    for f in glob.glob(str(THEME_DIR / "T_*.json")):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        for n in d.get("nodes", []):
            if n.get("type") != "ASSET":
                continue
            ex = n.get("exposure", {}) or {}
            tk = (ex.get("ticker") or "").strip()
            if not tk:
                continue
            if tk not in out:
                out[tk] = {"ticker": tk, "exchange": (ex.get("exchange") or "").strip(),
                           "country": (ex.get("country") or "US").strip(), "name": n.get("name", tk)}
    for b in BENCH:
        out[b["ticker"]] = {k: b[k] for k in ("ticker", "exchange", "country", "name")}
    return out


def fetch_rows(tk, co, exch):
    if co == "KR":
        return mb.hist_kr(tk)
    if co == "HK":
        return mb.hist_hk(tk)
    suf = FMP_SUFFIX.get((exch or "").upper(), "")
    return mb.hist_us(tk + suf)


def compute_jrow(a):
    tk, co, name = a["ticker"], a.get("country", "US"), a.get("name", a["ticker"])
    exch = a.get("exchange", "")
    sector = a.get("sector", "")
    link = (f"https://finance.naver.com/item/main.naver?code={tk}" if co == "KR"
            else f"https://finance.yahoo.com/quote/{tk}")
    base = {"sector": sector, "name": name, "ticker": tk, "country": co, "link": link}
    rows = fetch_rows(tk, co, exch)
    if not rows:
        return {**base, "close": None, "g5": None, "g20": None, "g60": None, "g120": None, "hg": None,
                "align": "na", "above": -1, "bucket": "na", "bucketLabel": "—", "seq7": "—",
                "signal": "데이터 없음", "interp": "데이터 없음(해석 불가).", "_asof": None}
    cl, dd = mb.closes_desc(rows)
    if len(cl) < 30:
        return {**base, "close": None, "g5": None, "g20": None, "g60": None, "g120": None, "hg": None,
                "align": "na", "above": -1, "bucket": "na", "bucketLabel": "—", "seq7": "—",
                "signal": "데이터 부족", "interp": "데이터 부족(해석 불가).", "_asof": dd}
    c0 = cl[0]
    m5, m20, m60, m120 = mb.sma(cl, 5), mb.sma(cl, 20), mb.sma(cl, 60), mb.sma(cl, 120)
    if None not in (m20, m60, m120) and m20 > m60 > m120:
        align_key = "bull"
    elif None not in (m20, m60, m120) and m20 < m60 < m120:
        align_key = "bear"
    else:
        align_key = "flat"
    sym7, _ = mb.seq7(cl)
    sig = mb.signals(cl)
    sig_txt = " · ".join(sig) if sig else "—"
    bs = mb.band_state(cl)
    bw = bs["bw"] if bs else None
    bw_state = ("수렴" if bs and bs["squeeze"] else "확산전환" if bs and bs["breakout"] else "")
    hg, _ = mb.high_gap(cl)
    above_ct = sum(1 for m in (m5, m20, m60, m120) if m is not None and c0 >= m)
    gap5, gap20, gap60, gap120 = mb.gapnum(c0, m5), mb.gapnum(c0, m20), mb.gapnum(c0, m60), mb.gapnum(c0, m120)
    bkey = mb.bucket_of(align_key, above_ct)
    hp = mb.high_phrase(hg)
    mom = mb.momentum_text(cl)
    interp_full = mb.interpret(c0, m20, m60, m120, align_key, sig) + ((" " + hp + ".") if hp else "")
    return {**base, "close": c0, "g5": gap5, "g20": gap20, "g60": gap60, "g120": gap120, "hg": hg,
            "align": align_key, "above": above_ct, "bucket": bkey, "bucketLabel": mb.BUCKETS[bkey],
            "seq7": sym7, "signal": sig_txt, "bw": bw, "bwState": bw_state,
            "interp": (interp_full + " " + mom + "."), "_asof": dd}


def main():
    assets = collect_assets()
    limit = int(os.environ.get("MA_THEME_LIMIT", "0"))
    keys = list(assets.keys())
    if limit > 0:
        keys = keys[:limit]
    items = {}
    asof = None
    ok = miss = 0
    for i, tk in enumerate(keys, 1):
        jr = compute_jrow(assets[tk])
        d = jr.pop("_asof", None)
        if d and (asof is None or d > asof):
            asof = d
        items[tk] = jr
        if jr["close"] is None:
            miss += 1
        else:
            ok += 1
        if i % 100 == 0:
            print(f"  {i}/{len(keys)} (ok={ok} miss={miss})", flush=True)
    payload = {"asof": asof or mb.TO, "generated": mb.TODAY.isoformat(),
               "count": len(items), "buckets": mb.BUCKETS, "items": items}
    (OUT / "assets.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"✅ assets.json: {len(items)}종목 (ok={ok} miss={miss}) asof={payload['asof']}")


if __name__ == "__main__":
    main()
