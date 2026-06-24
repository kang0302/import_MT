# -*- coding: utf-8 -*-
import io, sys, csv, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
AROWS=list(csv.DictReader(open('ssot/asset_ssot.csv',encoding='utf-8',newline='')))
byid={r['asset_id']:r for r in AROWS}
TID='T_557'; TNAME='글로벌 바이오: 특허절벽 타격'
EX={'Merck':'A_1316','BMS':'A_2190','JNJ':'A_1703','Pfizer':'A_1214','Regeneron':'A_1001','AbbVie':'A_1006'}
def ako(k): return byid[EX[k]].get('asset_name_ko') or byid[EX[k]].get('asset_name_en')
def aexp(k):
    r=byid[EX[k]]; return {'ticker':r.get('ticker',''),'exchange':r.get('exchange',''),'country':r.get('country','')}
GC={'NYSE':'NYSE','NASDAQ':'NASDAQ'}
def link(k):
    e=aexp(k); t=e['ticker']; g=GC.get(e['exchange'],e['exchange'])
    return f"[{ako(k)} ({t})](https://www.google.com/finance/quote/{t}:{g})"
METRICS={k:None for k in ['return_1d','return_3d','return_7d','return_15d','return_1m','return_ytd','return_1y','return_2y','return_3y','close','marketCap','pe_ttm','valuationAsOf','valuationSource','returnsAsOf','returnsSource']}
MACROS=['M_1075','M_102','M_1078']  # 특허절벽 · 빅파마특허만료 · 美약가인하(IRA)
MACNAME={r['macro_id']:r.get('macro_name_ko') for r in csv.DictReader(open('ssot/macro_ssot.csv',encoding='utf-8'))}
# (key, 위상CHR(타격/방어), biz, eco, drv)
ITEMS=[
 ('Merck','절대규모 1위·방어 약함','키트루다 의존(매출 약 56%)','2028 IV특허 만료·큐렉스(SC) 재제형','1)키트루다 절벽(28) 2)큐렉스 SC전환 3)Januvia/Lynparza/Gardasil 중첩 4)약가'),
 ('BMS','집중도 1위·방어 약함','엘리퀴스+옵디보(매출 약 45%)','2030까지 매출 47% 노출·비용감축','1)엘리퀴스(28)·옵디보 절벽 2)상위5품목 -62% 3)포트폴리오 재편 4)약가'),
 ('JNJ','타격 큼·방어 중간','스텔라라(108억) 시밀러 진입','2025 시밀러 시대 진입·분산 방어','1)스텔라라 시밀러(25) 2)경쟁 지연 방어 3)면역·MedTech 분산 4)약가'),
 ('Pfizer','타격 큼·방어 중간','엘리퀴스 등(BMS와 공유)','2028 절벽·코로나매출 소멸·M&A 보충','1)엘리퀴스 절벽 공유 2)코로나 매출 소멸 3)Seagen ADC M&A 4)약가'),
 ('Regeneron','집중 높음·방어 약함','아일리아 의존','HD아일리아 전환 더딤·Dupixent 성장','1)아일리아 시밀러 2)HD아일리아 채택 3)Dupixent 4)약가'),
 ('AbbVie','이미 흡수·방어 강함(전환성공)','휴미라 의존 39%→9% 축소','린버크·스카이리치 14%→43% 전환','1)휴미라 절벽 흡수 2)린버크·스카이리치 3)차세대 전환 4)낮은 LOE리스크'),
]
EVENTS=[
 ["휴미라 특허절벽 개막","2023.01~2023.12","📉 타격 (강력)","휴미라 美 바이오시밀러 출시 → 애브비 휴미라 매출 급감(LOE 사이클 개막)","12개월","✅ 차세대 전환"],
 ["IRA 약가협상 대상 지정","2023.08~2024.12","📉 타격","IRA 약가협상 대상(엘리퀴스·자디앙 등) → 빅파마 매출 추가 압박","18개월","—"],
 ["스텔라라 LOE","2025.01~2025.12","📉 타격","J&J 스텔라라 바이오시밀러 진입 → 매출 감소(분산으로 일부 방어)","진행 중","—"],
 ["빅파마 비용·인력 감축·M&A","2024.06~2025.12","⚠️ 진행중","BMS 등 절벽 대비 구조조정·M&A → 포트폴리오 재편","진행 중","—"],
 ["키트루다·엘리퀴스 2028 절벽 임박","2025.01~2025.12","📉 타격 (강력)","키트루다·엘리퀴스 2028 만료 임박 → 머크·BMS 리레이팅 압박","진행 중","—"],
 ["차세대 파이프라인 전환 경쟁","2024.01~2025.12","⚠️ 진행중","비만·ADC·면역 차세대 전환 성공/실패로 명암(애브비 모범·릴리 무풍)","진행 중","—"],
 ["특허절벽: 방어력에 명암","2025.06~진행중","⚠️ 진행중","절벽 구조적 타격 / 재제형·파이프라인 방어력 따라 종목별 차별화","진행 중","미정 (전환·방어력 의존)"],
]
charstore=[]; CC=[1560]
def nc(kr): cid=f"C_{CC[0]}"; CC[0]+=1; charstore.append((cid,kr)); return cid
period={"periodDefault":"3d","periodOptions":["1d","3d","7d","15d","1m","ytd","1y","2y","3y"],"dataAsOf":"2026-06-24T00:00:00+09:00"}
nodes=[{"id":TID,"type":"THEME","name":TNAME}]; edges=[]
for key,ch,biz,eco,drv in ITEMS:
    a=EX[key]
    nodes.append({"id":a,"type":"ASSET","name":ako(key),"exposure":aexp(key),"metrics":dict(METRICS)})
    edges.append({"from":a,"to":TID,"type":"THEMED_AS"})
    cid=nc(ch); nodes.append({"id":cid,"type":"CHARACTER","name":ch})
    edges.append({"from":a,"to":cid,"type":"HAS_TRAIT"})
for m in MACROS:
    nodes.append({"id":m,"type":"MACRO","name":MACNAME[m]}); edges.append({"from":m,"to":TID,"type":"IMPACTS"})
obj={"schemaVersion":"v5","themeId":TID,"themeName":TNAME,"meta":{**period,"description":"키트루다·엘리퀴스·스텔라라·아일리아·휴미라 등 대형 바이오 특허만료(LOE)로 매출 절벽에 노출된 글로벌 제약사 — 종목별 타격강도·방어력 차별화.","notes":f"{TID} 신규. 자산별 CHARACTER(타격/방어력), MACRO 3(재사용)","sources":[]},"nodes":nodes,"edges":edges}
obj["links"]=obj["edges"]
json.dump(obj,open(f'theme/{TID}.json','w',encoding='utf-8'),ensure_ascii=False,indent=2)
EVHDR=("\n\n---\n\n## 이벤트 × 테마 DB (지난 5년 핵심 변동요인 7)\n\n| 이벤트명 | 시기 | 방향 | 핵심 메커니즘 | 지속기간 | 회복여부 |\n|---|---|---|---|---|---|\n")
lines=["| 종목 | 핵심 사업(타격/방어 위상) | 사업생태계 | 주가 핵심 동인 |","| --- | --- | --- | --- |"]
for key,ch,biz,eco,drv in ITEMS: lines.append(f"| {link(key)} | {biz} | {eco} | {drv} |")
open(f'briefing/{TID}.md','w',encoding='utf-8').write("\n".join(lines)+EVHDR+''.join('| '+' | '.join(r)+' |\n' for r in EVENTS))
with open('ssot/character_ssot.csv','a',encoding='utf-8',newline='') as f:
    w=csv.writer(f)
    for cid,kr in charstore: w.writerow([cid,kr,kr])
idx=json.load(open('theme/index.json',encoding='utf-8'))
if TID not in {e['themeId'] for e in idx}: idx.append({"themeId":TID,"themeName":TNAME})
json.dump(idx,open('theme/index.json','w',encoding='utf-8'),ensure_ascii=False,indent=2)
print('theme',TID,'| assets',len(ITEMS),'| chars',len(charstore),'| macros',len(MACROS),'| index',len(idx))
