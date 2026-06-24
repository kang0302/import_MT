# -*- coding: utf-8 -*-
import io, sys, csv, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
AROWS=list(csv.DictReader(open('ssot/asset_ssot.csv',encoding='utf-8',newline='')))
AFIELDS=list(AROWS[0].keys()); byid={r['asset_id']:r for r in AROWS}
TID='T_556'
TNAME='글로벌바이오: 미국 바이오시밀러 규제완화'

# new asset Organon
NEW=[('OGN','A_2576','Organon','오가논','OGN','NYSE','US')]
NEWMETA={n[0]:{'id':n[1],'en':n[2],'ko':n[3],'ticker':n[4],'exch':n[5],'country':n[6]} for n in NEW}
EX={'Celltrion':'A_717','SamsungEpis':'A_718','SamsungBio':'A_719','Alteogen':'A_716','Sandoz':'A_2471','Amgen':'A_1004'}
def aid(k): return EX.get(k) or NEWMETA[k]['id']
def ako(k):
    if k in NEWMETA: return NEWMETA[k]['ko']
    return byid[EX[k]].get('asset_name_ko') or byid[EX[k]].get('asset_name_en')
def aexp(k):
    if k in NEWMETA:
        m=NEWMETA[k]; return {'ticker':m['ticker'],'exchange':m['exch'],'country':m['country']}
    r=byid[EX[k]]; return {'ticker':r.get('ticker',''),'exchange':r.get('exchange',''),'country':r.get('country','')}
GC={'KOSPI':'KRX','KOSDAQ':'KRX','KRX':'KRX','NYSE':'NYSE','NASDAQ':'NASDAQ','SWX':'SWX','SIX':'SWX'}
def link(k):
    e=aexp(k); t=e['ticker']; g=GC.get(e['exchange'],e['exchange'])
    return f"[{ako(k)} ({t})](https://www.google.com/finance/quote/{t}:{g})" if (t and t!='0126Z0') else f"[{ako(k)} (비상장)](https://www.google.com)"
METRICS={k:None for k in ['return_1d','return_3d','return_7d','return_15d','return_1m','return_ytd','return_1y','return_2y','return_3y','close','marketCap','pe_ttm','valuationAsOf','valuationSource','returnsAsOf','returnsSource']}

# 5 new macros (user-specified)
MACROS=[
 ('M_1075','특허절벽($3,200억·25-35)'),
 ('M_1076','美시밀러규제완화법(S.1954)'),
 ('M_1077','FDA PK임상간소화(Q&A 4차)'),
 ('M_1078','美약가인하(IRA)'),
 ('M_1079','美중국바이오견제(BIOSECURE)'),
]
# 7 chars: (key, 위상char, biz, eco, drv)
ITEMS=[
 ('Celltrion','코어·전주기직판 최대수혜','개발-생산-직판 전주기 시밀러','글로벌 매출 3위($42억·FDA 10개)','1)대체조제 마진포착 2)키트루다(CT-P51)·다잘렉스 3)면역항암 임상비 -25% 4)규제완화'),
 ('SamsungEpis','코어·오가논판매·파트너리스크','시밀러 개발(오가논 통한 판매)','FDA 9개·스텔라라·프롤리아·아일리아','1)키트루다 3상·ADC 2)오가논 마진희석 3)선파마 피인수 리스크 4)규제완화'),
 ('SamsungBio','간접·CDMO/BIOSECURE 수혜','CDMO(위탁생산)','글로벌 CDMO 1위급(별도 모테마)','1)BIOSECURE 디커플링 2)시밀러 증산 위탁 3)수주 4)증설'),
 ('Alteogen','Picks&Shovels·SC전환 인에이블러','SC 제형전환 플랫폼(ALT-B4)','글로벌 기술이전(MSD 등)','1)SC전환 메가트렌드 2)기술이전 마일스톤 3)히알루로니다제 4)로열티'),
 ('Sandoz','코어·글로벌1위 순수시밀러','순수 바이오시밀러 글로벌 1위','시판 13개+파이프라인 32개','1)황금의10년 선점 2)인터체인저블 선구 3)면역질환 4)약가'),
 ('Amgen','하이브리드·오리지널+시밀러','오리지널 바이오+시밀러(Wezlana 등)','레거시 브랜드+고성장 시밀러','1)시밀러 고성장 2)자기품목 잠식 3)비만약(마리타이드) 4)파이프라인'),
 ('OGN','이벤트·선파마 피인수 M&A','여성건강+바이오시밀러','삼성에피스 시밀러 美판매 파트너','1)선파마 120억달러 인수 2)시밀러 톱10 진입 3)판매망 4)부채'),
]
EVENTS=[
 ["휴미라 특허절벽·시밀러 출시","2023.01~2023.12","📈 수혜","휴미라 美 바이오시밀러 출시 → 자가면역 시밀러 본격 경쟁·시장 개화","12개월","—"],
 ["IRA 약가협상 입법","2022.08~2023.12","⚠️ 진행중","IRA 약가협상 → 고가 바이오의약품 압박 / 시밀러 수요 촉진 양면","18개월","—"],
 ["FDA 인터체인저블 완화","2024.01~2024.12","📈 수혜","FDA 인터체인저블 지정 간소화 추진 → 약국 대체조제 가속","12개월","—"],
 ["BIOSECURE 중국 디커플링","2024.06~2025.06","📈 수혜","BIOSECURE法 → 중국 CDMO 디커플링, 삼바·서구 CDMO 반사수혜","12개월","—"],
 ["특허절벽 슈퍼사이클($3,200억)","2025.01~2025.12","📈 수혜 (강력)","키트루다·다잘렉스 등 대형 바이오 특허만료 임박 → 시밀러 슈퍼사이클","진행 중","—"],
 ["S.1954·PK임상 간소화","2025.01~2025.12","📈 수혜","규제완화법 S.1954·FDA PK임상 Q&A → 시밀러 개발비·기간 단축","진행 중","—"],
 ["글로벌 시밀러: 규제·약가 양면","2025.06~진행중","⚠️ 진행중","규제완화·특허절벽 구조적 성장 / 약가인하·경쟁 심화 양면","진행 중","미정 (규제·약가 의존)"],
]

charstore=[]; CC=[1553]
def nc(kr): cid=f"C_{CC[0]}"; CC[0]+=1; charstore.append((cid,kr)); return cid
period={"periodDefault":"3d","periodOptions":["1d","3d","7d","15d","1m","ytd","1y","2y","3y"],"dataAsOf":"2026-06-24T00:00:00+09:00"}
nodes=[{"id":TID,"type":"THEME","name":TNAME}]; edges=[]
for key,ch,biz,eco,drv in ITEMS:
    a=aid(key)
    nodes.append({"id":a,"type":"ASSET","name":ako(key),"exposure":aexp(key),"metrics":dict(METRICS)})
    edges.append({"from":a,"to":TID,"type":"THEMED_AS"})
    cid=nc(ch); nodes.append({"id":cid,"type":"CHARACTER","name":ch})
    edges.append({"from":a,"to":cid,"type":"HAS_TRAIT"})
for mid,mname in MACROS:
    nodes.append({"id":mid,"type":"MACRO","name":mname}); edges.append({"from":mid,"to":TID,"type":"IMPACTS"})
obj={"schemaVersion":"v5","themeId":TID,"themeName":TNAME,"meta":{**period,"description":"미국 바이오시밀러 규제완화(S.1954·PK임상 간소화)와 특허절벽·BIOSECURE 수혜 글로벌 바이오 밸류체인.","notes":f"{TID} 신규. 자산별 CHARACTER(테마내 위상), MACRO 5(신규)","sources":[]},"nodes":nodes,"edges":edges}
obj["links"]=obj["edges"]
json.dump(obj,open(f'theme/{TID}.json','w',encoding='utf-8'),ensure_ascii=False,indent=2)

EVHDR=("\n\n---\n\n## 이벤트 × 테마 DB (지난 5년 핵심 변동요인 7)\n\n| 이벤트명 | 시기 | 방향 | 핵심 메커니즘 | 지속기간 | 회복여부 |\n|---|---|---|---|---|---|\n")
lines=["| 종목 | 핵심 사업(테마 내 위상) | 사업생태계 | 주가 핵심 동인 |","| --- | --- | --- | --- |"]
for key,ch,biz,eco,drv in ITEMS: lines.append(f"| {link(key)} | {biz} | {eco} | {drv} |")
open(f'briefing/{TID}.md','w',encoding='utf-8').write("\n".join(lines)+EVHDR+''.join('| '+' | '.join(r)+' |\n' for r in EVENTS))

# register macros
mfields=csv.DictReader(open('ssot/macro_ssot.csv',encoding='utf-8')).fieldnames
mids={r['macro_id'] for r in csv.DictReader(open('ssot/macro_ssot.csv',encoding='utf-8'))}
with open('ssot/macro_ssot.csv','a',encoding='utf-8',newline='') as f:
    w=csv.writer(f)
    for mid,mname in MACROS:
        if mid not in mids: w.writerow([{'macro_id':mid,'macro_name_ko':mname,'macro_type':'POLICY'}.get(c,'') for c in mfields])
# register new asset
with open('ssot/asset_ssot.csv','a',encoding='utf-8',newline='') as f:
    w=csv.writer(f)
    for key,aid_,en,ko,tick,exch,ctry in NEW:
        row={'asset_id':aid_,'asset_name_en':en,'asset_name_ko':ko,'ticker':tick,'exchange':exch,'country':ctry,'asset_type':'STOCK'}
        w.writerow([row.get(c,'') for c in AFIELDS])
# register chars
with open('ssot/character_ssot.csv','a',encoding='utf-8',newline='') as f:
    w=csv.writer(f)
    for cid,kr in charstore: w.writerow([cid,kr,kr])
# index
idx=json.load(open('theme/index.json',encoding='utf-8'))
if TID not in {e['themeId'] for e in idx}: idx.append({"themeId":TID,"themeName":TNAME})
json.dump(idx,open('theme/index.json','w',encoding='utf-8'),ensure_ascii=False,indent=2)
print('theme',TID,'| assets',len(ITEMS),'| new asset',len(NEW),'| chars',len(charstore),'| macros',len(MACROS),'| index',len(idx))
PY="ok"
