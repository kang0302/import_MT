"""
apply_edge_proposals.py — AI 인제스트 게이트(#3): 근거 없는 엣지는 절대 반영하지 않는다.

입력: data/staging/edge_proposals.jsonl  (propose_edges.py 산출 또는 수기 작성)
동작:
  1) 각 제안 검증:
     - theme JSON 존재, from/to 가 그 테마의 기존 노드 ID 로 해석됨(새 노드 생성 안 함 — 거부)
     - type 허용값, confidence 0~1
     - 근거: quote 필수 + source.publisher 필수 + (source.url 또는 source.published) 필수
  2) 통과분만:
     - evidence_ssot.jsonl 에 evidence 레코드 추가(중복 publisher+url+quote 는 재사용), EV_xxxxxx 발급
     - theme JSON 의 edges+links 에 엣지 추가(동일 from/to/type 있으면 근거로 보강), status=proposed
       (사람 검수 후 status 를 verified 로 승격 — 별도 수기/도구)
  3) 거부분: data/staging/edge_proposals.rejected.jsonl 로 사유와 함께 기록.
모드:
  --dry-run  : 검증·요약만, 파일 변경 없음 (기본)
  --apply    : 실제 반영
  --status verified : (사람 검수 완료 시) 반영 status 를 verified 로 (기본 proposed)
반영 후 권장: python scripts/validate_provenance.py
"""
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
KST = timezone(timedelta(hours=9))
ALLOWED_TYPES = {"THEMED_AS", "IMPACTS", "HAS_TRAIT", "OPERATES", "SUPPLIES", "EXPOSED_TO", "IN_ETF"}
EVIDENCE_KINDS = {"broadcast", "article", "filing", "company_disclosure", "public_report", "manual"}
STAGING = ROOT / "data" / "staging" / "edge_proposals.jsonl"
REJECTED = ROOT / "data" / "staging" / "edge_proposals.rejected.jsonl"
EVIDENCE = ROOT / "data" / "ssot" / "evidence_ssot.jsonl"


def load_evidence():
    recs = []
    if EVIDENCE.exists():
        for line in EVIDENCE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                recs.append(json.loads(line))
    return recs


def next_ev_id(recs):
    mx = 0
    for r in recs:
        eid = r.get("evidence_id", "")
        if eid.startswith("EV_") and eid[3:].isdigit():
            mx = max(mx, int(eid[3:]))
    return mx


def validate(p):
    errs = []
    tid = p.get("theme", "")
    if not tid.startswith("T_"):
        errs.append("theme 누락/형식오류")
        return errs, None
    tpath = ROOT / "data" / "theme" / f"{tid}.json"
    if not tpath.exists():
        errs.append(f"테마 JSON 미존재 {tid}")
        return errs, None
    theme = json.loads(tpath.read_text(encoding="utf-8"))
    node_ids = {n.get("id") for n in theme.get("nodes", [])}
    if p.get("from") not in node_ids:
        errs.append(f"from '{p.get('from')}' 가 {tid} 노드에 없음(새 노드 생성 금지)")
    if p.get("to") not in node_ids:
        errs.append(f"to '{p.get('to')}' 가 {tid} 노드에 없음(새 노드 생성 금지)")
    if p.get("type") not in ALLOWED_TYPES:
        errs.append(f"type '{p.get('type')}' 허용값 아님")
    c = p.get("confidence")
    if not isinstance(c, (int, float)) or not (0.0 <= float(c) <= 1.0):
        errs.append(f"confidence 0~1 아님 ({c!r})")
    q = (p.get("quote") or "").strip()
    if not q:
        errs.append("quote(근거 문장) 누락 — 근거 없는 엣지 거부")
    src = p.get("source") or {}
    if not (src.get("publisher") or "").strip():
        errs.append("source.publisher 누락")
    if not ((src.get("url") or "").strip() or (src.get("published") or "").strip()):
        errs.append("source.url/published 중 최소 하나 필요")
    if (src.get("kind") or "article") not in EVIDENCE_KINDS:
        errs.append(f"source.kind 허용값 아님 ({src.get('kind')})")
    return errs, theme


def main():
    args = set(sys.argv[1:])
    apply = "--apply" in args
    status = "verified" if "--status" in args and "verified" in args else "proposed"
    if not apply:
        print("※ DRY-RUN (검증만, 변경 없음). 실제 반영은 --apply")

    if not STAGING.exists():
        sys.exit(f"❌ staging 없음: {STAGING}")
    proposals = [json.loads(l) for l in STAGING.read_text(encoding="utf-8").splitlines() if l.strip()]
    if not proposals:
        sys.exit("제안 0건")

    ev_recs = load_evidence()
    ev_counter = next_ev_id(ev_recs)
    # 기존 evidence 재사용 인덱스: (publisher,url,quote) -> evidence_id
    ev_index = {(r.get("publisher"), r.get("url"), r.get("quote")): r.get("evidence_id") for r in ev_recs}
    captured_now = datetime.now(KST).isoformat()

    accepted, rejected = [], []
    new_ev = []           # 새 evidence 레코드
    theme_edits = {}      # tid -> theme dict (메모리 수정)

    for p in proposals:
        errs, theme = validate(p)
        if errs:
            rejected.append({"proposal": p, "reasons": errs})
            continue
        tid = p["theme"]
        theme = theme_edits.get(tid, theme)
        src = p["source"]
        key = (src.get("publisher"), src.get("url") or None, p["quote"])
        eid = ev_index.get(key)
        if not eid:
            ev_counter += 1
            eid = f"EV_{ev_counter:06d}"
            ev_index[key] = eid
            rec = {
                "evidence_id": eid,
                "kind": src.get("kind", "article"),
                "publisher": src.get("publisher"),
                "url": src.get("url") or None,
                "published": src.get("published") or None,
                "quote": p["quote"],
                "captured": p.get("captured", captured_now),
                "captured_by": p.get("captured_by", "unknown"),
                "reviewed_by": "human" if status == "verified" else None,
            }
            new_ev.append(rec)
        # 엣지 추가/보강
        e_new = {"from": p["from"], "to": p["to"], "type": p["type"],
                 "evidence": [eid], "confidence": float(p["confidence"]), "status": status}

        def upsert(arr):
            for e in arr:
                if e.get("from") == e_new["from"] and e.get("to") == e_new["to"] and e.get("type") == e_new["type"]:
                    evset = list(dict.fromkeys((e.get("evidence") or []) + [eid]))
                    e["evidence"] = evset
                    e["confidence"] = float(p["confidence"])
                    e["status"] = status
                    return "강화"
            arr.append(dict(e_new))
            return "추가"

        act = upsert(theme.setdefault("edges", []))
        upsert(theme.setdefault("links", []))
        theme_edits[tid] = theme
        accepted.append({"proposal": p, "evidence_id": eid, "action": act, "status": status})

    # 요약 출력
    print("=" * 60)
    print(f"제안 {len(proposals)} | 통과 {len(accepted)} | 거부 {len(rejected)} | 신규 evidence {len(new_ev)} | status={status}")
    for a in accepted:
        pp = a["proposal"]
        print(f"  ✓ {pp['theme']} {pp['from']}->{pp['to']}({pp['type']}) [{a['action']}] {a['evidence_id']} conf={pp['confidence']}")
    for r in rejected:
        pp = r["proposal"]
        print(f"  ✗ {pp.get('theme')} {pp.get('from')}->{pp.get('to')}({pp.get('type')}): {'; '.join(r['reasons'])}")
    print("=" * 60)

    if not apply:
        print("DRY-RUN 종료 — 변경 없음. 반영하려면 --apply")
        # 거부분은 dry-run 에서도 참고용으로 남기지 않음(읽기전용)
        sys.exit(0)

    # 실제 반영
    for tid, theme in theme_edits.items():
        (ROOT / "data" / "theme" / f"{tid}.json").write_text(
            json.dumps(theme, ensure_ascii=False, indent=2), encoding="utf-8")
    if new_ev:
        with open(EVIDENCE, "a", encoding="utf-8") as f:
            for r in new_ev:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    if rejected:
        with open(REJECTED, "a", encoding="utf-8") as f:
            for r in rejected:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    # 처리한 staging 비우기(재반영 방지)
    STAGING.write_text("", encoding="utf-8")
    print(f"✅ 반영 완료. 테마 {len(theme_edits)}개, evidence +{len(new_ev)}. 거부 {len(rejected)}건은 {REJECTED.name} 기록.")
    print("권장: python scripts/validate_provenance.py")


if __name__ == "__main__":
    main()
