# -*- coding: utf-8 -*-
"""
출처(Provenance) 검증기 — MONEYTREE 엣지 근거 추적.

검사 항목:
  1) evidence_ssot.jsonl 레코드 필수필드 + (url|published) 택1
  2) 비-legacy 엣지: evidence(≥1, 저장소 해석가능) / confidence 0~1 / status 값
  3) edges == links 일관성
출력: 위반 목록 + 테마별 근거 커버리지%. 위반 시 exit 1.

스키마 상세: data/ssot/PROVENANCE.md
사용: python scripts/validate_provenance.py   (repo 루트 또는 data 상위에서)
"""
import io, sys, os, json, glob

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# 리포지토리 내 data 경로 자동 탐색
HERE = os.path.dirname(os.path.abspath(__file__))
CANDS = [os.path.join(HERE, "..", "data"), os.path.join(os.getcwd(), "data"), os.getcwd()]
DATA = next((p for p in CANDS if os.path.isdir(os.path.join(p, "theme"))), None)
if not DATA:
    print("ERROR: data/theme 디렉터리를 찾을 수 없음"); sys.exit(2)

EVIDENCE_KINDS = {"broadcast", "article", "filing", "company_disclosure", "public_report", "manual"}
EDGE_STATUS = {"verified", "proposed", "legacy"}
REQUIRED_EV = ["evidence_id", "kind", "publisher", "quote", "captured", "captured_by"]

errors = []
warnings = []


def load_evidence():
    path = os.path.join(DATA, "ssot", "evidence_ssot.jsonl")
    store = {}
    if not os.path.exists(path):
        warnings.append("evidence_ssot.jsonl 없음 — 출처 저장소 비어있음(비-legacy 엣지 검증 불가).")
        return store
    for i, line in enumerate(open(path, encoding="utf-8"), 1):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception as e:
            errors.append(f"evidence_ssot.jsonl L{i}: JSON 파싱 실패 ({e})")
            continue
        eid = r.get("evidence_id")
        for f in REQUIRED_EV:
            if not r.get(f):
                errors.append(f"evidence {eid or f'L{i}'}: 필수필드 '{f}' 누락")
        if r.get("kind") and r["kind"] not in EVIDENCE_KINDS:
            errors.append(f"evidence {eid}: kind '{r['kind']}' 허용값 아님 {sorted(EVIDENCE_KINDS)}")
        if not (r.get("url") or r.get("published")):
            errors.append(f"evidence {eid}: url 또는 published 중 최소 하나 필요")
        if eid:
            if eid in store:
                errors.append(f"evidence {eid}: 중복 ID")
            store[eid] = r
    return store


def check_edge(tid, arr_name, e, store):
    desc = f"{tid}/{arr_name} {e.get('from')}->{e.get('to')}({e.get('type')})"
    has_ev = "evidence" in e
    status = e.get("status", "legacy" if not has_ev else None)
    attributed = has_ev or e.get("status") in ("verified", "proposed")
    if not attributed:
        # legacy: provenance 필드가 단독으로 붙어선 안 됨
        if "confidence" in e or e.get("status") not in (None, "legacy"):
            errors.append(f"{desc}: evidence 없이 confidence/status만 존재(legacy 규칙 위반)")
        return False  # not attributed → coverage 미포함
    # attributed edge 검증
    if status not in ("verified", "proposed"):
        errors.append(f"{desc}: status는 verified|proposed 여야 함(현재 {status!r})")
    ev = e.get("evidence")
    if not isinstance(ev, list) or len(ev) < 1:
        errors.append(f"{desc}: evidence는 비어있지 않은 배열이어야 함")
    else:
        for eid in ev:
            if eid not in store:
                errors.append(f"{desc}: evidence_id '{eid}' 저장소에 없음")
    c = e.get("confidence")
    if not isinstance(c, (int, float)) or not (0.0 <= float(c) <= 1.0):
        errors.append(f"{desc}: confidence 0~1 실수 필요(현재 {c!r})")
    return True


def main():
    store = load_evidence()
    theme_files = sorted(glob.glob(os.path.join(DATA, "theme", "T_*.json")))
    total_edges = total_attr = 0
    per_theme = []
    for tf in theme_files:
        tid = os.path.splitext(os.path.basename(tf))[0]
        try:
            o = json.load(open(tf, encoding="utf-8"))
        except Exception as e:
            errors.append(f"{tid}: JSON 파싱 실패 ({e})"); continue
        edges = o.get("edges", [])
        links = o.get("links", [])
        if edges != links:
            errors.append(f"{tid}: edges != links")
        n = len(edges); attr = 0
        for e in edges:
            if check_edge(tid, "edges", e, store):
                attr += 1
        total_edges += n; total_attr += attr
        if attr > 0:
            per_theme.append((tid, attr, n))

    print("=" * 60)
    print(f"출처 저장소: evidence {len(store)}건")
    print(f"테마 {len(theme_files)}개 | 엣지 {total_edges}개 | 근거부착(비-legacy) {total_attr}개")
    cov = (100.0 * total_attr / total_edges) if total_edges else 0.0
    print(f"전체 근거 커버리지: {cov:.2f}%")
    if per_theme:
        print("-- 근거 부착된 테마 --")
        for tid, a, n in per_theme:
            print(f"   {tid}: {a}/{n} ({100.0*a/n:.0f}%)")
    if warnings:
        print("-- 경고 --")
        for w in warnings:
            print("   ! " + w)
    print("=" * 60)
    if errors:
        print(f"검증 실패: 위반 {len(errors)}건")
        for er in errors:
            print("   ✗ " + er)
        sys.exit(1)
    print("검증 통과: 위반 없음")
    sys.exit(0)


if __name__ == "__main__":
    main()
