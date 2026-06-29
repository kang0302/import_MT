"""
propose_edges.py — AI 인제스트 계약(#3): LLM이 '근거 부착 엣지'만 제안하게 강제.

핵심 원칙:
  - LLM은 theme JSON을 직접 수정하지 않는다. 오직 제안(EdgeProposal)만 staging 파일로 emit.
  - 출처 포인터(매체·URL·일자)는 *운영자(사람)*가 SOURCE_* 로 공급한다 → LLM이 출처를 지어낼 수 없음.
  - LLM은 (a) 제공된 출처 텍스트에서 근거 문장(quote)을 그대로 발췌, (b) 컨텍스트에 제시된 기존 노드 ID로
    from/to 매핑, (c) 관계 type·confidence 만 결정. tool-use(structured output)로 형식을 강제.
  - quote/from/to/type/confidence 가 없으면 애초에 제안이 성립하지 않음(스키마 required).

게이트는 apply_edge_proposals.py 가 담당(근거 검증 후에만 반영, status=proposed).

Env vars:
  ANTHROPIC_API_KEY  (필수)
  THEME_ID           대상 테마 T_xxx (필수) — 노드 컨텍스트 제공
  SOURCE_FILE        출처 텍스트 파일 경로 (SOURCE_TEXT 와 택1)
  SOURCE_TEXT        출처 텍스트 직접 입력 (택1)
  SOURCE_PUBLISHER   출처 매체/주체 (필수) — 예: "한경 와우넷 6/12 방송"
  SOURCE_URL         1차 출처 URL (URL 또는 PUBLISHED 중 최소 1)
  SOURCE_PUBLISHED   발행 일자/시기 (예: 2026-06-12)
  SOURCE_KIND        broadcast|article|filing|company_disclosure|public_report|manual (기본 article)
  MODEL              기본 claude-sonnet-4-5

Output:
  data/staging/edge_proposals.jsonl  (append) — apply_edge_proposals.py 입력
"""
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
KST = timezone(timedelta(hours=9))
ALLOWED_TYPES = ["THEMED_AS", "IMPACTS", "HAS_TRAIT", "OPERATES", "SUPPLIES", "EXPOSED_TO", "IN_ETF"]
EVIDENCE_KINDS = ["broadcast", "article", "filing", "company_disclosure", "public_report", "manual"]

TOOL = {
    "name": "emit_edge_proposals",
    "description": "제공된 출처 텍스트에서 근거가 확인되는 관계(엣지)만 제안한다. 텍스트에 근거가 없으면 제안하지 않는다(빈 배열 허용).",
    "input_schema": {
        "type": "object",
        "properties": {
            "proposals": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "from": {"type": "string", "description": "출발 노드 ID — 반드시 컨텍스트에 제시된 기존 ID(A_/M_/BF_/T_) 중 하나"},
                        "to": {"type": "string", "description": "도착 노드 ID — 반드시 컨텍스트에 제시된 기존 ID 중 하나"},
                        "type": {"type": "string", "enum": ALLOWED_TYPES},
                        "confidence": {"type": "number", "description": "0.0~1.0. 텍스트 단정도에 비례. 추정/보도면 낮게."},
                        "quote": {"type": "string", "description": "제공된 출처 텍스트에서 '그대로' 발췌한 근거 문장. 창작 금지."},
                        "rationale": {"type": "string", "description": "왜 이 엣지인지 한 줄(선택)"},
                    },
                    "required": ["from", "to", "type", "confidence", "quote"],
                },
            }
        },
        "required": ["proposals"],
    },
}


def load_theme_nodes(theme_id: str):
    p = ROOT / "data" / "theme" / f"{theme_id}.json"
    if not p.exists():
        sys.exit(f"❌ 테마 JSON 미존재: {p}")
    d = json.loads(p.read_text(encoding="utf-8"))
    nodes = [{"id": n.get("id"), "type": n.get("type"), "name": n.get("name")} for n in d.get("nodes", [])]
    return d.get("themeName", theme_id), nodes


def build_prompt(theme_id, theme_name, nodes, source_text):
    node_lines = "\n".join(f"  - {n['id']} [{n['type']}] {n['name']}" for n in nodes)
    return f"""당신은 머니트리 투자 온톨로지의 관계 추출기입니다.
대상 테마: {theme_id} {theme_name}

[이 테마에 존재하는 노드 — from/to 는 반드시 아래 ID 중에서만 선택]
{node_lines}

[허용 관계 type]
{", ".join(ALLOWED_TYPES)}

[출처 텍스트 — 근거는 오직 이 안에서만]
{source_text}

[지시]
- 위 출처 텍스트에서 '명시적으로 확인되는' 관계만 emit_edge_proposals 로 제안하세요.
- from/to 는 위 노드 목록의 ID 중에서만 고르세요(새 노드 생성 금지). 목록에 없으면 제안하지 마세요.
- quote 는 출처 텍스트에서 그대로 발췌하세요. 요약·창작·외부지식 사용 금지.
- 텍스트로 뒷받침되지 않으면 제안하지 마세요. 근거 없는 관계는 절대 만들지 마세요(빈 배열도 정상).
- 반드시 emit_edge_proposals 도구로만 응답하세요."""


def call_anthropic(prompt, model, api_key):
    payload = {
        "model": model,
        "max_tokens": 4000,
        "tools": [TOOL],
        "tool_choice": {"type": "tool", "name": "emit_edge_proposals"},
        "messages": [{"role": "user", "content": prompt}],
    }
    req = urllib.request.Request(
        ANTHROPIC_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "x-api-key": api_key, "anthropic-version": "2023-06-01"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        sys.exit(f"❌ Anthropic API 오류 {e.code}: {e.read().decode('utf-8', errors='replace')}")
    for c in data.get("content", []):
        if c.get("type") == "tool_use" and c.get("name") == "emit_edge_proposals":
            return c.get("input", {}).get("proposals", [])
    sys.exit("❌ 모델이 tool_use 를 반환하지 않음")


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        sys.exit("❌ ANTHROPIC_API_KEY 필수")
    theme_id = os.environ.get("THEME_ID", "").strip()
    if not theme_id.startswith("T_"):
        sys.exit("❌ THEME_ID (T_xxx) 필수")
    publisher = os.environ.get("SOURCE_PUBLISHER", "").strip()
    if not publisher:
        sys.exit("❌ SOURCE_PUBLISHER 필수 (출처 매체/주체는 운영자가 공급)")
    url = os.environ.get("SOURCE_URL", "").strip()
    published = os.environ.get("SOURCE_PUBLISHED", "").strip()
    if not (url or published):
        sys.exit("❌ SOURCE_URL 또는 SOURCE_PUBLISHED 중 최소 하나 필요")
    kind = os.environ.get("SOURCE_KIND", "article").strip()
    if kind not in EVIDENCE_KINDS:
        sys.exit(f"❌ SOURCE_KIND 허용값: {EVIDENCE_KINDS}")
    model = os.environ.get("MODEL", "claude-sonnet-4-5").strip()

    src_text = os.environ.get("SOURCE_TEXT", "").strip()
    src_file = os.environ.get("SOURCE_FILE", "").strip()
    if src_file:
        src_text = Path(src_file).read_text(encoding="utf-8").strip()
    if not src_text:
        sys.exit("❌ SOURCE_TEXT 또는 SOURCE_FILE 필요")

    theme_name, nodes = load_theme_nodes(theme_id)
    print(f"🤖 관계 추출 ({model}) — {theme_id} {theme_name}, 노드 {len(nodes)}개", file=sys.stderr)
    proposals = call_anthropic(build_prompt(theme_id, theme_name, nodes, src_text), model, api_key)

    captured = datetime.now(KST).isoformat()
    src = {"publisher": publisher, "url": url or None, "published": published or None, "kind": kind}
    out_dir = ROOT / "data" / "staging"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "edge_proposals.jsonl"
    n = 0
    with open(out_path, "a", encoding="utf-8") as f:
        for p in proposals:
            rec = {
                "theme": theme_id,
                "from": p.get("from"),
                "to": p.get("to"),
                "type": p.get("type"),
                "confidence": p.get("confidence"),
                "quote": p.get("quote"),
                "rationale": p.get("rationale", ""),
                "source": src,
                "captured": captured,
                "captured_by": f"llm:{model}",
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    print(f"✅ 제안 {n}건 → {out_path}", file=sys.stderr)
    print(f"다음: python scripts/apply_edge_proposals.py --dry-run  (검증)  →  --apply  (반영)", file=sys.stderr)


if __name__ == "__main__":
    main()
