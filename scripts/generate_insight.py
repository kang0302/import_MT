"""
generate_insight.py — 테마/자산 ID 입력받아 Claude API 로 인사이트 생성 → data/insights/{ID}.md 저장.

Used by: .github/workflows/generate-insight.yml (workflow_dispatch).

Env vars:
  ANTHROPIC_API_KEY: API key (GitHub secret).
  TARGET_ID:         T_xxx 또는 A_xxx (필수).
  EXTRA_CONTEXT:     추가 컨텍스트 텍스트 (선택).
  MODEL:             claude-sonnet-4-5 | claude-opus-4-7 (기본 sonnet).

Output:
  data/insights/{TARGET_ID}.md  — frontmatter (title·updated_at·tags) + 본문 markdown.
"""
import csv
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Constants
ROOT = Path(__file__).resolve().parent.parent
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
KST = timezone(timedelta(hours=9))


def today_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def load_theme_context(theme_id: str) -> dict:
    """Load theme JSON, briefing (if exists), and last 5 commits touching the theme."""
    ctx = {"id": theme_id, "kind": "theme"}
    theme_path = ROOT / "data" / "theme" / f"{theme_id}.json"
    if not theme_path.exists():
        return {"error": f"테마 JSON 미존재: {theme_path}"}
    with open(theme_path, "r", encoding="utf-8") as f:
        d = json.load(f)
    ctx["name"] = d.get("themeName", "")
    ctx["description"] = d.get("meta", {}).get("description", "")
    # 노드 요약
    nodes_by_type = {}
    for n in d.get("nodes", []):
        t = n.get("type", "?")
        nodes_by_type.setdefault(t, []).append(
            {"id": n.get("id"), "name": n.get("name")}
        )
    ctx["nodes_by_type"] = nodes_by_type
    ctx["edge_count"] = len(d.get("edges", []))

    # 브리핑 (있으면)
    briefing_path = ROOT / "data" / "briefing" / f"{theme_id}.md"
    if briefing_path.exists():
        ctx["briefing"] = briefing_path.read_text(encoding="utf-8")[:4000]
    return ctx


def load_asset_context(asset_id: str) -> dict:
    """Load SSOT row + themes that include this asset."""
    ctx = {"id": asset_id, "kind": "asset"}
    asset_csv = ROOT / "data" / "ssot" / "asset_ssot.csv"
    with open(asset_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("asset_id") == asset_id:
                ctx["name_ko"] = row.get("asset_name_ko", "")
                ctx["name_en"] = row.get("asset_name_en", "")
                ctx["ticker"] = row.get("ticker", "")
                ctx["exchange"] = row.get("exchange", "")
                ctx["country"] = row.get("country", "")
                ctx["asset_type"] = row.get("asset_type", "")
                break
    if "name_ko" not in ctx:
        return {"error": f"자산 미존재: {asset_id}"}

    # 어떤 테마에 포함되는지 스캔
    themes = []
    theme_dir = ROOT / "data" / "theme"
    for theme_file in sorted(theme_dir.glob("T_*.json")):
        try:
            with open(theme_file, "r", encoding="utf-8") as f:
                d = json.load(f)
            for n in d.get("nodes", []):
                if n.get("id") == asset_id:
                    themes.append({"id": d.get("themeId"), "name": d.get("themeName")})
                    break
        except Exception:
            continue
    ctx["themes_included"] = themes[:20]  # cap
    return ctx


def build_prompt(ctx: dict, extra: str) -> str:
    target_id = ctx["id"]
    kind = ctx.get("kind", "theme")
    extra_block = f"\n[추가 컨텍스트]\n{extra}\n" if extra else ""

    if kind == "theme":
        ctx_block = f"""[테마 정보]
- ID: {target_id}
- 이름: {ctx.get('name', '')}
- 설명: {ctx.get('description', '')}
- 구성 노드 (타입별):
{json.dumps(ctx.get('nodes_by_type', {}), ensure_ascii=False, indent=2)}
- 엣지 수: {ctx.get('edge_count')}

[브리핑 (참고용)]
{ctx.get('briefing', '(브리핑 파일 없음)')[:3000]}
"""
    else:
        themes_str = "\n".join(
            f"  - {t['id']}: {t['name']}" for t in ctx.get("themes_included", [])
        )
        ctx_block = f"""[자산 정보]
- ID: {target_id}
- 한글명: {ctx.get('name_ko')}
- 영문명: {ctx.get('name_en')}
- 티커: {ctx.get('ticker')} ({ctx.get('exchange')}/{ctx.get('country')})
- 타입: {ctx.get('asset_type')}

[포함된 테마 ({len(ctx.get('themes_included', []))}개)]
{themes_str or '(없음)'}
"""

    return f"""당신은 머니트리 투자 온톨로지 분석 전문가입니다. 아래 정보를 기반으로 한국어 투자 인사이트를 작성하세요.

{ctx_block}{extra_block}

[출력 규칙]
1. 반드시 다음 frontmatter 로 시작:
```
---
title: (한 줄 요약 제목, 30자 이내)
updated_at: {today_kst()}
tags: [3-5개 키워드 한글/영문]
---
```

2. 본문 구조:
- `## 핵심 포인트` — 글머리표 3-5개, 각 한 줄
- `## 상세 분석` — 2-4 문단, actionable 내용 중심
- `## 결론` — 매수/관망/매도 명확히 + 핵심 리스크 1-2개

3. 분량: 600-1200자 (본문 기준, frontmatter 제외)
4. 사실 기반 분석. 추측 시 "추정"/"가능성" 표기.
5. 한국 투자자 관점 (KRW·KOSPI 거래 시간 고려).
6. **마크다운 코드블록·설명·인사 문구 없이 frontmatter 부터 바로 본문 시작.**

지금 작성하세요:
"""


def call_anthropic(prompt: str, model: str, api_key: str) -> str:
    payload = {
        "model": model,
        "max_tokens": 4000,
        "messages": [{"role": "user", "content": prompt}],
    }
    req = urllib.request.Request(
        ANTHROPIC_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        sys.exit(f"❌ Anthropic API 오류 {e.code}: {body}")
    text = "".join(
        c.get("text", "") for c in data.get("content", []) if c.get("type") == "text"
    )
    # 코드블록 마커 제거 (가끔 ```markdown 으로 감쌈)
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # 첫 ``` 다음줄부터 마지막 ``` 이전까지
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    return text.strip()


def main():
    target_id = os.environ.get("TARGET_ID", "").strip()
    if not target_id:
        sys.exit("❌ TARGET_ID 환경변수 필수")
    if not (target_id.startswith("T_") or target_id.startswith("A_")):
        sys.exit(f"❌ TARGET_ID 형식 오류: {target_id} (T_xxx 또는 A_xxx)")

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        sys.exit("❌ ANTHROPIC_API_KEY 환경변수 필수 (GitHub secret 확인)")

    model = os.environ.get("MODEL", "claude-sonnet-4-5").strip()
    extra = os.environ.get("EXTRA_CONTEXT", "").strip()

    print(f"🔍 컨텍스트 수집: {target_id}", file=sys.stderr)
    if target_id.startswith("T_"):
        ctx = load_theme_context(target_id)
    else:
        ctx = load_asset_context(target_id)
    if "error" in ctx:
        sys.exit(f"❌ {ctx['error']}")

    print(f"🤖 Anthropic API 호출 ({model})", file=sys.stderr)
    prompt = build_prompt(ctx, extra)
    markdown = call_anthropic(prompt, model, api_key)

    out_path = ROOT / "data" / "insights" / f"{target_id}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(markdown, encoding="utf-8")
    print(f"✅ 저장 완료: {out_path}", file=sys.stderr)
    print(str(out_path))  # stdout: 경로 (workflow 가 사용)


if __name__ == "__main__":
    main()
