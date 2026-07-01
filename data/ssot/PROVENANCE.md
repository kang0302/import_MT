# 출처(Provenance) 스키마 — 엣지 근거 추적

MONEYTREE의 모든 관계(엣지)에 "근거"를 부착하기 위한 스키마. 목표: 지도 위 모든 연결선이
"왜 이렇게 연결했는가(어떤 출처·언제)"에 답할 수 있게 → 신뢰·재현성 확보, 그리고 AI 인제스트의
하드 게이트(근거 없으면 엣지 없음).

저장 모델: **중앙 저장소 + 참조(refs)**. 출처 원본은 `evidence_ssot.jsonl`에 1건당 1레코드로 한 번만
저장하고, 엣지는 `evidence_id`로 참조한다. 하나의 방송/기사가 여러 엣지를 정당화할 때 중복을 막고,
감사(audit) 지점을 한 곳으로 모은다.

## 1. 중앙 출처 저장소 — `data/ssot/evidence_ssot.jsonl`

JSONL(한 줄당 1 JSON). 레코드 스키마:

| 필드 | 필수 | 설명 |
|---|---|---|
| `evidence_id` | ✅ | `EV_` + 6자리 숫자. 전역 고유. |
| `kind` | ✅ | `broadcast` \| `article` \| `filing` \| `company_disclosure` \| `public_report` \| `manual` |
| `publisher` | ✅ | 출처 주체(방송사·매체·기업 IR 등). |
| `quote` | ✅ | 근거가 되는 원문/요지 인용(한 문장 이상). |
| `captured` | ✅ | 수집 시각(ISO8601). |
| `captured_by` | ✅ | `manual-seed` \| `human:<id>` \| `llm:<model>` 등. |
| `url` 또는 `published` | ✅(택1) | 둘 중 최소 하나. `url`=1차 출처 링크, `published`=발행 일자/시기. |
| `reviewed_by` | – | 사람 검수자(있으면). |
| `note` | – | 비고(시드/한계 등). |
| `source_type` | – | 출처 유형(사람친화 한글): `공시`\|`IR`\|`뉴스`\|`산업리포트`\|`판단`. `kind`의 사람용 라벨(공시→filing, IR→company_disclosure, 뉴스→article, 산업리포트→public_report, 판단→manual). |
| `source_ref` | – | 문서명·페이지 등 사람이 읽는 출처 식별자(예: "SEC 424B3 (CIK 1327068) FY2026"). `url` 보완. |
| `as_of` | – | 근거 시점(관계는 시간에 따라 변함 — backfill의 핵심). 보통 `published`와 동일. |
| `conf_grade` | – | `H`\|`M`\|`L`. 엣지 `confidence`(0~1)의 등급 요약(H≈0.9·M≈0.7·L≈0.5). SUPPLIES/IN_ETF/OPERATES=사실기반→H 용이, THEMED_AS/IMPACTS/EXPOSED_TO=해석→M/L 명시. |

> **BACKFILL 정책(2026-07-01):** 사실기반 관계(SUPPLIES·IN_ETF·OPERATES)부터 `source_type=공시/IR` + `source_ref`(1차 출처) + `as_of` + `conf_grade=H`로 채운 뒤, 해석적 관계(THEMED_AS·IMPACTS·EXPOSED_TO)로 확장. 파일럿: T_001 USO THEMED_AS → SEC 424B3 공시 기반(EV_000377, H). manual-seed는 in-place 업그레이드(엣지 ref·id 유지).

## 2. 엣지 확장 필드 (`data/theme/*.json`의 `edges`/`links`)

기존 엣지는 `{from, to, type}`. 아래 3필드를 **선택적**으로 추가(하위호환):

```json
{ "from":"A_217", "to":"T_257", "type":"THEMED_AS",
  "evidence":["EV_000001"], "confidence":0.9, "status":"verified" }
```

| 필드 | 설명 |
|---|---|
| `evidence` | `evidence_id` 배열(≥1). |
| `confidence` | 0.0~1.0. |
| `status` | `verified`(검증) \| `proposed`(AI/미검수 제안) \| `legacy`(근거 미기록). |

규칙:
- `evidence` 키가 **있으면** → `status`는 `verified` 또는 `proposed`, `confidence`는 0~1 필수, 모든
  `evidence_id`가 저장소에 존재해야 함.
- `evidence` 키가 **없으면** → 그 엣지는 `legacy`(근거 미기록)로 간주. 기존 557개 테마의 모든 엣지가
  여기 해당하므로 **레트로핏 없이 즉시 도입 가능**. UI는 "출처 미기록(legacy)"으로 표기해 백필을 유도.
- `edges`와 `links`는 동일하게 유지(빌드 freeze 규칙).

## 3. 검증기 — `scripts/validate_provenance.py`

CI/커밋 흐름에서 실행. 검사: (a) 저장소 레코드 필수필드, (b) 비-legacy 엣지의 evidence 해석 가능 여부·
confidence 범위·status 값, (c) edges==links 일관성. 또한 테마별 **근거 커버리지%**(비-legacy/전체)를 출력 —
BAROMETER처럼 품질 지표로 노출 가능. 위반 시 종료코드 1.

## 4. AI 인제스트 계약 — 근거 없는 엣지는 시스템에 못 들어온다

LLM은 theme JSON을 **직접 수정하지 않는다.** 오직 `EdgeProposal`을 staging으로 emit하고, 게이트가
검증을 통과한 것만 반영한다. 근거 없는 제안은 게이트가 거부 → 출처(#3)가 AI 가드레일(#2)을 자동 강제.

**무결성 핵심 — 출처 포인터는 사람이 공급한다.** 매체·URL·일자(`SOURCE_*`)는 운영자가 입력하고,
LLM은 (a) 제공된 출처 텍스트에서 근거 문장(`quote`)을 **그대로 발췌**, (b) 컨텍스트에 제시된 **기존
노드 ID**로 from/to 매핑, (c) 관계 type·confidence만 결정. → LLM이 출처를 지어낼 수 없다. 반영 시
`status=proposed`(captured_by=`llm:<model>`)로 들어가고, 사람 검수 후 `verified`로 승격.

**도구**
- `scripts/propose_edges.py` — Claude tool-use(structured output)로 EdgeProposal[] 강제 생성.
  필수 env: `ANTHROPIC_API_KEY` `THEME_ID` `SOURCE_PUBLISHER` `SOURCE_TEXT|SOURCE_FILE` +
  (`SOURCE_URL` 또는 `SOURCE_PUBLISHED`). 산출: `data/staging/edge_proposals.jsonl`.
- `scripts/apply_edge_proposals.py` — **게이트**. 검증: 테마 존재 / from·to가 그 테마의 기존 노드 /
  type 허용값 / confidence 0~1 / `quote`+`publisher`+(`url`|`published`) 필수. 통과분만
  evidence_ssot에 EV 발급·엣지 추가(status=proposed). 거부분은 `edge_proposals.rejected.jsonl`로
  사유 기록. 모드: `--dry-run`(기본, 변경 없음) / `--apply` / `--apply --status verified`(검수완료).

**파이프라인**
```
ANTHROPIC_API_KEY=... THEME_ID=T_257 SOURCE_PUBLISHER="한경 와우넷 6/12" \
  SOURCE_PUBLISHED=2026-06-12 SOURCE_FILE=article.txt python scripts/propose_edges.py
python scripts/apply_edge_proposals.py --dry-run      # 게이트 검증·요약
python scripts/apply_edge_proposals.py --apply        # 통과분만 반영(proposed)
python scripts/validate_provenance.py                 # 최종 정합성·커버리지
```
staging 런타임 파일은 .gitignore 대상(샘플 `edge_proposals.sample.jsonl`만 추적). 관련 메모: 벤치마크-팔란티어.
