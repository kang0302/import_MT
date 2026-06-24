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

## 4. AI 인제스트 계약(후속)

LLM은 JSON을 직접 쓰지 않고 `EdgeProposal`을 emit하며, 여기엔 `evidence(url+quote+date)`가 **필수**.
근거 없으면 동일 검증기가 거부 → 출처(#3)가 AI 가드레일(#2)을 자동으로 강제. 관련 메모: 벤치마크-팔란티어.
