# FREEZE THEME JSON LOCK (data/theme/T_xxx.json)

## 0. 결론
- data/theme/T_xxx.json은 "웹 시각화 입력물"이며, 한 번 정상 확인되면 Freeze 한다.
- Freeze 후에는 재생성/재포맷을 최소화하고, 교체가 필요할 때만 전체 교체한다.

---

## 1. T_xxx.json 최상위 구조 (LOCK)
반드시 객체 { } 로 시작한다.
배열 [ ] 로 시작하면 실패로 간주한다.

필수 키:
- schemaVersion (string) 예: "v5"
- themeId (string) 예: "T_011"
- themeName (string)
- meta (object)
- nodes (array)
- edges (array)

---

## 2. 금지 패턴 (LOCK)
Neo4j 결과를 그대로 저장하면 아래 형태가 생길 수 있다. 이 경우는 실패다.

- [ { "json": "....(escaped string)...." } ]  ❌
- \" 같은 escape가 파일 본문에 대량 포함 ❌

정상 산출물은 escape 없이 아래처럼 nodes/edges가 실제 배열이어야 한다. ✅
{
  "schemaVersion": "v5",
  ...
  "nodes": [ ... ],
  "edges": [ ... ]
}

---

## 3. Neo4j(Aura) APOC 제약 대응 (LOCK)
Aura/Browser 환경에서 apoc.convert.toJson는 1개 인자만 허용될 수 있다.
- apoc.convert.toJson(doc, {pretty:true}) ❌ 금지 (에러 가능)
- apoc.convert.toJson(doc) ✅ 허용

Browser에서 긴 문자열이 "한 줄"로 보이는 것은 UI 특성이다.
-> 결과 저장 시 반드시 "정상 JSON 형태로 파싱/복원"하여 커밋한다.

---

## 4. node/edge 식별자 일치 규칙 (LOCK)
- nodes[].id 는 반드시 edges[].from / edges[].to 와 문자열로 1:1 매칭되어야 한다.
- id prefix는 프로젝트 규칙을 따른다. (예: T_###, A_###, BF_### 등)

---

## 5. Freeze 체크 (Release 전 30초)
- [ ] raw URL로 T_xxx.json이 { 로 시작하는가?
- [ ] themeId/themeName이 정상 문자열인가?
- [ ] nodes/edges 배열이 존재하고 length > 0 인가?
- [ ] index.json의 nodeCount/edgeCount가 실제와 일치하는가?
