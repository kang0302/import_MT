# SSOT THEME INDEX LOCK (data/theme/index.json)

## 0. 결론
- data/theme/index.json은 테마 목록의 SSOT(단일 진실 원천)이다.
- 개별 T_xxx.json이 있어도 index.json에 없으면 서비스에 존재하지 않는 것으로 간주한다.

---

## 1. index.json 항목 스키마 (LOCK)
index.json의 각 항목은 반드시 아래 키만 가진다.
(다른 키 금지: description, category 등)

필수 키:
- themeId (string)  예: "T_011"
- themeName (string) 예: "글로벌 데이터센터 액침 냉각 관련주"
- nodeCount (number)
- edgeCount (number)
- source (string) 기본: "auto"
- updatedAt (string) "YYYY-MM-DD"

### 예시
{
  "themeId": "T_011",
  "themeName": "글로벌 데이터센터 액침 냉각 관련주",
  "nodeCount": 14,
  "edgeCount": 17,
  "source": "auto",
  "updatedAt": "2026-01-24"
}

---

## 2. placeholder 금지 (LOCK)
- themeName: "..." 금지
- nodeCount/edgeCount: 누락 금지 (누락 시 UI에서 목록 제외/오동작 가능)

---

## 3. 업데이트 규칙 (LOCK)
- 새 테마 Freeze 시, 반드시 두 파일을 함께 갱신한다.
  1) data/theme/T_xxx.json
  2) data/theme/index.json (동일 스키마 + nodeCount/edgeCount 반영)

---

## 4. 운영 체크 (Release 전 30초)
- [ ] index.json raw에서 T_xxx 항목이 보이는가?
- [ ] themeName이 실제 문자열인가?
- [ ] nodeCount/edgeCount가 숫자인가?
