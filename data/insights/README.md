# 투자 인사이트 (Investment Insights)

테마·자산별 Claude 리서치 내용을 저장하는 폴더.

## 파일 명명 규칙
- 테마 인사이트: `T_xxx.md` (예: `T_286.md`)
- 자산 인사이트: `A_xxx.md` (예: `A_055.md`)

## 파일 포맷

YAML frontmatter + Markdown 본문.

```markdown
---
title: 분석 제목 (선택)
updated_at: 2026-06-09
author: Claude (선택)
tags: [Rubin, HBM, SoCAMM]  (선택)
---

본문 markdown ...

## 핵심 포인트
- ...
- ...

## 결론
...
```

## 표시 위치
- `/graph/[themeId]` 페이지 하단 "투자 인사이트" 섹션
- 해당 테마 + 테마에 포함된 ASSET 의 인사이트 자동 표시
- `updated_at` 이 **24시간 이내** 이면 NEW 배지 자동 부착

## fetch URL
`https://raw.githubusercontent.com/kang0302/import_MT/main/data/insights/T_xxx.md`

## 작성·갱신 워크플로
1. Claude 에서 리서치 후 결과 받기
2. 이 폴더에 `T_xxx.md` 또는 `A_xxx.md` 로 저장 (frontmatter 포함)
3. `git commit && git push`
4. 5분 GitHub raw CDN 캐시 후 그래프 페이지에 표시됨
