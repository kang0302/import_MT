# 테마 카드 이미지

테마 그래프 좌측 상단 **테마 설명 박스 하단**에 표시되는 이미지.

- 경로 규약: `data/theme_images/{themeId}.{ext}` (예: `T_244.jpg`)
- 지원 확장자: jpg, png, webp, jpeg (프론트가 순차 시도, 전부 없으면 미표시)
- 프론트는 GitHub raw(`kang0302/import_MT/main/data/theme_images/...`)에서 직접 로드 → 이미지 파일만 push하면 즉시 반영(Vercel 재배포 불필요).
- 렌더러: `moneytree-web/src/components/ForceGraphWrapper.tsx` `ThemeCardImage`.
- 권장: 가로형(약 300×116 표시, object-cover), 저작권 확인된 이미지.
