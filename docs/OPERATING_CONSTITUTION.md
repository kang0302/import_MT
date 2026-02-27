좋다.
이제부터 이건 “기억용 설명”이 아니라 **MoneyTree 운영 헌법(LOCK)** 이다.
그대로 `docs/OPERATING_CONSTITUTION.md`에 붙여 넣어도 된다.

---

# 🧭 MONEYTREE DATA OPERATING CONSTITUTION (LOCK)

## Version: v1.0

## Status: LOCKED

## Scope: KR + US (PYKRX + FMP)

---

# 1️⃣ SINGLE SOURCE OF TRUTH (SSOT)

### 📌 원칙

> 모든 데이터는 SSOT → CACHE → FREEZE → THEME → RAW → WEB 순으로 흐른다.
> 중간을 건너뛰지 않는다.

---

## 1.1 데이터 흐름 구조 (절대 변경 금지)

```
asset_ssot.csv
        ↓
update_valuation_kr.py
update_return_kr.py
update_valuation_fmp.py
update_return_fmp.py
        ↓
data/cache/*.json
        ↓
scripts/build_freeze.py
        ↓
data/theme/T_*.json
        ↓
GitHub commit/push
        ↓
GitHub raw
        ↓
moneytree-web
```

---

# 2️⃣ CACHE LAYER 규칙

## 2.1 KR

| 파일                | 소스    |
| ----------------- | ----- |
| valuation_kr.json | PYKRX |
| returns_kr.json   | PYKRX |

---

## 2.2 US

| 파일                 | 소스  |
| ------------------ | --- |
| valuation_fmp.json | FMP |
| returns_fmp.json   | FMP |

---

## 2.3 캐시 규칙

* 파일이 없거나 깨져도 build_freeze는 죽지 않는다.
* 402(PAYMENT_REQUIRED)는 **skip** 한다.
* 심볼 단위 실패는 전체 파이프라인을 멈추지 않는다.
* source, asOf는 반드시 기록한다.

---

# 3️⃣ FREEZE LAYER (build_freeze.py)

## 3.1 절대 원칙

> ❗ THEME JSON을 직접 수정하지 않는다.
> 모든 metrics 주입은 build_freeze.py 한 군데에서만 한다.

---

## 3.2 ASSET metrics 스키마 LOCK

모든 ASSET 노드는 아래 키를 항상 가진다 (값이 null이어도 존재):

### 📊 Valuation

* close
* marketCap
* pe_ttm
* valuationAsOf
* valuationSource

### 📈 Returns

* return_3d
* return_7d
* return_1m
* return_ytd
* return_1y
* return_3y
* returnsAsOf
* returnsSource

> 키 누락은 UI 오류를 유발한다.
> build_freeze는 구조 보정(schema normalize)을 항상 수행한다.

---

# 4️⃣ GITHUB ACTIONS LOCK

## 4.1 커밋 대상 파일 (절대 고정)

```
data/cache/*.json
data/theme/T_*.json
data/theme/index.json
```

> cache만 커밋하거나 theme만 커밋하지 않는다.
> 반드시 세 영역을 동시에 관리한다.

---

## 4.2 실패 허용 규칙

| 상황       | 처리            |
| -------- | ------------- |
| 402 심볼   | skip          |
| 일부 심볼 실패 | continue      |
| 전체 유효값 0 | workflow fail |

---

# 5️⃣ RAW → WEB 연결 규칙

* 웹은 GitHub raw를 읽는다.
* public mirror는 안전 장치일 뿐, SSOT는 data/theme 이다.
* raw가 최신 커밋을 가리키는지 항상 확인한다.

---

# 6️⃣ 운영 점검 체크리스트

배포 전 반드시 확인:

### ✅ 1. cache 생성 로그 존재

```
wrote valuation_fmp.json
wrote returns_fmp.json
```

### ✅ 2. freeze 주입 로그 존재

```
updated x/y asset nodes
```

### ✅ 3. raw 테마 파일에 metrics 존재

### ✅ 4. 웹에서

* valuation 표시 정상
* returns 표시 정상
* period toggle 정상
* top movers 정상
* barometer 정상

---

# 7️⃣ 금지 사항 (DO NOT)

❌ theme JSON 수동 수정
❌ metrics 키 삭제
❌ source/asOf 제거
❌ cache만 업데이트 후 freeze 생략
❌ push 없이 raw 테스트

---

# 8️⃣ 확장 규칙 (미래)

추가 국가 확장 시:

1. update_xxx.py 생성
2. cache 파일 생성
3. build_freeze에서 asset_id 기반 주입
4. metrics 키는 변경하지 않는다

---

# 9️⃣ 시스템 철학

> MoneyTree는 “추천 서비스”가 아니라
> “구조 기반 데이터 엔진”이다.

데이터 흐름은 단방향이며,
theme JSON은 최종 소비물이다.

---

# 🔒 FINAL LOCK STATEMENT

KR + US valuation + returns 파이프라인은
현재 구조로 운영을 고정한다.

향후 수정은 반드시:

1. 캐시 생성 로직
2. build_freeze 주입 로직
3. Actions 커밋 규칙

이 세 영역을 동시에 검토한 후 수행한다.

---

이제부터는
“작동 여부 확인” 단계가 아니라
“구조 안정화 + 확장” 단계다.

---

원하면 다음 단계로:

* 📊 테마 단위 종합지수 산출 LOCK
* 🌡 Barometer 계산 표준화 LOCK
* 🧠 Top movers 알고리즘 고정

어디로 갈까?
