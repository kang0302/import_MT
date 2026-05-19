# import_MT/scripts/source_router.py
# 자산 데이터 소스 라우팅 룰 — 라이브 fetch + 배치 freeze 양쪽 단일 진실원천.
#
# 정책 (2026-05-19 사용자 확정):
#   - US 거래소 → FMP (1순위)
#   - 그 외 모든 거래소 → EODHD (1순위)
#   - primary 가 error / null 반환 시 → YAHOO (universal fallback)
#   - unknown country/exchange → YAHOO 단독
#
# TypeScript 동치 모듈: src/lib/sourceRouter.ts (mapping 일치 필수).

from typing import Literal, Optional, Tuple

DataSource = Literal["FMP", "EODHD", "YAHOO"]

# ─── FMP 1순위 거래소 (US) ───
FMP_EXCHANGES = {
    "NYSE", "NASDAQ", "AMEX", "NYSEARCA", "BATS",
    "OTC", "OTCMKTS", "OTCBB", "PINK", "CBOE",
}
FMP_COUNTRIES = {"US", "USA"}

# ─── EODHD 1순위 거래소 (전 글로벌, US 제외) ───
EODHD_EXCHANGES = {
    # Korea
    "KOSPI", "KOSDAQ", "KRX", "KONEX",
    # Japan
    "TSE", "TYO", "TOKYO",
    # China
    "SHA", "SSE", "SHANGHAI", "SHE", "SZSE", "SHENZHEN",
    # Hong Kong
    "HKG", "HKEX", "HK",
    # Taiwan
    "TWSE", "TPE", "TAI",
    # Europe — UK
    "LSE", "LON",
    # Europe — DE
    "XETRA", "ETR", "FRA",
    # Europe — FR / NL / BE
    "EPA", "EURONEXT", "EAM", "AMS", "EBR", "BRU",
    # Europe — IT / ES
    "MIL", "BIT", "MCE", "BME",
    # Europe — CH / AT
    "SWX", "SIX", "VIE",
    # Europe — Nordic
    "OSL", "OL", "STO", "ST", "CPH", "CO", "HEL", "HE",
    # Europe — others
    "LIS", "LS", "WSE", "WAR", "IST", "IS",
    # Canada
    "TSX", "TO", "TSXV", "CVE", "V",
    # Australia / NZ
    "ASX", "AX",
    # India / Singapore / Indonesia
    "BSE", "BOM", "NSE", "NSI", "SGX", "SI", "IDX",
    # LatAm / Africa / Mideast
    "B3", "BVMF", "BMV", "BVL", "JSE", "TADAWUL",
}
EODHD_COUNTRIES = {
    "KR", "JP", "JPN", "CN", "CHN", "HK", "HKG",
    "TW", "TWN", "GB", "UK", "DE", "DEU", "FR", "FRA",
    "NL", "NLD", "BE", "BEL", "IT", "ITA", "ES", "ESP",
    "CH", "CHE", "AT", "AUT", "NO", "NOR", "SE", "SWE",
    "DK", "DNK", "FI", "FIN", "PT", "PRT", "PL", "POL",
    "TR", "TUR", "CA", "CAN", "AU", "AUS", "NZ", "NZL",
    "IN", "IND", "SG", "SGP", "ID", "IDN", "BR", "BRA",
    "MX", "MEX", "AR", "ARG", "ZA", "ZAF", "SA", "SAU",
    "QA", "QAT", "AE", "ARE", "IE", "IRL",
}


def pick_primary_source(country: Optional[str], exchange: Optional[str]) -> DataSource:
    """1순위 데이터 소스 결정.

    매칭 우선순위: exchange 가 country 보다 강함 (NASDAQ 같은 명확한 신호).

    Args:
        country: ISO-2 code (US/KR/JP/...). None/빈 문자열 허용.
        exchange: 거래소 코드 (NASDAQ/KOSPI/TSE/...). None/빈 문자열 허용.

    Returns:
        "FMP" | "EODHD" | "YAHOO" (unknown 시 YAHOO)
    """
    c = (country or "").upper().strip()
    ex = (exchange or "").upper().strip()

    # exchange 우선
    if ex:
        if ex in FMP_EXCHANGES:
            return "FMP"
        if ex in EODHD_EXCHANGES:
            return "EODHD"
    # country fallback
    if c:
        if c in FMP_COUNTRIES:
            return "FMP"
        if c in EODHD_COUNTRIES:
            return "EODHD"
    # unknown → YAHOO last resort
    return "YAHOO"


def pick_fallback_source(_primary: DataSource) -> Optional[DataSource]:
    """Fallback source (primary 실패·null 시 사용). 현재 정책: 항상 YAHOO."""
    return "YAHOO"


def pick_sources(country: Optional[str], exchange: Optional[str]) -> Tuple[DataSource, Optional[DataSource]]:
    """Primary + Fallback 둘 다 반환 — 호출자가 순차 시도.

    Returns:
        (primary, fallback) — primary 가 YAHOO 면 fallback 은 None.
    """
    primary = pick_primary_source(country, exchange)
    fallback: Optional[DataSource] = None if primary == "YAHOO" else pick_fallback_source(primary)
    return primary, fallback
