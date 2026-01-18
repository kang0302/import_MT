#!/usr/bin/env python3
# scripts/rebuild_theme_index.py
#
# DAY62-1
# - data/theme/*.json 을 스캔해서
# - data/theme/index.json 을 자동 재생성한다.
# - themeId/themeName 누락/불일치 방지 (파일명 T_0XX 기준으로 보정)
#
# 결과 포맷(예):
# [
#   {"themeId":"T_006","themeName":"글로벌 로봇 반도체"},
#   {"themeId":"T_009","themeName":"..."}
# ]

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
THEME_DIR = ROOT / "data" / "theme"
INDEX_PATH = THEME_DIR / "index.json"

THEME_FILE_RE = re.compile(r"^(T_\d{3})\.json$", re.IGNORECASE)

def safe_read_json(p: Path) -> Dict[str, Any]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def pick_theme_name(obj: Dict[str, Any], fallback: str) -> str:
    # themeName 우선, 없으면 THEME 노드 name 탐색, 그래도 없으면 fallback
    tn = obj.get("themeName")
    if isinstance(tn, str) and tn.strip():
        return tn.strip()

    nodes = obj.get("nodes")
    if isinstance(nodes, list):
        for n in nodes:
            if not isinstance(n, dict):
                continue
            t = str(n.get("type", "")).upper()
            if t == "THEME":
                name = n.get("name")
                if isinstance(name, str) and name.strip():
                    return name.strip()
    return fallback

def norm_theme_id_from_filename(fname: str) -> str | None:
    m = THEME_FILE_RE.match(fname)
    if not m:
        return None
    return m.group(1).upper()

def main() -> None:
    if not THEME_DIR.exists():
        raise SystemExit(f"[ERR] missing folder: {THEME_DIR}")

    items: List[Tuple[str, str, str]] = []  # (themeId, themeName, filename)
    for p in sorted(THEME_DIR.glob("T_*.json")):
        tid = norm_theme_id_from_filename(p.name)
        if not tid:
            continue

        obj = safe_read_json(p)

        # ✅ 파일명 기준 themeId를 최우선으로 고정 (내부 themeId가 틀려도 보정)
        theme_name = pick_theme_name(obj, fallback=tid)

        items.append((tid, theme_name, p.name))

    # 정렬: 숫자 기준 (T_006 < T_010)
    def key(item: Tuple[str, str, str]) -> int:
        tid = item[0]
        try:
            return int(tid.split("_")[1])
        except Exception:
            return 999999

    items = sorted(items, key=key)

    out: List[Dict[str, str]] = [{"themeId": tid, "themeName": tname} for tid, tname, _ in items]

    THEME_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"[OK] index rebuilt: {INDEX_PATH}")
    print(f"[OK] themes found: {len(out)}")
    if out[:5]:
        print("[INFO] first 5:")
        for x in out[:5]:
            print(" -", x["themeId"], x["themeName"])

if __name__ == "__main__":
    main()
