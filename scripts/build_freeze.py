# import_MT/scripts/build_freeze.py
# -*- coding: utf-8 -*-

import json
from pathlib import Path
from datetime import datetime

REPO_ROOT = Path(__file__).resolve().parents[1]  # import_MT/
VAL_PATH = REPO_ROOT / "data" / "cache" / "valuation_kr.json"
THEME_DIR = REPO_ROOT / "data" / "theme"
INDEX_PATH = THEME_DIR / "index.json"


def read_json(path: Path):
    # Windows/편집기에서 UTF-8 BOM이 붙어도 안전하게 처리
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def inject_metrics_to_theme(theme_obj: dict, val_payload: dict) -> dict:
    """
    theme_obj.nodes[] 중 id가 A_### 인 ASSET 노드에 valuation metrics 주입.
    """
    asof = val_payload.get("asOf")  # "YYYY-MM-DD"
    source = val_payload.get("source", "PYKRX")
    items = (val_payload.get("items") or {})  # { "A_004": {...}, ... }

    data_asof = f"{asof}T16:00:00+09:00" if asof else None

    # meta 업데이트(있으면 유지, 없으면 생성)
    meta = theme_obj.get("meta") or {}
    if data_asof:
        meta["dataAsOf"] = data_asof
    theme_obj["meta"] = meta

    nodes = theme_obj.get("nodes") or []
    for n in nodes:
        if (n.get("type") != "ASSET"):
            continue
        aid = n.get("id")
        if not aid or aid not in items:
            continue

        v = items[aid] or {}
        metrics = n.get("metrics") or {}

        # 요청한 필드명 그대로 주입(A안)
        metrics["close"] = v.get("close")
        metrics["marketCap"] = v.get("marketCap")
        metrics["pe_ttm"] = v.get("pe_ttm")
        if asof:
            metrics["valuationAsOf"] = asof
        metrics["valuationSource"] = source

        n["metrics"] = metrics

    theme_obj["nodes"] = nodes
    return theme_obj


def count_edges(theme_obj: dict) -> int:
    # v5 freeze에서 edges 키가 있을 수도, links 일 수도 있음(프로젝트별 변형 대응)
    if isinstance(theme_obj.get("edges"), list):
        return len(theme_obj["edges"])
    if isinstance(theme_obj.get("links"), list):
        return len(theme_obj["links"])
    return 0


def main():
    if not VAL_PATH.exists():
        raise RuntimeError(f"valuation 캐시가 없습니다: {VAL_PATH}")
    if not THEME_DIR.exists():
        raise RuntimeError(f"테마 디렉토리가 없습니다: {THEME_DIR}")

    val_payload = read_json(VAL_PATH)

    # 1) 모든 T_*.json 갱신
    theme_files = sorted(THEME_DIR.glob("T_*.json"))
    if not theme_files:
        raise RuntimeError("data/theme/T_*.json 파일을 찾지 못했습니다.")

    updated = []
    for tf in theme_files:
        theme_obj = read_json(tf)
        theme_obj = inject_metrics_to_theme(theme_obj, val_payload)
        write_json(tf, theme_obj)
        updated.append(tf.name)

    # 2) index.json 갱신(존재하면)
    if INDEX_PATH.exists():
        idx = read_json(INDEX_PATH)
        themes = idx.get("themes") or []

        today = val_payload.get("asOf") or datetime.now().strftime("%Y-%m-%d")

        # 파일별 node/edge 카운트 업데이트
        theme_map = {}
        for tf in theme_files:
            tobj = read_json(tf)
            tid = tobj.get("themeId") or tobj.get("id")  # 혹시 id로 들어간 경우 대비
            if not tid:
                continue
            theme_map[tid] = {
                "nodeCount": len(tobj.get("nodes") or []),
                "edgeCount": count_edges(tobj),
                "updatedAt": today,
            }

        for t in themes:
            tid = t.get("themeId")
            if tid in theme_map:
                t["nodeCount"] = theme_map[tid]["nodeCount"]
                t["edgeCount"] = theme_map[tid]["edgeCount"]
                t["updatedAt"] = theme_map[tid]["updatedAt"]

        idx["themes"] = themes
        write_json(INDEX_PATH, idx)

    print("[OK] build_freeze done")
    print(f"     updated themes: {len(updated)}")
    print(f"     touched index: {INDEX_PATH.exists()}")


if __name__ == "__main__":
    main()