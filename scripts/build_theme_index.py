# scripts/build_theme_index.py
import json
import os
from glob import glob

THEME_DIR = "data/theme"
OUT_PATH = "data/theme/index.json"

def main():
    os.makedirs(THEME_DIR, exist_ok=True)

    items = []
    for path in sorted(glob(os.path.join(THEME_DIR, "T_*.json"))):
        try:
            with open(path, "r", encoding="utf-8") as f:
                j = json.load(f)
            tid = (j.get("themeId") or os.path.splitext(os.path.basename(path))[0]).strip()
            tname = (j.get("themeName") or tid).strip()
            if tid and tname:
                items.append({"themeId": tid, "themeName": tname})
        except Exception as e:
            print(f"[WARN] failed to read {path}: {e}")

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    print(f"[OK] index.json written: {OUT_PATH} (count={len(items)})")

if __name__ == "__main__":
    main()
