import json
import glob
import os
from datetime import datetime

THEME_DIR = "data/theme"
OUTPUT = os.path.join(THEME_DIR, "index.json")

items = []

for path in sorted(glob.glob(os.path.join(THEME_DIR, "T_*.json"))):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        theme_id = data.get("themeId") or os.path.splitext(os.path.basename(path))[0]
        theme_name = data.get("themeName", theme_id)

        nodes = data.get("nodes", []) or []
        edges = data.get("edges", []) or []

        items.append({
            "themeId": theme_id,
            "themeName": theme_name,
            "nodeCount": len(nodes),
            "edgeCount": len(edges),
            "source": "auto",
            "updatedAt": datetime.utcnow().strftime("%Y-%m-%d"),
        })

    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}")

os.makedirs(THEME_DIR, exist_ok=True)

with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(items, f, ensure_ascii=False, indent=2)

print(f"[OK] theme index generated: {OUTPUT} ({len(items)} themes)")
