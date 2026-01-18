#!/usr/bin/env python3
# scripts/validate_theme_json.py
# DAY61 - Theme JSON Validator (LOCK)
#
# Checks:
# - theme JSON schema sanity
# - node id uniqueness
# - edge endpoints exist
# - node types are allowed
# - ASSET/BUSINESS_FIELD/CHARACTER/MACRO ids exist in master CSVs
# - theme node exists and matches themeId/themeName
#
# Exit code:
# - 0: PASS
# - 1: FAIL

import argparse
import csv
import glob
import json
import os
import sys
from typing import Dict, List, Set, Tuple, Any


# =========================
# LOCK: Allowed node types
# =========================
ALLOWED_NODE_TYPES = {
    "THEME",
    "ASSET",
    "BUSINESS_FIELD",
    "CHARACTER",
    "MACRO",
}

# Some repos use variants; normalize by uppercase and mapping
TYPE_ALIASES = {
    "BUSINESSFIELD": "BUSINESS_FIELD",
    "BUSINESS-FIELD": "BUSINESS_FIELD",
    "FIELD": "BUSINESS_FIELD",  # treat FIELD as BUSINESS_FIELD for compatibility
}

# =========================
# Master CSV locations (default)
# =========================
DEFAULT_MASTER_DIR = "data/master"
DEFAULT_THEME_DIR = "data/theme"


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def load_csv_id_set(path: str, id_columns: List[str]) -> Set[str]:
    if not os.path.exists(path):
        return set()

    ids: Set[str] = set()
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return set()

        for row in reader:
            rid = ""
            for col in id_columns:
                if col in row and row[col]:
                    rid = str(row[col]).strip()
                    break
            if rid:
                ids.add(rid)
    return ids


def norm_type(t: str) -> str:
    if not t:
        return ""
    x = str(t).strip().upper().replace(" ", "").replace("\t", "")
    # keep underscore if present
    x = x.replace("-", "_")
    return TYPE_ALIASES.get(x, x)


def safe_list(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


def validate_one_theme_json(
    path: str,
    asset_ids: Set[str],
    bf_ids: Set[str],
    char_ids: Set[str],
    macro_ids: Set[str],
) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    # load
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as ex:
        errors.append(f"[JSON] cannot read/parse: {ex}")
        return False, errors

    theme_id = str(data.get("themeId", "")).strip()
    theme_name = str(data.get("themeName", "")).strip()

    if not theme_id:
        errors.append("[SCHEMA] missing themeId")
    if not theme_name:
        errors.append("[SCHEMA] missing themeName")

    nodes = safe_list(data.get("nodes"))
    edges = safe_list(data.get("edges"))

    if not nodes:
        errors.append("[SCHEMA] nodes is empty or missing")
    if not isinstance(edges, list):
        errors.append("[SCHEMA] edges must be a list")

    # nodes basic checks
    node_ids: List[str] = []
    node_id_set: Set[str] = set()
    theme_node_found = False

    for i, n in enumerate(nodes):
        if not isinstance(n, dict):
            errors.append(f"[NODE] nodes[{i}] is not an object")
            continue

        nid = str(n.get("id", "")).strip()
        ntype_raw = str(n.get("type", "")).strip()
        ntype = norm_type(ntype_raw)

        if not nid:
            errors.append(f"[NODE] nodes[{i}] missing id")
            continue

        if nid in node_id_set:
            errors.append(f"[NODE] duplicate node id: {nid}")
        node_id_set.add(nid)
        node_ids.append(nid)

        if not ntype:
            errors.append(f"[NODE] {nid} missing type")
        else:
            if ntype not in ALLOWED_NODE_TYPES and ntype != "BUSINESS_FIELD":
                # "BUSINESS_FIELD" already in allowed set; keep compatibility
                errors.append(f"[NODE] {nid} invalid type: {ntype_raw}")

        # theme node check
        if ntype == "THEME" or nid == theme_id:
            # Allow either explicit THEME type or id == themeId
            if theme_id and nid != theme_id and ntype == "THEME":
                # if THEME typed node exists but id differs, warn
                errors.append(f"[THEME] theme node id mismatch (themeId={theme_id}, themeNodeId={nid})")
            theme_node_found = True

        # master existence checks
        if ntype == "ASSET":
            if asset_ids and nid not in asset_ids:
                errors.append(f"[MASTER] ASSET id not found in master asset.csv: {nid}")

        if ntype == "BUSINESS_FIELD":
            if bf_ids and nid not in bf_ids:
                errors.append(f"[MASTER] BUSINESS_FIELD id not found in master business_field.csv: {nid}")

        if ntype == "CHARACTER":
            if char_ids and nid not in char_ids:
                errors.append(f"[MASTER] CHARACTER id not found in master character.csv: {nid}")

        if ntype == "MACRO":
            if macro_ids and nid not in macro_ids:
                errors.append(f"[MASTER] MACRO id not found in master macro.csv: {nid}")

    if theme_id and not theme_node_found:
        errors.append(f"[THEME] theme node not found (need node id=={theme_id} or type==THEME)")

    # edges checks
    for j, e in enumerate(edges):
        if not isinstance(e, dict):
            errors.append(f"[EDGE] edges[{j}] is not an object")
            continue

        frm = str(e.get("from", "")).strip()
        to = str(e.get("to", "")).strip()
        etype = str(e.get("type", "")).strip()

        if not frm or not to:
            errors.append(f"[EDGE] edges[{j}] missing from/to")
            continue

        if frm not in node_id_set:
            errors.append(f"[EDGE] edges[{j}] from id not in nodes: {frm}")
        if to not in node_id_set:
            errors.append(f"[EDGE] edges[{j}] to id not in nodes: {to}")
        if not etype:
            errors.append(f"[EDGE] edges[{j}] missing type ({frm} -> {to})")

    ok = len(errors) == 0
    return ok, errors


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--theme-dir", default=DEFAULT_THEME_DIR, help="Theme JSON directory (default: data/theme)")
    ap.add_argument("--master-dir", default=DEFAULT_MASTER_DIR, help="Master CSV directory (default: data/master)")
    ap.add_argument("--glob", default="T_*.json", help="Theme JSON glob pattern (default: T_*.json)")
    ap.add_argument("--strict-missing-master", action="store_true",
                    help="If master files missing, fail (default: do not fail, but checks may be skipped)")
    args = ap.parse_args()

    theme_dir = args.theme_dir
    master_dir = args.master_dir

    asset_csv = os.path.join(master_dir, "asset.csv")
    bf_csv = os.path.join(master_dir, "business_field.csv")
    char_csv = os.path.join(master_dir, "character.csv")
    macro_csv = os.path.join(master_dir, "macro.csv")

    # Load masters
    asset_ids = load_csv_id_set(asset_csv, ["asset_id", "id", "assetId"])
    bf_ids = load_csv_id_set(bf_csv, ["bf_id", "business_field_id", "id"])
    char_ids = load_csv_id_set(char_csv, ["character_id", "c_id", "id"])
    macro_ids = load_csv_id_set(macro_csv, ["macro_id", "m_id", "id"])

    # If strict mode and master missing -> fail early
    if args.strict_missing_master:
        missing = []
        for p in [asset_csv, bf_csv, char_csv, macro_csv]:
            if not os.path.exists(p):
                missing.append(p)
        if missing:
            eprint("[FAIL] Missing master CSV files:")
            for m in missing:
                eprint(" -", m)
            return 1

    paths = sorted(glob.glob(os.path.join(theme_dir, args.glob)))
    if not paths:
        eprint(f"[FAIL] No theme JSON found in {theme_dir} ({args.glob})")
        return 1

    total = 0
    failed = 0

    for p in paths:
        total += 1
        ok, errs = validate_one_theme_json(
            p,
            asset_ids=asset_ids,
            bf_ids=bf_ids,
            char_ids=char_ids,
            macro_ids=macro_ids,
        )
        rel = os.path.relpath(p)
        if ok:
            print(f"[PASS] {rel}")
        else:
            failed += 1
            eprint(f"[FAIL] {rel}")
            for msg in errs:
                eprint("  -", msg)

    if failed:
        eprint(f"\n[RESULT] FAIL ({failed}/{total})")
        return 1

    print(f"\n[RESULT] PASS ({total}/{total})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
