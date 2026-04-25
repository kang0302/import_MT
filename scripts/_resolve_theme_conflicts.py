"""
One-shot script — fix/theme-json-conflicts 브랜치의 미해결 git merge conflict 마커를
"Stashed changes"(=newer FMP snapshot) 쪽으로 일괄 해결.

검사한 27개 테마 모두 패턴이 동일:
    <<<<<<< Updated upstream
    ... older (2026-02-27/28, no returnsAsOf) ...
    =======
    ... newer (2026-03-01, with returnsAsOf/Source) ...
    >>>>>>> Stashed changes

처리 후:
- 각 파일이 valid JSON인지 검증
- 변경 파일 수 / 해결한 conflict block 수 출력

사용:
    cd import_MT
    python scripts/_resolve_theme_conflicts.py [--dry-run]
"""
import json
import re
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
THEME_DIR = BASE / "data" / "theme"

CONFLICT_RE = re.compile(
    r"^<<<<<<<[^\n]*\n"
    r"(?P<old>.*?)"
    r"^=======\s*\n"
    r"(?P<new>.*?)"
    r"^>>>>>>>[^\n]*\n",
    re.DOTALL | re.MULTILINE,
)


def resolve_text(text: str) -> tuple[str, int]:
    """모든 conflict block을 'new'(Stashed changes) 쪽으로 교체. (resolved_text, n_blocks) 반환."""
    n = 0

    def _take_new(m: re.Match) -> str:
        nonlocal n
        n += 1
        return m.group("new")

    return CONFLICT_RE.sub(_take_new, text), n


def main() -> int:
    dry = "--dry-run" in sys.argv

    files = sorted(THEME_DIR.glob("T_*.json"))
    total_files = 0
    total_blocks = 0
    failed: list[tuple[str, str]] = []

    for fp in files:
        text = fp.read_text(encoding="utf-8")
        if "<<<<<<<" not in text:
            continue

        new_text, n_blocks = resolve_text(text)
        if "<<<<<<<" in new_text:
            failed.append((fp.name, "still has conflict markers after resolve"))
            continue

        # JSON validity check
        try:
            json.loads(new_text)
        except json.JSONDecodeError as e:
            failed.append((fp.name, f"invalid JSON after resolve: {e}"))
            continue

        total_files += 1
        total_blocks += n_blocks
        if dry:
            print(f"  [dry] would resolve {n_blocks} block(s) in {fp.name}")
        else:
            fp.write_text(new_text, encoding="utf-8")
            print(f"  [OK] resolved {n_blocks} block(s) in {fp.name}")

    print()
    print(f"{'(dry-run) ' if dry else ''}files touched: {total_files}, conflict blocks resolved: {total_blocks}")

    if failed:
        print()
        print(f"❌ {len(failed)} file(s) failed:")
        for name, reason in failed:
            print(f"    {name}: {reason}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
