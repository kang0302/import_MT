import subprocess
import sys

def git(cmd):
    return subprocess.check_output(cmd).decode("utf-8", "replace")

# 루트 디렉토리 목록
raw = subprocess.check_output(
    ["git", "ls-tree", "-z", "-d", "--name-only", "HEAD"]
)
dirs = [d.decode("utf-8", "replace") for d in raw.split(b"\x00") if d]

existing = set(dirs)
renamed = []

def is_invalid(name: str) -> bool:
    return name.endswith(" ") or name.endswith(".")

for old in dirs:
    if not is_invalid(old):
        continue

    base = old.rstrip(" .")
    if not base:
        base = "__EMPTY__"

    new = base
    if new in existing:
        new = f"{base}__SANITIZED"

    i = 2
    while new in existing:
        new = f"{base}__SANITIZED_{i}"
        i += 1

    print(f"RENAME: {old!r} -> {new!r}")
    subprocess.check_call(["git", "mv", "-f", old, new])

    existing.add(new)
    renamed.append((old, new))

if not renamed:
    print("No invalid paths found.")
    sys.exit(0)

subprocess.check_call(["git", "status", "--porcelain"])
subprocess.check_call(["git", "config", "user.name", "moneytree-bot"])
subprocess.check_call([
    "git", "config", "user.email",
    "moneytree-bot@users.noreply.github.com"
])
subprocess.check_call(["git", "add", "-A"])
subprocess.check_call([
    "git", "commit", "-m",
    "chore: sanitize invalid windows paths"
])
subprocess.check_call(["git", "push"])
