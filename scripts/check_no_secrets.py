"""Fail if secrets could reach Git: .env must be ignored, and no tracked/untracked-unignored file may
contain a JWT-like token or a filled-in secret assignment.

Usage: .venv\\Scripts\\python scripts\\check_no_secrets.py
"""
import re
import subprocess

import _bootstrap
ROOT = _bootstrap.ROOT

PATTERNS = [re.compile(rb"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),  # JWT
            re.compile(rb"(?m)^(DHAN_ACCESS_TOKEN|PG_PASSWORD|TELEGRAM_BOT_TOKEN)=\S+"),
            re.compile(rb"\d{8,10}:[A-Za-z0-9_-]{30,}")]  # Telegram bot token


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


problems = []
if (ROOT / ".env").exists() and not git("check-ignore", ".env").strip():
    problems.append(".env exists but is NOT git-ignored")
files = set(git("ls-files").split("\n")) | set(git("ls-files", "--others", "--exclude-standard").split("\n"))
for rel in sorted(f for f in files if f):
    p = ROOT / rel
    if not p.is_file() or p.stat().st_size > 5_000_000:
        continue
    data = p.read_bytes()
    for pat in PATTERNS:
        if pat.search(data):
            problems.append(f"possible secret in {rel}")
            break
print("\n".join(problems) if problems else f"ok: {len(files)} candidate files scanned, no secrets found")
raise SystemExit(1 if problems else 0)
