"""Store a secret in Windows Credential Manager (service 'NiftyTradingAI'). Value is typed hidden.

Usage: .venv\\Scripts\\python scripts\\set_secret.py DHAN_ACCESS_TOKEN
"""
import getpass
import sys

import _bootstrap  # noqa: F401
from app.secrets import set_secret

ALLOWED = {"DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN", "PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD",
           "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"}

if len(sys.argv) != 2 or sys.argv[1] not in ALLOWED:
    sys.exit(f"usage: set_secret.py <{'|'.join(sorted(ALLOWED))}>")
value = getpass.getpass(f"{sys.argv[1]}: ").strip()
if not value:
    sys.exit("empty value; nothing stored")
set_secret(sys.argv[1], value)
print(f"stored {sys.argv[1]} in Windows Credential Manager")
