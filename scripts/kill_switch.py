"""Console kill switch (v1.3 §9.5) — every action is written to the hash-chained operator log.

  .venv\\Scripts\\python scripts\\kill_switch.py status
  .venv\\Scripts\\python scripts\\kill_switch.py on  --actor anoop --reason "feed looks wrong" [--emergency-close]
  .venv\\Scripts\\python scripts\\kill_switch.py off --actor anoop --reason "feed fixed" --confirm RESUME
  .venv\\Scripts\\python scripts\\kill_switch.py ack --actor anoop --reason "checked clock skew"   # automatic trip
  .venv\\Scripts\\python scripts\\kill_switch.py log                                              # verify + tail
"""
import argparse
import json

import _bootstrap  # noqa: F401

from app.control.actions import (ActionError, OperatorLog, acknowledge_auto, kill_off, kill_on, kill_status)
from app.notify.telegram import Alert, CRITICAL, Notifier, kill_switch_alert, make_transport


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=("status", "on", "off", "ack", "log"))
    ap.add_argument("--actor")
    ap.add_argument("--reason")
    ap.add_argument("--confirm", default="")
    ap.add_argument("--emergency-close", action="store_true")
    a = ap.parse_args()
    try:
        if a.cmd == "status":
            print(json.dumps(kill_status(), indent=2))
            return 0
        if a.cmd == "log":
            log = OperatorLog()
            print(f"chain intact: {log.verify()}")
            for r in log.records()[-10:]:
                print(f"{r['seq']:>4} {r['at']} {r['action']:<24} {r['actor']:<12} {r['reason']}")
            return 0
        if a.cmd == "on":
            kill_on(a.actor, a.reason, emergency_close=a.emergency_close)
            alert = kill_switch_alert(True, a.actor, a.reason, a.emergency_close)
        elif a.cmd == "off":
            kill_off(a.actor, a.reason, a.confirm)
            alert = kill_switch_alert(False, a.actor, a.reason)
        else:
            acknowledge_auto(a.actor, a.reason)
            alert = Alert("kill_switch", CRITICAL, f"Automatic kill-switch acknowledgement requested by {a.actor}: {a.reason}")
    except ActionError as e:
        print(f"refused: {e}")
        return 2
    print(json.dumps(kill_status(), indent=2))
    print(f"telegram: {Notifier(make_transport()).send(alert)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
