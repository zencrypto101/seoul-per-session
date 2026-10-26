#!/usr/bin/env python3
"""Durable sink for the Phase A demand test.

The page posts events to an ntfy topic. ntfy keeps messages only for a
limited time, so reading the topic once at the end of a 14-day window
returns nothing. This job polls on a schedule and appends every message
it has not seen before to data/events.csv, keyed by the ntfy message id,
so the history outlives the topic.

Measured on 2026-10-26: events sent 2026-10-23 returned 0 rows from the
same topic, so retention is under 2.5 days and not enough on its own.
"""
import csv
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

TOPIC = os.environ.get("NTFY_TOPIC", "4bc41ce028725aeea6d96d114f5872f9")
URL = "https://ntfy.sh/%s/json?poll=1&since=all" % TOPIC
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "data", "events.csv")
HEARTBEAT = os.path.join(HERE, "..", "data", "last_poll.json")
FIELDS = ["ntfy_time_utc", "publisher_time_utc", "ntfy_id",
          "e", "ch", "sid", "extra", "body"]


def fetch():
    req = urllib.request.Request(URL, headers={"User-Agent": "phase-a-sink/1"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read().decode("utf-8")
    rows = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("event") != "message":
            continue
        rows.append(msg)
    return rows


def parse_body(body):
    fields, rest = {}, []
    for part in body.split("&"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        if k in ("e", "ch", "sid"):
            fields[k] = v
        else:
            rest.append("%s=%s" % (k, v))
    return fields.get("e", ""), fields.get("ch", ""), fields.get("sid", ""), " ".join(sorted(rest))


def iso(ts):
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_seen():
    if not os.path.exists(OUT):
        return set()
    with open(OUT, newline="", encoding="utf-8") as f:
        return {r["ntfy_id"] for r in csv.DictReader(f) if r.get("ntfy_id")}


def main():
    seen = load_seen()
    added = 0
    try:
        msgs = fetch()
    except Exception as exc:  # a dead poller must be loud, not silent
        print("poll failed: %r" % (exc,), file=sys.stderr)
        write_heartbeat("error", str(exc), 0)
        return 1

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    new_file = not os.path.exists(OUT)
    with open(OUT, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            w.writeheader()
        for msg in msgs:
            mid = msg.get("id", "")
            if not mid or mid in seen:
                continue
            body = msg.get("message", "") or ""
            e, ch, sid, extra = parse_body(body)
            w.writerow({
                "ntfy_time_utc": iso(msg.get("time")),
                "publisher_time_utc": iso(msg.get("publish_time") or msg.get("time")),
                "ntfy_id": mid,
                "e": e,
                "ch": ch,
                "sid": sid,
                "extra": extra,
                "body": body,
            })
            seen.add(mid)
            added += 1

    write_heartbeat("ok", "", added)
    print("topic=%s cached=%d new=%d total=%d" % (TOPIC, len(msgs), added, len(seen)))
    return 0


def write_heartbeat(status, error, added):
    os.makedirs(os.path.dirname(HEARTBEAT), exist_ok=True)
    payload = {
        "polled_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status,
        "error": error,
        "events_added": added,
    }
    with open(HEARTBEAT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


if __name__ == "__main__":
    sys.exit(main())
