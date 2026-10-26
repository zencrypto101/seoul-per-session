# data/

`events.csv` is written by `.github/workflows/sink-poll.yml` (every 3 hours)
and is the durable record of Phase A events. The ntfy topic itself only keeps
messages briefly — on 2026-10-26, events sent on 2026-10-23 returned nothing.

Columns: `ntfy_time_utc`, `publisher_time_utc`, `ntfy_id`, `e`, `ch`, `sid`,
`extra`, `body`. `ntfy_id` is the dedupe key, so re-polls do not duplicate rows.

No values a visitor typed are ever sent: `sid` is
`SHA-256(UTC day | User-Agent | viewport)[:16]`, and `extra` holds counters
such as `f=` (fields filled) and `t=` (seconds), never amounts.

`last_poll.json` is a heartbeat: if `polled_utc` goes stale, the poller is dead
and events older than the retention window are being lost.
